"""服务重启后自动恢复中断研究；重复投递/多执行进程只认当前有效结果。

运行：cd backend && PYTHONPATH=. python3 -m pytest tests/test_recovery.py
这里用 SQLite 与 FakeRedis 模拟数据库/队列，不依赖容器；用例覆盖领走前/后、
写结果前/后中断、通知丢失、重复投递与多执行争抢，并核对状态/档案/留痕一致。
"""
import os
import sys
import threading
import time
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
TMP = Path(os.environ["ARTIFACT_DIR"]).parent  # conftest 已设置隔离目录

import pytest
from src.config.database import ensure_schema, SessionLocal
from src.constants.status import RunStatus
from src.models import ArchiveStudy, ClimateWindow, FeatureSpec, DeviationRun, ReviewNote, Audit
from src.services import job_service, execution_service
from src.services.analysis_service import analyze_deviation
from src.services.run_service import create_run
from src.schemas.run import RunInput

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fake_redis import FakeRedis


class FakeUser:
    name = "温砚 · 联合量研究员"
    email = "analyst@shuangleng.demo"


@pytest.fixture(scope="module", autouse=True)
def schema():
    ensure_schema()


@pytest.fixture(autouse=True)
def clean_db():
    """每个用例独立数据；保留表结构，覆盖中断状态需要重复运行。"""
    with SessionLocal() as db:
        db.query(ReviewNote).delete()
        db.query(DeviationRun).delete()
        db.query(ClimateWindow).delete()
        db.query(FeatureSpec).delete()
        db.query(ArchiveStudy).delete()
        db.query(Audit).delete()
        db.commit()
    for f in Path(os.environ["ARTIFACT_DIR"]).glob("run-*.json"):
        f.unlink()
    yield


@pytest.fixture()
def study():
    with SessionLocal() as db:
        s = ArchiveStudy(code="SL-01", name="主群", description="d", source="synthetic")
        db.add(s)
        db.flush()
        for i, code in enumerate(["temperature", "humidity", "particles", "variation"]):
            db.add(FeatureSpec(study_id=s.id, code=code, label=code, unit="u", position=i, meaning=code))
        import numpy as np
        rng = np.random.default_rng(1)
        X = rng.normal(size=(40, 4)) * np.array([1.5, 5., 4., .12]) + np.array([19.5, 47., 18., .35])
        for i, row in enumerate(X):
            split = "train" if i < 25 else "cal" if i < 35 else "test"
            db.add(ClimateWindow(study_id=s.id, code=f"W{i:03}", split=split,
                                 values={c: float(v) for c, v in zip(["temperature", "humidity", "particles", "variation"], row)}, note=""))
        db.commit()
    return s


@pytest.fixture()
def queue():
    return FakeRedis()


def make_run(study, queue, alpha=.05, seed=2026):
    with SessionLocal() as db:
        return create_run(db, RunInput(name="恢复验证", study_id=study.id, alpha=alpha, seed=seed), FakeUser())


def get(rid):
    with SessionLocal() as db:
        return db.get(DeviationRun, rid)


def artifact(rid):
    import json
    return json.loads(execution_service.artifact_path(rid).read_text())


def audits(action, target):
    with SessionLocal() as db:
        return [(a.action, a.target) for a in db.query(Audit).filter_by(action=action, target=str(target))]


def claim_with(rid, execution_id):
    with SessionLocal() as db:
        return job_service.claim(db, rid, execution_id)


def complete_with(rid, token, result):
    with SessionLocal() as db:
        return job_service.complete(db, rid, token, result)


def requeue_due_direct():
    with SessionLocal() as db:
        return job_service.requeue_due(db)


def drain(queue):
    while queue.blpop(job_service.JOB_KEY):
        pass


# ---------- 创建入队与通知丢失 ----------

def test_notification_lost_is_recovered_from_database(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    drain(queue)  # 模拟通知丢失
    assert get(run["id"]).status == RunStatus.QUEUED
    pending, expired = job_service.recover(queue)
    assert run["id"] in pending and not expired
    token = claim_with(run["id"], "exec-recover")
    assert token and execution_service.run_claim(run["id"], token, "exec-recover")
    assert get(run["id"]).status == RunStatus.COMPLETED


def test_queue_outage_keeps_run_queued_for_recovery(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    def boom(*args, **kwargs):
        raise ConnectionError("redis down")
    monkeypatch.setattr(queue, "rpush", boom)
    run = make_run(study, queue)
    row = get(run["id"])
    assert row.status == RunStatus.QUEUED and row.error
    deferred = len(audits("deviation.enqueue.deferred", run["id"]))
    assert deferred == 1
    monkeypatch.undo()
    pending, _ = job_service.recover(queue)
    assert run["id"] in pending


# ---------- 中断点：领走前 / 领走后 ----------

def test_crash_before_claim_resumes_frozen_inputs(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    frozen = get(run["id"]).input_snapshot
    drain(queue)  # 进程在领取前中断，队列提示丢失
    expected = analyze_deviation(frozen, .05, 2026)
    pending, _ = job_service.recover(queue)
    assert run["id"] in pending
    token = claim_with(run["id"], "exec-after-restart")
    assert execution_service.run_claim(run["id"], token, "exec-after-restart")
    row = get(run["id"])
    assert row.status == RunStatus.COMPLETED
    assert row.result["input_hash"] == expected["input_hash"]
    assert row.result["fit_hash"] == expected["fit_hash"]
    assert row.attempt == 1
    assert artifact(run["id"])["input_hash"] == row.result["input_hash"]


def test_crash_after_claim_is_reclaimed_when_lease_expires(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    drain(queue)
    rid = run["id"]
    token = claim_with(rid, "exec-crashed")
    assert token
    assert get(rid).status == RunStatus.RUNNING  # 停在 running，原实现会一直挂着
    assert not requeue_due_direct()  # 租约未过期不抢
    # 模拟进程消失且租约过期
    with SessionLocal() as db:
        row = db.get(DeviationRun, rid)
        row.lease_expires_at = "2000-01-01T00:00:00+00:00"
        db.commit()
    expired = requeue_due_direct()
    assert rid in expired
    job_service.recover(queue)
    token2 = claim_with(rid, "exec-restarted")
    assert token2 and token2 != token
    assert execution_service.run_claim(rid, token2, "exec-restarted")
    row = get(rid)
    assert row.status == RunStatus.COMPLETED and row.attempt == 2
    assert row.result["input_hash"] == analyze_deviation(row.input_snapshot, row.alpha, row.seed)["input_hash"]
    assert ("deviation.resumed", str(rid)) in audits("deviation.resumed", rid)
    # 旧执行晚到，不能覆盖
    assert not execution_service.run_claim(rid, token, "exec-crashed")


# ---------- 写结果前后中断 ----------

def test_interrupt_after_result_computed_before_artifact(study, queue, monkeypatch, tmp_path):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    drain(queue)
    rid = run["id"]
    real_write = execution_service.write_artifact

    def crash(*args, **kwargs):
        raise KeyboardInterrupt("power loss after commit")

    monkeypatch.setattr(execution_service, "write_artifact", crash)
    token = claim_with(rid, "exec-a")
    with pytest.raises(KeyboardInterrupt):
        execution_service.run_claim(rid, token, "exec-a")
    monkeypatch.setattr(execution_service, "write_artifact", real_write)
    # 数据库已提交完成，档案缺失
    assert get(rid).status == RunStatus.COMPLETED
    assert not execution_service.artifact_path(rid).exists()
    with SessionLocal() as db:
        repaired = execution_service.reconcile_artifact(db.get(DeviationRun, rid))
    assert repaired is True
    assert artifact(rid)["input_hash"] == get(rid).result["input_hash"]


def test_interrupt_before_result_commit_does_not_strand_or_duplicate(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    drain(queue)
    rid = run["id"]
    # 领取后、拟合中中断：结果从未提交
    token = claim_with(rid, "exec-a")
    assert get(rid).status == RunStatus.RUNNING and get(rid).result in ({}, None)
    with SessionLocal() as db:
        row = db.get(DeviationRun, rid)
        row.lease_expires_at = "2000-01-01T00:00:00+00:00"
        db.commit()
    job_service.recover(queue)
    token2 = claim_with(rid, "exec-b")
    assert execution_service.run_claim(rid, token2, "exec-b")
    result = get(rid).result
    assert get(rid).status == RunStatus.COMPLETED
    # 结果档案与库一致，且只有当前执行写入
    assert artifact(rid)["fit_hash"] == result["fit_hash"]
    assert get(rid).finished_at


# ---------- 重复投递与多执行争抢 ----------

def test_duplicate_delivery_executes_only_once(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    rid = run["id"]
    for _ in range(5):
        queue.rpush(job_service.JOB_KEY, rid)  # 重复投递同一研究
    completed = 0
    for _ in range(5):
        job = queue.blpop(job_service.JOB_KEY)
        if not job:
            continue
        assert int(job[1]) == rid
        token = claim_with(rid, "exec-dup")
        if not token:
            continue
        if execution_service.run_claim(rid, token, "exec-dup"):
            completed += 1
    assert completed == 1
    row = get(rid)
    assert row.status == RunStatus.COMPLETED and row.attempt == 1
    assert len(audits("deviation.completed", rid)) == 1


def test_concurrent_claim_has_single_winner(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    rid = run["id"]
    winners = []
    barrier = threading.Barrier(6)

    def race(name):
        barrier.wait()
        token = claim_with(rid, name)
        if token:
            winners.append((name, token))

    threads = [threading.Thread(target=race, args=(f"exec-{i}",)) for i in range(6)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(winners) == 1
    # 获胜者完成；其余执行晚到也改写不了
    name, token = winners[0]
    assert execution_service.run_claim(rid, token, name)
    assert get(rid).status == RunStatus.COMPLETED
    assert not execution_service.run_claim(rid, "not-a-token", "late-exec")


def test_stale_execution_cannot_overwrite_completed_result(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    rid = run["id"]
    first = claim_with(rid, "exec-1")
    assert execution_service.run_claim(rid, first, "exec-1")
    before = get(rid).result
    # 模拟旧执行晚到：拿着旧令牌/新令牌都无效
    assert not execution_service.run_claim(rid, first, "exec-1")
    assert not complete_with(rid, first, {"hijacked": True})
    assert get(rid).result == before


# ---------- worker 循环：重启自动恢复、续约、多进程争抢 ----------

def test_worker_loop_recovers_queued_after_restart(study, queue, monkeypatch):
    """模拟 worker 进程被杀：notify 已丢，重启 worker main 循环自动恢复并完成。"""
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    drain(queue)  # 通知丢失，研究停在 queued
    import src.worker as worker

    class Stop(Exception):
        pass

    monkeypatch.setattr(worker, "RECOVERY", 0.01)
    calls = {"n": 0}

    def blpop(key, timeout=0):
        calls["n"] += 1
        value = queue.blpop(key)
        if value:
            return value
        if calls["n"] > 20:
            raise Stop
        return None

    monkeypatch.setattr(worker, "queue", type("Q", (), {"blpop": staticmethod(blpop), "rpush": staticmethod(queue.rpush)})())
    with pytest.raises(Stop):
        worker.main()
    assert get(run["id"]).status == RunStatus.COMPLETED


def test_long_execution_keeps_lease_alive(study, queue, monkeypatch):
    """执行时长超过租约时，续约线程保活，另一个执行进程抢不走。"""
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    rid = run["id"]
    import src.worker as worker
    token = claim_with(rid, "exec-slow")
    real = execution_service.analyze_deviation

    def slow(snapshot, alpha, seed):
        time.sleep(3.2 if worker.LEASE == 2 else worker.LEASE * 1.5)
        return real(snapshot, alpha, seed)

    monkeypatch.setattr(execution_service, "analyze_deviation", slow)
    monkeypatch.setattr(worker, "HEARTBEAT", 0.3)
    stolen = {}

    def attempt_steal():
        time.sleep(1.0)
        stolen["token"] = claim_with(rid, "exec-thief")

    thief = threading.Thread(target=attempt_steal)
    thief.start()
    committed = worker._execute(rid, token, "exec-slow")
    thief.join()
    assert committed
    assert stolen["token"] is None  # 租约一直被续，争抢失败
    assert get(rid).status == RunStatus.COMPLETED and get(rid).attempt == 1


def test_multi_process_race_only_current_result_survives(study, queue, monkeypatch):
    """两个执行进程同时跑同一研究：旧令牌写到时被拒，库内结果只来自当前令牌。"""
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    rid = run["id"]
    old = claim_with(rid, "exec-old")
    with SessionLocal() as db:
        row = db.get(DeviationRun, rid)
        row.lease_expires_at = "2000-01-01T00:00:00+00:00"
        db.commit()
    new = claim_with(rid, "exec-new")
    assert new and new != old
    order = []
    real = execution_service.analyze_deviation

    def slow(snapshot, alpha, seed):
        time.sleep(0.2)
        order.append(snapshot["study"]["code"])
        return real(snapshot, alpha, seed)

    monkeypatch.setattr(execution_service, "analyze_deviation", slow)
    results = {}
    threads = [
        threading.Thread(target=lambda: results.__setitem__("old", execution_service.run_claim(rid, old, "exec-old"))),
        threading.Thread(target=lambda: results.__setitem__("new", execution_service.run_claim(rid, new, "exec-new"))),
    ]
    for t in threads: t.start()
    for t in threads: t.join()
    assert results["new"] is True  # 新执行提交
    row = get(rid)
    assert row.status == RunStatus.COMPLETED and row.attempt == 2
    assert row.claim_owner == "exec-new"
    if results.get("old") is True:
        # 若旧执行先提交，它的令牌在领取时已被替换，提交必须被拒
        raise AssertionError("旧执行不应写成功")
    assert row.result["input_hash"] == real(row.input_snapshot, row.alpha, row.seed)["input_hash"]
    assert artifact(rid)["input_hash"] == row.result["input_hash"]


# ---------- 已完成研究与获批建议保持不变 ----------

def test_completed_run_and_approved_note_unchanged(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue)
    rid = run["id"]
    token = claim_with(rid, "exec-1")
    assert execution_service.run_claim(rid, token, "exec-1")
    with SessionLocal() as db:
        db.add(ReviewNote(run_id=rid, title="补测", interpretation="独立核验", proposal="补测计划",
                          status="approved", owner="analyst", reviewer="admin", review_note="批准"))
        db.commit()
    result = get(rid).result
    audit_count = len(audits("deviation.completed", rid))
    # 再次恢复/重复投递都不应改动完成研究
    job_service.recover(queue)
    assert not claim_with(rid, "exec-2")
    execution_service.reconcile_artifact(get(rid))
    row = get(rid)
    assert row.status == RunStatus.COMPLETED and row.result == result
    assert row.attempt == 1 and len(audits("deviation.completed", rid)) == audit_count
    with SessionLocal() as db:
        note = db.query(ReviewNote).filter_by(run_id=rid).one()
        assert note.status == "approved" and note.review_note == "批准"


def test_resumed_result_matches_uninterrupted_for_same_frozen_inputs(study, queue, monkeypatch):
    monkeypatch.setattr("src.services.run_service.queue", queue)
    run = make_run(study, queue, alpha=.07, seed=42)
    rid = run["id"]
    frozen = dict(get(rid).input_snapshot)
    reference = analyze_deviation(frozen, .07, 42)
    # 中途用户改了来源窗口批注，恢复执行不得读取新批注
    with SessionLocal() as db:
        w = db.query(ClimateWindow).filter_by(study_id=study.id).first()
        w.note = "后来批注"
        db.commit()
    claim_with(rid, "exec-x")
    with SessionLocal() as db:
        row = db.get(DeviationRun, rid)
        row.lease_expires_at = "2000-01-01T00:00:00+00:00"
        db.commit()
    job_service.recover(queue)
    token = claim_with(rid, "exec-y")
    assert execution_service.run_claim(rid, token, "exec-y")
    row = get(rid)
    assert row.alpha == .07 and row.seed == 42
    assert row.result["fit_hash"] == reference["fit_hash"]
    assert row.result["input_hash"] == reference["input_hash"]
    assert row.result["input_snapshot"] == frozen
