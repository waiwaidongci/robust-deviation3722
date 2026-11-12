"""进程级验证：真实 worker 进程被 SIGKILL 后自动恢复中断研究。

用 SQLite + 文件队列（tests/proc_worker_boot.py）避免外部服务；验证的是同一套
条件领取/令牌提交/租约恢复代码路径。覆盖：
1) 排队通知丢失（stub 队列不可用）→ 重启 worker 自动扫描数据库恢复；
2) 领走后、拟合中被 SIGKILL → 重启后租约过期重新领取并按冻结输入完成；
3) 完成后结果档案、状态、完成留痕一致，且旧执行无法覆盖。
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
TMP = tempfile.mkdtemp(prefix="proc-restart-")
ENV = {
    **os.environ,
    "PYTHONPATH": f"{REPO / 'backend'}:{REPO / 'backend' / 'tests'}",
    "DATABASE_URL": f"sqlite:///{TMP}/proc.db",
    "ARTIFACT_DIR": f"{TMP}/artifacts",
    "TEST_QUEUE_FILE": f"{TMP}/queue.txt",
    "TEST_BACKEND": str(REPO / "backend"),
    "WORKER_LEASE_SECONDS": "3",
    "WORKER_HEARTBEAT_SECONDS": "1",
    "WORKER_RECOVERY_SECONDS": "1",
    "SEED_DEMO": "false",
}
CWD = str(REPO / "backend")


def py(code, *args, extra_env=None):
    env = {**ENV, **(extra_env or {})}
    return subprocess.run([sys.executable, "-c", code, *map(str, args)], env=env, cwd=CWD, capture_output=True, text=True)


SETUP = """
import numpy as np
from src.config.database import ensure_schema, SessionLocal
from src.models import ArchiveStudy, ClimateWindow, FeatureSpec
from src.schemas.run import RunInput
from src.services.run_service import create_run
ensure_schema()
with SessionLocal() as db:
    s = ArchiveStudy(code='SL-01', name='主群', description='d', source='s')
    db.add(s); db.flush()
    for i, c in enumerate(['temperature', 'humidity', 'particles', 'variation']):
        db.add(FeatureSpec(study_id=s.id, code=c, label=c, unit='u', position=i, meaning=c))
    rng = np.random.default_rng(7)
    X = rng.normal(size=(40, 4)) * np.array([1.5, 5., 4., .12]) + np.array([19.5, 47., 18., .35])
    for i, row in enumerate(X):
        split = 'train' if i < 25 else 'cal' if i < 35 else 'test'
        db.add(ClimateWindow(study_id=s.id, code=f'W{i:03}', split=split, note='',
                             values={c: float(v) for c, v in zip(['temperature', 'humidity', 'particles', 'variation'], row)}))
    db.commit()
    class U:
        name = '温砚'; email = 'analyst@shuangleng.demo'
    run = create_run(db, RunInput(name='进程中断验证', study_id=s.id, alpha=.05, seed=2026), U)
    print(run['id'])
"""

QUERY = """
import json, sys
from src.config.database import SessionLocal
from src.models import DeviationRun, Audit
with SessionLocal() as db:
    run = db.get(DeviationRun, int(sys.argv[1]))
    payload = {
        'status': run.status, 'attempt': run.attempt, 'owner': run.claim_owner,
        'finished': bool(run.finished_at), 'fit_hash': run.result.get('fit_hash'),
        'input_hash': run.result.get('input_hash'), 'error': run.error,
        'resumed_audits': db.query(Audit).filter_by(action='deviation.resumed', target=str(run.id)).count(),
        'completed_audits': db.query(Audit).filter_by(action='deviation.completed', target=str(run.id)).count(),
    }
    print(json.dumps(payload))
"""

MARK_LOST = """
import sys
from src.config.database import SessionLocal
from src.models import DeviationRun
with SessionLocal() as db:
    row = db.get(DeviationRun, int(sys.argv[1]))
    row.status = 'running'; row.claim_token = 'live-token'; row.claim_owner = 'killed-worker'
    row.lease_expires_at = '2000-01-01T00:00:00+00:00'
    db.commit()
"""


def state(rid):
    q = py(QUERY, rid)
    assert q.returncode == 0, q.stderr
    return json.loads(q.stdout.strip().splitlines()[-1])


def wait_for(rid, predicate, timeout=25):
    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = state(rid)
        if predicate(last):
            return last
        time.sleep(0.2)
    raise AssertionError(f"超时，最后状态 {last}")


def spawn(extra_env=None):
    boot = REPO / "backend" / "tests" / "proc_worker_boot.py"
    env = {**ENV, **(extra_env or {})}
    return subprocess.Popen([sys.executable, str(boot)], env=env, cwd=CWD,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True)


def kill(proc):
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait(timeout=10)


def test_worker_restart_recovers_all_interruptions():
    setup = py(SETUP)
    assert setup.returncode == 0, setup.stderr
    rid = int(setup.stdout.strip().splitlines()[-1])

    # 1) 通知丢失：研究停在 queued；启动 worker 后自动扫描并完成。
    worker = spawn()
    first = wait_for(rid, lambda s: s["status"] == "completed")
    kill(worker)
    assert first["attempt"] == 1 and first["resumed_audits"] == 0, first
    assert first["completed_audits"] == 1 and first["fit_hash"] and first["input_hash"], first
    assert Path(ENV["ARTIFACT_DIR"], f"run-{rid}.json").exists()

    # 2) 领走后、拟合中被 SIGKILL：重启后租约过期重新领取并完成。
    mark = py(MARK_LOST, rid)
    assert mark.returncode == 0, mark.stderr
    doomed = spawn({"TEST_SLOW_ANALYSIS": "30"})
    wait_for(rid, lambda s: s["status"] == "running" and s["attempt"] == 2)
    kill(doomed)
    assert state(rid)["status"] == "running"
    restarted = spawn()
    final = wait_for(rid, lambda s: s["status"] == "completed")
    kill(restarted)
    assert final["attempt"] == 3, final
    assert final["resumed_audits"] == 2, final
    assert final["completed_audits"] == 2, final
    # 冻结输入 => 与首次完成完全相同的 fit/input 指纹
    assert final["fit_hash"] == first["fit_hash"], final
    assert final["input_hash"] == first["input_hash"], final
    artifact = json.loads(Path(ENV["ARTIFACT_DIR"], f"run-{rid}.json").read_text())
    assert artifact["fit_hash"] == final["fit_hash"]
    assert artifact["input_hash"] == final["input_hash"]
