"""可恢复的偏离研究执行进程。

队列只提示“有活”，数据库是权威：启动时和运行中都会把 queued 及租约过期的
running 研究重新提示；领取用条件更新产生唯一 claim_token；结果只在仍持有该
token 时提交。进程中断（领取前后、拟合中、写结果前后）后，重启会重新领取并
沿用创建时冻结的快照/alpha/seed；已完成研究不会被任何旧执行改写。
"""
import json
import threading
import time
import traceback
from .config.database import ensure_schema, SessionLocal
from .config.settings import WORKER_HEARTBEAT_SECONDS, WORKER_LEASE_SECONDS, WORKER_RECOVERY_SECONDS
from .models import DeviationRun
from .constants.status import RunStatus
from .services.run_service import queue
from .services import job_service
from .services.execution_service import artifact_dir, reconcile_artifact, run_claim

LEASE = WORKER_LEASE_SECONDS
HEARTBEAT = min(WORKER_HEARTBEAT_SECONDS, max(1, LEASE // 3))
RECOVERY = WORKER_RECOVERY_SECONDS


def _heartbeat():
    try:
        path = artifact_dir() / "heartbeat"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(time.time()))
    except OSError:
        traceback.print_exc()


def _recover(announced):
    try:
        pending, expired = job_service.recover(queue, announced=announced)
        if pending or expired:
            print(json.dumps({"recovered_queued": len(pending), "recovered_expired": len(expired)}, ensure_ascii=False), flush=True)
    except Exception:
        traceback.print_exc()


def _reconcile_artifacts():
    """已完成研究的结果档案缺失/过期时用库内 result 补齐，保持档案与状态一致。"""
    try:
        with SessionLocal() as db:
            rows = db.query(DeviationRun).filter(DeviationRun.status == RunStatus.COMPLETED).all()
        for run in rows:
            reconcile_artifact(run)
    except Exception:
        traceback.print_exc()


def claim_lease(rid, execution_id):
    """领取是独立短事务，避免执行线程的事务持有旧视图影响条件更新。"""
    with SessionLocal() as db:
        return job_service.claim(db, rid, execution_id)


def _execute(rid, token, execution_id):
    """在续约线程保护下执行；执行完成即停止续约，令牌保持提交时的有效性。"""
    done = threading.Event()
    outcome = {}

    def renew():
        while not done.wait(HEARTBEAT):
            try:
                with SessionLocal() as db:
                    if not job_service.renew(db, rid, token):
                        return
            except Exception:
                traceback.print_exc()

    def work():
        try:
            outcome["committed"] = run_claim(rid, token, execution_id)
        except Exception:
            traceback.print_exc()
            outcome["committed"] = False
        finally:
            done.set()

    renewer = threading.Thread(target=renew, name=f"renew-{rid}", daemon=True)
    worker = threading.Thread(target=work, name=f"run-{rid}", daemon=True)
    renewer.start()
    worker.start()
    worker.join()
    done.set()
    return outcome.get("committed", False)


def main():
    ensure_schema()
    execution_id = job_service.worker_id()
    announced = set()
    _heartbeat()
    _recover(announced)
    _reconcile_artifacts()
    last_recovery = time.time()
    while True:
        _heartbeat()
        if time.time() - last_recovery >= RECOVERY:
            _recover(announced)
            _reconcile_artifacts()
            last_recovery = time.time()
        job = queue.blpop(job_service.JOB_KEY, timeout=2)
        if not job:
            continue
        rid = int(job[1])
        announced.discard(rid)
        token = claim_lease(rid, execution_id)
        if not token:
            # 已完成、已被别的执行领取，或重复投递的旧提示：直接忽略。
            continue
        _execute(rid, token, execution_id)


if __name__ == "__main__":
    main()
