"""偏离研究的可恢复领取与一次性结果提交。

队列只作为“有活”的提示，事实来源始终是数据库中的研究行：

* 领取 = 在一个短事务里把 queued/过期的 running 行改成本次运行的 claim_token。
  重复投递同一条提示不会产生第二次执行，多个执行进程争抢时只有一个能改到行。
* 提交 = 只有仍持有当前 token、且状态仍为 running 时才写入 result/status。
  旧执行（续约失败、进程重启后被重新领取）的 token 已失效，结果被拒绝，
  因此晚到的旧执行不能覆盖当前有效结果。

已完成研究是终态，任何执行都不会再改动它的状态或结果。
"""
import socket
import uuid
from datetime import datetime, timedelta, timezone
from sqlalchemy import update
from ..config.database import SessionLocal
from ..config.settings import WORKER_ID, WORKER_LEASE_SECONDS
from ..constants.status import RunStatus
from ..models import DeviationRun
from ..utils.clock import now

JOB_KEY = "deviation:jobs"


def worker_id():
    """每个执行进程一个稳定标识；容器重启后仍可辨认旧租约。"""
    return WORKER_ID or f"{socket.gethostname()}:{uuid.uuid4().hex[:8]}"


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def lease_deadline(seconds=None):
    return _iso(datetime.now(timezone.utc) + timedelta(seconds=WORKER_LEASE_SECONDS if seconds is None else seconds))


def claim(db, rid, execution_id):
    """尝试领取一个待执行或租约已过期的研究；返回新的 claim_token 或 None。

    使用带条件的 UPDATE，多个执行进程同时争抢时只有一个能改到行。
    新 token 意味着上一次领取（若有）作废，它的结果提交将被拒绝。
    """
    stamp = now()
    row = db.execute(
        update(DeviationRun)
        .where(
            DeviationRun.id == rid,
            DeviationRun.status.in_((RunStatus.QUEUED, RunStatus.RUNNING)),
            # queued 尚未被领取，或 running 的租约已过期（执行进程中断）
            (DeviationRun.claim_token == "")
            | (DeviationRun.lease_expires_at == "")
            | (DeviationRun.lease_expires_at < stamp),
        )
        .values(
            status=RunStatus.RUNNING,
            claim_token=uuid.uuid4().hex,
            claim_owner=execution_id,
            attempt=DeviationRun.attempt + 1,
            lease_expires_at=lease_deadline(),
            started_at=stamp,
            error="",
        )
        .returning(DeviationRun.claim_token)
    ).first()
    db.commit()
    return row[0] if row else None


def renew(db, rid, token):
    """续约当前领取；返回 False 表示令牌已失效（研究被重新领取或已完成）。"""
    stamp = now()
    row = db.execute(
        update(DeviationRun)
        .where(
            DeviationRun.id == rid,
            DeviationRun.claim_token == token,
            DeviationRun.status == RunStatus.RUNNING,
        )
        .values(lease_expires_at=lease_deadline())
        .returning(DeviationRun.id)
    ).first()
    db.commit()
    return row is not None


def complete(db, rid, token, result):
    """提交结果。只有当前有效领取能成功；旧执行返回 False 且不写任何内容。

    不在此提交事务：调用方在同一事务里写完成留痕，保证状态与留痕原子一致。
    """
    stamp = now()
    row = db.execute(
        update(DeviationRun)
        .where(
            DeviationRun.id == rid,
            DeviationRun.claim_token == token,
            DeviationRun.status == RunStatus.RUNNING,
        )
        .values(
            status=RunStatus.COMPLETED,
            result=result,
            error="",
            finished_at=stamp,
        )
        .returning(DeviationRun.id)
    ).first()
    return row is not None


def fail(db, rid, token, error):
    """把当前领取标记为失败；旧执行不能改写已被重新领取的研究。"""
    stamp = now()
    row = db.execute(
        update(DeviationRun)
        .where(
            DeviationRun.id == rid,
            DeviationRun.claim_token == token,
            DeviationRun.status == RunStatus.RUNNING,
        )
        .values(status=RunStatus.FAILED, error=error[:2000], finished_at=stamp)
        .returning(DeviationRun.id)
    ).first()
    return row is not None


def requeue_due(db, limit=100):
    """找出需要重新提示的研究：租约已过期（或旧版遗留）的 running 研究。

    中断发生在领取后、写结果前的研究都停在 running；这里不改结果，
    只让下一次执行重新领取并沿用创建时冻结的 input_snapshot/alpha/seed。
    已完成研究不在查询范围内。
    """
    return [r[0] for r in _expired_query(db, limit).all()]


def _expired_query(db, limit=100):
    """running 但租约已过期，或旧版本遗留的 running（无领取令牌/租约）都需重新执行。"""
    stamp = now()
    return db.query(DeviationRun.id).filter(
        DeviationRun.status == RunStatus.RUNNING,
        (DeviationRun.claim_token == "")
        | (DeviationRun.lease_expires_at == "")
        | (DeviationRun.lease_expires_at < stamp),
    ).order_by(DeviationRun.id).limit(limit)


def recover(queue, limit=100, announced=None):
    """把待执行与租约过期的研究重新放回提示列表。

    Redis 中的原提示可能在中断时已消失，这里以数据库为准补齐，做到排队通知可恢复。
    提示是幂等的：同一研究被重复投递时，领取阶段仍只有一个执行能拿到令牌。
    announced 为本进程已投递且尚未消费的编号集合，避免周期恢复反复堆积；
    未成功投递的编号保留在待投列表，下轮仍会重试。
    返回 (待执行的 queued, 租约过期需重领的 running)。
    """
    with SessionLocal() as db:
        pending = [r[0] for r in db.query(DeviationRun.id).filter(
            DeviationRun.status == RunStatus.QUEUED,
        ).order_by(DeviationRun.id).limit(limit).all()]
        expired = requeue_due(db, limit)
    todo = [rid for rid in pending + expired if announced is None or rid not in announced]
    for rid in todo:
        try:
            queue.rpush(JOB_KEY, rid)
            if announced is not None:
                announced.add(rid)
        except Exception:
            pass
    return pending, expired

