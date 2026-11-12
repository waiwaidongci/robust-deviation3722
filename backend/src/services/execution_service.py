"""执行一个已领取的偏离研究。

研究创建时把 input_snapshot/alpha/seed 冻结在行上；恢复执行只读这些冻结值，
绝不重新读取来源窗口或用户后来的批注，因此重启前后、重复投递前后结果一致。
数据库状态是唯一权威：结果档案从已提交的 result 写出，绝不由失败/过期执行写入。
"""
import json
import os
import traceback
from pathlib import Path
from ..config.database import SessionLocal
from ..constants.status import RunStatus
from ..models import DeviationRun
from ..services.analysis_service import analyze_deviation
from ..services.audit_service import log
from . import job_service


def artifact_dir():
    return Path(os.environ.get("ARTIFACT_DIR", "/artifacts"))


def artifact_path(rid):
    return artifact_dir() / f"run-{rid}.json"


def write_artifact(rid, result):
    """先写临时文件再原子替换，进程中断不会留下半份结果档案。"""
    target = artifact_path(rid)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(result, ensure_ascii=False))
    os.replace(tmp, target)


def run_claim(rid, token, execution_id):
    """执行一次已领取的研究。

    返回 True 表示本次执行写入了当前有效结果；False 表示令牌已失效（研究已被
    重新领取或已完成），本执行的任何结果/错误都不会落库。
    中断场景：领取后、拟合中、写结果前后都可以重来，因为所有输入都来自冻结快照。
    """
    with SessionLocal() as db:
        run = db.get(DeviationRun, rid)
        if run is None or run.claim_token != token:
            return False
        if run.status == RunStatus.COMPLETED:
            return False
        attempt = run.attempt
        snapshot, alpha, seed = run.input_snapshot, run.alpha, run.seed
        if attempt > 1 and job_service.renew(db, rid, token):
            # 恢复执行的留痕；结果仍只来自创建时冻结的同一份输入。
            log(db, "worker", "deviation.resumed", rid)
            db.commit()
        try:
            result = analyze_deviation(snapshot, alpha, seed)
        except Exception as exc:
            traceback.print_exc()
            if job_service.fail(db, rid, token, str(exc)):
                log(db, "worker", "deviation.failed", rid)
                db.commit()
            else:
                db.rollback()
            return False
        # 只有当前领取能把 running 改成 completed；旧执行的令牌已失效，直接退出。
        if not job_service.complete(db, rid, token, result):
            db.rollback()
            return False
        log(db, "worker", "deviation.completed", rid)
        db.commit()
        # 提交成功后才写档案；若此处中断，恢复例程会用库内 result 补齐档案。
        try:
            write_artifact(rid, result)
        except OSError:
            traceback.print_exc()
        return True


def reconcile_artifact(run):
    """让已完成研究的结果档案与库内 result 一致；返回是否发生修复。"""
    if run.status != RunStatus.COMPLETED or not run.result:
        return False
    try:
        current = json.loads(artifact_path(run.id).read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        current = None
    if (
        current is not None
        and current.get("fit_hash") == run.result.get("fit_hash")
        and current.get("input_hash") == run.result.get("input_hash")
    ):
        return False
    write_artifact(run.id, run.result)
    return True
