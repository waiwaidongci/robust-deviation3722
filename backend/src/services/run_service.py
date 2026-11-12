import csv,io
from redis import Redis
from fastapi import HTTPException
from ..models import DeviationRun,ReviewNote
from ..config.settings import REDIS_URL
from ..utils.serialize import record
from .study_service import get_study,create_snapshot
from .analysis_service import prepare_snapshot
from .audit_service import log
from .job_service import JOB_KEY
queue=Redis.from_url(REDIS_URL,decode_responses=True)
def public(row):
    """对外记录：保留恢复/完成时间线，但不暴露内部领取令牌。"""
    data=record(row);data.pop("claim_token",None);return data
def list_runs(db):return [public(r) for r in db.query(DeviationRun).order_by(DeviationRun.id.desc())]
def get_run(db,rid):
    row=db.get(DeviationRun,rid)
    if not row:raise HTTPException(404,"偏离研究不存在")
    return row
def create_run(db,data,user):
    study=get_study(db,data.study_id);snapshot=create_snapshot(db,study)
    try:prepare_snapshot(snapshot)
    except ValueError as e:raise HTTPException(422,str(e))
    row=DeviationRun(**data.model_dump(),input_snapshot=snapshot,owner=user.name);db.add(row);db.flush();log(db,user.email,"deviation.enqueue",row.id);db.commit()
    try:queue.rpush(JOB_KEY,row.id)
    except Exception as e:
        # 通知可丢，但研究不能挂死：状态保持 queued，worker 启动/周期恢复会补投并执行。
        row.error=f"排队通知暂未送达，worker 会自动恢复（{e.__class__.__name__}）";log(db,user.email,"deviation.enqueue.deferred",row.id);db.commit()
    return public(row)
def export_run(db,rid):
    row=get_run(db,rid)
    if row.status!='completed' or not db.query(ReviewNote).filter_by(run_id=rid,status='approved').first():raise HTTPException(403,'仅获批完成研究可导出')
    o=io.StringIO();w=csv.writer(o);features=row.result['features'];w.writerow(['window','split',*[f['code'] for f in features],'D_squared','cal_ge_count','upper_tail_p','preset_alpha','selected','cal_m','seed','fit_hash','input_hash'])
    for x in row.result['test']:v=x['window'];w.writerow([v['code'],v['split'],*[v['values'][f['code']] for f in features],x['d2'],x['cal_ge_count'],x['p'],row.alpha,x['selected'],row.result['counts']['cal'],row.seed,row.result['fit_hash'],row.result['input_hash']])
    return o.getvalue()
