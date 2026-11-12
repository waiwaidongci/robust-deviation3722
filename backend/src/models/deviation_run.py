from sqlalchemy import Text,JSON,ForeignKey,Double,UniqueConstraint,Index
from sqlalchemy.orm import Mapped,mapped_column
from ..config.database import Base
from ..utils.clock import now
class DeviationRun(Base):
    __tablename__="deviation_runs"
    __table_args__=(Index("run_status_lease_idx","status","lease_expires_at"),)
    id:Mapped[int]=mapped_column(primary_key=True)
    name:Mapped[str]
    study_id:Mapped[int]=mapped_column(ForeignKey("archive_studies.id"))
    alpha:Mapped[float]=mapped_column(Double)
    seed:Mapped[int]
    input_snapshot:Mapped[dict]=mapped_column(JSON)
    status:Mapped[str]=mapped_column(default="queued")
    result:Mapped[dict]=mapped_column(JSON,default=dict)
    error:Mapped[str]=mapped_column(Text,default="")
    owner:Mapped[str]
    created_at:Mapped[str]=mapped_column(default=now)
    # 领取令牌与租约：只有持有当前令牌的执行进程可以写入结果。
    claim_token:Mapped[str]=mapped_column(default="",server_default="")
    claim_owner:Mapped[str]=mapped_column(default="",server_default="")
    attempt:Mapped[int]=mapped_column(default=0,server_default="0")
    lease_expires_at:Mapped[str]=mapped_column(default="",server_default="")
    started_at:Mapped[str]=mapped_column(default="",server_default="")
    finished_at:Mapped[str]=mapped_column(default="",server_default="")
