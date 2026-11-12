from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .settings import DATABASE_URL
class Base(DeclarativeBase):
    pass
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

# create_all 只建新表、不改旧表；补上领取/租约列，既有部署也能恢复中断研究。
_UPGRADE_COLUMNS = {
    "claim_token": "VARCHAR NOT NULL DEFAULT ''",
    "claim_owner": "VARCHAR NOT NULL DEFAULT ''",
    "attempt": "INTEGER NOT NULL DEFAULT 0",
    "lease_expires_at": "VARCHAR NOT NULL DEFAULT ''",
    "started_at": "VARCHAR NOT NULL DEFAULT ''",
    "finished_at": "VARCHAR NOT NULL DEFAULT ''",
}

def ensure_schema():
    Base.metadata.create_all(engine)
    inspector = inspect(engine)
    if "deviation_runs" not in inspector.get_table_names():
        return
    existing = {column["name"] for column in inspector.get_columns("deviation_runs")}
    if all(name in existing for name in _UPGRADE_COLUMNS):
        return
    with engine.begin() as connection:
        for name, ddl in _UPGRADE_COLUMNS.items():
            if name not in existing:
                try:
                    connection.execute(text(f"ALTER TABLE deviation_runs ADD COLUMN {name} {ddl}"))
                except SQLAlchemyError:
                    # backend 与 worker 同时启动时另一进程可能已补列；确认后继续。
                    inspector = inspect(engine)
                    if name not in {c["name"] for c in inspector.get_columns("deviation_runs")}:
                        raise
def get_db():
    with SessionLocal() as db:
        yield db
