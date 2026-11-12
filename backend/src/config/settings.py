import os
DATABASE_URL=os.environ.get("DATABASE_URL","postgresql+psycopg://deviation:deviation@db/deviation")
REDIS_URL=os.environ.get("REDIS_URL","redis://redis:6379/0")
JWT_SECRET=os.environ.get("JWT_SECRET","local-robust-deviation-change-this-key")
SEED_DEMO=os.environ.get("SEED_DEMO","true").lower()=="true"
WORKER_ID=os.environ.get("WORKER_ID","")
WORKER_LEASE_SECONDS=max(1,int(os.environ.get("WORKER_LEASE_SECONDS","90")))
WORKER_HEARTBEAT_SECONDS=max(1,int(os.environ.get("WORKER_HEARTBEAT_SECONDS","20")))
WORKER_RECOVERY_SECONDS=max(1,int(os.environ.get("WORKER_RECOVERY_SECONDS","10")))
