"""SQLite 与文件队列上的恢复测试（不依赖容器）。"""
import os
import sys
import tempfile

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
sys.path.insert(0, os.path.join(BACKEND, "tests"))
TMP = tempfile.mkdtemp(prefix="recovery-")
os.environ["DATABASE_URL"] = f"sqlite:///{TMP}/test.db"
os.environ["ARTIFACT_DIR"] = f"{TMP}/artifacts"
os.environ["TEST_QUEUE_FILE"] = f"{TMP}/queue.txt"
os.environ["WORKER_LEASE_SECONDS"] = "2"
os.environ["WORKER_HEARTBEAT_SECONDS"] = "1"
os.environ["WORKER_RECOVERY_SECONDS"] = "1"
