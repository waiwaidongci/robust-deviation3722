"""测试用文件队列与 worker 启动壳，供跨进程中断验证。"""
import fcntl
import os
import time


class FileQueue:
    """基于文件的阻塞列表，模拟 Redis BLPOP（仅测试）。"""

    def __init__(self, path):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        open(path, "a").close()

    def rpush(self, key, value):
        with open(self.path, "a") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(f"{value}\n")
            fcntl.flock(f, fcntl.LOCK_UN)
        return 1

    def blpop(self, key, timeout=0):
        deadline = None if not timeout else time.time() + timeout
        while True:
            with open(self.path, "r+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                lines = f.read().splitlines()
                if lines:
                    value = lines[0]
                    rest = lines[1:]
                    f.seek(0)
                    f.truncate()
                    f.write("\n".join(rest) + ("\n" if rest else ""))
                    fcntl.flock(f, fcntl.LOCK_UN)
                    return (key, value)
                fcntl.flock(f, fcntl.LOCK_UN)
            if deadline and time.time() > deadline:
                return None
            time.sleep(0.05)

    def ping(self):
        return True

    def drain(self):
        while self.blpop("x", timeout=0.01):
            pass


def install():
    """把进程内所有队列引用替换为文件队列；可选让分析变慢便于“计算中被杀”。"""
    from src.services import run_service, execution_service
    q = FileQueue(os.environ["TEST_QUEUE_FILE"])
    run_service.queue = q
    import src.worker as worker
    worker.queue = q
    slow = float(os.environ.get("TEST_SLOW_ANALYSIS", "0"))
    if slow:
        real = execution_service.analyze_deviation
        def slow_analyze(snapshot, alpha, seed):
            time.sleep(slow)
            return real(snapshot, alpha, seed)
        execution_service.analyze_deviation = slow_analyze
    return q


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.environ["TEST_BACKEND"])
    install()
    import src.worker as worker
    worker.main()
