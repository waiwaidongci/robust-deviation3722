"""最小 Redis 替身，只在测试里使用（真实部署仍用 redis:7）。"""
import threading
from collections import deque

class FakeRedis:
    def __init__(self):
        self.lists = {}
        self.lock = threading.Lock()

    def rpush(self, key, value):
        with self.lock:
            self.lists.setdefault(key, deque()).appendleft(value)
            return len(self.lists[key])

    def lpush(self, key, value):
        with self.lock:
            self.lists.setdefault(key, deque()).append(value)
            return len(self.lists[key])

    def blpop(self, key, timeout=0):
        with self.lock:
            q = self.lists.get(key)
            if q:
                return (key, q.pop())
        return None

    def ping(self):
        return True
