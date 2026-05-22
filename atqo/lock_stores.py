from abc import abstractmethod
from collections import defaultdict
from queue import Queue
from threading import Lock


class LockStoreBase:
    def acquire(self, key) -> Lock:
        lock = self.get(key)
        lock.acquire()
        return lock

    @abstractmethod
    def get(self, key: str) -> Lock:
        pass  # pragma: no cover


class ThreadLockStore(LockStoreBase):
    def __init__(self) -> None:
        self._locks = defaultdict(Lock)

    def get(self, key: str) -> Lock:
        return self._locks[key]


class MpLockStore(ThreadLockStore):
    def __init__(self, main_lock: Lock, lock_dict: dict, lock_queue: Queue) -> None:
        self._main_lock = main_lock
        self._locks = lock_dict
        self._lock_queue = lock_queue

    def get(self, key: str) -> Lock:
        self._main_lock.acquire()
        try:
            out = self._locks[key]
        except KeyError:
            for _ in range(5):
                try:
                    out = self._lock_queue.get()
                    break
                except TypeError:  # pragma: no cover
                    pass
            else:
                raise OSError("lock queue exhausted")  # pragma: no cover
            self._locks[key] = out
        finally:
            self._main_lock.release()
        return out
