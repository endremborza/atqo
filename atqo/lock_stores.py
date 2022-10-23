from abc import abstractmethod
from collections import defaultdict
from pathlib import Path
from queue import Queue
from tempfile import TemporaryDirectory
from threading import Lock

SEPARATOR = "://"


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
            out = self._lock_queue.get()
            self._locks[key] = out
        finally:
            self._main_lock.release()
        return out


class FileLockStore(LockStoreBase):
    def __init__(self, root: str = None) -> None:

        from portalocker import Lock

        self._lock_cls = Lock
        self.root = root or TemporaryDirectory().name

    def get(self, key) -> Lock:
        return self._lock_cls(self._get_path(key))

    def _get_path(self, key):
        try:
            subpath = Path(key).relative_to(Path(self.root).root)
        except ValueError:
            subpath = Path(key)
        path = Path(self.root, subpath).with_suffix(".lock")
        path.parent.mkdir(exist_ok=True, parents=True)
        return path
