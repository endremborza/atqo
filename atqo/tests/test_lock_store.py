from queue import Queue

import pytest

from atqo.lock_stores import FileLockStore, LockStoreBase, MpLockStore, ThreadLockStore


@pytest.mark.parametrize("lock_store_cls", [ThreadLockStore, FileLockStore])
def test_lock_store(lock_store_cls):

    lock_store: LockStoreBase = lock_store_cls()

    lock = lock_store.get("lock")

    lock.acquire()
    lock.release()

    l2 = lock_store.acquire("other")
    l2.release()


def test_multi_lock_store():

    base = ThreadLockStore()

    main_lock = base.get("main")
    q = Queue()

    mp_store = MpLockStore(main_lock, {}, q)

    q.put(base.get("other"))

    other_key = mp_store.get("this")

    other_key.acquire()
    other_key.release()
