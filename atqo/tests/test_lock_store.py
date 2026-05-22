from queue import Queue

from atqo import acquire_lock, get_lock
from atqo.lock_stores import MpLockStore, ThreadLockStore


def test_thread_lock_store():
    store = ThreadLockStore()

    lock = store.get("lock")
    lock.acquire()
    lock.release()

    l2 = store.acquire("other")
    l2.release()

    l3 = acquire_lock("l3")
    l4 = get_lock("l4")
    assert l3.locked()
    l3.release()
    assert not l4.locked()


def test_mp_lock_store_delegates_to_queue():
    base = ThreadLockStore()
    main_lock = base.get("main")
    q = Queue()

    mp_store = MpLockStore(main_lock, {}, q)
    q.put(base.get("other"))

    key_lock = mp_store.get("this")
    key_lock.acquire()
    key_lock.release()
