import ctypes
import multiprocessing as mp
import os
import queue
import signal
import sys
from asyncio import Future, wrap_future
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from multiprocessing.managers import SyncManager
from threading import Thread
from typing import TYPE_CHECKING, Any

from tblib import Traceback

from .bases import ActorBase, DistAPIBase
from .exceptions import DistantException
from .lock_stores import MpLockStore, ThreadLockStore

if TYPE_CHECKING:
    from .core import SchedulerTask  # pragma: no cover

# Poll interval for blocking queue reads, so a worker that died without
# answering is noticed promptly rather than blocking a get() forever; and the
# grace a worker gets to exit on poison before it is force-killed.
_RESULT_POLL = 0.2
_WORKER_STOP_GRACE = 5.0


class SyncAPI(DistAPIBase):
    pass


class MultiProcAPI(DistAPIBase):
    def __init__(self) -> None:
        mp.set_start_method("spawn", force=True)
        self.man = mp.Manager()
        self._lock_queue = self.man.Queue(2)
        self._lock_queue.put(self.man.Lock())
        self._filler = Thread(target=self._fill_lock_q, name="filler", daemon=True)
        self._filler.start()
        self._store = MpLockStore(self.man.Lock(), self.man.dict(), self._lock_queue)

    def get_running_actor(self, actor_creator) -> "ActorBase":
        return MPActorWrap(actor_creator, self.man, self._store)

    @staticmethod
    def get_future(actor: ActorBase, next_task: "SchedulerTask") -> Future:
        return wrap_future(actor.consume(next_task.argument))

    def join(self):
        self.man.shutdown()
        # the filler unblocks (its manager calls raise) once the manager is
        # down; joining it keeps a stray thread from lingering past shutdown.
        self._filler.join(timeout=1)

    def _fill_lock_q(self):
        try:
            while True:
                _lock = self.man.Lock()
                self._lock_queue.put(_lock)
        except (EOFError, ConnectionResetError, BrokenPipeError, FileNotFoundError):
            pass


class MPActorWrap(ActorBase):
    def __init__(self, actor_creator, man: SyncManager, store):
        self._inner_actor = actor_creator
        self._in_q = man.Queue(maxsize=1)
        self._out_q = man.Queue(maxsize=1)
        self.pool = ProcessPoolExecutor(1)
        _setup_q = mp.Queue()
        _args = (actor_creator, self._in_q, self._out_q, _setup_q, store)
        self.proc = mp.Process(target=_work_mp_actor, args=_args)
        self.proc.start()
        # bounded so an actor that dies while constructing (e.g. an unpicklable
        # arg, an import blowing up) surfaces instead of hanging this get().
        res = _recv_or_worker_died(_setup_q, self.proc.pid)
        if isinstance(res, Exception):
            raise res

    def consume(self, task_arg):
        return self.pool.submit(
            _add_task_mp, task_arg, self._in_q, self._out_q, self.proc.pid
        )

    def stop(self):
        try:
            self.pool.submit(_poison_queue, self._in_q)
        except RuntimeError:  # pool already shut down
            pass
        self.proc.join(timeout=_WORKER_STOP_GRACE)
        if self.proc.is_alive():
            self.proc.kill()
            self.proc.join(timeout=_WORKER_STOP_GRACE)
        # safe now: a result wait blocked on a dead worker unblocks via the
        # liveness check in _add_task_mp, so wait=True can't hang here.
        self.pool.shutdown(wait=True, cancel_futures=True)


@dataclass
class DistArg:
    arg: Any
    poison: bool = False


def _work_mp_actor(actor_cls, in_q, out_q, setup_q, store):  # pragma: no cover
    _set_parent_death_signal()
    global _GLOBAL_LOCK_STORE
    _GLOBAL_LOCK_STORE = store
    try:
        actor: ActorBase = actor_cls()
    except Exception as e:
        setup_q.put(e)
        return
    setup_q.put(0)
    while True:
        arg: DistArg = in_q.get()
        if arg.poison:
            actor.stop()
            return
        try:
            res = actor.consume(arg.arg)
        except Exception as e:
            res = DistantException(e, Traceback(e.__traceback__))
        out_q.put(res)


def _add_task_mp(task_arg, in_q: mp.Queue, out_q: mp.Queue, worker_pid: int):
    in_q.put(DistArg(task_arg))
    return _recv_or_worker_died(out_q, worker_pid)


def _recv_or_worker_died(q, worker_pid: int):
    """Block on ``q`` but give up if the worker process is gone, so a crashed
    worker (segfault, OOM, os._exit) surfaces as an error instead of an
    indefinite hang on a result that will never arrive."""
    while True:
        try:
            return q.get(timeout=_RESULT_POLL)
        except queue.Empty:
            if not _pid_alive(worker_pid):
                raise RuntimeError(
                    f"worker process {worker_pid} died without responding"
                ) from None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # exists but not ours; treat as alive
        return True
    # The pid is in the table, but a worker that exited and hasn't been reaped
    # yet lingers as a zombie until stop() joins it — and stop() runs after the
    # result wait. os.kill(pid, 0) can't tell a zombie from a live process, so
    # check /proc; a zombie will never answer, so it's "dead" for our purposes.
    if sys.platform != "linux":
        return True
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            state = f.read().rpartition(b")")[2].split()[0]
    except (FileNotFoundError, ProcessLookupError, OSError):
        return False
    return state != b"Z"


def _set_parent_death_signal():  # pragma: no cover
    """Linux: have the kernel SIGKILL this worker if the parent dies, so an
    abruptly-killed scheduler (SIGKILL, crash) can't leak blocked workers that
    sit forever holding resources/ports."""
    if sys.platform != "linux":
        return
    try:
        PR_SET_PDEATHSIG = 1
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(
            PR_SET_PDEATHSIG, signal.SIGKILL
        )
        if os.getppid() == 1:  # parent already died before prctl ran
            os.kill(os.getpid(), signal.SIGKILL)
    except Exception:
        pass


def _poison_queue(in_q: mp.Queue):  # pragma: no cover
    in_q.put(DistArg(None, True))


try:
    _GLOBAL_LOCK_STORE
except NameError:
    _GLOBAL_LOCK_STORE = ThreadLockStore()


def get_lock(lock_id):
    return _GLOBAL_LOCK_STORE.get(lock_id)


def acquire_lock(lock_id):
    return _GLOBAL_LOCK_STORE.acquire(lock_id)
