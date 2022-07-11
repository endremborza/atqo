import asyncio
import multiprocessing as mp
from asyncio import Future
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Type

from structlog import get_logger

from .bases import ActorBase, DistAPIBase

if TYPE_CHECKING:
    from .core import SchedulerTask  # pragma: no cover

logger = get_logger()


class SyncAPI(DistAPIBase):
    pass


class MultiProcAPI(DistAPIBase):
    def __init__(self) -> None:
        self.man = mp.Manager()

    def get_running_actor(self, actor_cls: Type["ActorBase"]) -> "ActorBase":
        return MPActorWrap(actor_cls, self.man)

    @staticmethod
    def get_future(actor: ActorBase, next_task: "SchedulerTask") -> Future:
        return asyncio.wrap_future(actor.consume(next_task.argument))

    def join(self):
        self.man.shutdown()


class MPActorWrap(ActorBase):
    def __init__(self, inner_actor_cls: Type["ActorBase"], man: mp.Manager):

        self._inner_actor = inner_actor_cls
        self._in_q = man.Queue(maxsize=1)
        self._out_q = man.Queue(maxsize=1)
        self.pool = ProcessPoolExecutor(1)
        self.proc = mp.Process(
            target=_work_mp_actor,
            args=(inner_actor_cls, self._in_q, self._out_q),
        )
        self.proc.start()

    def consume(self, task_arg):
        self.pool.submit(self._in_q.put, task_arg)
        return self.pool.submit(self._out_q.get)

    def stop(self):
        self.proc.kill()
        self.proc.join()
        self.pool.shutdown()

    @property
    def restart_after(self):
        return self._inner_actor.restart_after


def _work_mp_actor(actor_cls, in_q, out_q):  # pragma: no cover
    actor = actor_cls()
    while True:
        arg = in_q.get()
        try:
            res = actor.consume(arg)
        except Exception as e:
            res = e
        out_q.put(res)


DEFAULT_DIST_API_KEY = "sync"
DEFAULT_MULTI_API = "mp"
DIST_API_MAP = {DEFAULT_DIST_API_KEY: SyncAPI, DEFAULT_MULTI_API: MultiProcAPI}


def get_dist_api(key) -> "DistAPIBase":
    try:
        return DIST_API_MAP[key]
    except KeyError:
        default = DIST_API_MAP[DEFAULT_DIST_API_KEY]
        err = f"unknown distributed system: {key}, defaulting {default}"
        logger.warning(err)
        return default
