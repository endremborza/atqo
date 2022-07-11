from functools import partial
from itertools import islice
from multiprocessing import cpu_count

from .bases import ActorBase
from .core import Scheduler, SchedulerTask
from .distributed_apis import DEFAULT_MULTI_API
from .resource_handling import Capability, CapabilitySet
from .utils import partial_cls

_RES = "CPU"
_CAP = Capability({_RES: 1})
_Task = partial_cls(SchedulerTask, requirements=[_CAP])


class BatchProd:
    def __init__(self, iterable, batch_size, mapper=_Task) -> None:
        self._size = batch_size
        self._it = iter(iterable)
        self._mapper = mapper

    def __call__(self):
        return [*map(self._mapper, islice(self._it, self._size))]


class ActWrap(ActorBase):
    def __init__(self, fun) -> None:
        self._f = fun

    def consume(self, task_arg):
        return self._f(task_arg)


def get_simp_scheduler(n, fun, dist_sys, verbose, max_runs) -> Scheduler:
    ActWrap.restart_after = max_runs

    return Scheduler(
        actor_dict={CapabilitySet([_CAP]): partial_cls(ActWrap, fun=fun)},
        resource_limits={_RES: n},
        distributed_system=dist_sys,
        verbose=verbose,
    )


def parallel_map(
    fun,
    iterable,
    dist_api=DEFAULT_MULTI_API,
    batch_size=None,
    min_queue_size=None,
    workers=None,
    raise_errors=True,
    verbose=False,
    pbar=False,
    restart_after=float("inf"),
):
    nw = workers or cpu_count()
    batch_size = batch_size or nw * 5
    min_queue_size = min_queue_size or batch_size // 2

    if pbar:
        from tqdm import tqdm

        iterable = tqdm(iterable)

    scheduler = get_simp_scheduler(nw, fun, dist_api, verbose, restart_after)

    out = []
    outmap = partial(_raiser, li=out) if raise_errors else out.append

    scheduler.process(
        batch_producer=BatchProd(iterable, batch_size),
        result_processor=lambda li: [*map(outmap, li)],
        min_queue_size=min_queue_size,
    )
    scheduler.join()
    return out


def _raiser(e, li):
    if isinstance(e, Exception):
        raise e
    li.append(e)
