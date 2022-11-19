from functools import partial
from itertools import islice
from multiprocessing import cpu_count

from .bases import ActorBase
from .core import Scheduler, SchedulerTask
from .distributed_apis import DEFAULT_MULTI_API
from .resource_handling import Capability, CapabilitySet

_RES = "CPU"
_CAP = Capability({_RES: 1})
_Task = partial(SchedulerTask, requirements=[_CAP])


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
    # TODO this will not work
    ActWrap.restart_after = max_runs

    return Scheduler(
        actor_dict={CapabilitySet([_CAP]): partial(ActWrap, fun=fun)},
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

    pinger = get_pinger(iterable) if pbar else lambda: None
    scheduler = get_simp_scheduler(nw, fun, dist_api, verbose, restart_after)

    out_iter = scheduler.process(
        batch_producer=BatchProd(iterable, batch_size),
        min_queue_size=min_queue_size,
    )
    try:
        for e in out_iter:
            if raise_errors and isinstance(e, Exception):
                raise e
            pinger()
            yield e
    finally:
        scheduler.join()


def get_pinger(iterable):
    from tqdm import tqdm

    try:
        total = len(iterable)
    except Exception:
        total = None

    return tqdm(total=total).update
