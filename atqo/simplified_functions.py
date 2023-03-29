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


def get_simp_scheduler(n, Actor, dist_sys, verbose) -> Scheduler:
    return Scheduler(
        actor_dict={CapabilitySet([_CAP]): Actor},
        resource_limits={_RES: n},
        distributed_system=dist_sys,
        verbose=verbose,
    )


def parallel_consume(
    Actor: type[ActorBase],
    iterable,
    dist_api=DEFAULT_MULTI_API,
    batch_size=None,
    min_queue_size=None,
    workers=None,
    raise_errors=True,
    verbose=False,
    pbar=False,
    allowed_fail_count=0,
):
    nw = workers or cpu_count()
    batch_size = batch_size or nw * 5
    min_queue_size = min_queue_size or batch_size // 2

    pinger = get_pinger(iterable, pbar)
    scheduler = get_simp_scheduler(nw, Actor, dist_api, verbose)

    out_iter = scheduler.process(
        batch_producer=BatchProd(
            iterable, batch_size, partial(_Task, allowed_fail_count=allowed_fail_count)
        ),
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
    allowed_fail_count=0,
):
    return parallel_consume(
        partial(ActWrap, fun=fun),
        iterable,
        dist_api,
        batch_size,
        min_queue_size,
        workers,
        raise_errors,
        verbose,
        pbar,
        allowed_fail_count,
    )


def get_pinger(iterable, pbar):
    if pbar is None:
        return lambda: None

    from tqdm import tqdm

    try:
        total = len(iterable)
    except Exception:
        total = None
    return tqdm(total=total, desc=pbar if isinstance(pbar, str) else "parallel").update
