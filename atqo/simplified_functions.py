from functools import partial
from itertools import islice
from multiprocessing import cpu_count

from .bases import ActorBase, SingleCPUActor
from .core import Scheduler, SchedulerTask
from .distributed_apis import MultiProcAPI


class _ActWrap(SingleCPUActor):
    def __init__(self, fun) -> None:
        self._f = fun

    def consume(self, task_arg):
        return self._f(task_arg)


class BatchProd:
    def __init__(self, iterable, batch_size, mapper=None) -> None:
        self._size = batch_size
        self._it = iter(iterable)
        self._mapper = mapper or (lambda x: SchedulerTask(x))

    def __call__(self):
        return [self._mapper(x) for x in islice(self._it, self._size)]


def get_simp_scheduler(n, Actor, dist_sys, verbose) -> Scheduler:
    return Scheduler(
        actors=[Actor],
        resources={"cpu": n},
        distributed_system=dist_sys,
        verbose=verbose,
    )


def parallel_consume(
    Actor: type[ActorBase],
    iterable,
    dist_api=MultiProcAPI,
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

    mapper = partial(SchedulerTask, allowed_fail_count=allowed_fail_count)
    out_iter = scheduler.process(
        batch_producer=BatchProd(iterable, batch_size, mapper),
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
    dist_api=MultiProcAPI,
    batch_size=None,
    min_queue_size=None,
    workers=None,
    raise_errors=True,
    verbose=False,
    pbar=False,
    allowed_fail_count=0,
):
    return parallel_consume(
        partial(_ActWrap, fun=fun),
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
    if not pbar:
        return lambda: None

    from tqdm import tqdm

    try:
        total = len(iterable)
    except Exception:
        total = None
    return tqdm(total=total, desc=pbar if isinstance(pbar, str) else "parallel").update
