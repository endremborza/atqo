import asyncio
import concurrent.futures as cf
import logging
import time
import uuid
from dataclasses import dataclass, field
from functools import partial
from queue import Empty, Queue
from threading import Thread
from typing import Any, Callable, Iterable, Optional, Union

from .bases import ActorBase, DistAPIBase
from .distributed_apis import SyncAPI
from .exceptions import (
    ActorListenBreaker,
    ActorPoisoned,
    DistantException,
    NotEnoughResources,
    NotEnoughResourcesToContinue,
    SchedulerStalled,
    UnknownActor,
    UnknownResource,
)
from .exchange import ResourceExchange
from .rate import RateGate, RateLimit
from .utils import dic_val_filt

POISON_PILL = None
DEFAULT_POLL_INTERVAL = 0.05
DEFAULT_POISON_TIMEOUT = 5.0
ACTOR_RESIZE_TIMEOUT_MULTIPLIER = 6


def _start_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _get_loop_of_daemon():
    loop = asyncio.new_event_loop()
    thread = Thread(target=_start_loop, args=(loop,), daemon=True)
    thread.start()
    return loop, thread


def _default_resources() -> dict[str, int]:
    from multiprocessing import cpu_count

    return {"cpu": cpu_count()}


def _underlying_class(actor) -> type[ActorBase]:
    cls = actor.func if isinstance(actor, partial) else actor
    if not isinstance(cls, type) or not issubclass(cls, ActorBase):
        raise TypeError(f"{actor!r} is not an ActorBase subclass or partial of one")
    return cls


class Scheduler:
    """Resource-aware async task scheduler.

    Hang protection:
      - All blocking waits use ``poll_interval`` and re-check exit conditions.
      - If ``stall_timeout`` is set, no progress for that long raises
        ``SchedulerStalled``.
      - If ``task_timeout`` is set, individual tasks taking longer fail their
        attempt (and may exhaust ``allowed_fail_count``).
      - ``cleanup()`` and ``join()`` always cancel listeners and stop the loop;
        they will never wait indefinitely.
    """

    def __init__(
        self,
        actors: Optional[Iterable[Union[type[ActorBase], partial]]] = None,
        resources: Optional[dict[str, int]] = None,
        rate_limits: Optional[dict[str, RateLimit]] = None,
        distributed_system: type[DistAPIBase] = SyncAPI,
        task_timeout: Optional[float] = None,
        stall_timeout: Optional[float] = None,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poison_timeout: float = DEFAULT_POISON_TIMEOUT,
        verbose: bool = False,
    ) -> None:
        actors = list(actors or [])
        resources = dict(resources) if resources is not None else _default_resources()
        rate_limits = dict(rate_limits) if rate_limits else {}

        partials: dict[type[ActorBase], partial] = {}
        for entry in actors:
            cls = _underlying_class(entry)
            if cls in partials:
                raise ValueError(
                    f"actor class {cls.__name__} registered more than once"
                )
            partials[cls] = entry if isinstance(entry, partial) else partial(entry)

        for cls in partials:
            for res, n in cls.requirements.items():
                if res not in resources:
                    raise UnknownResource(
                        f"{cls.__name__} requires {res!r}, not in scheduler "
                        f"resources {list(resources)}"
                    )
                if not isinstance(n, int) or n <= 0:
                    raise ValueError(
                        f"{cls.__name__}.requirements[{res!r}] must be a positive "
                        f"int, got {n!r}"
                    )
                if n > resources[res]:
                    raise NotEnoughResources(
                        f"{cls.__name__} requires {res}={n} but resource pool "
                        f"has {res}={resources[res]}"
                    )

        self._actor_classes = list(partials.keys())
        self._actor_partials = partials
        self._resources = resources
        self._rate_gate = RateGate(rate_limits)
        self._task_timeout = task_timeout
        self._stall_timeout = stall_timeout
        self._poll_interval = poll_interval
        self._poison_timeout = poison_timeout
        self._put_timeout = stall_timeout
        self._verbose = verbose

        self._loop, self._thread = _get_loop_of_daemon()
        self._result_queue: Queue[TaskResult] = Queue()
        self._task_queues: dict[type[ActorBase], TaskQueue] = {
            cls: TaskQueue(self._loop) for cls in self._actor_classes
        }
        self._dist_api = distributed_system()
        self._actor_sets: dict[type[ActorBase], ActorSet] = {
            cls: ActorSet(
                partials[cls],
                self._dist_api,
                cls,
                self._task_queues[cls],
                self._result_queue,
                self._loop,
                self._rate_gate,
                self._task_timeout,
                self._poison_timeout,
                self._verbose,
            )
            for cls in self._actor_classes
        }
        self._exchange = ResourceExchange(self._actor_classes, resources)
        self._closed = False
        self._log("scheduler ready", actors=list(partials), resources=resources)

    def __del__(self):
        try:
            self.cleanup()
        except Exception:
            pass

    def __enter__(self) -> "Scheduler":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.cleanup()
        return False

    def process(
        self,
        batch_producer: Callable[[], list["SchedulerTask"]],
        min_queue_size: int = 0,
    ):
        while True:
            next_batch = batch_producer()
            batch_size = len(next_batch)
            empty_batch = batch_size == 0
            self._log(
                "empty batch" if empty_batch else "new batch",
                size=batch_size,
                was_done=self.is_empty,
            )
            if self.is_empty and empty_batch:
                self._reorganize_actors()
                break
            self.refill_task_queue(next_batch)
            target = 0 if empty_batch else min_queue_size
            try:
                yield from self.iter_until_n_tasks_remain(target)
            except KeyboardInterrupt:  # pragma: no cover
                self._log("Interrupted")
                break

    def refill_task_queue(self, task_batch: Iterable["SchedulerTask"]):
        tasks = list(task_batch)
        for task in tasks:
            self._validate_task(task)
        if not tasks:
            return
        delta: dict = {}
        for task in tasks:
            delta[task.actor] = delta.get(task.actor, 0) + 1
        need = {
            cls: tq.size + delta.get(cls, 0) for cls, tq in self._task_queues.items()
        }
        new_ideals = self._exchange.set_values(need)
        if (
            any(v > 0 for v in need.values()) and self._exchange.idle
        ):  # pragma: no cover
            self.cleanup()
            raise NotEnoughResourcesToContinue(
                f"no actor type can fulfill demand "
                f"{ {c.__name__: n for c, n in need.items() if n} }"
            )
        self._set_actor_sets(new_ideals)
        for task in tasks:
            self._task_queues[task.actor].put(task, timeout=self._put_timeout)

    def iter_until_n_tasks_remain(self, remaining_tasks: int = 0):
        if self._closed:
            return
        last_progress = time.monotonic()
        last_seen = (self._in_progress_or_queued, self.queued_task_count)
        while True:
            if (
                self._in_progress_or_queued <= remaining_tasks
                and self._result_queue.empty()
            ):
                return
            try:
                tr = self._result_queue.get(timeout=self._poll_interval)
            except Empty:
                current = (self._in_progress_or_queued, self.queued_task_count)
                if current != last_seen:
                    last_seen = current
                    last_progress = time.monotonic()
                elif self._stall_timeout is not None:
                    if time.monotonic() - last_progress > self._stall_timeout:
                        raise SchedulerStalled(
                            f"no progress for {self._stall_timeout}s; "
                            f"in_progress_or_queued={current[0]}, "
                            f"queued={current[1]}"
                        )
                continue
            last_progress = time.monotonic()
            last_seen = (self._in_progress_or_queued, self.queued_task_count)
            if (tr.source_queue.size == 0) and (self.queued_task_count > 0):
                self._reorganize_actors()
            yield tr.value

    def join(self):
        if self._closed:
            return []
        try:
            out = list(self.iter_until_n_tasks_remain(0))
        except Exception:
            self.cleanup()
            raise
        self.cleanup()
        return out

    def cleanup(self):
        if self._closed:
            return
        self._closed = True

        async def _shutdown():
            tasks: list[asyncio.Task] = []
            for aset in self._actor_sets.values():
                tasks.extend(aset._actor_listening_async_task_dict.values())
                tasks.append(aset.poison_queue.getting_task)
            for q in self._task_queues.values():
                tasks.append(q.getting_task)
            for t in tasks:
                if not t.done():
                    t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            for aset in self._actor_sets.values():
                aset._actor_listening_async_task_dict.clear()

        if self._loop.is_running():
            try:
                fut = asyncio.run_coroutine_threadsafe(_shutdown(), self._loop)
                fut.result(timeout=2.0)
            except (cf.TimeoutError, RuntimeError):
                pass

        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except RuntimeError:  # pragma: no cover
            pass
        try:
            self._dist_api.join()
        except Exception:  # pragma: no cover
            pass

    @property
    def is_empty(self) -> bool:
        return self.is_idle and self._result_queue.empty()

    @property
    def is_idle(self) -> bool:
        return not self._in_progress_or_queued

    @property
    def queued_task_count(self) -> int:
        return sum(tq.size for tq in self._task_queues.values())

    def _validate_task(self, task: "SchedulerTask"):
        if task.actor is None:
            if len(self._actor_classes) != 1:
                raise UnknownActor(
                    "task.actor=None requires exactly one registered actor "
                    f"class; have {[c.__name__ for c in self._actor_classes]}"
                )
            task.actor = self._actor_classes[0]
        else:
            task.actor = _underlying_class(task.actor)
            if task.actor not in self._task_queues:
                raise UnknownActor(
                    f"task.actor={task.actor!r} not registered; "
                    f"have {[c.__name__ for c in self._actor_classes]}"
                )
        if task.rate_costs:
            self._rate_gate.validate_cost(task.rate_costs)

    def _log(self, msg, **kwargs):
        if self._verbose:
            ctx = {
                "api": type(self._dist_api).__name__,
                "queued": self.queued_task_count,
                "working": self._running_consumer_count,
                **kwargs,
            }
            logging.getLogger("atqo.scheduler").info(f"{msg} {ctx}")

    def _reorganize_actors(self):
        need_dic = {cls: tq.size for cls, tq in self._task_queues.items()}
        curr = {cls: aset.running_actor_count for cls, aset in self._actor_sets.items()}
        new_ideals = self._exchange.set_values(need_dic)
        for tag, dic in [("need", need_dic), ("from", curr), ("to", new_ideals)]:
            self._log(f"reorganizing {tag} {dic_val_filt(dic)}")
        self._set_actor_sets(new_ideals)
        if self.queued_task_count and self._exchange.idle:  # pragma: no cover
            self.cleanup()
            raise NotEnoughResourcesToContinue(
                f"{self.queued_task_count} tasks remaining and no launchable actors"
            )

    def _set_actor_sets(self, ideals: dict):
        runs = [
            run
            for cls, n in ideals.items()
            for run in self._actor_sets[cls].set_running_actors_to(n)
        ]
        if not runs:
            return

        async def _run_all():
            return await asyncio.gather(*runs, return_exceptions=True)

        fut = asyncio.run_coroutine_threadsafe(_run_all(), loop=self._loop)
        try:
            results = fut.result(
                timeout=self._poison_timeout * ACTOR_RESIZE_TIMEOUT_MULTIPLIER
            )
        except Exception:  # pragma: no cover
            fut.cancel()
            raise
        for r in results:
            if isinstance(r, BaseException):
                raise r

    @property
    def _running_consumer_count(self):
        return sum(aset.running_actor_count for aset in self._actor_sets.values())

    @property
    def _in_progress_or_queued(self):
        return self.queued_task_count + sum(
            aset.in_prog for aset in self._actor_sets.values()
        )


class TaskQueue:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.queue: asyncio.Queue = asyncio.Queue()
        self.getting_task = loop.create_task(self.queue.get())

    def pop(self):
        out = self.getting_task.result()
        self.getting_task = self.loop.create_task(self.queue.get())
        return out

    def put(self, item, timeout: Optional[float] = None):
        fut = asyncio.run_coroutine_threadsafe(self.queue.put(item), self.loop)
        try:
            fut.result(timeout=timeout)
        except cf.TimeoutError as e:
            fut.cancel()
            raise SchedulerStalled(
                "event loop unresponsive: failed to enqueue task within "
                f"{timeout}s (an actor is likely blocking the loop)"
            ) from e

    def done(self):
        return self.getting_task.done()

    @property
    def size(self):
        return self.queue.qsize() + int(self.getting_task.done())


class ActorSet:
    def __init__(
        self,
        actor_partial: partial,
        dist_api: DistAPIBase,
        actor_cls: type[ActorBase],
        task_queue: TaskQueue,
        result_queue: Queue,
        loop: asyncio.AbstractEventLoop,
        rate_gate: RateGate,
        task_timeout: Optional[float],
        poison_timeout: float,
        debug: bool,
    ) -> None:
        self.actor_partial = actor_partial
        self.dist_api = dist_api
        self.actor_cls = actor_cls
        self._task_queue = task_queue
        self._result_queue = result_queue
        self._loop = loop
        self._rate_gate = rate_gate
        self._task_timeout = task_timeout
        self._poison_timeout = poison_timeout
        self._debug = debug

        self.poison_queue = TaskQueue(loop)
        self._poisoning_done_future = loop.create_future()
        self._actor_listening_async_task_dict: dict[str, asyncio.Task] = {}
        self.in_prog = 0

    def __repr__(self):
        return (
            f"ActorSet({self.actor_cls.__name__}, "
            f"running={self.running_actor_count}, in_prog={self.in_prog})"
        )

    def set_running_actors_to(self, target_count):
        if target_count < self.running_actor_count:
            yield self.drain_to(target_count)
        elif target_count > self.running_actor_count:
            for _ in range(self.running_actor_count, target_count):
                yield self.add_new_actor()

    async def drain_to(self, target_count: int):
        while self.running_actor_count > target_count:
            await self.poison_queue.queue.put(POISON_PILL)
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._poisoning_done_future),
                    timeout=self._poison_timeout,
                )
            except asyncio.TimeoutError:
                self._force_cancel_one()
                self._poisoning_done_future.cancel()
            self._poisoning_done_future = self._loop.create_future()

    def _force_cancel_one(self):
        if not self._actor_listening_async_task_dict:
            return
        _, task = self._actor_listening_async_task_dict.popitem()
        task.cancel()

    async def add_new_actor(self):
        running_actor = self.dist_api.get_running_actor(
            actor_creator=self.actor_partial
        )
        listener_name = uuid.uuid1().hex
        coro = self._listen(running_actor=running_actor, name=listener_name)
        task = self._loop.create_task(coro, name=listener_name)
        self._actor_listening_async_task_dict[listener_name] = task

    @property
    def running_actor_count(self):
        return len(self._actor_listening_async_task_dict)

    async def _listen(self, running_actor: ActorBase, name: str):
        try:
            while True:
                next_task, src_q = await self._get_next_task()
                try:
                    self.in_prog += 1
                    await self._process_task(running_actor, next_task, src_q)
                except ActorListenBreaker as e:
                    await self._end_actor(running_actor, e, name)
                    return
                finally:
                    self.in_prog -= 1
        except asyncio.CancelledError:
            self._safe_kill(running_actor)
            return
        except Exception as e:  # pragma: no cover
            self._log("listener crashed", error=repr(e))
            self._result_queue.put(TaskResult(e, False, self._task_queue))
            self._safe_kill(running_actor)
            self._actor_listening_async_task_dict.pop(name, None)
            return

    def _safe_kill(self, running_actor):
        try:
            self.dist_api.kill(running_actor)
        except Exception:  # pragma: no cover
            pass

    async def _get_next_task(self):
        while True:
            await asyncio.wait(
                [self._task_queue.getting_task, self.poison_queue.getting_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self.poison_queue.done():
                return self.poison_queue.pop(), self.poison_queue
            if self._task_queue.done():
                return self._task_queue.pop(), self._task_queue

    async def _process_task(
        self,
        running_actor: ActorBase,
        task: "SchedulerTask",
        source_queue: TaskQueue,
    ):
        if task is POISON_PILL:
            raise ActorPoisoned("poisoned")
        if task.rate_costs:
            while True:
                wait = self._rate_gate.try_consume(task.rate_costs)
                if wait <= 0:
                    break
                await asyncio.sleep(wait)
        try:
            future = self.dist_api.get_future(running_actor, task)
            if self._task_timeout is not None:
                out = await asyncio.wait_for(future, timeout=self._task_timeout)
            else:
                out = await future
            if isinstance(out, Exception):
                if isinstance(out, DistantException):
                    out = out.e.with_traceback(out.tb.as_traceback())
                raise out
            self._result_queue.put(TaskResult(out, True, source_queue))
            return
        except ActorListenBreaker:
            await self._task_queue.queue.put(task)
            raise
        except asyncio.TimeoutError:
            task.fail_count += 1
            if task.fail_count > task.max_fails:
                err = TimeoutError(
                    f"task timed out after {self._task_timeout}s "
                    f"(actor={self.actor_cls.__name__})"
                )
                self._result_queue.put(TaskResult(err, False, source_queue))
            else:
                await self._task_queue.queue.put(task)
            raise ActorListenBreaker("task_timeout")
        except self.dist_api.exception as e:
            self._log("remote task error", error=repr(e))
            task.fail_count += 1
            if task.fail_count > task.max_fails:
                exc = self.dist_api.parse_exception(e)
                self._result_queue.put(TaskResult(exc, False, source_queue))
            else:
                await self._task_queue.queue.put(task)

    async def _end_actor(self, running_actor: ActorBase, e, name):
        self._safe_kill(running_actor)
        self._actor_listening_async_task_dict.pop(name, None)
        if isinstance(e, ActorPoisoned):
            if not self._poisoning_done_future.done():
                self._poisoning_done_future.set_result(True)
        else:
            await self.add_new_actor()

    def _log(self, msg, **kwargs):
        if self._debug:
            ctx = {
                "actor": self.actor_cls.__name__,
                "running": self.running_actor_count,
                "in_prog": self.in_prog,
                **kwargs,
            }
            logging.getLogger("atqo.actor_set").info(f"{msg} {ctx}")


@dataclass
class SchedulerTask:
    argument: Any
    actor: Optional[type[ActorBase]] = None
    rate_costs: dict[str, int] = field(default_factory=dict)
    allowed_fail_count: int = 1

    def __post_init__(self):
        self.max_fails = self.allowed_fail_count
        self.fail_count = 0

    def __repr__(self) -> str:
        cls = getattr(self.actor, "__name__", self.actor)
        return f"Task({self.argument!r}, actor={cls})"


@dataclass
class TaskResult:
    value: Any
    is_ok: bool
    source_queue: "TaskQueue"
