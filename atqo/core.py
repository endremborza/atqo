import asyncio
import uuid
from dataclasses import dataclass
from enum import Enum
from functools import partial
from itertools import chain
from queue import Queue
from threading import Thread
from typing import Any, Callable, Iterable, Union

from structlog import get_logger

from .bases import ActorBase, DistAPIBase, TaskPropertyBase
from .distributed_apis import DEFAULT_DIST_API_KEY, get_dist_api
from .exceptions import (
    ActorListenBreaker,
    ActorPoisoned,
    DistantException,
    NotEnoughResourcesToContinue,
)
from .exchange import CapsetExchange
from .resource_handling import Capability, CapabilitySet, NumStore
from .utils import dic_val_filt

POISON_KEY = frozenset([])  # just make sure it comes before any other
POISON_PILL = None
ALLOWED_CONSUMER_FAILS = 5


def _start_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _get_loop_of_daemon():
    loop = asyncio.new_event_loop()
    thread = Thread(target=_start_loop, args=(loop,), daemon=True)
    thread.start()
    return loop, thread


class Scheduler:
    def __init__(
        self,
        actor_dict: dict[CapabilitySet, Union[type["ActorBase"], partial]],
        resource_limits: dict[Enum, float],
        distributed_system: str = DEFAULT_DIST_API_KEY,
        verbose=False,
    ) -> None:
        """Core scheduler class

        default reorganize when:
          - new tasks are added
          - no new task can be consumed
          -

        """
        for _, v in actor_dict.items():
            _ab = v.func if isinstance(v, partial) else v
            assert ActorBase in _ab.mro()

        self._loop, self._thread = _get_loop_of_daemon()
        self._result_queue: Queue[TaskResult] = Queue()
        self._task_queues: dict[CapabilitySet, TaskQueue] = {}
        self._verbose = verbose
        self._dist_api: DistAPIBase = get_dist_api(distributed_system)()
        self._actor_sets: dict[CapabilitySet, ActorSet] = dict(
            self._get_actor_set_items(actor_dict)
        )
        self._capset_exchange = CapsetExchange(actor_dict.keys(), resource_limits)

        # TODO
        # concurrent_task_limit: Callable[[List[TaskPropertyBase]], bool]
        # self._active_task_properties = ActiveTaskPropertySet()
        # self._task_limiter = concurrent_task_limit

    def __del__(self):
        try:
            self._dist_api.join()
        except AttributeError:  # pragma: no cover
            pass

    def process(
        self,
        batch_producer: Callable[[], list["SchedulerTask"]],
        min_queue_size: int = 0,
    ):

        while True:
            next_batch = batch_producer()
            batch_size = len(next_batch)
            empty_batch = batch_size == 0
            logstr = f"{'empty' if empty_batch else 'new'} batch"
            self._log(logstr, size=batch_size, was_done=self.is_empty)
            if self.is_empty and empty_batch:
                break
            self.refill_task_queue(next_batch)
            target = 0 if empty_batch else min_queue_size
            try:
                for res in self.iter_until_n_tasks_remain(target):
                    yield res
            except KeyboardInterrupt:  # pragma: no cover
                self._log(f"Interrupted waiting for {self}")
                break

    def refill_task_queue(self, task_batch: Iterable["SchedulerTask"]):
        # invalid state error is raised if future is already set on task
        for scheduler_task in task_batch:
            capset = scheduler_task.requirements
            q = self._task_queues.get(capset) or self._q_of_new_capset(capset)
            q.put(scheduler_task)
        self._reorganize_actors()

    def iter_until_n_tasks_remain(self, remaining_tasks: int = 0):
        while True:
            if self._in_progress_or_queued >= remaining_tasks:
                if self.is_idle and self._result_queue.empty():
                    return
                tr = self._result_queue.get()
                if tr.is_last_in_queue:
                    self._reorganize_actors()
                yield tr.value
            else:
                break

    def join(self):
        out = list(self.iter_until_n_tasks_remain(0))
        self._set_actor_sets({k: 0 for k in self._actor_sets.keys()})
        self.cleanup()
        self._dist_api.join()
        return out

    def cleanup(self):
        for aset in self._actor_sets.values():
            aset.poison_queue.cancel()
        for t_queue in self._task_queues.values():
            t_queue.cancel()
        self._loop.stop()

    @property
    def is_empty(self) -> bool:
        return self.is_idle and self._result_queue.empty()

    @property
    def is_idle(self) -> bool:
        return not self._in_progress_or_queued

    @property
    def queued_task_count(self):
        return sum([tq.size for tq in self._task_queues.values()])

    def _log(self, logstr, **kwargs):
        if self._verbose:
            get_logger(
                api=type(self._dist_api).__name__,
                queued=self.queued_task_count,
                working=self._running_consumer_count,
            ).info(logstr, **kwargs)

    def _q_of_new_capset(self, capset: CapabilitySet) -> "TaskQueue":
        new_task_queue = TaskQueue(self._loop)
        self._task_queues[capset] = new_task_queue
        for task_cs, task_queue in self._task_queues.items():
            if task_cs > capset:
                task_queue.reset_ping()
        return new_task_queue

    def _get_actor_set_items(self, actor_dict: dict):
        for capset, actor in actor_dict.items():
            pactor = actor if isinstance(actor, partial) else partial(actor)
            yield capset, ActorSet(
                pactor,
                self._dist_api,
                capset,
                self._task_queues,
                self._result_queue,
                self._loop,
                self._verbose,
            )

    def _reorganize_actors(self):
        """optimize actor set sizes

        target: minimize max n(tasks<=capset) / n(actors>=capset)
                for all task queue capsets
        limit: capset resource use * n_actors <=total resource avail
               for all actorset capsets

        heuristic:
        value of adding: decrease caused in  target / number possible remaining

        """
        need_dic = {cs: t.size for cs, t in self._task_queues.items()}
        new_needs = NumStore(need_dic)
        new_ideals = self._capset_exchange.set_values(new_needs)
        curr = {c: a.running_actor_count for c, a in self._actor_sets.items()}
        for pref, dic in [("need", need_dic), ("from", curr), ("to", new_ideals)]:
            self._log(f"reorganizing {pref} {dic_val_filt(dic)}")

        self._set_actor_sets(new_ideals)
        self._log("reorganized")

        dead_end = self.queued_task_count and self._capset_exchange.idle

        if dead_end:
            # TODO: can be stuck here sometimes somehow :(
            self.cleanup()
            raise NotEnoughResourcesToContinue(
                f"{self.queued_task_count} remaining and no launchable actors"
            )

    def _set_actor_sets(self, ideals: dict):
        set_runs = [
            self._actor_sets[cs].set_running_actors_to(new_ideal)
            for cs, new_ideal in ideals.items()
        ]
        coro = asyncio.wait(map(self._loop.create_task, set_runs))
        done, _ = asyncio.run_coroutine_threadsafe(coro, loop=self._loop).result()
        for t in done:
            e = t.exception()
            if e:
                raise e

        # self._thread.join()
        # self._loop.close()

    @property
    def _running_consumer_count(self):
        return sum([aset.running_actor_count for aset in self._actor_sets.values()])

    @property
    def _in_progress_or_queued(self):
        # TODO: race condition here?
        return self.queued_task_count + sum(
            aset.in_prog for aset in self._actor_sets.values()
        )


class TaskQueue:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.queue = asyncio.Queue()
        self.getting_task = loop.create_task(self.queue.get())
        self.ping = loop.create_future()

    def reset_ping(self):
        self.ping.set_result(None)
        self.ping = self.loop.create_future()

    def pop(self):
        out = self.getting_task.result()
        self.getting_task = self.loop.create_task(self.queue.get())
        return out

    def put(self, item):
        asyncio.run_coroutine_threadsafe(self.queue.put(item), self.loop).result()

    @property
    def cancel(self):
        return self.getting_task.cancel

    @property
    def done(self):
        return self.getting_task.done

    @property
    def size(self):
        return self.queue.qsize() + int(self.getting_task.done())

    @property
    def tasks(self):
        return [self.ping, self.getting_task]


class ActorSet:
    def __init__(
        self,
        actor_partial: partial,
        dist_api: "DistAPIBase",
        capset: CapabilitySet,
        task_queues: dict[CapabilitySet, TaskQueue],
        result_queue: Queue,
        loop: asyncio.AbstractEventLoop,
        debug: bool,
    ) -> None:
        self.actor_partial = actor_partial
        self.dist_api = dist_api
        self.capset = capset

        self.poison_queue = TaskQueue(loop)
        self._poisoning_done_future = loop.create_future()
        self._loop = loop
        self._task_queues = task_queues
        self._result_queue = result_queue
        self._actor_listening_async_task_dict: dict[str, asyncio.Task] = {}
        self._debug = debug
        self.in_prog = 0

    def __repr__(self):
        dic_str = [f"{k}={v}" for k, v in self._log_dic.items()]
        return f"{type(self).__name__}({', '.join(dic_str)}"

    async def set_running_actors_to(self, target_count):
        if target_count < self.running_actor_count:
            await self.drain_to(target_count)
        elif target_count > self.running_actor_count:
            for _ in range(self.running_actor_count, target_count):
                await self.add_new_actor()

    async def drain_to(self, target_count: int) -> int:
        n = 0
        for _ in range(target_count, self.running_actor_count):
            n += 1
            await self.poison_queue.queue.put(POISON_PILL)
            await self._poisoning_done_future
            self._poisoning_done_future = self._loop.create_future()
        return n

    async def add_new_actor(self):
        running_actor = self.dist_api.get_running_actor(
            actor_creator=self.actor_partial
        )
        listener_name = uuid.uuid1().hex
        coroutine = self._listen(running_actor=running_actor, name=listener_name)
        task = self._loop.create_task(coroutine, name=listener_name)
        self._log("adding consumer", listener_task=task.get_name())
        self._actor_listening_async_task_dict[listener_name] = task

    @property
    def task_count(self):
        return sum([q.size for q in self._task_queues.values()])

    @property
    def running_actor_count(self):
        return len(self._actor_listening_async_task_dict)

    async def _listen(self, running_actor: "ActorBase", name: str):
        # WARNING: if error happens here, it will get swallowed to the abyss
        self._log("consumer listening", running=type(running_actor).__name__)
        while True:
            next_task, is_last = await self._get_next_task()
            try:
                self.in_prog += 1
                await self._process_task(running_actor, next_task, is_last)
            except ActorListenBreaker as e:
                await self._end_actor(running_actor, e, name)
                return
            finally:
                self.in_prog -= 1

    async def _get_next_task(self) -> tuple["SchedulerTask", bool]:
        while True:
            await asyncio.wait(
                self._wait_on_tasks,
                return_when="FIRST_COMPLETED",
            )
            for t_queue in self._sorted_queues:
                if t_queue.done():
                    return t_queue.pop(), (t_queue.size == 0)

    async def _process_task(
        self, running_actor: "ActorBase", task: "SchedulerTask", is_last: bool
    ):
        if task is POISON_PILL:
            raise ActorPoisoned("poisoned")
        try:
            out = await self.dist_api.get_future(running_actor, task)
            if isinstance(out, Exception):
                if isinstance(out, DistantException):
                    out = out.e.with_traceback(out.tb.as_traceback())
                raise out
            self._result_queue.put(TaskResult(out, True, is_last))
            return 0
        except ActorListenBreaker as e:
            await self._task_queues[task.requirements].queue.put(task)
            raise e
        except self.dist_api.exception as e:
            self._log("Remote consumption error ", e=e, te=type(e).__name__, task=task)
            task.fail_count += 1
            if task.fail_count > task.max_fails:
                result = TaskResult(self.dist_api.parse_exception(e), False, is_last)
                self._result_queue.put(result)
            else:
                await self._task_queues[task.requirements].queue.put(task)

    async def _end_actor(self, running_actor: "ActorBase", e, name):
        self._log("stopping consumer", reason=e, running=type(running_actor).__name__)
        self.dist_api.kill(running_actor)
        del self._actor_listening_async_task_dict[name]
        if not isinstance(e, ActorPoisoned):
            self._log("restarting consumer")
            await self.add_new_actor()
        else:
            self._poisoning_done_future.set_result(True)

    def _log(self, s, **kwargs):
        if self._debug:
            get_logger(**self._log_dic).info(s, **kwargs)

    @property
    def _wait_on_tasks(self):
        return chain(
            *[self._task_queues[k].tasks for k in self._task_keys],
            [self.poison_queue.getting_task],
        )

    @property
    def _sorted_queues(self):
        keys = sorted(self._task_keys)
        return reversed([self.poison_queue, *map(self._task_queues.get, keys)])

    @property
    def _task_keys(self):
        return filter(self.capset.__ge__, self._task_queues.keys())

    @property
    def _log_dic(self):
        return {
            "actor": getattr(self.actor_partial.func, "__name__", None),
            "tasks": self.task_count,
            "actors_running": self.running_actor_count,
        }


class SchedulerTask:
    def __init__(
        self,
        argument: Any,
        requirements: list[Capability] = None,
        properties: list[TaskPropertyBase] = None,
        allowed_fail_count: int = 1,
    ):
        self.argument = argument
        self.requirements = CapabilitySet(requirements or [])
        self.properties = properties or []
        self.max_fails = allowed_fail_count
        self.fail_count = 0

    def __repr__(self) -> str:
        return f"Task: {self.argument}, " f"Requires: {self.requirements}, "


@dataclass
class TaskResult:
    value: Any
    is_ok: bool
    is_last_in_queue: bool
