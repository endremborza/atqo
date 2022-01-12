import asyncio
import itertools
import uuid
from enum import Enum
from queue import Empty, Queue
from threading import Thread
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Type

from structlog import get_logger

from atqo.bases import TaskPropertyBase

from .distributed_apis import get_dist_api
from .exceptions import (
    ActorListenBreaker,
    ActorPoisoned,
    NotEnoughResourcesToContinue,
)
from .exchange import CapsetExchange
from .resource_handling import (
    ActiveTaskPropertySet,
    Capability,
    CapabilitySet,
    NumStore,
)

if TYPE_CHECKING:
    from .bases import ActorBase, DistAPIBase  # pragma: no cover


POISON_KEY = frozenset([])  # just make sure it comes before any other
POISON_PILL = None
ALLOWED_CONSUMER_FAILS = 5


def _start_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


class Scheduler:
    def __init__(
        self,
        actor_dict: Dict[CapabilitySet, Type["ActorBase"]],
        resource_limits: Dict[Enum, float],
        concurrent_task_limit: Callable[[List[TaskPropertyBase]], bool] = None,
        distributed_system: str = "sync",
        actor_init_args=(),
        reorganize_after_every_task: bool = True,  # overkill
        verbose=False,
    ) -> None:
        """Core scheduler class

        default reorganize when:
          - new tasks are added
          - no new task can be consumed
          -

        """

        self._result_queue = Queue()
        self._active_async_tasks = set()

        self._loop = asyncio.new_event_loop()

        self._thread = Thread(
            target=_start_loop, args=(self._loop,), daemon=True
        )
        self._thread.start()

        self._frequent_reorg = reorganize_after_every_task
        self._verbose = verbose
        self._task_limiter = concurrent_task_limit
        self._dist_api: DistAPIBase = get_dist_api(distributed_system)()

        self._actor_init_args = actor_init_args

        self._actor_sets = {}
        self._run(self._add_actor_sets(actor_dict))

        self._capset_exchange = CapsetExchange(
            actor_dict.keys(), resource_limits
        )
        self._task_queues: Dict[CapabilitySet, asyncio.Queue] = {}

        self._used_resources = NumStore()
        self._active_task_properties = ActiveTaskPropertySet()

    def __del__(self):
        self._dist_api.join()

    def process(
        self,
        batch_producer: Callable[[], List["SchedulerTask"]],
        result_processor=None,
        min_queue_size: int = 0,
    ):

        while True:
            is_done = self.is_idle
            next_batch = batch_producer()
            (result_processor or list)(self.get_processed_results())
            batch_size = len(next_batch)
            empty_batch = batch_size == 0
            self._log("new batch", size=batch_size, was_done=is_done)
            if is_done and empty_batch:
                break
            if empty_batch:
                self.wait_until_n_tasks_remain(0)
                continue

            self.refill_task_queue(next_batch)
            try:
                self.wait_until_n_tasks_remain(min_queue_size)
            except KeyboardInterrupt:  # pragma: nocover
                self._log(f"Interrupted waiting for {self}")
                break

    def refill_task_queue(self, task_batch: Iterable["SchedulerTask"]):
        self._run(self._refill_task_queue(task_batch))

    def wait_until_n_tasks_remain(self, remaining_tasks: int = 0):
        self._run(self._await_until(remaining_tasks))

    def join(self):
        self.wait_until_n_tasks_remain(0)
        self._run(self._drain_all_actor_sets())
        try:
            self._run(asyncio.wait(self._all_actors))
        except AssertionError:
            pass
        self._run(self._cleanup())
        self._dist_api.join()

    def get_processed_results(self) -> Iterable:
        while True:
            try:
                yield self._result_queue.get(False)
            except Empty:
                break

    @property
    def is_empty(self) -> bool:
        return self.is_idle and self._result_queue.empty()

    @property
    def is_idle(self) -> bool:
        return not self._active_async_tasks

    @property
    def queued_task_count(self):
        return sum([tq.qsize() for tq in self._task_queues.values()])

    def _run(self, coro, wait=True):
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        if wait:
            fut.result()

    def _log(self, logstr, **kwargs):
        if self._verbose:
            get_logger(
                api=type(self._dist_api).__name__,
                queued=self.queued_task_count,
                working=self._running_consumer_count,
            ).info(logstr, **kwargs)

    def _q_of_new_capset(self, capset: CapabilitySet) -> asyncio.Queue:

        new_task_queue = asyncio.Queue()
        self._task_queues[capset] = new_task_queue
        for a_capset, actor_set in self._actor_sets.items():
            if a_capset >= capset:
                actor_set.add_task_queue(new_task_queue, capset)
        return new_task_queue

    async def _add_actor_sets(self, actor_dict):
        self._actor_sets = {
            capset: ActorSet(actor_cls, self._dist_api)
            for capset, actor_cls in actor_dict.items()
        }

    async def _refill_task_queue(self, task_batch: Iterable["SchedulerTask"]):
        for scheduler_task in task_batch:
            await self._add_task(scheduler_task)
        await self._reorganize_actors()

    async def _add_task(self, scheduler_task: "SchedulerTask"):
        coro = self._await_future_and_put_result_to_queue(scheduler_task)
        async_task = self._loop.create_task(coro)
        self._active_async_tasks.add(async_task)
        capset = scheduler_task.requirements
        q = self._task_queues.get(capset) or self._q_of_new_capset(capset)
        await q.put(scheduler_task)

    async def _await_future_and_put_result_to_queue(
        self, scheduler_task: "SchedulerTask"
    ):
        scheduler_task.init_future()
        task_result = await scheduler_task.future
        self._result_queue.put(task_result)

    async def _reorganize_actors(self):
        """optimize actor set sizes

        target: minimize max n(tasks<=capset) / n(actors>=capset)
                for all task queue capsets
        limit: capset resource use * n_actors <=total resource avail
               for all actorset capsets

        heuristic:
        value of adding: decrease caused in  target / number possible remaining

        """
        needs = {
            actor_capset: actorset.total_task_count
            for actor_capset, actorset in self._actor_sets.items()
        }

        new_needs = NumStore(needs)
        new_ideals = self._capset_exchange.set_values(new_needs)
        self._log(f"reorganizing on {needs}")
        self._log(f"reorganizing to {new_ideals}")

        for cs, new_ideal in new_ideals.items():
            await self._actor_sets[cs].set_running_actors_to(new_ideal)

        dead_end = self.queued_task_count and self._capset_exchange.idle

        if dead_end:
            await self._cleanup()
            await self._cancel_remaining_tasks()
            raise NotEnoughResourcesToContinue(
                f"{self.queued_task_count} remaining and no launchable actors"
            )

    async def _await_until(self, remaining_tasks: int = 0):
        return_when = (
            "FIRST_COMPLETED"
            if (self._frequent_reorg or remaining_tasks) > 0
            else "ALL_COMPLETED"
        )
        while len(self._active_async_tasks) > remaining_tasks:
            done, _ = await asyncio.wait(
                self._active_async_tasks, return_when=return_when
            )
            self._active_async_tasks.difference_update(done)
            if self._frequent_reorg:
                await self._reorganize_actors()

        await self._reorganize_actors()

    async def _drain_all_actor_sets(self):
        for actor_set in self._actor_sets.values():
            await actor_set.drain_to(0)

    async def _cleanup(self):
        for aset in self._actor_sets.values():
            aset.kill()

    async def _cancel_remaining_tasks(self):
        for atask in self._active_async_tasks:
            atask.cancel()

    @property
    def _running_consumer_count(self):
        return sum(
            [aset.running_actor_count for aset in self._actor_sets.values()]
        )

    @property
    def _all_actors(self):
        return itertools.chain(
            *[aset.all_actor_tasks for aset in self._actor_sets.values()]
        )


class ActorSet:
    def __init__(
        self,
        actor_cls: Type["ActorBase"],
        dist_api: "DistAPIBase",
    ) -> None:
        self.actor_cls = actor_cls
        self.dist_api = dist_api

        self._poison_queue = asyncio.Queue()
        self._async_queue_dict: Dict[str, asyncio.Queue] = {
            POISON_KEY: self._poison_queue
        }
        self._actor_listening_async_task_dict: Dict[str, asyncio.Task] = {}
        self._poisoning_done_future = asyncio.Future()
        self._async_queue_get_task_dict = {
            POISON_KEY: asyncio.create_task(self._poison_queue.get())
        }

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
            await self._poison_queue.put(POISON_PILL)
            await self._poisoning_done_future
            self._poisoning_done_future = asyncio.Future()
        return n

    async def add_new_actor(self):
        running_actor = self.dist_api.get_running_actor(
            actor_cls=self.actor_cls
        )
        listener_name = uuid.uuid1().hex
        coroutine = self._listen(
            running_actor=running_actor,
            name=listener_name,
        )
        task = asyncio.create_task(coroutine, name=listener_name)
        self._logger.info("adding consumer", listener_task=task.get_name())
        self._actor_listening_async_task_dict[listener_name] = task

    def add_task_queue(self, task_queue: asyncio.Queue, key: CapabilitySet):
        # TODO: ping here to expand what this is waiting for
        # in _get_next_task
        self._async_queue_dict[key] = task_queue
        self._async_queue_get_task_dict[key] = asyncio.create_task(
            self._async_queue_dict[key].get()
        )

    def kill(self):

        for task in self._async_queue_get_task_dict.values():
            task.cancel()

    @property
    def queued_task_count(self):
        return sum([q.qsize() for q in self._async_queue_dict.values()])

    @property
    def processing_task_count(self):
        return sum(
            [t.done() for t in self._async_queue_get_task_dict.values()]
        )

    @property
    def total_task_count(self):
        return self.processing_task_count + self.queued_task_count

    @property
    def running_actor_count(self):
        return len(self._actor_listening_async_task_dict)

    @property
    def all_actor_tasks(self):
        return self._actor_listening_async_task_dict.values()

    async def _listen(self, running_actor: "ActorBase", name: str):
        self._logger.info(
            "consumer listening",
            actor=type(running_actor).__name__,
        )
        fails = 0
        while True:
            next_task = await self._get_next_task()
            try:
                fails = await self._process_task(
                    running_actor, next_task, fails
                )
            except ActorListenBreaker as e:
                self._logger.info(
                    "stopping consumer",
                    reason=e,
                    actor=type(running_actor).__name__,
                )
                self.dist_api.kill(running_actor)
                del self._actor_listening_async_task_dict[name]
                self._poisoning_done_future.set_result(True)
                if not isinstance(e, ActorPoisoned):
                    await self.add_new_actor()
                return

    async def _get_next_task(self) -> "SchedulerTask":
        while True:
            await asyncio.wait(
                self._async_queue_get_task_dict.values(),
                return_when="FIRST_COMPLETED",
            )
            for rkey, astask in sorted(
                self._async_queue_get_task_dict.items(),
                key=lambda kv: kv[0],
                reverse=True,  # TODO why??
            ):
                if astask.done():
                    next_task = self._async_queue_get_task_dict.pop(
                        rkey
                    ).result()
                    self._async_queue_get_task_dict[
                        rkey
                    ] = asyncio.create_task(self._async_queue_dict[rkey].get())
                    return next_task

    async def _process_task(
        self,
        running_actor: "ActorBase",
        next_task: "SchedulerTask",
        fails: int,
    ):
        if next_task is POISON_PILL:
            raise ActorPoisoned("poisoned")
        try:
            out = await self.dist_api.get_future(running_actor, next_task)
            next_task.set_future(out)
            return 0
        except self.dist_api.exception as e:
            self._logger.warning(
                "Remote consumption error ",
                e=e,
                te=type(e),
            )
            next_task.fail_count += 1
            if next_task.fail_count > next_task.max_fails:
                next_task.set_future(self.dist_api.parse_exception(e))
            else:
                await self._async_queue_dict[next_task.requirements].put(
                    next_task
                )
        if fails >= ALLOWED_CONSUMER_FAILS:
            raise ActorListenBreaker(f"{fails} number of fails reached")
        return fails + 1

    @property
    def _logger(self):
        return get_logger(**self._log_dic)

    @property
    def _log_dic(self):
        return {
            "actor": self.actor_cls.__name__,
            "queued": self.queued_task_count,
            "processing": self.processing_task_count,
            "actors_running": self.running_actor_count,
        }


class SchedulerTask:
    def __init__(
        self,
        argument: Any,
        requirements: List[Capability] = None,
        properties: List[TaskPropertyBase] = None,
        allowed_fail_count: int = 1,
    ):
        self.argument = argument
        self.requirements = CapabilitySet(requirements or [])
        self.properties = properties or []
        self.max_fails = allowed_fail_count
        self.fail_count = 0
        self.future = None

    def init_future(self):
        self.future = asyncio.Future()

    def set_future(self, value):
        self.future.set_result(value)
