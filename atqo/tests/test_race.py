import time

from atqo.bases import ActorBase
from atqo.core import Scheduler, SchedulerTask
from atqo.distributed_apis import DEFAULT_MULTI_API
from atqo.resource_handling import Capability, CapabilitySet
from atqo.utils import ArgRunner


class Actor(ActorBase):
    def __init__(self) -> None:
        self.e = 1

    def consume(self, task_arg):
        return self.e + task_arg


class ActorSlow(Actor):
    def __init__(self) -> None:
        time.sleep(0.5)
        self.e = 20


def test_runoff():

    cap1 = Capability({"A": 1})
    cap2 = Capability({"B": 1})

    out = []

    scheduler = Scheduler(
        actor_dict={
            CapabilitySet([cap1]): ArgRunner(Actor),
            CapabilitySet([cap2]): ArgRunner(ActorSlow),
        },
        resource_limits={"A": 3, "B": 2},
        distributed_system=DEFAULT_MULTI_API,
        verbose=True,
    )

    def _get_st(cap, n):
        return [SchedulerTask(i, [cap]) for i in range(n)]

    class _BP:
        def __init__(self) -> None:
            self.i = -1
            self._batches = [
                _get_st(cap1, 10),
                _get_st(cap2, 3),
                _get_st(cap1, 5),
                _get_st(cap2, 10),
            ]

        def __call__(self):
            self.i += 1
            if self.i >= len(self._batches):
                return []
            return self._batches[self.i]

    scheduler.process(_BP(), lambda l: [out.append(e) for e in l], min_queue_size=5)

    assert len(out) == 28


def test_slow_start():

    cap1 = Capability({"A": 1})
    cap2 = Capability({"A": 1, "B": 1})

    scheduler = Scheduler(
        actor_dict={
            CapabilitySet([cap1]): ArgRunner(Actor),
            CapabilitySet([cap2]): ArgRunner(ActorSlow),
        },
        resource_limits={"A": 3, "B": 2},
        distributed_system=DEFAULT_MULTI_API,
        verbose=True,
    )

    rtasks = [SchedulerTask(i, [cap1]) for i in range(10)]
    stasks = [SchedulerTask(i, [cap2]) for i in range(2)]

    scheduler.refill_task_queue(rtasks + stasks)
    scheduler.join()
    assert sorted(scheduler.get_processed_results()) == [*range(1, 11), *range(20, 22)]
