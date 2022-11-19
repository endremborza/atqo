import time

import pytest

from atqo.bases import ActorBase
from atqo.core import Scheduler, SchedulerTask
from atqo.distributed_apis import DEFAULT_MULTI_API
from atqo.resource_handling import Capability, CapabilitySet


class Actor(ActorBase):
    def __init__(self) -> None:  # pragma: no cover
        self.e = 1

    def consume(self, task_arg):  # pragma: no cover
        return self.e + task_arg


class ActorSlow(Actor):
    def __init__(self) -> None:  # pragma: no cover
        time.sleep(0.5)
        self.e = 20


cap1 = Capability({"A": 1})
cap2 = Capability({"B": 1})


@pytest.fixture
def test_scheduler():
    return Scheduler(
        actor_dict={
            CapabilitySet([cap1]): Actor,
            CapabilitySet([cap2]): ActorSlow,
        },
        resource_limits={"A": 3, "B": 2},
        distributed_system=DEFAULT_MULTI_API,
        verbose=True,
    )


def test_runoff(test_scheduler: Scheduler):
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

    out = list(test_scheduler.process(_BP(), min_queue_size=5))
    test_scheduler.join()
    assert len(out) == 28


def test_slow_start(test_scheduler: Scheduler):

    rtasks = [SchedulerTask(i, [cap1]) for i in range(10)]
    stasks = [SchedulerTask(i, [cap2]) for i in range(2)]

    test_scheduler.refill_task_queue(rtasks + stasks)
    assert sorted(test_scheduler.join()) == [
        *range(1, 11),
        *range(20, 22),
    ]
