from functools import partial

import pytest

from atqo.bases import ActorBase
from atqo.core import Scheduler, SchedulerTask
from atqo.distributed_apis import DEFAULT_DIST_API_KEY, DIST_API_MAP
from atqo.exceptions import ActorListenBreaker, NotEnoughResources
from atqo.resource_handling import Capability, CapabilitySet

from .test_race import cap1


def test_empty_scheduler():
    scheduler = Scheduler({}, {})
    assert scheduler.is_idle
    assert scheduler.is_empty
    assert scheduler.queued_task_count == 0


@pytest.mark.parametrize("dist_api", DIST_API_MAP.keys())
def test_dead_end(dist_api):

    cap2 = Capability({"A": 2})

    scheduler = Scheduler(
        {CapabilitySet([cap1]): ActorBase}, {"A": 1}, distributed_system=dist_api
    )

    with pytest.raises(NotEnoughResources):
        scheduler.refill_task_queue([SchedulerTask("x", requirements=[cap1, cap2])])


class Actor(ActorBase):
    def __init__(self, n: int) -> None:
        self.e = n + 3

    def consume(self, task_arg):
        if self.e > 6:
            raise ActorListenBreaker()
        self.e += 1
        return task_arg


@pytest.mark.parametrize("dist_api", DIST_API_MAP.keys())
def test_bad_init(dist_api):

    scheduler = Scheduler(
        {CapabilitySet([cap1]): partial(Actor, ("XX",))},
        {"A": 1},
        distributed_system=dist_api,
    )

    with pytest.raises(TypeError):
        scheduler.refill_task_queue([SchedulerTask(10, requirements=[cap1])])
    scheduler.cleanup()


def test_restart():
    scheduler = Scheduler(
        {CapabilitySet([cap1]): partial(Actor, 1)},
        {"A": 1},
        distributed_system=DEFAULT_DIST_API_KEY,
        verbose=True,
    )

    scheduler.refill_task_queue([SchedulerTask(i) for i in range(6)])
    out = sorted(scheduler.iter_until_n_tasks_remain(0))
    assert out == list(range(6))
    scheduler.join()
