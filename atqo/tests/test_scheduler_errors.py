import pytest

from atqo.bases import ActorBase
from atqo.core import Scheduler, SchedulerTask
from atqo.distributed_apis import DIST_API_MAP
from atqo.exceptions import NotEnoughResources
from atqo.resource_handling import Capability, CapabilitySet
from atqo.utils import ArgRunner


def test_empty_scheduler():
    scheduler = Scheduler({}, {})
    assert scheduler.is_idle
    assert scheduler.is_empty
    assert scheduler.queued_task_count == 0


@pytest.mark.parametrize(
    "dist_api",
    DIST_API_MAP.keys(),
)
def test_dead_end(dist_api):

    cap1 = Capability({"A": 1})
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
        return self.e + task_arg


@pytest.mark.parametrize(
    "dist_api",
    DIST_API_MAP.keys(),
)
def test_bad_init(dist_api):

    cap1 = Capability({"A": 1})

    scheduler = Scheduler(
        {CapabilitySet([cap1]): ArgRunner(Actor, ("XX",))},
        {"A": 1},
        distributed_system=dist_api,
    )

    with pytest.raises(TypeError):
        scheduler.refill_task_queue([SchedulerTask(10, requirements=[cap1])])
