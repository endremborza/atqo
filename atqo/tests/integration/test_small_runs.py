import pytest

from atqo import ActorBase, Capability, CapabilitySet, Scheduler, SchedulerTask
from atqo.distributed_apis import DEFAULT_DIST_API_KEY
from atqo.simplified_functions import BatchProd

LIMIT_DIC = {"A": 3}

cap1 = Capability({"A": 1})
cap2 = Capability({"A": 1})


class Actor(ActorBase):
    def consume(self, task_arg):
        return f"done-{task_arg}"


@pytest.fixture
def test_scheduler():
    actor_dict = {
        CapabilitySet([cap2, cap1]): Actor,
    }

    return Scheduler(
        actor_dict=actor_dict,
        resource_limits=LIMIT_DIC,
        distributed_system=DEFAULT_DIST_API_KEY,
        verbose=True,
    )


class ListProd:
    def __init__(self, list_base, batch_size) -> None:
        self._size = batch_size
        self._list = list_base
        self._i = 0

    def __call__(self):
        out = []
        for _ in range(self._size):
            try:
                out.append(self._list[self._i])
                self._i += 1
            except IndexError:
                break
        return out


def test_over_actors(test_scheduler: Scheduler):

    tasks = [
        SchedulerTask("task1", requirements=[cap1]),
    ]

    out = list(test_scheduler.process(BatchProd(tasks, 2, lambda x: x)))
    test_scheduler.join()

    assert out == ["done-task1"]


def test_recurse(test_scheduler: Scheduler):

    tasks = [SchedulerTask(f"task{i}", requirements=[cap1]) for i in range(1, 9)]

    out = []
    for e in test_scheduler.process(ListProd(tasks, 10)):
        out.append(e)
        if len(out) % 2 == 0:
            tasks.append(tasks[len(out) // 2])

    test_scheduler.join()
    muls = [(1, 1), *[(i, 2) for i in range(2, 9)]]
    assert sorted(out) == sum([[f"done-task{i}"] * k for i, k in muls], [])
