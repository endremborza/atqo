from atqo import ActorBase, Capability, CapabilitySet, Scheduler, SchedulerTask
from atqo.distributed_apis import DEFAULT_DIST_API_KEY
from atqo.simplified_functions import BatchProd

LIMIT_DIC = {"A": 3}

cap1 = Capability({"A": 1})
cap2 = Capability({"A": 1})


class Actor(ActorBase):
    def consume(self, task_arg):
        return f"done-{task_arg}"


def test_over_actors():

    dist_sys = DEFAULT_DIST_API_KEY
    actor_dict = {
        CapabilitySet([cap2, cap1]): Actor,
    }

    scheduler = Scheduler(
        actor_dict=actor_dict,
        resource_limits=LIMIT_DIC,
        distributed_system=dist_sys,
        verbose=True,
    )

    tasks = [
        SchedulerTask("task1", requirements=[cap1]),
    ]

    out = []

    def _proc(ol):
        for e in ol:
            out.append(e)

    scheduler.process(BatchProd(tasks, 2, lambda x: x), result_processor=_proc)
    scheduler.join()

    assert out == ["done-task1"]
