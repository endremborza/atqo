from functools import partial

from atqo import (
    ActorBase,
    Scheduler,
    SchedulerTask,
    SingleCPUActor,
    SyncAPI,
    UnknownActor,
)
from atqo.exceptions import ActorListenBreaker


class Echo(SingleCPUActor):
    def consume(self, x):
        return f"echo-{x}"


class Adder(SingleCPUActor):
    def __init__(self, base=0):
        self.base = base

    def consume(self, x):
        return self.base + x


class HeavyActor(ActorBase):
    requirements = {"cpu": 2, "mem": 1}

    def consume(self, x):
        return ("heavy", x)


def test_empty_scheduler():
    with Scheduler(actors=[], resources={}) as sch:
        assert sch.is_idle
        assert sch.is_empty
        assert sch.queued_task_count == 0


def test_single_actor_single_task():
    with Scheduler(actors=[Echo], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([SchedulerTask("hello")])
        out = sch.join()
    assert out == ["echo-hello"]


def test_process_with_list():
    with Scheduler(actors=[Echo], resources={"cpu": 2}) as sch:
        out = list(sch.process([SchedulerTask(i, actor=Echo) for i in range(5)]))
    assert sorted(out) == sorted([f"echo-{i}" for i in range(5)])


def test_process_with_batch_producer():
    with Scheduler(actors=[Echo], resources={"cpu": 2}) as sch:
        items = list(range(8))

        class Producer:
            def __init__(self):
                self.i = 0

            def __call__(self):
                if self.i >= len(items):
                    return []
                chunk = items[self.i : self.i + 3]
                self.i += 3
                return [SchedulerTask(x, actor=Echo) for x in chunk]

        out = list(sch.process(Producer()))
    assert sorted(out) == sorted(f"echo-{i}" for i in range(8))


def test_partial_actor_with_init_arg():
    with Scheduler(actors=[partial(Adder, base=100)], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([SchedulerTask(i) for i in range(5)])
        out = sorted(sch.join())
    assert out == [100, 101, 102, 103, 104]


def test_multi_actor_routing():
    with Scheduler(
        actors=[Echo, HeavyActor],
        resources={"cpu": 4, "mem": 4},
    ) as sch:
        sch.refill_task_queue(
            [
                SchedulerTask("a", actor=Echo),
                SchedulerTask("b", actor=HeavyActor),
                SchedulerTask("c", actor=Echo),
            ]
        )
        out = sorted(sch.join(), key=str)
    assert out == [("heavy", "b"), "echo-a", "echo-c"]


def test_task_with_no_registered_actors_rejected():
    import pytest

    with Scheduler(actors=[], resources={"cpu": 1}) as sch:
        with pytest.raises(UnknownActor):
            list(sch.process([SchedulerTask(1)]))


def test_user_exception_surfaced_after_retries():
    class Boom(SingleCPUActor):
        def consume(self, x):
            raise ValueError(f"bad: {x}")

    with Scheduler(
        actors=[Boom], resources={"cpu": 1}, distributed_system=SyncAPI
    ) as sch:
        sch.refill_task_queue([SchedulerTask(1, actor=Boom, allowed_fail_count=0)])
        out = sch.join()
    assert len(out) == 1
    assert isinstance(out[0], ValueError)


def test_listen_breaker_restarts_actor():
    state = {"calls": 0}

    class Flaky(SingleCPUActor):
        def consume(self, x):
            state["calls"] += 1
            if state["calls"] == 1:
                raise ActorListenBreaker("once")
            return x

    with Scheduler(actors=[Flaky], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([SchedulerTask(7, actor=Flaky)])
        out = sch.join()
    assert out == [7]
    assert state["calls"] >= 2
