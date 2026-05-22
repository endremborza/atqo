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


def test_refill_then_join_multi_task():
    with Scheduler(actors=[Echo], resources={"cpu": 2}) as sch:
        sch.refill_task_queue([SchedulerTask(i, actor=Echo) for i in range(5)])
        out = sch.join()
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


def test_process_refills_before_full_drain():
    """min_queue_size backpressure: producer must be re-invoked while work
    is still in flight, not only after the scheduler is fully idle.
    """
    import time

    class Slow(SingleCPUActor):
        def consume(self, x):
            time.sleep(0.02)
            return x

    sizes_seen = []
    total = 20
    batch = 10
    min_queue = 4

    with Scheduler(actors=[Slow], resources={"cpu": 2}) as sch:
        produced = {"i": 0}

        def producer():
            sizes_seen.append(sch._in_progress_or_queued)
            if produced["i"] >= total:
                return []
            chunk = list(range(produced["i"], produced["i"] + batch))
            produced["i"] += batch
            return [SchedulerTask(x, actor=Slow) for x in chunk]

        out = list(sch.process(producer, min_queue_size=min_queue))

    assert sorted(out) == list(range(total))
    # Producer must be invoked between batches while work is still in flight.
    # If backpressure is broken (only refills after full drain), every observed
    # size would be 0.
    mid_refills = [s for s in sizes_seen if s > 0]
    assert mid_refills, (
        f"backpressure broken: producer only re-called at idle ({sizes_seen})"
    )
    # And those refills should fire near min_queue_size, not far above it.
    assert max(mid_refills) <= min_queue, (
        f"refill fired too late, in_flight={mid_refills}, threshold={min_queue}"
    )


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
            sch.refill_task_queue([SchedulerTask(1)])


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
