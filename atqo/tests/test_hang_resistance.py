"""Each test here MUST terminate. They probe the worst-case scheduler paths:
silent crashes, missing progress, impossible tasks, force-cancel."""

import time

import pytest

from atqo import (
    Scheduler,
    SchedulerStalled,
    SchedulerTask,
    SingleCPUActor,
    SyncAPI,
)


class Echo(SingleCPUActor):
    def consume(self, x):
        return x


class AlwaysFails(SingleCPUActor):
    def consume(self, x):
        raise RuntimeError(f"nope: {x}")


@pytest.mark.timeout(5)
def test_empty_join_is_instant():
    start = time.monotonic()
    with Scheduler(actors=[Echo], resources={"cpu": 1}) as sch:
        out = sch.join()
    assert out == []
    assert time.monotonic() - start < 1.5


@pytest.mark.timeout(5)
def test_all_failures_terminate():
    with Scheduler(
        actors=[AlwaysFails],
        resources={"cpu": 1},
        distributed_system=SyncAPI,
    ) as sch:
        sch.refill_task_queue(
            [SchedulerTask(i, allowed_fail_count=0) for i in range(5)]
        )
        out = sch.join()
    assert len(out) == 5
    assert all(isinstance(r, RuntimeError) for r in out)


@pytest.mark.timeout(5)
def test_stalled_scheduler_raises():
    import threading

    class Wedged(SingleCPUActor):
        def consume(self, x):
            threading.Event().wait()  # pragma: no cover

    with Scheduler(
        actors=[Wedged],
        resources={"cpu": 1},
        distributed_system=SyncAPI,
        stall_timeout=0.3,
        poll_interval=0.05,
    ) as sch:
        sch.refill_task_queue([SchedulerTask(1)])
        with pytest.raises(SchedulerStalled):
            sch.join()


@pytest.mark.timeout(10)
def test_cleanup_after_partial_processing():
    class Slow(SingleCPUActor):
        def consume(self, x):
            time.sleep(0.01)
            return x

    with Scheduler(actors=[Slow], resources={"cpu": 2}) as sch:
        sch.refill_task_queue([SchedulerTask(i, actor=Slow) for i in range(50)])
        collected = []
        for r in sch.iter_until_n_tasks_remain(0):
            collected.append(r)
            if len(collected) >= 10:
                break
    assert len(collected) >= 10


@pytest.mark.timeout(5)
def test_repeated_cleanup_idempotent():
    sch = Scheduler(actors=[Echo], resources={"cpu": 1})
    sch.cleanup()
    sch.cleanup()
    sch.cleanup()


@pytest.mark.timeout(5)
def test_join_after_join():
    with Scheduler(actors=[Echo], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([SchedulerTask(1)])
        first = sch.join()
        assert first == [1]
        second = sch.join()
        assert second == []


@pytest.mark.timeout(5)
def test_context_manager_cleans_up_on_exception():
    class Boom(SingleCPUActor):
        def consume(self, x):  # pragma: no cover
            return x

    try:
        with Scheduler(actors=[Boom], resources={"cpu": 1}) as sch:
            sch.refill_task_queue([SchedulerTask(1)])
            raise RuntimeError("user error")
    except RuntimeError:
        pass
    assert sch._closed
