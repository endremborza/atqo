"""Targeted tests for branches that aren't naturally exercised by the
behaviour-focused suites. Anything truly unreachable is marked ``# pragma:
no cover`` in source."""

import threading
import time
from functools import partial

import pytest

from atqo import (
    ActorBase,
    MultiProcAPI,
    RateLimit,
    Scheduler,
    SchedulerStalled,
    SchedulerTask,
    SingleCPUActor,
    SyncAPI,
)
from atqo.exchange import NumStore, ResourceExchange
from atqo.rate import RateGate


class Echo(SingleCPUActor):
    def consume(self, x):
        return x


class _SlowForTimeout(SingleCPUActor):
    def consume(self, x):
        time.sleep(2.0)
        return x


class _BadInit(SingleCPUActor):
    def __init__(self):
        raise RuntimeError("init failed in subprocess")

    def consume(self, x):  # pragma: no cover
        return x


class _Boom(SingleCPUActor):
    def consume(self, x):
        raise ValueError(f"boom: {x}")


class _MPWedger(SingleCPUActor):
    """Wedges only the subprocess (not the main loop). MultiProc only."""

    def consume(self, x):
        time.sleep(60)
        return x


class _AlwaysTimeout(SingleCPUActor):
    def consume(self, x):
        time.sleep(2.0)
        return x


def test_actor_base_log_emits():
    a = Echo()
    a._log("hello", k=1)
    a._log("no kwargs")


def test_numstore_from_numstore():
    inner = NumStore({"a": 1, "b": 2})
    outer = NumStore(inner)
    assert outer.base_dict == {"a": 1, "b": 2}


def test_numstore_eq():
    a = NumStore({"x": 1})
    assert a == NumStore({"x": 1})
    assert not (a == NumStore({"x": 2}))


def test_resource_exchange_repr():
    cex = ResourceExchange([Echo], {"cpu": 4})
    s = repr(cex)
    assert "ResourceExchange" in s
    assert "Echo" in s


def test_resource_exchange_empty_requirements():
    class Free(ActorBase):
        requirements = {}

        def consume(self, x):
            return x

    cex = ResourceExchange([Free], {"cpu": 4})
    out = cex.set_values({Free: 5})
    assert out[Free] >= 1


def test_actor_with_empty_requirements_runs():
    class Free(ActorBase):
        requirements = {}

        def consume(self, x):
            return ("free", x)

    with Scheduler(actors=[Free], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([SchedulerTask(1, actor=Free)])
        out = sch.join()
    assert out == [("free", 1)]


def test_rate_gate_empty_costs_returns_zero():
    gate = RateGate({"r": RateLimit(2, per_seconds=1.0)})
    assert gate.try_consume({}) == 0.0


def test_rate_gate_negative_cost_rejected():
    gate = RateGate({"r": RateLimit(2, per_seconds=1.0)})
    with pytest.raises(ValueError):
        gate.validate_cost({"r": 0})
    with pytest.raises(ValueError):
        gate.validate_cost({"r": -1})


def test_refill_with_empty_list_noop():
    with Scheduler(actors=[Echo], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([])
        assert sch.queued_task_count == 0


def test_partial_actor_on_task():
    with Scheduler(actors=[Echo], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([SchedulerTask(1, actor=partial(Echo))])
        out = sch.join()
    assert out == [1]


def test_verbose_logging_paths(caplog):
    import logging

    caplog.set_level(logging.INFO, logger="atqo.scheduler")
    caplog.set_level(logging.INFO, logger="atqo.actor_set")
    with Scheduler(actors=[Echo], resources={"cpu": 1}, verbose=True) as sch:
        sch.refill_task_queue([SchedulerTask(1)])
        sch.join()
    assert any("atqo" in r.name for r in caplog.records)


def test_actor_set_repr():
    with Scheduler(actors=[Echo], resources={"cpu": 1}) as sch:
        aset = sch._actor_sets[Echo]
        s = repr(aset)
    assert "ActorSet" in s and "Echo" in s


def test_scheduler_task_repr():
    t = SchedulerTask("x", actor=Echo)
    assert "Echo" in repr(t)
    t2 = SchedulerTask("y")
    assert "None" in repr(t2)


def test_task_retried_then_succeeds(caplog):
    """Covers the retry branch in _process_task and ActorSet._log on
    remote-error (verbose=True wires up the debug log)."""
    import logging

    caplog.set_level(logging.INFO, logger="atqo.actor_set")
    state = {"n": 0}

    class Flaky(SingleCPUActor):
        def consume(self, x):
            state["n"] += 1
            if state["n"] == 1:
                raise ValueError("first call fails")
            return x

    with Scheduler(
        actors=[Flaky],
        resources={"cpu": 1},
        distributed_system=SyncAPI,
        verbose=True,
    ) as sch:
        sch.refill_task_queue([SchedulerTask(42, actor=Flaky, allowed_fail_count=2)])
        out = sch.join()
    assert out == [42]
    assert state["n"] == 2
    assert any("remote task error" in r.getMessage() for r in caplog.records)


@pytest.mark.timeout(15)
def test_task_timeout_fires_and_kills_actor():
    with Scheduler(
        actors=[_SlowForTimeout],
        resources={"cpu": 1},
        distributed_system=MultiProcAPI,
        task_timeout=0.2,
    ) as sch:
        sch.refill_task_queue(
            [SchedulerTask(1, actor=_SlowForTimeout, allowed_fail_count=0)]
        )
        out = sch.join()
    assert len(out) == 1
    assert isinstance(out[0], TimeoutError)


@pytest.mark.timeout(20)
def test_multiproc_init_failure_propagates():
    with Scheduler(
        actors=[_BadInit],
        resources={"cpu": 1},
        distributed_system=MultiProcAPI,
    ) as sch:
        with pytest.raises(RuntimeError):
            sch.refill_task_queue([SchedulerTask(1, actor=_BadInit)])


@pytest.mark.timeout(20)
def test_task_timeout_retried_then_fails():
    """First timeout retries (fail_count <= max_fails), second exhausts."""
    with Scheduler(
        actors=[_AlwaysTimeout],
        resources={"cpu": 1},
        distributed_system=MultiProcAPI,
        task_timeout=0.2,
    ) as sch:
        sch.refill_task_queue(
            [SchedulerTask(1, actor=_AlwaysTimeout, allowed_fail_count=1)]
        )
        out = sch.join()
    assert len(out) == 1
    assert isinstance(out[0], TimeoutError)


def test_drain_to_with_busy_actor_directly():
    """Exercise drain_to's force-cancel path against a listener that's
    awaiting an unfulfillable future, without going through MultiProcAPI's
    stop sequence (which itself can hang on subprocess-pool shutdown).

    We drive ActorSet.drain_to directly with a synthetic actor whose
    ``consume`` returns a never-resolving future, then run drain_to in the
    scheduler's loop and assert it terminates."""
    import asyncio as _asyncio

    class StuckFuture(SingleCPUActor):
        def consume(self, x):  # pragma: no cover
            return x

    with Scheduler(
        actors=[StuckFuture],
        resources={"cpu": 1},
        poison_timeout=0.3,
    ) as sch:
        aset = sch._actor_sets[StuckFuture]
        # Hand-craft a listener task that will never resolve and never check
        # for cancellation cooperatively.
        never = sch._loop.create_future()

        async def _fake_listen():
            try:
                await never
            except _asyncio.CancelledError:
                return

        fut_setup = _asyncio.run_coroutine_threadsafe(
            _spawn_fake(aset, _fake_listen()), sch._loop
        )
        fut_setup.result(timeout=2)
        assert aset.running_actor_count == 1

        # Now drain — poison won't be picked up; force-cancel must fire.
        drain_fut = _asyncio.run_coroutine_threadsafe(aset.drain_to(0), sch._loop)
        drain_fut.result(timeout=3)
        assert aset.running_actor_count == 0


async def _spawn_fake(aset, coro):
    import uuid as _uuid

    name = _uuid.uuid1().hex
    task = aset._loop.create_task(coro, name=name)
    aset._actor_listening_async_task_dict[name] = task


@pytest.mark.timeout(20)
def test_multiproc_distant_exception_unwraps():
    with Scheduler(
        actors=[_Boom],
        resources={"cpu": 1},
        distributed_system=MultiProcAPI,
    ) as sch:
        sch.refill_task_queue([SchedulerTask(1, actor=_Boom, allowed_fail_count=0)])
        out = sch.join()
    assert len(out) == 1
    assert isinstance(out[0], ValueError)
    assert out[0].__traceback__ is not None


@pytest.mark.timeout(5)
def test_put_timeout_when_loop_wedged_raises_stalled():
    """When an earlier task has wedged the event loop, subsequent puts must
    fail loudly via SchedulerStalled instead of hanging forever."""

    class Wedger(SingleCPUActor):
        def consume(self, x):
            threading.Event().wait()  # pragma: no cover

    with Scheduler(
        actors=[Wedger],
        resources={"cpu": 1},
        distributed_system=SyncAPI,
        stall_timeout=0.3,
    ) as sch:
        sch.refill_task_queue([SchedulerTask(1, actor=Wedger)])
        time.sleep(0.2)  # let listener pick up task and wedge loop
        with pytest.raises(SchedulerStalled):
            sch.refill_task_queue([SchedulerTask(2, actor=Wedger)])


@pytest.mark.timeout(5)
def test_iter_after_close_returns_empty():
    sch = Scheduler(actors=[Echo], resources={"cpu": 1})
    sch.cleanup()
    assert list(sch.iter_until_n_tasks_remain(0)) == []


@pytest.mark.timeout(20)
def test_pbar_progress_bar(capsys):
    pytest.importorskip("tqdm")
    from atqo import parallel_map

    out = sorted(
        parallel_map(
            lambda x: x + 1,
            range(5),
            dist_api=SyncAPI,
            pbar=True,
        )
    )
    assert out == [1, 2, 3, 4, 5]


@pytest.mark.timeout(20)
def test_pbar_named_progress_bar():
    pytest.importorskip("tqdm")
    from atqo import parallel_map

    out = sorted(
        parallel_map(
            lambda x: x * 2,
            range(3),
            dist_api=SyncAPI,
            pbar="custom-label",
        )
    )
    assert out == [0, 2, 4]


@pytest.mark.timeout(20)
def test_pbar_with_unsized_iterable():
    """Generator inputs have no len(); get_pinger must fall back to total=None."""
    pytest.importorskip("tqdm")
    from atqo import parallel_map

    def gen():
        for i in range(4):
            yield i

    out = sorted(parallel_map(lambda x: x, gen(), dist_api=SyncAPI, pbar=True))
    assert out == [0, 1, 2, 3]


@pytest.mark.timeout(10)
def test_multi_actor_reorganize_between_results():
    """When one actor's queue drains while another still has work, the
    scheduler reorganizes mid-stream (covers the in-iter reorganize path)."""

    class A(SingleCPUActor):
        def consume(self, x):
            return ("a", x)

    class B(SingleCPUActor):
        def consume(self, x):
            return ("b", x)

    with Scheduler(actors=[A, B], resources={"cpu": 2}) as sch:
        tasks = [SchedulerTask(i, actor=A) for i in range(3)] + [
            SchedulerTask(i, actor=B) for i in range(3)
        ]
        sch.refill_task_queue(tasks)
        out = sorted(sch.join(), key=str)
    assert ("a", 0) in out and ("b", 0) in out
    assert len(out) == 6
