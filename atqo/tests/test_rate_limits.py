import time

import pytest

from atqo import (
    RateLimit,
    Scheduler,
    SchedulerTask,
    SingleCPUActor,
)
from atqo.rate import RateGate


class Echo(SingleCPUActor):
    def consume(self, x):
        return x


def test_rate_gate_grants_burst():
    gate = RateGate({"r": RateLimit(3, per_seconds=10)})
    assert gate.try_consume({"r": 1}) == 0
    assert gate.try_consume({"r": 1}) == 0
    assert gate.try_consume({"r": 1}) == 0
    wait = gate.try_consume({"r": 1})
    assert wait > 0


def test_rate_gate_validate_cost():
    gate = RateGate({"r": RateLimit(3, per_seconds=10)})
    with pytest.raises(Exception):
        gate.validate_cost({"unknown": 1})
    with pytest.raises(Exception):
        gate.validate_cost({"r": 99})


def test_rate_gate_refills():
    gate = RateGate({"r": RateLimit(2, per_seconds=0.2)})
    gate.try_consume({"r": 2})
    time.sleep(0.25)
    assert gate.try_consume({"r": 2}) == 0


def test_rate_gate_multi_bucket_max_wait():
    gate = RateGate(
        {
            "fast": RateLimit(10, per_seconds=1.0),
            "slow": RateLimit(1, per_seconds=2.0),
        }
    )
    gate.try_consume({"fast": 1, "slow": 1})
    wait = gate.try_consume({"fast": 1, "slow": 1})
    assert wait > 1.0


def test_scheduler_paces_via_rate_limit():
    with Scheduler(
        actors=[Echo],
        resources={"cpu": 1},
        rate_limits={"r": RateLimit(2, per_seconds=0.4)},
    ) as sch:
        tasks = [SchedulerTask(i, actor=Echo, rate_costs={"r": 1}) for i in range(5)]
        start = time.monotonic()
        sch.refill_task_queue(tasks)
        out = sch.join()
        elapsed = time.monotonic() - start
    assert sorted(out) == [0, 1, 2, 3, 4]
    assert elapsed >= 0.5
    assert elapsed < 5.0


def test_zero_rate_cost_skipped():
    with Scheduler(
        actors=[Echo],
        resources={"cpu": 1},
        rate_limits={"r": RateLimit(1, per_seconds=10.0)},
    ) as sch:
        sch.refill_task_queue([SchedulerTask(1, actor=Echo)])
        out = sch.join()
    assert out == [1]
