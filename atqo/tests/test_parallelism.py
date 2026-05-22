"""Wall-clock tests that the scheduler actually parallelizes work and that
producer-pump backpressure overlaps refill with execution.

Uses a test-local ThreadedAPI: SyncAPI runs consume on the event-loop
thread (sleep blocks the loop, no real concurrency), and MultiProcAPI
spends ~100ms+ on process startup per actor. asyncio.to_thread gives us
real concurrency at near-zero overhead.

Each sleep (0.1s) is well above scheduling noise; thresholds leave a
comfortable margin for the broken-vs-correct gap.
"""

import asyncio
import time

from atqo import ActorBase, RateLimit, Scheduler, SchedulerTask, SingleCPUActor
from atqo.bases import DistAPIBase

SLEEP = 0.1


class ThreadedAPI(DistAPIBase):
    @staticmethod
    def get_future(actor, next_task):
        return asyncio.ensure_future(
            asyncio.to_thread(actor.consume, next_task.argument)
        )


class Slow(SingleCPUActor):
    def consume(self, x):
        time.sleep(SLEEP)
        return x


def _scheduler(cpu):
    return Scheduler(
        actors=[Slow],
        resources={"cpu": cpu},
        distributed_system=ThreadedAPI,
    )


def _time(fn):
    start = time.monotonic()
    out = fn()
    return out, time.monotonic() - start


def test_workers_run_in_parallel():
    """4 tasks at 0.1s on cpu=2 → ~0.2s, definitely not ~0.4s (serial)."""
    n = 4
    with _scheduler(cpu=2) as sch:
        sch.refill_task_queue([SchedulerTask(i) for i in range(n)])
        out, elapsed = _time(sch.join)
    assert sorted(out) == list(range(n))
    assert elapsed < 0.3, (
        f"workers not parallel: {elapsed:.3f}s for {n} × {SLEEP}s on cpu=2 "
        f"(parallel ≈ {(n // 2) * SLEEP:.2f}s, serial ≈ {n * SLEEP:.2f}s)"
    )


def test_process_overlaps_refill_with_workers():
    """5 then 3 tasks via process(), cpu=2, 0.1s each.

    Perfect overlap:    ceil(8/2) × 0.1 = 0.4s
    Drain-then-refill:  3×0.1 + 2×0.1   = 0.5s   (broken backpressure)
    """
    batches = iter(
        [
            [SchedulerTask(i) for i in range(5)],
            [SchedulerTask(i) for i in range(5, 8)],
        ]
    )

    def producer():
        return next(batches, [])

    with _scheduler(cpu=2) as sch:
        out, elapsed = _time(lambda: list(sch.process(producer, min_queue_size=2)))
    assert sorted(out) == list(range(8))
    assert elapsed < 0.45, (
        f"refill not overlapping with workers: {elapsed:.3f}s; "
        f"expected ≈0.4s with overlap, 0.5s if drain-then-refill"
    )


def test_workers_stay_busy_across_batches():
    """Pumped in 4 batches of 2; cpu=2; 0.1s each.

    Workers should never go idle between batches. 8 tasks ÷ 2 workers ×
    0.1s = 0.4s lower bound; anything ≥0.6s means meaningful idle gaps.
    """
    batches = iter([[SchedulerTask(i + 2 * b) for i in range(2)] for b in range(4)])

    def producer():
        return next(batches, [])

    with _scheduler(cpu=2) as sch:
        out, elapsed = _time(lambda: list(sch.process(producer, min_queue_size=1)))
    assert sorted(out) == list(range(8))
    assert elapsed < 0.55, (
        f"workers idle between batches: {elapsed:.3f}s for 4 batches of 2 "
        f"× {SLEEP}s on cpu=2 (target ≈0.4s)"
    )


def test_more_workers_runs_faster():
    """Same workload, double the workers ≈ half the wall time."""
    n = 8

    def make_tasks():
        return [SchedulerTask(i) for i in range(n)]

    with _scheduler(cpu=1) as sch:
        sch.refill_task_queue(make_tasks())
        _, t1 = _time(sch.join)

    with _scheduler(cpu=2) as sch:
        sch.refill_task_queue(make_tasks())
        _, t2 = _time(sch.join)

    assert t1 / t2 > 1.7, f"cpu=2 not ≈2× faster than cpu=1: {t1:.3f}s vs {t2:.3f}s"


def test_multi_actor_classes_run_in_parallel():
    """Two actor classes with disjoint resource pools should process
    concurrently, not serialized.

    1 cpu-bound worker + 1 mem-bound worker. 2 tasks of each type.
    Each task takes 0.1s; each actor processes its 2 tasks serially.
    If the two actor sets actually run in parallel: ≈0.2s.
    If serialized somehow: ≈0.4s.
    """

    class CpuSlow(ActorBase):
        requirements = {"cpu": 1}

        def consume(self, x):
            time.sleep(SLEEP)
            return ("cpu", x)

    class MemSlow(ActorBase):
        requirements = {"mem": 1}

        def consume(self, x):
            time.sleep(SLEEP)
            return ("mem", x)

    with Scheduler(
        actors=[CpuSlow, MemSlow],
        resources={"cpu": 1, "mem": 1},
        distributed_system=ThreadedAPI,
    ) as sch:
        tasks = [SchedulerTask(i, actor=CpuSlow) for i in range(2)] + [
            SchedulerTask(i, actor=MemSlow) for i in range(2)
        ]
        sch.refill_task_queue(tasks)
        out, elapsed = _time(sch.join)

    assert sorted(out) == [("cpu", 0), ("cpu", 1), ("mem", 0), ("mem", 1)]
    assert elapsed < 0.3, (
        f"actor classes not running in parallel: {elapsed:.3f}s "
        f"(parallel ≈0.2s, serialized ≈0.4s)"
    )


def test_rate_limit_sustained_throughput():
    """With workers >> rate, the rate-limit (not workers) is the bottleneck;
    observed wall-time should match the configured rate.

    capacity=5, refill 5 tokens / 0.5s → sustained 10 tokens/s.
    20 tasks, each cost 1, near-instant consume. Expected:
      - first 5 burst-consume the bucket → t ≈ 0
      - remaining 15 paced at 10/s        → t ≈ 1.5s
    Floor 1.2s ensures the gate is actually pacing; ceiling 2.0s leaves
    margin for scheduling overhead.
    """

    class Instant(SingleCPUActor):
        def consume(self, x):
            return x

    n = 20
    with Scheduler(
        actors=[Instant],
        resources={"cpu": 4},
        rate_limits={"r": RateLimit(5, per_seconds=0.5)},
        distributed_system=ThreadedAPI,
    ) as sch:
        sch.refill_task_queue([SchedulerTask(i, rate_costs={"r": 1}) for i in range(n)])
        out, elapsed = _time(sch.join)

    assert sorted(out) == list(range(n))
    assert 1.2 < elapsed < 2.0, (
        f"rate throughput off: {elapsed:.3f}s for {n} tasks @ 10/s (expected ≈1.5s)"
    )
