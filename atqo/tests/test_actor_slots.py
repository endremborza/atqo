"""One ActorBase subclass backing several scheduler slots, each with its own
init kwargs and resource profile — the case that used to force a distinct
subclass per profile."""

import time

import pytest

from atqo import ActorBase, ActorSlot, Scheduler, SchedulerTask
from atqo.exceptions import NotEnoughResources, UnknownActor

from .conftest import ThreadedAPI


class Tagger(ActorBase):
    requirements = {"cpu": 1}

    def __init__(self, tag="?"):
        self.tag = tag

    def consume(self, x):
        return (self.tag, x)


def test_two_slots_same_class_distinct_kwargs():
    with Scheduler(
        actors=[
            ActorSlot(Tagger, name="a", kwargs={"tag": "A"}),
            ActorSlot(Tagger, name="b", kwargs={"tag": "B"}),
        ],
        resources={"cpu": 2},
    ) as sch:
        sch.refill_task_queue(
            [SchedulerTask(1, actor="a"), SchedulerTask(2, actor="b")]
        )
        out = sorted(sch.join())
    assert out == [("A", 1), ("B", 2)]


def test_slot_object_as_task_actor():
    slot = ActorSlot(Tagger, name="a", kwargs={"tag": "A"})
    with Scheduler(actors=[slot], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([SchedulerTask(9, actor=slot)])
        out = sch.join()
    assert out == [("A", 9)]


def test_slot_requirements_override_class():
    """Per-slot requirements override the class default and are validated
    against the pool independently."""
    with pytest.raises(NotEnoughResources):
        Scheduler(
            actors=[ActorSlot(Tagger, name="big", requirements={"cpu": 5})],
            resources={"cpu": 2},
        )


def test_slots_with_disjoint_pools_run_in_parallel():
    """Two slots of one class on disjoint resource pools process concurrently."""

    class Slow(ActorBase):
        requirements = {"cpu": 1}

        def __init__(self, tag="?"):
            self.tag = tag

        def consume(self, x):
            time.sleep(0.1)
            return (self.tag, x)

    with Scheduler(
        actors=[
            ActorSlot(Slow, name="cpu", kwargs={"tag": "c"}, requirements={"cpu": 1}),
            ActorSlot(Slow, name="mem", kwargs={"tag": "m"}, requirements={"mem": 1}),
        ],
        resources={"cpu": 1, "mem": 1},
        distributed_system=ThreadedAPI,
    ) as sch:
        tasks = [SchedulerTask(i, actor="cpu") for i in range(2)] + [
            SchedulerTask(i, actor="mem") for i in range(2)
        ]
        sch.refill_task_queue(tasks)
        start = time.monotonic()
        out = sch.join()
        elapsed = time.monotonic() - start
    assert sorted(out) == [("c", 0), ("c", 1), ("m", 0), ("m", 1)]
    assert elapsed < 0.3, f"slots serialized: {elapsed:.3f}s (parallel ≈0.2s)"


def test_duplicate_slot_name_rejected():
    with pytest.raises(ValueError):
        Scheduler(
            actors=[
                ActorSlot(Tagger, name="dup"),
                ActorSlot(Tagger, name="dup"),
            ],
            resources={"cpu": 2},
        )


def test_default_slot_name_is_class_name():
    with Scheduler(actors=[ActorSlot(Tagger)], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([SchedulerTask(1, actor="Tagger")])
        out = sch.join()
    assert out == [("?", 1)]


def test_unknown_slot_name_rejected():
    with Scheduler(actors=[ActorSlot(Tagger, name="a")], resources={"cpu": 1}) as sch:
        with pytest.raises(UnknownActor):
            sch.refill_task_queue([SchedulerTask(1, actor="nope")])
