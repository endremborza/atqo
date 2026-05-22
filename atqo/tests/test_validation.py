from functools import partial

import pytest

from atqo import (
    ActorBase,
    NotEnoughResources,
    RateLimit,
    Scheduler,
    SchedulerTask,
    SingleCPUActor,
    UnknownActor,
    UnknownRateLimit,
    UnknownResource,
)
from atqo.exceptions import ImpossibleRateCost


class GoodActor(SingleCPUActor):
    def consume(self, x):
        return x


class BadResActor(ActorBase):
    requirements = {"gpu": 1}

    def consume(self, x):  # pragma: no cover
        return x


class TooBigActor(ActorBase):
    requirements = {"cpu": 10}

    def consume(self, x):  # pragma: no cover
        return x


class NegReqActor(ActorBase):
    requirements = {"cpu": 0}

    def consume(self, x):  # pragma: no cover
        return x


def test_unknown_resource_at_construction():
    with pytest.raises(UnknownResource):
        Scheduler(actors=[BadResActor], resources={"cpu": 1})


def test_requirement_exceeds_pool_at_construction():
    with pytest.raises(NotEnoughResources):
        Scheduler(actors=[TooBigActor], resources={"cpu": 1})


def test_zero_requirement_rejected():
    with pytest.raises(ValueError):
        Scheduler(actors=[NegReqActor], resources={"cpu": 1})


def test_non_actor_class_rejected():
    class NotAnActor:
        pass

    with pytest.raises(TypeError):
        Scheduler(actors=[NotAnActor], resources={"cpu": 1})


def test_duplicate_registration_rejected():
    with pytest.raises(ValueError):
        Scheduler(actors=[GoodActor, GoodActor], resources={"cpu": 2})


def test_partial_dedup_against_class():
    with pytest.raises(ValueError):
        Scheduler(actors=[GoodActor, partial(GoodActor)], resources={"cpu": 2})


def test_default_resources_used():
    with Scheduler(actors=[GoodActor]) as sch:
        assert "cpu" in sch._resources


def test_unknown_actor_on_task():
    class Other(SingleCPUActor):
        def consume(self, x):  # pragma: no cover
            return x

    with Scheduler(actors=[GoodActor], resources={"cpu": 1}) as sch:
        with pytest.raises(UnknownActor):
            sch.refill_task_queue([SchedulerTask(1, actor=Other)])


def test_actor_none_with_multiple_classes():
    class B(SingleCPUActor):
        def consume(self, x):  # pragma: no cover
            return x

    with Scheduler(actors=[GoodActor, B], resources={"cpu": 2}) as sch:
        with pytest.raises(UnknownActor):
            sch.refill_task_queue([SchedulerTask(1)])


def test_actor_none_with_single_class_autobinds():
    with Scheduler(actors=[GoodActor], resources={"cpu": 1}) as sch:
        sch.refill_task_queue([SchedulerTask(1)])
        out = sch.join()
    assert out == [1]


def test_unknown_rate_limit_on_task():
    with Scheduler(
        actors=[GoodActor],
        resources={"cpu": 1},
        rate_limits={"site_a": RateLimit(5, per_seconds=1.0)},
    ) as sch:
        with pytest.raises(UnknownRateLimit):
            sch.refill_task_queue(
                [SchedulerTask(1, actor=GoodActor, rate_costs={"site_b": 1})]
            )


def test_rate_cost_exceeds_capacity():
    with Scheduler(
        actors=[GoodActor],
        resources={"cpu": 1},
        rate_limits={"site_a": RateLimit(5, per_seconds=1.0)},
    ) as sch:
        with pytest.raises(ImpossibleRateCost):
            sch.refill_task_queue(
                [SchedulerTask(1, actor=GoodActor, rate_costs={"site_a": 99})]
            )


def test_rate_limit_bad_construction():
    with pytest.raises(ValueError):
        RateLimit(0, per_seconds=1.0)
    with pytest.raises(ValueError):
        RateLimit(1, per_seconds=0)
