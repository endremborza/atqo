"""Async task queue orchestrator with resource-aware scheduling."""

from .bases import ActorBase, DistAPIBase, SingleCPUActor
from .core import Scheduler, SchedulerTask
from .distributed_apis import (
    MultiProcAPI,
    SyncAPI,
    acquire_lock,
    get_lock,
)
from .exceptions import (
    ImpossibleRateCost,
    NotEnoughResources,
    NotEnoughResourcesToContinue,
    SchedulerStalled,
    UnknownActor,
    UnknownRateLimit,
    UnknownResource,
)
from .rate import RateLimit
from .simplified_functions import parallel_consume, parallel_map

__version__ = "1.0.0"

__all__ = [
    "ActorBase",
    "DistAPIBase",
    "ImpossibleRateCost",
    "MultiProcAPI",
    "NotEnoughResources",
    "NotEnoughResourcesToContinue",
    "RateLimit",
    "Scheduler",
    "SchedulerStalled",
    "SchedulerTask",
    "SingleCPUActor",
    "SyncAPI",
    "UnknownActor",
    "UnknownRateLimit",
    "UnknownResource",
    "acquire_lock",
    "get_lock",
    "parallel_consume",
    "parallel_map",
]
