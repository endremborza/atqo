import asyncio
import logging
from asyncio import Future
from typing import TYPE_CHECKING, Type

from structlog import get_logger

from .bases import DistAPIBase

if TYPE_CHECKING:
    from .core import ActorBase, SchedulerTask

logger = get_logger()


class RayAPI(DistAPIBase):
    def __init__(self):
        import ray
        from ray.exceptions import RayError

        self._exc_cls = RayError
        self._ray_module = ray

        ray_specs = ray.init(
            # resources=_limitset_to_ray_init(limit_set),
            log_to_driver=False,
            logging_level=logging.WARNING,
        )
        logger.info(f"ray dashboard: http://{ray_specs.get('webui_url')}")
        logger.info("launched ray with resources", **ray.cluster_resources())
        self._running = True

    @property
    def exception(self):
        return self._exc_cls

    def join(self):
        if self._running:
            self._ray_module.shutdown()
            self._running = False

    def kill(self, actor):
        self._ray_module.wait([actor.stop.remote()])
        self._ray_module.kill(actor)

    def get_running_actor(self, actor_cls: Type["ActorBase"]) -> "ActorBase":

        # ray should get the resources here...

        return self._ray_module.remote(actor_cls).remote()

    @staticmethod
    def get_future(actor, next_task: "SchedulerTask") -> Future:
        return asyncio.wrap_future(
            actor.consume.remote(next_task.argument).future()
        )

    @staticmethod
    def parse_exception(e):
        # return e.cause_cls(e.traceback_str.strip().split("\n")[-1])
        return e


class SyncAPI(DistAPIBase):
    pass


DIST_API_MAP = {"sync": SyncAPI, "ray": RayAPI}


def get_dist_api(key) -> "DistAPIBase":
    try:
        return DIST_API_MAP[key]
    except KeyError:
        logger.warning(
            f"unknown distributed system: {key}, defaulting to sync api"
        )
        return SyncAPI
