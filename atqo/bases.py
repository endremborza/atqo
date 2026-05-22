import logging
from abc import ABC, abstractmethod
from asyncio import Future
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from .core import SchedulerTask  # pragma: no cover


class ActorBase(ABC):
    requirements: ClassVar[dict[str, int]] = {}

    @abstractmethod
    def consume(self, task_arg):
        pass  # pragma: no cover

    def stop(self):
        pass  # pragma: no cover

    def _log(self, msg, **kwargs):
        logging.getLogger(f"atqo.{type(self).__name__}").info(
            f"{msg} {kwargs}" if kwargs else msg
        )


class SingleCPUActor(ActorBase):
    requirements: ClassVar[dict[str, int]] = {"cpu": 1}

    def consume(self, task_arg):  # pragma: no cover
        raise NotImplementedError


class DistAPIBase(ABC):
    @property
    def exception(self):
        return Exception

    def join(self):
        pass

    @staticmethod
    def kill(actor: ActorBase):
        actor.stop()

    @staticmethod
    def get_running_actor(actor_creator) -> ActorBase:
        return actor_creator()

    @staticmethod
    def get_future(actor: ActorBase, next_task: "SchedulerTask") -> Future:
        f = Future()
        f.set_result(actor.consume(next_task.argument))
        return f

    @staticmethod
    def parse_exception(e):
        return e
