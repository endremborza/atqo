import traceback


class ActorListenBreaker(Exception):
    pass


class ActorPoisoned(ActorListenBreaker):
    pass


class NotEnoughResources(Exception):
    pass


class NotEnoughResourcesToContinue(NotEnoughResources):
    pass


class UnknownResource(Exception):
    pass


class UnknownRateLimit(Exception):
    pass


class ImpossibleRateCost(Exception):
    pass


class UnknownActor(Exception):
    pass


class SchedulerStalled(Exception):
    pass


class RemoteTraceback(Exception):
    pass


class DistantException(Exception):
    """Ships a worker exception across the process boundary with its traceback
    as text, since traceback objects do not pickle."""

    def __init__(self, e: Exception, tb: str) -> None:
        super().__init__(e, tb)
        self.e = e
        self.tb = tb

    @classmethod
    def wrap(cls, e: Exception) -> "DistantException":  # pragma: no cover
        return cls(e, "".join(traceback.format_exception(e)))

    def unwrap(self) -> Exception:
        self.e.__cause__ = RemoteTraceback(f'\n"""\n{self.tb}"""')
        return self.e
