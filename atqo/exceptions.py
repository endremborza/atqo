from tblib import Traceback


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


class DistantException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        self.e: Exception = args[0]
        self.tb: Traceback = args[1]
