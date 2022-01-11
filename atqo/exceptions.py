class UnexpectedCapabilities(Exception):
    pass


class ActorListenBreaker(Exception):
    pass


class ActorPoisoned(ActorListenBreaker):
    pass


class NotEnoughResourcesToContinue(Exception):
    pass
