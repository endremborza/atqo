import time
from dataclasses import dataclass
from threading import Lock

from .exceptions import ImpossibleRateCost, UnknownRateLimit


@dataclass(frozen=True)
class RateLimit:
    capacity: int
    per_seconds: float

    def __post_init__(self):
        if self.capacity <= 0:
            raise ValueError(f"RateLimit.capacity must be > 0, got {self.capacity}")
        if self.per_seconds <= 0:
            raise ValueError(
                f"RateLimit.per_seconds must be > 0, got {self.per_seconds}"
            )


class _Bucket:
    __slots__ = ("capacity", "refill_rate", "tokens", "last")

    def __init__(self, limit: RateLimit, now: float):
        self.capacity = float(limit.capacity)
        self.refill_rate = limit.capacity / limit.per_seconds
        self.tokens = float(limit.capacity)
        self.last = now

    def _refresh(self, now: float):
        elapsed = now - self.last
        if elapsed > 0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
            self.last = now

    def wait_for(self, cost: int, now: float) -> float:
        self._refresh(now)
        if self.tokens >= cost:
            return 0.0
        return (cost - self.tokens) / self.refill_rate

    def deduct(self, cost: int):
        self.tokens -= cost


class RateGate:
    def __init__(self, limits: dict[str, RateLimit]):
        self._limits = dict(limits)
        now = time.monotonic()
        self._buckets = {k: _Bucket(v, now) for k, v in self._limits.items()}
        self._lock = Lock()

    def validate_cost(self, costs: dict[str, int]):
        for key, cost in costs.items():
            if key not in self._limits:
                raise UnknownRateLimit(
                    f"unknown rate-limit key {key!r}; declared: {list(self._limits)}"
                )
            if cost <= 0:
                raise ValueError(f"rate_cost for {key!r} must be > 0, got {cost}")
            if cost > self._limits[key].capacity:
                raise ImpossibleRateCost(
                    f"rate_cost {cost} for {key!r} exceeds capacity "
                    f"{self._limits[key].capacity} — task could never run"
                )

    def try_consume(self, costs: dict[str, int]) -> float:
        if not costs:
            return 0.0
        with self._lock:
            now = time.monotonic()
            max_wait = 0.0
            for key, cost in costs.items():
                wait = self._buckets[key].wait_for(cost, now)
                if wait > max_wait:
                    max_wait = wait
            if max_wait > 0:
                return max_wait
            for key, cost in costs.items():
                self._buckets[key].deduct(cost)
            return 0.0
