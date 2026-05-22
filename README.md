# atqo

[![pypi](https://img.shields.io/pypi/v/atqo.svg)](https://pypi.org/project/atqo/)

Async task queue orchestrator with resource-aware scheduling.

Each actor class declares what it consumes (`requirements`); the scheduler holds the pool (`resources`) and decides how many actors of each class to run. Optional per-task rate limits gate dispatch against time-windowed budgets.

## Install

```bash
uv add atqo
```

## Usage

```python
from atqo import ActorBase, Scheduler, SchedulerTask, SingleCPUActor

class Scraper(SingleCPUActor):
    def consume(self, url):
        return fetch(url)

class HeavyActor(ActorBase):
    requirements = {"cpu": 2, "mem": 4}
    def consume(self, arg):
        return process(arg)

scheduler = Scheduler(
    actors=[Scraper, HeavyActor],
    resources={"cpu": 8, "mem": 16},
)

scheduler.refill_task_queue(
    [SchedulerTask(u, actor=Scraper) for u in urls]
    + [SchedulerTask(j, actor=HeavyActor) for j in jobs]
)
results = scheduler.join()
```

## Rate limits

Per-task budgets recover over time (token bucket), independent of the static resource pool:

```python
from atqo import RateLimit

scheduler = Scheduler(
    actors=[Scraper],
    resources={"cpu": 4},
    rate_limits={"site_a": RateLimit(10, per_seconds=60)},
)
SchedulerTask(url, actor=Scraper, rate_costs={"site_a": 1})
```

A task whose `rate_costs` exceeds a bucket's `capacity` raises `ImpossibleRateCost` at ingress — it could never run.

## Simple parallel API

```python
from atqo import parallel_map, parallel_consume

results = parallel_map(expensive_fn, items, workers=4)
parallel_consume(MyActor, items, workers=4)
```

## Patterns

### Stateful actors (logged-in browser, warm cache, etc.)

Register a separate actor class. Its `__init__` performs the setup; the scheduler routes tasks needing that state via `actor=`.

```python
class Browser(ActorBase):
    requirements = {"browser_slot": 1}
    def __init__(self): 
        self.driver = open_browser()
    def consume(self, url): 
        return self.driver.fetch(url)

class LoggedInBrowser(Browser):
    def __init__(self):
        super().__init__()
        self.driver.login(USER, PW)

scheduler = Scheduler(
    actors=[Browser, LoggedInBrowser],
    resources={"browser_slot": 4},
)
SchedulerTask(url, actor=LoggedInBrowser)
```

## Hang protection

Every blocking wait in the scheduler is bounded. Optional knobs on `Scheduler(...)`:

- `task_timeout`: per-task wall-clock cap. Exceeded attempts fail like any exception (count against `allowed_fail_count`).
- `stall_timeout`: if no progress for this long, raise `SchedulerStalled`.
- `poison_timeout`: how long graceful actor drain waits before force-cancel (default 5s).

`cleanup()` and `join()` always terminate; they cancel listeners and stop the event loop unconditionally.
