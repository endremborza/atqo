## v2.1.0

- Added `ActorSlot` to register several resource profiles (and init kwargs) for one actor class under a single `Scheduler`, addressable by name from `SchedulerTask(actor=...)`.
- Multiproc workers that die mid-task (segfault, OOM, kill) without answering now surface as a failure instead of hanging `join()`/`stop()` indefinitely; the parent also kills workers if it dies (Linux).
- Dropped the `tblib` runtime dependency: `atqo` now has zero runtime dependencies. Worker tracebacks are shipped as text and attached as `__cause__` via a `RemoteTraceback` instead of a `tblib.Traceback` object on `DistantException`.

---

## v2.0.0

cleaner rated, focused
