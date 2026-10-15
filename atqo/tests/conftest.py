import asyncio

from atqo.bases import DistAPIBase


class ThreadedAPI(DistAPIBase):
    """Test-only backend: runs ``consume`` in a worker thread so blocking
    actors give real concurrency without subprocess startup cost."""

    @staticmethod
    def get_future(actor, next_task):
        return asyncio.ensure_future(
            asyncio.to_thread(actor.consume, next_task.argument)
        )
