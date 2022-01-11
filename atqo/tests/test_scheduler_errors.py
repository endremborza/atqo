from atqo.core import Scheduler


def test_empty_scheduler():
    scheduler = Scheduler({}, {})
    assert scheduler.is_idle
    assert scheduler.is_empty
    assert scheduler.queued_task_count == 0
