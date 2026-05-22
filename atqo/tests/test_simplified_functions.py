import pytest

from atqo import parallel_map
from atqo.distributed_apis import SyncAPI


def add2(x):
    return x + 2


def div(x):
    return 10 / x


def test_parallel_map_sync():
    out = list(parallel_map(add2, range(5), dist_api=SyncAPI))
    assert sorted(out) == [2, 3, 4, 5, 6]


def test_parallel_map_with_errors_collected():
    out = list(
        parallel_map(
            div,
            [1, 2, 0, 5],
            dist_api=SyncAPI,
            raise_errors=False,
            allowed_fail_count=0,
        )
    )
    ok = sorted(x for x in out if not isinstance(x, Exception))
    errs = [x for x in out if isinstance(x, Exception)]
    assert ok == [2.0, 5.0, 10.0]
    assert len(errs) == 1


def test_parallel_map_raises_on_error():
    with pytest.raises(ZeroDivisionError):
        list(
            parallel_map(
                div,
                [1, 0, 5],
                dist_api=SyncAPI,
                allowed_fail_count=0,
            )
        )


def test_parallel_map_generator_input():
    def g():
        for i in range(10):
            yield i

    out = sorted(parallel_map(add2, g(), dist_api=SyncAPI))
    assert out == list(range(2, 12))
