from atqo import parallel_map, parallel_run


def _f(x):
    return x + 2


def test_maps():

    arr = [3, 1, 10]
    assert sorted(parallel_map(_f, arr)) == sorted(parallel_run(_f, arr))
