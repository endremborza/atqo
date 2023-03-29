from atqo import parallel_map


def fun(x):
    return x + 1  # pragma: no cover


def test_large_launch():
    # TODO: this is way too slow
    dist_api = "mp"
    out_l = sorted(parallel_map(fun, range(50), dist_api, workers=40))
    assert out_l == list(range(1, 51))
