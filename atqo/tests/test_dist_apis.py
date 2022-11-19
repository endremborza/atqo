from atqo.distributed_apis import get_dist_api


def test_get_dist_api():
    get_dist_api("wrong")
