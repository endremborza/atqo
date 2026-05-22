from atqo.bases import DistAPIBase
from atqo.distributed_apis import MultiProcAPI, SyncAPI


def test_api_types():
    assert issubclass(SyncAPI, DistAPIBase)
    assert issubclass(MultiProcAPI, DistAPIBase)
