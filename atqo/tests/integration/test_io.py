from dataclasses import dataclass
from pathlib import Path
from random import Random, random
from time import sleep

import pytest

from atqo import get_lock, parallel_map
from atqo.distributed_apis import MultiProcAPI, SyncAPI


@dataclass
class IoArg:
    path: Path
    add: int

    @classmethod
    def from_args(cls, args):
        return cls(*args)


def write(arg: IoArg):
    with get_lock(arg.path.as_posix()):
        i = int(arg.path.read_text())
        sleep(random() / 5000)
        arg.path.write_text(str(i + arg.add))


@pytest.mark.parametrize("dist_api", [SyncAPI, MultiProcAPI])
@pytest.mark.parametrize("size,nfiles", [(5, 2), (10, 3)])
def test_para_io(tmp_path: Path, size, nfiles, dist_api):
    rng = Random(120)

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    data_files = [data_dir / f"file-{i}" for i in range(nfiles)]
    for df in data_files:
        df.write_text("0")

    args = [(f, a) for f in data_files for a in range(size)]
    rng.shuffle(args)

    list(parallel_map(write, map(IoArg.from_args, args), dist_api=dist_api))

    for fp in data_files:
        assert int(fp.read_text()) == sum(range(size))
