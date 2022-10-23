from dataclasses import dataclass
from itertools import product
from pathlib import Path
from random import Random, random
from time import sleep

import pytest

from atqo import get_lock, parallel_map
from atqo.distributed_apis import DIST_API_MAP


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


@pytest.mark.parametrize(
    ["size", "nfiles", "dkey"],
    product([5, 20], [2, 10], DIST_API_MAP.keys()),
)
def test_para_io(tmp_path, size, nfiles, dkey):

    rng = Random(120)

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    data_files = [data_dir / f"file-{i}" for i in range(nfiles)]
    for df in data_files:
        df.write_text("0")

    args = [*product(data_files, range(size))]
    rng.shuffle(args)

    parallel_map(
        write,
        map(IoArg.from_args, args),
        dkey,
    )

    for fp in data_files:
        assert int(fp.read_text()) == sum(range(size))
