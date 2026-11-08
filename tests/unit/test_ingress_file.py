"""A file entry that has not changed is not put again."""

import queue
from pathlib import Path

from desflow.connectors.ingress.file import FileIngress


def test_an_unchanged_nan_entry_is_put_once(tmp_path: Path):
    ingress = FileIngress(tmp_path / "params.yaml")
    puts: queue.Queue = queue.Queue()
    first = {"S.variables.x": float("nan"), "S.variables.y": 1}
    second = {"S.variables.x": float("nan"), "S.variables.y": 2}

    ingress._put_changes(first, {}, puts)
    ingress._put_changes(second, first, puts)

    paths = []
    while not puts.empty():
        paths.append(puts.get()[0])
    assert paths == ["S.variables.x", "S.variables.y", "S.variables.y"]
