"""A file entry that has not changed is not put again."""

import queue
from pathlib import Path

import pytest

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


def test_a_poll_that_is_not_a_positive_number_is_refused(tmp_path: Path):
    """The constructor refuses a `poll` of 0. `checked_poll` has its own test, so one
    refused value shows that the constructor calls it."""
    with pytest.raises(ValueError, match="poll is a number of seconds above 0"):
        FileIngress(tmp_path / "params.yaml", poll=0)
