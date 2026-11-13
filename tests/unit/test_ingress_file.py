"""A file entry that has not changed is not put again."""

import asyncio
import os
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


@pytest.mark.asyncio
async def test_an_edit_that_keeps_the_modification_time_and_size_is_applied(
    tmp_path: Path,
):
    """A coarse file system clock gives two quick saves one time. The new bytes are
    still read within a poll."""
    path = tmp_path / "params.yaml"
    path.write_text("Shop.arrival.orders.rate: 1.0\n", encoding="utf-8")
    changes: queue.Queue = queue.Queue()
    task = asyncio.create_task(FileIngress(path, poll=0.05).run(changes))
    await asyncio.sleep(0.3)
    first = path.stat().st_mtime_ns
    path.write_text("Shop.arrival.orders.rate: 2.0\n", encoding="utf-8")
    os.utime(path, ns=(first, first))
    await asyncio.sleep(0.5)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    values = [changes.get_nowait()[1] for _ in range(changes.qsize())]
    assert values == [1.0, 2.0]
