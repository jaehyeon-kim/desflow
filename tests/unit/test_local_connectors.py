import asyncio
import logging
import queue
import subprocess
import sys

import pytest

from desflow.connectors.egress.local import ConsoleEgress
from desflow.connectors.ingress.file import FileIngress
from desflow.connectors.ingress.local import LocalIngress


@pytest.mark.asyncio
async def test_local_ingress():
    """Verify LocalIngress accurately schedules and queues updates."""
    ingress_queue = queue.Queue()
    schedule = [(0.01, "Shop.packers.capacity", 5), (0.02, "Shop.packers.capacity", 10)]
    ingress = LocalIngress(schedule)

    await ingress.run(ingress_queue)

    assert ingress_queue.qsize() == 2
    assert ingress_queue.get_nowait() == ("Shop.packers.capacity", 5)
    assert ingress_queue.get_nowait() == ("Shop.packers.capacity", 10)


@pytest.mark.asyncio
async def test_console_egress(caplog):
    """ConsoleEgress logs each batch in one call, a line per record."""
    caplog.set_level(logging.INFO)
    egress_queue = queue.Queue()
    egress = ConsoleEgress()

    batch = [
        {"stream_type": "telemetry", "path_id": "test", "value": 1},
        {"stream_type": "event", "key": "task-1", "value": {"status": "ok"}},
    ]
    egress_queue.put(batch)

    task = asyncio.create_task(egress.run(egress_queue))
    await asyncio.sleep(0.05)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    # One log call for the batch, one line per record
    messages = [
        r.getMessage() for r in caplog.records if r.name.endswith("egress.local")
    ]
    assert messages == [
        "[TEL] {'path_id': 'test', 'value': 1}\n"
        "[EVT] {'key': 'task-1', 'value': {'status': 'ok'}}"
    ]

    # Every egress provider receives the same dictionaries, so printing must not
    # take stream_type away from another sink attached to the same simulation.
    assert batch[0]["stream_type"] == "telemetry"
    assert batch[1]["stream_type"] == "event"


@pytest.mark.asyncio
async def test_console_egress_drops_reserved_keys(caplog):
    """The console prints no reserved payload key, and warns once per key."""
    caplog.set_level(logging.INFO)
    egress_queue = queue.Queue()
    record = {"stream_type": "event", "key": "orders-1", "value": {"__x__": 1, "n": 2}}
    egress_queue.put([record, record])
    task = asyncio.create_task(ConsoleEgress().run(egress_queue))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert "[EVT] {'key': 'orders-1', 'value': {'n': 2}}" in caplog.text
    assert caplog.text.count("drops the payload key '__x__'") == 1


@pytest.mark.asyncio
async def test_console_json_writes_one_line_per_record_at_any_log_level(capsys):
    """The json format writes each record as published, stream_type and all, to
    stdout, drops reserved keys as the log format does, and does not depend on the
    log level, because it is output."""
    import orjson

    logging.getLogger("desflow.connectors.egress.local").setLevel(logging.WARNING)
    egress_queue = queue.Queue()
    batch = [
        {"stream_type": "telemetry", "path_id": "test", "value": 1},
        {"stream_type": "event", "key": "orders-1", "value": {"__x__": 1, "n": 2}},
    ]
    egress_queue.put(batch)
    task = asyncio.create_task(ConsoleEgress(format="json").run(egress_queue))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    logging.getLogger("desflow.connectors.egress.local").setLevel(logging.NOTSET)
    lines = capsys.readouterr().out.splitlines()
    assert [orjson.loads(line) for line in lines] == [
        {"stream_type": "telemetry", "path_id": "test", "value": 1},
        {"stream_type": "event", "key": "orders-1", "value": {"n": 2}},
    ]


@pytest.mark.asyncio
async def test_a_closed_stdout_pipe_does_not_stop_the_sink(monkeypatch, caplog):
    """A reader such as `head` closes the pipe. The sink goes on taking batches, so
    the queue drains, and it logs no error."""

    class ClosedPipe:
        def write(self, text):
            raise BrokenPipeError(32, "Broken pipe")

        def flush(self):
            pass

    monkeypatch.setattr(sys, "stdout", ClosedPipe())
    egress_queue = queue.Queue()
    for number in range(3):
        egress_queue.put([{"stream_type": "event", "key": "k", "value": number}])
    task = asyncio.create_task(ConsoleEgress(format="json").run(egress_queue))
    await asyncio.sleep(0.5)
    assert egress_queue.empty()
    assert not task.done()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert "Broken pipe" not in caplog.text


@pytest.mark.asyncio
async def test_console_json_logs_a_batch_it_cannot_encode_and_goes_on(capsys, caplog):
    """A batch with a value JSON cannot hold, a set, is logged at ERROR and the next
    batch is still printed."""
    egress_queue = queue.Queue()
    egress_queue.put([{"stream_type": "event", "key": "a", "value": {"x": {1, 2}}}])
    egress_queue.put([{"stream_type": "event", "key": "b", "value": {"x": 1}}])
    with caplog.at_level(logging.ERROR, logger="desflow.connectors.egress.local"):
        task = asyncio.create_task(ConsoleEgress(format="json").run(egress_queue))
        await asyncio.sleep(0.2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert capsys.readouterr().out == (
        '{"stream_type":"event","key":"b","value":{"x":1}}\n'
    )
    assert "Console: batch not printed" in caplog.text


_QUIET_SCRIPT = """\
import logging

from desflow import ConsoleEgress, SimulationContext

{setup}
app = SimulationContext(sim_id="Shop", factor=0).add_egress(ConsoleEgress({format}))
app.add_arrival("orders", dist="constant", value=1, spawn="t")
app.task(name="t")(lambda task_id: {{"x": 1}})
app.run(until=2)
"""


@pytest.mark.parametrize(
    "setup, format, warned",
    [
        ("", "", True),
        ("logging.basicConfig(level=logging.INFO)", "", False),
        ("", 'format="json"', False),
    ],
)
def test_console_warns_when_the_log_format_would_print_nothing(
    tmp_path, setup, format, warned
):
    """A script run with `python FILE` and no logging set up gets one warning on
    stderr that names its own line; logging at INFO or the json format gets none."""
    script = tmp_path / "shop.py"
    script.write_text(_QUIET_SCRIPT.format(setup=setup, format=format))
    done = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, check=True
    )
    found = done.stderr.count("ConsoleEgress writes each batch with logging at INFO")
    assert found == (1 if warned else 0), done.stderr
    if warned:
        assert f"{script}:9: UserWarning" in done.stderr


def test_console_refuses_an_unknown_format():
    with pytest.raises(ValueError, match="format must be log or json, got 'csv'"):
        ConsoleEgress(format="csv")  # type: ignore[arg-type]


async def _watch(ingress, steps):
    """Runs `ingress` on a plain queue while `steps` edits the file, then stops it."""
    ingress_queue = queue.Queue()
    task = asyncio.create_task(ingress.run(ingress_queue))
    await asyncio.sleep(0.05)
    await steps(ingress_queue)
    task.cancel()
    await task
    return ingress_queue


def _drain(ingress_queue):
    items = []
    while not ingress_queue.empty():
        items.append(ingress_queue.get_nowait())
    return items


def _write(path, text, mtime):
    """Writes the file and sets a modification time that differs from the last one."""
    import os

    path.write_text(text)
    os.utime(path, ns=(mtime, mtime))


@pytest.mark.asyncio
async def test_file_ingress_applies_the_first_read_and_each_edit(tmp_path):
    """The file at the start counts as a first read; a changed value is applied again."""
    path = tmp_path / "params.yaml"
    _write(path, "Shop.a: 1\nShop.b: 2\n", 1_000_000_000)
    ingress = FileIngress(path, poll=0.01)

    async def steps(ingress_queue):
        assert _drain(ingress_queue) == [("Shop.a", 1), ("Shop.b", 2)]
        _write(path, "Shop.a: 5\nShop.b: 2\n", 2_000_000_000)
        await asyncio.sleep(0.05)
        assert _drain(ingress_queue) == [("Shop.a", 5)]

    await _watch(ingress, steps)


@pytest.mark.asyncio
async def test_file_ingress_puts_nothing_for_a_save_with_no_change(tmp_path):
    path = tmp_path / "params.yaml"
    _write(path, "Shop.a: 1\n", 1_000_000_000)

    async def steps(ingress_queue):
        _drain(ingress_queue)
        _write(path, "Shop.a: 1\n", 2_000_000_000)
        await asyncio.sleep(0.05)
        assert _drain(ingress_queue) == []

    await _watch(FileIngress(path, poll=0.01), steps)


@pytest.mark.asyncio
async def test_file_ingress_puts_nothing_for_a_removed_entry(tmp_path):
    path = tmp_path / "params.yaml"
    _write(path, "Shop.a: 1\nShop.b: 2\n", 1_000_000_000)

    async def steps(ingress_queue):
        _drain(ingress_queue)
        _write(path, "Shop.a: 1\n", 2_000_000_000)
        await asyncio.sleep(0.05)
        assert _drain(ingress_queue) == []

    await _watch(FileIngress(path, poll=0.01), steps)


@pytest.mark.asyncio
async def test_file_ingress_waits_for_a_file_created_after_it_starts(tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    path = tmp_path / "params.yaml"

    async def steps(ingress_queue):
        await asyncio.sleep(0.05)
        assert _drain(ingress_queue) == []
        _write(path, "Shop.a: 3\n", 1_000_000_000)
        await asyncio.sleep(0.05)
        assert _drain(ingress_queue) == [("Shop.a", 3)]

    await _watch(FileIngress(path, poll=0.01), steps)
    assert not [r for r in caplog.records if r.name.endswith("ingress.file")]


@pytest.mark.asyncio
async def test_file_ingress_waits_through_an_empty_save_without_a_warning(
    tmp_path, caplog
):
    """An editor can leave the file empty for a moment while it saves; that is no
    read, so the same values afterwards are not applied again."""
    caplog.set_level(logging.DEBUG)
    path = tmp_path / "params.yaml"
    _write(path, "Shop.a: 1\n", 1_000_000_000)

    async def steps(ingress_queue):
        _drain(ingress_queue)
        _write(path, "", 2_000_000_000)
        await asyncio.sleep(0.05)
        _write(path, "Shop.a: 1\n", 3_000_000_000)
        await asyncio.sleep(0.05)
        assert _drain(ingress_queue) == []

    await _watch(FileIngress(path, poll=0.01), steps)
    assert not [r for r in caplog.records if r.name.endswith("ingress.file")]


@pytest.mark.asyncio
async def test_file_ingress_skips_an_invalid_file_and_compares_with_the_last_valid_read(
    tmp_path, caplog
):
    caplog.set_level(logging.WARNING)
    path = tmp_path / "params.yaml"
    _write(path, "Shop.a: 1\n", 1_000_000_000)

    async def steps(ingress_queue):
        _drain(ingress_queue)
        _write(path, "Shop.a: [1\n", 2_000_000_000)
        await asyncio.sleep(0.05)
        _write(path, "- 1\n- 2\n", 3_000_000_000)
        await asyncio.sleep(0.05)
        assert _drain(ingress_queue) == []
        # The same value as the last valid read is not applied again
        _write(path, "Shop.a: 1\nShop.b: 4\n", 4_000_000_000)
        await asyncio.sleep(0.05)
        assert _drain(ingress_queue) == [("Shop.b", 4)]

    await _watch(FileIngress(path, poll=0.01), steps)
    lines = [r.getMessage() for r in caplog.records]
    assert len(lines) == 2
    assert all(f"Skipped the ingress file {path}" in line for line in lines)
    assert "expected a mapping of paths to values, got list" in lines[1]


@pytest.mark.asyncio
async def test_file_ingress_skips_a_null_value_with_a_warning(tmp_path, caplog):
    caplog.set_level(logging.WARNING)
    path = tmp_path / "params.yaml"
    _write(path, "Shop.a:\nShop.b: 2\n", 1_000_000_000)

    async def steps(ingress_queue):
        assert _drain(ingress_queue) == [("Shop.b", 2)]

    await _watch(FileIngress(path, poll=0.01), steps)
    assert caplog.text.count("Skipped an entry of the ingress file") == 1
    assert "non-null value" in caplog.text


@pytest.mark.asyncio
async def test_console_envelope_false_prints_the_value_alone(capsys):
    """With `envelope` off the json format writes each event's value and nothing
    else, a null value as `null`, and a label record stays whole."""
    import orjson

    egress_queue = queue.Queue()
    provider = ConsoleEgress(format="json")
    provider.envelope = False
    egress_queue.put(
        [
            {"stream_type": "event", "key": "o-1", "value": {"n": 2}},
            {"stream_type": "event", "key": "o-2", "value": None},
            {"stream_type": "label", "path_id": "x", "value": 1},
        ]
    )
    task = asyncio.create_task(provider.run(egress_queue))
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    lines = capsys.readouterr().out.splitlines()
    assert [orjson.loads(line) for line in lines] == [
        {"n": 2},
        None,
        {"stream_type": "label", "path_id": "x", "value": 1},
    ]
