import asyncio
import logging
import queue

import pytest

from desflow.connectors.egress.local import ConsoleEgress
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


def test_console_refuses_an_unknown_format():
    with pytest.raises(ValueError, match="format must be log or json, got 'csv'"):
        ConsoleEgress(format="csv")  # type: ignore[arg-type]
