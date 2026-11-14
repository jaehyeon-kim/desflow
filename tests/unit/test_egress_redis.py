import asyncio
import json
import queue
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from redis import exceptions as redis_exceptions

from desflow.connectors.egress.redis import RedisEgress


@pytest.mark.asyncio
async def test_redis_egress(run_until):
    egress_queue = queue.Queue()

    # Put two batches
    egress_queue.put(
        [
            {"stream_type": "event", "key": "e1", "value": "v1"},
            {"stream_type": "telemetry", "key": "t1", "value": "v2"},
        ]
    )
    egress_queue.put([{"__stream__": "custom_stream", "key": "c1"}])

    egress = RedisEgress(url="redis://localhost:6379/0", stream_name="default_stream")

    with patch("desflow.connectors.egress.redis.redis.from_url") as mock_from_url:
        from unittest.mock import MagicMock

        mock_client = AsyncMock()
        # xadd is synchronous on a redis-py pipeline; only execute is awaited
        mock_pipe = MagicMock()
        mock_pipe.execute = AsyncMock()
        mock_client.pipeline = MagicMock(return_value=mock_pipe)
        mock_from_url.return_value = mock_client

        await run_until(
            egress.run(egress_queue),
            lambda: mock_pipe.execute.called and egress_queue.empty(),
        )

        mock_from_url.assert_called_once_with("redis://localhost:6379/0")
        assert mock_pipe.xadd.call_count == 3
        # Check first call (default stream)
        args, _ = mock_pipe.xadd.call_args_list[0]
        assert args[0] == "default_stream"
        assert "payload" in args[1]

        # Check third call (custom stream)
        args, _ = mock_pipe.xadd.call_args_list[2]
        assert args[0] == "custom_stream"


@pytest.mark.asyncio
async def test_stream_key_is_read_from_the_nested_payload(run_until):
    """`publish_event` nests the caller's dict under `value`, so `__stream__` is there
    rather than at the top level. Reading only the top level sent every record to the
    default stream and the documented routing silently did nothing."""
    egress = RedisEgress("redis://localhost:6379", stream_name="default_events")
    egress.client = AsyncMock()
    pipe = MagicMock()
    pipe.execute = AsyncMock()
    egress.client.pipeline = MagicMock(return_value=pipe)

    q: queue.Queue = queue.Queue()
    q.put(
        [
            {
                "stream_type": "event",
                "key": "part-1",
                "value": {"__stream__": "part_events", "status": "queued"},
            },
            {"stream_type": "telemetry", "value": {"metric": 1}},
        ]
    )

    await run_until(egress.run(q), lambda: pipe.execute.called and q.empty())

    targets = [call.args[0] for call in pipe.xadd.call_args_list]
    assert targets == ["part_events", "default_events"]

    routed = json.loads(pipe.xadd.call_args_list[0].args[1]["payload"])
    assert "__stream__" not in routed["value"], "the key should not reach the stream"
    assert routed["value"] == {"status": "queued"}


@pytest.mark.asyncio
async def test_reserved_keys_other_than_stream_are_not_written(run_until, caplog):
    """`__table__` and the other reserved keys are dropped with one warning, as every
    other sink does, and `__stream__` still routes."""
    egress = RedisEgress("redis://localhost:6379", stream_name="default_events")
    egress.client = AsyncMock()
    pipe = MagicMock()
    pipe.execute = AsyncMock()
    egress.client.pipeline = MagicMock(return_value=pipe)
    q: queue.Queue = queue.Queue()
    q.put(
        [
            {
                "stream_type": "event",
                "key": "k1",
                "value": {"order": 1, "__table__": "shop.orders", "__stream__": "s1"},
            }
        ]
    )

    await run_until(egress.run(q), lambda: pipe.execute.called and q.empty())

    assert pipe.xadd.call_args_list[0].args[0] == "s1"
    written = json.loads(pipe.xadd.call_args_list[0].args[1]["payload"])
    assert written["value"] == {"order": 1}
    assert "RedisEgress drops the payload key '__table__'" in caplog.text


@pytest.mark.asyncio
async def test_a_record_json_cannot_hold_is_skipped_alone(run_until, caplog):
    """An integer over 64 bits cannot be written as JSON. Its record is skipped with
    a warning and the rest of the batch is published."""
    egress = RedisEgress("redis://localhost:6379", stream_name="events")
    egress.client = AsyncMock()
    pipe = MagicMock()
    pipe.execute = AsyncMock()
    egress.client.pipeline = MagicMock(return_value=pipe)
    q: queue.Queue = queue.Queue()
    q.put(
        [
            {"stream_type": "event", "key": "a", "value": {"x": 1}},
            {"stream_type": "event", "key": "b", "value": {"x": 2**64}},
            {"stream_type": "event", "key": "c", "value": {"x": 3}},
        ]
    )

    await run_until(egress.run(q), lambda: pipe.execute.called and q.empty())

    keys = [json.loads(c.args[1]["payload"])["key"] for c in pipe.xadd.call_args_list]
    assert keys == ["a", "c"]
    assert "skipped the record with key b" in caplog.text
    assert "Error publishing batch" not in caplog.text


@pytest.mark.asyncio
async def test_envelope_false_stores_the_value_alone(run_until):
    """With `envelope` off the `payload` field holds the event's value as JSON,
    without `__stream__`, and a value that is not a mapping is stored too, a null
    value as JSON `null`."""
    egress_queue = queue.Queue()
    egress_queue.put(
        [
            {"stream_type": "event", "key": "e1", "value": {"__stream__": "s", "n": 1}},
            {"stream_type": "event", "key": "e2", "value": "text"},
            {"stream_type": "event", "key": "e3", "value": None},
        ]
    )
    egress = RedisEgress(url="redis://localhost:6379/0", stream_name="default")
    egress.envelope = False

    with patch("desflow.connectors.egress.redis.redis.from_url") as from_url:
        client = AsyncMock()
        pipe = MagicMock()
        pipe.execute = AsyncMock()
        client.pipeline = MagicMock(return_value=pipe)
        from_url.return_value = client
        await run_until(
            egress.run(egress_queue),
            lambda: pipe.execute.called and egress_queue.empty(),
        )

    assert [
        (c.args[0], json.loads(c.args[1]["payload"])) for c in pipe.xadd.call_args_list
    ] == [
        ("s", {"n": 1}),
        ("default", "text"),
        ("default", None),
    ]


@pytest.mark.asyncio
async def test_an_empty_or_refused_batch_leaves_the_sink_running(run_until, caplog):
    """An empty batch writes nothing, and a batch Redis refuses is logged. The sink
    goes on with the next batch, with no write counted as in progress."""
    egress = RedisEgress("redis://localhost:6379", stream_name="events")
    egress.client = AsyncMock()
    pipe = MagicMock()
    pipe.execute = AsyncMock(side_effect=[Exception("down"), None])
    egress.client.pipeline = MagicMock(return_value=pipe)
    egress_queue = queue.Queue()
    for batch in ([], [{"key": "a", "value": 1}], [{"key": "b", "value": 2}]):
        egress_queue.put(batch)

    await run_until(egress.run(egress_queue), lambda: pipe.execute.call_count == 2)

    assert pipe.xadd.call_count == 2
    assert "Error publishing batch to Redis: down" in caplog.text
    assert egress.active_tasks == 0


@pytest.mark.asyncio
async def test_a_sink_that_cannot_write_at_teardown_drops_its_backlog_at_once(
    run_until, caplog
):
    """Redis is down and teardown has begun: the 8 queued batches are dropped with one
    warning that counts them, not one for each second of back-off."""
    egress = RedisEgress("redis://localhost:6379", stream_name="events")
    egress.client = AsyncMock()
    pipe = MagicMock()
    pipe.execute = AsyncMock(side_effect=redis_exceptions.ConnectionError("down"))
    egress.client.pipeline = MagicMock(return_value=pipe)
    egress._ending = True
    egress_queue = queue.Queue()
    for n in range(8):
        egress_queue.put([{"key": f"k{n}", "value": n}])

    started = time.monotonic()
    await run_until(egress.run(egress_queue), lambda: egress_queue.empty())

    assert time.monotonic() - started < 0.5
    assert pipe.execute.call_count == 1
    assert caplog.text.count("RedisEgress could not write at teardown") == 1
    assert "8 batches were dropped" in caplog.text


@pytest.mark.asyncio
async def test_a_batch_that_redis_refuses_at_teardown_does_not_drop_the_others(
    run_until, caplog
):
    """Redis is up and refuses the first batch alone, during teardown: the other
    batches are still written and nothing is dropped."""
    egress = RedisEgress("redis://localhost:6379", stream_name="events")
    egress.client = AsyncMock()
    pipe = MagicMock()
    pipe.execute = AsyncMock(
        side_effect=[redis_exceptions.ResponseError("bad batch"), None, None]
    )
    egress.client.pipeline = MagicMock(return_value=pipe)
    egress._ending = True
    egress_queue = queue.Queue()
    for n in range(3):
        egress_queue.put([{"key": f"k{n}", "value": n}])

    await run_until(egress.run(egress_queue), lambda: pipe.execute.call_count == 3)

    assert pipe.execute.call_count == 3
    assert "were dropped" not in caplog.text


@pytest.mark.asyncio
async def test_a_stream_that_is_not_text_is_ignored_with_a_warning(run_until, caplog):
    """redis-py refuses a stream name that is not text only when the pipeline runs,
    which lost the whole batch. The record goes to the default stream with one
    warning, and the rest of the batch is published."""
    egress = RedisEgress("redis://localhost:6379", stream_name="events")
    egress.client = AsyncMock()
    pipe = MagicMock()
    pipe.execute = AsyncMock()
    egress.client.pipeline = MagicMock(return_value=pipe)
    q: queue.Queue = queue.Queue()
    q.put(
        [
            {"stream_type": "event", "key": "a", "value": {"__stream__": "custom"}},
            {"stream_type": "event", "key": "b", "value": {"__stream__": True}},
            {"stream_type": "event", "key": "c", "value": {"__stream__": ["x"]}},
        ]
    )

    await run_until(egress.run(q), lambda: pipe.execute.called and q.empty())

    assert [c.args[0] for c in pipe.xadd.call_args_list] == [
        "custom",
        "events",
        "events",
    ]
    assert caplog.text.count("RedisEgress ignores __stream__") == 1
    assert "Error publishing batch" not in caplog.text


@pytest.mark.asyncio
async def test_a_setting_redis_py_refuses_ends_the_sink(caplog):
    """A `TypeError` from redis-py, such as a misspelt keyword, ends the task with its
    error instead of logging every batch as not published."""
    egress = RedisEgress(
        "redis://localhost:6379", stream_name="events", socket_timout=2
    )
    q: queue.Queue = queue.Queue()
    q.put([{"stream_type": "event", "key": "a", "value": {"x": 1}}])

    with pytest.raises(TypeError, match="socket_timout"):
        await asyncio.wait_for(egress.run(q), timeout=5)

    assert "Error publishing batch" not in caplog.text
