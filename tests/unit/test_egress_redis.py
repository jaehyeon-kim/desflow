import json
import queue
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
