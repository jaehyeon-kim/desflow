import asyncio
import queue
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from desflow.connectors.ingress.redis import RedisIngress


@pytest.mark.asyncio
async def test_redis_ingress():
    update_queue = queue.Queue()
    ingress = RedisIngress(
        url="redis://localhost:6379/0", channel_name="test_params", poll_interval=0.01
    )

    with patch("desflow.connectors.ingress.redis.redis.from_url") as mock_from_url:
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_client.pubsub = MagicMock(return_value=mock_pubsub)
        mock_from_url.return_value = mock_client

        # Mock get_message to return a valid message, then None to sleep
        mock_pubsub.get_message.side_effect = [
            {
                "type": "message",
                "data": orjson.dumps({"path": "a.b", "value": 42}),
            },
            None,  # Sleep
            asyncio.CancelledError(),  # Break out of loop
        ]

        task = asyncio.create_task(ingress.run(update_queue))

        try:
            await task
        except asyncio.CancelledError:
            pass

        mock_from_url.assert_called_once_with("redis://localhost:6379/0")
        mock_pubsub.subscribe.assert_called_once_with("test_params")

        assert not update_queue.empty()
        path, val = update_queue.get_nowait()
        assert path == "a.b"
        assert val == 42

        mock_pubsub.unsubscribe.assert_called_once_with("test_params")
        mock_client.aclose.assert_called_once()


@pytest.mark.asyncio
async def test_redis_ingress_logs_and_skips_bad_messages(caplog):
    """A message that is not JSON, uses the old keys or has no value is logged and skipped."""
    update_queue = queue.Queue()
    ingress = RedisIngress(url="redis://localhost:6379/0", poll_interval=0.01)
    messages = [
        b"not json",
        orjson.dumps({"param_path": "a.b", "param_value": 1}),
        orjson.dumps({"path": "a.b"}),
        orjson.dumps({"path": "a.b", "value": 2}),
    ]

    with patch("desflow.connectors.ingress.redis.redis.from_url") as mock_from_url:
        mock_client = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_client.pubsub = MagicMock(return_value=mock_pubsub)
        mock_from_url.return_value = mock_client
        mock_pubsub.get_message.side_effect = [
            *({"type": "message", "data": data} for data in messages),
            asyncio.CancelledError(),
        ]

        with caplog.at_level("WARNING", logger="desflow.connectors.ingress.redis"):
            await ingress.run(update_queue)

    assert update_queue.get_nowait() == ("a.b", 2)
    assert update_queue.empty()
    skipped = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
    assert len(skipped) == 3
    assert all(m.startswith("Skipped a Redis ingress message") for m in skipped)
    assert "renamed to path and value in desflow 0.21.0" in skipped[1]
