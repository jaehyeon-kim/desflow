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
            {"type": "subscribe", "data": 1},
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
async def test_redis_ingress_connects_again_when_the_connection_fails(
    caplog, run_until
):
    """A connection that fails, at `subscribe` or in `get_message`, is logged and
    opened again, so the messages after it still arrive."""
    import redis.exceptions

    update_queue = queue.Queue()
    ingress = RedisIngress(url="redis://localhost:6379/0", poll_interval=0.01)
    pubsubs = []

    def client_for_attempt(*args, **kwargs):
        pubsub = AsyncMock()
        attempt = len(pubsubs)
        pubsubs.append(pubsub)
        if attempt == 0:
            pubsub.subscribe.side_effect = redis.exceptions.ConnectionError("down")
        elif attempt == 1:
            pubsub.get_message.side_effect = redis.exceptions.ConnectionError("reset")
        else:
            pubsub.get_message.side_effect = [
                {"type": "subscribe", "data": 1},
                {
                    "type": "message",
                    "data": orjson.dumps({"path": "a.b", "value": 1}),
                },
                *[None] * 1000,
            ]
        client = AsyncMock()
        client.pubsub = MagicMock(return_value=pubsub)
        return client

    with (
        patch("desflow.connectors.ingress.redis.redis.from_url", client_for_attempt),
        patch("desflow.connectors.ingress.redis.FIRST_DELAY", 0.01),
    ):
        await run_until(ingress.run(update_queue), lambda: not update_queue.empty())

    assert update_queue.get_nowait() == ("a.b", 1)
    assert len(pubsubs) == 3
    assert "RedisIngress connection failed: down" in caplog.text
    assert "RedisIngress connection failed: reset" in caplog.text


@pytest.mark.asyncio
async def test_redis_ingress_waits_longer_while_redis_refuses_the_subscription(
    caplog, run_until
):
    """Redis answers SUBSCRIBE after `subscribe()` returns, so a refusal, such as an
    ACL that denies the channel, comes from the first read. Until Redis confirms the
    subscription, each wait is twice the one before."""
    import redis

    update_queue = queue.Queue()
    ingress = RedisIngress(url="redis://localhost:6379/0", poll_interval=0.01)
    pubsubs = []

    def client_for_attempt(url, **kwargs):
        pubsub = AsyncMock()
        pubsubs.append(pubsub)
        if len(pubsubs) <= 3:
            pubsub.get_message.side_effect = redis.exceptions.ResponseError("NOPERM")
        else:
            pubsub.get_message.side_effect = [
                {"type": "subscribe", "data": 1},
                {"type": "message", "data": orjson.dumps({"path": "a.b", "value": 1})},
                *[None] * 1000,
            ]
        client = AsyncMock()
        client.pubsub = MagicMock(return_value=pubsub)
        return client

    with (
        patch("desflow.connectors.ingress.redis.redis.from_url", client_for_attempt),
        patch("desflow.connectors.ingress.redis.FIRST_DELAY", 0.01),
    ):
        await run_until(ingress.run(update_queue), lambda: not update_queue.empty())

    assert update_queue.get_nowait() == ("a.b", 1)
    waits = [
        r.getMessage().rpartition(" ")[2]
        for r in caplog.records
        if "NOPERM" in r.getMessage()
    ]
    assert waits == ["0.01s", "0.02s", "0.04s"]


@pytest.mark.asyncio
async def test_redis_ingress_connects_again_when_redis_never_confirms_the_subscription(
    caplog, run_until
):
    """A connection that stays open but never answers SUBSCRIBE fails once the
    client's socket timeout has passed, and the ingress connects again."""
    update_queue = queue.Queue()
    ingress = RedisIngress(url="redis://localhost:6379/0", poll_interval=0.05)
    pubsubs = []
    silent_times = []

    async def never_confirms(timeout, **kwargs):
        loop = asyncio.get_running_loop()
        silent_times.append(loop.time())
        await asyncio.sleep(timeout)
        silent_times.append(loop.time())
        return None

    def client_for_attempt(url, **kwargs):
        pubsub = AsyncMock()
        pubsubs.append(pubsub)
        if len(pubsubs) == 1:
            pubsub.connection.socket_timeout = 0.12
            pubsub.get_message.side_effect = never_confirms
        else:
            pubsub.get_message.side_effect = [
                {"type": "subscribe", "data": 1},
                {"type": "message", "data": orjson.dumps({"path": "a.b", "value": 1})},
                *[None] * 1000,
            ]
        client = AsyncMock()
        client.pubsub = MagicMock(return_value=pubsub)
        return client

    with (
        patch("desflow.connectors.ingress.redis.redis.from_url", client_for_attempt),
        patch("desflow.connectors.ingress.redis.FIRST_DELAY", 0.01),
    ):
        await run_until(ingress.run(update_queue), lambda: not update_queue.empty())

    assert update_queue.get_nowait() == ("a.b", 1)
    assert len(pubsubs) == 2
    # The wait ends at the first read that ends past the limit. On Windows the loop
    # clock moves in steps of about 16 ms, so a sleep can end early by the loop clock,
    # the next read is asked for, and the sum of the timeouts asked for passes the limit
    assert silent_times[-1] - silent_times[0] < 0.12 + 0.05
    assert "did not confirm the subscription" in caplog.text


@pytest.mark.asyncio
async def test_redis_ingress_refuses_a_url_with_an_unknown_scheme_at_once(caplog):
    """A URL that `redis.from_url` refuses is a setting no wait fixes, so the task
    ends with its `ValueError` instead of retrying for ever."""
    ingress = RedisIngress(url="localhost:6379/0", poll_interval=0.01)

    with pytest.raises(ValueError, match="scheme"):
        await asyncio.wait_for(ingress.run(queue.Queue()), timeout=5)

    assert "Retrying" not in caplog.text


@pytest.mark.asyncio
async def test_redis_ingress_reconnects_after_a_value_error_while_reading(
    caplog, run_until
):
    """A `ValueError` raised after the client was built, such as the
    `UnicodeDecodeError` redis-py raises for bytes that are not UTF-8, is a failed
    connection like any other, so the ingress connects again."""
    update_queue = queue.Queue()
    ingress = RedisIngress(url="redis://localhost:6379/0", poll_interval=0.01)
    pubsubs = []

    def client_for_attempt(url, **kwargs):
        pubsub = AsyncMock()
        pubsubs.append(pubsub)
        if len(pubsubs) == 1:
            pubsub.get_message.side_effect = UnicodeDecodeError(
                "utf-8", b"\xff", 0, 1, "invalid start byte"
            )
        else:
            pubsub.get_message.side_effect = [
                {"type": "subscribe", "data": 1},
                {
                    "type": "message",
                    "data": orjson.dumps({"path": "a.b", "value": 1}),
                },
                *[None] * 1000,
            ]
        client = AsyncMock()
        client.pubsub = MagicMock(return_value=pubsub)
        return client

    with (
        patch("desflow.connectors.ingress.redis.redis.from_url", client_for_attempt),
        patch("desflow.connectors.ingress.redis.FIRST_DELAY", 0.01),
    ):
        await run_until(ingress.run(update_queue), lambda: not update_queue.empty())

    assert update_queue.get_nowait() == ("a.b", 1)
    assert len(pubsubs) == 2
    assert "RedisIngress connection failed" in caplog.text


def test_redis_ingress_refuses_a_poll_interval_that_is_not_a_positive_number():
    """The constructor refuses a `poll_interval` of 0. `checked_poll` has its own test,
    so one refused value shows that the constructor calls it."""
    with pytest.raises(
        ValueError, match="poll_interval is a number of seconds above 0"
    ):
        RedisIngress(url="redis://localhost:6379/0", poll_interval=0)


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
            {"type": "subscribe", "data": 1},
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
