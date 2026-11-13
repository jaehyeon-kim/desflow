import asyncio
import logging
import math
import queue
from typing import Any

import orjson
import redis.asyncio as redis

from desflow.connectors._checks import checked_poll
from desflow.connectors._retry import FIRST_DELAY, next_delay
from desflow.connectors.ingress.base import BaseIngress, parse_change

logger = logging.getLogger(__name__)

# redis-py's own `socket_timeout` default from 8.0; earlier versions have none
_DEFAULT_ANSWER_LIMIT = 5.0


def _answer_limit(connection: Any) -> float:
    """Returns the seconds a connection gives Redis to answer a command.

    A `socket_timeout` of None or infinity sets no limit, so the limit is then
    `_DEFAULT_ANSWER_LIMIT`.
    """
    timeout = getattr(connection, "socket_timeout", None)
    if isinstance(timeout, (int, float)) and 0 < timeout < math.inf:
        return float(timeout)
    return _DEFAULT_ANSWER_LIMIT


class RedisIngress(BaseIngress):
    """
    Asynchronous ingress provider for dynamic parameter updates via Redis Pub/Sub.

    This connector utilizes `redis.asyncio` to listen on a specified Redis channel
    for parameter updates, parsing them from JSON, and pushing them to the simulation's
    internal update queue. Each message is a JSON object with `path` and `value`,
    such as `{"path": "Shop.arrival.orders.rate", "value": 10.0}`; a message that
    cannot be parsed or does not have both is logged as a warning and skipped.

    Attributes:
        url (str): The connection URL for the Redis server.
        channel_name (str): The Redis Pub/Sub channel to subscribe to.
        poll_interval (float): Sleep interval when no messages are found.
        client (redis.Redis | None): The underlying redis-py asynchronous client instance.
        pubsub (redis.client.PubSub | None): The active Pub/Sub subscription instance.
        **kwargs: Additional configuration dictionary passed to `redis.from_url`.
    """

    def __init__(
        self,
        url: str,
        channel_name: str = "params",
        poll_interval: float = 0.5,
        **kwargs: Any,
    ):
        """
        Initializes the RedisIngress with connection and channel settings.

        Args:
            url: Redis connection URL (e.g., redis://127.0.0.1:6379/0).
            channel_name: Redis Pub/Sub channel to subscribe to.
            poll_interval: Sleep interval when no messages are found.
            **kwargs: Additional configuration dictionary passed to `redis.from_url`.
        """
        self.url = url
        self.channel_name = channel_name
        self.poll_interval = checked_poll(poll_interval, "poll_interval")
        self.kwargs = kwargs
        self.client: redis.Redis | None = None
        self.pubsub: redis.client.PubSub | None = None

    async def run(self, update_queue: queue.Queue) -> None:
        """
        The main execution loop for subscribing to Redis and receiving updates.

        Continuously polls the Pub/Sub channel for incoming JSON messages containing
        `path` and `value`, placing successfully parsed updates onto the
        internal update queue. When Redis is down, refuses the subscription or the
        connection fails or stops answering, it logs the error and connects again
        with a back-off that grows to one minute, as the Kafka ingress does. The
        back-off starts again from the first wait once Redis confirms a subscription.

        Args:
            update_queue: A thread-safe queue where incoming parameters are pushed
                so they can be applied within the SimPy context.

        Raises:
            ValueError: If `redis.from_url` refuses the URL, such as one with a scheme
                it does not know. No wait fixes it, so it is raised at once, as the
                PostgreSQL connectors do.
            TypeError: If redis-py refuses a keyword setting, such as a misspelt
                `socket_timeout`, when it makes the connection. It is raised at once
                too.
        """
        delay = FIRST_DELAY
        try:
            while True:
                subscribed = False
                try:
                    self.client = redis.from_url(self.url, **self.kwargs)
                    self.pubsub = self.client.pubsub()
                    await self.pubsub.subscribe(self.channel_name)
                    await self._confirm_subscription()
                    subscribed = True
                    logger.info(
                        f"RedisIngress subscribed to channel '{self.channel_name}'"
                    )
                    delay = FIRST_DELAY
                    await self._receive(update_queue)
                except Exception as e:
                    if isinstance(e, ValueError) and self.client is None:
                        # `redis.from_url` refused the URL: no wait fixes it
                        raise
                    if isinstance(e, TypeError) and not subscribed:
                        # redis-py refused a keyword when it made the connection: no
                        # wait fixes it
                        raise
                    logger.error(
                        f"RedisIngress connection failed: {e}. Retrying in {delay}s"
                    )
                    await self._disconnect()
                    await asyncio.sleep(delay)
                    delay = next_delay(delay)
        except asyncio.CancelledError:
            pass
        finally:
            await self._disconnect()

    async def _confirm_subscription(self) -> None:
        """Reads the answer to SUBSCRIBE until Redis confirms the subscription.

        `subscribe()` sends the command without reading the answer, so a refusal,
        such as an ACL that denies the channel, is raised by the first read. Reading
        it here keeps the back-off growing while Redis refuses.

        The answer is awaited for at most the connection's `socket_timeout`, the
        time redis-py gives any other command to answer, which the URL or the
        `socket_timeout` keyword sets. A connection with no `socket_timeout`, the
        default before redis-py 8.0, gets 5 seconds, the default from 8.0. A connection
        that stays open but never answers then fails like one that is down, and
        the ingress connects again after the next back-off.

        Raises:
            TimeoutError: If Redis does not confirm the subscription in that time.
        """
        assert self.pubsub is not None
        limit = _answer_limit(self.pubsub.connection)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + limit
        while (remaining := deadline - loop.time()) > 0:
            message = await self.pubsub.get_message(
                timeout=min(self.poll_interval, remaining)
            )
            if message and message["type"] == "subscribe":
                return
        raise TimeoutError(
            f"Redis did not confirm the subscription to channel "
            f"'{self.channel_name}' within {limit}s"
        )

    async def _receive(self, update_queue: queue.Queue) -> None:
        """Reads messages from the subscription until an error or a cancel ends it."""
        assert self.pubsub is not None
        while True:
            message = await self.pubsub.get_message(
                ignore_subscribe_messages=True, timeout=self.poll_interval
            )
            if message and message["type"] == "message":
                try:
                    update_queue.put(parse_change(orjson.loads(message["data"])))
                except Exception as e:
                    logger.warning(f"Skipped a Redis ingress message: {e}")
            else:
                await asyncio.sleep(self.poll_interval)

    async def _disconnect(self) -> None:
        """Leaves the channel and closes the client, ignoring errors from a connection
        that is already down."""
        pubsub, client = self.pubsub, self.client
        self.pubsub = self.client = None
        try:
            if pubsub:
                await pubsub.unsubscribe(self.channel_name)
        except Exception:
            pass
        try:
            if client:
                await client.aclose()
        except Exception:
            pass
