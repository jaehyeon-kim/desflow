import asyncio
import logging
import queue
from typing import Any

import orjson
import redis.asyncio as redis

from desflow.connectors._checks import checked_poll
from desflow.connectors._retry import FIRST_DELAY, next_delay
from desflow.connectors.ingress.base import BaseIngress, parse_change

logger = logging.getLogger(__name__)


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
        internal update queue. When Redis is down or the connection fails, it logs
        the error and connects again with a back-off that grows to one minute, as the
        Kafka ingress does.

        Args:
            update_queue: A thread-safe queue where incoming parameters are pushed
                so they can be applied within the SimPy context.

        Raises:
            ValueError: If `redis.from_url` refuses the URL, such as one with a scheme
                it does not know. No wait fixes it, so it is raised at once, as the
                PostgreSQL connectors do.
        """
        delay = FIRST_DELAY
        try:
            while True:
                try:
                    self.client = redis.from_url(self.url, **self.kwargs)
                    self.pubsub = self.client.pubsub()
                    await self.pubsub.subscribe(self.channel_name)
                    logger.info(
                        f"RedisIngress subscribed to channel '{self.channel_name}'"
                    )
                    delay = FIRST_DELAY
                    await self._receive(update_queue)
                except Exception as e:
                    if isinstance(e, ValueError) and self.client is None:
                        # `redis.from_url` refused the URL: no wait fixes it
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
