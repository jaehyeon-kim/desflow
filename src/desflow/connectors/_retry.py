"""The back-off that a connector waits while its service is down."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

FIRST_DELAY = 1.0
LONGEST_DELAY = 60.0


def next_delay(delay: float) -> float:
    """Returns the wait after `delay`: twice as long, up to one minute."""
    return min(delay * 2, LONGEST_DELAY)


async def connect_with_retry(
    connect: Callable[[], Awaitable[None]],
    disconnect: Callable[[], Awaitable[None]],
    name: str,
) -> None:
    """Calls `connect` until it succeeds, waiting longer after each failure.

    Every connector that needs its service at start uses this, so a service that is
    still starting, as a database container is, delays the connector and does not
    end it, as the Kafka connectors already do. A `ValueError` is a setting that no
    wait fixes, so it is raised at once.

    Args:
        connect: Opens the connection and sets up what the connector needs.
        disconnect: Closes what a failed `connect` left open.
        name: The connector's name, for the log line.

    Raises:
        ValueError: `connect` raised it, for a setting that is wrong.
    """
    delay = FIRST_DELAY
    while True:
        try:
            await connect()
            return
        except ValueError:
            raise
        except Exception as error:
            logger.error(f"{name} could not connect: {error}. Retrying in {delay}s")
            await disconnect()
            await asyncio.sleep(delay)
            delay = next_delay(delay)
