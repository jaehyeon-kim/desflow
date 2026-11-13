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


def is_temporary_postgres_error(error: Exception) -> bool:
    """Returns True when an error of connecting to PostgreSQL is one a wait can fix.

    A refused or timed-out connection is an `OSError`. The server answers with the
    asyncpg errors below while it starts, restarts or has no free connection. Any
    other error comes from a setting, such as a syntax error from a wrong table name
    or a refused password. A `ValueError` is a setting too, even when it is also an
    `OSError`, as a certificate that does not verify is.

    Args:
        error: The exception that connecting raised.

    Returns:
        bool: Whether connecting again after a wait can succeed.
    """
    import asyncpg

    if isinstance(error, ValueError):
        return False
    return isinstance(
        error,
        (
            OSError,
            asyncpg.PostgresConnectionError,
            asyncpg.CannotConnectNowError,
            asyncpg.AdminShutdownError,
            asyncpg.CrashShutdownError,
            asyncpg.TooManyConnectionsError,
        ),
    )


async def connect_with_retry(
    connect: Callable[[], Awaitable[None]],
    disconnect: Callable[[], Awaitable[None]],
    name: str,
    is_temporary: Callable[[Exception], bool],
) -> None:
    """Calls `connect` until it succeeds, waiting longer after each failure.

    Every connector that needs its service at start uses this, so a service that is
    still starting, as a database container is, delays the connector and does not
    end it, as the Kafka connectors already do. Only an error that `is_temporary`
    accepts is retried. Any other error is a setting that no wait fixes, so it is
    raised at once.

    Args:
        connect: Opens the connection and sets up what the connector needs.
        disconnect: Closes what a failed `connect` left open.
        name: The connector's name, for the log line.
        is_temporary: Says whether an error means the service is not ready yet.

    Raises:
        Exception: `connect` raised an error that `is_temporary` refuses.
    """
    delay = FIRST_DELAY
    while True:
        try:
            await connect()
            return
        except Exception as error:
            if not is_temporary(error):
                raise
            logger.error(f"{name} could not connect: {error}. Retrying in {delay}s")
            await disconnect()
            await asyncio.sleep(delay)
            delay = next_delay(delay)
