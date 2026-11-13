"""Runs a blocking call of a connector in a thread that the end of the process does
not wait for."""

import asyncio
import contextvars
import threading
from typing import Any, Callable


async def run_in_daemon_thread(function: Callable[..., Any], *args: Any) -> Any:
    """Runs `function(*args)` in its own daemon thread and waits for its result.

    `asyncio.to_thread` uses the loop's executor, whose threads Python joins when the
    process ends. A call that never answers, such as a request to a service that
    accepts the connection and says nothing, would keep the process from ending after
    the run has stopped waiting for it. A daemon thread does not.

    Args:
        function: The blocking call.
        *args: Its arguments.

    Returns:
        Any: What `function` returns.

    Raises:
        Exception: What `function` raises.
    """
    loop = asyncio.get_running_loop()
    done: asyncio.Future = loop.create_future()
    context = contextvars.copy_context()

    def settle(result: Any, error: BaseException | None) -> None:
        if done.done():
            return  # the wait was cancelled
        if error is not None:
            done.set_exception(error)
        else:
            done.set_result(result)

    def work() -> None:
        result, error = None, None
        try:
            result = context.run(function, *args)
        except BaseException as exc:
            error = exc
        try:
            loop.call_soon_threadsafe(settle, result, error)
        except RuntimeError:
            pass  # the loop has closed

    threading.Thread(target=work, daemon=True, name="desflow-connector").start()
    return await done
