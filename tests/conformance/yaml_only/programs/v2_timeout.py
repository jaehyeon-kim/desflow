"""The SimPy program of V2: waiters that wait for an event or for a timeout whose
length they compute, with the event sent at a given time."""

from typing import Any, List, Optional, Tuple

import simpy

Trace = List[Tuple[Any, ...]]


def reference(
    n: int,
    limit: float,
    at: float,
    timeout_first: bool = False,
    cut_at: Optional[float] = None,
) -> Trace:
    """The (time, key, what happened) of `n` waiters created after the sender. A waiter
    ends with whether the event, the timeout or both were done when it woke, or is
    interrupted at `cut_at`; a negative `limit` raises `ValueError`."""
    env = simpy.Environment()
    sent = env.event()
    trace: Trace = []

    def sender():
        yield env.timeout(at)
        trace.append((env.now, "sender-1", "sent"))
        sent.succeed(1)

    def waiter(index: int):
        key = f"waiters-{index}"
        timeout = env.timeout(limit)
        try:
            found = yield (timeout | sent) if timeout_first else (sent | timeout)
        except simpy.Interrupt:
            trace.append((env.now, key, "interrupted"))
            return
        trace.append((env.now, key, "woke", sent in found, timeout in found))

    env.process(sender())
    waiters = [env.process(waiter(i)) for i in range(1, n + 1)]
    if cut_at is not None:

        def cutter():
            yield env.timeout(cut_at)
            for target in waiters:
                if target.is_alive:
                    target.interrupt()

        env.process(cutter())
    env.run()
    return trace
