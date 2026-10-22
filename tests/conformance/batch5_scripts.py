"""Process helpers for the Batch 5 conformance blueprints.

A blueprint names these with `!python`, as the builder passes the same functions to
`add_process`.
"""

from typing import Any

from desflow import DynamicRealtimeEnvironment


def pinger(context: Any):
    """A model event every 10 s, as the plain SimPy reference process has."""
    while True:
        yield context.env.timeout(10)


def fail_closing(context: Any):
    """Fails the signal `closing` at 5, so a run that stops on it ends with an error."""
    yield context.env.timeout(5)
    context.signal("closing").fail("scratched")


def start_go(context: Any):
    """Fires the signal `go` at 5."""
    yield context.env.timeout(5)
    context.signal("go").fire()


class TracingEnvironment(DynamicRealtimeEnvironment):
    """An environment subclass that records `(now, peek())` after every `step()`."""

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.trace: list = []

    def step(self) -> None:
        super().step()
        self.trace.append((self.now, self.peek()))
