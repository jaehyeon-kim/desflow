"""Process helpers for the Batch 5 conformance blueprints.

A blueprint names these with `!python`, as the builder passes the same functions to
`add_process`.
"""

from typing import Any


def pinger(context: Any):
    """A model event every 10 s, as the plain SimPy reference process has."""
    while True:
        yield context.env.timeout(10)


def fail_closing(context: Any):
    """Fails the signal `closing` at 5, so a run that stops on it ends with an error."""
    yield context.env.timeout(5)
    context.signal("closing").fail("scratched")
