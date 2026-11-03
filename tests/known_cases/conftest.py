"""A known case that does not end fails after a minute instead of holding the suite."""

import signal
from typing import Iterator

import pytest

LIMIT_SECONDS = 60


@pytest.fixture(autouse=True)
def _a_case_that_does_not_end_fails() -> Iterator[None]:
    if not hasattr(
        signal, "SIGALRM"
    ):  # Windows has no alarm; the cases end on their own
        yield
        return

    def stop(signum: int, frame: object) -> None:
        raise TimeoutError(f"the case did not end in {LIMIT_SECONDS} seconds")

    previous = signal.signal(signal.SIGALRM, stop)
    signal.alarm(LIMIT_SECONDS)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
