"""Signals: named events that tasks and processes fire and wait for.

A `Signal` wraps SimPy's shared-event pattern: one pending `simpy.Event` that every
waiter yields, succeeded or failed when the signal fires, and replaced by a fresh
one at once, so the signal can fire again. Every waiter of one firing resumes; a
waiter that starts after a firing waits for the next.
"""

import logging
from typing import Any, Callable, List, Optional

from simpy import Environment, Event
from simpy.events import NORMAL, URGENT

logger = logging.getLogger(__name__)


class SignalFailed(Exception):
    """The exception a waiter of a failed signal receives.

    Attributes:
        signal (str): The signal's name.
        reason (Any): What `Signal.fail` was given, such as a task's failure reason.
    """

    def __init__(self, signal: str, reason: Any):
        # SimPy copies a failure as type(error)(*error.args), so args are the
        # constructor's arguments
        super().__init__(signal, reason)
        self.signal = signal
        self.reason = reason

    def __str__(self) -> str:
        return f"signal '{self.signal}' failed: {self.reason}"


class Signal:
    """
    A named event that can fire many times, each firing waking every waiter.

    Attributes:
        env: The environment.
        name (str): The signal's name.
        count (int): How many times it has fired or failed.
        ok (Optional[bool]): Whether the last firing succeeded; None before the first.
        value (Any): The value of the last firing, or the failure reason.
    """

    def __init__(self, env: Environment, name: str = "signal", once: bool = False):
        """
        Creates a signal that has not fired yet.

        Args:
            env: The environment.
            name: Its name, as a blueprint's `signals` and `signal.<name>` use it.
            once: When True the signal fires once, as SimPy's `Event` does: it does
                not re-arm, so a wait after the firing resumes at once, and a second
                firing is refused.
        """
        self.env = env
        self.name = name
        self.once = once
        self.count = 0
        self.ok: Optional[bool] = None
        self.value: Any = None
        # Callbacks run when the signal fires, copied onto each new pending event
        self.callbacks: List[Callable[[Any], None]] = []
        self._event = env.event()

    def attach(self, callbacks: List[Callable[[Any], None]]) -> None:
        """Keeps `callbacks` as the signal's `on:` callbacks and adds them to the
        current pending event, so they run when the signal next fires, before any
        waiter, as SimPy runs a callback added before the waiters.

        Args:
            callbacks: Functions called with the fired event, copied onto each new
                pending event so they run on every firing.
        """
        self.callbacks = list(callbacks)
        for callback in self.callbacks:
            self._event.callbacks.append(callback)

    def wait(self) -> Event:
        """
        Returns the event of the next firing, for a process to yield.

        Returns:
            simpy.Event: Succeeds with the firing's value, or fails with
                `SignalFailed`, when the signal next fires.
        """
        return self._event

    def fire(self, value: Any = None, urgent: bool = False, after: float = 0.0) -> None:
        """
        Fires the signal: every waiter resumes with `value`.

        Args:
            value: What the waiters receive, such as a task's finished payload.
            urgent: When True the waiters resume before the other events at their
                time, as SimPy's `URGENT` priority orders them. Needs a one-shot
                signal.
            after: Simulation seconds from now at which the waiters resume. The
                signal counts as fired now, so a second firing is refused. Needs a
                one-shot signal.

        Raises:
            ValueError: If `urgent` or `after` is given to a signal that re-arms,
                or `after` is negative.
        """
        self._check_timing(urgent, after)
        if self.once and self.count >= 1:
            # A one-shot signal fires once. A second firing is SimPy's RuntimeError
            # under `stop`; under `continue` it is dropped with a log line.
            if getattr(self.env, "on_unhandled_failure", "stop") == "stop":
                raise RuntimeError(f"signal '{self.name}' is one-shot and has fired")
            logger.warning(
                "signal '%s' is one-shot and has already fired; the second firing "
                "is dropped",
                self.name,
            )
            return
        self.count += 1
        self.ok, self.value = True, value
        if self.once:
            # Keep the triggered event, so a later wait resumes at once.
            if urgent or after:
                self._schedule(True, value, urgent, after)
            else:
                self._event.succeed(value)
        else:
            event, self._event = self._event, self.env.event()
            for callback in self.callbacks:
                self._event.callbacks.append(callback)
            event.succeed(value)
        self._notify()

    def fail(self, reason: Any, urgent: bool = False, after: float = 0.0) -> None:
        """
        Fails the signal: every waiter receives `SignalFailed` with the reason.

        A failure nobody waits for is not an error: the event is defused, so the run
        goes on.

        Args:
            reason: Why it failed, such as a task's failure reason.
            urgent: As for `fire()`.
            after: As for `fire()`.

        Raises:
            ValueError: As for `fire()`.
        """
        self._check_timing(urgent, after)
        if self.once and self.count >= 1:
            if getattr(self.env, "on_unhandled_failure", "stop") == "stop":
                raise RuntimeError(f"signal '{self.name}' is one-shot and has fired")
            logger.warning(
                "signal '%s' is one-shot and has already fired; the second failure "
                "is dropped",
                self.name,
            )
            return
        self.count += 1
        self.ok, self.value = False, reason
        if self.once:
            event = self._event
        else:
            event, self._event = self._event, self.env.event()
            for callback in self.callbacks:
                self._event.callbacks.append(callback)
        # A re-arming signal's failure is always defused, so a firing nobody waits
        # for is not an error. A one-shot signal's failure is defused only under
        # `continue`; under `stop` it is left to stop the run, as SimPy's event does.
        if not self.once or getattr(self.env, "on_unhandled_failure", "stop") == (
            "continue"
        ):
            event.defused = True
        if urgent or after:
            self._schedule(False, SignalFailed(self.name, reason), urgent, after)
        else:
            event.fail(SignalFailed(self.name, reason))
        self._notify()

    def _check_timing(self, urgent: bool, after: float) -> None:
        """Refuses `urgent` or `after` for a signal that re-arms, or a negative `after`."""
        if (urgent or after) and not self.once:
            raise ValueError(
                "`urgent` and `after` need a one-shot signal (`once=True`); "
                "a re-arming signal fires now"
            )
        if after < 0:
            raise ValueError(f"after must be 0 or more, got {after:g}")

    def _schedule(self, ok: bool, value: Any, urgent: bool, after: float) -> None:
        """Triggers the one-shot event with a priority and a delay, as
        `env.schedule(event, priority, delay)` does after `succeed()` or `fail()`."""
        event = self._event
        event._ok, event._value = ok, value
        self.env.schedule(event, URGENT if urgent else NORMAL, after)

    def _notify(self) -> None:
        notify = getattr(self.env, "notify_change", None)
        if notify is not None:
            notify()

    def __repr__(self) -> str:
        return f"Signal({self.name!r}, count={self.count}, ok={self.ok})"
