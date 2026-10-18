"""Signals: named events that tasks and processes fire and wait for.

A `Signal` wraps SimPy's shared-event pattern: one pending `simpy.Event` that every
waiter yields, succeeded or failed when the signal fires, and replaced by a fresh
one at once, so the signal can fire again. Every waiter of one firing resumes; a
waiter that starts after a firing waits for the next.
"""

from typing import Any, Optional

from simpy import Environment, Event


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

    def __init__(self, env: Environment, name: str = "signal"):
        """
        Creates a signal that has not fired yet.

        Args:
            env: The environment.
            name: Its name, as a blueprint's `signals` and `signal.<name>` use it.
        """
        self.env = env
        self.name = name
        self.count = 0
        self.ok: Optional[bool] = None
        self.value: Any = None
        self._event = env.event()

    def wait(self) -> Event:
        """
        Returns the event of the next firing, for a process to yield.

        Returns:
            simpy.Event: Succeeds with the firing's value, or fails with
                `SignalFailed`, when the signal next fires.
        """
        return self._event

    def fire(self, value: Any = None) -> None:
        """
        Fires the signal: every waiter resumes with `value`.

        Args:
            value: What the waiters receive, such as a task's finished payload.
        """
        self.count += 1
        self.ok, self.value = True, value
        event, self._event = self._event, self.env.event()
        event.succeed(value)
        self._notify()

    def fail(self, reason: Any) -> None:
        """
        Fails the signal: every waiter receives `SignalFailed` with the reason.

        A failure nobody waits for is not an error: the event is defused, so the run
        goes on.

        Args:
            reason: Why it failed, such as a task's failure reason.
        """
        self.count += 1
        self.ok, self.value = False, reason
        event, self._event = self._event, self.env.event()
        event.defused = True
        event.fail(SignalFailed(self.name, reason))
        self._notify()

    def _notify(self) -> None:
        notify = getattr(self.env, "notify_change", None)
        if notify is not None:
            notify()

    def __repr__(self) -> str:
        return f"Signal({self.name!r}, count={self.count}, ok={self.ok})"
