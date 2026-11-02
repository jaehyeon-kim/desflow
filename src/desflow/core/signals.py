"""Signals: named events that tasks and processes fire and wait for.

A `Signal` wraps SimPy's shared-event pattern: one `simpy.Event` that every waiter
yields, succeeded or failed when the signal fires. By default the signal fires once
and keeps that event, so a waiter that starts after the firing resumes at once. With
`once=False` the event is replaced by a fresh one at the firing, so the signal can fire
again; every waiter of one firing resumes, and a waiter that starts after a firing
waits for the next.
"""

import logging
import operator
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


def event_priority(priority: Any, urgent: bool = False) -> Optional[int]:
    """The priority a firing asks for, as a whole number.

    Args:
        priority: A whole number, a NumPy integer included, or None for the default.
        urgent: Whether the firing is urgent, which is priority 0, so it takes no
            priority of its own.

    Returns:
        Optional[int]: The priority, or None when none was given.

    Raises:
        TypeError: If `priority` is a boolean or not a whole number.
        ValueError: If `urgent` is given with a priority.
    """
    if priority is None:
        return None
    if isinstance(priority, bool):
        raise TypeError(f"a priority is a whole number, got {priority!r}")
    try:
        found = operator.index(priority)
    except TypeError:
        raise TypeError(f"a priority is a whole number, got {priority!r}") from None
    if urgent:
        raise ValueError("urgent is priority 0, so a firing takes urgent or priority")
    return found


def timing_problem(
    urgent: bool, after: Any, priority: Optional[int], once: bool, once_text: str
) -> Optional[str]:
    """Why a firing cannot ask for `urgent`, `after` or a priority, or None when it can.

    Args:
        urgent: Whether the firing is urgent.
        after: Seconds from now, or None for now.
        priority: The priority asked for, or None. A signal that re-arms refuses any
            number, as it refuses `urgent` and `after`.
        once: Whether the signal fires once.
        once_text: How the caller writes `once: true`, for the message.

    Returns:
        Optional[str]: The message, or None.
    """
    if once:
        return None
    if urgent or after:
        return (
            f"`urgent` and `after` need a one-shot signal ({once_text}); "
            "a re-arming signal fires now"
        )
    if priority is not None:
        return (
            f"`priority` needs a one-shot signal ({once_text}); "
            "a re-arming signal fires now"
        )
    return None


class Signal:
    """
    A named event that fires once, or many times with `once=False`, each firing waking
    every waiter.

    Attributes:
        env: The environment.
        name (str): The signal's name.
        count (int): How many times it has fired or failed.
        ok (Optional[bool]): Whether the last firing succeeded; None before the first.
        value (Any): The value of the last firing, or the failure reason.
        defused (bool): Whether every failure of the signal is defused, as a callback's
            `event.defused = True` does.
    """

    def __init__(
        self,
        env: Environment,
        name: str = "signal",
        once: bool = True,
        defused: bool = False,
    ):
        """
        Creates a signal that has not fired yet.

        Args:
            env: The environment.
            name: Its name, as a blueprint's `signals` and `signal.<name>` use it.
            once: When True, the default, the signal fires once, as SimPy's `Event`
                does: it does not re-arm, so a wait after the firing resumes at once,
                and a second firing is refused. When False it is replaced by a fresh
                event after each firing, so it can fire again.
            defused: When True, a failure with no waiter does not stop the run, as
                a callback that sets `event.defused = True` does for each failure.
        """
        self.env = env
        self.name = name
        self.once = once
        self.defused = defused
        self.count = 0
        self.ok: Optional[bool] = None
        self.value: Any = None
        # Callbacks run when the signal fires, copied onto each new pending event
        self.callbacks: List[Callable[[Any], None]] = []
        self._event = env.event()
        # The event of the latest firing, for `processed`
        self._fired: Optional[Event] = None

    @property
    def processed(self) -> bool:
        """bool: Whether the latest firing's event has been processed, as SimPy's
        `event.processed`; False before any firing. It is `signal.<name>.processed` in
        an expression."""
        return self._fired is not None and self._fired.processed

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

    def fire(
        self,
        value: Any = None,
        urgent: bool = False,
        after: float = 0.0,
        priority: Optional[int] = None,
    ) -> None:
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
            priority: A whole number, 1 when left out, the priority the firing is
                scheduled with: an event at one time runs by priority, lowest first,
                and then in the order it was scheduled, as
                `env.schedule(event, priority=...)` orders it. 0 is what `urgent`
                gives. Needs a one-shot signal, and is not given with `urgent`.

        Raises:
            ValueError: If `urgent`, `after` or a `priority` other than 1 is given to a
                signal that re-arms, `after` is negative, or `urgent` and `priority` are
                both given.
            TypeError: If `priority` is not a whole number.
        """
        priority = event_priority(priority, urgent)
        self._check_timing(urgent, after, priority)
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
            self._fired = self._event
            # Keep the triggered event, so a later wait resumes at once.
            if urgent or after or priority not in (None, NORMAL):
                self._schedule(True, value, urgent, after, priority)
            else:
                self._event.succeed(value)
        else:
            event, self._event = self._event, self.env.event()
            self._fired = event
            for callback in self.callbacks:
                self._event.callbacks.append(callback)
            event.succeed(value)
        self._notify()

    def fail(
        self,
        reason: Any,
        urgent: bool = False,
        after: float = 0.0,
        priority: Optional[int] = None,
        defused: bool = False,
    ) -> None:
        """
        Fails the signal: every waiter receives `SignalFailed` with the reason.

        A failure of a signal that re-arms is always defused, and a failure that nobody
        waits for does not stop the run then. A one-shot signal's failure with no waiter
        is left to stop the run under `on_unhandled_failure: stop`, as SimPy's failed
        event does, unless `defused` is given or the signal is defused.

        Args:
            reason: Why it failed, such as a task's failure reason.
            urgent: As for `fire()`.
            after: As for `fire()`.
            priority: As for `fire()`.
            defused: When True, this failure does not stop the run if nobody waits for
                it, as `event.defused = True` does.

        Raises:
            ValueError: As for `fire()`.
            TypeError: As for `fire()`.
        """
        priority = event_priority(priority, urgent)
        self._check_timing(urgent, after, priority)
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
        self._fired = event
        # A re-arming signal's failure is always defused, so a firing nobody waits
        # for is not an error. A one-shot signal's failure is defused only under
        # `continue`; under `stop` it is left to stop the run, as SimPy's event does.
        if (
            not self.once
            or defused
            or self.defused
            or getattr(self.env, "on_unhandled_failure", "stop") == "continue"
        ):
            event.defused = True
        if urgent or after or priority not in (None, NORMAL):
            self._schedule(
                False, SignalFailed(self.name, reason), urgent, after, priority
            )
        else:
            event.fail(SignalFailed(self.name, reason))
        self._notify()

    def _check_timing(
        self, urgent: bool, after: float, priority: Optional[int] = None
    ) -> None:
        """Refuses `urgent`, `after` or a `priority` other than 1 for a signal that
        re-arms, or a negative `after`."""
        problem = timing_problem(urgent, after, priority, self.once, "`once=True`")
        if problem is not None:
            raise ValueError(problem)
        if after < 0:
            raise ValueError(f"after must be 0 or more, got {after:g}")

    def _schedule(
        self,
        ok: bool,
        value: Any,
        urgent: bool,
        after: float,
        priority: Optional[int] = None,
    ) -> None:
        """Triggers the one-shot event with a priority and a delay, as
        `env.schedule(event, priority, delay)` does after `succeed()` or `fail()`."""
        event = self._event
        event._ok, event._value = ok, value
        if urgent:
            priority = URGENT
        elif priority is None:
            priority = NORMAL
        self.env.schedule(event, priority, after)

    def _notify(self) -> None:
        notify = getattr(self.env, "notify_change", None)
        if notify is not None:
            notify()

    def __repr__(self) -> str:
        return f"Signal({self.name!r}, count={self.count}, ok={self.ok})"
