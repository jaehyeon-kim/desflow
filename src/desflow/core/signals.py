"""Signals: named events that tasks and processes fire and wait for.

A `Signal` wraps SimPy's shared-event pattern: one `simpy.Event` that every waiter
yields, succeeded or failed when the signal fires. By default the signal fires once
and keeps that event, so a waiter that starts after the firing resumes at once. With
`once=False` the event is replaced by a fresh one at the firing, so the signal can fire
again; every waiter of one firing resumes, and a waiter that starts after a firing
waits for the next.

`Signals` holds what a model declares for its signals, and each run's `SignalRun` the
signals of that run, which it fires and whose `on:` actions it runs.
"""

import functools
import logging
import operator
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

from simpy import Environment, Event
from simpy.events import NORMAL, URGENT

from desflow.core.families import family_of
from desflow.models.flow import Entity

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


class Signals:
    """What a model declares for its signals.

    Attributes:
        names: The signals, in the order they were declared.
        once: Whether each signal fires once.
        defused: Whether each signal's failures are defused, as `event.defused =
            True` does.
        on: Each signal's `on:` actions, in normalized form, run when it fires.
    """

    def __init__(self) -> None:
        self.names: List[str] = []
        self.once: Dict[str, bool] = {}
        self.defused: Dict[str, bool] = {}
        self.on: Dict[str, List[Dict[str, Any]]] = {}

    def new_run(
        self,
        env: Environment,
        families: Mapping[str, Any],
        run_action: Callable[..., None],
        end_processed: Callable[[Any], None],
    ) -> "SignalRun":
        """The signals of a new run: one for each declared signal that is not a
        family, whose members the run makes as it names them.

        Args:
            env: The run's environment.
            families: The model's families, by name.
            run_action: Runs one `on:` action, given the action, its owner for the
                messages, the entity and the upstream.
            end_processed: Counts out an entity whose pending end has run.
        """
        run = SignalRun(self, env, families, run_action, end_processed)
        for name in self.names:
            if name not in families:
                run.make(name, name)
        return run


@dataclass
class SignalRun:
    """The signals of one run, by name. Each run's build makes a new one.

    Attributes:
        signals: The declarations.
        env: The run's environment.
        families: The model's families, by name.
        run_action: Runs one `on:` action.
        end_processed: Counts out an entity whose pending end has run.
        made: The signals the run has made, members included.
    """

    signals: Signals
    env: Environment
    families: Mapping[str, Any]
    run_action: Callable[..., None]
    end_processed: Callable[[Any], None]
    made: Dict[str, Signal] = field(default_factory=dict)

    def make(self, name: str, base: str) -> Signal:
        """Makes a signal with the settings of the declared signal `base`, which is
        the signal itself or the family it is a member of, so the run adds nothing to
        the declarations.

        Args:
            name: The signal's name.
            base: The declared signal whose settings it takes.
        """
        signal = Signal(
            self.env,
            name,
            once=self.signals.once.get(base, True),
            defused=self.signals.defused.get(base, False),
        )
        if self.signals.on.get(base):
            signal.attach([functools.partial(self.fired, name)])
        self.made[name] = signal
        return signal

    def fire(
        self,
        name: str,
        ok: bool,
        value: Any,
        entity: Optional[Entity],
        urgent: bool = False,
        after: Optional[float] = None,
        priority: Optional[int] = None,
        defused: bool = False,
    ) -> None:
        """Fires or fails a signal through one path, counting its `on:` actions
        against `entity` so a drain waits for them.

        When the firing is new, the signal has `on:` actions, and `entity` has not
        left, 1 is added to `entity.ends_pending` and the entity is stored on the
        pending event, so the signal's last callback counts it back out.

        A firing with `urgent`, `after` or a `priority` is for a one-shot signal and is
        scheduled with that priority and delay. `defused` is for a failure nobody waits
        for. With `after` it is not counted against
        `entity`, because the entity may leave before the signal fires, so a drain
        can end before the signal's `on:` actions run.

        Args:
            name: The signal's name.
            ok: True fires the signal, False fails it.
            value: The value it fires with, or the reason of the failure.
            entity: The entity its `on:` actions count against; None for none.
            urgent: Schedules the firing as urgent.
            after: The delay before it fires; None for now.
            priority: The priority it is scheduled with; None for the default.
            defused: Marks a failure nobody waits for.
        """
        signal = self.made[name]
        pending = signal.wait()
        if (
            after is None
            and not pending.triggered
            and self.signals.on.get(self.declared(name))
            and entity is not None
            and not entity.left
        ):
            entity.ends_pending += 1
            pending._desflow_entity = entity  # type: ignore[attr-defined]
        if ok:
            signal.fire(value, urgent=urgent, after=after or 0.0, priority=priority)
        else:
            signal.fail(
                value,
                urgent=urgent,
                after=after or 0.0,
                priority=priority,
                defused=defused,
            )

    def declared(self, name: str) -> str:
        """The declared signal whose settings a signal takes: a member's family, or
        the signal itself.

        Args:
            name: The signal's name.
        """
        return family_of(name, self.families) or name

    def fired(self, name: str, event: Any) -> None:
        """The signal's last `on:` callback: runs its actions, then counts the firing
        task's entity out, as a pending `ended` is counted out.

        Args:
            name: The signal's name.
            event: The fired event.
        """
        entity = getattr(event, "_desflow_entity", None)
        for action in self.signals.on.get(self.declared(name), []):
            self.run_action(action, f"signal '{name}'", entity, None)
        if entity is not None:
            self.end_processed(entity)
