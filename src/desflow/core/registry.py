import copy
import logging
import math
import numbers
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from simpy import Environment
from simpy.resources.store import StoreGet

from desflow.models.distributions import get_distribution
from desflow.internal import InternalStore
from desflow.utils import suggestion
from desflow.models.params import (
    CapacityConfig,
    DistributionConfig,
    SimParameter,
    check_distribution,
)

logger = logging.getLogger(__name__)


_AS_GIVEN_KINDS = ("service", "arrival", "containers")
_CAPACITY_KINDS = ("resources", "containers", "stores")
_CAPACITY_ATTRS = ("current_cap", "max_cap")


def _is_number(value: Any) -> bool:
    """Whether `value` is an int or a float, and not a bool."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


@dataclass(frozen=True)
class _PathKind:
    """What a registry path is, recorded when the path is registered.

    Attributes:
        kind (str): The section it belongs to: `arrival`, `service`, `resources`,
            `containers`, `stores`, `breakdown` or `variables`.
        config (str): The path of its config object, as `Shop.resources.desk`.
        attr (str): The attribute or key of that config it sets, as `current_cap`.
    """

    kind: str
    config: str
    attr: str


def _stored_as_given(kind: str, current: Any, new: Any) -> bool:
    """Whether a live update is stored as written, with no cast to the current type.

    That holds for a distribution parameter or a container value that gets a number or
    a bool, and for a variable that gets a finite number, when the current value is a
    number. Infinity and NaN are not finite, so they go to the cast to the current
    type, which refuses them over an integer variable.
    """
    if not _is_number(current):
        return False
    if kind in _AS_GIVEN_KINDS:
        return _is_number(new) or isinstance(new, bool)
    if kind == "variables" and isinstance(new, bool):
        return True
    if kind == "variables" and _is_number(new):
        return math.isfinite(new)
    return False


def queue_limit_value(path: str, value: Any) -> Optional[int]:
    """The waiting-line limit a write to a `queue_limit` path means.

    Args:
        path: The path, for the message.
        value: None for no limit, a whole number of 0 or more (a NumPy integer or a
            whole float too), or text that is one.

    Returns:
        Optional[int]: The limit, or None for no limit.

    Raises:
        ParamRejected: If `value` is anything else, such as a negative number, a
            fraction, `true` or text that is not a whole number.
    """
    number = value
    if isinstance(value, str):
        text = value.strip()
        number = int(text) if _WHOLE_TEXT.fullmatch(text) else value
    if value is None:
        return None
    if (
        isinstance(number, bool)
        or not isinstance(number, numbers.Real)
        or not math.isfinite(number)
        or number != int(number)  # type: ignore[call-overload]
        or number < 0
    ):
        raise ParamRejected(
            path,
            value,
            f"'{path}' is a whole number of 0 or more, or null for no limit, got "
            f"{value!r}",
        )
    return int(number)


def _is_capacity(registered: _PathKind) -> bool:
    """Whether a path is a `current_cap` or `max_cap` of a resource, container or store."""
    return registered.kind in _CAPACITY_KINDS and registered.attr in _CAPACITY_ATTRS


def _infinite_capacity(registered: _PathKind, value: Any) -> bool:
    """Whether `value` is infinity, or the text "inf", written to a capacity path."""
    if not _is_capacity(registered):
        return False
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return False
    return isinstance(value, float) and math.isinf(value) and value > 0


_WHOLE_TEXT = re.compile(r"[+-]?[0-9]+")
_HOLDER_WORDS = {"resources": "resource", "containers": "container", "stores": "store"}


def _refusal(path: str, value: Any, reason: str) -> "ParamRejected":
    """A refusal of a live value, which `update` logs as the others, ending with
    "Update ignored."."""
    refused = ParamRejected(path, value, reason)
    refused._log = f"{reason}. Update ignored."
    return refused


def _read_number(value: Any) -> Any:
    """The number a text means, when the whole text is one; any other value as given."""
    if not isinstance(value, str):
        return value
    text = value.strip()
    if _WHOLE_TEXT.fullmatch(text):
        return int(text)
    try:
        return float(text)
    except ValueError:
        return value


def _capacity_value(path: str, registered: _PathKind, value: Any) -> Any:
    """The `current_cap` or `max_cap` a write to a capacity path means.

    Raises:
        ParamRejected: With the declaration's text, if it is not a whole number of
            0 or more or infinity (a container's: a number of 0 or more). A negative
            `current_cap` passes, because the registry clamps it to 0.
    """
    from desflow.core.holders import check_capacity  # a circular import at module level

    whole = registered.kind != "containers"
    number = _read_number(value)
    holder = registered.config.rsplit(".", 1)[-1]
    word = _HOLDER_WORDS[registered.kind]
    try:
        if isinstance(number, str):
            raise ValueError(
                f"{word} '{holder}' needs a {'whole ' if whole else ''}number of 0 "
                f"or more for {registered.attr}, got {value!r}"
            )
        # A live `current_cap` below 0 is clamped to 0, as the docs say; only its
        # kind (whole, not NaN, not a bool, not -inf) is checked here
        negative_ok = (
            registered.attr == "current_cap"
            and isinstance(number, numbers.Real)
            and not isinstance(number, bool)
            and float(number) > -math.inf
        )
        check_capacity(
            word, holder, registered.attr, abs(number) if negative_ok else number, whole
        )
    except ValueError as exc:
        raise _refusal(path, value, str(exc)) from None
    if math.isinf(number):
        return math.inf
    return int(number) if whole else number


def _level_value(path: str, value: Any) -> Any:
    """The container level a write means: a finite number, read from text."""
    number = _read_number(value)
    if (
        isinstance(number, bool)
        or not isinstance(number, numbers.Real)
        or not math.isfinite(number)
    ):
        holder = path.rsplit(".", 2)[-2]
        raise _refusal(
            path,
            value,
            f"container '{holder}' needs a level that is a finite number, got "
            f"{value!r}",
        )
    # A level written as text is a float, "4" as 4.0
    return float(number) if isinstance(value, str) else number


def _flag_value(path: str, value: Any) -> bool:
    """The bool a write to a true-or-false path means: `true`, `false`, that text, or
    the whole number 1 or 0."""
    if isinstance(value, bool):
        return value
    if isinstance(value, numbers.Integral) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise _refusal(path, value, f"'{path}' is true or false, got {value!r}")


@dataclass(frozen=True, kw_only=True)
class ParamChange:
    """
    One accepted change to a registry path.

    `SimulationRegistry.change` returns it, and each `on_change` callback receives it.
    It is frozen, so a callback cannot alter what the next callback receives.

    Attributes:
        path (str): The dot-notation path that changed, such as
            `Shop.resources.packers.current_cap`.
        old (Any): The value before the change.
        new (Any): The value stored, after any cast or clamp.
        requested (Any): The value the caller asked for.
        at (float): The simulation time at which the change was applied.
        source (Optional[str]): Where the change came from, such as `"ingress"` or
            `"scenario"`. None for a change the model makes itself.
        actor (Optional[str]): Who made the change, such as the ingress connector's
            class name.
        reason (Optional[str]): Why the caller made the change.
    """

    path: str
    old: Any
    new: Any
    requested: Any
    at: float
    source: Optional[str] = None
    actor: Optional[str] = None
    reason: Optional[str] = None

    @property
    def changed(self) -> bool:
        """Whether the stored value differs from the value before the change."""
        return self.old != self.new


class ParamRejected(ValueError):
    """
    Raised by `SimulationRegistry.change` when the registry refuses a value.

    It is a `ValueError`, so a caller that catches `ValueError` also catches it.
    `str(exc)` is the reason.

    Attributes:
        path (str): The path the caller asked to change.
        value (Any): The value the registry refused.
        reason (str): Why the registry refused it.
    """

    def __init__(self, path: str, value: Any, reason: str):
        # `args` holds all three, because SimPy rebuilds an exception that ends a
        # process as `type(exc)(*exc.args)`, and unpickling rebuilds it the same way.
        super().__init__(path, value, reason)
        self.path = path
        self.value = value
        self.reason = reason
        # The log level and line `update` writes for this refusal
        self._level = logging.ERROR
        self._log = reason

    def __str__(self) -> str:
        return self.reason


class DynamicValue:
    """
    The atom of the registry. Holds a value and a SimPy event that triggers whenever the value changes.

    This class is used internally by the `SimulationRegistry` to wrap configuration
    values so that SimPy processes can yield and wait for them to change.

    Attributes:
        env (Environment): The active SimPy environment.
        name (str): The dot-notation path/name of this value (e.g., 'Shop.resources.packers.current_cap').
    """

    def __init__(self, env: Environment, name: str, initial_value: Any):
        self.env = env
        self.name = name
        self._value = initial_value
        # Use a Store to signal changes (prevents missed events)
        self._signal = InternalStore(env, capacity=1)

    @property
    def value(self):
        """The current underlying value."""
        return self._value

    def update(self, new_value: Any):
        """
        Updates the value and signals any waiting SimPy processes.

        Args:
            new_value (Any): The new value to store.
        """
        if self._value != new_value:
            self._value = new_value
            # Signal a change if not already signaled
            if len(self._signal.items) == 0:
                self._signal.put(True)

    def track(self, new_value: Any):
        """
        Records a value the model changed itself, without signalling a change.

        A container's level changes whenever a task puts or takes an amount. The
        registry keeps that level so a reader sees it, but the container that made
        the change must not be told to apply it again.

        Args:
            new_value (Any): The value now in effect.
        """
        self._value = new_value

    def wait_for_change(self) -> StoreGet:
        """
        Returns a SimPy event that will be triggered when the value updates.

        Returns:
            simpy.events.StoreGet: A get-request for the internal signal.
        """
        return self._signal.get()


class SimulationRegistry:
    """
    A central registry that maps dot-notation paths to dynamic simulation parameters.

    The Registry acts as the single source of truth for the simulation's state. It allows
    external streams (like Kafka or Redis) to update parameters while the simulation
    runs, and passes each change to the SimPy processes that use it.

    Attributes:
        env (Environment): The active SimPy environment.
        on_change (List[Callable[[ParamChange], None]]): Callbacks that receive a
            `ParamChange` for every accepted change, in the order they were added.
    """

    def __init__(self, env: Environment):
        self.env = env
        self._values: Dict[str, DynamicValue] = {}
        self._configs: Dict[str, Any] = {}
        # What each path is, recorded when it is registered, so no reader works it
        # out from the text of the path
        self._kinds: Dict[str, _PathKind] = {}
        # Called with a `ParamChange` for every accepted change, in the order added
        self.on_change: List[Callable[[ParamChange], None]] = []

    def get(self, path: str) -> DynamicValue:
        """
        Retrieve the `DynamicValue` object at a specific path.

        Args:
            path (str): The dot-notation path (e.g., 'Shop.arrival.orders.rate').

        Returns:
            DynamicValue: The wrapper object for the requested parameter.

        Raises:
            KeyError: If the path does not exist in the registry.
        """
        if path not in self._values:
            raise KeyError(f"Path '{path}' not found in Simulation Registry.")
        return self._values[path]

    def get_config(self, path: str) -> Any:
        """
        Retrieve the live configuration object (e.g., `DistributionConfig`).

        Args:
            path (str): The dot-notation path (e.g., 'Shop.service.packing').

        Returns:
            Any: The synchronized configuration object.

        Raises:
            KeyError: If the config path does not exist in the registry.
        """
        if path not in self._configs:
            raise KeyError(f"Config path '{path}' not found in Simulation Registry.")
        return self._configs[path]

    def is_queue_limit(self, path: str) -> bool:
        """
        Whether `path` is a resource's `queue_limit`, as registered.

        Args:
            path (str): A registry path.

        Returns:
            bool: True for a registered `queue_limit` path.
        """
        registered = self._kinds.get(path)
        return (
            registered is not None
            and registered.kind == "resources"
            and registered.attr == "queue_limit"
        )

    def paths(self) -> List[str]:
        """
        Returns every value path, such as `Shop.resources.packers.current_cap`.

        Returns:
            List[str]: The paths `get` and `update` take, in the order they were
                registered.
        """
        return list(self._values)

    def configs(self) -> Dict[str, Any]:
        """
        Returns every configuration object by its path, such as `Shop.service.packing`.

        An experiment records them as the model's compiled parameters. The objects
        are the live ones, so read them rather than change them.

        Returns:
            Dict[str, Any]: Path to `DistributionConfig`, `CapacityConfig` or the
                variables mapping, in the order they were registered.
        """
        return dict(self._configs)

    def change(
        self,
        path: str,
        value: Any,
        *,
        source: Optional[str] = None,
        actor: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> ParamChange:
        """
        Changes a value as `update` does, and reports what happened.

        The checks are the ones `update` runs. Where `update` logs a refusal and goes
        on, `change` raises `ParamRejected`, for a caller that must know. A value that
        is cast or clamped is accepted, and `ParamChange.new` shows the value stored.
        Every `on_change` callback receives the change before `change` returns.

        Args:
            path (str): The dot-notation path to change.
            value (Any): The new value.
            source (Optional[str]): Where the change came from, such as `"ingress"`.
            actor (Optional[str]): Who made the change.
            reason (Optional[str]): Why the change was made.

        Returns:
            ParamChange: The old value, the value stored and the time of the change.

        Raises:
            ParamRejected: If the path does not exist, the value cannot be converted
                to the stored type, or a distribution parameter is out of its range.
        """
        return self._apply(path, value, source, actor, reason)

    def update(
        self,
        path: str,
        new_value: Any,
        *,
        source: Optional[str] = None,
        actor: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        """
        Update a value safely and synchronize its parent configuration object.

        Includes dynamic type casting to protect the simulation from crashing if
        external systems send improperly typed data (e.g., sending the string "5"
        instead of the integer 5). A number written over a number is stored as given
        on a distribution parameter, a container value and a variable, so 0.5 over 2
        stays 0.5 and 9.5 over a variable of 10 stays 9.5.

        A capacity path (`current_cap` or `max_cap`) takes `inf` and the text `"inf"`
        as no limit, whatever type it held.

        A distribution parameter outside its range (for example a negative std) is
        refused with an error log line, as a value of the wrong type is, and the old
        value is kept. A `current_cap` outside `[0, max_cap]` is clamped to that range
        with a warning log line, also when a lower `max_cap` arrives, and so is a
        container `level` outside `[0, current_cap]`. A path the model does not have,
        such as a parameter the distribution's family does not take, is refused with a
        warning log line. `update` never raises for a refused value; `change` does, and
        returns what was applied.

        Args:
            path (str): The dot-notation path to update.
            new_value (Any): The new value to apply.
            source (Optional[str]): Where the change came from, such as `"ingress"`.
            actor (Optional[str]): Who made the change.
            reason (Optional[str]): Why the change was made.
        """
        try:
            self._apply(path, new_value, source, actor, reason)
        except ParamRejected as exc:
            logger.log(exc._level, exc._log)

    def _apply(
        self,
        path: str,
        new_value: Any,
        source: Optional[str],
        actor: Optional[str],
        reason: Optional[str],
    ) -> ParamChange:
        """Applies one change for `change` and `update`, and calls the callbacks.

        Raises:
            ParamRejected: For the three refusals, carrying the level and the text
                that `update` logs.
        """
        validated_value = self.check_value(path, new_value)
        return self._store(path, new_value, validated_value, source, actor, reason)

    def check_value(self, path: str, new_value: Any) -> Any:
        """The value a path accepts for `new_value`, cast to the path's type.

        The one check a change, a scenario step and an `on:` action share, so each
        refuses the same values with the same text. A capacity, `max_cap` or queue
        limit is a whole number of 0 or more (a container's capacity any number of 0
        or more), read from text only when the text is such a number; a container
        level is a finite number; a true-or-false path takes `true`, `false` or the
        text "true" or "false"; a variable takes `true` as 1, and a
        distribution parameter refuses it.

        Args:
            path: The registered path.
            new_value: The value to write.

        Raises:
            ParamRejected: For an unknown path, a value the path refuses, or a
                distribution parameter the distribution refuses.
        """
        if path not in self._values:
            rejected = ParamRejected(
                path,
                new_value,
                f"'{path}' is not a registry path" + suggestion(path, self._values),
            )
            rejected._level = logging.WARNING
            rejected._log = f"Attempted to update non-existent path: {path}"
            raise rejected

        current_val = self._values[path].value
        validated_value = new_value
        registered = self._kinds[path]
        kind, parent_path, attr = registered.kind, registered.config, registered.attr

        # Type Validation & Casting
        if self.is_queue_limit(path):
            # A whole number or null, whether the limit is set now or not
            validated_value = queue_limit_value(path, new_value)
        elif _is_capacity(registered):
            validated_value = _capacity_value(path, registered, new_value)
        elif kind == "containers" and attr == "level":
            validated_value = _level_value(path, new_value)
        elif isinstance(current_val, bool):
            validated_value = _flag_value(path, new_value)
        elif current_val is not None and _infinite_capacity(registered, new_value):
            # No limit: inf, or the text "inf", on a capacity path of any type
            validated_value = math.inf
        elif current_val is not None and _stored_as_given(kind, current_val, new_value):
            # A number written over a number keeps its type, as SimPy's keeps it:
            # 0.5 over 2 is 0.5, 3 over 2.5 is 3, and true over a variable is 1
            # `true` over a distribution parameter stays a bool, which the
            # distribution's own check refuses; over any other number it is 1
            validated_value = (
                int(new_value)
                if isinstance(new_value, bool) and kind not in ("service", "arrival")
                else new_value
            )
        elif current_val is not None:
            expected_type = type(current_val)
            if _is_number(current_val) and kind in _AS_GIVEN_KINDS:
                # Text, such as "0.5" from a connector, is read as a float, as it was
                # when these values were always floats
                expected_type = float
            # If types don't match, attempt a safe cast (e.g., "5" -> 5)
            if not isinstance(new_value, expected_type):
                try:
                    validated_value = expected_type(new_value)
                    logger.debug(
                        f"Cast '{new_value}' to {expected_type.__name__} for '{path}'"
                    )
                except (ValueError, TypeError, OverflowError):
                    rejected = ParamRejected(
                        path,
                        new_value,
                        f"'{path}' holds a {expected_type.__name__}, and "
                        f"{new_value!r} cannot be converted to one",
                    )
                    rejected._log = (
                        f"Type mismatch for '{path}'. Expected {expected_type.__name__}, "
                        f"got {type(new_value).__name__} with value '{new_value}'. Update ignored."
                    )
                    raise rejected

        parent_obj = self._configs.get(parent_path)

        # Check a distribution parameter with the same rules as at load
        if isinstance(parent_obj, DistributionConfig):
            candidate = copy.copy(parent_obj)
            setattr(candidate, attr, validated_value)
            try:
                check_distribution(candidate)
            except ValueError as exc:
                rejected = ParamRejected(
                    path, new_value, f"invalid value for '{path}': {exc}"
                )
                rejected._log = f"Invalid value for '{path}': {exc}. Update ignored."
                raise rejected from exc
        return validated_value

    def _store(
        self,
        path: str,
        new_value: Any,
        validated_value: Any,
        source: Optional[str],
        actor: Optional[str],
        reason: Optional[str],
    ) -> ParamChange:
        """The second phase of `_apply`: clamps the value to its range, stores it,
        keeps the config in step, and calls the callbacks."""
        current_val = self._values[path].value
        changes: List[ParamChange] = []
        registered = self._kinds[path]
        parent_path, attr = registered.config, registered.attr
        parent_obj = self._configs.get(parent_path)

        # Keep a container level within [0, current_cap], what the container holds
        if (
            isinstance(parent_obj, CapacityConfig)
            and attr == "level"
            and validated_value is not None
        ):
            cap = self._values[f"{parent_path}.current_cap"].value
            clamped = type(validated_value)(min(max(validated_value, 0), cap))
            if clamped != validated_value:
                logger.warning(
                    f"level {validated_value} for '{path}' is outside "
                    f"[0, current_cap {cap}]. Stored as {clamped}."
                )
                validated_value = clamped

        # Keep current_cap within [0, max_cap], the range the resource uses
        if isinstance(parent_obj, CapacityConfig) and attr == "current_cap":
            max_cap = self._values[f"{parent_path}.max_cap"].value
            clamped = type(validated_value)(min(max(validated_value, 0), max_cap))
            if clamped != validated_value:
                logger.warning(
                    f"current_cap {validated_value} for '{path}' is outside "
                    f"[0, max_cap {max_cap}]. Stored as {clamped}."
                )
                validated_value = clamped

        # A lower max_cap clamps current_cap first, so no reader sees it above
        if (
            isinstance(parent_obj, CapacityConfig)
            and attr == "max_cap"
            and validated_value is not None
        ):
            lowered = self._clamp_current_cap(parent_path, parent_obj, validated_value)
            if lowered is not None:
                sibling = f"{parent_path}.current_cap"
                stored = self._values[sibling].value
                changes.append(
                    ParamChange(
                        path=sibling,
                        old=lowered,
                        new=stored,
                        requested=stored,
                        at=self.env.now,
                        source=source,
                        actor=actor,
                        reason=reason,
                    )
                )

        # Update the DynamicValue (triggers SimPy events)
        self._values[path].update(validated_value)

        # Synchronize the attribute on the original config object
        # Check if the parent is a dictionary (like `variables`)
        if isinstance(parent_obj, dict):
            parent_obj[attr] = validated_value
        elif parent_obj is not None:
            setattr(parent_obj, attr, validated_value)

        # Waits on a shared condition check it again after every change
        notify = getattr(self.env, "notify_change", None)
        if notify is not None:
            notify()

        change = ParamChange(
            path=path,
            old=current_val,
            new=validated_value,
            requested=new_value,
            at=self.env.now,
            source=source,
            actor=actor,
            reason=reason,
        )
        changes.append(change)
        for accepted in changes:
            self._call_on_change(accepted)
        return change

    def _call_on_change(self, change: ParamChange) -> None:
        """Calls each `on_change` callback with `change`, in the order added.

        The loop runs over a copy, so a callback that adds or removes a callback
        affects only the next change. A callback that raises is logged, and the next
        one still runs, because the value is already stored.
        """
        for callback in list(self.on_change):
            try:
                callback(change)
            except Exception:
                logger.exception(
                    f"on_change callback {callback!r} failed for '{change.path}'"
                )

    def set_exactly(
        self,
        path: str,
        value: Any,
        *,
        source: Optional[str] = None,
        actor: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        """
        Sets a path to exactly `value`, raising where `update` would log and go on.

        Replications and experiments set their variants' values this way before the
        clock starts, so a misspelt path or a refused value stops the experiment rather
        than leaving the model's own value in place.

        Args:
            path (str): The dot-notation path.
            value (Any): The value it must hold.
            source (Optional[str]): Where the change came from; see `change`.
            actor (Optional[str]): Who made the change.
            reason (Optional[str]): Why the change was made.

        Raises:
            ParamRejected: A `ValueError` that carries the refused `path`, if the path
                does not exist, or the registry refuses, casts to another value or
                clamps the value, or a `max_cap` would clamp the `current_cap` beside
                it.
        """
        if path not in self._values:
            raise ParamRejected(
                path,
                value,
                f"'{path}' is not a registry path" + suggestion(path, self._values),
            )
        registered = self._kinds[path]
        sibling = f"{registered.config}.current_cap"
        if _is_capacity(registered) and registered.attr == "max_cap":
            current = self._values[sibling].value
            if value is not None and current is not None and current > value:
                # update would lower current_cap to it without a word
                raise ParamRejected(
                    path,
                    value,
                    f"'{path}' cannot be set to {value!r}: '{sibling}' is "
                    f"{current!r}; set current_cap first, to {value!r} or less",
                )
        self.update(path, value, source=source, actor=actor, reason=reason)
        kept = self._values[path].value
        if kept != value:
            raise ParamRejected(
                path,
                value,
                f"'{path}' cannot be set to {value!r}; the registry kept {kept!r}",
            )

    def _clamp_current_cap(
        self, parent_path: str, config: CapacityConfig, max_cap: Any
    ) -> Optional[Any]:
        """Lowers a stored `current_cap` above a new `max_cap` to it, with a warning.

        Returns:
            The `current_cap` before it was lowered, or None if it was not above.
        """
        path = f"{parent_path}.current_cap"
        current = self._values[path].value
        if current is None or current <= max_cap:
            return None
        clamped = type(current)(max_cap)
        logger.warning(
            f"current_cap {current} for '{path}' is above the new max_cap {max_cap}. "
            f"Stored as {clamped}."
        )
        self._values[path].update(clamped)
        config.current_cap = clamped
        return current

    def register_sim_parameter(self, param: SimParameter):
        """
        Takes a `SimParameter` instance and flattens it into the registry.

        Args:
            param (SimParameter): The initial state schema to register.
        """
        prefix = param.sim_id

        # Register Arrivals
        for name, dist_config in param.arrival.items():
            path = f"{prefix}.arrival.{name}"
            self._configs[path] = dist_config
            self._register_dist(path, dist_config, "arrival", prefix)

        # Register Service Steps
        for name, dist_config in param.service.items():
            path = f"{prefix}.service.{name}"
            self._configs[path] = dist_config
            self._register_dist(path, dist_config, "service", prefix)

        # Register Resources, Containers and Stores
        for name, cap_config in param.resources.items():
            self.register_resource(prefix, name, cap_config)
        for name, cap_config in param.containers.items():
            self.register_container(prefix, name, cap_config)
        for name, cap_config in param.stores.items():
            self.register_store(prefix, name, cap_config)

        # Register Breakdowns: each resource's time to failure and repair time
        for name, breakdown in param.breakdowns.items():
            self.register_breakdown(prefix, name, breakdown)

        # Register Custom Variables
        self._configs[f"{prefix}.variables"] = param.variables
        for name, value in param.variables.items():
            path = f"{prefix}.variables.{name}"
            self._values[path] = DynamicValue(self.env, path, value)
            self._kinds[path] = _PathKind("variables", f"{prefix}.variables", name)

    def register_breakdown(self, sim_id: str, name: str, breakdown: Any):
        """
        Registers a resource's time to failure and repair time.

        Args:
            sim_id (str): The simulation ID.
            name (str): The resource's name, which for a member of a family is its
                bracketed name such as `desk[3]`.
            breakdown (Any): The breakdown, with `time_to_failure` and `repair`.
        """
        for part in ("time_to_failure", "repair"):
            path = f"{sim_id}.breakdown.{name}.{part}"
            dist_config = getattr(breakdown, part)
            self._configs[path] = dist_config
            self._register_dist(path, dist_config, "breakdown", sim_id)

    def register_resource(self, sim_id: str, name: str, cap_config: CapacityConfig):
        """
        Registers a resource's capacity paths, filling in what the config leaves out.

        A `current_cap` left as None is 1, and a `max_cap` left as None is the
        `current_cap`. The config is updated to the values in effect.

        Args:
            sim_id (str): The simulation ID.
            name (str): The resource's name.
            cap_config (CapacityConfig): Its capacity, which may leave values as None.

        Raises:
            ValueError: If only `max_cap` is given and it is below 1, the default
                `current_cap`.
        """
        if cap_config.current_cap is None:
            if cap_config.max_cap is not None and cap_config.max_cap < 1:
                raise ValueError(
                    f"resource '{name}' has max_cap {cap_config.max_cap} and no "
                    f"current_cap, whose default is 1; set current_cap to "
                    f"{cap_config.max_cap} or less"
                )
            cap_config.current_cap = 1
        if cap_config.max_cap is None:
            cap_config.max_cap = cap_config.current_cap
        path = f"{sim_id}.resources.{name}"
        self._configs[path] = cap_config
        self._register_cap(path, cap_config, "resources")
        if getattr(cap_config, "limit_path", True):
            limit = getattr(cap_config, "queue_limit", None)
            self._values[f"{path}.queue_limit"] = DynamicValue(
                self.env, f"{path}.queue_limit", limit
            )
            self._kinds[f"{path}.queue_limit"] = _PathKind(
                "resources", path, "queue_limit"
            )

    def _forget(self, path: str) -> None:
        """
        Removes a path, if the registry has it.

        Args:
            path (str): The dot-notation path.
        """
        self._values.pop(path, None)
        self._kinds.pop(path, None)

    def register_container(self, sim_id: str, name: str, cap_config: CapacityConfig):
        """
        Registers a container's capacity paths and its `.level` path.

        A `current_cap` left as None is the `max_cap` if that is given and no limit
        (infinity) if not, and a `max_cap` left as None is the `current_cap`. The
        config is updated to the values in effect.

        Args:
            sim_id (str): The simulation ID.
            name (str): The container's name.
            cap_config (CapacityConfig): Its capacity and starting level.
        """
        self._fill_unbounded(cap_config)
        path = f"{sim_id}.containers.{name}"
        self._configs[path] = cap_config
        self._register_cap(path, cap_config, "containers")
        level = getattr(cap_config, "level", 0)
        self._values[f"{path}.level"] = DynamicValue(self.env, f"{path}.level", level)
        self._kinds[f"{path}.level"] = _PathKind("containers", path, "level")

    def register_store(self, sim_id: str, name: str, cap_config: CapacityConfig):
        """
        Registers a store's capacity paths, filling in what the config leaves out.

        A `current_cap` left as None is the `max_cap` if that is given and no limit
        (infinity) if not, and a `max_cap` left as None is the `current_cap`. The
        config is updated to the values in effect.

        Args:
            sim_id (str): The simulation ID.
            name (str): The store's name.
            cap_config (CapacityConfig): Its capacity, which may leave values as None.
        """
        self._fill_unbounded(cap_config)
        path = f"{sim_id}.stores.{name}"
        self._configs[path] = cap_config
        self._register_cap(path, cap_config, "stores")

    @staticmethod
    def _fill_unbounded(cap_config: CapacityConfig) -> None:
        """Fills in what a store's or container's config leaves out: no limit.

        A `current_cap` left as None is the `max_cap` if that is given and infinity
        if not, and a `max_cap` left as None is the `current_cap`. The config is
        updated to the values in effect.
        """
        if cap_config.current_cap is None:
            cap_config.current_cap = (
                cap_config.max_cap if cap_config.max_cap is not None else math.inf
            )
        if cap_config.max_cap is None:
            cap_config.max_cap = cap_config.current_cap

    def _register_dist(
        self, path_prefix: str, dist_config: Any, kind: str, sim_id: str
    ):
        """Internal: registers one path per parameter its family declares.

        It also names the config's random sequence: its path without the sim id.
        The values of an `empirical` distribution are lists, taken at load, so they
        get no path.
        """
        dist_config.stream = path_prefix[len(sim_id) + 1 :]
        for name in get_distribution(dist_config.dist).live:
            value = getattr(dist_config, name)
            if value is not None:
                path = f"{path_prefix}.{name}"
                self._values[path] = DynamicValue(self.env, path, value)
                self._kinds[path] = _PathKind(kind, path_prefix, name)

    def _register_cap(self, path_prefix: str, cap_config: Any, kind: str):
        """Internal: Flattens a CapacityConfig into its two paths.

        A `current_cap` outside `[0, max_cap]` is clamped to that range with a warning,
        as a live update is, and the config is updated, so `explain` reads the
        capacity the run uses.
        """
        current_cap, max_cap = cap_config.current_cap, cap_config.max_cap
        clamped = type(current_cap)(min(max(current_cap, 0), max_cap))
        if clamped != current_cap:
            logger.warning(
                f"current_cap {current_cap} for '{path_prefix}.current_cap' is "
                f"outside [0, max_cap {max_cap}]. Stored as {clamped}."
            )
            current_cap = cap_config.current_cap = clamped
        self._values[f"{path_prefix}.current_cap"] = DynamicValue(
            self.env, f"{path_prefix}.current_cap", current_cap
        )
        self._values[f"{path_prefix}.max_cap"] = DynamicValue(
            self.env, f"{path_prefix}.max_cap", max_cap
        )
        for attr in _CAPACITY_ATTRS:
            self._kinds[f"{path_prefix}.{attr}"] = _PathKind(kind, path_prefix, attr)
