import functools
from typing import Any, Callable, Optional

import simpy

from desflow.core.internal import internal_process
from desflow.core.registry import DynamicValue
from desflow.models.params import CapacityConfig

# The kinds that register themselves, and the registry method that does it
_SELF_REGISTERING = {
    "resources": "register_resource",
    "stores": "register_store",
    "containers": "register_container",
}


def _notifying(method: Callable[..., Any]) -> Callable[..., Any]:
    """Makes a `_do_put` or `_do_get` report a change of the holder's state once.

    The outermost call compares the state before and after and reports a change, so
    an override that calls `super()` reports once, and an override that does not
    still reports.
    """

    @functools.wraps(method)
    def wrapper(self: Any, event: Any) -> Optional[bool]:
        if self._depth:
            return method(self, event)
        before = self._state()
        self._depth += 1
        try:
            done = method(self, event)
        finally:
            self._depth -= 1
        if self._state() != before and self._on_change is not None:
            self._on_change()
        return done

    wrapper._desflow_notifying = True  # type: ignore[attr-defined]
    return wrapper


class _Notifier:
    """What `StoreBase` and `ContainerBase` share: change and queue-length reports.

    A change of the state (`_state()`, the item count of a store or the level of a
    container) made by a `_do_put` or `_do_get` calls `_on_change` once, whichever
    class defines the method: `__init_subclass__` wraps the method a subclass
    inherits or defines. A change of the length of `put_queue` or `get_queue`
    calls `env.notify_queue_change()`, when the environment has it, after each
    trigger and after a `cancel()` of a put or get event.
    """

    _on_change: Optional[Callable[[], None]] = None
    """Called after the state changed; set by the holder after construction."""
    _depth = 0
    _queue_lens = (0, 0)

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        for name in ("_do_put", "_do_get"):
            method = getattr(cls, name, None)
            if method is not None and not getattr(method, "_desflow_notifying", False):
                setattr(cls, name, _notifying(method))

    def _state(self) -> Any:
        raise NotImplementedError

    def _check_queues(self) -> None:
        """Reports a change of the queue lengths since the last report."""
        lens = (len(self.put_queue), len(self.get_queue))  # type: ignore[attr-defined]
        if lens != self._queue_lens:
            self._queue_lens = lens
            notify = getattr(self._env, "notify_queue_change", None)  # type: ignore[attr-defined]
            if notify is not None:
                notify()

    def _trigger_put(self, get_event: Any) -> None:
        super()._trigger_put(get_event)  # type: ignore[misc]
        self._check_queues()

    def _trigger_get(self, put_event: Any) -> None:
        super()._trigger_get(put_event)  # type: ignore[misc]
        self._check_queues()


class _CancelReports:
    """Mixed into a put or get event class: a cancel reports the queue change."""

    def cancel(self) -> None:
        super().cancel()  # type: ignore[misc]
        self.resource._check_queues()  # type: ignore[attr-defined]


class BaseDynamicResource:
    """A resource, store or container whose capacity follows the registry.

    It reads `<sim_id>.<category>.<obj_id>.current_cap` and `.max_cap`, and a
    process applies every change to them; each subclass says how. `DynamicResource`,
    `DynamicStore` and `DynamicContainer` build on it.

    Args:
        env: The environment, which has a registry.
        sim_id: The simulation's id.
        obj_id: The object's name.
        category: `resources`, `stores` or `containers`.
    """

    def __init__(self, env: simpy.Environment, sim_id: str, obj_id: str, category: str):
        self.env = env
        self.sim_id = sim_id
        self.obj_id = obj_id
        self._category = category
        # The environment's built-in statistics, when it has them. A plain SimPy
        # environment with a registry works too, without statistics.
        self._stats = getattr(env, "stats", None)

        path = f"{sim_id}.{category}.{obj_id}"
        try:
            env.registry.get(f"{path}.current_cap")
        except KeyError:
            # Built with no blueprint or builder entry, as `simpy.Resource(env)` is:
            # the object registers itself with the default capacity
            if category not in _SELF_REGISTERING:
                raise
            getattr(env.registry, _SELF_REGISTERING[category])(
                sim_id, obj_id, CapacityConfig()
            )
        self._current_cap_val: DynamicValue = env.registry.get(f"{path}.current_cap")
        self._max_cap_val: DynamicValue = env.registry.get(f"{path}.max_cap")

        internal_process(self.env, self._watch_capacity())
        internal_process(self.env, self._watch_max_capacity())

    def _register(self):
        """Adds this object to the statistics; each subclass calls it once it is built."""
        if self._stats is not None:
            self._stats.add(self._category, self.obj_id, self)

    def _changed(self):
        """Tells the statistics that a reading changed, so time averages stay exact,
        and the environment, so waits on a shared condition check it again."""
        if self._stats is not None:
            self._stats.changed(self._category, self.obj_id)
        notify = getattr(self.env, "notify_change", None)
        if notify is not None:
            notify()

    def _watch_capacity(self):
        while True:
            # Wakes up immediately if a signal is in the Store
            yield self._current_cap_val.wait_for_change()
            # Each subclass converts the value, so a container keeps a fractional capacity
            self._handle_capacity_change(self._current_cap_val.value)

    def _watch_max_capacity(self):
        while True:
            # A new max_cap re-applies current_cap, so a lower limit takes effect at once
            yield self._max_cap_val.wait_for_change()
            self._handle_capacity_change(self._current_cap_val.value)

    def _handle_capacity_change(self, new_target: float):
        pass  # To be implemented by subclasses
