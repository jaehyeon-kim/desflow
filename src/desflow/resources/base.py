import bisect
import functools
import itertools
from typing import TYPE_CHECKING, Any, Callable, Optional

import simpy

from desflow.internal import internal_process
from desflow.models.params import CapacityConfig

if TYPE_CHECKING:
    from desflow.core.registry import DynamicValue

# The kinds that register themselves, and the registry method that does it
_SELF_REGISTERING = {
    "resources": "register_resource",
    "stores": "register_store",
    "containers": "register_container",
}


class RankedQueue(list):
    """The waiting requests, gets or puts of a holder, in service order.

    It is a list, so `len(queue)` is how many wait and `queue[0]` is the next to be
    served. `append` inserts an event by its rank and raises `RuntimeError` when
    `maxlen` events wait already, as SimPy's `SortedQueue` does. An event is ranked by
    its `_rank` when it has one, which a resource gives its requests. An event with no
    `_rank` is ranked by its `rank` attribute (0 when it has none) and then by the
    order it arrived in, `ties="fifo"`, or the reverse, `ties="lifo"`.

    Args:
        maxlen: The most events that can wait, or None for no limit.
        ties: `fifo` serves equal ranks in the order they arrived, `lifo` the latest
            first.
    """

    def __init__(self, maxlen: Optional[int] = None, ties: str = "fifo") -> None:
        super().__init__()
        self.maxlen = maxlen
        self.ties = ties
        self._sequence = itertools.count()

    def append(self, item: Any) -> None:
        """Inserts an event by its rank.

        Args:
            item: The request, get or put event.

        Raises:
            RuntimeError: If `maxlen` events wait already.
        """
        if self.maxlen is not None and len(self) >= self.maxlen:
            raise RuntimeError("Cannot append event. Queue is full.")
        if getattr(item, "_rank", None) is None:
            order = next(self._sequence)
            item._rank = (
                getattr(item, "rank", 0),
                order if self.ties == "fifo" else -order,
            )
        bisect.insort(self, item, key=lambda waiting: waiting._rank)

    def remove(self, item: Any) -> bool:  # type: ignore[override]
        """Takes an event out.

        Args:
            item: The event to take out.

        Returns:
            bool: True if it was taken out, False, with nothing changed, if it is
                not in the queue.
        """
        for index, waiting in enumerate(self):
            if waiting is item:
                del self[index]
                return True
        return False


def check_ties(
    get_ties: Optional[str], put_ties: Optional[str], holder_type: Optional[Any]
) -> None:
    """Checks the `get_ties` and `put_ties` of a store or container.

    Args:
        get_ties: None, `fifo` or `lifo`.
        put_ties: None, `fifo` or `lifo`.
        holder_type: The `type` the holder is built from, or None for the default.

    Raises:
        ValueError: If a value is not None, `fifo` or `lifo`, or one is given with a
            `type`, whose events and queues are its own.
    """
    for name, value in (("get_ties", get_ties), ("put_ties", put_ties)):
        if value is not None and value not in ("fifo", "lifo"):
            raise ValueError(f"{name} must be 'fifo' or 'lifo', got {value!r}")
        if value is not None and holder_type is not None:
            raise ValueError(
                f"{name} ranks the waiting events of the default queue, so it cannot "
                f"be given with a type, which sets its own queues"
            )


class _Ranked:
    """Mixed into a put or get event class: a `rank` keyword ranks the event in the
    queue of a holder that has a `RankedQueue`, lowest first. With no rank, an event
    entering a ranked queue has rank 0."""

    def __init__(self, resource: Any, *args: Any, rank: Any = None, **kwargs: Any):
        if rank is not None:
            self.rank = rank
        super().__init__(resource, *args, **kwargs)  # type: ignore[call-arg]


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


class _WrappedHolder(BaseDynamicResource):
    """A store or container: a SimPy holder, `_holder`, whose capacity follows the
    registry and whose waiting puts and gets this object shows.

    `DynamicStore` and `DynamicContainer` build on it. `DynamicResource` does not,
    because it is the SimPy resource itself and assigns its own `put_queue`.
    """

    _holder: Any

    def _wrap(
        self,
        build: Callable[[Any], Any],
        capacity: Any,
        smallest: Any,
        on_change: Callable[[], None],
        get_ties: Optional[str],
        put_ties: Optional[str],
    ) -> None:
        """Builds the SimPy holder and ranks its queues as `get_ties` and `put_ties`
        say.

        Args:
            build: Builds the holder from the capacity it is given.
            capacity: The capacity the run starts with, which may be 0.
            smallest: The capacity to build with when `capacity` is 0, 1 in the
                holder's own number type.
            on_change: Called once each time a put or get changes the holder.
            get_ties: None, `fifo` or `lifo`, as `check_ties` allows.
            put_ties: None, `fifo` or `lifo`, as `check_ties` allows.
        """
        # SimPy refuses a capacity of 0, which desflow accepts because a capacity
        # changes during a run; the holder is built with 1 and held at 0, as a
        # capacity change sets it
        self._holder = build(capacity or smallest)
        self._holder._on_change = on_change
        self._holder._capacity = capacity
        if get_ties is not None:
            self._holder.get_queue = RankedQueue(ties=get_ties)
        if put_ties is not None:
            self._holder.put_queue = RankedQueue(ties=put_ties)
        self.PutQueue = self._holder.put_queue.__class__
        self.GetQueue = self._holder.get_queue.__class__

    @property
    def put_queue(self) -> list:
        """list: The waiting puts, SimPy's `put_queue`."""
        return self._holder.put_queue

    @property
    def get_queue(self) -> list:
        """list: The waiting gets, SimPy's `get_queue`."""
        return self._holder.get_queue

    @staticmethod
    def _waiting(events: list) -> int:
        """The events of a queue that still wait. A put or get granted at once is
        not counted, although SimPy removes it from its queue only after the grant."""
        return sum(1 for event in events if not event.triggered)
