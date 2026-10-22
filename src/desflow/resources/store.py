import heapq
import itertools
import math
from typing import Any, Iterable, Optional, Type, Union

from simpy import Environment, Event, FilterStore, PriorityItem, PriorityStore, Store
from simpy.resources.base import BoundClass
from simpy.resources.store import FilterStoreGet, StoreGet, StorePut

from desflow.resources.base import BaseDynamicResource, _CancelReports, _Notifier


def _whole(value: float) -> Union[int, float]:
    """A store's capacity as a whole number; infinity, a store with no limit, stays."""
    return value if math.isinf(value) else int(value)


class DynamicStorePut(_CancelReports, StorePut):
    """The put event of `StoreBase`: SimPy's `StorePut`, whose `cancel()` also reports
    the change of the queue length. Subclass it and set `put = BoundClass(MyPut)`."""


class DynamicStoreGet(_CancelReports, StoreGet):
    """The get event of `StoreBase`: SimPy's `StoreGet`, whose `cancel()` also reports
    the change of the queue length. Subclass it and set `get = BoundClass(MyGet)`."""


class DynamicFilterStoreGet(_CancelReports, FilterStoreGet):
    """The get event of a filtered store: SimPy's `FilterStoreGet`, whose `cancel()`
    also reports the change of the queue length."""


class StoreBase(_Notifier, Store):
    """
    A SimPy `Store` that tells `DynamicStore` about every change, the class to
    subclass for a store of your own.

    Subclass it as you subclass `simpy.Store`: set `PutQueue` or `GetQueue`, set
    `put` or `get` to a `BoundClass` of an event class, or override `_do_put(event)`
    and `_do_get(event)`. Use the subclass with `type=` in `add_store` or `type:
    !python module.Class` in a blueprint. The constructor is SimPy's,
    `(env, capacity=inf)`; a subclass that defines `__init__` passes its arguments
    on.

    A change of the number of items made by `_do_put` or `_do_get` is reported
    once to the statistics and to the conditions that read the store, whether the
    override calls `super()` or not. A change of the length of `put_queue` or
    `get_queue` is reported to the conditions that read `putters.` or `getters.`
    (and `count`, `items`); a queue class must support `len()` and `remove()`.

    Attributes:
        items: The items held, in the order a get returns them.
        put_queue: The waiting puts, SimPy's queue.
        get_queue: The waiting gets, SimPy's queue.
        put: Makes a `DynamicStorePut`; a `BoundClass`.
        get: Makes a `DynamicStoreGet`; a `BoundClass`.
    """

    put = BoundClass(DynamicStorePut)
    get = BoundClass(DynamicStoreGet)

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)
        # A subclass that is also a simpy.FilterStore takes filtered gets, so its
        # get makes the filtered event, unless it already has a get event class of
        # its own, set on the class or taken from a base or a mixin
        owner = next(base for base in cls.__mro__ if "get" in base.__dict__)
        if issubclass(cls, FilterStore) and owner in (StoreBase, Store, FilterStore):
            cls.get = BoundClass(DynamicFilterStoreGet)

    def _state(self) -> int:
        return len(self.items)

    def _do_put(self, event: StorePut) -> Optional[bool]:
        return super()._do_put(event)

    def _do_get(self, event: StoreGet) -> Optional[bool]:
        return super()._do_get(event)


# The filter a FilterStore get has when it is given none
_NO_FILTER = FilterStoreGet.__init__.__defaults__[0]  # type: ignore[index]


class _Store(StoreBase, FilterStore):
    get = BoundClass(DynamicFilterStoreGet)

    def _do_get(self, event: StoreGet) -> Optional[bool]:
        done = super()._do_get(event)
        # simpy.Store grants one waiting get each time it is triggered, and
        # FilterStore goes on to the next. While no waiting get has a filter, the
        # store serves gets exactly as simpy.Store does; a filtered get needs the
        # next ones looked at, as FilterStore does.
        if any(get.filter is not _NO_FILTER for get in self.get_queue):  # type: ignore[attr-defined]
            return done
        return None


class _PriorityStore(StoreBase, PriorityStore):
    pass


def check_store_type(store_type: Optional[Any], priority: bool = False) -> None:
    """Checks the `type` a store is declared with.

    Args:
        store_type: A subclass of `StoreBase`, or None for the default store.
        priority: Whether the store is declared with `priority`.

    Raises:
        ValueError: If `store_type` is not a subclass of `StoreBase`, or it is given
            with `priority`: a subclass of `StoreBase` sets its own order.
    """
    if store_type is not None and not (
        isinstance(store_type, type) and issubclass(store_type, StoreBase)
    ):
        raise ValueError(f"type must be a subclass of StoreBase, got {store_type!r}")
    if store_type is not None and priority:
        raise ValueError(
            "a store takes a type or priority, not both: a type sets its own order "
            "(subclass StoreBase and PriorityStore for a ranked one)"
        )


class DynamicStore(BaseDynamicResource):
    """
    A wrapper for SimPy FilterStore and PriorityStore with dynamic capacity updates.

    Useful for modeling buffers, queues, or staging areas that hold discrete,
    heterogeneous items. By setting `priority=True`, the store will automatically
    sort incoming items (which must be sortable or wrapped in `simpy.PriorityItem`)
    so high-priority items are retrieved first.

    **Capacity Shrinkage Behavior:**
    If the capacity is dynamically shrunk below the current number of `items`
    in the store, the existing items are NOT destroyed. The store will temporarily
    exist in an "over-capacity" state. All pending and future `put` requests
    will be blocked until downstream processes `get` enough items to bring the
    total item count below the new capacity.

    Attributes:
        env (Environment): The active SimPy simulation environment.
        sim_id (str): The parent simulation ID prefix.
        obj_id (str): The specific store name/identifier.
    """

    def __init__(
        self,
        env: Environment,
        sim_id: str,
        store_id: str,
        priority: bool = False,
        items: Optional[Iterable[Any]] = None,
        type: Optional[Type[StoreBase]] = None,
    ):
        """
        Initializes the DynamicStore and binds its capacity to the registry.

        Args:
            env (Environment): The SimPy environment (must include Registry interface).
            sim_id (str): The parent simulation ID.
            store_id (str): The unique name of this store.
            priority (bool): If True, uses `simpy.PriorityStore` instead of
                `simpy.FilterStore`, which serves first in, first out as
                `simpy.Store` does and also takes a filter. Defaults to False.
            items (Iterable, optional): The items it holds when the run starts, in
                the order a get returns them. A priority store takes `(priority,
                item)` pairs and holds each as a `PriorityItem`, lowest priority
                first and in the order given among equal priorities.
            type (Type[StoreBase], optional): A subclass of `StoreBase` to build the
                store from, in place of the default; it is built as
                `type(env, capacity=...)`. Defaults to None, the default store.

        Raises:
            ValueError: If there are more items than the capacity, a priority
                store's item is not a `(priority, item)` pair, `type` is not a
                subclass of `StoreBase`, or `type` is given with `priority`.
        """
        check_store_type(type, priority)
        super().__init__(env, sim_id, store_id, "stores")

        max_cap = _whole(self._max_cap_val.value)
        # Prevent initialization out-of-bounds
        initial_capacity = max(0, min(_whole(self._current_cap_val.value), max_cap))

        # Dynamically select the underlying SimPy storage engine
        self.priority = priority
        # A rising number for every priority item, so equal priorities keep the
        # order they were added in and their items are never compared
        self._order = itertools.count()
        kind = type or (_PriorityStore if priority else _Store)
        # SimPy refuses a capacity of 0, which desflow accepts because a capacity
        # changes during a run; the store is built with 1 and held at 0, as a
        # capacity change sets it
        self._store = kind(env, capacity=initial_capacity or 1)
        self._store._on_change = self._changed
        self._store._capacity = initial_capacity
        start = list(items or [])
        if len(start) > initial_capacity:
            raise ValueError(
                f"store '{store_id}' holds {initial_capacity} items, so it cannot "
                f"start with {len(start)}"
            )
        for item in start:
            if priority:
                if not (isinstance(item, (tuple, list)) and len(item) == 2):
                    raise ValueError(
                        f"a priority store starts with (priority, item) pairs, got "
                        f"{item!r}"
                    )
                heapq.heappush(self._store.items, self.wrap(item[1], item[0]))
            else:
                self._store.items.append(item)
        self._register()

    def wrap(self, item: Any, priority: float) -> PriorityItem:
        """
        Wraps an item for a priority store, ranked by `priority` and then by order.

        The builder and YAML put every item through this, so equal priorities come
        out first in, first out, which a plain `PriorityItem` does not promise.

        Args:
            item: The item.
            priority: Its priority, lowest returned first.

        Returns:
            PriorityItem: The item, ranked by `(priority, order)`; `.item` is the item.
        """
        return PriorityItem((priority, next(self._order)), item)

    @property
    def capacity(self) -> int:
        """
        int: The current active maximum slot capacity of the store.
        This value reflects the live state from the Registry.
        """
        return self._store.capacity

    @property
    def items(self) -> list:
        """
        list: The actual list of distinct item objects currently residing
        in the store.
        """
        return self._store.items

    @property
    def level(self) -> int:
        """int: The number of items held."""
        return len(self._store.items)

    @property
    def getters_waiting(self) -> int:
        """int: Gets waiting for an item. A get granted at once is not counted,
        although SimPy removes it from `get_queue` only after the grant."""
        return sum(1 for event in self._store.get_queue if not event.triggered)

    @property
    def putters_waiting(self) -> int:
        """int: Puts waiting for room. A put granted at once is not counted,
        although SimPy removes it from `put_queue` only after the grant."""
        return sum(1 for event in self._store.put_queue if not event.triggered)

    def put(self, *args: Any, **kwargs: Any) -> Event:
        """
        Request to put a specific distinct item into the store.

        Args:
            *args: The item to place into the store. A custom `type` whose `put`
                takes more arguments gets them all.
            **kwargs: Keyword arguments, passed to the store's `put`.

        Returns:
            simpy.events.Event: A SimPy event that triggers when a slot is available.
        """
        return self._store.put(*args, **kwargs)

    def get(self, *args: Any, **kwargs: Any) -> StoreGet:
        """
        Request to get the next available item from the store.

        Args:
            *args: The filter, if any. A custom `type` whose `get` takes more
                arguments gets them all.
            **kwargs: Keyword arguments, passed to the store's `get`. `filter`, for a
                store without priority, is a function of an item that
                returns True for an item this get may take, as SimPy's
                `FilterStore.get` takes. The get takes the oldest matching item and
                waits while none matches; a waiting get that matches nothing does
                not stop a later one from taking an item that matches it.

        Returns:
            simpy.resources.store.StoreGet: A SimPy event that yields the
                requested item object when available. Its `cancel()` takes a
                waiting get out of the queue.

        Raises:
            ValueError: If a priority store is given a filter: SimPy has no store
                that both ranks and filters.
        """
        # A filter of None is no filter
        if args and args[0] is None:
            args = args[1:]
        if kwargs.get("filter", 0) is None:
            del kwargs["filter"]
        if self.priority and (args or "filter" in kwargs):
            raise ValueError(
                "a priority store takes no filter: SimPy has no store that both "
                "ranks and filters its items"
            )
        return self._store.get(*args, **kwargs)

    def _handle_capacity_change(self, new_target: float):
        """
        Internal callback triggered when the Registry capacity value changes.
        Updates the physical SimPy store and processes pending events.

        Args:
            new_target (float): The new slot capacity limit, rounded down to a whole number.
        """
        # Safely bind the new target to absolute physical limits [0, max_cap]
        max_cap = _whole(self._max_cap_val.value)
        new_target = max(0, min(_whole(new_target), max_cap))

        self._store._capacity = new_target

        # If capacity grew, pending put requests might now have open slots to succeed.
        # This exact same trigger works for both Store and PriorityStore.
        if self._store.put_queue:
            self._store._trigger_put(None)
