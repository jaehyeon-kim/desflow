from typing import Any, Optional, Type

from simpy import Container, Environment, Event
from simpy.resources.base import BoundClass
from simpy.resources.container import ContainerGet, ContainerPut

from desflow.core.internal import internal_process
from desflow.resources.base import BaseDynamicResource, _CancelReports, _Notifier


class DynamicContainerPut(_CancelReports, ContainerPut):
    """The put event of `ContainerBase`: SimPy's `ContainerPut`, whose `cancel()` also
    reports the change of the queue length. Subclass it and set
    `put = BoundClass(MyPut)`."""


class DynamicContainerGet(_CancelReports, ContainerGet):
    """The get event of `ContainerBase`: SimPy's `ContainerGet`, whose `cancel()` also
    reports the change of the queue length. Subclass it and set
    `get = BoundClass(MyGet)`."""


class ContainerBase(_Notifier, Container):
    """
    A SimPy `Container` that tells `DynamicContainer` about every change, the class
    to subclass for a container of your own.

    Subclass it as you subclass `simpy.Container`: set `PutQueue` or `GetQueue`, set
    `put` or `get` to a `BoundClass` of an event class, or override `_do_put(event)`
    and `_do_get(event)`. Use the subclass with `type=` in `add_container` or `type:
    !python module.Class` in a blueprint. The constructor is SimPy's,
    `(env, capacity=inf, init=0)`; a subclass that defines `__init__` passes its
    arguments on.

    A change of the level made by `_do_put` or `_do_get` is reported once to the
    statistics and to the conditions that read the container, whether the override
    calls `super()` or not, and whatever it returns. A change of the length of
    `put_queue` or `get_queue` is reported to the conditions that read `putters.` or
    `getters.`; a queue class must support `len()` and `remove()`.

    Attributes:
        level: The amount held.
        put_queue: The waiting puts, SimPy's queue.
        get_queue: The waiting gets, SimPy's queue.
        put: Makes a `DynamicContainerPut`; a `BoundClass`.
        get: Makes a `DynamicContainerGet`; a `BoundClass`.
    """

    put = BoundClass(DynamicContainerPut)
    get = BoundClass(DynamicContainerGet)

    def _state(self) -> float:
        return self._level

    def _do_put(self, event: ContainerPut) -> Optional[bool]:
        return super()._do_put(event)

    def _do_get(self, event: ContainerGet) -> Optional[bool]:
        return super()._do_get(event)


def check_container_type(container_type: Optional[Any]) -> None:
    """Checks the `type` a container is declared with.

    Args:
        container_type: A subclass of `ContainerBase`, or None for the default.

    Raises:
        ValueError: If `container_type` is not a subclass of `ContainerBase`.
    """
    if container_type is not None and not (
        isinstance(container_type, type) and issubclass(container_type, ContainerBase)
    ):
        raise ValueError(
            f"type must be a subclass of ContainerBase, got {container_type!r}"
        )


class DynamicContainer(BaseDynamicResource):
    """
    A wrapper for SimPy Container with dynamic capacity updates.

    Useful for modeling continuous bulk materials (fluids, gases, battery charge) and
    stock. Unlike a standard `simpy.Container`, the capacity of this container listens
    to a `SimulationRegistry` path and can be mutated at runtime.

    `current_cap` is the capacity, the largest level, as it is for a resource. The
    level has a registry path of its own, `<sim_id>.containers.<name>.level`: the
    registry follows every put and get, and a live update to the path sets the level,
    such as a restock, waking the tasks it lets through.

    **Capacity Shrinkage Behavior:**
    If the capacity is dynamically shrunk below the current `level` of the container,
    the material is NOT destroyed. The container will temporarily exist in an
    "overflow" state. All pending and future `put` requests will be strictly
    blocked until downstream processes `get` enough material to drain the `level`
    back below the new, smaller capacity.

    Attributes:
        env (Environment): The active SimPy simulation environment.
        sim_id (str): The parent simulation ID prefix.
        obj_id (str): The specific container name/identifier.
    """

    def __init__(
        self,
        env: Environment,
        sim_id: str,
        container_id: str,
        init: Optional[float] = None,
        type: Optional[Type[ContainerBase]] = None,
    ):
        """
        Initializes the DynamicContainer and binds its capacity to the registry.

        Args:
            env (Environment): The SimPy environment (must include Registry interface).
            sim_id (str): The parent simulation ID.
            container_id (str): The unique name of this container.
            init (float, optional): The amount in the container at simulation start.
                None, the default, takes the registry's `level`, which is the
                `level` of a `ContainerConfig` and 0 for a plain `CapacityConfig`.
            type (Type[ContainerBase], optional): A subclass of `ContainerBase` to
                build the container from, in place of the default; it is built as
                `type(env, init=..., capacity=...)`. Defaults to None.

        Raises:
            ValueError: If `init` is above the capacity, or `type` is not a subclass
                of `ContainerBase`.
        """
        check_container_type(type)
        super().__init__(env, sim_id, container_id, "containers")

        self._level_val = env.registry.get(f"{sim_id}.containers.{container_id}.level")
        max_cap = float(self._max_cap_val.value)
        # Prevent initialization out-of-bounds
        initial_capacity = max(0.0, min(float(self._current_cap_val.value), max_cap))
        if init is None:
            # The registry holds the level as a float, so later updates stay
            # fractional; the configured level keeps the type it was given
            config = env.registry.get_config(f"{sim_id}.containers.{container_id}")
            init = getattr(config, "level", self._level_val.value)
        start = init
        # A number keeps its type, so a whole level stays an int, as SimPy keeps it
        if isinstance(start, bool) or not isinstance(start, (int, float)):
            start = float(start)

        if start > initial_capacity:
            # SimPy's own check, which the capacity of 1 below would not make
            raise ValueError('"init" must be <= "capacity".')
        # SimPy refuses a capacity of 0, which desflow accepts because a capacity
        # changes during a run; the container is built with 1 and held at 0, as a
        # capacity change sets it
        self._container = (type or ContainerBase)(
            env, init=start, capacity=initial_capacity or 1.0
        )
        self._container._on_change = self._level_changed
        self._container._capacity = initial_capacity
        self._level_val.track(self._container.level)
        internal_process(self.env, self._watch_level())
        self._register()

    @property
    def capacity(self) -> float:
        """
        float: The current active maximum capacity of the container.
        This value reflects the live state from the Registry.
        """
        return self._container.capacity

    @property
    def level(self) -> float:
        """float: The current amount of bulk material stored inside the container."""
        return self._container.level

    @property
    def getters_waiting(self) -> int:
        """int: Gets waiting for an amount. A get granted at once is not counted,
        although SimPy removes it from `get_queue` only after the grant."""
        return sum(1 for event in self._container.get_queue if not event.triggered)

    @property
    def putters_waiting(self) -> int:
        """int: Puts waiting for room. A put granted at once is not counted,
        although SimPy removes it from `put_queue` only after the grant."""
        return sum(1 for event in self._container.put_queue if not event.triggered)

    def put(self, *args: Any, **kwargs: Any) -> Event:
        """
        Request to put a specific amount of material into the container.

        Args:
            *args: The amount of material to add. A custom `type` whose `put` takes
                more arguments gets them all.
            **kwargs: Keyword arguments, passed to the container's `put`.

        Returns:
            simpy.events.Event: A SimPy event that triggers when capacity is available.
        """
        return self._container.put(*args, **kwargs)

    def get(self, *args: Any, **kwargs: Any) -> ContainerGet:
        """
        Request to get a specific amount of material from the container.

        Args:
            *args: The amount of material to withdraw. A custom `type` whose `get`
                takes more arguments gets them all.
            **kwargs: Keyword arguments, passed to the container's `get`.

        Returns:
            simpy.resources.container.ContainerGet: A SimPy event that triggers
                when enough material is available.
        """
        return self._container.get(*args, **kwargs)

    def _level_changed(self):
        """Keeps the registry's level and the statistics in step with the container."""
        self._level_val.track(self._container.level)
        self._changed()

    def _watch_level(self):
        """Applies a live update to the level path, such as a restock."""
        while True:
            yield self._level_val.wait_for_change()
            self._container._level = float(self._level_val.value)
            self._changed()
            # A higher level can serve waiting gets, a lower one can make room for puts
            if self._container.get_queue:
                self._container._trigger_get(None)
            if self._container.put_queue:
                self._container._trigger_put(None)

    def _handle_capacity_change(self, new_target: float):
        """
        Internal callback triggered when the Registry capacity value changes.
        Updates the physical SimPy container and processes pending events.

        Args:
            new_target (float): The new capacity limit dictated by the control plane.
        """
        # Safely bind the new target to absolute physical limits [0, max_cap]
        max_cap = float(self._max_cap_val.value)
        new_target = max(0.0, min(float(new_target), max_cap))

        self._container._capacity = new_target

        # If capacity grew, pending put requests might now have enough room to succeed.
        # We manually trigger SimPy's internal put processor to wake them up.
        if self._container.put_queue:
            self._container._trigger_put(None)
