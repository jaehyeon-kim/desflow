from typing import Any, Callable, Optional

from simpy import Container, Environment, Event
from simpy.resources.container import ContainerGet, ContainerPut

from desflow.resources.base import BaseDynamicResource


class _Container(Container):
    """A SimPy Container that reports every change of its level."""

    def __init__(self, env: Environment, on_change: Callable[[], None], **kwargs: Any):
        self._on_change = on_change
        super().__init__(env, **kwargs)

    def _do_put(self, event: ContainerPut) -> Optional[bool]:
        done = super()._do_put(event)
        if done:
            self._on_change()
        return done

    def _do_get(self, event: ContainerGet) -> Optional[bool]:
        done = super()._do_get(event)
        if done:
            self._on_change()
        return done


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
        """
        super().__init__(env, sim_id, container_id, "containers")

        self._level_val = env.registry.get(f"{sim_id}.containers.{container_id}.level")
        max_cap = float(self._max_cap_val.value)
        # Prevent initialization out-of-bounds
        initial_capacity = max(0.0, min(float(self._current_cap_val.value), max_cap))
        start = self._level_val.value if init is None else init
        # A number keeps its type, so a whole level stays an int, as SimPy keeps it
        if isinstance(start, bool) or not isinstance(start, (int, float)):
            start = float(start)

        if start > initial_capacity:
            # SimPy's own check, which the capacity of 1 below would not make
            raise ValueError('"init" must be <= "capacity".')
        # SimPy refuses a capacity of 0, which desflow accepts because a capacity
        # changes during a run; the container is built with 1 and held at 0, as a
        # capacity change sets it
        self._container = _Container(
            env, self._level_changed, init=start, capacity=initial_capacity or 1.0
        )
        self._container._capacity = initial_capacity
        self._level_val.track(self._container.level)
        self.env.process(self._watch_level())
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
        """int: Requests waiting to take an amount."""
        return len(self._container.get_queue)

    @property
    def putters_waiting(self) -> int:
        """int: Requests waiting to add an amount."""
        return len(self._container.put_queue)

    def put(self, amount: float) -> Event:
        """
        Request to put a specific amount of material into the container.

        Args:
            amount (float): The amount of material to add.

        Returns:
            simpy.events.Event: A SimPy event that triggers when capacity is available.
        """
        return self._container.put(amount)

    def get(self, amount: float) -> ContainerGet:
        """
        Request to get a specific amount of material from the container.

        Args:
            amount (float): The amount of material to withdraw.

        Returns:
            simpy.resources.container.ContainerGet: A SimPy event that triggers
                when enough material is available.
        """
        return self._container.get(amount)

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
