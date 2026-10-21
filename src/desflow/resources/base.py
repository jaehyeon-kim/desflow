import simpy

from desflow.core.registry import DynamicValue
from desflow.models.params import CapacityConfig

# The kinds that register themselves, and the registry method that does it
_SELF_REGISTERING = {"resources": "register_resource"}


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

        self.env.process(self._watch_capacity())
        self.env.process(self._watch_max_capacity())

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
