import copy
import logging
from typing import Any, Callable, Dict, Optional

from simpy import Environment, Store

from desflow.models.params import (
    CapacityConfig,
    DistributionConfig,
    SimParameter,
    check_distribution,
)

logger = logging.getLogger(__name__)


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
        self._signal = Store(env, capacity=1)

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

    def wait_for_change(self):
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
    """

    def __init__(self, env: Environment):
        self.env = env
        self._values: Dict[str, DynamicValue] = {}
        self._configs: Dict[str, Any] = {}

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

    def update(self, path: str, new_value: Any):
        """
        Update a value safely and synchronize its parent configuration object.

        Includes dynamic type casting to protect the simulation from crashing if
        external systems send improperly typed data (e.g., sending the string "5"
        instead of the integer 5).

        A distribution parameter outside its range (for example a negative std) is
        refused with an error log line, as a value of the wrong type is, and the old
        value is kept. A `current_cap` outside `[0, max_cap]` is clamped to that range
        with a warning log line.

        Args:
            path (str): The dot-notation path to update.
            new_value (Any): The new value to apply.
        """
        if path in self._values:
            current_val = self._values[path].value
            validated_value = new_value

            # Type Validation & Casting
            if current_val is not None:
                expected_type = type(current_val)
                # If types don't match, attempt a safe cast (e.g., "5" -> 5)
                if not isinstance(new_value, expected_type):
                    try:
                        validated_value = expected_type(new_value)
                        logger.debug(
                            f"Cast '{new_value}' to {expected_type.__name__} for '{path}'"
                        )
                    except (ValueError, TypeError):
                        logger.error(
                            f"Type mismatch for '{path}'. Expected {expected_type.__name__}, "
                            f"got {type(new_value).__name__} with value '{new_value}'. Update ignored."
                        )
                        return  # Exit early to prevent crashing the simulation

            parent_path, attr = path.rsplit(".", 1) if "." in path else (path, "")
            parent_obj = self._configs.get(parent_path)

            # Check a distribution parameter with the same rules as at load
            if isinstance(parent_obj, DistributionConfig):
                candidate = copy.copy(parent_obj)
                setattr(candidate, attr, validated_value)
                try:
                    check_distribution(candidate)
                except ValueError as exc:
                    logger.error(f"Invalid value for '{path}': {exc}. Update ignored.")
                    return

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

            # Update the DynamicValue (triggers SimPy events)
            self._values[path].update(validated_value)

            # Synchronize the attribute on the original config object
            # Check if the parent is a dictionary (like `variables`)
            if isinstance(parent_obj, dict):
                parent_obj[attr] = validated_value
            elif parent_obj is not None:
                setattr(parent_obj, attr, validated_value)
        else:
            logger.warning(f"Attempted to update non-existent path: {path}")

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
            self._register_dist(path, dist_config)

        # Register Service Steps
        for name, dist_config in param.service.items():
            path = f"{prefix}.service.{name}"
            self._configs[path] = dist_config
            self._register_dist(path, dist_config)

        # Register Resources
        for name, cap_config in param.resources.items():
            path = f"{prefix}.resources.{name}"
            self._configs[path] = cap_config
            self._register_cap(path, cap_config)

        # Register Containers
        for name, cap_config in param.containers.items():
            path = f"{prefix}.containers.{name}"
            self._configs[path] = cap_config
            # A container level is continuous, so an integer starting value must not
            # make later updates integers through the type cast in update()
            self._register_cap(path, cap_config, cast=float)

        # Register Stores
        for name, cap_config in param.stores.items():
            path = f"{prefix}.stores.{name}"
            self._configs[path] = cap_config
            self._register_cap(path, cap_config)

        # Register Custom Variables
        self._configs[f"{prefix}.variables"] = param.variables
        for name, value in param.variables.items():
            path = f"{prefix}.variables.{name}"
            self._values[path] = DynamicValue(self.env, path, value)

    def _register_dist(self, path_prefix: str, dist_config: Any):
        """Internal: Flattens a DistributionConfig."""
        if dist_config.dist == "exponential":
            self._values[f"{path_prefix}.rate"] = DynamicValue(
                self.env, f"{path_prefix}.rate", dist_config.rate
            )
        elif dist_config.dist == "constant":
            self._values[f"{path_prefix}.value"] = DynamicValue(
                self.env, f"{path_prefix}.value", dist_config.value
            )
        else:
            self._values[f"{path_prefix}.mean"] = DynamicValue(
                self.env, f"{path_prefix}.mean", dist_config.mean
            )
            self._values[f"{path_prefix}.std"] = DynamicValue(
                self.env, f"{path_prefix}.std", dist_config.std
            )
            if dist_config.dist == "truncated_normal":
                self._values[f"{path_prefix}.low"] = DynamicValue(
                    self.env, f"{path_prefix}.low", dist_config.low
                )

    def _register_cap(
        self, path_prefix: str, cap_config: Any, cast: Optional[Callable] = None
    ):
        """Internal: Flattens a CapacityConfig, converting both values with `cast` if given."""
        current_cap, max_cap = cap_config.current_cap, cap_config.max_cap
        if cast is not None:
            current_cap, max_cap = cast(current_cap), cast(max_cap)
        self._values[f"{path_prefix}.current_cap"] = DynamicValue(
            self.env, f"{path_prefix}.current_cap", current_cap
        )
        self._values[f"{path_prefix}.max_cap"] = DynamicValue(
            self.env, f"{path_prefix}.max_cap", max_cap
        )
