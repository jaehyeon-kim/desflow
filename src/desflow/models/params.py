from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from desflow.core.distributions import (
    LIST_PARAMETERS,
    PARAMETERS,
    get_distribution,
)


@dataclass
class DistributionConfig:
    """
    Configuration for a statistical distribution used in the simulation.

    This config is dynamically updatable. For example, you can change the `rate`
    of an 'exponential' distribution via Kafka, and the `Sampler` will instantly
    use the new rate for the next event. The family (`dist`) is fixed at load, and
    only its parameters change live.

    Numbers are stored as floats, so a live update to 0.5 is not cast to an integer.
    The family and its parameters are checked when the config is built, against the
    registry of distributions in `desflow.core.distributions`.

    Attributes:
        dist (str): The family: exponential, normal, lognormal, truncated_normal,
            constant, uniform, triangular, gamma, weibull or empirical.
        rate (Optional[float]): Events per second, for exponential. Greater than 0.
        mean (Optional[float]): The mean, for normal, lognormal and truncated_normal.
            Greater than 0 for lognormal.
        std (Optional[float]): The standard deviation, 0 or more; 0 (or no value)
            gives the mean every time.
        low (Optional[float]): The lowest value: of a truncated normal (0 by
            default, and a draw below it is drawn again), a uniform or a triangular.
        value (Optional[float]): The time a constant distribution always gives.
            0 or more.
        high (Optional[float]): The highest value of a uniform or a triangular.
        mode (Optional[float]): The most likely value of a triangular.
        shape (Optional[float]): The shape of a gamma or a weibull. Greater than 0.
        scale (Optional[float]): The scale of a gamma or a weibull. Greater than 0.
        values (Optional[List[float]]): The values an empirical draws from.
        weights (Optional[List[float]]): One weight per empirical value.
        edges (Optional[List[float]]): The bin edges of an empirical histogram.
        counts (Optional[List[float]]): One count per empirical histogram bin.
        stream (Optional[str]): The name of the random sequence it draws from: its
            registry path without the sim id, such as `arrival.orders`. Set by the
            registry when the config is registered; not a constructor argument.

    Raises:
        ValueError: If the family is unknown, a parameter it does not take is given,
            or a parameter it takes is missing or out of range.
    """

    dist: str
    rate: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    low: Optional[float] = None
    value: Optional[float] = None
    high: Optional[float] = None
    mode: Optional[float] = None
    shape: Optional[float] = None
    scale: Optional[float] = None
    values: Optional[List[float]] = None
    weights: Optional[List[float]] = None
    edges: Optional[List[float]] = None
    counts: Optional[List[float]] = None
    stream: Optional[str] = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self):
        family = get_distribution(self.dist)
        for name in PARAMETERS:
            value = getattr(self, name)
            if value is None:
                continue
            try:
                if name in LIST_PARAMETERS:
                    setattr(self, name, [float(item) for item in value])
                else:
                    setattr(self, name, float(value))
            except (TypeError, ValueError):
                kind = "a list of numbers" if name in LIST_PARAMETERS else "a number"
                raise ValueError(
                    f"{self.dist} {name} must be {kind}, got {value!r}"
                ) from None
        for name, default in family.defaults.items():
            if getattr(self, name) is None:
                setattr(self, name, default)
        check_distribution(self)


def check_distribution(config: DistributionConfig) -> None:
    """
    Checks a config against its family's rules in the registry of distributions.

    The registry calls this before it applies a live update, so the rules at load
    and on a live update are the same.

    Args:
        config: The config to check.

    Raises:
        ValueError: If the family is unknown, a parameter it does not take is set,
            or a parameter is missing or out of range.
    """
    family = get_distribution(config.dist)
    extra = [
        name
        for name in PARAMETERS
        if name not in family.parameters and getattr(config, name) is not None
    ]
    if extra:
        raise ValueError(
            f"{config.dist} does not take {', '.join(extra)}; it takes "
            f"{', '.join(family.parameters)}"
        )
    family.check(config)


@dataclass
class CapacityConfig:
    """
    Configuration for the capacity of a simulated resource, container, or store.

    Attributes:
        current_cap (Union[int, float]): The currently active capacity.
        max_cap (Union[int, float]): The absolute physical maximum capacity.
    """

    current_cap: Union[int, float]
    max_cap: Union[int, float]


@dataclass
class ContainerConfig(CapacityConfig):
    """
    Configuration for a container: its capacity and the level it starts at.

    `current_cap` is the container's capacity, its largest level, as it is a
    resource's capacity. The level has a registry path of its own, `.level`, so a
    scenario or an ingress message can set it during a run, such as a restock.

    Attributes:
        current_cap (Union[int, float]): The capacity: the largest level.
        max_cap (Union[int, float]): The largest capacity a live update can set.
        level (float): The amount held when the run starts. Defaults to 0.
    """

    level: float = 0.0


@dataclass
class SimParameter:
    """
    The master schema representing the state of a specific simulation entity (like a production line).

    This object is registered with the `SimulationRegistry`, which flattens
    the nested dictionaries into dot-notation paths (e.g., 'Shop.arrival.orders.rate').

    Attributes:
        sim_id (str): The unique prefix for this group of parameters (e.g., 'Shop').
        arrival (Dict[str, DistributionConfig]): Configurations for arrival generation rates.
        service (Dict[str, DistributionConfig]): Configurations for process task durations.
        resources (Dict[str, CapacityConfig]): Configurations for standard SimPy Resources.
        containers (Dict[str, CapacityConfig]): Configurations for continuous SimPy Containers.
            A `ContainerConfig` also sets the level the container starts at; a plain
            `CapacityConfig` starts it empty.
        stores (Dict[str, CapacityConfig]): Configurations for discrete SimPy Stores.
        variables (Dict[str, Any]): Flexible user-defined state variables (e.g., int, float, bool, str)
            used to drive custom logic, track string-based states, or act as external control dials.
    """

    sim_id: str
    arrival: Dict[str, DistributionConfig] = field(default_factory=dict)
    service: Dict[str, DistributionConfig] = field(default_factory=dict)
    # Standardized categories
    resources: Dict[str, CapacityConfig] = field(default_factory=dict)
    containers: Dict[str, CapacityConfig] = field(default_factory=dict)
    stores: Dict[str, CapacityConfig] = field(default_factory=dict)
    # Custom / User-Defined states
    variables: Dict[str, Any] = field(default_factory=dict)
