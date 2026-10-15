from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional, Union


@dataclass
class DistributionConfig:
    """
    Configuration for a statistical distribution used in the simulation.

    This config is dynamically updatable. For example, you can change the `rate`
    of an 'exponential' distribution via Kafka, and the `Sampler` will instantly
    use the new rate for the next event.

    Numbers are stored as floats, so a live update to 0.5 is not cast to an integer.
    The parameters the distribution uses are checked when the config is built, and
    an invalid one raises `ValueError`.

    Attributes:
        dist (str): The type of distribution: 'exponential', 'normal', 'lognormal',
            'truncated_normal' or 'constant'.
        rate (Optional[float]): The rate parameter (lambda) for exponential distributions.
            Must be greater than 0.
        mean (Optional[float]): The mean for normal and lognormal distributions.
            Must be greater than 0 for lognormal.
        std (Optional[float]): The standard deviation for normal and lognormal distributions.
            Must be 0 or more; 0 (or no value) means a constant equal to the mean.
        low (Optional[float]): The lowest value of a truncated normal, 0 by default.
            A draw below it is drawn again, so no draws pile up at the limit.
        value (Optional[float]): The time a constant distribution always gives.
            Must be 0 or more.

    Raises:
        ValueError: If a parameter the distribution uses is missing or out of range.
    """

    dist: Literal["exponential", "normal", "lognormal", "truncated_normal", "constant"]
    rate: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    low: Optional[float] = None
    value: Optional[float] = None

    def __post_init__(self):
        for name in ("rate", "mean", "std", "low", "value"):
            value = getattr(self, name)
            if value is not None:
                try:
                    setattr(self, name, float(value))
                except (TypeError, ValueError):
                    raise ValueError(
                        f"{self.dist} {name} must be a number, got {value!r}"
                    ) from None
        if (
            self.dist in ("normal", "lognormal", "truncated_normal")
            and self.std is None
        ):
            self.std = 0.0
        if self.dist == "truncated_normal" and self.low is None:
            self.low = 0.0
        check_distribution(self)


_DISTRIBUTIONS = ("exponential", "normal", "lognormal", "truncated_normal", "constant")


def check_distribution(config: DistributionConfig) -> None:
    """
    Checks the parameters that `config.dist` uses.

    The registry calls this before it applies a live update, so the rules at load
    and on a live update are the same.

    Raises:
        ValueError: If a parameter is missing or out of range.
    """
    if config.dist not in _DISTRIBUTIONS:
        raise ValueError(
            f"Unsupported distribution '{config.dist}'. "
            f"Use one of: {', '.join(_DISTRIBUTIONS)}"
        )
    if config.dist == "constant":
        if config.value is None or not config.value >= 0:
            raise ValueError(f"constant value must be 0 or more, got {config.value}")
    elif config.dist == "exponential":
        if config.rate is None or not config.rate > 0:
            raise ValueError(
                f"exponential rate must be greater than 0, got {config.rate}"
            )
    elif config.dist in ("normal", "lognormal", "truncated_normal"):
        if config.mean is None:
            raise ValueError(f"{config.dist} needs a mean")
        if config.dist == "lognormal" and not config.mean > 0:
            raise ValueError(
                f"lognormal mean must be greater than 0, got {config.mean}"
            )
        if config.std is None or not config.std >= 0:
            raise ValueError(f"{config.dist} std must be 0 or more, got {config.std}")
        if config.dist == "truncated_normal":
            if config.low is None:
                raise ValueError("truncated_normal needs a low")
            if config.std == 0 and config.mean < config.low:
                raise ValueError(
                    f"truncated_normal with std 0 needs a mean of at least low "
                    f"{config.low}, got {config.mean}"
                )


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
