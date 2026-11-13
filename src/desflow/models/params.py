import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

import numpy as np

from desflow.models.distributions import (
    LIST_PARAMETERS,
    PARAMETERS,
    Distribution,
    get_distribution,
)


def _number(value: Any) -> Union[int, float]:
    """A number as given, so an integer stays an integer; anything else as a float."""
    if isinstance(value, bool):
        raise ValueError(f"{value!r} is not a number")
    if isinstance(value, (int, float)):
        return value
    return float(value)


def finite_number(name: str, value: Any) -> None:
    """Refuses a parameter that is a bool, NaN or infinite, naming it.

    The one check of a distribution parameter, run at load and on a live update.

    Args:
        name: The parameter's name.
        value: The value given.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number, got {value!r}")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        raise ValueError(
            f"{name} must be a finite number, and this whole number is too large "
            f"for a float"
        ) from None
    if not finite:
        raise ValueError(f"{name} must be a finite number, got {value!r}")


@dataclass
class DistributionConfig:
    """
    Configuration for a statistical distribution used in the simulation.

    This config is dynamically updatable. For example, you can change the `rate`
    of an 'exponential' distribution via Kafka, and the `Sampler` will instantly
    use the new rate for the next event. The family (`dist`) is fixed at load, and
    only its parameters change live.

    Numbers are stored as given: an integer stays an integer, as in SimPy, and a live
    update to 0.5 is not cast to an integer.
    The family and its parameters are checked when the config is built, against the
    registry of distributions in `desflow.models.distributions`.

    Attributes:
        dist (str): The family: exponential, normal, lognormal, truncated_normal,
            poisson, geometric, constant, uniform, triangular, gamma, weibull or
            empirical.
        rate (Optional[float]): Events per second, for exponential. Greater than 0.
        mean (Optional[float]): The mean, for normal, lognormal, truncated_normal and
            poisson. Greater than 0 for lognormal; 0 or more for poisson.
        p (Optional[float]): The chance of success on each try, for geometric. Above 0
            and at most 1.
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
    p: Optional[float] = None
    stream: Optional[str] = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self):
        family = get_distribution(self.dist)
        for name in PARAMETERS:
            value = getattr(self, name)
            if value is None:
                continue
            try:
                if name in LIST_PARAMETERS:
                    if isinstance(value, (str, bytes)) or not isinstance(
                        value, (Sequence, np.ndarray)
                    ):
                        # Read item by item, a text or a mapping would give numbers
                        # its writer never meant
                        raise TypeError(f"{value!r} is not a list")
                    setattr(self, name, [_number(item) for item in value])
                else:
                    setattr(self, name, _number(value))
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
    check_parameter_values(config)
    family.check(config)


def check_parameter_values(config: DistributionConfig) -> None:
    """
    Checks each set parameter of a config on its own: a number, not a bool, NaN or
    infinite.

    These checks do not read the other parameters, so a live update that is written
    before the others change can still be held to them.

    Args:
        config: The config to check.

    Raises:
        ValueError: If a parameter the family takes is not a finite number.
    """
    family = get_distribution(config.dist)
    for name in family.parameters:
        value = getattr(config, name)
        if value is None:
            continue
        for item in value if name in LIST_PARAMETERS else [value]:
            finite_number(f"{config.dist} {name}", item)


@dataclass
class CapacityConfig:
    """
    Configuration for the capacity of a simulated resource, container, or store.

    A value left as None is filled in when the holder registers. A resource's
    `current_cap` is 1, and `max_cap` is `current_cap`. A store's or container's
    `current_cap` is `max_cap` when that is given and no limit if not, and its
    `max_cap` is `current_cap`. `float("inf")` also means no limit.

    Attributes:
        current_cap (Optional[Union[int, float]]): The currently active capacity.
        max_cap (Optional[Union[int, float]]): The absolute physical maximum capacity.
    """

    current_cap: Optional[Union[int, float]] = None
    max_cap: Optional[Union[int, float]] = None


@dataclass
class ResourceConfig(CapacityConfig):
    """
    Configuration for a resource: its capacity and its waiting-line limit.

    Attributes:
        current_cap (Optional[Union[int, float]]): The currently active capacity.
        max_cap (Optional[Union[int, float]]): The absolute physical maximum capacity.
        queue_limit (Optional[int]): The most requests that may wait, or None for no
            limit. It is the registry path `<sim_id>.resources.<name>.queue_limit`,
            and a change to the path applies from the next request.
        limit_path (bool): Whether the resource has that path. A resource built from
            a class with its own `PutQueue` sets its own limit and has none.
    """

    queue_limit: Optional[int] = None
    limit_path: bool = True


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
        level (Union[int, float]): The amount held when the run starts, kept as the
            type given, as `simpy.Container` keeps it. Defaults to 0.
    """

    level: Union[int, float] = 0


def _always_zero(config: DistributionConfig) -> bool:
    """Whether every draw of `config` is 0: no spread, and a mean of 0 or below, which
    the sampler raises to 0. A variance too large or too small to compute (an
    exponential `rate` of 1e-170 squares to 0, a uniform range of 1e200 squares past
    the largest float) is a spread, not a draw of 0."""
    family = get_distribution(config.dist)
    try:
        spread = family.variance(config)
    except ArithmeticError:
        return False
    return spread == 0 and family.mean(config) <= 0


def check_breakdown_advances(
    time_to_failure: DistributionConfig, repair: DistributionConfig, what: str
) -> None:
    """Refuses a breakdown whose failure and repair both always take 0 seconds.

    Such a resource fails and is repaired for ever at one instant, so the clock never
    moves and the run never reaches its end time.

    Args:
        time_to_failure: The time from a repair, or the start, to the next failure.
        repair: How long a repair takes.
        what: The place, such as `"resource 'm' breakdown"`.

    Raises:
        ValueError: With `what` in the message, if both always give 0.
    """
    if _always_zero(time_to_failure) and _always_zero(repair):
        raise ValueError(
            f"{what}: time_to_failure and repair both always give 0, so the "
            f"resource would fail and be repaired for ever at one instant; give "
            f"one of them a time above 0"
        )


@dataclass
class BreakdownConfig:
    """
    How a resource breaks down: the time to each failure, the repair time, and what
    happens to the work in progress.

    The next failure is drawn when a repair ends, or at the start. With the `calendar`
    clock it counts that time busy or idle, and with the `busy` clock only while the
    resource holds a unit. While down, the resource's capacity is 0; its `current_cap`
    keeps the planned value, which a change during the repair sets for its return.

    Attributes:
        time_to_failure (DistributionConfig): The time from a repair, or the start,
            to the next failure.
        repair (DistributionConfig): How long a repair takes.
        work (str): `interrupt` (the default) interrupts the tasks being served,
            which follow their `on_interrupt`; `finish` lets them finish, and no new
            task starts until the repair ends.
        clock (str): `calendar` (the default) or `busy`, the time the time to failure
            counts.
    """

    time_to_failure: DistributionConfig
    repair: DistributionConfig
    work: str = "interrupt"
    clock: str = "calendar"

    def __post_init__(self) -> None:
        if self.work not in ("interrupt", "finish"):
            raise ValueError(f"work must be interrupt or finish, got '{self.work}'")
        if self.clock not in ("calendar", "busy"):
            raise ValueError(f"clock must be calendar or busy, got '{self.clock}'")


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
        breakdowns (Dict[str, BreakdownConfig]): How each named resource breaks
            down, registered at `<sim_id>.breakdown.<resource>.time_to_failure` and
            `.repair`.
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
    breakdowns: Dict[str, BreakdownConfig] = field(default_factory=dict)
    # Custom / User-Defined states
    variables: Dict[str, Any] = field(default_factory=dict)


def parameters_from_moments(
    family: Distribution, mean: float, std: float
) -> Dict[str, float]:
    """Returns the parameters of `family` that give this mean and standard deviation.

    The mean must be above 0, except for `constant`, whose value 0 gives a time
    of 0 on every draw. The result is checked against the family's rules, so a pair the family cannot have is
    refused.

    Args:
        family: The distribution family.
        mean: The mean, in seconds.
        std: The standard deviation, in seconds.

    Returns:
        Dict[str, float]: The family's parameters.

    Raises:
        ValueError: If no simple formula gives the family's parameters (the
            message is the family's `no_formula`), the std is negative, the mean
            is not above 0 for a family other than `constant`, or the family
            cannot have this pair.
    """
    if family.from_moments is None:
        raise ValueError(f"{family.name}: {family.no_formula}")
    if not std >= 0:
        raise ValueError(f"std must be 0 or more, got {std:g}")
    if family.name != "constant" and not mean > 0:
        raise ValueError(
            f"mean must be above 0 for {family.name}, got {mean:g}; for a time of 0 "
            "on every draw, use constant with value 0"
        )
    parameters = family.from_moments(mean, std)
    DistributionConfig(dist=family.name, **parameters)  # type: ignore[arg-type]
    return parameters
