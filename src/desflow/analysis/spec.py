"""The experiment file: what an experiment runs, compares and checks.

An experiment file is YAML read with the blueprint loader, so `!python` references and
`${VAR}` work as in a blueprint, and every model here forbids unknown keys, so a
misspelt key is reported with its line. `load_experiment` reads a file;
`ExperimentSpec` also takes the same settings as a Python mapping.
"""

from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from desflow.blueprint.loader import BlueprintError, SourceMap, read_document
from desflow.blueprint.models import unknown_key
from desflow.analysis.models import LowLevelModel
from desflow.utils import suggestion, time_to_seconds


def _seconds(value: Any) -> Any:
    """Reads seconds, or a duration such as `"8 h"`."""
    return time_to_seconds(value) if isinstance(value, str) else value


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


class LowLevel(_Model):
    """A `model` written with the low-level API: its function and sim id."""

    function: Callable[..., Any]
    sim_id: str


class Compare(_Model):
    """A `compare` entry: two variants compared run by run with the same seeds."""

    baseline: str
    variant: str


class Check(_Model):
    """A `checks` entry: a bound on one measure of one variant, or of every variant.

    `on: interval`, the default, checks the far end of the confidence interval (the
    upper end against `max`, the lower against `min`), so noise cannot pass a check;
    `on: mean` checks the mean only.
    """

    measure: str
    variant: Optional[str] = None
    max: Optional[float] = None
    min: Optional[float] = None
    on: Literal["mean", "interval"] = "interval"

    @model_validator(mode="after")
    def _a_bound(self) -> "Check":
        if self.max is None and self.min is None:
            raise ValueError("a check needs max, min or both")
        return self


class Observed(_Model):
    """The `observed` section: a CSV of values measured on the real system.

    Its columns are `measure`, `value`, `unit`, `population` and `horizon`. Each row
    is compared with the simulated measure of `variant`, the first variant when it
    is not given.
    """

    file: str
    variant: Optional[str] = None


class ExperimentSpec(_Model):
    """A whole experiment file, or the same settings given in Python.

    `model` is a blueprint path, a builder factory (a function with no arguments
    returning a new `SimulationContext`), a `LowLevelModel`, or in a file a mapping
    of a low-level `function` and its `sim_id`. Relative paths are read from the
    experiment file's folder. `until` is where each replication's arrivals stop;
    `stop` and `drain` end each replication as `SimulationContext.run` does, and
    when they are not set a blueprint's own `run.stop` and `run.drain` apply.
    """

    experiment: str
    question: Optional[str] = None
    model: Any
    assumptions: List[str] = Field(default_factory=list)
    inputs: Dict[str, str] = Field(default_factory=dict)
    until: float = Field(gt=0)
    # Ends each replication early, `{signal: name}` or `{condition: "..."}`, or
    # for a low-level model a function of the environment; None keeps a
    # blueprint's run.stop
    stop: Any = None
    # At until, stop the arrivals and run on until the model is empty: true, or
    # {limit: duration}; None keeps a blueprint's run.drain
    drain: Any = None
    warmup: Optional[float] = Field(default=None, ge=0)
    replications: int = Field(ge=2)
    seed: int
    level: float = Field(default=0.95, gt=0, lt=1)
    pairing: Literal["common", "independent"] = "common"
    measures: List[str] = Field(min_length=1)
    variants: Dict[str, Dict[str, Any]] = Field(default_factory=lambda: {"base": {}})
    sweep: Dict[str, List[Any]] = Field(default_factory=dict)
    compare: List[Compare] = Field(default_factory=list)
    checks: List[Check] = Field(default_factory=list)
    observed: Optional[Observed] = None
    limits: List[str] = Field(default_factory=list)
    output: Optional[str] = None
    workers: int = Field(default=1, ge=1)

    @field_validator("until", "warmup", mode="before")
    @classmethod
    def _duration(cls, value: Any) -> Any:
        return _seconds(value)

    @field_validator("model", mode="before")
    @classmethod
    def _model_form(cls, value: Any) -> Any:
        if isinstance(value, dict):
            low = LowLevel.model_validate(value)
            return LowLevelModel(low.function, low.sim_id)
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, (str, LowLevelModel)) or callable(value):
            return value
        raise ValueError(
            "model is a blueprint path, a !python builder factory, or a mapping of "
            "a low-level function and its sim_id"
        )

    @field_validator("sweep")
    @classmethod
    def _finite_lists(cls, value: Dict[str, List[Any]]) -> Dict[str, List[Any]]:
        for path, values in value.items():
            if not values:
                raise ValueError(f"sweep {path} needs at least one value")
        return value

    @model_validator(mode="after")
    def _names_exist(self) -> "ExperimentSpec":
        if not self.variants:
            raise ValueError("variants needs at least one variant")
        names = set(self.variants)
        for path in self.sweep:
            for name, settings in self.variants.items():
                if path in settings:
                    raise ValueError(
                        f"variant {name} sets {path}, which the sweep also sets; "
                        f"set it in one place"
                    )
        if self.compare and self.pairing == "independent":
            raise ValueError(
                "compare needs pairing: common, because a comparison is made seed "
                "by seed"
            )
        for entry in self.compare:
            for name in (entry.baseline, entry.variant):
                if name not in names:
                    raise ValueError(
                        f"compare names {name}, which is not a variant"
                        + _variant(name, names)
                    )
        for check in self.checks:
            if check.variant is not None and check.variant not in names:
                raise ValueError(
                    f"a check names {check.variant}, which is not a variant"
                    + _variant(check.variant, names)
                )
            if check.measure not in self.measures:
                raise ValueError(
                    f"a check reads {check.measure}; add it to measures first"
                )
        if self.observed and self.observed.variant not in (None, *names):
            raise ValueError(
                f"observed names {self.observed.variant}, which is not a variant"
                + _variant(self.observed.variant, names)
            )
        return self


def _variant(name: Any, names: Any) -> str:
    """The end of a message for a name no variant has: the closest, or the variants."""
    return suggestion(name, names) or f"; the variants are {', '.join(sorted(names))}"


def load_experiment(path: Union[str, Path]) -> "tuple[ExperimentSpec, SourceMap]":
    """
    Reads and validates an experiment file.

    Args:
        path: The experiment file.

    Returns:
        The settings, and the map from each key to its line.

    Raises:
        BlueprintError: If the file is invalid; the message names the file and line.
    """
    data, source = read_document(path)
    try:
        spec = ExperimentSpec.model_validate(data)
    except ValidationError as exc:
        raise _error(exc, source) from None
    return spec, source


def _error(exc: ValidationError, source: SourceMap) -> BlueprintError:
    """Every validation error, each with the line of the key it names."""
    messages = []
    for error in exc.errors():
        location = tuple(error["loc"])
        where = ".".join(str(part) for part in location) or "top level"
        text = error["msg"]
        if error["type"] == "extra_forbidden":
            text = unknown_key(ExperimentSpec, location)
        messages.append(str(source.error(location, f"{where}: {text}")))
    return BlueprintError("\n".join(messages))
