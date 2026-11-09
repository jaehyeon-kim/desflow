"""Replications, experiments and input fitting: answers from many runs, not one.

Fitting needs the `fit` extra; `fit_column` imports scipy only when it runs.
"""

from desflow.analysis.fit import Candidate, FitResult, fit_column, read_column
from desflow.analysis.intervals import Interval, interval, t_quantile
from desflow.analysis.models import LowLevelModel, RunResult
from desflow.analysis.results import CheckFailed, Design, ExperimentResult
from desflow.analysis.runner import Replications, replicate, run_experiment
from desflow.analysis.spec import (
    Check,
    Compare,
    ExperimentSpec,
    Observed,
    load_experiment,
)
from desflow.blueprint.loader import SourceMap

__all__ = [
    "Candidate",
    "Check",
    "CheckFailed",
    "Compare",
    "Design",
    "FitResult",
    "Interval",
    "LowLevelModel",
    "Observed",
    "Replications",
    "RunResult",
    "SourceMap",
    "ExperimentResult",
    "ExperimentSpec",
    "fit_column",
    "interval",
    "load_experiment",
    "read_column",
    "replicate",
    "run_experiment",
    "t_quantile",
]
