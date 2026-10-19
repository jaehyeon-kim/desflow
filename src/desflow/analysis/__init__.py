"""Replications, experiments and input fitting: answers from many runs, not one.

Fitting needs the `fit` extra; `fit_column` imports scipy only when it runs.
"""

from desflow.analysis.fit import FitResult, fit_column
from desflow.analysis.intervals import Interval, interval, t_quantile
from desflow.analysis.models import LowLevelModel
from desflow.analysis.results import CheckFailed, ExperimentResult
from desflow.analysis.runner import Replications, replicate, run_experiment
from desflow.analysis.spec import ExperimentSpec, load_experiment

__all__ = [
    "CheckFailed",
    "FitResult",
    "Interval",
    "LowLevelModel",
    "Replications",
    "ExperimentResult",
    "ExperimentSpec",
    "fit_column",
    "interval",
    "load_experiment",
    "replicate",
    "run_experiment",
    "t_quantile",
]
