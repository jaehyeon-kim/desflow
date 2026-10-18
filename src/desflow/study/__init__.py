"""Replications, local studies and input fitting: answers from many runs, not one."""

from desflow.study.intervals import Interval, interval, t_quantile
from desflow.study.models import LowLevelModel
from desflow.study.results import CheckFailed, StudyResult
from desflow.study.runner import Replications, replicate, run_study
from desflow.study.spec import StudySpec, load_study

__all__ = [
    "CheckFailed",
    "Interval",
    "LowLevelModel",
    "Replications",
    "StudyResult",
    "StudySpec",
    "interval",
    "load_study",
    "replicate",
    "run_study",
    "t_quantile",
]
