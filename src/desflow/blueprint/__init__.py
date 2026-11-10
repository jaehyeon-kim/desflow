"""Builds a `SimulationContext` from a YAML blueprint file.

Use `SimulationContext.from_yaml` from Python, or `desflow run <file>.yaml` from a
shell.
"""

from desflow.blueprint.build import Preview, build, preview_builder
from desflow.blueprint.loader import BlueprintError
from desflow.blueprint.model_check import ModelCheck, check_model
from desflow.blueprint.models import Drain, Run, RunSegment, SegmentEvery

__all__ = [
    "BlueprintError",
    "Drain",
    "ModelCheck",
    "Preview",
    "Run",
    "RunSegment",
    "SegmentEvery",
    "build",
    "check_model",
    "preview_builder",
]
