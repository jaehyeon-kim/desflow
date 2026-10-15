"""Builds a `SimulationContext` from a YAML blueprint file.

Use `SimulationContext.from_yaml` from Python, or `desflow run <file>.yaml` from a
shell.
"""

from desflow.blueprint.build import build
from desflow.blueprint.loader import BlueprintError

__all__ = ["BlueprintError", "build"]
