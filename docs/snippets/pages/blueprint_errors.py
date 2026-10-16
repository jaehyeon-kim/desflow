"""Loading a blueprint with a mistake, as the YAML Blueprints page shows it.

tests/docs runs this file: the load fails with a BlueprintError that names the file
and the line, before anything runs.
"""

import os
from pathlib import Path

from desflow import SimulationContext
from desflow.blueprint.loader import BlueprintError

os.chdir(Path(__file__).resolve().parent)  # the blueprint's folder

# --8<-- [start:load]
try:
    SimulationContext.from_yaml("blueprint_error.yaml")
except BlueprintError as error:
    print(f"Error: {error}")  # what desflow run prints, before exiting with code 1
# --8<-- [end:load]
