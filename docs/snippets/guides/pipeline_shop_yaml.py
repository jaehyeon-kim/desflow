"""The Test a Pipeline page, YAML form: runs test_a_pipeline.yaml, beside this file, with
its sinks under `folder`. Save it as pipeline_shop.py for the tests."""

from pathlib import Path

from desflow import SimulationContext


# --8<-- [start:shop]
def run_shop(folder: Path) -> None:
    """Runs the shop of the blueprint, writing its data and labels under `folder`."""
    blueprint = Path(__file__).with_name("test_a_pipeline.yaml")
    SimulationContext.from_yaml(blueprint, variables={"OUT": str(folder)}).run()


# --8<-- [end:shop]
