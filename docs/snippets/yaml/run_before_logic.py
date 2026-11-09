"""Python for docs/snippets/yaml/run_before.yaml: a setup function."""

import os


def make_output_folder() -> None:
    """Called once by run.before, when the run starts, not when the file is loaded."""
    os.makedirs("events", exist_ok=True)
