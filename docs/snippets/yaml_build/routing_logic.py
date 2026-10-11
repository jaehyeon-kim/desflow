"""Python for docs/snippets/yaml_build/connectors.yaml: a router and a setup step."""

import os


def daily_path(record: dict):
    """Writes each event under its day; drops telemetry by returning None."""
    if record["stream_type"] != "event":
        return None
    return f"events/day={record['timestamp'][:10]}/events.jsonl"


def make_output_folder() -> None:
    """Called once by run.before, when the run starts, not when the file is loaded."""
    os.makedirs("events", exist_ok=True)
