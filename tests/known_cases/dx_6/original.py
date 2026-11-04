"""DX-6: A process that takes every packer off for a break.

The original is the snippet's own Python: `original.yaml` runs `breaks` of
docs/snippets/yaml/shop_logic.py as a process. The test runs it, and the YAML form, which
does the same with two tasks, gives the same down and up events at the same times.
"""

PYTHON_FORM = "tests/known_cases/dx_6/original.yaml"
LOGIC = "docs/snippets/yaml"  # the folder of shop_logic.py


def project(records):
    """The time, key and value of the events a break publishes: the engine's `queued`
    and `started` records of the two tasks are not in the process's."""
    return [
        (r["sim_ts"], r["key"], r["value"])
        for r in records
        if r["stream_type"] == "event" and r["value"].get("status") in ("down", "up")
    ]
