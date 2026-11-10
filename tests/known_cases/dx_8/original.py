"""DX-8: A resource added during the run and a job on every resource each round.

The original is the snippet's own Python: `original.yaml` runs `supervisor` of
docs/snippets/yaml/dynamic_topology_logic.py as a process. The test runs it, and the YAML form,
which has the wrappers from the start with capacity 0 and a scenario step at 12 s, gives
the same finished jobs on the same resources at the same times. The step's record goes to
the labels sink, not to the records of the run, so it is not compared.
"""

PYTHON_FORM = "tests/known_cases/dx_8/original.yaml"
LOGIC = "docs/snippets/yaml"  # the folder of dynamic_topology_logic.py


def project(records):
    """The time and resource of each finished job."""
    return sorted(
        (r["sim_ts"], r["value"]["resource"])
        for r in records
        if r["stream_type"] == "event" and r["value"].get("status") == "finished"
    )
