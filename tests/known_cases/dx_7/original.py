"""DX-7: Custom readings every 30 seconds.

The original is the snippet's own Python: `original.yaml` runs `packers_report` of
docs/snippets/yaml/shop_logic.py as a telemetry function. The test runs it, and the YAML
form, which has the readings as expressions, publishes the same readings at the same times.
"""

PYTHON_FORM = "tests/known_cases/dx_7/original.yaml"
LOGIC = "docs/snippets/yaml"  # the folder of shop_logic.py
READINGS = ("Shop.waiting_per_packer", "Shop.arrival_rate")


def project(records):
    """The time, name and value of each custom reading."""
    found = [
        (r["sim_ts"], r["path_id"], r["value"])
        for r in records
        if r["stream_type"] == "telemetry" and r.get("path_id") in READINGS
    ]
    assert found
    return found
