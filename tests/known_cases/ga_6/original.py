"""GA-6: Five sensors each drift from their previous value.

The original is the test asserts of the section of yaml-only.md Part 4: `check` states
them and the test runs it on the records of the YAML form.
"""


def check(records):
    """Exactly five sensor ids appear, and for each sensor consecutive readings differ by
    at most 1."""
    readings: dict = {}
    for r in records:
        if r["stream_type"] == "event":
            readings.setdefault(r["value"]["sensor_id"], []).append(r["value"]["value"])
    assert len(readings) == 5
    for values in readings.values():
        assert len(values) > 1
        assert abs(values[0]) <= 1
        assert all(abs(b - a) <= 1 for a, b in zip(values, values[1:]))
