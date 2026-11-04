"""GA-2: A machine slows down as it wears, then is repaired.

The original is the test asserts of the section of yaml-only.md Part 4: `check` states
them and the test runs it on the records of the YAML form.
"""


def check(records):
    """For each hour, the mean of `finished` minus `started` of a part is within 0.5 s of
    10, 12, 14, 16 and 10, by the hour the part started."""
    started: dict = {}
    spans: dict = {}
    for r in records:
        value = r["value"] if r["stream_type"] == "event" else {}
        if value.get("status") == "started":
            started[r["key"]] = r["sim_ts"]
        elif r["stream_type"] == "event" and value.get("event_type") == "stamped":
            hour = int(started[r["key"]] // 3600)
            spans.setdefault(hour, []).append(r["sim_ts"] - started[r["key"]])
    for hour, mean in enumerate([10, 12, 14, 16, 10]):
        assert spans[hour], hour
        assert abs(sum(spans[hour]) / len(spans[hour]) - mean) < 0.5, hour
