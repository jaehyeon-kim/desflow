"""GA-7: A day of history is written in seconds, then events continue in real time.

The original is the test asserts of the section of yaml-only.md Part 4: `check` states
them and the test runs it on the records of the YAML form and the seconds the run took.
The case is paced: the tail after `go_live_at` runs against the wall clock, so it runs in
the step of the paced cases.
"""

from datetime import datetime, timedelta, timezone

PACED = True


def check(records, seconds):
    """The first event's timestamp is within 1 minute of the logical start (a day before
    now), at least one event is stamped at or after go-live (the logical start and a
    day), and the run takes between 2.5 s and 30 s of wall time, so the history was
    unpaced and the tail was paced."""
    events = [r for r in records if r["stream_type"] == "event"]
    first = datetime.fromisoformat(events[0]["timestamp"])
    start = first - timedelta(seconds=events[0]["sim_ts"])
    assert abs(start - (datetime.now(timezone.utc) - timedelta(days=1))) < timedelta(
        minutes=1
    )
    last = datetime.fromisoformat(events[-1]["timestamp"])
    assert last >= start + timedelta(days=1)
    assert 2.5 <= seconds <= 30, seconds
