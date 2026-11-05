"""GA-3: Capacity drops to one at 20 seconds, and the queue grows.

The original is the test asserts of the section of yaml-only.md Part 4: `check` states
them and the test runs it on the records of the YAML form. The three forms are the
files of the gallery example `capacity_drops_and_queue_grows`.
"""

EXAMPLE = "capacity_drops_and_queue_grows"


def check(records):
    """`capacity` is 3 before 20 s and 1 after, the longest queue after 20 s is longer
    than the longest before, and the queue at 59 s holds at least 10 orders."""

    def series(name):
        return [
            (r["sim_ts"], r["value"])
            for r in records
            if r.get("path_id") == f"Shop.stats.resources.packers.{name}"
        ]

    capacity = series("capacity")
    assert all(v == 3 for t, v in capacity if t < 20)
    assert all(v == 1 for t, v in capacity if t > 20)
    queue = series("queue_length")
    before = max(v for t, v in queue if t < 20)
    after = max(v for t, v in queue if t >= 20)
    assert after > before
    assert [v for t, v in queue if t == 59][0] >= 10
