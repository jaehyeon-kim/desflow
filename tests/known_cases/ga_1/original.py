"""GA-1: Orders wait for busy packers.

The original is the test asserts of the section of yaml-only.md Part 4: `check` states
them and the test runs it on the records of the YAML form. The three forms are the
files of the gallery example `orders_wait_for_busy_packers`.
"""

EXAMPLE = "orders_wait_for_busy_packers"


def check(records):
    """`in_use` never exceeds 3, an order starts later than it queued, and the queue
    reaches at least 1."""
    in_use = [
        r["value"]
        for r in records
        if r.get("path_id") == "Shop.stats.resources.packers.in_use"
    ]
    assert in_use and max(in_use) <= 3
    queued: dict = {}
    waited = 0
    for r in records:
        value = r["value"] if r["stream_type"] == "event" else {}
        if value.get("status") == "queued":
            queued[r["key"]] = r["sim_ts"]
        elif value.get("status") == "started" and r["sim_ts"] > queued[r["key"]]:
            waited += 1
    assert waited >= 1
    lengths = [
        r["value"]
        for r in records
        if r.get("path_id") == "Shop.stats.resources.packers.queue_length"
    ]
    assert max(lengths) >= 1
