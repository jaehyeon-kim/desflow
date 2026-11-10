"""GA-4: Orders belong to existing customers.

The original is the test asserts of the section of yaml-only.md Part 4: `check` states
them and the test runs it on the records of the YAML form.
"""


def check(records):
    """Every order's `customer_id` equals the `customer_id` of a sign-up event with an
    earlier or equal `sim_ts`."""
    events = [r for r in records if r["stream_type"] == "event"]
    signed = [
        (r["sim_ts"], r["value"]["customer_id"])
        for r in events
        if r["task"] == "sign_up"
    ]
    orders = [r for r in events if r["task"] == "place_order"]
    assert signed and orders
    for r in orders:
        assert any(
            t <= r["sim_ts"] and c == r["value"]["customer_id"] for t, c in signed
        )
