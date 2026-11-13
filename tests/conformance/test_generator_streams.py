"""The seeded draws of the generators in a `group`, a `spawn` count and `spawn` args.

Each generator draws from a stream named for its place, so a seed fixes its numbers.
The numbers below were produced by the blueprint at its commit, and a change to the
name of a stream or to what a place binds changes them.
"""

import pytest

from tests.conformance.harness import run_yaml

# (time, key, u, n) of every courier, in the order the records are published
COURIERS = [
    (1.0, "courier-1", 0.0009891876378977926, -0.3928484415288612),
    (1.0, "courier-2", 0.7991988384887363, -0.4850482360448896),
    (1.0, "courier-3", 0.7707324186431405, 0.6582942994382386),
    (1.0, "courier-4", 0.1303755650713605, 0.3346342593241817),
    (1.0, "courier-5", 0.5354507128525459, -0.3208464208154382),
    (1.0, "courier-6", 0.7887062563971784, -0.07120582104419047),
    (2.0, "courier-7", 0.37278383017430783, -1.8581219169813468),
    (2.0, "courier-8", 0.9770788351967868, -1.0065033363528475),
]


def test_group_spawn_count_and_spawn_args_draws_are_fixed_by_the_seed():
    """Three entities arrive at 1 and two at 2 (the `group`). Their `pack` tasks spawn 6
    couriers at 1 and 2 at 2 (the `count`), each with a `u` and an `n`."""
    records, _ = run_yaml("generator_streams.yaml")
    events = [r for r in records if r["stream_type"] == "event"]

    packs = [r["sim_ts"] for r in events if r["task"] == "pack"]
    assert packs == [1.0, 1.0, 1.0, 2.0, 2.0]
    couriers = [
        (r["sim_ts"], r["key"], r["value"]["u"], r["value"]["n"])
        for r in events
        if r["task"] == "courier"
    ]
    assert [c[:2] for c in couriers] == [c[:2] for c in COURIERS]
    drawn = [x for c in couriers for x in c[2:]]
    assert drawn == pytest.approx([x for c in COURIERS for x in c[2:]])
