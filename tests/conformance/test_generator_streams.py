"""The seeded draws of the generators in a `group`, a `spawn` count and `spawn` args.

Each generator draws from a stream named for its place, so a seed fixes its numbers.
The numbers below were produced by the blueprint at its commit, and a change to the
name of a stream or to what a place binds changes them.
"""

import pytest

from tests.conformance.harness import run_yaml

# (time, key, u, n) of every courier, in the order the records are published
COURIERS = [
    (1.0, "courier-1", 0.8146182150880799, -0.7691257892060109),
    (1.0, "courier-2", 0.5668287477754712, -0.07613804615064032),
    (1.0, "courier-3", 0.11445755409801073, -0.36479276651297565),
    (1.0, "courier-4", 0.30728673097618175, -1.336376170293762),
    (1.0, "courier-5", 0.9878825202083326, 0.0920282698481228),
    (1.0, "courier-6", 0.37896584927779176, -0.8417411032218027),
    (2.0, "courier-7", 0.46918160441808865, -0.2115849001554326),
    (2.0, "courier-8", 0.1401486272319371, -0.7743956531184715),
    (2.0, "courier-9", 0.07918013216562547, -0.9251803135105392),
    (2.0, "courier-10", 0.4292375498489196, -0.26662493290079803),
]


def test_group_spawn_count_and_spawn_args_draws_are_fixed_by_the_seed():
    """Two entities arrive at 1 and at 2 (the `group`). Their `pack` tasks spawn 3 and 3
    couriers at 1, and 1 and 3 at 2 (the `count`), each with a `u` and an `n`."""
    records, _ = run_yaml("generator_streams.yaml")
    events = [r for r in records if r["stream_type"] == "event"]

    packs = [r["sim_ts"] for r in events if r["task"] == "pack"]
    assert packs == [1.0, 1.0, 2.0, 2.0]
    couriers = [
        (r["sim_ts"], r["key"], r["value"]["u"], r["value"]["n"])
        for r in events
        if r["task"] == "courier"
    ]
    assert [c[:2] for c in couriers] == [c[:2] for c in COURIERS]
    drawn = [x for c in couriers for x in c[2:]]
    assert drawn == pytest.approx([x for c in COURIERS for x in c[2:]])
