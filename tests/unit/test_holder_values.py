"""Holder declarations take only capacities, levels and amounts that can work.

Each value is refused with the holder's name before the clock moves, in YAML (with
the file and line) and in the Builder."""

import numpy as np
import pytest

from desflow.builder.context import SimulationContext

nan = float("nan")

HEAD = (
    "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
    "arrivals: {a: {gap: 1, max_events: 3, spawn: t}}\n"
)
TAIL = "run: {until: 10}\n"

RESOURCE_CASES = [
    ("current_cap: -1", "current_cap"),
    ("current_cap: -.inf", "current_cap"),
    ("current_cap: .nan", "current_cap"),
    ("current_cap: true, max_cap: 4", "current_cap"),
    ("current_cap: 2.5", "current_cap"),
    ("current_cap: 1, max_cap: -1", "max_cap"),
    ("max_cap: -.inf", "max_cap"),
    ("max_cap: -1", "max_cap"),
    ("current_cap: 1, max_cap: 4, queue_limit: true", "queue_limit"),
    ("current_cap: 1, max_cap: 4, queue_limit: -1", "queue_limit"),
]


@pytest.mark.parametrize("given, field", RESOURCE_CASES)
def test_yaml_refuses_a_bad_resource_value_with_the_line(tmp_path, given, field):
    path = tmp_path / "m.yaml"
    path.write_text(
        HEAD
        + f"resources: {{desk: {{{given}}}}}\n"
        + "tasks: {t: {resource: desk, service_time: 1, payload: {}}}\n"
        + TAIL
    )
    with pytest.raises(Exception) as caught:
        SimulationContext.from_yaml(path)
    text = str(caught.value)
    assert "m.yaml:3" in text and field in text, text
    assert "whole number of 0 or more" in text or "queue_limit" in text, text


@pytest.mark.parametrize(
    "kwargs",
    [
        {"current_cap": -1},
        {"current_cap": float("-inf")},
        {"current_cap": nan},
        {"current_cap": True},
        {"current_cap": 2.5},
        {"current_cap": 1, "max_cap": -1},
        {"max_cap": float("-inf")},
        {"current_cap": 1, "max_cap": 4, "queue_limit": True},
        {"current_cap": 1, "max_cap": 4, "queue_limit": 2.5},
    ],
)
def test_builder_refuses_a_bad_resource_value(kwargs):
    with pytest.raises(ValueError, match="desk"):
        SimulationContext("S", factor=0).add_resource("desk", **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"current_cap": 5, "level": 10},
        {"current_cap": 5, "level": -1},
        {"current_cap": 5, "level": nan},
        {"current_cap": -1},
        {"current_cap": nan},
    ],
)
def test_builder_refuses_a_bad_container(kwargs):
    with pytest.raises(ValueError, match="container 'tank'"):
        SimulationContext("S", factor=0).add_container("tank", **kwargs)


@pytest.mark.parametrize(
    "given",
    [
        "current_cap: 5, level: 10",
        "current_cap: 5, level: -1",
        "current_cap: 5, level: .nan",
    ],
)
def test_yaml_refuses_a_bad_container_level(tmp_path, given):
    path = tmp_path / "m.yaml"
    path.write_text(
        HEAD
        + f"containers: {{tank: {{{given}}}}}\ntasks: {{t: {{payload: {{}}}}}}\n"
        + TAIL
    )
    with pytest.raises(Exception, match=r"m\.yaml:3: container 'tank'"):
        SimulationContext.from_yaml(path)


@pytest.mark.parametrize("amount", ["-1", "0", ".nan", ".inf", "true"])
def test_yaml_refuses_a_bad_container_amount(tmp_path, amount):
    path = tmp_path / "m.yaml"
    path.write_text(
        HEAD
        + "containers: {tank: {current_cap: 5, level: 5}}\n"
        + f"tasks: {{t: {{get: {{container: tank, amount: {amount}}}, payload: {{}}}}}}\n"
        + TAIL
    )
    with pytest.raises(Exception, match="m.yaml|amount"):
        SimulationContext.from_yaml(path)


@pytest.mark.parametrize("amount", [0, -1, nan, float("inf")])
def test_builder_refuses_a_bad_container_amount(amount):
    app = SimulationContext("S", factor=0)
    app.add_container("tank", current_cap=5, level=5)
    with pytest.raises(ValueError, match="amount"):
        app.task(
            name="take", service_time=0, get={"container": "tank", "amount": amount}
        )(lambda task_id: {})


@pytest.mark.parametrize("make", [np.int32, np.int64, np.float32, np.float64])
def test_builder_takes_a_numpy_container_amount_as_the_plain_number(make):
    """A NumPy number stands for the plain one in a container `get` and `put`, as it
    does in a batch size and a seize priority, and a bad one is still refused."""
    app = SimulationContext("S", factor=0)
    app.add_container("tank", current_cap=5, level=5)
    app.task(name="take", service_time=0, get={"container": "tank", "amount": make(2)})(
        lambda task_id: {}
    )
    app.task(name="give", service_time=0, put={"container": "tank", "amount": make(2)})(
        lambda task_id: {}
    )
    with pytest.raises(ValueError, match="amount"):
        app.task(
            name="none", service_time=0, get={"container": "tank", "amount": make(0)}
        )(lambda task_id: {})


def test_a_family_with_members_that_read_the_entity_is_refused(tmp_path):
    with pytest.raises(ValueError, match="members"):
        SimulationContext("S", factor=0).add_resource(
            "pool", family=True, members="attr.k"
        )
    path = tmp_path / "m.yaml"
    path.write_text(
        HEAD
        + "resources: {desk: {family: true, members: 'attr.k'}}\n"
        + "tasks: {t: {payload: {}}}\n"
        + TAIL
    )
    with pytest.raises(Exception, match=r"m\.yaml:\d+"):
        SimulationContext.from_yaml(path)


def test_a_family_member_capacity_from_an_expression_is_checked_when_it_is_made(
    tmp_path,
):
    path = tmp_path / "m.yaml"
    path.write_text(
        HEAD
        + "resources: {desk: {family: true, current_cap: '0 - 2'}}\n"
        + "tasks: {t: {resource: {of: desk, key: '0'}, service_time: 1, payload: {}}}\n"
        + TAIL
    )
    app = SimulationContext.from_yaml(path)
    with pytest.raises(Exception, match=r"desk.*current_cap.*desk\[0\]|desk\[0\].*-2"):
        app.run(until=10)
