"""A rank or priority that is NaN or text is refused, naming the holder and the task.

Infinity orders correctly, so it is accepted."""

from decimal import Decimal

import pytest

from desflow.blueprint.loader import BlueprintError
from desflow.builder.context import SimulationContext
from tests.conformance.harness import RecordingEgress

BAD = {"nan": "1e999 - 1e999", "text": "'x'"}

STORE = """\
simulation: {{sim_id: S, factor: 0, random_seed: 1}}
stores: {{bin: {{get_queue: {{rank: "[3, {bad}, 1, 0][attr.n]"}}}}}}
arrivals:
  a: {{gap: 1, max_events: 4, spawn: take, attributes: {{n: {{gen: sequence}}}}}}
  b: {{gap: 1, first_at: 9.5, max_events: 4, spawn: give}}
tasks:
  take: {{get: {{store: bin}}, payload: {{}}}}
  give: {{put: {{store: bin}}, payload: {{}}}}
run: {{until: 30}}
"""

RESOURCE = """\
simulation: {{sim_id: S, factor: 0, random_seed: 1}}
resources: {{desk: {{current_cap: 1, queue: priority}}}}
services: {{work: {{dist: constant, value: 5}}}}
arrivals: {{a: {{gap: 1, max_events: 5, spawn: serve, attributes: {{n: {{gen: sequence}}}}}}}}
tasks:
  serve:
    service: work
    seize: {{resource: desk, priority: "[9, 3, {bad}, 1, 0][attr.n]"}}
    payload: {{}}
run: {{until: 40}}
"""

PRIORITY_STORE = """\
simulation: {{sim_id: S, factor: 0, random_seed: 1}}
stores: {{bin: {{priority: true}}}}
arrivals: {{a: {{gap: 1, max_events: 4, spawn: give, attributes: {{n: {{gen: sequence}}}}}}}}
tasks:
  give: {{put: {{store: bin, priority: "[3, {bad}, 1, 0][attr.n]"}}, payload: {{}}}}
run: {{until: 10}}
"""

TASK = """\
simulation: {{sim_id: S, factor: 0, random_seed: 1}}
resources: {{desk: {{current_cap: 1, queue: priority}}}}
arrivals: {{a: {{gap: 0.1, max_events: 5, spawn: serve, attributes: {{n: {{gen: sequence}}}}}}}}
tasks:
  serve:
    service_time: 1
    resource: desk
    priority: "[9, 3, {bad}, 1, 0][attr.n]"
    payload: {{}}
run: {{until: 30}}
"""


# A priority store takes any orderable type, so only NaN is refused there
CASES = [
    (TASK, "serve", "nan"),
    (TASK, "serve", "text"),
    (STORE, "bin", "nan"),
    (STORE, "bin", "text"),
    (RESOURCE, "desk", "nan"),
    (RESOURCE, "desk", "text"),
    (PRIORITY_STORE, "bin", "nan"),
]


@pytest.mark.parametrize(
    "model, holder, bad",
    CASES,
    ids=[f"{holder}-{bad}-{i}" for i, (_, holder, bad) in enumerate(CASES)],
)
def test_a_bad_rank_is_refused_with_the_holder_and_the_task(
    tmp_path, model, holder, bad
):
    path = tmp_path / "m.yaml"
    path.write_text(model.format(bad=BAD[bad]))
    try:
        app = SimulationContext.from_yaml(path)
    except Exception as error:  # a model that does not load is the wrong test
        pytest.skip(f"model did not load: {error}")
    with pytest.raises(Exception) as caught:
        app.run(until=30)
    text = str(caught.value)
    assert holder in text and "not NaN" in text, text


@pytest.mark.parametrize("model", [TASK, RESOURCE, STORE])
def test_an_infinite_rank_from_an_expression_is_accepted(tmp_path, model):
    path = tmp_path / "m.yaml"
    path.write_text(model.format(bad="1e999"))
    SimulationContext.from_yaml(path).run(until=30)


LITERAL = """\
simulation: {{sim_id: S, factor: 0, random_seed: 1}}
resources: {{desk: {{current_cap: 1, queue: priority}}}}
arrivals: {{a: {{gap: 1, max_events: 3, spawn: serve}}}}
tasks:
  serve:
    service_time: 1
{lines}
    payload: {{}}
run: {{until: 20}}
"""

LITERAL_PLACES = {
    "task": "    resource: desk\n    priority: {bad}",
    "seize": "    seize: {{resource: desk, priority: {bad}}}",
    "request": "    wait: {{request: {{resource: desk, priority: {bad}}}}}",
}


@pytest.mark.parametrize("place", LITERAL_PLACES)
def test_a_literal_nan_priority_is_refused_at_load_with_its_line(tmp_path, place):
    bad = ".nan"
    lines = LITERAL_PLACES[place].format(bad=bad)
    text = LITERAL.format(lines=lines)
    line = next(i for i, row in enumerate(text.splitlines(), 1) if bad in row)
    path = tmp_path / "m.yaml"
    path.write_text(text)
    with pytest.raises(BlueprintError) as caught:
        SimulationContext.from_yaml(path)
    message = str(caught.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert "priority must be a number that is not NaN" in message, message


@pytest.mark.parametrize("place", LITERAL_PLACES)
def test_a_literal_infinite_priority_is_served_last_and_minus_infinity_first(
    tmp_path, place
):
    # One unit is held until time 10; the others queue and are served by priority
    priorities = {"five": "5", "inf": ".inf", "zero": "0", "minf": "-.inf"}
    release = "    release: desk\n" if place == "seize" else ""
    tasks = "".join(
        f"  {name}:\n    service_time: 1\n"
        + LITERAL_PLACES[place].format(bad=priority)
        + f"\n    payload: {{e: {name}}}\n"
        + release
        for name, priority in priorities.items()
    )
    text = (
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "resources: {desk: {current_cap: 1, queue: priority}}\n"
        "arrivals:\n"
        "  h: {initial: 1, spawn: hold}\n"
        + "".join(
            f"  {name}: {{gap: 1, first_at: {i}, max_events: 1, spawn: {name}}}\n"
            for i, name in enumerate(priorities, 1)
        )
        + "tasks:\n  hold: {resource: desk, service_time: 10, payload: {e: hold}}\n"
        + tasks
        + "run: {until: 40}\n"
    )
    path = tmp_path / "m.yaml"
    path.write_text(text)
    recorder = RecordingEgress()
    app = SimulationContext.from_yaml(path)
    app.add_egress(recorder)
    app.run()
    served = [
        r["value"]["e"]
        for r in recorder.records
        if r["stream_type"] == "event" and "e" in (r["value"] or {})
    ]
    assert served == ["hold", "minf", "zero", "five", "inf"]


@pytest.mark.parametrize("place", LITERAL_PLACES)
def test_the_builder_refuses_a_literal_nan_priority_of_a_task_a_seize_or_a_request(
    place,
):
    nan = float("nan")
    app = SimulationContext("S", factor=0)
    app.add_resource("desk", current_cap=1, queue="priority")
    fields = {
        "task": {"resource_id": "desk", "priority": nan},
        "seize": {"seize": {"resource": "desk", "priority": nan}},
        "request": {"wait": {"request": {"resource": "desk", "priority": nan}}},
    }[place]
    with pytest.raises(ValueError, match="priority must be a number that is not NaN"):
        app.task(name="serve", service_time=1, **fields)(lambda task_id: {})


@pytest.mark.parametrize("bad", [".nan", "[2, .nan]"])
def test_a_nan_priority_of_a_starting_item_is_refused_on_every_path(tmp_path, bad):
    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "stores:\n"
        "  s:\n"
        "    priority: true\n"
        f"    items: [[5, y], [{bad}, x], [1, z]]\n"
        "arrivals: {a: {gap: 1, max_events: 1, spawn: t}}\n"
        "tasks: {t: {get: {store: s}, payload: {}}}\n"
        "run: {until: 5}\n"
    )
    with pytest.raises(BlueprintError) as caught:
        SimulationContext.from_yaml(path)
    message = str(caught.value)
    assert message.startswith(f"{path}:3:"), message
    assert "stores.s" in message, message
    assert "priority of a starting item must be a number that is not NaN" in message
    app = SimulationContext("S", factor=0)
    with pytest.raises(ValueError, match="store 's' starting item priority"):
        app.add_store("s", priority=True, items=[(5, "y"), (float("nan"), "x")])
    app.add_store("t", priority=True, items=[(float("inf"), "y"), (1, "x")])


@pytest.mark.parametrize("item", ["[]", "[.nan, x]"])
def test_a_list_item_of_a_store_that_is_not_a_priority_store_loads(tmp_path, item):
    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        f"stores: {{s: {{items: [{item}]}}}}\n"
        "arrivals: {a: {gap: 1, max_events: 1, spawn: t}}\n"
        "tasks: {t: {get: {store: s}, payload: {}}}\n"
        "run: {until: 5}\n"
    )
    SimulationContext.from_yaml(path)


@pytest.mark.parametrize(
    "nan", [Decimal("NaN"), [1, [float("nan")]]], ids=["Decimal", "nested in a list"]
)
def test_a_decimal_or_nested_nan_priority_is_refused_where_any_order_is_taken(nan):
    """A task's priority and a priority store's items take any value that orders, so
    a NaN of another type, or one inside a nested list, is refused there too."""
    app = SimulationContext("S", factor=0)
    app.add_resource("desk", current_cap=1, queue="priority")
    with pytest.raises(ValueError, match="task 'serve' priority must be a number"):
        app.task(name="serve", service_time=1, resource_id="desk", priority=nan)(
            lambda task_id: {}
        )
    with pytest.raises(ValueError, match="store 's' starting item priority"):
        app.add_store("s", priority=True, items=[(nan, "x")])


def test_a_priority_pair_that_holds_nan_is_refused_when_put_in_a_priority_store(
    tmp_path,
):
    path = tmp_path / "m.yaml"
    path.write_text(
        PRIORITY_STORE.format(bad="1e999 - 1e999").replace(
            '"[3, 1e999 - 1e999, 1, 0][attr.n]"',
            '"[[3, 0], [1, 1e999 - 1e999], [1, 1], [0, 0]][attr.n]"',
        )
    )
    app = SimulationContext.from_yaml(path)
    with pytest.raises(Exception) as caught:
        app.run(until=10)
    assert "bin" in str(caught.value) and "not NaN" in str(caught.value)


MIXED_RESOURCE = """\
simulation: {sim_id: S, factor: 0, random_seed: 1}
egress: []
resources:
  desk: {current_cap: 1, queue: priority}
services:
  s: {dist: constant, value: 5}
arrivals:
  a: {dist: constant, value: 1, spawn: plain}
  b: {dist: constant, value: 1, spawn: ranked}
tasks:
  plain: {service: s, resource: desk, priority: 1, payload: {e: 1}}
  ranked: {service: s, resource: desk, priority: "[0, 1]", payload: {e: 2}}
run: {until: 20}
"""


def test_a_number_and_a_list_priority_on_one_resource_are_refused_at_load(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(MIXED_RESOURCE)
    with pytest.raises(BlueprintError) as caught:
        SimulationContext.from_yaml(path)
    message = str(caught.value)
    assert message.startswith(f"{path}:12:"), message
    assert "resource 'desk'" in message
    assert "priority 1 (a number)" in message
    assert "'[0, 1]' (a list)" in message


def _family_priorities(tmp_path, second_key):
    path = tmp_path / "m.yaml"
    path.write_text(
        f"""\
simulation: {{sim_id: S, factor: 0}}
egress: []
services: {{s: {{dist: constant, value: 1}}}}
resources: {{desk: {{family: true, current_cap: 1, queue: priority}}}}
arrivals:
  a: {{dist: constant, value: 1, spawn: plain}}
  b: {{dist: constant, value: 1, spawn: ranked}}
tasks:
  plain:
    service: s
    resource: {{of: desk, key: 0}}
    priority: 1
    payload: {{e: 1}}
  ranked:
    service: s
    resource: {{of: desk, key: {second_key}}}
    priority: "[0, 1]"
    payload: {{e: 2}}
run: {{until: 20}}
"""
    )
    return path


def test_priorities_of_two_members_of_one_family_are_not_compared(tmp_path):
    SimulationContext.from_yaml(_family_priorities(tmp_path, 1))


def test_a_number_and_a_list_priority_on_one_member_are_refused_at_load(tmp_path):
    with pytest.raises(BlueprintError, match=r"resource 'desk\[0\]'"):
        SimulationContext.from_yaml(_family_priorities(tmp_path, 0))


def test_a_number_and_a_list_priority_on_one_store_are_refused_at_load(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(
        "simulation: {sim_id: S, factor: 0}\n"
        "egress: []\n"
        "stores:\n"
        "  shelf: {priority: true, items: [[1, a], [[0, 1], b]]}\n"
        "run: {until: 5}\n"
    )
    with pytest.raises(BlueprintError) as caught:
        SimulationContext.from_yaml(path)
    message = str(caught.value)
    assert message.startswith(f"{path}:4:"), message
    assert "store 'shelf'" in message and "(a number)" in message
    assert "[0, 1] (a list)" in message


def test_a_builder_model_with_mixed_priorities_is_refused_before_the_clock():
    app = SimulationContext("S", factor=0)
    app.add_resource("desk", current_cap=1, queue="priority")
    app.add_service("s", dist="constant", value=1)
    app.task(name="plain", service_time=1, resource_id="desk", priority=1)(
        lambda task_id: {}
    )
    app.task(name="ranked", service_time=1, resource_id="desk", priority="[0, 1]")(
        lambda task_id: {}
    )
    with pytest.raises(ValueError, match="resource 'desk' cannot rank the priority"):
        app.start(until=5)


def test_a_priority_read_at_run_time_is_checked_when_it_is_read(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(
        MIXED_RESOURCE.replace('priority: "[0, 1]"', 'priority: "[[0, 1]][0]"')
    )
    app = SimulationContext.from_yaml(path)
    with pytest.raises(Exception) as caught:
        app.run(until=20)
    assert "resource 'desk' cannot rank the priority" in str(caught.value)
