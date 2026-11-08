"""A store `put` in a `wait` or an `open` is typed as a task's own `put` is."""

import pytest

from desflow.core.context import SimulationContext

HEAD = (
    "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
    "stores: {bin: {}}\n"
    "arrivals: {a: {gap: 1, max_events: 2, spawn: t}}\n"
    "run: {until: 10}\n"
)
UNIFORM = "{gen: uniform, low: 0, high: 1}"

PLACES = {
    "put": f"{{payload: {{}}, put: {{store: bin, item: {UNIFORM}}}}}",
    "wait": f"{{payload: {{}}, wait: {{put: {{store: bin, item: {UNIFORM}}}}}}}",
    "open": f"{{payload: {{}}, open: {{h: {{put: {{store: bin, item: {UNIFORM}}}}}}}}}",
    "all": "{payload: {}, wait: {all: {p: {put: {store: bin, item: {k: "
    + UNIFORM
    + "}}}}}}",
}


def _items(tmp_path, task):
    path = tmp_path / "m.yaml"
    path.write_text(HEAD + "tasks: {t: " + task + "}\n")
    app = SimulationContext.from_yaml(path)
    app.run(until=4)
    return app._run.holders.stores["bin"].items


@pytest.mark.parametrize("place", ["put", "wait", "open"])
def test_a_put_draws_its_item_in_every_place(tmp_path, place):
    items = _items(tmp_path, PLACES[place])
    assert len(items) == 2
    assert all(isinstance(item, float) and 0 <= item <= 1 for item in items), items


def test_a_put_under_all_draws_a_generator_inside_its_mapping(tmp_path):
    items = _items(tmp_path, PLACES["all"])
    assert all(isinstance(item["k"], float) for item in items), items


def test_a_put_under_each_reads_its_expression(tmp_path):
    items = _items(
        tmp_path,
        "{payload: {}, wait: {each: 2, branch: {put: {store: bin, "
        "item: {gen: expr, expr: 'each.index'}}}}}",
    )
    assert items == [0, 1, 0, 1]


@pytest.mark.parametrize(
    "task",
    [
        "{payload: null, wait: {put: {store: bin}}}",
        "{payload: null, open: {h: {put: {store: bin}}}}",
        "{payload: null, wait: {each: 2, branch: {put: {store: bin}}}}",
        "{payload: null, wait: {all: {p: {put: {store: bin}}}}}",
    ],
)
def test_payload_null_refuses_a_put_in_a_wait_or_an_open(tmp_path, task):
    path = tmp_path / "m.yaml"
    path.write_text(HEAD + "tasks: {t: " + task + "}\n")
    with pytest.raises(Exception, match="payload: null publishes no record"):
        SimulationContext.from_yaml(path)
