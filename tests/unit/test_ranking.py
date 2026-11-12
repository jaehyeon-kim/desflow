"""Ranking and the queue limit (Batch 8, part 2): a `queue` rank and a `preempt` rule on a
resource, a computed `preempt` and a `request` mapping, a rank on the waiting gets and
puts of a store and a container, and `queue_limit` as a registry path. The refusals each
key makes at load, the Builder and Low-level forms, and the state left after a run."""

import re
from pathlib import Path
from typing import Any, List, Tuple

import numpy as np
import pytest

from desflow import SimulationContext, replicate
from desflow.blueprint.loader import BlueprintError
from desflow.core.registry import ParamRejected
from desflow.resources.resource import DynamicResource
from tests.conformance.harness import run_builder, run_yaml_text

BASE = """\
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}, p: {current_cap: 1, queue: priority, preempt: true}, r: {current_cap: 1, queue: {rank: "attr.x"}}}
stores: {s: {}}
containers: {c: {level: 0}}
arrivals: {a: {dist: constant, value: 1, spawn: t, attributes: {x: 1}}}
tasks:
  t: TASK
  u: {payload: {e: u}}
run: {until: 5}
"""


def load(tmp_path: Path, task: str, base: str = BASE) -> SimulationContext:
    path = tmp_path / "model.yaml"
    path.write_text(base.replace("TASK", task))
    return SimulationContext.from_yaml(path)


def run_text(tmp_path: Path, text: str) -> List[Tuple[Any, ...]]:
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    return [
        (round(r["sim_ts"], 3), r["key"], r["value"].get("e") or r["value"]["status"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and ("e" in r["value"] or r["value"].get("status") in ("balked", "preempted"))
    ]


# A task line 7 of BASE; each is refused with the place and the reason
REFUSED_TASKS = [
    (
        "{resource: r, priority: 3, service_time: 1, payload: {e: 1}}",
        "has a priority, but resource 'r' ranks its queue by queue.rank, which is every "
        "request's priority; remove the priority",
    ),
    (
        "{seize: {resource: r, priority: 3}, payload: {e: 1}}",
        "has a priority on its seize, but resource 'r' ranks its queue by queue.rank",
    ),
    (
        "{wait: {request: {resource: r, priority: 3}}, payload: {e: 1}}",
        "has a priority on its request in its wait, but resource 'r' ranks its queue by "
        "queue.rank",
    ),
    (
        "{open: {k: {request: {resource: r, priority: 3}}}, payload: {e: 1}}",
        "has a priority on its request in its open, but resource 'r' ranks its queue by "
        "queue.rank",
    ),
    (
        "{wait: {request: {resource: m, priority: 3}}, payload: {e: 1}}",
        "resource 'm' does not set queue: priority",
    ),
    (
        "{wait: {request: {resource: p, nope: 3}}, payload: {e: 1}}",
        "request takes a resource and, optionally, priority and preempt",
    ),
    (
        "{wait: {request: {resource: p, preempt: 'attr.nosuch +'}}, payload: {e: 1}}",
        "'attr.nosuch +' is not a valid expression",
    ),
    (
        "{priority: 2, wait: {request: {resource: p, priority: 1}}, payload: {e: 1}}",
        "every request of this task is a mapping",
    ),
    (
        "{preempt: false, wait: {any: {a: {request: {resource: p}}, b: {request: {resource: p, priority: 1}}}}, payload: {e: 1}}",
        "every request of this task is a mapping",
    ),
    (
        "{priority: 'attr.x', open: {k: {request: {resource: p}}}, payload: {e: 1}}",
        "every request of this task is a mapping",
    ),
    ("{resource: p, preempt: 3, service_time: 1, payload: {e: 1}}", "preempt"),
    ("{resource: p, preempt: 'wait.x', service_time: 1, payload: {e: 1}}", "wait"),
]


@pytest.mark.parametrize("task,message", REFUSED_TASKS)
def test_a_bad_request_option_is_refused_at_load_with_its_line(tmp_path, task, message):
    with pytest.raises(BlueprintError, match=re.escape(message)) as error:
        load(tmp_path, task)
    assert "model.yaml:" in str(error.value)


GOOD_MAPPING_TASKS = [
    "{priority: 2, wait: {request: p}, payload: {e: 1}}",
    "{priority: 2, wait: {all: {a: {request: p}, b: {request: {resource: p}}}}, payload: {e: 1}}",
    "{preempt: false, seize: p, wait: {request: {resource: p}}, payload: {e: 1}}",
    "{priority: 2, open: {k: {request: p}}, payload: {e: 1}}",
    "{preempt: true, wait: {request: {resource: p}}, payload: {e: 1}}",
]


@pytest.mark.parametrize("task", GOOD_MAPPING_TASKS)
def test_a_task_priority_is_allowed_where_a_request_site_takes_it(tmp_path, task):
    load(tmp_path, task)


def test_the_task_that_a_mapping_only_priority_is_refused_for_is_named(tmp_path):
    with pytest.raises(BlueprintError) as error:
        load(tmp_path, "{priority: 2, wait: {request: {resource: p}}, payload: {e: 1}}")
    assert "model.yaml:7" in str(error.value)


@pytest.mark.parametrize(
    "keys,refused",
    [
        ({"priority": 2}, True),
        ({"preempt": False}, True),
        ({"preempt": "f"}, True),
        ({"preempt": "t"}, False),
        ({"preempt": True}, False),
        ({}, False),
    ],
)
def test_the_builder_refuses_a_task_priority_that_only_mapping_requests_could_use(
    keys, refused
):
    app = SimulationContext("S", factor=0)
    app.add_resource("p", queue="priority", preempt=True)
    app.add_arrival("a", dist="constant", value=1, spawn="t")

    def build():
        app.task(name="t", wait={"request": {"resource": "p"}}, service_time=1, **keys)(
            lambda task_id: {"e": 1}
        )

    if refused:
        with pytest.raises(ValueError, match=r"task 't': .*every request of this task"):
            build()
    else:
        build()


def test_a_request_of_a_resource_that_is_not_added_fails_when_the_run_starts(tmp_path):
    app = load(tmp_path, "{wait: {request: {resource: nosuch}}, payload: {e: 1}}")
    with pytest.raises(ValueError, match="task 't' requests resource 'nosuch'"):
        app.run(until=1)


# A resources, stores or containers line 2 to 4 of BASE, refused at load with its line
REFUSED_KEYS = [
    ("resources", "{r: {queue: {rank: 'wait.x'}}}", "2:", "write wait.<name>.done"),
    (
        "resources",
        "{r: {queue: {rank: 'attr.x +'}}}",
        "2:",
        "is not a valid expression",
    ),
    (
        "resources",
        "{r: {queue: {rank: 'item'}}}",
        "2:",
        "reads item, which a request has not got",
    ),
    (
        "resources",
        "{r: {queue: {rank: 'handle.k.value'}}}",
        "2:",
        "reads handle, which a request",
    ),
    (
        "resources",
        "{r: {queue: {rank: 'request.rank'}}}",
        "2:",
        "which only the when and victim",
    ),
    ("resources", "{r: {queue: {rank: 1}}}", "2:", "Input should be a valid string"),
    ("resources", "{r: {queue: {ties: lifo}}}", "2:", "rank: Field required"),
    (
        "resources",
        "{r: {queue: {rank: 'attr.x', ties: sideways}}}",
        "2:",
        "Input should be 'fifo' or 'lifo'",
    ),
    (
        "resources",
        "{r: {queue: {rank: 'attr.x', order: 1}}}",
        "2:",
        "unknown key 'order'",
    ),
    (
        "resources",
        "{r: {queue: {rank: 'attr.x'}, preempt: {when: 'attr.x > 1'}}}",
        "2:",
        "reads attr, which a preempt rule cannot",
    ),
    (
        "resources",
        "{r: {queue: priority, preempt: {when: 'holder.nosuch > 1'}}}",
        "2:",
        "write holder.rank",
    ),
    (
        "resources",
        "{r: {queue: priority, preempt: {when: 'holder.rank >'}}}",
        "2:",
        "is not a valid expression",
    ),
    (
        "resources",
        "{r: {queue: priority, preempt: {victim: 'handle.k.value'}}}",
        "2:",
        "reads handle, which a preempt rule cannot",
    ),
    (
        "resources",
        "{r: {queue: priority, preempt: {when: 'queue.nosuch > 1'}}}",
        "2:",
        "queue.nosuch names no resource",
    ),
    (
        "resources",
        "{r: {queue: priority, preempt: {when: 't > 1', then: 1}}}",
        "2:",
        "unknown key 'then'",
    ),
    (
        "resources",
        "{r: {queue: fifo, preempt: {when: 't > 1'}}}",
        "2:",
        "needs queue: priority or a queue rank",
    ),
    ("resources", "{r: {queue_limit: -1}}", "2:", "queue_limit must be 0 or more"),
    ("resources", "{r: {queue_limit: 1.5}}", "2:", "a number with a fractional part"),
    (
        "stores",
        "{s: {get_queue: {rank: 'attr.x', ties: x}}}",
        "3:",
        "Input should be 'fifo' or 'lifo'",
    ),
    (
        "stores",
        "{s: {get_queue: {rank: 'item'}}}",
        "3:",
        "reads item, which a request has not got",
    ),
    (
        "stores",
        "{s: {put_queue: fifo}}",
        "3:",
        "valid dictionary or instance of QueueRule",
    ),
    (
        "containers",
        "{c: {get_queue: {rank: 'attr.x', order: 1}}}",
        "4:",
        "unknown key 'order'",
    ),
    (
        "containers",
        "{c: {put_queue: {rank: 'wait.a'}}}",
        "4:",
        "write wait.<name>.done",
    ),
]


@pytest.mark.parametrize("key,value,line,message", REFUSED_KEYS)
def test_a_bad_rule_is_refused_at_load_with_its_line(
    tmp_path, key, value, line, message
):
    text = BASE.replace("TASK", "{service_time: 1, payload: {e: 1}}")
    text = "\n".join(
        f"{key}: {value}" if row.startswith(f"{key}:") else row
        for row in text.split("\n")
    )
    path = tmp_path / "model.yaml"
    path.write_text(text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    assert f"model.yaml:{line}" in str(error.value)
    assert message in str(error.value)


GOOD_TASKS = [
    "{resource: p, preempt: true, priority: 1, service_time: 1, payload: {e: 1}}",
    "{resource: p, preempt: 'attr.x > 1', priority: 1, service_time: 1, payload: {e: 1}}",
    "{resource: r, preempt: 'attr.x > 1', service_time: 1, payload: {e: 1}}",
    "{wait: {request: {resource: p, priority: 'attr.x', preempt: false}}, payload: {e: 1}}",
    "{wait: {request: {resource: r, preempt: 'attr.x > 1'}}, payload: {e: 1}}",
    "{open: {k: {request: {resource: p, priority: 2}}}, payload: {e: 1}}",
    "{get: {store: s}, payload: {e: 1}}",
]


@pytest.mark.parametrize("task", GOOD_TASKS)
def test_a_good_request_option_loads(tmp_path, task):
    load(tmp_path, task)


def test_a_rule_that_loaded_as_a_plain_word_still_loads(tmp_path):
    """`queue: priority`, `fifo`, `lifo` and `preempt: true` load as they did."""
    text = BASE.replace("TASK", "{service_time: 1, payload: {e: 1}}").replace(
        'r: {current_cap: 1, queue: {rank: "attr.x"}}',
        "r: {current_cap: 1, queue: lifo}, q: {current_cap: 2, queue: priority, preempt: false}",
    )
    path = tmp_path / "model.yaml"
    path.write_text(text)
    SimulationContext.from_yaml(path)


# -------------------------------------------------------------- boolean words


@pytest.mark.parametrize(
    "word,preempts",
    [
        ("t", True),
        ("yes", True),
        ("on", True),
        ("1", True),
        ("true", True),
        ("f", False),
        ("no", False),
        ("off", False),
        ("0", False),
        ("false", False),
    ],
)
@pytest.mark.parametrize("place", ["task", "request", "seize"])
def test_a_boolean_word_for_preempt_is_a_boolean(tmp_path, word, preempts, place):
    """`preempt: "t"` means true and `preempt: "off"` false, in a task, a `request`
    mapping and a `seize`, as the boolean words of YAML validation always did."""
    options = {
        "task": f"resource: p, preempt: '{word}', priority: 0, service_time: 4",
        "request": f"wait: {{request: {{resource: p, priority: 0, preempt: '{word}'}}}}, service_time: 4",
        "seize": f"seize: {{resource: p, priority: 0, preempt: '{word}'}}, service_time: 4, release: p",
    }[place]
    text = f"""
simulation: {{sim_id: S, factor: 0}}
resources: {{p: {{current_cap: 1, queue: priority, preempt: true}}}}
arrivals:
  low: {{dist: constant, value: 1, spawn: low, max_events: 1, first_at: 0}}
  high: {{dist: constant, value: 1, spawn: high, max_events: 1, first_at: 1}}
tasks:
  low: {{resource: p, priority: 5, service_time: 4, on_interrupt: abandon, payload: {{e: low_done}}}}
  high: {{{options}, payload: {{e: high_done}}}}
run: {{until: 20}}
"""
    marks = run_text(tmp_path, text)
    assert ((1.0, "low-1", "preempted") in marks) is preempts
    assert (1.0 + 4 if preempts else 4.0 + 4, "high-1", "high_done") in marks


def test_other_text_for_preempt_is_an_expression(tmp_path):
    with pytest.raises(BlueprintError, match="is not a valid expression"):
        load(
            tmp_path,
            "{resource: p, preempt: 'yes please', service_time: 1, payload: {e: 1}}",
        )


# ------------------------------------------------------- Builder and Low-level


RANKED_YAML = """
simulation: {sim_id: S, factor: 0}
resources:
  m: {current_cap: 1, queue: {rank: "attr.rank", ties: lifo}, preempt: {when: "request.rank < holder.rank and t - holder.since > 1", victim: "holder.rank"}}
stores: {s: {get_queue: {rank: "attr.rank", ties: lifo}}}
arrivals:
  a: {dist: constant, spawn: job, value: 1000, first_at: 0, attributes: {rank: 5, pre: true}}
  b: {dist: constant, value: 1000, spawn: job, first_at: 2, attributes: {rank: 1, pre: true}}
  c: {dist: constant, value: 1000, spawn: job, first_at: 3, attributes: {rank: 1, pre: false}}
  g1: {dist: constant, spawn: get, value: 1000, first_at: 0, attributes: {rank: 2}}
  g2: {dist: constant, spawn: get, value: 1000, first_at: 0, attributes: {rank: 2}}
  p1: {dist: constant, value: 1000, spawn: put, first_at: 1}
  p2: {dist: constant, value: 1000, spawn: put, first_at: 2}
tasks:
  job: {resource: m, preempt: "attr.pre", service_time: 3, on_interrupt: abandon, payload: {e: done}}
  get: {get: {store: s}, payload: {e: got}}
  put: {put: {store: s}, payload: {e: put}}
run: {until: 30}
"""


def ranked_builder(limit: Any = None, rank_type: Any = int) -> SimulationContext:
    app = SimulationContext("S", factor=0)
    app.add_resource(
        "m",
        current_cap=1,
        queue={"rank": "attr.rank", "ties": "lifo"},
        preempt={
            "when": "request.rank < holder.rank and t - holder.since > 1",
            "victim": "holder.rank",
        },
    )
    app.add_store("s", get_queue={"rank": "attr.rank", "ties": "lifo"})
    for name, at, rank, pre in (
        ("a", 0, 5, True),
        ("b", 2, 1, True),
        ("c", 3, 1, False),
    ):
        app.add_arrival(
            name,
            dist="constant",
            value=1000,
            spawn="job",
            first_at=at,
            attributes={"rank": rank_type(rank), "pre": pre},
        )
    for name in ("g1", "g2"):
        app.add_arrival(
            name,
            dist="constant",
            value=1000,
            spawn="get",
            first_at=0,
            attributes={"rank": rank_type(2)},
        )
    app.add_arrival("p1", dist="constant", value=1000, spawn="put", first_at=1)
    app.add_arrival("p2", dist="constant", value=1000, spawn="put", first_at=2)
    app.task(
        name="job",
        resource_id="m",
        preempt="attr.pre",
        service_time=3,
        on_interrupt="abandon",
    )(lambda task_id: {"e": "done"})
    app.task(name="get", get={"store": "s"})(lambda task_id: {"e": "got"})
    app.task(name="put", put={"store": "s"})(lambda task_id: {"e": "put"})
    return app


def marks(records) -> List[Tuple[Any, ...]]:
    return [
        (round(r["sim_ts"], 3), r["key"], r["value"].get("e") or r["value"]["status"])
        for r in records
        if r["stream_type"] == "event"
        and ("e" in r["value"] or r["value"].get("status") == "preempted")
    ]


@pytest.mark.parametrize("rank_type", [int, np.int64, float, np.float32])
def test_the_builder_runs_the_rules_as_the_blueprint_does(tmp_path, rank_type):
    """A `queue` rank, `ties`, a `preempt` rule, a computed `preempt` and a store's
    `get_queue` through the Builder give the records of the blueprint, with the ranks
    given as NumPy numbers too."""
    outcome = run_yaml_text(tmp_path, RANKED_YAML, {})
    assert outcome.error is None, repr(outcome.error)
    wanted = marks(outcome.records)
    assert any(m[2] == "preempted" for m in wanted) and any(
        m[2] == "got" for m in wanted
    )
    assert marks(run_builder(ranked_builder(rank_type=rank_type), 30)) == wanted


def test_the_builder_refuses_what_the_blueprint_refuses_in_a_rule():
    app = SimulationContext("S", factor=0)
    with pytest.raises(ValueError, match="rank"):
        app.add_resource("a", queue={"ties": "lifo"})
    with pytest.raises(ValueError, match="wait.x"):
        app.add_resource("a", queue={"rank": "wait.x"})
    with pytest.raises(ValueError, match="sideways"):
        app.add_resource("a", queue={"rank": "attr.x", "ties": "sideways"})
    with pytest.raises(ValueError, match="needs queue='priority'"):
        app.add_resource("a", queue="fifo", preempt={"when": "t > 1"})
    with pytest.raises(ValueError, match="attr"):
        app.add_resource("a", queue="priority", preempt={"when": "attr.x > 1"})
    with pytest.raises(ValueError, match="holder.nosuch"):
        app.add_resource("a", queue="priority", preempt={"victim": "holder.nosuch"})
    with pytest.raises(ValueError, match="sideways"):
        app.add_store("s", get_queue={"rank": "attr.x", "ties": "sideways"})
    with pytest.raises(ValueError, match="item"):
        app.add_container("c", put_queue={"rank": "item"})
    # nothing that failed was added
    app.add_resource("a")
    app.add_store("s")
    app.add_container("c")


def test_the_builder_refuses_a_priority_where_the_queue_ranks_by_a_rule():
    app = SimulationContext("S", factor=0)
    app.add_resource("r", queue={"rank": "attr.x"})
    app.add_arrival("a", dist="constant", value=1, spawn="t", attributes={"x": 1})
    app.task(name="t", resource_id="r", priority=2, service_time=1)(
        lambda task_id: {"e": 1}
    )
    with pytest.raises(ValueError, match="ranks its queue by an expression"):
        app.run(until=2)


def test_a_custom_store_type_takes_no_get_queue():
    class Mine(DynamicResource):
        pass

    from desflow.resources.store import DynamicStore

    class MyStore(DynamicStore):
        pass

    app = SimulationContext("S", factor=0)
    with pytest.raises(ValueError, match="type"):
        app.add_store("s", type=MyStore, get_queue={"rank": "attr.x"})


# ------------------------------------------------------------- queue_limit as a path

LIMIT_PATH = "S.resources.m.queue_limit"


def limit_app(limit: Any = 1) -> SimulationContext:
    """A model that has run to 0.5 s, so its registry holds the resource."""
    app = SimulationContext("S", factor=0)
    app.add_resource("m", current_cap=1, queue_limit=limit)
    app.add_arrival("a", dist="constant", value=1000, spawn="t")
    app.task(name="t", resource_id="m", service_time=1)(lambda task_id: {"e": 1})
    run_builder(app, 0.5)
    return app


def test_queue_limit_is_a_registry_path_whose_start_value_is_the_limit_given():
    for limit in (None, 0, 3, np.int64(2)):
        app = limit_app(limit)
        assert app._env.registry.get(LIMIT_PATH).value == limit
    # a resource with no limit has the path too
    app = SimulationContext("S", factor=0)
    app.add_resource("m", current_cap=1)
    app.add_arrival("a", dist="constant", value=1000, spawn="t")
    app.task(name="t", resource_id="m", service_time=1)(lambda task_id: {"e": 1})
    run_builder(app, 0.5)
    assert app._env.registry.get(LIMIT_PATH).value is None


@pytest.mark.parametrize(
    "written,stored",
    [
        (None, None),
        (0, 0),
        (5, 5),
        (np.int64(2), 2),
        (2.0, 2),
        (np.float64(3.0), 3),
        ("4", 4),
        (" 6 ", 6),
        ("4.0", 4),
        ("1e3", 1000),
    ],
)
def test_a_write_to_queue_limit_takes_a_whole_number_or_null(written, stored):
    app = limit_app(1)
    change = app._env.registry.change(LIMIT_PATH, written)
    assert change.new == stored
    assert app._env.registry.get(LIMIT_PATH).value == stored
    assert app._run.holders.resources["m"].queue_limit == stored


@pytest.mark.parametrize(
    "written",
    [-1, 1.5, True, "many", "", float("inf"), float("nan"), [1], np.float64(0.5)],
)
def test_a_bad_write_to_queue_limit_is_refused_by_name_and_keeps_the_limit(written):
    app = limit_app(2)
    with pytest.raises(
        ParamRejected, match="whole number of 0 or more, or null"
    ) as error:
        app._env.registry.change(LIMIT_PATH, written)
    assert LIMIT_PATH in str(error.value)
    assert app._env.registry.get(LIMIT_PATH).value == 2


def test_a_resource_with_its_own_put_queue_has_no_queue_limit_path():
    class Mine(DynamicResource):
        class PutQueue(list):
            def __init__(self, maxlen=None):
                super().__init__()
                self.maxlen = maxlen

            def append(self, item):
                super().append(item)

    app = SimulationContext("S", factor=0)
    app.add_resource("m", current_cap=1, type=Mine)
    app.add_resource("k", current_cap=1)
    app.add_arrival("a", dist="constant", value=1000, spawn="t")
    app.task(name="t", resource_id="m", service_time=1)(lambda task_id: {"e": 1})
    run_builder(app, 0.5)
    with pytest.raises(KeyError):
        app._env.registry.get(LIMIT_PATH)
    # the default resource keeps the path
    assert app._env.registry.get("S.resources.k.queue_limit").value is None


LIMIT_TEXT = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue_limit: 1}}
arrivals: {j: {dist: constant, value: 1, spawn: job, max_events: 8, first_at: 0}}
tasks:
  job: {resource: m, service_time: 20, payload: {e: done}}
run: {until: 100}
"""


def test_a_smaller_limit_removes_nobody_who_waits_and_applies_from_the_next_request(
    tmp_path,
):
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue_limit: 4}}
arrivals:
  j: {dist: constant, value: 1, spawn: job, max_events: 8, first_at: 0}
  look: {dist: constant, value: 1000, spawn: probe, first_at: 7.5}
tasks:
  job: {resource: m, service_time: 20, payload: {e: done}}
  probe: {payload: {e: probe, x: {gen: expr, expr: "queue.m"}}}
scenario: [{at: 4.5, path: S.resources.m.queue_limit, value: 1}, {at: 6.5, path: S.resources.m.queue_limit, value: null}]
run: {until: 100}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    seen = [
        (r["sim_ts"], r["key"])
        for r in outcome.records
        if r["stream_type"] == "event" and r["value"].get("status") == "balked"
    ]
    # j-1 holds; j-2 to j-5 wait (4); the limit is 1 at 4.5 and nobody leaves; j-6 at 5
    # and j-7 at 6 find 4 waiting and balk; j-8 at 7 is let in under no limit
    assert seen == [(5.0, "j-6"), (6.0, "j-7")]
    assert outcome.app_or_env._run.holders.resources["m"].queue_limit is None
    waiting = [
        r["value"]["x"] for r in outcome.records if r["value"].get("e") == "probe"
    ]
    assert waiting == [5]


def test_a_limit_of_zero_balks_even_at_a_free_unit(tmp_path):
    text = LIMIT_TEXT.replace("queue_limit: 1", "queue_limit: 0")
    outcome = run_yaml_text(tmp_path, text, {})
    statuses = [
        r["value"].get("status") for r in outcome.records if r["stream_type"] == "event"
    ]
    assert statuses.count("balked") == 8
    resource = outcome.app_or_env._run.holders.resources["m"]
    assert not resource.queue and not resource.users


def test_a_write_to_queue_limit_is_checked_at_load_with_its_line(tmp_path):
    text = LIMIT_TEXT.replace(
        "run: {until: 100}",
        "scenario:\n  - {at: 2, path: S.resources.m.queue_limit, value: -3}\nrun: {until: 100}",
    )
    path = tmp_path / "model.yaml"
    path.write_text(text)
    with pytest.raises(BlueprintError, match="whole number of 0 or more") as error:
        SimulationContext.from_yaml(path)
    assert "model.yaml:" in str(error.value)


def test_a_write_to_queue_limit_of_a_resource_that_is_not_there_is_refused_at_load(
    tmp_path,
):
    text = LIMIT_TEXT.replace(
        "run: {until: 100}",
        "scenario:\n  - {at: 2, path: S.resources.nosuch.queue_limit, value: 3}\nrun: {until: 100}",
    )
    path = tmp_path / "model.yaml"
    path.write_text(text)
    with pytest.raises(BlueprintError, match="is not a registry path"):
        SimulationContext.from_yaml(path)


def test_a_limit_written_applies_to_a_wait_request_and_an_open_request(tmp_path):
    """The limit is read at each request, whatever makes it: a task's resource, a wait
    `request` branch and an `open` request."""
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue_limit: 1}}
arrivals:
  h: {dist: constant, value: 1000, spawn: hold, first_at: 0}
  w: {dist: constant, value: 1000, spawn: ask, first_at: 1}
  o: {dist: constant, value: 1000, spawn: opened, first_at: 3}
  look: {dist: constant, value: 1000, spawn: probe, first_at: 5}
tasks:
  hold: {resource: m, service_time: 8, payload: {e: held}}
  ask: {wait: {request: m}, payload: {e: asked}}
  opened: {open: {k: {request: m}}, payload: {e: opened}}
  probe: {payload: {e: probe, x: {gen: expr, expr: "queue.m"}}}
scenario: [{at: 2, path: S.resources.m.queue_limit, value: LIMIT}]
run: {until: 30}
"""
    outcome = run_yaml_text(tmp_path, text.replace("LIMIT", "2"), {})
    assert outcome.error is None, repr(outcome.error)
    probe = [r["value"]["x"] for r in outcome.records if r["value"].get("e") == "probe"]
    assert probe == [2]
    # with the limit left at 1 the open request meets a full line and balks
    outcome = run_yaml_text(tmp_path, text.replace("LIMIT", "1"), {})
    assert outcome.error is None, repr(outcome.error)
    balked = [
        (r["sim_ts"], r["key"])
        for r in outcome.records
        if r["value"].get("status") == "balked"
    ]
    assert balked == [(3.0, "o-1")]


# ----------------------------------------------------- no state left behind

LEAVERS = {
    "a renege from a ranked line": """
resources: {m: {current_cap: 1, queue: {rank: "attr.x"}}}
arrivals: {h: {dist: constant, value: 1000, spawn: hold, first_at: 0, attributes: {x: 1}}, w: {dist: constant, value: 1, spawn: want, max_events: 4, first_at: 1, attributes: {x: 2}}}
tasks:
  hold: {resource: m, service_time: 6, payload: {e: held}}
  want: {resource: m, service_time: 1, patience: 2, payload: {e: served}}
""",
    "a balk from a ranked line under a limit": """
resources: {m: {current_cap: 1, queue: {rank: "attr.x"}, queue_limit: 1}}
arrivals: {h: {dist: constant, value: 1000, spawn: hold, first_at: 0, attributes: {x: 1}}, w: {dist: constant, value: 1, spawn: want, max_events: 4, first_at: 1, attributes: {x: 2}}}
tasks:
  hold: {resource: m, service_time: 6, payload: {e: held}}
  want: {resource: m, service_time: 1, payload: {e: served}}
""",
    "an interrupt of a ranked request that waits": """
resources: {m: {current_cap: 1, queue: {rank: "attr.x"}}}
arrivals: {h: {dist: constant, value: 1000, spawn: hold, first_at: 0, attributes: {x: 1}}, w: {dist: constant, value: 1000, spawn: want, first_at: 1, attributes: {x: 2}}, c: {dist: constant, value: 1000, spawn: cut, first_at: 2, attributes: {x: 2}}}
tasks:
  hold: {resource: m, service_time: 6, payload: {e: held}}
  want: {resource: m, service_time: 1, on_interrupt: abandon, payload: {e: served}}
  cut: {interrupt: {task: want, cause: stop}, payload: {e: cut}}
""",
    "a preemption by a rule": """
resources: {m: {current_cap: 1, queue: {rank: "attr.x"}, preempt: {when: "request.rank < holder.rank"}}}
arrivals: {h: {dist: constant, value: 1000, spawn: hold, first_at: 0, attributes: {x: 5}}, w: {dist: constant, value: 1000, spawn: want, first_at: 1, attributes: {x: 1}}}
tasks:
  hold: {resource: m, service_time: 6, on_interrupt: abandon, payload: {e: held}}
  want: {resource: m, service_time: 1, payload: {e: served}}
""",
    "a cancel of a ranked get": """
stores: {s: {get_queue: {rank: "attr.x"}}}
arrivals: {w: {dist: constant, value: 1000, spawn: open_get, first_at: 0, attributes: {x: 2}}}
tasks:
  open_get: {open: {g: {get: {store: s}}}, next: drop, payload: {e: opened}}
  drop: {service_time: 1, cancel: g, payload: {e: dropped}}
""",
    "a renege of a ranked get": """
stores: {s: {get_queue: {rank: "attr.x"}}}
arrivals: {w: {dist: constant, value: 1000, spawn: want, first_at: 0, attributes: {x: 2}}}
tasks:
  want: {get: {store: s}, patience: 2, payload: {e: got}}
""",
    "a cancel of a ranked put": """
stores: {s: {current_cap: 1, put_queue: {rank: "attr.x"}}}
arrivals: {f: {dist: constant, value: 1000, spawn: fill, first_at: 0, attributes: {x: 1}}, w: {dist: constant, value: 1000, spawn: add, first_at: 1, attributes: {x: 2}}}
tasks:
  fill: {put: {store: s}, payload: {e: filled}}
  add: {open: {q: {put: {store: s}}}, next: drop, payload: {e: added}}
  drop: {service_time: 1, cancel: q, payload: {e: dropped}}
""",
    "a renege of a ranked container get": """
containers: {c: {level: 0, get_queue: {rank: "attr.x"}}}
arrivals: {w: {dist: constant, value: 1000, spawn: want, first_at: 0, attributes: {x: 2}}}
tasks:
  want: {get: {container: c, amount: 3}, patience: 2, payload: {e: got}}
""",
}


@pytest.mark.parametrize("name", sorted(LEAVERS))
def test_a_flow_that_leaves_a_ranked_line_leaves_nothing_in_it(tmp_path, name):
    """A renege, a balk, an interrupt, a cancel and a preemption take the request, get
    or put out of its ranked line and give back its unit, so nothing is left once the
    flows have ended."""
    text = "simulation: {sim_id: S, factor: 0}\n" + LEAVERS[name] + "run: {until: 50}\n"
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    app = outcome.app_or_env
    for resource in app._run.holders.resources.values():
        assert not resource.queue and not resource.users, name
    for holder in list(app._run.holders.stores.values()) + list(
        app._run.holders.containers.values()
    ):
        assert not holder.get_queue and not holder.put_queue, name
    # a get or put that is made after the line was left is not given to the one that left
    statuses = {
        r["value"].get("status") for r in outcome.records if r["stream_type"] == "event"
    }
    assert (
        statuses & {"reneged", "balked", "interrupted", "preempted"} or "cancel" in name
    ), name


def test_replicate_runs_a_model_with_a_rank_a_rule_and_a_changing_limit(tmp_path):
    path = tmp_path / "m.yaml"
    path.write_text(
        """
simulation: {sim_id: S, factor: 0, random_seed: 3}
resources:
  m: {current_cap: 1, queue: {rank: "attr.x", ties: lifo}, preempt: {when: "request.rank < holder.rank"}, queue_limit: 3}
arrivals: {a: {dist: exponential, rate: 1, spawn: job, attributes: {x: {gen: integer, bounds: [1, 5]}}}}
services: {s: {dist: exponential, rate: 1.2}}
tasks:
  job: {resource: m, service: s, preempt: "attr.x < 3", on_interrupt: resume, payload: {event_type: done}}
scenario: [{at: 40, path: S.resources.m.queue_limit, value: 6}]
stats: typical
run: {until: 100}
"""
    )
    result = replicate(str(path), 3, seed=1, measures=["resources.m.utilization_avg"])
    values = result.values["resources.m.utilization_avg"]
    assert len(values) == 3 and all(0 < v <= 100 for v in values)
    assert len(set(values)) == 3


def test_explain_names_a_preempt_rule_and_a_computed_preempt_as_preemption(tmp_path):
    """A resource with a `preempt` rule and a task whose `preempt` is an expression
    are preemption, which the queueing formulas leave out; a rank queue alone is not."""
    from typer.testing import CliRunner

    from desflow.cli import app

    path = tmp_path / "m.yaml"
    path.write_text(
        """
simulation: {sim_id: S, factor: 0}
resources:
  m: {current_cap: 1, queue: {rank: "attr.x"}, preempt: {when: "request.rank < holder.rank"}}
  f: {current_cap: 1, queue: {rank: "attr.x"}}
arrivals: {a: {dist: exponential, rate: 0.1, spawn: job, attributes: {x: 1}}, b: {dist: exponential, rate: 0.1, spawn: plain, attributes: {x: 1}}}
services: {s: {dist: exponential, rate: 1}}
tasks:
  job: {resource: m, service: s, preempt: "attr.x < 3", payload: {event_type: done}}
  plain: {resource: f, service: s, payload: {event_type: done}}
run: {until: 100}
"""
    )
    result = CliRunner().invoke(app, ["explain", str(path)])
    assert result.exit_code == 0, result.output
    assert "task job: it has preemption" in result.output
    assert "task plain" not in result.output


@pytest.mark.parametrize(
    "ties,order", [("fifo", ["g-1", "g-2", "g-3"]), ("lifo", ["g-3", "g-2", "g-1"])]
)
@pytest.mark.parametrize("holder", ["store", "container"])
def test_ties_order_equal_ranks_of_waiting_gets_in_a_store_and_a_container(
    tmp_path, ties, order, holder
):
    """Of gets of one rank, `ties: fifo` serves the earliest and `lifo` the latest."""
    if holder == "store":
        part = f"stores: {{s: {{get_queue: {{rank: 'attr.x', ties: {ties}}}}}}}"
        get, put = "{get: {store: s}", "{put: {store: s}"
    else:
        part = f"containers: {{s: {{level: 0, get_queue: {{rank: 'attr.x', ties: {ties}}}}}}}"
        get, put = "{get: {container: s, amount: 1}", "{put: {container: s, amount: 1}"
    text = f"""
simulation: {{sim_id: S, factor: 0}}
{part}
arrivals:
  g: {{dist: constant, value: 0.1, spawn: get, max_events: 3, first_at: 0, attributes: {{x: 1}}}}
  p: {{dist: constant, value: 1, spawn: put, max_events: 3, first_at: 1}}
tasks:
  get: {get}, payload: {{e: got}}}}
  put: {put}, payload: {{e: put}}}}
run: {{until: 20}}
"""
    marks = run_text(tmp_path, text)
    assert [key for _, key, label in marks if label == "got"] == order


def test_a_scenario_can_set_a_limit_on_a_resource_that_had_none_and_remove_it_again(
    tmp_path,
):
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
arrivals: {j: {dist: constant, value: 1, spawn: job, max_events: 8, first_at: 0}}
tasks:
  job: {resource: m, service_time: 20, payload: {e: done}}
scenario:
  - {at: 2.5, path: S.resources.m.queue_limit, value: 1}
  - {at: 5.5, path: S.resources.m.queue_limit, value: null}
run: {until: 100}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, repr(outcome.error)
    balked = [r["key"] for r in outcome.records if r["value"].get("status") == "balked"]
    # j-1 holds, j-2 and j-3 wait under no limit; from 2.5 the limit is 1 and j-4 (at 3),
    # j-5 and j-6 (at 4, 5) balk; from 5.5 there is no limit again
    assert balked == ["j-4", "j-5", "j-6"]


def test_a_preempt_rule_reads_the_attributes_of_the_holder(tmp_path):
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1, queue: {rank: "attr.x"}, preempt: {when: "request.rank < holder.rank and holder.attr.cheap == 1"}}}
arrivals:
  a: {dist: constant, value: 1000, spawn: job, first_at: 0, attributes: {x: 5, cheap: 0}}
  b: {dist: constant, value: 1000, spawn: job, first_at: 1, attributes: {x: 1, cheap: 0}}
  c: {dist: constant, value: 1000, spawn: job, first_at: 10, attributes: {x: 5, cheap: 1}}
  d: {dist: constant, value: 1000, spawn: job, first_at: 11, attributes: {x: 1, cheap: 0}}
tasks:
  job: {resource: m, service_time: 4, on_interrupt: abandon, payload: {e: done}}
run: {until: 40}
"""
    marks = run_text(tmp_path, text)
    # a holds a cheap=0 unit, so b does not take it; c holds with cheap=1, so d does
    assert [(t, k, label) for t, k, label in marks if label == "preempted"] == [
        (11.0, "c-1", "preempted")
    ]
    assert (4.0, "a-1", "done") in marks and (15.0, "d-1", "done") in marks


@pytest.mark.parametrize(
    "resource,scenario,refused",
    [
        ("{current_cap: 1}", "", False),
        ("{current_cap: 1, queue_limit: 2}", "", True),
        (
            "{current_cap: 1}",
            "scenario: [{at: 1, path: S.resources.m.queue_limit, value: 2}]",
            True,
        ),
        (
            "{current_cap: 1}",
            "scenario: [{at: 1, path: S.resources.m.current_cap, value: 2}]",
            False,
        ),
    ],
)
@pytest.mark.parametrize(
    "asks",
    [
        "resource: m",
        "seize: m",
        "open: {r: {request: m}}",
    ],
)
def test_a_task_of_a_resource_whose_limit_is_written_can_end_before_its_vars_have_a_value(
    tmp_path, resource, scenario, refused, asks
):
    """A task that can balk ends before its `vars` have a value, so an `on:` action
    cannot read one: when the limit is given, and when a scenario step writes it, for a
    `resource`, a `seize` and an `open` request alike."""
    text = f"""
simulation: {{sim_id: S, factor: 0}}
resources: {{m: {resource}}}
variables: {{z: 0}}
arrivals: {{j: {{dist: constant, value: 1, spawn: job}}}}
tasks:
  job: {{{asks}, service_time: 1, vars: {{v: 1}}, on: [{{path: S.variables.z, value: {{gen: var, name: v}}}}], payload: {{e: 1}}}}
{scenario}
run: {{until: 3}}
"""
    path = tmp_path / "model.yaml"
    path.write_text(text)
    if refused:
        with pytest.raises(BlueprintError, match="is not a var that has a value here"):
            SimulationContext.from_yaml(path)
    else:
        SimulationContext.from_yaml(path)


def test_a_limit_written_by_a_listen_entry_lets_a_task_end_before_its_vars_have_a_value(
    tmp_path,
):
    """The `path` actions of a `listen` count as writes: a task of a resource whose
    `queue_limit` a listen entry writes can balk, so its `on:` cannot read a var."""
    text = """
simulation: {sim_id: S, factor: 0}
resources: {m: {current_cap: 1}}
variables: {z: 0}
signals: {go: {once: true}}
arrivals: {j: {dist: constant, value: 1, spawn: job}}
tasks:
  job: {resource: m, service_time: 1, vars: {v: 1}, on: [{path: S.variables.z, value: {gen: var, name: v}}], payload: {e: 1}}
  watcher: {listen: {signal: go, on: [{path: S.resources.m.queue_limit, value: 2}]}, payload: {e: 1}}
run: {until: 3}
"""
    path = tmp_path / "model.yaml"
    path.write_text(text)
    with pytest.raises(BlueprintError, match="is not a var that has a value here"):
        SimulationContext.from_yaml(path)


@pytest.mark.parametrize("form", ["resource", "seize", "open"])
@pytest.mark.parametrize(
    "limit,scenario,refused",
    [(None, False, False), (2, False, True), (None, True, True)],
)
def test_the_builder_lets_a_task_end_early_when_its_limit_is_given_or_written(
    form, limit, scenario, refused
):
    """The Builder counts a `queue_limit`, given or written by a scenario step, of the
    resource of a `resource`, a `seize` or an `open` request."""
    from desflow.values.models import Var

    app = SimulationContext("S", factor=0)
    app.add_variable("z", 0)
    app.add_resource("m", current_cap=1, queue_limit=limit)
    if scenario:
        app.add_scenario([{"at": 1, "path": LIMIT_PATH, "value": 2}])
    app.add_arrival("j", dist="constant", value=1, spawn="job")
    asks = {
        "resource": {"resource_id": "m"},
        "seize": {"seize": "m"},
        "open": {"open": {"r": {"request": "m"}}},
    }[form]
    app.task(
        name="job",
        service_time=1,
        vars={"v": 1},
        on=[{"path": "S.variables.z", "value": Var(gen="var", name="v")}],
        **asks,
    )(lambda task_id: {"e": 1})
    if refused:
        with pytest.raises(ValueError, match="is not a var that has a value here"):
            app.run(until=3)
    else:
        app.run(until=3)
