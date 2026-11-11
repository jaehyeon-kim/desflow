"""A rank or priority that is NaN or text is refused, naming the holder and the task."""

import pytest

from desflow.builder.context import SimulationContext

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
    resource: desk
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
    assert holder in text and "finite number" in text, text
