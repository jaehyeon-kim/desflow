"""`process.target` of a wait on a family member reads the member's name."""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
from harness import RecordingEgress, event_trace  # noqa: E402

from desflow.builder.context import SimulationContext  # noqa: E402

MODEL = """\
simulation: {{sim_id: P, factor: 0, random_seed: 42}}
resources: {{desk: {{family: true, members: 2, current_cap: 1}}}}
arrivals:
  first: {{initial: 1, spawn: hold, attributes: {{i: 0}}}}
  viares: {{dist: constant, value: 0.5, max_events: 1, spawn: byres, attributes: {{i: 0}}}}
  viawait: {{dist: constant, value: 0.5, max_events: 1, spawn: bywait, attributes: {{i: 0}}}}
  bell: {{dist: constant, value: 2, spawn: ring, max_events: 1}}
tasks:
  hold: {{resource: {{of: desk, key: "attr.i"}}, service_time: 10, payload: {{event_type: held}}}}
  byres: {{resource: {{of: desk, key: "attr.i"}}, service_time: 1, on_interrupt: abandon, payload: {{event_type: byres}}}}
  bywait: {{wait: {{request: {{of: desk, key: "attr.i"}}}}, service_time: 1, on_interrupt: abandon, payload: {{event_type: bywait}}}}
  ring: {{payload: {{event_type: bell}}, interrupt: {{task: byres, where: "process.target == 'resource.desk[0]'"}}, next: ring2}}
  ring2: {{payload: {{event_type: bell2}}, interrupt: {{task: bywait, where: "process.target == '{where}'"}}}}
run: {{until: 30}}
"""


def _interrupted(tmp_path, where):
    path = tmp_path / "m.yaml"
    path.write_text(MODEL.format(where=where))
    app = SimulationContext.from_yaml(path)
    sink = RecordingEgress()
    app.add_egress(sink)
    app.run(until=30)
    return [
        task
        for _t, _key, task, status in event_trace(sink.records)
        if status == "interrupted"
    ]


def test_a_wait_on_a_member_reads_the_member_name(tmp_path):
    member = _interrupted(tmp_path, "resource.desk[0]")
    family = _interrupted(tmp_path, "resource.desk")
    assert "bywait" in member, member
    assert "bywait" not in family, family


STORE_MODEL = """\
simulation: {{sim_id: P, factor: 0, random_seed: 42}}
stores: {{bin: {{family: true, members: 2, current_cap: 1}}}}
arrivals:
  first: {{initial: 1, spawn: fill, attributes: {{i: 0}}}}
  viaget: {{dist: constant, value: 0.5, max_events: 1, spawn: byget, attributes: {{i: 1}}}}
  viaput: {{dist: constant, value: 0.5, max_events: 1, spawn: byput, attributes: {{i: 0}}}}
  bell: {{dist: constant, value: 2, spawn: ring, max_events: 1}}
tasks:
  fill: {{put: {{store: {{of: bin, key: "attr.i"}}, item: 1}}, payload: {{event_type: filled}}}}
  byget: {{get: {{store: {{of: bin, key: "attr.i"}}}}, on_interrupt: abandon, payload: {{event_type: byget}}}}
  byput: {{put: {{store: {{of: bin, key: "attr.i"}}, item: 2}}, on_interrupt: abandon, payload: {{event_type: byput}}}}
  ring: {{payload: {{event_type: bell}}, interrupt: {{task: byget, where: "process.target == 'get.bin[1]'"}}, next: ring2}}
  ring2: {{payload: {{event_type: bell2}}, interrupt: {{task: byput, where: "process.target == '{where}'"}}}}
run: {{until: 30}}
"""


def test_a_tasks_own_get_and_put_on_a_member_read_the_member_name(tmp_path):
    """The get of `byget` reads `get.bin[1]`, and the put of `byput`, blocked by the
    full member, reads `put.bin[0]`, as a wait on the same member does."""
    path = tmp_path / "m.yaml"
    path.write_text(STORE_MODEL.format(where="put.bin[0]"))
    app = SimulationContext.from_yaml(path)
    sink = RecordingEgress()
    app.add_egress(sink)
    app.run(until=30)
    hit = [
        t for _t, _k, t, status in event_trace(sink.records) if status == "interrupted"
    ]
    assert sorted(hit) == ["byget", "byput"], hit
