"""`process.target` of a wait on a family member reads the member's name."""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
from harness import RecordingEgress, event_trace  # noqa: E402

from desflow.core.context import SimulationContext  # noqa: E402

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
