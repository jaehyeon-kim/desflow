"""Two family references that read as the same text are compared by their parts."""

import pytest

from desflow.builder.context import SimulationContext

MODEL = """\
simulation: {{sim_id: P, factor: 0, random_seed: 42}}
resources: {{desk: {{family: true, members: 2, current_cap: 1}}, a: {{current_cap: 1}}, b: {{current_cap: 1}}}}
arrivals:
  x: {{dist: constant, value: 1, max_events: 1, spawn: tx, attributes: {{i: 0}}}}
  y: {{dist: constant, value: 1, max_events: 1, spawn: ty, attributes: {{i: 1}}}}
tasks:
  tx: {{resource: {rx}, service_time: 1, batch: {{name: lot, size: 2}}, payload: {{event_type: x}}}}
  ty: {{resource: {ry}, service_time: 1, batch: {{name: lot, size: 2}}, payload: {{event_type: y}}}}
run: {{until: 10}}
"""


@pytest.mark.parametrize(
    "rx, ry",
    [
        ("a", "b"),
        ('{of: desk, key: "attr.i"}', '{of: desk, key: "1 - attr.i"}'),
    ],
    ids=["plain names", "family references"],
)
def test_two_tasks_that_share_a_batch_and_differ_in_resource_are_refused(
    tmp_path, rx, ry
):
    path = tmp_path / "m.yaml"
    path.write_text(MODEL.format(rx=rx, ry=ry))
    with pytest.raises(Exception, match="differ in resource"):
        SimulationContext.from_yaml(path)


def test_two_equal_references_are_accepted(tmp_path):
    path = tmp_path / "m.yaml"
    same = '{of: desk, key: "attr.i"}'
    path.write_text(MODEL.format(rx=same, ry=same))
    SimulationContext.from_yaml(path)
