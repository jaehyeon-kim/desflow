"""`add_arrival(max_events=...)` needs a spawn. Its forms and refusals are cases of
`test_builder_arrival_parity.py`."""

import pytest

from desflow import SimulationContext


def test_max_events_needs_a_spawn():
    app = SimulationContext("S", factor=0)
    with pytest.raises(ValueError, match="max_events needs a spawn"):
        app.add_arrival("a", gap=1, max_events=2)
