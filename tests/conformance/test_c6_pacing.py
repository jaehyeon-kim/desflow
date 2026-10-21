"""R1 real-time factor: desflow paces a run as SimPy's RealtimeEnvironment does.

The fake clock drives `simpy.rt`'s `monotonic` and `sleep`, so a paced run advances
in fake seconds with no real waiting. The test compares the fake wall offset at each
model event between desflow and plain SimPy, at two factors.
"""

import simpy

from desflow import DynamicRealtimeEnvironment
from tests.conformance.harness import FakeClock


def _offsets(env):
    """The (simulation time, fake wall offset from the run's start) at each event, for
    events at +2, +5, +5 (a tie) and +8 from the start."""
    seen = []

    def process():
        for step in (2.0, 3.0, 0.0, 3.0):
            yield env.timeout(step)
            import simpy.rt as rt

            seen.append((round(env.now, 3), round(rt.monotonic() - env.real_start, 6)))

    env.process(process())
    env.run()
    return seen


def test_c6_real_time_factor(monkeypatch):
    """At factor 0.5 and 2, each event's wall offset is the factor times the
    simulation time since the start, and two events at one instant add no wall time,
    exactly as SimPy's RealtimeEnvironment paces them."""
    for factor in (0.5, 2.0):
        FakeClock().install(monkeypatch)
        reference = simpy.rt.RealtimeEnvironment(
            factor=factor, strict=False, initial_time=100
        )
        expected = _offsets(reference)
        assert expected[0] == (102.0, round(factor * 2, 6))
        assert expected[1] == expected[2]  # the tie adds no wall time

        FakeClock().install(monkeypatch)
        env = DynamicRealtimeEnvironment(factor=factor, strict=False, initial_time=100)
        assert _offsets(env) == expected
