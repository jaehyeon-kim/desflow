from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.signals import Signal


def test_a_signal_failure_nobody_waits_for_is_defused():
    """A `Signal.fail` with no waiter defuses the failed event, so the run goes on."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    closing = Signal(env, "closing")

    def process():
        yield env.timeout(1)
        closing.fail("shutdown")

    env.process(process())
    env.run(until=5)
    assert closing.ok is False
    assert closing.count == 1
    assert closing.value == "shutdown"
