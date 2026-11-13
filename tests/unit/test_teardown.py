"""The final flush gives every sink its records, whichever sink has stopped consuming."""

import asyncio
import logging

import pytest

from desflow.connectors.egress.base import BaseEgress
from desflow.builder.context import SimulationContext


class _Keeper(BaseEgress):
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        import queue

        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


class _Stuck(BaseEgress):
    async def run(self, egress_queue):
        while True:
            await asyncio.sleep(1)


def _run(stuck_first, caplog):
    keeper, stuck = _Keeper(), _Stuck()
    app = SimulationContext("S", factor=0, random_seed=1)
    app.with_batching(
        batch_size=10, flush_interval=100, max_queued_batches=1, drain_stall_seconds=0.5
    )
    app.add_arrival("a", "constant", value=1, spawn="job", max_events=8)
    app.task(service_time=0, name="job")(lambda task_id: {})
    for sink in [stuck, keeper] if stuck_first else [keeper, stuck]:
        app.add_egress(sink)
    with caplog.at_level(logging.WARNING):
        app.run(until=20)
    return keeper


@pytest.mark.parametrize(
    "stuck_first", [False, True], ids=["healthy first", "healthy second"]
)
def test_a_healthy_sink_gets_every_record_in_both_orders(stuck_first, caplog):
    keeper = _run(stuck_first, caplog)
    events = [r for r in keeper.records if r.get("stream_type") == "event"]
    assert len(events) == 16, len(events)


def test_teardown_returns_after_a_sink_has_run_its_close_code():
    import queue

    from desflow.core.environment import DynamicRealtimeEnvironment

    closed = []

    class Closing(BaseEgress):
        async def run(self, egress_queue):
            try:
                while True:
                    try:
                        egress_queue.get_nowait()
                    except queue.Empty:
                        await asyncio.sleep(0.01)
            finally:
                await asyncio.sleep(0.3)
                closed.append(True)

    env = DynamicRealtimeEnvironment(factor=0)
    env.setup_egress([Closing()])
    env.publish_event("k", {"v": 1})
    env.teardown()
    assert closed == [True]
    assert not env._egress_thread.is_alive()


def test_a_provider_reused_with_the_envelope_back_on_gets_whole_records():
    from desflow.core.environment import DynamicRealtimeEnvironment

    keeper = _Keeper()
    for envelopes, expected in (([False], False), (None, True)):
        env = DynamicRealtimeEnvironment(factor=0)
        env.setup_egress([keeper], envelopes=envelopes)
        env.teardown()
        assert keeper.envelope is expected
