"""The batching settings of the Batching and Delivery page, in Python.

tests/unit runs this file: each sink is built, nothing is connected, and a short
unpaced run uses the console sinks.
"""

from datetime import datetime

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, SimulationContext

stream, lake, audit = ConsoleEgress(), ConsoleEgress(), ConsoleEgress()
hot_from = datetime.now().isoformat(timespec="milliseconds")

app = SimulationContext(sim_id="Shop", factor=0.0)

# --8<-- [start:every-sink]
app.add_egress(stream).add_egress(lake)  # both receive every record
# --8<-- [end:every-sink]

app = SimulationContext(sim_id="Shop", factor=0.0)

# --8<-- [start:when]
app.add_egress(stream, when=lambda r: r["timestamp"] >= hot_from)  # hot tail
app.add_egress(lake, when=lambda r: r["timestamp"] < hot_from)  # cold history
app.add_egress(audit)  # no predicate: everything
# --8<-- [end:when]

# --8<-- [start:defaults]
app.with_batching(
    batch_size=5_000,  # records per batch
    flush_interval=1.0,  # simulation seconds; not used at factor=0
    max_queued_batches=2_000,  # a full queue makes the simulation wait
    drain_stall_seconds=30.0,  # how long teardown waits for a queue that stopped
)
# --8<-- [end:defaults]

app = SimulationContext(sim_id="Shop", factor=0.0)

# --8<-- [start:per-sink]
app.with_batching(batch_size=500, flush_interval=1.0)  # the default for every sink
app.add_egress(stream, batch_size=50, flush_interval=0.5)  # small and often
app.add_egress(lake, batch_size=200_000)  # large files, the default interval
# --8<-- [end:per-sink]

# --8<-- [start:low-level]
env = DynamicRealtimeEnvironment(factor=0.0)
env.setup_egress(
    [stream, lake],
    batch_size=500,  # defaults for every provider
    flush_interval=1.0,
    predicates=[None, lambda r: r["stream_type"] == "event"],  # by position
    batch_sizes=[50, None],  # None: the default
    flush_intervals=[0.5, None],
)
# --8<-- [end:low-level]

if __name__ == "__main__":
    try:
        env.run(until=3)
    finally:
        env.teardown()
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.run(until=3)
