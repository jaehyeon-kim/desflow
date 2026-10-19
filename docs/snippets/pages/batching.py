"""The delivery settings of the Egress page, in Python.

tests/docs runs this file: each sink is built, nothing is connected, and a short
unpaced run uses the console sinks.
"""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, SimulationContext

stream, lake, audit = ConsoleEgress(), ConsoleEgress(), ConsoleEgress()

app = SimulationContext(sim_id="Shop", factor=0.0)

# --8<-- [start:every-sink]
app.add_egress(stream).add_egress(lake)  # both receive every record
# --8<-- [end:every-sink]

app = SimulationContext(sim_id="Shop", factor=0.0)

# --8<-- [start:when]
# A record's timestamp is an ISO string with its zone offset, so read it as a time
hot_from = datetime.now(timezone.utc)


def is_hot(record) -> bool:
    return datetime.fromisoformat(record["timestamp"]) >= hot_from


app.add_egress(stream, when=is_hot)  # hot tail
app.add_egress(lake, when=lambda r: not is_hot(r))  # cold history
app.add_egress(audit)  # no predicate: everything
# --8<-- [end:when]

# A time written in another zone compares by the instant it names
later = (hot_from + timedelta(seconds=1)).astimezone(ZoneInfo("Australia/Melbourne"))
assert is_hot({"timestamp": later.isoformat(timespec="milliseconds")})

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

app = SimulationContext(sim_id="Shop", factor=0.0, warmup="2 min")

# --8<-- [start:labels]
app.add_egress(stream)  # events and telemetry, never a label
app.add_egress(audit, when="labels")  # ground-truth labels, and nothing else
# --8<-- [end:labels]

# --8<-- [start:low-level]
env = DynamicRealtimeEnvironment(factor=0.0)
env.setup_egress(
    [stream, lake],
    batch_size=500,  # defaults for every provider
    flush_interval=1.0,
    predicates=[None, lambda r: r["stream_type"] == "event"],  # by position
    batch_sizes=[50, None],  # None: the default
    flush_intervals=[0.5, None],
    labels=[False, False],  # True makes a provider a labels sink
    warmup=None,  # simulation seconds before which no provider gets a record
)
# --8<-- [end:low-level]

if __name__ == "__main__":
    try:
        env.run(until=3)
    finally:
        env.teardown()
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.run(until=3)
