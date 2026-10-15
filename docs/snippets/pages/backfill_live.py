"""Per-sink batching and the low-level `go_live_at` of the Backfill Then Go Live guide.

Building the connectors and the environment opens no connection and starts no run, so
tests/unit run this file without a broker.
"""

from datetime import datetime, timedelta

from desflow import (
    DynamicRealtimeEnvironment,
    KafkaEgress,
    ParquetStorageEgress,
    SimulationContext,
)

GO_LIVE_ISO = datetime.now().isoformat(timespec="milliseconds")


def is_history(record: dict) -> bool:
    return record["timestamp"] < GO_LIVE_ISO


def is_live(record: dict) -> bool:
    return record["timestamp"] >= GO_LIVE_ISO


parquet = ParquetStorageEgress(path_router=lambda data: "data/backfill/events.parquet")
kafka = KafkaEgress(bootstrap_servers="localhost:9092")
app = SimulationContext(sim_id="Shop", factor=0.0)

# --8<-- [start:per-sink]
app.add_egress(parquet, when=is_history, batch_size=200_000)
app.add_egress(kafka, when=is_live, batch_size=500, flush_interval=1.0)
# --8<-- [end:per-sink]

# --8<-- [start:low-level]
go_live_at = datetime.now()

env = DynamicRealtimeEnvironment(
    factor=0.0,
    logical_start_time=go_live_at - timedelta(days=7),
    go_live_at=go_live_at,
)
# --8<-- [end:low-level]

if __name__ == "__main__":
    # Until the logical clock reaches go_live_at, the run is unpaced.
    assert env.factor == 0.0
