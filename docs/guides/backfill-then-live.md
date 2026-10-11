# Backfill Then Go Live in One Run

A tiered demo usually needs two datasets: a backdated history sitting in a lake, and a live stream arriving now. They have to be the same factory, with the same machines, the same distributions and the same task ids, or the join between them is meaningless.

Producing them takes two settings that contradict each other. History wants `factor=0.0`, so a week of events is generated as fast as the machine allows. A live tail wants `factor=1.0`, so events arrive one simulated second per real second.

---

## Why one run rather than two

Before `go_live_at`, the only way to get both was to start two processes with the same `random_seed` and the same `logical_start_time`, one fast-forwarding into Parquet and one pacing into Kafka. That costs:

* A second full execution of the same simulation, so the history is generated twice.
* Seed and start instant duplicated in two places, which have to be edited together. Change one and the two halves silently stop being the same factory.
* No guarantee about the seam. Each process decides on its own where its half ends, so the join can overlap or leave a gap.

`go_live_at` replaces both processes with one. The run generates history unpaced up to that logical instant, then switches to real-time pacing and keeps going. There is one seed, one start instant and one seam.

---

## How the three settings fit together

Three separate settings make up the feature, and each answers a different question.

| Setting | Question it answers |
| --- | --- |
| `logical_start_time` | Where does the logical clock start? Set it in the past to backdate the history. |
| `go_live_at` | At which logical instant does pacing stop being `factor` and become real time? |
| `when` on `add_egress` | Which sink receives each record? |

They are independent. `go_live_at` changes pacing only; it never routes a record. `when` routes records only; it never changes pacing. Using them together is what produces a tiered dataset, and the two instants have to agree: pass the same instant to `go_live_at` and to the predicates, or the seam in the data will not match the seam in the pacing.

![From logical_start_time to go_live_at the run writes the backdated history to Parquet without waiting; from go_live_at to the end it streams records to Kafka in real time](../assets/diagrams/backfill-timeline.png)

---

## Worked example

[`examples/declarative/backfill_live_example.py`](https://github.com/jaehyeon-kim/dynamic-des/blob/main/examples/declarative/backfill_live_example.py) backdates the clock by ten minutes, writes those ten minutes to Parquet in well under a second, then publishes to Kafka in real time for sixty seconds. `HISTORY_MINUTES` and `LIVE_SECONDS` set the two halves; a day of history (about 130,000 records) still generates in about two seconds.

Three settings make the run:

```python title="examples/declarative/backfill_live_example.py:builder"
app = (
    SimulationContext(
        sim_id="Line_A",
        # Unpaced to begin with, so the history costs no real time.
        factor=0.0,
        random_seed=42,
        logical_start_time=LOGICAL_START_TIME,
        # From here the same run is paced at one simulated second per real second.
        go_live_at=GO_LIVE_AT,
    )
    .add_egress(ParquetStorageEgress(path_router=history_router), when=is_history)
    .add_egress(
        KafkaEgress(
            event_topic=EVENT_TOPIC,
            telemetry_topic=TELEMETRY_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
        ),
        when=is_live,
    )
    # Only batch_size governs the history. The interval flush is a simulation process,
    # started only when factor is non-zero as the egress is set up, and this run starts
    # at 0.0. It starts at go-live, so the live tail also flushes every 10 seconds.
    .with_batching(batch_size=2000, flush_interval=10.0)
    .add_resource("lathe", current_cap=4, max_cap=10)
    .add_service("milling", dist="normal", mean=2.0, std=0.2)
    .add_arrival("standard", dist="exponential", rate=0.5)
)
```

Two details in that script are easy to get wrong.

**Predicates compare strings, not datetimes.** A record carries its logical time as an ISO string, so `record["timestamp"] >= GO_LIVE_AT` raises a `TypeError`. Format the instant once with `isoformat(timespec="milliseconds")` and compare against that. Every timestamp is produced by the same formatter, so ordering by text is ordering by time.

```python title="examples/declarative/backfill_live_example.py:clock"
GO_LIVE_AT = datetime.now()
LOGICAL_START_TIME = GO_LIVE_AT - HISTORY

# Records carry their logical time as an ISO string, so the predicates that split the
# two sinks compare strings. That works because every timestamp comes from the same
# formatter: identical layout, so ordering by text is ordering by time.
GO_LIVE_ISO = GO_LIVE_AT.isoformat(timespec="milliseconds")
```

**`flush_interval` has no effect before `go_live_at`, so `batch_size` decides the history.** The interval flush is a simulation process, and it is only started if `factor` is non-zero when the egress is set up. This run starts at `factor=0.0`, so until go-live records leave the buffer only when it fills to `batch_size`. That sets the number of Parquet part files. From go-live the interval flush starts, so the live tail also flushes every `flush_interval`, and that decides how often Kafka sees anything. Give each sink its own `batch_size` and `flush_interval` to separate those two jobs:

```python title="docs/snippets/pages/backfill_live.py:per-sink"
app.add_egress(parquet, when=is_history, batch_size=200_000)
app.add_egress(kafka, when=is_live, batch_size=500, flush_interval=1.0)
```

---

## Running it

The script publishes to Kafka, so it needs a broker. [Backfill Then Go Live](../examples/backfill-live.md) in the examples has the commands to download it, start the broker with odctl and run it, with uv or pip.

The two halves look different enough that the second can be mistaken for a hang. History finishes within the same second the run starts, then nothing is logged at all until teardown, because from `go_live_at` the run waits on the wall clock exactly as a live twin does. `Logical clock reached go-live` is the line that confirms the switch.

---

## What happens at the boundary

[Time and Pacing](../architecture/time.md#what-happens-at-the-boundary) describes how the switch is applied, and the cases where `go_live_at` is before the start or after the end of the run.

---

## Low-level API

`go_live_at` behaves identically on `DynamicRealtimeEnvironment`, alongside `logical_start_time`:

```python title="docs/snippets/pages/backfill_live.py:low-level"
go_live_at = datetime.now()

env = DynamicRealtimeEnvironment(
    factor=0.0,
    logical_start_time=go_live_at - timedelta(days=7),
    go_live_at=go_live_at,
)
```

`env.factor` reports `0.0` until the logical clock reaches `go_live_at`, and `1.0` afterwards.
