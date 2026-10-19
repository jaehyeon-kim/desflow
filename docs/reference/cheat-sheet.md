# Cheat Sheet

The common YAML keys and builder calls side by side, each linked to the page that explains it. [YAML Format](yaml-format.md) lists every key, and the [API Reference](../api.md) every call. In the builder column, `app` is a `SimulationContext`, and a task's settings are arguments of `@app.task(...)`.

## Simulation

| To | YAML | Builder | Read |
|---|---|---|---|
| Name the model and set the clock | `simulation: {sim_id, factor, random_seed}` | `SimulationContext(sim_id, factor=, random_seed=)` | [Time and Pacing](../user-guide/time-and-pacing.md) |
| Leave the start of a run out of the statistics | `simulation.warmup` | `SimulationContext(warmup=)` | [Statistics](../user-guide/statistics.md) |
| Start the clock at a time other than 0 | `simulation.initial_time` | `SimulationContext(initial_time=)` | [Time and Pacing](../user-guide/time-and-pacing.md#initial-time) |
| Write timestamps in a time zone | `simulation.timezone` | `SimulationContext(timezone=)` | [Records and Telemetry](records-and-telemetry.md) |

## Arrivals and Services

| To | YAML | Builder | Read |
|---|---|---|---|
| Draw a service time | `services: {packing: {dist: normal, mean: 3, std: 0.5}}` | `app.add_service("packing", dist="normal", mean=3, std=0.5)` | [Arrivals and Services](../user-guide/arrivals-and-services.md) |
| Start a task at random gaps | `arrivals: {orders: {dist: exponential, rate: 0.5, spawn: pack}}` | `app.add_arrival("orders", dist="exponential", rate=0.5)` and an `arrival_loop` | [Arrivals and Services](../user-guide/arrivals-and-services.md) |
| Give each entity attributes | `arrivals.<name>.attributes` | `app.add_arrival(..., attributes={...})` | [Task Flow](../user-guide/task-flow.md) |
| Replay recorded arrivals | `arrivals.<name>.replay: {path, time}` | `app.add_arrival_replay(name, source)` | [Local Replay](../tutorials/local-replay.md) |
| Fit a service time to data | `desflow fit`, then `!include` | `fit_column(values)` | [Input Fitting](../user-guide/input-fitting.md) |

## Resources, Stores and Containers

| To | YAML | Builder | Read |
|---|---|---|---|
| Add staff or machines | `resources: {packers: {current_cap: 2, max_cap: 4}}` | `app.add_resource("packers", current_cap=2, max_cap=4)` | [Resources](../user-guide/resources.md) |
| Serve by priority | `queue: priority` and a task's `priority` | `add_resource(..., queue="priority")` and `@app.task(priority=)` | [Priorities](../tutorials/priorities.md) |
| Let urgent work take a busy unit | `preempt: true` | `add_resource(..., preempt=True)` | [Interruptions](../tutorials/interruptions.md) |
| Make a resource fail and be repaired | `resources.<name>.breakdown` | `app.add_breakdown(resource, time_to_failure=, repair=)` | [Breakdowns](../guides/breakdowns.md) |
| Hold an amount, such as boxes | `containers: {stock: {current_cap, max_cap, level}}` | `app.add_container("stock", current_cap, max_cap, level=)` | [Stores and Containers](../user-guide/stores-and-containers.md) |
| Hold items, such as parcels | `stores: {parcels: {current_cap, max_cap, items}}` | `app.add_store("parcels", current_cap, max_cap, items=)` | [Stores and Containers](../user-guide/stores-and-containers.md) |

## Tasks

| To | YAML, under `tasks.<name>` | Builder, in `@app.task(...)` | Read |
|---|---|---|---|
| Take a service time while holding a resource | `service`, `resource` | `service_id=`, `resource_id=` | [Tasks and Processes](../user-guide/tasks-and-processes.md) |
| Publish a payload when finished | `payload` | the function's return value | [Tasks and Processes](../user-guide/tasks-and-processes.md) |
| Take from or add to a store or container | `get`, `put` | `get=`, `put=` | [Stock](../tutorials/stock.md) |
| Go on to another task | `next` | `next=` | [Task Flow](../user-guide/task-flow.md) |
| Give up waiting, or refuse to join | `patience`, `balk` | `patience=`, `balk=` | [Patience](../tutorials/patience.md) |
| Wait for a signal, a task, a timeout or a condition | `wait` | `wait=` | [Process Interaction](../tutorials/process-interaction.md) |
| Fire a signal, or fail | `signal`, `fail`, `on_fail` | `signal=`, `fail=`, `on_fail=` | [Tasks and Processes](../user-guide/tasks-and-processes.md#wait-signals-and-child-flows) |
| Interrupt other tasks | `interrupt`, `on_interrupt` | `interrupt=`, `on_interrupt=` | [Interruptions](../tutorials/interruptions.md) |
| Hold several resources | `resource: [picker, bench]`, `keep` | `resource_id=["picker", "bench"]`, `keep=` | [Competing Resources](../tutorials/competing-resources.md) |

## Changes During a Run

| To | YAML | Builder | Read |
|---|---|---|---|
| Change a parameter at a time | `scenario: [{at, path, value}]` | `app.add_step(at, path=, value=)` | [Changing Demand](../tutorials/changing-demand.md) |
| Change it from Kafka, Redis or PostgreSQL | `ingress` | `app.add_ingress(...)` | [Live Parameters](../user-guide/live-parameters.md) |
| Publish a ground-truth label | `scenario: [{at, record}]` | `app.add_step(at, record=)` | [Live Parameters](../user-guide/live-parameters.md#record-what-a-change-was-for) |

## Statistics and Sinks

| To | YAML | Builder | Read |
|---|---|---|---|
| Publish built-in statistics | `stats: {level: basic, interval: 10}` | `app.with_stats("basic", interval=10)` | [Statistics](../user-guide/statistics.md) |
| Send records to a sink | `egress: [{type: Kafka, config: {...}}]` | `app.add_egress(KafkaEgress(...))` | [Egress](../user-guide/egress.md) |
| Send history and live records to different sinks | `egress[].when: history` or `live` | `add_egress(..., when="history")` | [Parquet History, Kafka Live](../examples/backfill-live.md) |

## Run

| To | YAML | Builder | Read |
|---|---|---|---|
| Run to a time | `run: {until: 1 h}` | `app.run(until=3600)` | [Time and Pacing](../user-guide/time-and-pacing.md) |
| Stop at a signal or a condition | `run.stop` | `app.run(until=, stop=)` | [End Conditions](../tutorials/end-conditions.md) |
| Finish the entities already in the model | `run.drain: true` | `app.run(until=, drain=True)` | [End Conditions](../tutorials/end-conditions.md) |
| Repeat a run with other seeds | `desflow run FILE --replications 20` | `replicate(model, 20, seed=, until=)` | [Replications](../user-guide/replications.md) |
| Compare designs | `desflow experiment FILE` | `run_experiment(spec)` | [Experiments](../user-guide/experiments.md) |
