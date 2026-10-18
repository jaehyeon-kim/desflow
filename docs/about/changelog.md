# Changelog

Each release, newest first: what changed, then under Upgrading what to change in code, commands or installs that use the previous release.

## Version 0.22.0

desflow matches SimPy operation by operation, in YAML, the builder and the low-level API, and each operation is checked against plain SimPy.

- A [SimPy Compatibility](../reference/simpy-compatibility.md) page lists every supported SimPy operation with its form in each API and the tests that compare it with SimPy.
- Patience and balking: a task's `patience` takes a duration such as `30 s`, and orders that leave or refuse to join are counted as `tasks.<task>.reneged` and `.balked`. See [Patience](../tutorials/patience.md) and [Reneging and Balking](../guides/reneging-and-balking.md).
- A store can start with items: `stores.<name>.items`, `add_store(..., items=[...])` and `DynamicStore(..., items=[...])`; a priority store takes `[priority, item]` pairs. `initial_time` takes a duration such as `8 h`. See [Stores and Containers](../user-guide/stores-and-containers.md#start-with-items-and-serve-by-priority) and [Time and Pacing](../user-guide/time-and-pacing.md#initial-time).
- A store `get` can take a `filter`, such as `item.area == attr.area`, and `patience` covers the wait for a `get` as well as for a resource; `item_field` adds the item a YAML get took to the payload. See [Stores and Containers](../user-guide/stores-and-containers.md#pick-an-item-or-give-up-waiting) and [Filtered Inventory](../tutorials/filtered-inventory.md).
- Tasks interact as SimPy processes do: `signals` that tasks fire and wait for, a `wait` for one or all of signals, child flows, timeouts and conditions, and `fail`, `fail_reason` and `on_fail` for a task that fails. `context.spawn` returns the process it starts, and `context.call` starts a child flow. See [Process Interaction](../tutorials/process-interaction.md), [Tasks and Processes](../user-guide/tasks-and-processes.md#wait-signals-and-child-flows) and [Wait for One or All](../guides/wait-for-one-or-all.md).
- Interrupts and preemption: an `interrupt` on a task or scenario step, or `context.interrupt`, interrupts running task instances; `on_interrupt: resume`, `restart` or `abandon` says what each does; `preempt: true` on a priority resource lets a lower priority value take a busy unit. See [Interruptions](../tutorials/interruptions.md) and [Resources](../user-guide/resources.md#preemption).
- A task can hold several resources, `resource: [picker, bench]`, acquired in that order, and hand a unit to the next task with `keep`; a model that acquires resources in opposite orders is refused. See [Competing Resources](../tutorials/competing-resources.md) and [Resources](../user-guide/resources.md#several-resources).
- End conditions: `run.stop` ends a run at a signal or a condition, and `run.drain` (or `desflow run --drain`) stops the arrivals at `until` and finishes the entities already in the model; `start()`, `step()`, `peek()` and `close()` step a builder or YAML run one event at a time. The statistics count `flows.<arrival>.started` and `in_system`, and `tasks.<task>.in_progress`. See [End Conditions](../tutorials/end-conditions.md) and [Time and Pacing](../user-guide/time-and-pacing.md#stop-early).
- A low-level request can be cancelled with `request.cancel()` and given back with `resource.release(request)`, as in SimPy. See [Resources](../user-guide/resources.md#how-it-works).
- Breakdowns: `resources.<name>.breakdown`, `add_breakdown` and `run_breakdowns` make a resource fail at random and be repaired, interrupting the work it was doing or letting it finish; each failure and repair publishes `down` and `repaired`, and the statistics count `resources.<name>.down`, `downtime` and `failures`. The Preemptive Breakdowns guide becomes [Breakdowns](../guides/breakdowns.md). See also [Resources](../user-guide/resources.md#breakdowns).
- Schedules in every API: the builder's `add_scenario` and `add_step`, and the low-level `run_scenario`, take the same steps as a blueprint's `scenario`. See [Time-varying Arrivals](../guides/time-varying-arrivals.md) and [Changing Demand](../tutorials/changing-demand.md).
- Replay: an arrival's `replay`, `add_arrival_replay` and the low-level `read_trace` take arrivals from the rows of a CSV or Parquet file, or a list of dicts, at their recorded times, with the row's fields as attributes. See [Local Replay](../tutorials/local-replay.md) and [Arrivals and Services](../user-guide/arrivals-and-services.md#replay).
- Faster runs at `factor: 0`: SimPy's own step processes each event, a record's timestamp is formatted once per second of the clock, and `ConsoleEgress` logs a batch with one call. A low-level workload runs 1.14 times faster than in 0.21.0, and printing every record costs about a seventh of what it did. See [Performance Tuning](../guides/performance-tuning.md), whose measurements `benchmarks/throughput.py` repeats.
- `simulation.lag_interval` and `SimulationContext(lag_interval=)` set how often `system.simulation.lag_seconds` is published. See [Time and Pacing](../user-guide/time-and-pacing.md#lag).
- The record contract: [Records and Telemetry](../reference/records-and-telemetry.md) gives every field its type, and a docs test fails when the page and the payload models differ. `simulation.timezone`, `SimulationContext(timezone=)` and `DynamicRealtimeEnvironment(timezone=)` name the zone timestamps are written in, and [Statistics](../user-guide/statistics.md#which-entities-a-count-covers) defines which entities each count covers.
- Replications: `desflow run FILE --replications N` and `desflow.replicate` run a model with seeds spawned from one seed and give each statistic's mean and 95% half-width; the model's sinks receive the result once, as `<sim_id>.stats.<measure>.replication.<i>`, `.mean`, `.half_width` and `.n`. See [Replications](../user-guide/replications.md) and [Trusting Results](../tutorials/trusting-results.md).
- Local studies: `desflow study` and `desflow.run_study` compare designs with paired replications and sweeps, apply checks that fail the command, compare with observed values, and write CSV and JSON results and a `report.md`; `desflow report` builds the report again. See [Local Studies](../user-guide/local-studies.md), [Comparing Designs](../tutorials/comparing-designs.md) and [Parameter Sweeps](../tutorials/parameter-sweeps.md).
- Input fitting: `desflow fit` and `desflow.fit_column` fit a column of measured times to every distribution family, with diagnostics and a plot, and write settings a blueprint loads with the new `!include` tag. See [Input Fitting](../user-guide/input-fitting.md), [Fit the Inputs](../tutorials/fit-the-inputs.md) and [Fit a Distribution](../guides/fit-a-distribution.md).

### Upgrading

- desflow needs SimPy 4.1.0 or later, where it accepted 4.0.0: SimPy 4.0 imports `pkg_resources`, which a new Python environment does not have. It is tested against SimPy 4.1.2, the newest release, and against 4.1.0.
- A `priority` on a store `get` is refused; priority orders a `put` into a priority store.
- Resources grant units with the same events as SimPy's `Resource`, so requests at the same simulation time are granted in SimPy's order, which can change seeded output where such ties occur.
- `patience` now covers a task's `get` as well as its resource, so a task with both can renege while it waits for stock. A task with `patience` needs a resource or a `get`.
- A store without priority is now a SimPy `FilterStore`, so a `get` can take a filter. While no waiting `get` has a filter, it grants gets as `simpy.Store` does, one waiting `get` each time it is triggered, as before; while a filtered `get` waits, it looks at every waiting `get` each time, as `FilterStore` does, so gets at the same instant can be granted in another order.
- A builder task whose function raises, or a YAML task whose `!python` payload function raises, now fails the task, publishing `failed` with the error as its reason and ending the entity's flow, instead of stopping the run.
- `DynamicResource.pool` and `DynamicResourceRequest.cancelled` are gone: the resource grants units as SimPy's `Resource` does. Call `request.cancel()` to leave the queue; `resource.queue.items` holds `(rank, request)` pairs, where it held `PriorityItem`s.
- The built-in statistics publish new names: `tasks.<task>.in_progress` from `minimal`, and `flows.<arrival>.started` and `in_system` from `basic`. A low-level process that calls `env.stats.task_queued`, `task_started` and `task_finished` publishes `in_progress` too, and one that calls `flow_started` publishes the flow counts.
- Events with the statuses `balked`, `reneged`, `failed`, `interrupted`, `preempted` and `resumed` reach the sinks, so a consumer that filters on `status` should expect them.
- A scenario step now applies before every other event at its time, as SimPy applies its own `until`, so an arrival at the step's time draws its next gap from the new value; seeded runs whose steps fall on the time of another event can give other numbers.
- At `factor: 0`, `system.simulation.lag_seconds` is no longer published unless `lag_interval` (low-level `lag_monitor_interval`) is set; a run that goes live publishes it from the go-live instant. At a `factor` above 0 it is published every simulation second, as before.
- `ConsoleEgress` logs each batch with one call, one line per record, so the logging format's prefix starts each batch rather than each line. Code that reads the console log record by record splits each message into lines.
- At `factor: 0`, `strict=True` is no longer checked, as there is no wall clock to fall behind.
- Every `timestamp` now carries a time zone offset, such as `2026-10-03T12:00:00.000+10:00`. By default timestamps are in UTC, `+00:00`, where they were in the machine's local time with no offset; set `timezone` to an IANA name to write them in a zone. A `logical_start_time` or `go_live_at` without an offset, `now` and the default start are read in the zone, so a start and a go-live, one with an offset and one without, no longer fail. The clock runs in UTC, so a daylight-saving change no longer adds or drops simulation time. A consumer that parses `timestamp` as a time without a zone reads the offset now.
- A payload key that starts and ends with two underscores, such as `__table__`, is reserved: Kafka, Parquet, JSON Lines, Iceberg and the console drop it, with a warning, where they wrote it as data. Rename such a key, such as `__source__` to `source`, to keep it. PostgreSQL and Redis still route by `__table__` and `__stream__`.
- Fitting needs the new `fit` extra, which installs scipy and matplotlib: `pip install "desflow[fit]"`.

## Version 0.21.0

The simulation model grows: more distributions, stores and containers that tasks use, flows of tasks, built-in statistics and ground-truth labels. The docs gain a Learn section with a tutorial, the Shop, and a User Guide with one page per part of the library.

- Five new distributions, `uniform`, `triangular`, `gamma`, `weibull` and `empirical`, and, in YAML, durations such as `5 min` for every time-valued parameter. The builder's distribution parameters take numbers in seconds, and its `warmup` and `run(until=)` also take durations; the low-level API takes numbers in seconds. See [Arrivals and Services](../user-guide/arrivals-and-services.md) and [Distributions](../reference/distributions.md).
- Each distribution draws from a random stream of its own, so adding an arrival leaves the other draws unchanged. A run without a seed draws one and logs it, and `desflow run --seed N` repeats it. See [Randomness and Seeds](../tutorials/randomness-and-seeds.md).
- Stores and containers that tasks take from and add to, with `get` and `put`; a container's `level` is a live path. See [Stores and Containers](../user-guide/stores-and-containers.md).
- A queue order per resource, `fifo`, `lifo` or `priority`, and a `priority` per task. See [Resources](../user-guide/resources.md).
- Flows of tasks: `next` by name, condition or weights, entity attributes with `attributes` and `set`, and timed steps with a service and no resource. A builder arrival loop takes a task's instance id from `context.next_instance(task)`, the counter `next` uses too. See [Task Flow](../user-guide/task-flow.md).
- Built-in statistics at five levels, `stats: none` to `stats: all`, and a `warmup` that leaves the start of a run out. See [Statistics](../user-guide/statistics.md).
- Ground-truth labels: a scenario step's `record` publishes what it changed to `when: labels` sinks only. See [Live Parameters](../user-guide/live-parameters.md#record-what-a-change-was-for).
- Every ingress connector reads `path` and `value`, and a bad message is logged and skipped. See [Ingress](../user-guide/ingress.md).

### Upgrading

- Python 3.11 or later is needed.
- Ingress messages use `path` and `value`: Kafka's `path_id` and Redis's `param_path` and `param_value` are renamed, and a Kafka message, or a PostgreSQL ingress row, with a null `value` is now refused. Call `send_config(topic, path=..., value=...)` where it was called with `path_id=`.
- For each table the PostgreSQL ingress reads, `simulation_params` by default, rename the columns: `ALTER TABLE simulation_params RENAME COLUMN param_path TO path;` and `ALTER TABLE simulation_params RENAME COLUMN param_value TO value;`
- An event's key is now the entity's key, the arrival name and a counter from 1 (`orders-1`), not `task-<task id>`, and every event has two new fields after `key`: `task` and `instance`. PostgreSQL rows gain `task` and `instance` columns where the table has them. See [Records and Telemetry](../reference/records-and-telemetry.md).
- `telemetry` entries in a blueprint no longer take `publish`. Use `stats: minimal`, which publishes the same readings as `Shop.stats.resources.<name>.<reading>`, and `function` for readings of your own.
- Seeded runs give other numbers than before, because each distribution has its own random stream, and requests of equal priority are now served first in, first out.
- A distribution parameter the family does not take, such as `rate` on a `normal`, is now refused at build. The builder's distribution parameters are keyword-only: write `add_service("packing", "normal", mean=3)`.
- Under the default `fifo` queue a request's `priority` is ignored. Add the resource with `queue="priority"` (YAML `queue: priority`; low-level `DynamicResource(env, sim_id, name, queue="priority")`) to rank requests; a task `priority` on another order is refused.
- `context.wait_for_arrival` now yields the new `Entity`. Pass it to the task as `entity=`, or the task starts an entity of its own keyed by the task's name.
- A container's `current_cap` is its capacity, its largest amount; a model that used it as a level moves to `level`. `DynamicContainer(init=None)` now starts at the registry's level, 0 unless the `ContainerConfig` gives one.

## Version 0.20.0

The package dynamic-des is renamed desflow. The classes, the YAML format, the records and the extras are the same as in 0.19.0.

### Upgrading

- Install `desflow` instead of `dynamic-des`, with the same extras: `pip install desflow`, `uv add desflow`, or for example `pip install "desflow[kafka]"`. The last dynamic-des release, 0.20.0, installs desflow with the same extras. To install desflow beside dynamic-des 0.19.0 instead, uninstall dynamic-des first: with pip, uninstalling it afterwards also removes `dynamic_des` and `ddes`, and `pip install --force-reinstall --no-deps desflow` puts them back.
- Import `desflow` instead of `dynamic_des`, for example `from desflow import SimulationContext`.
- Run the `desflow` command instead of `ddes`, for example `desflow run shop.yaml` or `desflow examples`.
- `import dynamic_des` still works in this release. It shows a `FutureWarning` and gives the desflow modules, submodules such as `dynamic_des.connectors.egress.kafka` included. The `ddes` command still works too: it prints a warning and runs `desflow`. Both are removed in a later release, which this page will name.
- The docs are at [jaehyeon.me/desflow](https://jaehyeon.me/desflow/), and the repository is [jaehyeon-kim/desflow](https://github.com/jaehyeon-kim/desflow) on GitHub.
