# Built-in Statistics

A run can measure itself: how busy each resource was, how long tasks waited, how many entities finished. This page shows how to switch the statistics on, what each level publishes and costs, how each value is computed, and how to add readings of your own.

---

## What It Is

The built-in statistics are measures that desflow keeps while the run goes on and publishes as telemetry, under fixed names such as `Shop.stats.tasks.pack_order.wait_mean`. They need no code of yours in the builder or YAML, and they reach every sink like any other telemetry. They are off by default, because most runs only generate data. [Measuring a Run](../../learn/first-model/measuring-a-run.md) reads them on the shop.

---

## How to Use It

### Choose a Level

A level switches them on. Each level publishes what the levels before it publish, and answers one more question:

| Level | Question | Adds |
|---|---|---|
| `none` | | nothing; the default |
| `minimal` | What is happening now? | resources: `capacity`, `in_use`, `queue_length`, `utilization`, and `down` for one that breaks down; stores and containers: `capacity`, `level`, `getters_waiting`, `putters_waiting`; tasks: `in_progress` |
| `basic` | How much got done, and how busy was it? | tasks and flows: `finished`; flows: `started` and `in_system`; tasks: `balked`, `reneged`, `failed`, `interrupted`, `preempted` and `resumed`, and flows: `failed`, once one happens; resources: `utilization_avg`, `queue_length_avg`, and `downtime` and `failures` for one that breaks down; stores and containers: `level_avg` |
| `typical` | How long did tasks take? | tasks: `wait_mean`, `wait_max`, `service_mean`, `service_max` |
| `all` | How long did the slowest take? | tasks: `wait_p50`, `wait_p95`, `wait_p99`, `service_p50`, `service_p95`, `service_p99` |

They are published every `interval` of simulation time, 10 seconds when it is not given. The first publication comes one interval after the start, and one more comes at the end of the run. When the timer has already published at the end's time, the end publishes only the values that changed since:

=== "YAML"

    ```yaml title="docs/snippets/yaml/statistics.yaml:levels"
    # none (the default), minimal, basic, typical or all, each adding to the one before;
    # published every interval of simulation time and once at the end
    stats: {level: typical, interval: 1 min}
    ```

    `stats: typical` is the short form, with the 10-second interval. The interval takes seconds or a duration.

=== "Builder"

    ```python title="docs/snippets/pages/built_in_statistics.py:builder-levels"
    # none (the default), minimal, basic, typical or all, each adding to the one before;
    # published every interval of simulation seconds and once at the end
    app.with_stats("typical", interval=60)
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/built_in_statistics.py:low-level-levels"
    env.setup_stats("Shop", "typical", interval=60)  # before or after the resources
    ```

    Resources, stores and containers add themselves to `env.stats` when they are built. Tasks are your own processes, so each reports its steps:

    ```python title="docs/snippets/pages/built_in_statistics.py:low-level-report"
    # A low-level task reports its own steps to the statistics
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
    env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")  # the order leaves the shop
    ```

### Read the Names

Every statistic is published as telemetry named `<sim_id>.stats.<kind>.<name>.<statistic>`, where the kind is `resources`, `stores`, `containers`, `tasks` or `flows`. A flow is named by the arrival that started its entities: `Shop.stats.flows.orders.finished` counts the orders that left the model. [Reference, Statistics](../../reference/statistics.md) lists every name with what it measures. Below are the statistics that [`statistics.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/docs/snippets/yaml/statistics.yaml), the blueprint whose `levels` part is shown above, publishes at the end of its two-minute run:

<!-- records: statistics-final -->
```text
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.capacity', 'value': 2}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.in_use', 'value': 2}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.queue_length', 'value': 6}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.utilization', 'value': 100.0}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.utilization_avg', 'value': 90.79208653095155}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.queue_length_avg', 'value': 1.9083531344309013}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.in_progress', 'value': 8}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.finished', 'value': 70}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.wait_mean', 'value': 2.7959715976263215}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.wait_max', 'value': 11.22214686679115}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.service_mean', 'value': 3.0335617388528364}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.service_max', 'value': 6.357944038555676}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.flows.orders.finished', 'value': 70}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.flows.orders.started', 'value': 78}
[TEL] {'sim_ts': 120.0, 'timestamp': '...', 'path_id': 'Shop.stats.flows.orders.in_system', 'value': 8}
```

The same values can be read by name, during or after the run:

=== "YAML"

    `desflow run --stdout` prints the same values as telemetry records, as above.

=== "Builder"

    `app.env.stats.readings()` gives the same values, by name, during or after the run.

=== "Low-level"

    ```python title="docs/snippets/pages/built_in_statistics.py:low-level-read"
    # Every statistic the level publishes, by its published name
    readings = env.stats.readings()
    print(f"mean wait {readings['Shop.stats.tasks.pack_order.wait_mean']:.2f} s")
    ```

### Which Entities a Count Covers

When the statistics are read, every entity that has arrived belongs to one population. Each population has its own counts, published from `basic` unless the table says otherwise. A mean wait or service time covers only the instances that reached that step, so report the counts beside it:

| Population | Which entities | Counts |
|---|---|---|
| completed | left the model after their last task | `flows.<arrival>.finished`; `tasks.<task>.finished` per task |
| still present | started and not yet ended when the statistics are read | `flows.<arrival>.in_system`; `tasks.<task>.in_progress`, the instances queued or started, from `minimal` |
| rejected | refused to join a queue, as their `balk` held | `tasks.<task>.balked` |
| abandoned | left a queue when their `patience` ran out, or ended by an interrupt under `on_interrupt: abandon` | `tasks.<task>.reneged`; an abandoned flow leaves `in_system` without adding to `finished` or `failed`, and `tasks.<task>.interrupted` counts the interrupts |
| failed | ended in a failure that no `on_fail` handled | `flows.<arrival>.failed`; `tasks.<task>.failed` counts failed instances |
| warm-up-crossing | started before the warm-up ended and ended after it | in `finished`, and in the wait and service times with their whole wait, but not in `started` |

An entity a task spawns ([Start a Flow Without Waiting](../tasks-and-processes/signals-and-waiting.md#start-a-flow-without-waiting)) counts as a flow of its own under the spawned task's name. So `flows.<task>.*` and `tasks.<task>.*` cover it as they cover any arrival's flow. The spawning entity's key is in `attr.parent`.

Without a [warm-up](../../learn/concepts/output-analysis.md#one-run-starts-empty), `started` is `finished` plus `in_system` plus the entities that balked, reneged, failed or were abandoned. A warm-up starts `started`, `finished`, `failed` and the outcome counts again from zero, and leaves `in_system` and `in_progress` as they are. After a warm-up, `finished` also counts the warm-up-crossing entities, which `started` leaves out, so `finished` can be larger than `started`. [End Conditions](../../learn/first-model/end-conditions.md) counts each population of a run.

### Add Readings of Your Own

Anything else is a custom reading: a function called at an interval of simulation time, which publishes with `context.publish(name, value)` as `<sim_id>.<name>`. It can read resources, stores, containers or any registry value.

=== "YAML"

    ```yaml title="docs/snippets/yaml/statistics.yaml:custom"
    telemetry:
      # Readings of your own: called as packers_report(context) every 30 seconds
      - interval: 30
        function: !python shop_logic.packers_report
    ```

    The function is in `shop_logic.py`, beside the blueprint:

    ```python title="docs/snippets/yaml/shop_logic.py:packers-report"
    def packers_report(context):
        """A custom reading: orders waiting per packer, and the live arrival rate."""
        packers = context.get_resource("packers")
        waiting = len(packers.queue)
        context.publish("waiting_per_packer", waiting / max(packers.capacity, 1))
        rate = context.env.registry.get(f"{context.sim_id}.arrival.orders.rate").value
        context.publish("arrival_rate", rate)  # as Shop.arrival_rate
    ```

=== "Builder"

    ```python title="docs/snippets/pages/built_in_statistics.py:builder-custom"
    @app.telemetry_loop(interval=30.0)
    def packers_report(context: SimulationContext):
        """Readings of your own: orders waiting per packer, and the live arrival rate."""
        packers = context.get_resource("packers")
        waiting = len(packers.queue)
        context.publish("waiting_per_packer", waiting / max(packers.capacity, 1))
        rate = context.env.registry.get("Shop.arrival.orders.rate").value
        context.publish("arrival_rate", rate)  # as Shop.arrival_rate
    ```

=== "Low-level"

    A process that loops, publishes with `env.publish_telemetry(name, value)` and waits with `env.timeout(interval)` does the same.

---

## How It Works

The environment keeps one `Statistics` object, `env.stats`, which collects nothing until a level above `none` is set. Its timer publishes every value at each interval, and the teardown publishes them once more. When the timer has already published at the teardown's time, the teardown publishes only the values that changed since.

**Readings now.** `capacity`, `in_use`, `queue_length` and `level` are read from the object at the moment of publishing. `in_use` counts the units held by tasks, and a capacity drop does not take busy units away. After a drop, `in_use` can therefore be above `capacity`, and `utilization` (`in_use ÷ capacity × 100`) can be above 100%. A resource with no limit (`current_cap: .inf`) publishes `capacity` as `null` and a `utilization` of 0. A store or container with no limit publishes its `capacity` as `null` too.

**Breakdowns.** A resource with a `breakdown` reads `down` now: 1 while it is down, 0 otherwise. `downtime` adds the length of each down period, and of one still going on. `failures` adds one at each failure. [Model Breakdowns](../../how-to/modelling/breakdowns.md) checks both against the events.

**Averages over time.** `utilization_avg`, `queue_length_avg` and `level_avg` weigh each value by how long it lasted. Each resource, store and container reports every change, and the statistics add the area under the value since the last change, so the average is exact at any moment rather than sampled:

![Busy packers over 10 seconds: 1 for 2 seconds, 3 for 4 seconds, 2 for 4 seconds; the areas 2, 12 and 8 add up to 22, so the time-weighted average is 2.2 busy packers, while a reading at 10 seconds sees 2](../../assets/diagrams/stats-time-weighted.png)

**Counts.** `finished` adds one when a task instance finishes, and a flow's `finished` adds one when an entity's last task has no next task. A flow's `started` adds one when an entity's first task instance begins, before its balk check and its queue, whether or not it is then served. An entity that the `spawn` key or an `on:` spawn action starts is counted at the spawn when the spawn has no `after`. With an `after`, it is counted when the spawned task starts, after the delay. `in_system` is the number of the flow's entities in the model now: started, and not yet finished, failed or gone. [Which Entities a Count Covers](#which-entities-a-count-covers) shows how `started` adds up. A task's `in_progress` is the number of its instances queued or started and not yet ended, now. A low-level process reports an entity's start with `env.stats.flow_started(arrival)`. It reports an entity that leaves without finishing or failing with `env.stats.flow_ended(arrival)`. A task's `balked` adds one when an instance refuses to join its queue, `reneged` when one leaves the queue after its patience, and `failed` when one fails ([Failures](../tasks-and-processes/failures.md#failures)). A flow's `failed` adds one when an entity's flow ends in a failure no `on_fail` handles. `interrupted` and `preempted` add one for each interrupt or preemption an instance receives, and `resumed` when it is served again. Each is published once it is above 0. A balked, reneged or failed instance adds no wait or service time. An interrupted instance that resumes adds one wait, the sum of all its waits, and one service time, the time it was served. A low-level process counts them with `env.stats.task_balked(task, instance)`, `env.stats.task_reneged(task, instance)`, `env.stats.task_failed(task, instance)`, `env.stats.flow_failed(arrival)`, `env.stats.task_interrupted(task, instance, preempted=...)` and `env.stats.task_resumed(task, instance)`.

**Wait and service.** The wait of a task instance runs from its `queued` step to its `started` step, and its service from `started` to finished. A task that never queues, such as a timed step, waits 0. `typical` keeps only a count, a total and a maximum of each, so the memory they use does not grow with the run. `all` keeps every observation and computes the percentiles exactly with NumPy's `percentile` at each publication, so its memory grows with the number of tasks.

**Warm-up.** A run's `warmup` resets every statistic once, at that time: the averages start again from zero area, and the counts and observations from zero. An instance that was queued before the reset and starts after it counts with its whole wait. [Time and Pacing](../time-and-pacing/index.md) describes the warm-up.

---

## Stability and Limits

- **Cost.** `minimal` reads a few values at each publication. `basic` adds a little work at every change of a resource, store or container. `typical` adds a little at every task step. `all` keeps every wait and service time in memory for the whole run. Use it for runs of thousands of tasks, not for a generator that runs for days.
- **One way to get resource readings.** Before 0.21.0, a `telemetry` entry could publish `<resource>.<reading>` by name. That shortcut is gone: `stats: minimal` publishes the same readings, under `Shop.stats.resources.<name>.<reading>`. `telemetry` now takes only a function, for readings of your own.
- **One run is one sample.** The statistics describe one run with one seed. Values from the same run are not independent of each other, and another seed gives other values. [Replications](../../learn/concepts/output-analysis.md#replications) give independent values. [Trusting Results](../../learn/studies/trusting-results.md) shows what that means for a result, and [Replications](replications.md) run a model with many seeds.
- **A task appears once it has run.** At `typical` and `all`, a task's statistics are published from the first time it starts. At `basic`, they are published from the first time it finishes.

---

## Example: Measuring a Run

The `measuring` example publishes every statistic of the packers and the pack task every 5 minutes of simulation time, and leaves out a 3-minute warm-up. When packing slows down, it sends a ground-truth record to a sink of its own. It needs no Docker service and ends on its own.

The tutorial page [Measuring a Run](../../learn/first-model/measuring-a-run.md) teaches these features step by step.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get measuring --api yaml
        cd measuring/yaml

        # 3. Run it
        desflow run measuring.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get measuring --api yaml
        cd measuring/yaml

        # 3. Run it
        desflow run measuring.yaml
        ```

    ```yaml title="examples/yaml/measuring.yaml:measure"
    # Every level: readings, counts and time averages, mean and maximum wait and service,
    # and their 50th, 95th and 99th percentiles, such as Shop.stats.tasks.pack.wait_p95.
    stats: {level: all, interval: 5 min}

    scenario:
      # Packing slows from 4 to 4.8 seconds on average. The record is the ground truth.
      - at: 12 min
        path: Shop.service.packing.scale
        value: 1.2
        record: {fault: slow_packing, cause: new packing material}
    ```

    The whole file is [`examples/yaml/measuring.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/measuring.yaml).

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get measuring --api builder
        cd measuring/builder

        # 3. Run it
        uv run --no-project --with desflow measuring_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get measuring --api builder
        cd measuring/builder

        # 3. Run it
        python measuring_example.py
        ```

    ```python title="examples/builder/measuring_example.py:fault"
    def slow_packing(context):
        """Packing slows from 4 to 4.8 seconds on average at 12 minutes, with its label."""
        yield context.env.timeout(720)
        path = "Shop.service.packing.scale"
        registry = context.env.registry
        old = registry.get(path).value
        registry.update(path, 1.2)
        context.env.publish_label(
            {"fault": "slow_packing", "cause": "new packing material"},
            path=path,
            old=old,
            new=registry.get(path).value,
        )


    app.add_process(slow_packing)
    ```

    The whole file is [`examples/builder/measuring_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/measuring_example.py).

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get measuring --api low-level
        cd measuring/low-level

        # 3. Run it
        uv run --no-project --with desflow measuring_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get measuring --api low-level
        cd measuring/low-level

        # 3. Run it
        python measuring_example.py
        ```

    ```python title="examples/low-level/measuring_example.py:measure"
    # Every level, published every 5 minutes, such as Shop.stats.tasks.pack.wait_p95
    env.setup_stats("Shop", "all", interval=300)
    # The second sink receives the ground-truth records, and nothing else
    env.setup_egress(
        [ConsoleEgress(), ConsoleEgress()], labels=[False, True], warmup=WARMUP
    )
    ```

    The whole file is [`examples/low-level/measuring_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/measuring_example.py).
