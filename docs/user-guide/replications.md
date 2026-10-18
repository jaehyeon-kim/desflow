# Replications

One run of a model is one sample: another seed draws other times and gives other values. Replications run the same model with many seeds and summarise each built-in statistic with a mean and a confidence interval. This page shows how to run them from the command line and from Python, in each of the three ways to write a model, what they return and send to the sinks, and how the seeds and intervals are made.

---

## What It Is

A replication is one run of the model with a seed of its own. N replications give N independent values of each [built-in statistic](statistics.md), such as the mean wait of a task. Their mean is the estimate, and the **half-width** of its confidence interval says how precise it is: with 95% confidence, the true value lies within the mean plus or minus the half-width. [Trusting Results](../tutorials/trusting-results.md) shows on the shop why one run is not an answer and how to choose the number of replications.

---

## How to Use It

### Run Replications

Give the model, the number of replications, the seed they are spawned from and the end time:

=== "YAML"

    ```bash
    desflow run shop.yaml --replications 20 --measure tasks.pack.wait_mean
    ```

    `--seed` overrides `simulation.random_seed`, and `--until` overrides `run.until`, as for one run. `--level` sets the confidence level (0.95 when it is not given), and `--workers` runs replications at once, each in its own process. The command prints the summary, the seed of each replication and the command that repeats one. The blueprint's own `warmup` applies to every replication:

    ```yaml title="examples/yaml/replications.yaml:seed"
    # The replications' seeds are spawned from this one; --seed overrides it
    random_seed: 42
    warmup: 3 min
    ```

=== "Builder"

    ```python title="docs/snippets/pages/replications.py:builder"
    # shop is a function returning a new SimulationContext, called once per run
    result = replicate(shop, 3, seed=42, until="1 h", warmup="3 min", measures=MEASURES)
    ```

    `shop` is a function with no arguments that returns a new `SimulationContext`, such as [the example's](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/replications_example.py), because each replication starts from a new context. A blueprint path works here too.

=== "Low-level"

    ```python title="docs/snippets/pages/replications.py:low-level"
    def model(env, sampler):
        """Builds the shop in the environment it is given, drawing from sampler."""
        env.registry.register_sim_parameter(PARAMS)
        env.process(orders(env, sampler, DynamicResource(env, "Shop", "packers")))


    def low_level():
        shop = LowLevelModel(model, sim_id="Shop")
        return replicate(shop, 3, seed=42, until=3600, warmup=180, measures=MEASURES)
    ```

    `LowLevelModel` names a function that builds the model in the environment and with the sampler it is given, and the `sim_id` its statistics are published under. The function registers the parameters, builds the resources and starts the processes; it sets up no statistics, because `replicate` does, and no sinks. Its tasks report their steps to `env.stats`, as [Statistics](statistics.md) shows.

`replicate` takes these options:

| Option | What it sets | Default |
|---|---|---|
| `n` | how many replications, 2 or more | |
| `seed` | the seed the replications' seeds are spawned from; None draws one and logs it | |
| `until` | the end of each replication, in seconds or a duration such as `"1 h"`; None uses a blueprint's `run.until` | |
| `warmup` | the end of each replication's warm-up | the model's own |
| `measures` | the statistics to summarise | every statistic the run publishes, at least up to `typical` |
| `level` | the confidence level of the half-widths | 0.95 |
| `workers` | replications run at once, each in its own process | 1 |
| `overrides` | registry paths and the values every replication starts with, such as `{"Shop.resources.packers.current_cap": 3}` | none |

### Choose the Measures

A measure is a statistic's published name without the sim id and `stats`, such as `tasks.pack.wait_mean` for `Shop.stats.tasks.pack.wait_mean`; [Reference, Statistics](../reference/statistics.md) lists them. Each replication keeps the statistics level its measures need, whatever level the model sets: `basic` for counts and time averages, `typical` for means and maxima of waits and services, and `all` for percentiles. A higher level the model sets stays.

A name that no replication publishes is refused, with the names the model does publish. A count that is published only once it is above 0, such as `reneged`, is 0 in a replication without one. A mean with no observation in a replication, such as the wait of a task that never started, is refused with the replication's number rather than left out.

### Read the Result

`replicate` returns a `Replications`. Its `summary` has one row per measure, with `n`, `mean`, `half_width` and `level`; its `table` has one row per replication and measure, with the replication's number and seed; `seeds` lists the seeds in order:

```python title="docs/snippets/pages/replications.py:read"
print(result.summary_text())  # each measure's n, mean and half-width
print(result.seeds)  # replication i ran with seeds[i - 1]
for row in result.table:  # one row per replication and measure
    print(row["replication"], row["seed"], row["measure"], row["value"])
```

The summary of the three replications above:

<!-- records: replications-summary -->
```text
measure                               n          mean    half-width (95%)
tasks.pack.wait_mean                  3          4.16                2.22
resources.packers.utilization_avg     3         78.51               4.363
```

Three replications give a wide interval; [Trusting Results](../tutorials/trusting-results.md#choose-the-precision) shows how to choose enough.

### Repeat One Replication

Each replication's seed is a plain integer, recorded in `seeds` and in every row of the table, so any replication repeats on its own: `desflow run shop.yaml --seed <its seed>`, `random_seed` in a builder context, or `Sampler(seed=...)` in the low-level API, with the same end time and warm-up. That run publishes its event records, which replications do not.

### What the Sinks Receive

When the model has sinks, a blueprint's `egress` or a builder's `add_egress`, they receive the result once, as telemetry stamped at the end time: each replication's value, then each measure's mean, half-width and count. A low-level model has no sinks of its own. These are the records a Console sink receives from the three replications above:

<!-- records: replications-telemetry -->
```text
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack.wait_mean.replication.1', 'value': 3.1279921040674012}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack.wait_mean.replication.2', 'value': 4.692163115004602}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack.wait_mean.replication.3', 'value': 4.6595156392512935}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.utilization_avg.replication.1', 'value': 77.72020813552408}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.utilization_avg.replication.2', 'value': 77.2928455841536}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.utilization_avg.replication.3', 'value': 80.52596995922565}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack.wait_mean.mean', 'value': 4.159890286107766}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack.wait_mean.half_width', 'value': 2.2203200883243843}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack.wait_mean.n', 'value': 3}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.utilization_avg.mean', 'value': 78.51300789296778}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.utilization_avg.half_width', 'value': 4.362949200693404}
[TEL] {'sim_ts': 3600.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.utilization_avg.n', 'value': 3}
```

[Records and Telemetry](../reference/records-and-telemetry.md#telemetry-names) lists the names.

### Example: Replications

The `replications` example runs one hour of the shop 20 times, with seeds spawned from 42 and a 3-minute warm-up, and prints the mean and half-width of every statistic up to `typical`. It needs no Docker service and ends on its own.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get replications --api yaml
        cd replications/yaml

        # 3. Run it
        desflow run replications.yaml --replications 20
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get replications --api yaml
        cd replications/yaml

        # 3. Run it
        desflow run replications.yaml --replications 20
        ```

    ```yaml title="examples/yaml/replications.yaml:seed"
    # The replications' seeds are spawned from this one; --seed overrides it
    random_seed: 42
    warmup: 3 min
    ```

    The whole file is [`examples/yaml/replications.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/replications.yaml).

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get replications --api builder
        cd replications/builder

        # 3. Run it
        uv run --no-project --with desflow replications_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get replications --api builder
        cd replications/builder

        # 3. Run it
        python replications_example.py
        ```

    ```python title="examples/builder/replications_example.py:replicate"
    # 20 seeds spawned from 42; each replication runs unpaced, with no sinks
    result = replicate(shop, 20, seed=42, until="1 h")
    print(result.summary_text())
    # Any replication repeats on its own: run shop() with its seed
    print("replication seeds:", result.seeds)
    ```

    The whole file is [`examples/builder/replications_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/replications_example.py).

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get replications --api low-level
        cd replications/low-level

        # 3. Run it
        uv run --no-project --with desflow replications_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get replications --api low-level
        cd replications/low-level

        # 3. Run it
        python replications_example.py
        ```

    ```python title="examples/low-level/replications_example.py:replicate"
    # 20 seeds spawned from 42; each replication runs unpaced, with no sinks
    shop = LowLevelModel(model, sim_id="Shop")
    result = replicate(shop, 20, seed=42, until=3600, warmup=WARMUP)
    print(result.summary_text())
    # Any replication repeats on its own: call model with Sampler(seed=...)
    print("replication seeds:", result.seeds)
    ```

    The whole file is [`examples/low-level/replications_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/replications_example.py).

---

## How It Works

**Seeds.** The replications' seeds are `numpy.random.SeedSequence(seed).spawn(n)`, each turned into one plain integer. The same seed gives the same list, and a longer list starts with the shorter one, so 40 replications begin with the 10 that `n=10` runs. Within a replication each distribution draws from a random sequence of its own, as in any run ([Randomness and Seeds](../tutorials/randomness-and-seeds.md)).

**One replication.** Each replication builds the model again: it reads the blueprint, calls the factory, or calls the low-level function with a new environment and sampler. It runs unpaced, at `factor` 0, with no ingress, no egress and no statistics timer, applies the warm-up and the `overrides` before the clock starts, and reads `env.stats.readings()` at its end. A blueprint's `run.before` functions run once per replication, and once more when the model has sinks, for the build that sends them the result.

**Intervals.** The half-width is t × s ÷ √n, where s is the sample standard deviation of the n values and t the Student t quantile at (1 + level) ÷ 2 with n − 1 degrees of freedom. desflow computes the quantile itself, from the t distribution's cumulative probability by bisection, so replications need nothing beyond a base install; the tests check it against a published table.

**Workers.** With `workers` above 1, replications run in a pool of that many processes. Each process imports the model, so it must be a blueprint path or a function defined at the top of a module; a lambda or a function defined inside another function is refused with that reason. The values are the same as with one worker.

**Sinks.** When the model has sinks, it is built once more after the last replication, with its sinks but without its ingress and with the clock at the end time, and is not run. Its own statistics are switched off, so the sinks receive only the result. A model without sinks is not built again.

---

## Stability and Limits

- **No event records.** Replications send no event records: twenty streams of orders with different seeds in one sink could not be told apart. Repeat one replication with its seed to get its records.
- **Independent seeds only.** Every replication of one call has its own seed. Comparing two designs with the same seeds, and checking that their random streams line up, is a study's job ([Local Studies](local-studies.md)).
- **Same model each time.** A builder factory must return a new context on every call; a context that has run keeps its entity counters and is not a new replication.
- **Built-in statistics only.** A replication summarises the built-in statistics at its end. Readings of your own, published with `context.publish`, are not summarised.
- **Steady state and terminating runs.** The warm-up applies to each replication. A run with a natural start and end, such as a shop day, keeps its empty start and needs no warm-up.
