# Arrivals and Services: `Sampler` and the Distributions

Two building blocks set the timing of every simulation. An **arrival** decides how often a new entity enters the model: an order, a visitor, a support call. A **service** decides how long a piece of work takes: packing an order, answering a call. Both are distributions of time, and each time one is needed, a value is drawn from the distribution with its current parameters.

---

## What It Is

| Block | Answers | Used by | Example |
|---|---|---|---|
| Arrival | How long until the next entity? | the arrival loop, which starts a task for each one | an order every 2 s on average |
| Service | How long does this piece of work take? | a task, while it holds its resource | 3 s of packing, give or take 0.5 s |

An arrival draws one gap after another, so entities arrive at uneven times. Look at the first five orders of the shop in [First Simulation](../tutorials/first-simulation.md):

![The first five orders arrive at 2.552, 2.696, 3.737, 4.020 and 6.196 seconds; each gap, from 0.14 to 2.55 seconds, is a new draw from an exponential distribution with rate 0.5](../assets/diagrams/arrival-timeline.png)

---

## How to Use It

Every distribution is a family, `dist`, and the parameters that family takes. There are ten families. A parameter outside its range, a missing one, or one the family does not take is refused when the model is built, with the YAML line when it comes from a blueprint. [Reference, Distributions](../reference/distributions.md) lists every family with its parameters, rules, mean and variance; the table names each family, the list under it gives each family's rules, and one example shows the form.

| Family | Use it for | Parameters |
|---|---|---|
| [`exponential`](../reference/distributions.md#exponential) | Gaps between arrivals that happen independently of each other: orders, calls, visits. Many short gaps and a few long ones, with mean `1 / rate`. | `rate` |
| [`normal`](../reference/distributions.md#normal) | Work with a typical time and a small, symmetric spread. | `mean`, `std` |
| [`truncated_normal`](../reference/distributions.md#truncated_normal) | Work with a typical time and a hard minimum. | `mean`, `std`, `low` |
| [`lognormal`](../reference/distributions.md#lognormal) | Times that are never negative and now and then much longer: repairs, support calls. | `mean`, `std` |
| [`constant`](../reference/distributions.md#constant) | Fixed times: a conveyor, a set-up, a step that takes no time. | `value` |
| [`uniform`](../reference/distributions.md#uniform) | Any time in a range, all equally likely, when only the range is known. | `low`, `high` |
| [`triangular`](../reference/distributions.md#triangular) | An expert's estimate: the shortest, the most likely and the longest time. | `low`, `mode`, `high` |
| [`gamma`](../reference/distributions.md#gamma) | Times less variable than exponential. A whole-number shape is the Erlang distribution: that many exponential steps in a row. | `shape`, `scale` |
| [`weibull`](../reference/distributions.md#weibull) | Time to failure: a shape below 1 fails early, a shape above 1 wears out. | `shape`, `scale` |
| [`empirical`](../reference/distributions.md#empirical) | Times taken from real data, when no family fits. | `values` with `weights`, or `edges` with `counts` |

The rules for each family's parameters:

- `exponential`: `rate` above 0, in events per second: 0.5 is one every 2 seconds on average, 2.0 is two a second. It is the one parameter that is not a time, so it takes no duration.
- `normal`: `mean`, and `std` 0 or more (0 when left out). A `std` of 0 gives the mean every time. A draw can be negative, and every time is raised to at least 0.00001 seconds, which raises the mean of a wide normal; use `truncated_normal` for that.
- `truncated_normal`: `mean`, `std` 0 or more, and `low`, 0 when left out; with `std` 0, `mean` must be at least `low`. A draw below `low` is thrown away and drawn again, so no draws pile up at the limit, and the mean is the truncated distribution's own: 1.29 seconds for `mean: 1, std: 1, low: 0`. When `low` is more than half a standard deviation above the mean, where plain draws are kept less and less often, it uses an exponential proposal (Robert, 1995), so it stays exact and fast for any `low`.
- `lognormal`: `mean` above 0 and `std` 0 or more (0 when left out). Both describe the times you get, not the underlying normal; desflow converts them.
- `constant`: `value` 0 or more. It uses no random number at all.
- `uniform`: `low` and `high`, with `low < high`. The mean is halfway between them.
- `triangular`: `low`, `mode` and `high`, all required, with `low <= mode <= high` and `low < high`. The mean is (low + mode + high) ÷ 3.
- `gamma`: `shape` and `scale`, both above 0. The mean is shape × scale and the variance shape × scale². From a mean and a standard deviation: shape = (mean ÷ std)² and scale = std² ÷ mean, so a mean of 1.5 seconds with a std of 0.5 is shape 9 and scale 0.1667.
- `weibull`: `shape` and `scale`, both above 0. The mean is scale × gamma(1 + 1 ÷ shape), where gamma is the gamma function.
- `empirical`: either `values`, each equally likely, with optional `weights`, one per value; or a histogram of `edges` and `counts`, one count per bin, where a draw picks a bin by its count and a time inside it uniformly. Weights and counts are 0 or more and add up to more than 0, and edges increase. The lists are taken when the model is loaded, so an `empirical` distribution has no registry path and does not change while the run goes on.

An arrival or a service takes the same form for every family, `dist` and the parameters of its row. An exponential arrival, in the three ways to write it:

=== "YAML"

    ```yaml title="docs/snippets/yaml/arrivals_services.yaml:exponential"
    # under arrivals: an order every 2 seconds on average
    orders: {dist: exponential, rate: 0.5, spawn: pack}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/arrivals_services.py:builder-exponential"
    # An order every 2 s on average, each starting the task pack
    app.add_arrival("orders", dist="exponential", rate=0.5, spawn="pack")
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/arrivals_services.py:low-level-exponential"
    gap = DistributionConfig(dist="exponential", rate=0.5)  # an order every 2 s on average
    ```

### Durations

In a blueprint, every time-valued parameter takes a duration as well as a number of seconds: `mean`, `std`, `low`, `mode`, `high`, `scale` and `value`, and the `values` and `edges` of an `empirical`. A plain number is seconds. `rate` stays a number per second.

```yaml title="docs/snippets/yaml/arrivals_services.yaml:durations"
# under services: every time-valued parameter takes a duration
support_call: {dist: lognormal, mean: 5 min, std: 3 min}
```

The builder and the low-level API take seconds.

### Seeding

A seed makes every draw the same on every run:

=== "YAML"

    ```yaml title="docs/snippets/yaml/arrivals_services.yaml:seed"
    # The same draws on every run; leave it out to draw a seed and log it
    random_seed: 42
    ```

    `desflow run arrivals_services.yaml --seed 7` overrides it without editing the file.

=== "Builder"

    ```python title="docs/snippets/pages/arrivals_services.py:builder-seed"
    # The same draws on every run; leave it out to draw a seed and log it
    app = SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/arrivals_services.py:low-level-seed"
    sampler = Sampler(seed=42)  # the same draws on every run
    ```

    In the low-level API you draw yourself, from the current values:

    ```python title="docs/snippets/pages/arrivals_services.py:low-level-draw"
    seconds = sampler.sample(packing)  # one draw, in seconds, from the current values
    ```

A run without a seed draws one and logs it, so it can be repeated: `desflow run` logs the command with `--seed`, and a builder run logs the value to pass as `random_seed`:

<!-- records: seed-line-builder -->
```text
No random_seed was given, so this run uses 3166812249. Pass random_seed=3166812249 to repeat it.
```

A low-level `Sampler()` with no seed draws nothing at random, as [How It Works](#how-it-works) explains. [Randomness and Seeds](../tutorials/randomness-and-seeds.md) shows seeds in use.

### Replay

An arrival can take its times from recorded rows instead of a distribution: each row of a CSV or Parquet file, or of a list of dicts, becomes one entity at the row's time, and the fields named in `fields` become its attributes. Here each row of `local_replay_orders.csv` is an order, at its time in seconds from the start of the run:

=== "YAML"

    ```yaml title="examples/yaml/local_replay.yaml:replay"
    arrivals:
      orders:
        replay:
          # A CSV or Parquet file, relative to this blueprint
          path: ../local_replay_orders.csv
          time: time
          unit: s
          fields: [order_id, items]
        spawn: pack
    ```

=== "Builder"

    ```python title="examples/builder/local_replay_example.py:replay"
    # A CSV or Parquet file, or a list of dicts
    .add_arrival_replay(
        "orders", TRACE, time="time", unit="s", fields=["order_id", "items"]
    )
    ```

    `add_arrival_replay` takes `spawn=` as `add_arrival` does, which starts the named task for each row. This example writes an `@app.arrival_loop` of its own instead, where `wait_for_arrival("orders")` waits for the next row.

=== "Low-level"

    ```python title="examples/low-level/local_replay_example.py:replay"
    def replay(env, sampler, packers):
        """Starts one order at each row's time, carrying the row's fields."""
        rows = read_trace(TRACE, time="time", unit="s", fields=["order_id", "items"])
        for number, row in enumerate(rows, start=1):
            yield env.timeout(row.time - env.now)
            key = f"orders-{number}"
            env.process(pack(env, sampler, packers, key, number - 1, row.fields))
    ```

    `read_trace` reads and checks the rows, and returns each with its simulation `time`, row `number` and `fields`.

`unit` is `ms`, `s`, `min` or `h` from the start of the run, or `timestamp` for a date and time read from `logical_start_time`. The rows must be in time order; rows at equal times arrive at that time in the order of the file; a row before the start stops the run; rows at or after `until` never arrive; after the last row the arrival stops, with one INFO line. A row without a valid time stops the run, unless `on_invalid` is `skip`, which leaves it out with a warning. A Parquet file needs the `parquet` extra. A replayed arrival has no registry path. [Local Replay](../tutorials/local-replay.md) compares a recorded morning with a model of it.

### Example: Local Replay

The `local-replay` example: one packer and the 40 orders of a recorded morning, replayed from a CSV file at their times. The last order arrives at 73 seconds. It needs no Docker service and ends on its own after 120 simulation seconds.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get local-replay --api yaml
        cd local-replay/yaml

        # 3. Run it
        desflow run local_replay.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get local-replay --api yaml
        cd local-replay/yaml

        # 3. Run it
        desflow run local_replay.yaml
        ```

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get local-replay --api builder
        cd local-replay/builder

        # 3. Run it
        uv run --no-project --with desflow local_replay_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get local-replay --api builder
        cd local-replay/builder

        # 3. Run it
        python local_replay_example.py
        ```

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get local-replay --api low-level
        cd local-replay/low-level

        # 3. Run it
        uv run --no-project --with desflow local_replay_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get local-replay --api low-level
        cd local-replay/low-level

        # 3. Run it
        python local_replay_example.py
        ```

---

## How It Works

**One registry of distributions.** Every family is declared once, in `desflow.core.distributions.DISTRIBUTIONS`, with its parameters, its range rules, how to draw from it, its mean and variance, and what it gives with no random generator. The YAML models, the builder, `DistributionConfig`, the `Sampler` and the registry's live paths all read it, so they accept the same families and parameters. These are 200,000 draws from each family:

![Histograms of 200,000 draws from each of the ten families, with the mean marked: exponential falls away from 0; normal with mean 1 and std 1 has a pile at 0 where negative draws are raised; truncated normal has none; lognormal has a long right tail; constant is one value; uniform is flat between 1 and 2; triangular rises to 1.25 and falls to 2.25; gamma and weibull are skewed to the right; the empirical histogram has three flat bins](../assets/charts/distribution-shapes.png)

**A draw reads the live configuration.** A `DistributionConfig` holds the family and its parameters. `Sampler.sample(config)` reads the parameters at the moment it is called, so a change made while the run goes on applies from the next draw. A task draws its service time once it has its resource and has published `started`; the arrival loop draws the next gap when it starts waiting. Every value is raised to at least 0.00001 seconds, so a time is never 0 or negative.

**One random stream per distribution.** A seeded `Sampler` makes one generator for each name: NumPy's `SeedSequence(seed, spawn_key=(CRC32 of the name,))`. When the registry registers a distribution, it names it by its path without the sim id, such as `arrival.orders` or `service.packing`, and every draw from that distribution comes from that stream. Probability routing draws from `route.<task>` in the same way, as [Task Flow](task-flow.md) describes. Adding a distribution therefore leaves every other one's draws unchanged, and renaming the simulation does not change them either:

![With the seed 42, the first run draws arrival.orders and service.packing; the second run, with returns added, draws the same numbers from those two streams and adds a stream of its own, arrival.returns](../assets/diagrams/seed-sequences.png)

In the low-level API, a `DistributionConfig` that was never registered draws from the `Sampler`'s main generator instead. Your own code can take a stream of its own with `sampler.generator("<name>")`:

```python title="docs/snippets/pages/arrivals_services.py:streams"
# Each registered distribution draws from the stream named by its path without
# the sim id; register the parameters, and sample() picks the stream
registry = SimulationRegistry(simpy.Environment())
registry.register_sim_parameter(SimParameter(sim_id="Shop", arrival={"orders": gap}))
assert gap.stream == "arrival.orders"
stream = Sampler(seed=42).generator("arrival.orders")  # the same seed, the same name
assert stream is not None and sampler.sample(gap) == stream.exponential(2.0)
```

`Sampler(rng=np.random.default_rng(42))` draws the same numbers as `Sampler(seed=42)`, because the streams come from the generator's seed.

**No random generator.** A `Sampler()` with neither a seed nor a generator draws nothing at random: every sample is the distribution's mean, or its value, which gives the time a model takes on average and suits a test. A `truncated_normal` gives its `mean` or its `low`, whichever is higher.

```python title="docs/snippets/pages/arrivals_services.py:no-generator"
fixed = Sampler()  # no random generator: every draw is the mean, or the value
assert fixed.sample(packing) == 3.0 and fixed.sample(gap) == 2.0
assert fixed.sample(checking) == 1.5  # shape × scale
```

**SimPy and desflow.** SimPy has no distributions: a SimPy model draws its own times, usually with `random.expovariate(0.5)` for a gap, and writes `yield env.timeout(...)` with the result. Nothing checks the parameters, a negative draw makes `timeout` raise, and the parameters are ordinary variables. desflow names each distribution and its parameters once, checks them, keeps them in the registry, and gives each a stream of its own; [SimPy and desflow](../des/simpy-and-desflow.md) compares the two.

---

## Under Live Changes

Each scalar parameter a family takes has a path in the registry: `<sim_id>.arrival.<name>.rate` for an exponential arrival, `<sim_id>.service.<name>.mean` and `.std` for a normal service, `.low`, `.mode` and `.high` for a triangular, `.shape` and `.scale` for a gamma or weibull, `.value` for a constant. A scenario, an ingress message or a process changes it while the run goes on, as [Live Parameters](live-parameters.md) explains:

- The next draw uses the new value. An order already being packed keeps the time it drew, and the arrival already being waited for keeps its gap.
- An update is checked on a copy of the configuration, by the same rules as at build. A value outside the range is refused with an error log line, and the old value stays:

```python title="docs/snippets/pages/arrivals_services.py:refused"
packing_path = "Shop.service.packing.std"
registry.register_sim_parameter(
    SimParameter(sim_id="Shop", service={"packing": packing})
)
registry.update(packing_path, -1.0)  # refused with an error log line
assert registry.get(packing_path).value == 0.5  # the old value stays
```

<!-- records: refused-update -->
```text
Invalid value for 'Shop.service.packing.std': normal std must be 0 or more, got -1.0. Update ignored.
```

- The family is fixed when the model is built. There is no path that turns a `normal` into an `exponential`, and an update to a parameter the family does not have, such as `.rate` on a normal, is refused as an unknown path, with a warning.
- An `empirical` distribution has no paths: its lists are taken at load.

---

## Stability and Limits

- **Refused at build.** The same checks refuse a bad distribution when the model is built. These three are an out-of-range `std`, a parameter `normal` does not take, and a `mode` above `high`:

    ```python title="docs/snippets/pages/arrivals_services.py:invalid"
    refused: List[Dict[str, Any]] = [
        {"dist": "normal", "mean": 3.0, "std": -1.0},  # out of range
        {"dist": "normal", "mean": 3.0, "rate": 2.0},  # a parameter normal does not take
        {"dist": "triangular", "low": 1.0, "mode": 3.0, "high": 2.0},  # mode above high
    ]
    for parameters in refused:
        try:
            DistributionConfig(**parameters)
        except ValueError as error:
            print(error)
    ```

    <!-- records: invalid-parameters -->
    ```text
    normal std must be 0 or more, got -1.0
    normal does not take rate; it takes mean, std
    triangular needs low <= mode <= high, got low 1.0, mode 3.0, high 2.0
    ```

- **A wide `normal`.** When `std` is half of `mean`, 2.3% of draws fall below 0 and are raised to the floor, and at `std` equal to `mean` it is 15.9%, so the average time rises: the normal panel in the chart above has a mean of 1.08, not 1. Use `truncated_normal`.
- **Rate against mean.** `exponential` takes a `rate` per second, not a mean gap: an order every 30 seconds is `rate: 0.0333`, not `30`.
- **A seed and the timestamps.** A seed fixes the draws and so the simulation times, but each record's `timestamp` also depends on when the run started, unless `logical_start_time` is set, as [Time and Pacing](time-and-pacing/index.md) explains.
- **Spread lengthens queues.** Two models with the same mean times but a different spread have different queues: more spread means longer waits, as [Queues and Capacity](../tutorials/queues-and-capacity.md) and [Choosing Inputs](../tutorials/choosing-inputs.md) measure.
