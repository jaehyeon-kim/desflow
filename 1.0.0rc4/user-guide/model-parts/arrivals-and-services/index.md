# Arrivals and Services

Two building blocks set the timing of every simulation. An **arrival** decides how often a new entity enters the model: an order, a visitor, a support call. A **service** decides how long a piece of work takes: packing an order, answering a call. Both are distributions of time, and each time one is needed, a value is drawn from the distribution with its current parameters.

______________________________________________________________________

## What It Is

| Block   | Answers                                | Used by                                            | Example                            |
| ------- | -------------------------------------- | -------------------------------------------------- | ---------------------------------- |
| Arrival | How long until the next entity?        | the arrival loop, which starts a task for each one | an order every 2 s on average      |
| Service | How long does this piece of work take? | a task, while it holds its resource                | 3 s of packing, give or take 0.5 s |

An arrival draws each gap afresh from its distribution, so the gaps differ and entities arrive at uneven times. Look at the first five orders of the shop in [First Simulation](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md):

______________________________________________________________________

## How to Use It

Every distribution is a family, `dist`, and the parameters that family takes. There are twelve families: ten of times, and `poisson` and `geometric` for counts. A parameter outside its range, a missing one, or one the family does not take is refused when the model is built, with the YAML line when it comes from a blueprint. [Reference, Distributions](https://jaehyeon.me/desflow/latest/reference/distributions/index.md) lists every family with its parameters, rules, mean and variance. On this page, the table below names each family, the list under it gives each family's rules, and one example shows the form.

| Family                                                                                             | Use it for                                                                                                                                       | Parameters                                        |
| -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------- |
| [`exponential`](https://jaehyeon.me/desflow/latest/reference/distributions/#exponential)           | Gaps between arrivals that happen independently of each other: orders, calls, visits. Many short gaps and a few long ones, with mean `1 / rate`. | `rate`                                            |
| [`normal`](https://jaehyeon.me/desflow/latest/reference/distributions/#normal)                     | Work with a typical time and a small, symmetric spread.                                                                                          | `mean`, `std`                                     |
| [`truncated_normal`](https://jaehyeon.me/desflow/latest/reference/distributions/#truncated_normal) | Work with a typical time and a hard minimum.                                                                                                     | `mean`, `std`, `low`                              |
| [`lognormal`](https://jaehyeon.me/desflow/latest/reference/distributions/#lognormal)               | Times that are never negative and now and then much longer: repairs, support calls.                                                              | `mean`, `std`                                     |
| [`constant`](https://jaehyeon.me/desflow/latest/reference/distributions/#constant)                 | Fixed times: a conveyor, a setup, a step that takes no time.                                                                                     | `value`                                           |
| [`uniform`](https://jaehyeon.me/desflow/latest/reference/distributions/#uniform)                   | Any time in a range, all equally likely, when only the range is known.                                                                           | `low`, `high`                                     |
| [`triangular`](https://jaehyeon.me/desflow/latest/reference/distributions/#triangular)             | An expert's estimate: the shortest, the most likely and the longest time.                                                                        | `low`, `mode`, `high`                             |
| [`gamma`](https://jaehyeon.me/desflow/latest/reference/distributions/#gamma)                       | Times less variable than exponential. A whole-number shape is the Erlang distribution: that many exponential steps in a row.                     | `shape`, `scale`                                  |
| [`weibull`](https://jaehyeon.me/desflow/latest/reference/distributions/#weibull)                   | Time to failure: a shape below 1 fails early, a shape above 1 wears out.                                                                         | `shape`, `scale`                                  |
| [`empirical`](https://jaehyeon.me/desflow/latest/reference/distributions/#empirical)               | Times taken from real data, when no family fits.                                                                                                 | `values` with `weights`, or `edges` with `counts` |
| [`poisson`](https://jaehyeon.me/desflow/latest/reference/distributions/#poisson)                   | A count of events in a period, such as the orders in a minute: whole numbers from 0.                                                             | `mean`                                            |
| [`geometric`](https://jaehyeon.me/desflow/latest/reference/distributions/#geometric)               | The number of tries up to and including the first success: whole numbers from 1.                                                                 | `p`                                               |

The rules for each family's parameters:

- `exponential`: `rate` above 0, in events per second: 0.5 is one every 2 seconds on average, 2.0 is two a second. It is the one parameter that is not a time, so it takes no duration.
- `normal`: `mean`, and `std` 0 or more (0 when left out). A `std` of 0 gives the mean every time. A draw can be negative, and a draw below 0 is raised to 0. In a wide normal, these raised draws raise the mean, so use `truncated_normal` instead.
- `truncated_normal`: `mean`, `std` 0 or more, and `low`, 0 when left out. With `std` 0, `mean` must be at least `low`. A draw below `low` is thrown away and drawn again, so no draws pile up at the limit. The mean is therefore the truncated distribution's own: 1.29 seconds for `mean: 1, std: 1, low: 0`. The higher `low` is above the mean, the fewer plain draws are kept. So when `low` is more than half a standard deviation above the mean, `truncated_normal` draws from an exponential proposal instead (Robert, 1995), which stays exact and fast for any `low`.
- `lognormal`: `mean` above 0 and `std` 0 or more (0 when left out). Both describe the times you get, not the underlying normal. desflow converts them.
- `constant`: `value` 0 or more. It uses no random number at all. A constant of 0 takes no time.
- `uniform`: `low` and `high`, with `low < high`. The mean is halfway between them.
- `triangular`: `low`, `mode` and `high`, all required, with `low <= mode <= high` and `low < high`. The mean is (low + mode + high) ÷ 3.
- `gamma`: `shape` and `scale`, both above 0. The mean is shape × scale and the variance shape × scale². From a mean and a standard deviation: shape = (mean ÷ std)² and scale = std² ÷ mean, so a mean of 1.5 seconds with a std of 0.5 is shape 9 and scale 0.1667.
- `weibull`: `shape` and `scale`, both above 0. The mean is scale × gamma(1 + 1 ÷ shape), where gamma is the gamma function.
- `empirical`: either `values`, each equally likely unless the optional `weights` give one weight per value; or a histogram of `edges` and `counts`, with one count per bin. A histogram draw picks a bin in proportion to its count, then any time inside the bin, all equally likely. Weights and counts are 0 or more and add up to more than 0, and edges increase. The lists are taken when the model is loaded, so an `empirical` distribution has no registry path and does not change while the run goes on.
- `poisson`: `mean` 0 or more. Every draw is a whole number from 0, and the mean and the variance are both `mean`.
- `geometric`: `p` above 0 and at most 1. Every draw is a whole number from 1, with mean 1 ÷ `p`.

`poisson` and `geometric` give counts, not times. Use them where a count is read, such as the `group` and `max_events` of an arrival in [Start Entities](#start-entities). A service or a gap that draws from them takes the count as seconds.

`desflow dist` lists the families, and `desflow dist <family>` prints one family's parameters and rules. With `--mean` and `--std` it converts them to the family's parameters, so the gamma above needs no arithmetic. `--p` sets the `p` of a `geometric`:

```bash
desflow dist gamma --mean 1.5 --std 0.5
```

```text
parameters: shape 9, scale 0.166667
YAML: {dist: gamma, shape: 9, scale: 0.166667}
Python: dist="gamma", shape=9, scale=0.166667
```

The last two lines are the service in a blueprint and in the builder. [Command Line](https://jaehyeon.me/desflow/latest/cli/#convert-parameters) shows the other conversions.

Three of these families in a blueprint: a labelling time of at least 0.2 seconds, a station that fails more often as it wears, a histogram of measured packing times with three bins, and a scanning time of 0.5 seconds four times in five and 2 seconds otherwise:

```yaml
# under services: a hard minimum, a time to failure, a histogram of data, and
# weighted values
labelling: {dist: truncated_normal, mean: 1 s, std: 1 s, low: 0.2 s}
station_failure: {dist: weibull, shape: 1.5, scale: 10 min}
measured_packing: {dist: empirical, edges: [1 s, 2 s, 3 s, 4 s], counts: [5, 3, 2]}
scanning: {dist: empirical, values: [0.5 s, 2 s], weights: [4, 1]}
```

```python
# A hard minimum, a time to failure, a histogram of data, and weighted values
app.add_service("labelling", dist="truncated_normal", mean=1.0, std=1.0, low=0.2)
app.add_service("station_failure", dist="weibull", shape=1.5, scale=600)
app.add_service(
    "measured_packing", dist="empirical", edges=[1, 2, 3, 4], counts=[5, 3, 2]
)
app.add_service("scanning", dist="empirical", values=[0.5, 2], weights=[4, 1])
```

Each is a `DistributionConfig` with the same parameters in seconds, such as `DistributionConfig(dist="weibull", shape=1.5, scale=600)`.

An arrival or a service takes the same form for every family: `dist`, and the parameters in that family's row of the table. An exponential arrival, in the three ways to write it:

```yaml
# under arrivals: an order every 2 seconds on average
orders: {dist: exponential, rate: 0.5, spawn: pack}
```

```python
# An order every 2 s on average, each starting the task pack
app.add_arrival("orders", dist="exponential", rate=0.5, spawn="pack")
```

```python
gap = DistributionConfig(dist="exponential", rate=0.5)  # an order every 2 s on average
```

An arrival draws its gap from data in the same way. Here a batch arrives 1 second after the last one three times in four, and 5 seconds after it otherwise:

```yaml
# under arrivals: a gap of 1 second three times in four, and 5 seconds otherwise
batches: {dist: empirical, values: [1 s, 5 s], weights: [3, 1], spawn: pack}
```

```python
# A gap of 1 s three times in four, and 5 s otherwise
app.add_arrival(
    "batches", dist="empirical", values=[1, 5], weights=[3, 1], spawn="pack"
)
```

`DistributionConfig(dist="empirical", values=[1, 5], weights=[3, 1])` is the gap.

### Durations

In a blueprint, every time-valued parameter takes a duration as well as a number of seconds: `mean`, `std`, `low`, `mode`, `high`, `scale` and `value`, and the `values` and `edges` of an `empirical`. A plain number is seconds, and so is text that is only a number, such as `'1e-3'` or `'5'`. `rate` stays a number per second.

```yaml
# under services: every time-valued parameter takes a duration
support_call: {dist: lognormal, mean: 5 min, std: 3 min}
```

The builder takes seconds: `app.add_service("support_call", dist="lognormal", mean=300, std=180)`.

The low-level API takes seconds: `DistributionConfig(dist="lognormal", mean=300, std=180)`.

### Seeding

A seed makes every draw the same on every run:

```yaml
# The same draws on every run; leave it out to draw a seed and log it
random_seed: 42
```

`desflow run arrivals_services.yaml --seed 7` overrides it without editing the file.

```python
# The same draws on every run; leave it out to draw a seed and log it
app = SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
```

```python
sampler = Sampler(seed=42)  # the same draws on every run
```

In the low-level API you draw yourself, from the current values:

```python
seconds = sampler.sample(packing)  # one draw, in seconds, from the current values
```

A run without a seed draws one and logs it, so it can be repeated: `desflow run` logs the command with `--seed`, and a builder run logs the value to pass as `random_seed`:

```text
No random_seed was given, so this run uses 3166812249. Pass random_seed=3166812249 to repeat it.
```

A low-level `Sampler()` with no seed draws nothing at random, as [How It Works](#how-it-works) explains. [Randomness and Seeds](https://jaehyeon.me/desflow/latest/learn/first-model/randomness-and-seeds/index.md) shows seeds in use.

### Replay

An arrival can take its times from recorded rows instead of a distribution. Each row of a CSV or Parquet file, or of a list of dicts, becomes one entity at the row's time. The fields named in `fields` become the entity's attributes. Here each row of `local_replay_orders.csv` is an order, at its time in seconds from the start of the run:

```yaml
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

```python
# A CSV or Parquet file, or a list of dicts
.add_arrival_replay(
    "orders", TRACE, time="time", unit="s", fields=["order_id", "items"]
)
```

`add_arrival_replay` takes `spawn=` as `add_arrival` does, which starts the named task for each row. This example writes an `@app.arrival_loop` of its own instead, where `wait_for_arrival("orders")` waits for the next row.

```python
def replay(env, sampler, packers):
    """Starts one order at each row's time, carrying the row's fields."""
    rows = read_trace(TRACE, time="time", unit="s", fields=["order_id", "items"])
    number = 0
    # Rows with one time start in one step, as the blueprint starts them
    for time, same_time in itertools.groupby(rows, key=lambda row: row.time):
        yield env.timeout(time - env.now)
        for row in same_time:
            number += 1
            key = f"orders-{number}"
            env.process(pack(env, sampler, packers, key, number - 1, row.fields))
```

`read_trace` reads and checks the rows, and returns each with its simulation `time`, row `number` and `fields`.

`unit` is `ms`, `s`, `min` or `h` from the start of the run, or `timestamp` for a date and time, placed on the run's clock from `logical_start_time`. The rows must be in time order, and the arrival follows these rules:

- Rows at equal times arrive at that time, in the order of the file.
- A row before the start stops the run, and rows at or after `until` never arrive.
- A row without a valid time stops the run, unless `on_invalid` is `skip`, which leaves it out with a warning.
- After the last row, the arrival stops and logs one INFO line.

A CSV field is a number only when it is a CSV number: `1_0`, `nan` and `0x1` stay text, and as a time they are refused. A CSV file may start with a byte order mark. A row with more or fewer values than the header, and a header that names a column twice, are refused with the row or the column named. A Parquet file needs the `parquet` extra. A replayed arrival has no registry path. [Local Replay](https://jaehyeon.me/desflow/latest/learn/inputs/local-replay/index.md) compares a recorded morning with a model of it.

### Start Entities

An arrival starts a new entity after each gap. These keys change when it starts, how many it starts and what each carries. Each is a blueprint key, and the builder's `add_arrival` takes the same names, apart from `count`, `keys` and `stagger`:

- `gap` replaces `dist` when the gap is not a distribution: a number of seconds, a duration, an expression or a generator, read at the start of each gap, so it can read `registry[...]`, `level.<name>` and `t`. An arrival takes exactly one of `dist`, `replay` and `gap`, and a boolean is refused in `gap` and `stagger`.
- `first_at` is the simulation time of the first arrival, in place of a first gap. A time already past arrives at once.
- `max_events` is how many times the arrival starts its task in all, read when the source starts. The last start draws no further gap.
- `group` is how many entities one arrival starts, read once at each arrival, after the gap. A group of 0 or below starts nobody, draws no attributes and takes no key number, and the next gap still runs. It is not used with `count`, `keys` or `replay`.
- `count` makes that many entities, each with a loop of its own that starts the task after each gap, and `stagger` is the time between the starts of two of them. `keys` lists the keys instead and gives the number, so it needs no `count`.
- `key` is read once for each entity, for its key, and `vars` are named values read once for each entity before its attributes and key, which read them as bare names. The order at a start is `vars`, `attributes`, `key`. A start whose key belongs to a running entity of the arrival is skipped with a warning and counts as one. A constant `key` with a `count` above 1 is refused when the blueprint loads, because every entity would take the same key.
- `initial` creates that many entities when the run is built, with the arrival's keys, before any gap passes. An arrival with `initial` alone needs no `dist`, `replay` or `gap`.
- `active: false` starts a source off. A task's or an `on:` entry's `activate` and `deactivate` turn a source on and off, and each must name an arrival that has a `spawn`. Under `off_mode: drop`, the default, an arrival that falls while the source is off is lost and takes no key number. Under `restart`, the pending arrival is dropped when the source goes off, and `activate` starts a whole new gap.

A second `add_arrival` of one name replaces the first entirely. `count`, `keys`, `max_events` and `stagger` need `spawn`, and `stagger` needs `count` or `keys`. `count`, `keys`, `first_at` and `stagger` do not apply to a replay. Every place here reads the run only: `attr`, `payload`, `item`, `wait`, `process` and a bare name are refused, and a `level`, a `signal`, a resource, a store and a registry name must exist.

```yaml
arrivals:
  # A fixed gap, a first order at 5 s, and a number of orders drawn once: 4 on average
  orders:
    gap: 10 s
    first_at: 5
    max_events: {gen: sample, dist: geometric, p: 0.25}
    # A var is drawn once for each order, and the attributes and the key read it
    vars:
      lane: {gen: integer, bounds: [1, 3]}
    attributes:
      lane: {gen: expr, expr: lane}
    spawn: pack
  # A party of orders at a time: its size is drawn at each arrival, and 0 starts nobody
  parties:
    dist: exponential
    rate: 0.05
    group: {gen: sample, dist: poisson, mean: 2}
    spawn: pack
  # Three scanners, each a loop of its own, starting 2 s apart, with their own keys
  scanners:
    dist: constant
    value: 20 s
    count: 3
    stagger: 2 s
    key: {gen: sequence, format: "scanner-{}"}
    spawn: scan
  # A source that starts off, and starts a whole new gap when it is turned on
  rush: {dist: exponential, rate: 0.5, active: false, off_mode: restart, spawn: pack}
  # One start at 60 s
  bell: {gap: 1 s, first_at: 60, max_events: 1, spawn: open_rush}
  # One start at 80 s
  lull: {gap: 1 s, first_at: 80, max_events: 1, spawn: close_rush}
  # Two orders that exist when the run starts
  regulars: {initial: 2, spawn: pack}
  # A gap of whole seconds, 4 on average
  walkins: {dist: geometric, p: 0.25, spawn: pack}
```

A service takes the same families. The count a `geometric` service draws is read as seconds:

```yaml
services:
  packing: {dist: constant, value: 1.5 s}
  # Whole seconds with a fixed chance of 0.5 of ending at each: 2 s on average
  checking: {dist: geometric, p: 0.5}
```

```yaml
open_rush:
  payload: {event_type: rush_opened}
  # Turns the source of rush on as the task finishes
  activate: rush
close_rush:
  payload: {event_type: rush_closed}
  # Turns the source of rush off again
  deactivate: rush
```

```python
# A fixed gap, a first order at 5 s, and a number of orders drawn once: 4 on average
app.add_arrival(
    "orders",
    gap="10 s",
    first_at=5,
    max_events=generator({"gen": "sample", "dist": "geometric", "p": 0.25}),
    # A var is drawn once for each order, and the attributes and the key read it
    vars={"lane": generator({"gen": "integer", "bounds": [1, 3]})},
    attributes={"lane": generator({"gen": "expr", "expr": "lane"})},
    spawn="pack",
)
# A party of orders at a time: its size is drawn at each arrival, and 0 starts nobody
app.add_arrival(
    "parties",
    dist="exponential",
    rate=0.05,
    group=generator({"gen": "sample", "dist": "poisson", "mean": 2}),
    spawn="pack",
)
# A source that starts off, and starts a whole new gap when it is turned on
app.add_arrival(
    "rush",
    dist="exponential",
    rate=0.5,
    active=False,
    off_mode="restart",
    spawn="pack",
)
# One start at 60 s
app.add_arrival("bell", gap="1 s", first_at=60, max_events=1, spawn="open_rush")
# One start at 80 s
app.add_arrival("lull", gap="1 s", first_at=80, max_events=1, spawn="close_rush")
# Two orders that exist when the run starts, then one every 50 s: the builder always
# takes a dist or a gap
app.add_arrival("regulars", dist="constant", value=50, initial=2, spawn="pack")
# A gap of whole seconds, 4 on average
app.add_arrival("walkins", dist="geometric", p=0.25, spawn="pack")
```

```python
# Turns the source of rush on as the task finishes
@app.task(activate="rush")
def open_rush(task_id):
    return {"event_type": "rush_opened"}


# Turns the source of rush off again
@app.task(deactivate="rush")
def close_rush(task_id):
    return {"event_type": "rush_closed"}
```

The builder's `add_arrival` has no `count`, `keys` or `stagger`. It always takes a `dist` or a `gap`, so the `regulars` above also arrive every 50 seconds.

An `@app.arrival_loop` starts entities itself: it waits with `context.wait_for_arrival("orders")`, which reads `gap`, `first_at`, `key` and `vars` as the page above describes, and starts a task for each entity.

______________________________________________________________________

## How It Works

**One registry of distributions.** Every family is declared once, in `desflow.models.distributions.DISTRIBUTIONS`, with its parameters, its range rules, how to draw from it, its mean and variance, and what it gives with no random generator. The YAML models, the builder, `DistributionConfig`, the `Sampler` and the registry's live paths all read it, so they accept the same families and parameters. These are 200,000 draws from each of the ten families of times:

**A draw reads the live configuration.** A `DistributionConfig` holds the family and its parameters. `Sampler.sample(config)` reads the parameters at the moment it is called, so a change made while the run goes on applies from the next draw. A task draws its service time once it has its resource and has published `started`. The arrival loop draws the next gap when it starts waiting. A draw below 0 is raised to 0, so a time is never negative.

**One random stream per distribution.** A seeded `Sampler` makes one generator for each name: NumPy's `SeedSequence(seed, spawn_key=(CRC32 of the name,))`. When the registry registers a distribution, it names it by its path without the sim id, such as `arrival.orders` or `service.packing`, and every draw from that distribution comes from that stream. Probability routing draws from `route.<task>` in the same way, as [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/index.md) describes. Each stream depends only on the seed and the distribution's name. Adding a distribution therefore leaves every other one's draws unchanged. The name leaves out the sim id, so renaming the simulation does not change them either:

In the low-level API, a `DistributionConfig` that was never registered draws from the `Sampler`'s main generator instead. Your own code can take a stream of its own with `sampler.generator("<name>")`:

A blueprint and a builder model register every distribution, so each draws from its own stream. Code of your own takes a stream from `context.sampler.generator("<name>")` once the run is built.

A blueprint and a builder model register every distribution, so each draws from its own stream. Code of your own takes a stream from `context.sampler.generator("<name>")` once the run is built.

```python
# Each registered distribution draws from the stream named by its path without
# the sim id; register the parameters, and sample() picks the stream
registry = SimulationRegistry(simpy.Environment())
registry.register_sim_parameter(SimParameter(sim_id="Shop", arrival={"orders": gap}))
assert gap.stream == "arrival.orders"
stream = Sampler(seed=42).generator("arrival.orders")  # the same seed, the same name
assert stream is not None and sampler.sample(gap) == stream.exponential(2.0)
```

`Sampler(rng=np.random.default_rng(42))` draws the same numbers as `Sampler(seed=42)`, because the streams come from the generator's seed.

**No random generator.** A `Sampler()` with neither a seed nor a generator draws nothing at random: every sample is the distribution's mean, or a constant's value. A run then uses the mean of every input and gives the same output each time, which suits a test. That output is not the model's average result: [variation lengthens waits](https://jaehyeon.me/desflow/latest/learn/concepts/queueing-basics/#variation-lengthens-waits), so a run on mean inputs gives shorter waits than random runs give on average. A `truncated_normal` gives its `mean` or its `low`, whichever is higher.

A blueprint and a builder model always have a random generator: the `random_seed`, or a seed drawn and logged at the start. Draws at the mean are for a low-level test.

A blueprint and a builder model always have a random generator: the `random_seed`, or a seed drawn and logged at the start. Draws at the mean are for a low-level test.

```python
fixed = Sampler()  # no random generator: every draw is the mean, or the value
assert fixed.sample(packing) == 3.0 and fixed.sample(gap) == 2.0
assert fixed.sample(checking) == 1.5  # shape × scale
```

**SimPy and desflow.** SimPy has no distributions: a SimPy model draws its own times, usually with `random.expovariate(0.5)` for a gap of 2 seconds on average, and writes `yield env.timeout(...)` with the result. Nothing checks the parameters, a negative draw makes `timeout` raise, and the parameters are ordinary variables. desflow names each distribution and its parameters once, checks them, keeps them in the registry, and gives each a stream of its own. [SimPy and desflow](https://jaehyeon.me/desflow/latest/simpy/index.md) compares the two.

______________________________________________________________________

## Under Live Changes

Each scalar parameter a family takes has a path in the registry: `<sim_id>.arrival.<name>.rate` for an exponential arrival, `<sim_id>.service.<name>.mean` and `.std` for a normal service, `.low`, `.mode` and `.high` for a triangular, `.shape` and `.scale` for a gamma or weibull, `.value` for a constant. A scenario, an ingress message or a process changes it while the run goes on, as [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/index.md) explains:

- The next draw uses the new value. An order already being packed keeps the time it drew, and the arrival already being waited for keeps its gap.
- An update is checked on a copy of the configuration, by the same rules as at build. A value outside the range is refused with an error log line, and the old value stays:

A scenario step or an ingress message with an out-of-range value is refused in the same way, with the same error log line.

A scenario step or an ingress message with an out-of-range value is refused in the same way, with the same error log line.

```python
packing_path = "Shop.service.packing.std"
registry.register_sim_parameter(
    SimParameter(sim_id="Shop", service={"packing": packing})
)
registry.update(packing_path, -1.0)  # refused with an error log line
assert registry.get(packing_path).value == 0.5  # the old value stays
```

```text
Invalid value for 'Shop.service.packing.std': normal std must be 0 or more, got -1.0. Update ignored.
```

- The family is fixed when the model is built. There is no path that turns a `normal` into an `exponential`. An update to a parameter the family does not have, such as `.rate` on a normal, is refused as an unknown path, with a warning.
- An `empirical` distribution has no paths: its lists are taken at load.

______________________________________________________________________

## Stability and Limits

- **Refused at build.** The same checks refuse a bad distribution when the model is built. A parameter that is NaN or infinity is refused with its name, at load and on a live update. `true` is refused at load, and a live update stores it as 1, as [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/index.md) says. A negative `low` or `value` in a distribution of times (`uniform`, `triangular`, `truncated_normal` and `empirical`) is refused. So is a parameter whose draw would overflow: a `weibull` `shape`, a `lognormal` `mean` or a `poisson` `mean`. These three are an out-of-range `std`, a parameter `normal` does not take, and a `mode` above `high`:

  `desflow run` and `SimulationContext.from_yaml` refuse the same parameters, with the file, the line and the same message: `shop.yaml:3: services.packing: Value error, normal std must be 0 or more, got -1.0`.

  `app.add_service` and `app.add_arrival` raise the same `ValueError`, such as `normal std must be 0 or more, got -1.0`.

  ```python
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

  ```text
  normal std must be 0 or more, got -1.0
  normal does not take rate; it takes mean, std
  triangular needs low <= mode <= high, got low 1.0, mode 3.0, high 2.0
  ```

- **A wide `normal`.** When `std` is half of `mean`, 2.3% of draws fall below 0 and are raised to the floor. When `std` equals `mean`, 15.9% are. The raised draws push the average time up: the 200,000 draws of the normal panel in the chart above, with mean 1 and std 1, average 1.08, not 1. Use `truncated_normal`.

- **A gap of 0.** A constant gap of 0 starts entities at one instant without end, so the run never ends and `until` does not stop it, as in SimPy. A service of 0 takes no time and does not stop the run.

- **Rate against mean.** `exponential` takes a `rate` per second, not a mean gap: an order every 30 seconds is `rate: 0.0333`, not `30`.

- **A seed and the timestamps.** A seed fixes the draws and so the simulation times, but each record's `timestamp` also depends on when the run started, unless `logical_start_time` is set, as [Time and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/index.md) explains.

- **Spread lengthens queues.** Two models with the same mean times but a different spread have different queues: more spread means longer waits, as [Queues and Capacity](https://jaehyeon.me/desflow/latest/learn/first-model/queues-and-capacity/index.md) and [Choosing Inputs](https://jaehyeon.me/desflow/latest/learn/inputs/choosing-inputs/index.md) measure.

______________________________________________________________________

## Example: Local Replay

The `local-replay` example has one packer and the 40 orders of a recorded morning, which it replays from a CSV file at their times. The last order arrives at 73 seconds. It needs no Docker service and ends on its own after 120 simulation seconds.

The tutorial page [Local Replay](https://jaehyeon.me/desflow/latest/learn/inputs/local-replay/index.md) teaches these features step by step.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local-replay --api yaml
cd local-replay/yaml

# 3. Run it
desflow run local_replay.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local-replay --api yaml
cd local-replay/yaml

# 3. Run it
desflow run local_replay.yaml
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local-replay --api builder
cd local-replay/builder

# 3. Run it
uv run --no-project --with desflow local_replay_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local-replay --api builder
cd local-replay/builder

# 3. Run it
python local_replay_example.py
```

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get local-replay --api low-level
cd local-replay/low-level

# 3. Run it
uv run --no-project --with desflow local_replay_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get local-replay --api low-level
cd local-replay/low-level

# 3. Run it
python local_replay_example.py
```
