# Arrivals and Services: `Sampler` and the Distributions

Two building blocks set the timing of every simulation. An **arrival** stream decides how often a new entity enters the model: an order, a visitor, a support call. A **service** decides how long a piece of work takes: packing an order, checking out a basket, answering a call. Both are distributions of time in seconds, and every time one is needed, a value is drawn from the distribution with its current parameters.

| Building block | Answers | Used by | Example |
|---|---|---|---|
| Arrival | How long until the next entity? | the arrival loop, which starts a task for each one | an order every 2 s on average |
| Service | How long does this piece of work take? | a task, while it holds its resource | 3 s of packing, give or take 0.5 s |

---

## Distributions

Five distributions are available. Each reads only its own parameters, and a value outside the range below is refused when the simulation is built, with the YAML line when it comes from a blueprint:

| `dist` | Parameters | Allowed | Shape | Typical use |
|---|---|---|---|---|
| `exponential` | `rate` | above 0 | many short gaps, a few long; mean `1 / rate` | arrivals that happen independently: orders, calls, visits |
| `normal` | `mean`, `std` | `std` 0 or more; 0 gives the mean every time | symmetric around the mean | work with a typical time, when `std` is small next to `mean` |
| `truncated_normal` | `mean`, `std`, `low` (0 by default) | `std` 0 or more; with `std` 0, `mean` at least `low` | a normal that never goes below `low` | work with a typical time and a hard minimum |
| `lognormal` | `mean`, `std` | `mean` above 0; `std` 0 or more | never negative, with a long tail of slow cases | repairs, support calls, anything now and then much slower |
| `constant` | `value` | 0 or more | the same value every time, with no random draw | fixed times: a conveyor, a set-up, a zero-time step |

`mean` and `std` are always in seconds and describe the times you get, including for `lognormal`, which converts them to the parameters of the underlying normal itself.

The chart shows 200,000 draws from each, in half-second bins:

![Bar charts of 200,000 draws: exponential with rate 0.5 falls away from 0 with mean 2; normal with mean 1 and std 1 has 15.8 percent raised to the floor, giving mean 1.08; truncated normal with low 0 has no pile at 0; lognormal with mean 2 and std 1.5 has a long right tail; constant 1.5 is a single bar](../assets/diagrams/distribution-shapes.png)

### Exponential

=== "YAML"

    ```yaml title="docs/snippets/yaml/arrivals_services.yaml:exponential"
    # under arrivals: an order every 2 seconds on average
    orders: {dist: exponential, rate: 0.5, spawn: pack}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/arrivals_services.py:builder-exponential"
    app.add_arrival("orders", dist="exponential", rate=0.5)  # an order every 2 s on average
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/arrivals_services.py:low-level-exponential"
    gap = DistributionConfig(dist="exponential", rate=0.5)  # an order every 2 s on average
    ```

`rate` is per second: 0.5 is one every 2 seconds on average, and 2.0 is two per second. It is the usual choice for arrivals, because it is what you get when entities arrive independently of each other.

### Normal

=== "YAML"

    ```yaml title="docs/snippets/yaml/arrivals_services.yaml:normal"
    # under services: 3 s on average, give or take 0.5 s
    packing: {dist: normal, mean: 3.0, std: 0.5}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/arrivals_services.py:builder-normal"
    app.add_service("packing", dist="normal", mean=3.0, std=0.5)  # 3 s, give or take 0.5 s
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/arrivals_services.py:low-level-normal"
    # 3 s, give or take 0.5 s
    packing = DistributionConfig(dist="normal", mean=3.0, std=0.5)
    ```

A normal draw can be negative. Every time is raised to at least 0.00001 seconds, so when `std` is large next to `mean` many draws land on that floor and the average time goes up: with `mean: 1` and `std: 1`, 15.8% of draws are raised and the mean becomes 1.08, as the yellow bar in the chart shows. Use `truncated_normal` for such a time.

### Truncated Normal

=== "YAML"

    ```yaml title="docs/snippets/yaml/arrivals_services.yaml:truncated-normal"
    # under services: 1 s on average, spread 1 s, never below 0.2 s
    labelling: {dist: truncated_normal, mean: 1.0, std: 1.0, low: 0.2}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/arrivals_services.py:builder-truncated-normal"
    app.add_service("labelling", dist="truncated_normal", mean=1.0, std=1.0, low=0.2)
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/arrivals_services.py:low-level-truncated-normal"
    labelling = DistributionConfig(dist="truncated_normal", mean=1.0, std=1.0, low=0.2)
    ```

A draw below `low` is thrown away and drawn again, so no draws pile up at the limit, and the mean is the truncated distribution's own: 1.29 s for `mean: 1, std: 1, low: 0`. Far into the tail, where plain draws would almost never land, it uses an exponential proposal (Robert, 1995), so it stays exact and fast for any `low`.

### Lognormal

=== "YAML"

    ```yaml title="docs/snippets/yaml/arrivals_services.yaml:lognormal"
    # under services: 4 s on average, now and then much longer
    repair: {dist: lognormal, mean: 4.0, std: 3.0}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/arrivals_services.py:builder-lognormal"
    # Now and then much longer
    app.add_service("repair", dist="lognormal", mean=4.0, std=3.0)
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/arrivals_services.py:low-level-lognormal"
    # Now and then much longer
    repair = DistributionConfig(dist="lognormal", mean=4.0, std=3.0)
    ```

### Constant

=== "YAML"

    ```yaml title="docs/snippets/yaml/arrivals_services.yaml:constant"
    # under services: always 0.5 s, with no random draw
    inspection: {dist: constant, value: 0.5}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/arrivals_services.py:builder-constant"
    app.add_service("inspection", dist="constant", value=0.5)  # always 0.5 s, no draw
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/arrivals_services.py:low-level-constant"
    inspection = DistributionConfig(dist="constant", value=0.5)  # always 0.5 s, no draw
    ```

A constant uses no random number, so adding one does not change the draws of the others in a seeded run. `value: 0` gives a step that takes no time.

---

## How a Draw Works

Every draw goes through a `Sampler`, which holds the random generator. A task draws its service time when it gets its resource, and the arrival loop draws the next gap when it starts waiting. Each draw reads the distribution's current parameters, so a change made while the simulation runs applies from the next draw.

**Seeding.** A seed makes every draw the same on every run, as long as the model draws in the same order:

=== "YAML"

    ```yaml title="docs/snippets/yaml/arrivals_services.yaml:seed"
    random_seed: 42  # the same draws on every run
    ```

=== "Builder"

    ```python title="docs/snippets/pages/arrivals_services.py:builder-seed"
    # Same draws every run
    app = SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/arrivals_services.py:low-level-seed"
    sampler = Sampler(rng=np.random.default_rng(42))  # same draws every run
    ```

In the low-level API you draw yourself, from the current values:

```python title="docs/snippets/pages/arrivals_services.py:low-level-draw"
seconds = sampler.sample(packing)  # one draw, in seconds, from the current values
```

A `Sampler` created with no generator draws nothing at random: every draw is the distribution's mean, or its value. That gives the time a model takes on average, which is useful in a test:

```python title="docs/snippets/pages/arrivals_services.py:no-generator"
fixed = Sampler()  # no random generator: every draw is the mean, or the value
assert fixed.sample(packing) == 3.0 and fixed.sample(gap) == 2.0
```

---

## SimPy and Dynamic DES

SimPy has no distributions: a SimPy model draws its own times, usually with `random.expovariate(0.5)` for a gap or `random.normalvariate(3.0, 0.5)` for a service, and writes `yield env.timeout(...)` with the result. Nothing checks the parameters, a negative draw is passed to `timeout`, which raises, and the parameters are ordinary variables that only the model's code can change.

Dynamic DES names each distribution and its parameters once, in a `DistributionConfig`. The parameters are checked when the model is built, every draw goes through one seeded `Sampler` and is raised to at least 0.00001 seconds, and the configuration lives in the registry, so a connector or a scenario can change it while the simulation runs.

---

## Under Live Changes

Each parameter has a path in the registry: `<sim_id>.arrival.<name>.rate` for an exponential arrival, `<sim_id>.service.<name>.mean` and `.std`, `.low` for a truncated normal, and `.value` for a constant. A connector, a scenario or a process can change it while the simulation runs, as [Live Parameters](../architecture/registry.md) explains:

- The next draw uses the new value. An order already being packed keeps the time it drew, and the arrival already being waited for keeps its gap.
- A value outside the range in the table is refused with an error log line, and the old value stays, as it would be refused at build:

```python title="docs/snippets/pages/arrivals_services.py:invalid"
try:
    DistributionConfig(dist="normal", mean=3.0, std=-1.0)
except ValueError as error:
    print(error)  # normal std must be 0 or more, got -1.0
```

- The distribution family is fixed when the simulation is built: there is no path that turns a `normal` into an `exponential` while it runs.

---

## Limits and Common Mistakes

- **A wide `normal`.** When `std` is half of `mean`, 2.3% of draws fall below 0 and are raised to the floor, and at `std` equal to `mean` it is 15.9%, so the average time rises. Use `truncated_normal`.
- **Rate against mean.** `exponential` takes a `rate` per second, not a mean gap: an order every 30 seconds is `rate: 0.0333`, not `30`.
- **A seed and the timestamps.** A seed fixes the draws and so the simulation times, but each record's `timestamp` also depends on when the run started, unless `logical_start_time` is set, as [Time and Pacing](../architecture/time.md) explains.
- **Queues grow faster than the averages suggest.** Two models with the same mean times but different spread have different queues: more spread means longer waits.
- **More distributions** are planned, including one built from values taken from real data; see the [Roadmap](../about/roadmap.md).
