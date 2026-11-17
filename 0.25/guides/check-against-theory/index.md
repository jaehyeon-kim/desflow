# Check Against Theory

Check a model against the queueing formulas, on a queue the formulas describe. A model that agrees is computing what it should. A model that differs has a defect to find. It builds on [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md), which explains [utilisation](https://jaehyeon.me/desflow/latest/des/queueing-basics/#utilisation) and the waits the formulas give, and on [Checking a Model](https://jaehyeon.me/desflow/latest/tutorials/checking-a-model/index.md), which places this check among the others. The formulas are those of [Queueing Basics](https://jaehyeon.me/desflow/latest/des/queueing-basics/index.md).

______________________________________________________________________

## Build a Queue the Formulas Describe

The [M/M/1](https://jaehyeon.me/desflow/latest/des/queueing-basics/#kendalls-notation) and M/M/c formulas hold when the gaps between arrivals and the service times are exponential, every arrival joins one queue and waits until it is served, and nothing changes while the model runs. This shop has one packer, an order every 1.33 seconds on average (`rate: 0.75`) and a mean packing time of 1 second. Its utilisation is therefore 0.75 × 1 ÷ 1 = 0.75:

```yaml
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  warmup: 2000  # seconds left out of every measure, while the queue fills

arrivals:
  # An order every 1.33 s on average (rate 0.75) with one packer; RATE=1.5 with PACKERS=2
  orders:
    dist: exponential
    rate: ${RATE:-0.75}
    spawn: pack_order

services:
  packing: {dist: exponential, rate: 1}  # 1 second on average

resources:
  packers:
    current_cap: ${PACKERS:-1}
    max_cap: 4

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

run:
  until: 20000
```

The highlighted lines are the [warm-up](https://jaehyeon.me/desflow/latest/des/output-analysis/#one-run-starts-empty), which leaves out the first 2,000 seconds while the queue fills from empty, and the three numbers the formulas read: the arrival rate, the service rate and the number of packers. `RATE` and `PACKERS` set the second case below.

Work out the mean wait by hand first. For one packer it is ρ ÷ (1 − ρ) × the mean service time, where ρ is the utilisation. For several packers it is the [Erlang C](https://jaehyeon.me/desflow/latest/des/queueing-basics/#several-packers) chance that every packer is busy, divided by (c ÷ the mean service time − the arrival rate):

```python
load = rate * service  # packers' worth of work arriving each second
utilisation = load / packers
if packers == 1:
    return utilisation / (1 - utilisation) * service  # M/M/1
# M/M/c: Erlang C, the chance that every packer is busy, over (c / service - rate)
last = load**packers / math.factorial(packers) / (1 - utilisation)
busy = last / (sum(load**k / math.factorial(k) for k in range(packers)) + last)
return busy / (packers / service - rate)
```

With one packer at utilisation 0.75, the mean wait is 0.75 ÷ 0.25 × 1 = 3 seconds. With two packers and 1.5 orders a second, it is 1.286 seconds.

______________________________________________________________________

## Verify It

`desflow explain` gives the same values from the blueprint, and `--verify` runs the model and compares. Each pair is a value from the formulas and the same measure from the runs. A pair differs by one of two rules:

- With `--replications 10`, the model runs ten times. A pair differs when the formula's value lies outside the [95% interval](https://jaehyeon.me/desflow/latest/des/output-analysis/#interval-and-half-width) of the runs and is more than 1 percent from their mean.
- With one run, a pair differs when the two values are more than 10 percent apart.

The 1 percent matters when every run gives the same value, as with constant times. The interval then has a width of 0, and the last order still being packed at the end would count as a difference. This command runs ten replications:

```bash
desflow explain check_against_theory.yaml --verify --replications 10
```

```text
Shop
`-- orders (arrival, rate 0.75/s)
    `-- spawn -> pack_order (task, rate 0.75/s)
        +-- uses packers (resource)
        `-- next (share 1) -> exit
Resources
+--------------------------------------------------+
| resource |    λ | μ | c |    ρ | Wq | L | status |
|----------+------+---+---+------+----+---+--------|
| packers  | 0.75 | 1 | 1 | 0.75 |  3 | 3 | OK     |
+--------------------------------------------------+
packers by units
+---------------------+
| c |      ρ | status |
|---+--------+--------|
| 1 |   0.75 | OK     |
| 2 |  0.375 | OK     |
| 3 |   0.25 | OK     |
| 4 | 0.1875 | OK     |
+---------------------+
Bottleneck: packers
Hints for utilisation 0.85:
  packers with 1 unit: utilisation 0.75, at most 0.85
  packers with 1 unit: utilisation 0.85 at a request every 1.18 seconds (51 a minute, 0.85 a second)
Verify: 10 runs to 20000 s, seed 42
+---------------------------------------------------------------------+
| node       | measure      | analysis | measured |        ± | result |
|------------+--------------+----------+----------+----------+--------|
| packers    | utilization  |     0.75 |   0.7508 |  0.00624 | agrees |
| packers    | queue_length |     2.25 |     2.25 |    0.132 | agrees |
| pack_order | wait         |        3 |    2.994 |    0.163 | agrees |
| orders     | throughput   |     0.75 |   0.7514 | 0.004534 | agrees |
+---------------------------------------------------------------------+
--verify checks the implementation against the queueing analysis where it applies; it does not show that the model is close enough to the real system
```

Every measure agrees: the utilisation, the queue length Lq = λ × Wq, the mean wait of 3 seconds and the throughput. With two packers at the same utilisation, the wait comes from Erlang C:

```bash
PACKERS=2 RATE=1.5 desflow explain check_against_theory.yaml --verify --replications 10
```

```text
Verify: 10 runs to 20000 s, seed 42
+---------------------------------------------------------------------+
| node       | measure      | analysis | measured |        ± | result |
|------------+--------------+----------+----------+----------+--------|
| packers    | utilization  |     0.75 |    0.752 | 0.004173 | agrees |
| packers    | queue_length |    1.929 |    1.972 |  0.07398 | agrees |
| pack_order | wait         |    1.286 |    1.312 |  0.04669 | agrees |
| orders     | throughput   |      1.5 |    1.503 |  0.00529 | agrees |
+---------------------------------------------------------------------+
--verify checks the implementation against the queueing analysis where it applies; it does not show that the model is close enough to the real system
```

From Python, `analyze(verify=True)` runs the same check on a context loaded from a blueprint or a builder file, and returns it as `explanation.verification`:

```python
app = SimulationContext.from_yaml(BLUEPRINT)
explanation = app.analyze(verify=True, replications=10)
for c in explanation.verification.comparisons:
    result = "differs" if c.differs else "agrees"
    print(
        f"{c.node} {c.measure}: analysis {c.analytical:.3f}, "
        f"runs {c.measured:.3f} ± {c.half_width:.3f}, {result}"
    )
```

```text
packers utilization: analysis 0.750, runs 0.751 ± 0.006, agrees
packers queue_length: analysis 2.250, runs 2.250 ± 0.132, agrees
pack_order wait: analysis 3.000, runs 2.994 ± 0.163, agrees
orders throughput: analysis 0.750, runs 0.751 ± 0.005, agrees
```

A context built in a script's own code refuses `verify=True`, because the runs need a new copy of the model, which only a file can give.

`--json` prints the same result as JSON, for a script or a CI job outside Python. Each comparison has `node`, `measure`, `analytical`, `measured`, `half_width` and `differs`. With `jq -e`, this command prints `true` and exits 0 when no pair differs, and exits 1 when one does:

```bash
desflow explain check_against_theory.yaml --verify --replications 10 --json | jq -e '[.verification.comparisons[] | select(.differs)] | length == 0'
```

The JSON shape is experimental through 1.x, as the [Changelog](https://jaehyeon.me/desflow/latest/about/changelog/index.md) says.

______________________________________________________________________

## Read a Difference

A pair that differs on a model the formulas describe points at the model: a wrong rate or unit, a task that holds its resource longer than its service, or a route that sends entities somewhere else.

Chance can also make a pair differ. A short run varies from one run to the next. With replications, each pair's 95% interval misses a correct value about 1 time in 20, so among many pairs one difference is expected now and then. Before looking for a defect, run the check again with more [replications](https://jaehyeon.me/desflow/latest/des/output-analysis/#replications), a longer `until` or another seed.

Agreement shows the program computes what the model on paper says. It says nothing about whether the model is close enough to the real shop, which only a comparison with the shop's own records can show, as [Checking a Model](https://jaehyeon.me/desflow/latest/tutorials/checking-a-model/index.md) does.

______________________________________________________________________

## When the Formulas Do Not Apply

`explain` estimates a wait only where a formula holds, and gives the reason where none does. Two packers whose packing always takes 1 second have no closed formula:

```python
app = SimulationContext(sim_id="Shop", factor=0)
app.add_arrival("orders", dist="exponential", rate=1.5, spawn="pack_order")
app.add_service("packing", dist="constant", value=1)
app.add_resource("packers", current_cap=2, max_cap=4)
app.task("packing", "packers", name="pack_order")(lambda task_id: {})
packers = app.analyze().resources[0]
print(f"utilisation {packers.utilization}, status {packers.status}")
print(f"wait: {packers.wait_reason}")
```

```text
utilisation 0.75, status OK
wait: no closed formula for c > 1 with non-exponential times; run --verify to measure it
```

The utilisation is still known here, because it needs only the rates. The same rule applies to every model. The formulas hold in a plain network of queues: each task holds at most one resource, each queue serves first come, first served, and nothing blocks, removes, delays or reorders work. Anything else takes the formulas out of use, from the part of the model where it happens to the end of the flow:

- a task's `patience`, `balk`, `fail`, `keep`, `get`, `put`, `signal`, `subscribe`, `interrupt` or `on` actions, preemption, or several resources held at once: that task and every task after it;
- a `wait` other than a single `request`, such as a wait with two or more requests, or a request that a timeout of the same wait can end: that task and every task after it;
- a resource's `queue_limit`, custom type or breakdowns: the tasks that use it and every task after them;
- an unstable resource: every task after its tasks, though the resource itself keeps its utilisation and status;
- a priority queue whose tasks have different priorities: its wait, and every task after its tasks;
- a scenario step that changes a parameter, live ingress, or an `add_process` function: every arrival and every task;
- a replayed arrival, a hand-written arrival loop or a `next` function: every rate they could change.

A task outside what the formulas cover has no rate, so each resource it uses has no utilisation, status or wait. Such a resource is not ranked as the [bottleneck](https://jaehyeon.me/desflow/latest/des/queueing-basics/#flow-balance-and-the-bottleneck). When no resource has a utilisation, `explain` says that no bottleneck can be named, and why. Each unknown value is printed under `Not estimated`, with the first reason found.

One value is given but not compared. With one packer and gaps that are not exponential, such as constant or uniform gaps, [Kingman's formula](https://jaehyeon.me/desflow/latest/des/queueing-basics/#variation-lengthens-waits) gives the wait only approximately, so `explain` shows it as an estimate. `--verify` marks that wait and the queue length `not compared`, because a correct model can differ from an approximation.

`--verify` still runs such a model and prints what it measures. It compares only the values the analysis gives. Every other row says `not compared`, with the reason, and a row whose measure did not happen in the run says `no reading`. The interval from replications is then the check, as the experiments of [Checking a Model](https://jaehyeon.me/desflow/latest/tutorials/checking-a-model/index.md) use it.

______________________________________________________________________

## Example

The script reads its blueprint from `docs/snippets/yaml/` in the repository, so run it in a clone:

```bash
# The script is in docs/snippets/guides/, and its blueprint in docs/snippets/yaml/
git clone https://github.com/jaehyeon-kim/desflow.git
cd desflow/docs/snippets/guides
python check_against_theory.py
```

Full files

```yaml
# Check Against Theory: a shop the queueing formulas describe exactly. Gaps and
# packing times are exponential, one queue, no limits, nothing changes during the run.
# Check it with: desflow explain check_against_theory.yaml --verify --replications 10
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  warmup: 2000  # seconds left out of every measure, while the queue fills

arrivals:
  # An order every 1.33 s on average (rate 0.75) with one packer; RATE=1.5 with PACKERS=2
  orders:
    dist: exponential
    rate: ${RATE:-0.75}
    spawn: pack_order

services:
  packing: {dist: exponential, rate: 1}  # 1 second on average

resources:
  packers:
    current_cap: ${PACKERS:-1}
    max_cap: 4

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

run:
  until: 20000

egress:
  - type: Console
```

```python
"""Check Against Theory: a shop the queueing formulas describe, checked against them.

check_against_theory.yaml is one packer with exponential gaps and packing times. The
script works out its wait by hand, verifies the model against `explain`'s analysis
from Python, and shows a shop the formulas do not cover, with the reason.
"""

import math
from pathlib import Path

from desflow import SimulationContext

BLUEPRINT = Path(__file__).resolve().parents[1] / "yaml" / "check_against_theory.yaml"


def by_hand(packers: int, rate: float, service: float = 1.0) -> float:
    """The mean wait in the queue from the M/M/1 and M/M/c formulas."""
    load = rate * service  # packers' worth of work arriving each second
    utilisation = load / packers
    if packers == 1:
        return utilisation / (1 - utilisation) * service  # M/M/1
    # M/M/c: Erlang C, the chance that every packer is busy, over (c / service - rate)
    last = load**packers / math.factorial(packers) / (1 - utilisation)
    busy = last / (sum(load**k / math.factorial(k) for k in range(packers)) + last)
    return busy / (packers / service - rate)


def verify():
    """The blueprint's one packer, verified with 10 runs from Python."""
    app = SimulationContext.from_yaml(BLUEPRINT)
    explanation = app.analyze(verify=True, replications=10)
    for c in explanation.verification.comparisons:
        result = "differs" if c.differs else "agrees"
        print(
            f"{c.node} {c.measure}: analysis {c.analytical:.3f}, "
            f"runs {c.measured:.3f} ± {c.half_width:.3f}, {result}"
        )
    return explanation


def unsupported():
    """Two packers whose packing always takes 1 second: no formula for the wait."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=1.5, spawn="pack_order")
    app.add_service("packing", dist="constant", value=1)
    app.add_resource("packers", current_cap=2, max_cap=4)
    app.task("packing", "packers", name="pack_order")(lambda task_id: {})
    packers = app.analyze().resources[0]
    print(f"utilisation {packers.utilization}, status {packers.status}")
    print(f"wait: {packers.wait_reason}")
    return packers


if __name__ == "__main__":
    print(f"one packer, an order every 1.33 s (rate 0.75): {by_hand(1, 0.75):.3f} s")
    print(f"two packers at 1.5 orders a second: {by_hand(2, 1.5):.3f} s")
    verify()
    unsupported()
```
