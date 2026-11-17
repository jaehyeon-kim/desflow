# Trusting Results

[Measuring a Run](https://jaehyeon.me/desflow/latest/learn/first-model/measuring-a-run/index.md) read five numbers from one run of the busy hour. The run started with an empty shop: no orders waiting and every packer free. A real shop at that hour is never empty. The first orders of the run find free packers, so they wait less than orders in the real shop would. This page asks: **is one run enough?**

- **Decision:** how much of the start to leave out, and how many runs give an answer precise enough to act on.
- **Measures:** the mean wait and the packers' [utilisation](https://jaehyeon.me/desflow/latest/learn/concepts/queueing-basics/#utilisation), and how much each varies from run to run.
- **Runs:** the busy hour with 3 packers, run once with the seed 42, then 40 times with seeds spawned from 42, then 5 times after each of three warm-ups.
- **Assumptions:** those of Measuring a Run: orders at random at 1.8 a second, and the 40 packing times measured at the bench.

## What You Will Learn

This page shows how to:

- leave a warm-up out of a run's statistics;
- run replications with spawned seeds and read the mean with its interval;
- choose a warm-up and a number of replications.

## What You Add

This page adds a `warmup` to the run, then [replications](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#replications): the same run repeated with other seeds. The model simulates the same busy hour as Measuring a Run.

______________________________________________________________________

## Plot the Start

Some runs have a natural start and end, such as a shop that opens empty at 9:00 and closes at 17:00. These are **[terminating](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#terminating-or-steady-state)** runs, and the empty start is part of what they measure. Other runs stand for a system that keeps going, such as the shop in the middle of its busy hour. These are **steady-state** runs, and their empty start is a distortion called **[initialisation bias](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#one-run-starts-empty)**. The busy hour is a steady-state run.

To see how long the distortion lasts, plot an output over time and look for where it stops trending. Save the files from [Full Files](#full-files) in one folder and run the commands there. Publish the statistics every 10 seconds: `wait_mean` is then the mean wait of every order so far, and `queue_length` is the queue at that moment.

```yaml
# Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
stats: {level: typical, interval: 10}

egress:
  - type: Console
```

`WARMUP=0` keeps the first minutes, and `--until` stops the run at 15 minutes. `head -4` keeps the first four lines, the queue and the mean wait at 10 and 20 seconds:

```bash
WARMUP=0 desflow run trusting_results.yaml --until "15 min" --stdout | grep -E 'packers\.queue_length"|pack_order\.wait_mean"' | head -4
```

```text
{"stream_type":"telemetry","sim_ts":10.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":0}
{"stream_type":"telemetry","sim_ts":10.0,"timestamp":"...","path_id":"Shop.stats.tasks.pack_order.wait_mean","value":0.45699808696098815}
{"stream_type":"telemetry","sim_ts":20.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length","value":2}
{"stream_type":"telemetry","sim_ts":20.0,"timestamp":"...","path_id":"Shop.stats.tasks.pack_order.wait_mean","value":0.5569464838069348}
```

```python
# Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
.with_stats("typical", interval=10)
.add_egress(ConsoleEgress())
```

The script runs the first 15 minutes without a warm-up when it is given the argument `start`. `head -4` keeps the first four lines, the queue and the mean wait at 10 and 20 seconds:

```bash
python trusting_results.py start 2>&1 | grep -E "packers\.queue_length'|pack_order\.wait_mean'" | head -4
```

```text
[TEL] {'sim_ts': 10.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.queue_length', 'value': 0}
[TEL] {'sim_ts': 10.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.wait_mean', 'value': 0.45699808696098815}
[TEL] {'sim_ts': 20.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.queue_length', 'value': 2}
[TEL] {'sim_ts': 20.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.wait_mean', 'value': 0.5569464838069348}
```

```python
# Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
env.setup_stats("Shop", "typical", interval=10)
# The warm-up is set with the sinks: no sink receives a record stamped before it
# ends, and the statistics start again from zero when it ends
env.setup_egress([ConsoleEgress()], warmup=warmup)
```

The script runs the first 15 minutes without a warm-up when it is given the argument `start`. `head -4` keeps the first four lines, the queue and the mean wait at 10 and 20 seconds:

```bash
python trusting_results_low_level.py start 2>&1 | grep -E "packers\.queue_length'|pack_order\.wait_mean'" | head -4
```

```text
[TEL] {'sim_ts': 10.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.queue_length', 'value': 0}
[TEL] {'sim_ts': 10.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.wait_mean', 'value': 0.45699808696098815}
[TEL] {'sim_ts': 20.0, 'timestamp': '...', 'path_id': 'Shop.stats.resources.packers.queue_length', 'value': 2}
[TEL] {'sim_ts': 20.0, 'timestamp': '...', 'path_id': 'Shop.stats.tasks.pack_order.wait_mean', 'value': 0.5569464838069348}
```

Without `head -4`, each tab prints the two values every 10 seconds of the first 15 minutes, 90 lines for each path. Plot the `value` of the two paths against `sim_ts`. Some of the values:

| `sim_ts` (s) | Queue | Mean wait so far (s) |
| ------------ | ----- | -------------------- |
| 10           | 0     | 0.46                 |
| 30           | 5     | 0.89                 |
| 60           | 0     | 1.12                 |
| 90           | 4     | 1.55                 |
| 120          | 3     | 1.41                 |
| 300          | 4     | 1.19                 |
| 600          | 0     | 1.40                 |
| 900          | 0     | 1.27                 |

The plot of all 90 readings:

The queue reaches its usual range within the first minute. The mean wait so far climbs for about a minute and a half, because the first orders found free packers. After that it moves up and down with no trend. The plot shows one seed only, and another seed could climb for longer. So the plot alone cannot settle the [warm-up](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#one-run-starts-empty). It suggests three candidates:

- 1 minute, when the queue reaches its usual range;
- 2 minutes, which covers the climb of the mean wait;
- 4 minutes, twice that, to see whether a longer warm-up still moves the mean.

The next section sets 2 minutes. [Compare Warm-ups](#compare-warm-ups), after the replications, checks the three candidates with five seeds.

______________________________________________________________________

## Leave Out the Start

A `warmup` time leaves the start out of everything the run reports:

```yaml
# Records and statistics start after the first 2 minutes; 0 keeps everything
warmup: ${WARMUP:-2 min}
```

```python
# Records and statistics start after the first 2 minutes; None keeps everything
SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup=warmup)
```

```python
# Records and statistics start after the first 2 minutes; None keeps everything
WARMUP = 120.0  # seconds, passed to setup_egress in run below
```

In the low-level API the warm-up is an argument of `env.setup_egress`, as the `plot` step above shows, because the sinks are what leave the records out.

A run has one warm-up, in simulation time. Every sink leaves out the records and telemetry stamped before the warm-up ends. The built-in statistics start again from zero when it ends, so they describe only the rest of the run. An order that queued before the warm-up ended and started after it counts with its whole wait. The warm-up differs from `go_live_at`, which sends early records to one sink and later ones to another, and drops nothing. [Time and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/index.md) compares the two.

Run the hour without and with the warm-up. Each tab prints the mean wait without it, then with it:

`WARMUP=0` runs the blueprint without the warm-up.

```bash
# The mean wait without the warm-up, then with it
WARMUP=0 desflow run trusting_results.yaml --stdout | grep -E 'pack_order\.wait_mean"' | tail -1
desflow run trusting_results.yaml --stdout | grep -E 'pack_order\.wait_mean"' | tail -1
```

```text
{"stream_type":"telemetry","sim_ts":3600.0,"timestamp":"...","path_id":"Shop.stats.tasks.pack_order.wait_mean","value":1.3816663590146558}
{"stream_type":"telemetry","sim_ts":3600.0,"timestamp":"...","path_id":"Shop.stats.tasks.pack_order.wait_mean","value":1.3806466063873228}
```

```bash
python trusting_results.py 2>/dev/null | grep -E '^(without|with a)'
```

```text
without a warm-up: mean wait 1.382 s
with a 2 min warm-up: mean wait 1.381 s
```

```bash
python trusting_results_low_level.py 2>/dev/null | grep -E '^(without|with a)'
```

```text
without a warm-up: mean wait 1.382 s
with a 2 min warm-up: mean wait 1.381 s
```

______________________________________________________________________

## Run It With Other Seeds

The run in [Leave Out the Start](#leave-out-the-start) used one seed, 42. Another seed draws other arrival gaps and packing times, and gives another mean wait. One run cannot show how far apart those answers are. Within one run, an order's wait depends on the orders queued ahead of it, so the waits are not independent samples. **Replications** show it: the same run repeated with other seeds, each giving one independent value of every statistic.

Run the busy hour 40 times. The 40 seeds are spawned from the seed 42, so every run of the command uses the same 40 seeds. Each replication leaves out the same 2-minute warm-up:

```bash
# 5, 10, 20 and 40 runs: the mean wait and the packers' utilisation, each with its interval
for n in 5 10 20 40; do
  desflow run trusting_results.yaml --replications $n --measure tasks.pack_order.wait_mean --measure resources.packers.utilization_avg 2>/dev/null | grep -E '^(tasks|resources)'
done
```

```text
tasks.pack_order.wait_mean            5         2.207              0.4688
resources.packers.utilization_avg     5         89.72               1.371
tasks.pack_order.wait_mean           10         2.252              0.2476
resources.packers.utilization_avg    10         90.34              0.9148
tasks.pack_order.wait_mean           20         2.479              0.3223
resources.packers.utilization_avg    20         90.33              0.6263
tasks.pack_order.wait_mean           40         2.319              0.1951
resources.packers.utilization_avg    40         90.03              0.4496
```

```python
MEASURES = ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"]


def replications(n=40):
    """Runs the busy hour n times, each after the warm-up, with seeds spawned from 42."""
    return replicate(shop, n, seed=42, until="1 h", measures=MEASURES)
```

```bash
python trusting_results.py 2>/dev/null | grep ' runs: '
```

```text
tasks.pack_order.wait_mean, 5 runs: 2.21 ± 0.47 (21.2%)
tasks.pack_order.wait_mean, 10 runs: 2.25 ± 0.25 (11.0%)
tasks.pack_order.wait_mean, 20 runs: 2.48 ± 0.32 (13.0%)
tasks.pack_order.wait_mean, 40 runs: 2.32 ± 0.20 (8.4%)
resources.packers.utilization_avg, 5 runs: 89.72 ± 1.37 (1.5%)
resources.packers.utilization_avg, 10 runs: 90.34 ± 0.91 (1.0%)
resources.packers.utilization_avg, 20 runs: 90.33 ± 0.63 (0.7%)
resources.packers.utilization_avg, 40 runs: 90.03 ± 0.45 (0.5%)
```

```python
MEASURES = ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"]


def model(env, sampler):
    """The busy hour in a given environment: replicate sets the statistics."""
    env.registry.register_sim_parameter(PARAMS)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, sampler, packers))


def replications(n=40):
    """Runs the busy hour n times, each after the warm-up, with seeds spawned from 42."""
    shop = LowLevelModel(model, sim_id="Shop")
    return replicate(shop, n, seed=42, until=3600, warmup=WARMUP, measures=MEASURES)
```

```bash
python trusting_results_low_level.py 2>/dev/null | grep ' runs: '
```

It prints the same eight lines as the Builder script.

`LowLevelModel` names the function that builds the model in an environment it is given, and the warm-up is an argument of `replicate`, because a replication has no sinks.

Each replication runs unpaced with no sinks and reads the built-in statistics at its end. [Replications](https://jaehyeon.me/desflow/latest/user-guide/results/replications/index.md) describes every option.

The mean of the 40 values is the estimate. Its **[95% interval](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#interval-and-half-width)** says how far off it may be: with 95% confidence, the true mean lies within the estimate plus or minus the **half-width**. The seeds are spawned in order, so the first 10 of the 40 replications are the 10 a run with `--replications 10` makes. Each tab prints the estimate after the first 5, 10 and 20 replications and after all 40, which the table gives with the half-width as a share of the mean:

| Replications | Mean wait (s) | Half-width (s) | Wait half-width share | Utilisation (%) | Half-width (%) | Utilisation half-width share |
| ------------ | ------------- | -------------- | --------------------- | --------------- | -------------- | ---------------------------- |
| 5            | 2.21          | 0.47           | 21.2%                 | 89.72           | 1.37           | 1.5%                         |
| 10           | 2.25          | 0.25           | 11.0%                 | 90.34           | 0.91           | 1.0%                         |
| 20           | 2.48          | 0.32           | 13.0%                 | 90.33           | 0.63           | 0.7%                         |
| 40           | 2.32          | 0.20           | 8.4%                  | 90.03           | 0.45           | 0.5%                         |

The mean waits of the 40 replications range from 1.45 to 4.39 seconds. The [single run with the seed 42](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/#busy-hour-figures) measured 1.38 seconds, lower than any of them.

______________________________________________________________________

## Choose the Precision

How many replications are enough depends on the decision, not on a fixed number such as 30. Choose the precision each output needs. Here it is a half-width within 10% of the mean. [Utilisation](https://jaehyeon.me/desflow/latest/learn/concepts/queueing-basics/#utilisation) meets it with 5 replications. Of the replication counts in the table of [Run It With Other Seeds](#run-it-with-other-seeds), the mean wait first meets it at 40, because waits vary far more from run to run than how busy the packers are. Each output therefore needs its own number of replications, and the study needs enough replications for the least precise output the decision uses.

The half-width shrinks in proportion to 1 ÷ √n, so a half-width k times smaller needs about k² times as many replications. 10 replications gave 11.0%, so 10% needs about 10 × (11.0 ÷ 10)² ≈ 12. That figure is a guide, not a promise: the half-width is itself an estimate, and 20 replications gave 13.0%. Add replications, then read the half-width again.

The precision also depends on the confidence level, which is 95% unless you choose another. `--level 0.9` gives 90% intervals from the same 40 runs (`level=0.9` in `replicate`, for a builder or a low-level model):

```bash
# 40 runs with a 90% interval
desflow run trusting_results.yaml --replications 40 --measure tasks.pack_order.wait_mean --level 0.9
```

```text
measure                        n          mean    half-width (90%)
tasks.pack_order.wait_mean    40         2.319              0.1625
```

The run also prints its seeds and how to repeat one replication. The half-width falls from 0.20 to 0.16 seconds, 7.0% of the mean, with no extra run. A 90% interval misses the true mean more often, 1 study in 10 instead of 1 in 20, so choose the level with the precision, before the runs.

______________________________________________________________________

## Compare Warm-ups

One seed gives one path through the hour. Run five replications after each candidate warm-up, with the same five seeds spawned from 42, and compare the mean waits. Five replications are enough here, although the mean wait needs 40 for 10% precision. The comparison does not need a precise mean: it asks only whether a longer warm-up moves the mean by more than its half-width. Each candidate runs the same five seeds, so a difference between candidates comes from the warm-up and not from the seeds:

```bash
# 5 runs with each candidate warm-up
for w in "1 min" "2 min" "4 min"; do
  WARMUP="$w" desflow run trusting_results.yaml --replications 5 --measure tasks.pack_order.wait_mean 2>/dev/null | grep '^tasks'
done
```

```text
tasks.pack_order.wait_mean     5         2.196              0.4656
tasks.pack_order.wait_mean     5         2.207              0.4688
tasks.pack_order.wait_mean     5         2.232              0.4725
```

```python
def warmups(n=5) -> dict:
    """The mean wait of n runs after each candidate warm-up, with seeds spawned from 42."""
    wait = ["tasks.pack_order.wait_mean"]
    return {
        warmup: replicate(shop, n, seed=42, until="1 h", warmup=warmup, measures=wait)
        for warmup in ("1 min", "2 min", "4 min")
    }
```

```bash
python trusting_results.py 2>/dev/null | grep '^warm-up'
```

```text
warm-up 1 min: mean wait 2.196 ± 0.47 s
warm-up 2 min: mean wait 2.207 ± 0.47 s
warm-up 4 min: mean wait 2.232 ± 0.47 s
```

```python
def warmups(n=5) -> dict:
    """The mean wait of n runs after each candidate warm-up, with seeds spawned from 42."""
    shop = LowLevelModel(model, sim_id="Shop")
    wait = ["tasks.pack_order.wait_mean"]
    return {
        warmup: replicate(shop, n, seed=42, until=3600, warmup=warmup, measures=wait)
        for warmup in (60, WARMUP, 240)
    }
```

```bash
python trusting_results_low_level.py 2>/dev/null | grep '^warm-up'
```

```text
warm-up 60 s: mean wait 2.196 ± 0.47 s
warm-up 120 s: mean wait 2.207 ± 0.47 s
warm-up 240 s: mean wait 2.232 ± 0.47 s
```

`model` is the function from [Run It With Other Seeds](#run-it-with-other-seeds) above.

The mean wait over the five replications, in seconds, with its 95% half-width:

| Warm-up   | Mean wait (s) |
| --------- | ------------- |
| 1 minute  | 2.196 ± 0.47  |
| 2 minutes | 2.207 ± 0.47  |
| 4 minutes | 2.232 ± 0.47  |

[Output Analysis](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#one-run-starts-empty) keeps the shortest candidate that passes this comparison and covers the climb in the plot. After 1 minute, the longer warm-ups move the mean by 0.011 and 0.036 seconds. Both are far less than the half-width of 0.47 seconds, so every candidate passes. The plot climbs until about a minute and a half, so the warm-up is **2 minutes**.

______________________________________________________________________

## What You Get and Why

Is one run enough? No, because one run's answer depends on its seed. The run with the seed 42 measured 1.38 seconds, lower than every one of 40 replications. Over 40 replications the mean wait is **2.32 ± 0.20 seconds** with 95% confidence, within the chosen 10% precision, and the packers are busy **90.03 ± 0.45%** of the time.

The empty start is not the problem. The warm-up is **2 minutes**, the shortest candidate that passes the comparison over five seeds and covers the climb in the plot. In the run with the seed 42, a 2-minute warm-up changes the mean wait only from 1.382 to 1.381 seconds. The shop fills within a minute or two, and those minutes are a small part of one hour. A shorter run, or a start further from the usual state, such as a shop that opens with a backlog of orders, needs the warm-up much more.

Initialisation bias ([Output Analysis](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#one-run-starts-empty) explains it) fades once the state of the shop no longer depends on how the run started. Here the queue, empty for the first orders, reaches its usual range within a minute. The warmed-up numbers still pass the [Little's law](https://jaehyeon.me/desflow/latest/learn/concepts/queueing-basics/#littles-law) check from Measuring a Run. In the 3,480 seconds after the warm-up, 6,024 orders were packed, 1.73 a second. 1.73 orders a second × 1.381 seconds of mean wait = 2.39 orders waiting on average, which matches the `queue_length_avg` of 2.40 that the same statistics report. Each tab prints the two counts:

```bash
desflow run trusting_results.yaml --stdout | grep -E 'pack_order\.finished"|queue_length_avg"' | tail -2
```

```text
{"stream_type":"telemetry","sim_ts":3600.0,"timestamp":"...","path_id":"Shop.stats.resources.packers.queue_length_avg","value":2.3953385163016563}
{"stream_type":"telemetry","sim_ts":3600.0,"timestamp":"...","path_id":"Shop.stats.tasks.pack_order.finished","value":6024}
```

```bash
python trusting_results.py 2>/dev/null | grep '^after'
```

```text
after the warm-up: 6024 orders packed, queue_length_avg 2.40
```

```bash
python trusting_results_low_level.py 2>/dev/null | grep '^after'
```

```text
after the warm-up: 6024 orders packed, queue_length_avg 2.40
```

[Statistics](https://jaehyeon.me/desflow/latest/user-guide/results/statistics/index.md) describes how the reset works.

Replications ([Output Analysis](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#replications) explains them) are independent of each other, because each has its own seed. So the spread of their values measures how much the answer depends on chance. With few replications the spread itself is uncertain. The [Student t quantile](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#interval-and-half-width) allows for it: the half-width is the standard error times 2.776 for 5 replications, and times 2.023 for 40.

## Key Takeaways

- One run's answer depends on its seed. Replications measure how much.
- Report the mean of the replications with its 95% interval, not the value of one run.
- A warm-up removes the start of a run, which begins empty. Choose the shortest warm-up that gives the same answer as longer ones.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Trusting Results tutorial: the busy hour, measured after a warm-up.
# Run it with: desflow run trusting_results.yaml
# Without the warm-up: WARMUP=0 desflow run trusting_results.yaml
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  # Records and statistics start after the first 2 minutes; 0 keeps everything
  warmup: ${WARMUP:-2 min}

resources:
  packers: {current_cap: 3, max_cap: 10}

arrivals:
  orders: {dist: exponential, rate: 1.8, spawn: pack_order}

services:
  # The 40 packing times measured at the bench, in seconds
  packing:
    dist: empirical
    values: [1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
             2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
             1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
             2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1]

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

# Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
stats: {level: typical, interval: 10}

egress:
  - type: Console

run:
  until: 1 h
```

```python
"""The Trusting Results tutorial: the busy hour, measured after a warm-up, builder.

Three packers pack orders that arrive at 1.8 a second on average, with the packing
times measured at the bench. Every record is printed to the terminal. At the end the
script prints the mean wait without and with the warm-up, the mean wait of 5 runs
after each of three candidate warm-ups, then the mean and half-width of two outputs
over 5, 10, 20 and 40 replications. To see only those lines, run:
python trusting_results.py 2>/dev/null
With the argument start, it runs the first 15 minutes without a warm-up, to plot:
python trusting_results.py start 2>&1
"""

import logging
import sys

from desflow import ConsoleEgress, SimulationContext, replicate
from desflow.analysis import interval

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# The 40 packing times measured at the bench, in seconds
MEASURED = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip


def shop(warmup="2 min") -> SimulationContext:
    """The busy hour; records and statistics start after `warmup`."""
    app = (
        # Records and statistics start after the first 2 minutes; None keeps everything
        SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup=warmup)
        .add_resource("packers", current_cap=3, max_cap=10)
        .add_arrival("orders", dist="exponential", rate=1.8)
        .add_service("packing", dist="empirical", values=MEASURED)
        # Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
        .with_stats("typical", interval=10)
        .add_egress(ConsoleEgress())
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def busy_hour(warmup) -> dict:
    """Runs the busy hour and returns the statistics it measured."""
    app = shop(warmup)
    app.run(until="1 h")
    return app.env.stats.readings()


def mean_wait(warmup) -> float:
    """Runs the busy hour and returns the mean wait it measured."""
    return busy_hour(warmup)["Shop.stats.tasks.pack_order.wait_mean"]


MEASURES = ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"]


def replications(n=40):
    """Runs the busy hour n times, each after the warm-up, with seeds spawned from 42."""
    return replicate(shop, n, seed=42, until="1 h", measures=MEASURES)


def warmups(n=5) -> dict:
    """The mean wait of n runs after each candidate warm-up, with seeds spawned from 42."""
    wait = ["tasks.pack_order.wait_mean"]
    return {
        warmup: replicate(shop, n, seed=42, until="1 h", warmup=warmup, measures=wait)
        for warmup in ("1 min", "2 min", "4 min")
    }


def precision(result) -> dict:
    """Each output's mean and half-width over the first 5, 10, 20 and 40 replications."""
    return {
        (measure, n): interval(result.values[measure][:n])
        for measure in MEASURES
        for n in (5, 10, 20, 40)
    }


WAIT = "Shop.stats.tasks.pack_order.wait_mean"
PACKED = "Shop.stats.tasks.pack_order.finished"
QUEUE = "Shop.stats.resources.packers.queue_length_avg"


def report() -> None:
    """Prints the mean waits, the Little's law figures and the precision."""
    print(f"without a warm-up: mean wait {mean_wait(None):.3f} s")
    warmed = busy_hour("2 min")
    print(f"with a 2 min warm-up: mean wait {warmed[WAIT]:.3f} s")
    print(
        f"after the warm-up: {warmed[PACKED]} orders packed, "
        f"queue_length_avg {warmed[QUEUE]:.2f}"
    )
    for warmup, result in warmups().items():
        row = result.summary[0]
        print(
            f"warm-up {warmup}: mean wait {row['mean']:.3f} ± {row['half_width']:.2f} s"
        )
    for (measure, n), result in precision(replications()).items():
        share = result.half_width / result.mean
        print(
            f"{measure}, {n} runs: {result.mean:.2f} ± {result.half_width:.2f} ({share:.1%})"
        )


if __name__ == "__main__":
    if sys.argv[1:] == ["start"]:
        # The statistics every 10 seconds of the first 15 minutes, without a warm-up
        shop(None).run(until="15 min")
    else:
        report()
```

```python
"""The Trusting Results tutorial: the busy hour, measured after a warm-up, low-level.

Three packers pack orders that arrive at 1.8 a second on average, with the packing
times measured at the bench. Every record is printed to the terminal. At the end the
script prints the mean wait without and with the warm-up, the mean wait of 5 runs
after each of three candidate warm-ups, then the mean and half-width of two outputs
over 5, 10, 20 and 40 replications. To see only those lines, run:
python trusting_results_low_level.py 2>/dev/null
With the argument start, it runs the first 15 minutes without a warm-up, to plot:
python trusting_results_low_level.py start 2>&1
"""

import logging
import sys

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    LowLevelModel,
    Sampler,
    SimParameter,
    replicate,
)
from desflow.analysis import interval

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

# The 40 packing times measured at the bench, in seconds
MEASURED = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=1.8)},
    service={"packing": DistributionConfig(dist="empirical", values=MEASURED)},
    resources={"packers": CapacityConfig(current_cap=3, max_cap=10)},
)


def pack_order(env, sampler, packers, key, task_id):
    """One order, which reports each step to the statistics."""
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.flow_started("orders")  # the order enters the shop
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
    env.stats.task_finished("pack_order", task_id)
    env.stats.flow_finished("orders")  # the order leaves the shop


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    task_id = 0
    while True:
        yield env.timeout(sampler.sample(arrival))
        env.process(pack_order(env, sampler, packers, f"orders-{task_id + 1}", task_id))
        task_id += 1


# Records and statistics start after the first 2 minutes; None keeps everything
WARMUP = 120.0  # seconds, passed to setup_egress in run below


def run(warmup=WARMUP, until=3600, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    # Every 10 seconds, wait_mean is the mean wait so far: plot it against sim_ts
    env.setup_stats("Shop", "typical", interval=10)
    # The warm-up is set with the sinks: no sink receives a record stamped before it
    # ends, and the statistics start again from zero when it ends
    env.setup_egress([ConsoleEgress()], warmup=warmup)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, Sampler(seed=42), packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


def mean_wait(warmup) -> float:
    """Runs the busy hour and returns the mean wait it measured."""
    return run(warmup).stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


MEASURES = ["tasks.pack_order.wait_mean", "resources.packers.utilization_avg"]


def model(env, sampler):
    """The busy hour in a given environment: replicate sets the statistics."""
    env.registry.register_sim_parameter(PARAMS)
    packers = DynamicResource(env, "Shop", "packers")
    env.process(order_generator(env, sampler, packers))


def replications(n=40):
    """Runs the busy hour n times, each after the warm-up, with seeds spawned from 42."""
    shop = LowLevelModel(model, sim_id="Shop")
    return replicate(shop, n, seed=42, until=3600, warmup=WARMUP, measures=MEASURES)


def warmups(n=5) -> dict:
    """The mean wait of n runs after each candidate warm-up, with seeds spawned from 42."""
    shop = LowLevelModel(model, sim_id="Shop")
    wait = ["tasks.pack_order.wait_mean"]
    return {
        warmup: replicate(shop, n, seed=42, until=3600, warmup=warmup, measures=wait)
        for warmup in (60, WARMUP, 240)
    }


def precision(result) -> dict:
    """Each output's mean and half-width over the first 5, 10, 20 and 40 replications."""
    return {
        (measure, n): interval(result.values[measure][:n])
        for measure in MEASURES
        for n in (5, 10, 20, 40)
    }


WAIT = "Shop.stats.tasks.pack_order.wait_mean"
PACKED = "Shop.stats.tasks.pack_order.finished"
QUEUE = "Shop.stats.resources.packers.queue_length_avg"


def report() -> None:
    """Prints the mean waits, the Little's law figures and the precision."""
    print(f"without a warm-up: mean wait {mean_wait(None):.3f} s")
    warmed = run(WARMUP).stats.readings()
    print(f"with a 2 min warm-up: mean wait {warmed[WAIT]:.3f} s")
    print(
        f"after the warm-up: {warmed[PACKED]} orders packed, "
        f"queue_length_avg {warmed[QUEUE]:.2f}"
    )
    for warmup, result in warmups().items():
        row = result.summary[0]
        print(
            f"warm-up {warmup:g} s: mean wait {row['mean']:.3f} ± {row['half_width']:.2f} s"
        )
    for (measure, n), result in precision(replications()).items():
        share = result.half_width / result.mean
        print(
            f"{measure}, {n} runs: {result.mean:.2f} ± {result.half_width:.2f} ({share:.1%})"
        )


if __name__ == "__main__":
    if sys.argv[1:] == ["start"]:
        # The statistics every 10 seconds of the first 15 minutes, without a warm-up
        run(None, until=900)
    else:
        report()
```
