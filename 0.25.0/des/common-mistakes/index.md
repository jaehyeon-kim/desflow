# Common Mistakes

Most wrong simulation results come from a short list of mistakes, and most of them are made before the first run. This page lists eight, in the order they happen in a study. Each one says what the mistake is, shows what it does to the packing shop, and gives the fix. The list follows the pitfalls in Law's paper "Twenty-Three Critical Pitfalls in Simulation Modeling and How to Avoid Them" (Winter Simulation Conference, 2024), which we read. The examples are our own runs. The checks in the code blocks are in one file, and each prints the number the text quotes.

______________________________________________________________________

## No Clear Question

A model built to "see what happens" has no way to be finished, because nothing says which details matter or when it answers. It grows, and the results cannot be judged. Write one question before you write the model, such as "how many packers keep the mean wait of the busy hour under 10 seconds?". [A Simulation Study](https://jaehyeon.me/desflow/latest/des/a-simulation-study/index.md) shows how to write it, with the measures that answer it.

______________________________________________________________________

## Copying the Real System in Full

A model with every detail of the real shop takes long to build, is hard to check, and needs data for each detail that nobody collected. Leave out whatever the question does not depend on. The test is to ask of each part: would the answer change if it were different? If not, drop it. [A Simulation Study](https://jaehyeon.me/desflow/latest/des/a-simulation-study/index.md) writes this down as the conceptual model, and lets the question decide how much detail goes in.

______________________________________________________________________

## Replacing a Distribution by Its Mean

Packing takes 1.5 seconds on average and an order arrives every 2.0 seconds on average, so it is tempting to put those two numbers in as constants. The shop then never has a queue. Real orders do not come at even gaps, and that variation is what makes a queue.

```python
def means():
    """One packer, an order every 2.0 s and 1.5 s of packing: all constant, then exponential gaps."""
    fixed = waits(constant(2.0), constant(1.5), 1, n=10, seed=42, until=3600)
    random_gaps = waits(exponential(2.0), constant(1.5), 1, n=10, seed=42, until=3600)
    return mean(fixed), mean(random_gaps)
```

With constant gaps and constant packing times, the mean wait over 10 [replications](https://jaehyeon.me/desflow/latest/des/output-analysis/#replications) is 0.00 seconds. With exponential gaps of the same mean and the same packing time, it is 2.45 seconds over the same 10 replications. Use the distribution you measured, not its mean. [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/index.md) shows the effect of the spread on the shop, and [Queueing Basics](https://jaehyeon.me/desflow/latest/des/queueing-basics/index.md) gives the formula that ties the wait to the variation.

______________________________________________________________________

## Choosing a Normal or Uniform by Habit

A normal distribution is a common first choice, but a time cannot be negative. A normal with mean 1.5 seconds and standard deviation 1.0 draws a negative packing time on some orders.

```python
def negative_share(mean_time=1.5, sd=1.0, draws=100_000):
    """The share of packing times a normal distribution draws below zero."""
    rng = random.Random(42)
    return sum(rng.gauss(mean_time, sd) < 0 for _ in range(draws)) / draws
```

About 6.7% of 100,000 draws are below zero. The model would have to clip them, which changes the mean, or fail. A uniform distribution has the opposite problem: it gives every value between its ends the same weight, which real times seldom have. Fit a distribution to the data, and let a goodness-of-fit check and a plot say which family it is. [Input Fitting](https://jaehyeon.me/desflow/latest/user-guide/input-fitting/index.md) does this, and [Fitting the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/index.md) works an example. With no data, a triangular distribution of the least, most likely and greatest values is a usual stand-in. Law's pitfall 18 warns against using one when data could be collected, because the greatest value is hard to choose and the long right tail of real times matters. Collect the data if you can.

______________________________________________________________________

## Taking One Run as the Answer

One run is one sample. Run the busy hour with three packers 10 times with different seeds, and the mean wait of a run is not the same twice.

```python
def spread_of_runs():
    """The busy hour, 3 packers: the mean wait of each of 10 runs, shortest and longest."""
    runs = waits(exponential(1 / 1.8), constant(1.5), 3, n=10, seed=42, until=3600)
    return min(runs), max(runs), mean(runs)
```

The runs range from 1.52 seconds, 28% below the mean, to 2.75 seconds, 30% above it, and the mean is 2.11 seconds. A report that gave one run alone could be that far off. Run replications and report the mean with an interval. [Output Analysis](https://jaehyeon.me/desflow/latest/des/output-analysis/index.md) explains replications and how many to use.

### Busy Hour Figures

The pages of this site run the busy hour with 3 packers in different ways, so they give different figures for its mean wait. A figure changes with the number of runs, the packing times, the [warm-up](https://jaehyeon.me/desflow/latest/des/output-analysis/#one-run-starts-empty), the run length and the seeds. The table lists each figure, the pages that give it and how it was run. Every run is one hour at 1.8 orders a second unless the row says otherwise. Two terms in the table need a definition:

- "Measured times" are the 40 packing times measured at the bench, drawn as an `empirical` distribution.
- "Replications" have seeds spawned from 42. A run of 5, 10 or 20 replications uses the first 5, 10 or 20 of the 40 seeds.

| Mean wait (s) | Pages                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | How it was run                                                                                                              |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------- |
| 1.27          | [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md), [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/index.md)                                                                                                                                                                                                                                                                                           | one run, seed 42, constant packing, no warm-up                                                                              |
| 1.28          | [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/index.md)                                                                                                                                                                                                                                                                                                                                                                                             | one run, seed 42, the expert's triangular packing (1 to 2.25 s, mode 1.25), no warm-up                                      |
| 1.35          | [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/index.md)                                                                                                                                                                                                                                                                                                                                                                                             | one run, seed 42, measured times in a service named `empirical`, no warm-up                                                 |
| 1.38          | [Measuring a Run](https://jaehyeon.me/desflow/latest/tutorials/measuring-a-run/index.md), [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/index.md), [Output Analysis](https://jaehyeon.me/desflow/latest/des/output-analysis/index.md)                                                                                                                                                                                                             | one run, seed 42, measured times in a service named `packing`, without a warm-up (1.382) or with a 2-minute warm-up (1.381) |
| 2.37          | [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/index.md)                                                                                                                                                                                                                                                                                                                                                                                             | one run, seed 42, exponential packing, no warm-up                                                                           |
| 2.11          | this page, [Taking One Run as the Answer](#taking-one-run-as-the-answer)                                                                                                                                                                                                                                                                                                                                                                                                             | 10 replications, constant packing, no warm-up                                                                               |
| 1.99          | this page, [Comparing Designs With One Run Each](#comparing-designs-with-one-run-each)                                                                                                                                                                                                                                                                                                                                                                                               | 20 replications with seeds spawned from 1, constant packing, no warm-up                                                     |
| 2.05 ± 0.13   | [Queueing Basics](https://jaehyeon.me/desflow/latest/des/queueing-basics/#check-a-formula-against-the-shop)                                                                                                                                                                                                                                                                                                                                                                          | 10 replications of 20,000 seconds, constant packing, the first 2,000 seconds left out                                       |
| 2.20 to 2.23  | [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/#compare-warm-ups)                                                                                                                                                                                                                                                                                                                                                                                  | 5 replications, measured times, warm-ups of 1, 2 and 4 minutes                                                              |
| 2.25 ± 0.25   | [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/index.md), [Checking a Model](https://jaehyeon.me/desflow/latest/tutorials/checking-a-model/index.md)                                                                                                                                                                                                                                                                                               | 10 replications, measured times, 2-minute warm-up                                                                           |
| 2.48 ± 0.32   | [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/index.md), [Comparing Designs](https://jaehyeon.me/desflow/latest/tutorials/comparing-designs/index.md), [Parameter Sweeps](https://jaehyeon.me/desflow/latest/tutorials/parameter-sweeps/index.md), [Checking a Model](https://jaehyeon.me/desflow/latest/tutorials/checking-a-model/index.md), [Checking and Validating](https://jaehyeon.me/desflow/latest/des/checking-and-validating/index.md) | 20 replications, measured times, 2-minute warm-up                                                                           |
| 2.32 ± 0.20   | [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/index.md), [Fitting the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/index.md), [Comparing Designs](https://jaehyeon.me/desflow/latest/tutorials/comparing-designs/index.md), [Output Analysis](https://jaehyeon.me/desflow/latest/des/output-analysis/index.md), [A Simulation Study](https://jaehyeon.me/desflow/latest/des/a-simulation-study/index.md)                   | 40 replications, measured times, 2-minute warm-up                                                                           |
| 2.33 ± 0.19   | [Fitting the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/index.md)                                                                                                                                                                                                                                                                                                                                                                                           | 40 replications, the gamma fitted to the measured times, 2-minute warm-up                                                   |
| 2.93 ± 0.25   | [Fitting the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/index.md)                                                                                                                                                                                                                                                                                                                                                                                           | 40 replications, the triangular fitted to the measured times, 2-minute warm-up                                              |

The 1.35 and 1.38 seconds come from the same measured times. The two services draw the times in a different order, because each distribution draws from a random number stream named after its registry path, `service.empirical` or `service.packing` ([Randomness and Seeds](https://jaehyeon.me/desflow/latest/tutorials/randomness-and-seeds/#add-returns) explains the streams).

______________________________________________________________________

## No Warm-up for a Steady State

A run starts with an empty shop, so the first orders find no queue, and the mean of a short run is biased low. The effect is largest when the shop is nearly full. Take one packer and an order about every 1.58 seconds on average. With 1.5 seconds of packing, the packer is busy 1.5 ÷ 1.58 = 95% of the time. Run the shop for 30 minutes from empty.

```python
def empty_start(until=1800, warmup=None):
    """One packer at 95% utilisation: 40 runs, and the formula's long-run wait (M/D/1)."""
    runs = replicate(
        lambda: shop(exponential(1.5 / 0.95), constant(1.5), 1),
        40,
        seed=42,
        until=until,
        warmup=warmup,
        measures=[WAIT],
    )
    row = runs.summary[0]
    long_run = 0.95 / (1 - 0.95) * 1.5 / 2  # half the M/M/1 wait
    return row["mean"], row["half_width"], long_run
```

The mean wait of 40 such runs is 11.4 seconds, with a 95% interval of 2.1 seconds either side. The formula for the long-run wait of this shop gives 14.25 seconds. The interval ends below it, so the empty start has pulled the figure down. Forty longer runs that each leave out their first 4,000 seconds remove the bias:

```python
value, half_width, long_run = empty_start(until=20_000, warmup=4000)
print(
    f"20,000 s runs leaving out the first 4,000 s: {value:.1f} ± {half_width:.1f} s"
)
```

```text
20,000 s runs leaving out the first 4,000 s: 14.2 ± 1.3 s
```

The interval now contains 14.25 seconds. Terminating questions do not need a warm-up, because the empty start is part of the question. [Output Analysis](https://jaehyeon.me/desflow/latest/des/output-analysis/index.md) explains how to choose the warm-up.

______________________________________________________________________

## Comparing Designs With One Run Each

Packing in 1.45 seconds instead of 1.5 is a real improvement. Twenty runs of each design show how little one run of each can say about it.

```python
def one_run_each():
    """Packing 1.45 s against 1.5 s: how often a single run of each says the faster design is not faster."""
    arrival = exponential(1 / 1.8)
    slow = waits(arrival, constant(1.5), 3, n=20, seed=1, until=3600)
    fast = waits(arrival, constant(1.45), 3, n=20, seed=2, until=3600)
    wrong = sum(s <= f for s, f in zip(slow, fast))
    return wrong, len(slow), mean(slow), mean(fast)
```

Over the 20 runs each, with seeds spawned from 1 for the slower design and from 2 for the faster one, the mean wait is 1.99 seconds for the slower design and 1.51 seconds for the faster. These runs are not paired, so they are not the runs of the paired comparison in [Output Analysis](https://jaehyeon.me/desflow/latest/des/output-analysis/#comparing-designs), which runs both designs with the seeds spawned from 42.

Now match the runs in order, first with first. In 4 of the 20 matches, the slower design's run had a mean wait at or below the faster design's run. In those matches, one run of each design would have said the change did not help.

Compare designs with the same number of replications each and an interval on the difference. Use [common random numbers](https://jaehyeon.me/desflow/latest/des/output-analysis/#comparing-designs) to reduce the random variation in the difference. [Comparing Designs](https://jaehyeon.me/desflow/latest/tutorials/comparing-designs/index.md) does this on the shop.

______________________________________________________________________

## Treating One Run's Statistics as Independent

The mean wait of orders in one run is a mean of values that depend on each other: a long wait is followed by another long wait, because the second order waited in the same queue, behind the first. The usual interval formula assumes independent values, so an interval built from the orders of one run is too narrow. Build the interval from the replications, each of which is independent of the others. [Output Analysis](https://jaehyeon.me/desflow/latest/des/output-analysis/index.md) shows how, and [Checking and Validating](https://jaehyeon.me/desflow/latest/des/checking-and-validating/index.md) lists the checks that catch the other mistakes before a result is reported.
