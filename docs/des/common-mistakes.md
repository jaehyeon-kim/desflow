# Common Mistakes

Most wrong simulation results come from a short list of mistakes, and most of them are made before the first run. This page lists eight, in the order they happen in a study. Each one says what the mistake is, shows what it does to the packing shop, and gives the fix. The list follows the pitfalls in Law's paper "Twenty-Three Critical Pitfalls in Simulation Modeling and How to Avoid Them" (Winter Simulation Conference, 2024), which we read, and the examples are our own runs. The checks in the code blocks are in one file, and each prints the number the text quotes.

---

## No Clear Question

A model built to "see what happens" has no way to be finished, because nothing says which details matter or when it answers. It grows, and the results cannot be judged. Write one question before you write the model, such as "how many packers keep the mean wait of the busy hour under 10 seconds?". [A Simulation Study](a-simulation-study.md) shows how to write it, with the measures that answer it.

---

## Copying the Real System in Full

A model with every detail of the real shop takes long to build, is hard to check, and needs data for each detail that nobody collected. Leave out whatever the question does not depend on. The test is to ask of each part: would the answer change if it were different? If not, drop it. [A Simulation Study](a-simulation-study.md) writes this down as the conceptual model, and lets the question decide how much detail goes in.

---

## Replacing a Distribution by Its Mean

Packing takes 1.5 seconds on average and an order arrives every 2.0 seconds on average, so it is tempting to put those two numbers in as constants. The shop then never has a queue. Real orders do not come at even gaps, and that variation is what makes a queue.

```python title="docs/snippets/pages/common_mistakes.py:means"
def means():
    """One packer, an order every 2.0 s and 1.5 s of packing: all constant, then exponential gaps."""
    fixed = waits(constant(2.0), constant(1.5), 1, n=10, seed=42, until=3600)
    random_gaps = waits(exponential(2.0), constant(1.5), 1, n=10, seed=42, until=3600)
    return mean(fixed), mean(random_gaps)
```

With constant gaps and constant packing times the mean wait is 0.00 seconds. With exponential gaps of the same mean and the same packing time it is 2.45 seconds, over the same 10 replications. Use the distribution you measured, not its mean. [Choosing Inputs](../tutorials/choosing-inputs.md) shows the effect of the spread on the shop, and [Queueing Basics](queueing-basics.md) gives the formula that ties the wait to the variation.

---

## Choosing a Normal or Uniform by Habit

A normal distribution is a common first choice, but a time cannot be negative. A normal with mean 1.5 seconds and standard deviation 1.0 draws a negative packing time on some orders.

```python title="docs/snippets/pages/common_mistakes.py:normal"
def negative_share(mean_time=1.5, sd=1.0, draws=100_000):
    """The share of packing times a normal distribution draws below zero."""
    rng = random.Random(42)
    return sum(rng.gauss(mean_time, sd) < 0 for _ in range(draws)) / draws
```

About 6.7% of the draws are below zero, so the model would have to clip them, which changes the mean, or fail. A uniform distribution has the opposite problem: it gives every value between its ends the same weight, which real times seldom have. Fit a distribution to the data, and let a goodness-of-fit check and a plot say which family it is. [Input Fitting](../user-guide/input-fitting.md) does this, and [Fit the Inputs](../tutorials/fit-the-inputs.md) works an example. With no data, a triangular distribution of the least, most likely and greatest values is a usual stand-in. Law's pitfall 18 warns against using one when data could be collected, because the greatest value is hard to choose and the long right tail of real times matters. Collect the data if you can.

---

## Taking One Run as the Answer

One run is one sample. Run the busy hour on three packers 10 times with different seeds, and the mean wait of a run is not the same twice.

```python title="docs/snippets/pages/common_mistakes.py:one_run"
def spread_of_runs():
    """The busy hour, 3 packers: the mean wait of each of 10 runs, shortest and longest."""
    runs = waits(exponential(1 / 1.8), constant(1.5), 3, n=10, seed=42, until=3600)
    return min(runs), max(runs), mean(runs)
```

The runs range from 1.52 seconds, 28% below the mean, to 2.75 seconds, 30% above it, and the mean is 2.11 seconds. This differs from the 2.25 seconds that [Trusting Results](../tutorials/trusting-results.md) reads after its first 10 replications, because that shop uses measured packing times and a 2-minute warm-up. A report that gave one run alone could be that far off. Run replications and report the mean with an interval. [Output Analysis](output-analysis.md) explains replications and how many to use.

---

## No Warm-up for a Steady State

A run starts with an empty shop, so the first orders find no queue, and the mean of a short run is biased low. The effect is largest when the shop is nearly full. Take one packer and an order about every 1.58 seconds, so that the packer is busy 95% of the time, and run it for 30 minutes from empty.

```python title="docs/snippets/pages/common_mistakes.py:warmup"
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

The mean wait of 40 such runs is 11.4 seconds, with an interval of 2.1 seconds either side, and the formula for the long-run wait of this shop gives 14.25 seconds. The interval does not reach it, so the start has pulled the figure down. Longer runs that leave out their first part remove the bias:

```python title="docs/snippets/pages/common_mistakes.py:warmup-fix"
value, half_width, long_run = empty_start(until=20_000, warmup=4000)
print(
    f"20,000 s runs leaving out the first 4,000 s: {value:.1f} ± {half_width:.1f} s"
)
```

```text
20,000 s runs leaving out the first 4,000 s: 14.2 ± 1.3 s
```

The interval now contains 14.25 seconds. Terminating questions do not need a warm-up, because the empty start is part of the question. [Output Analysis](output-analysis.md) explains how to choose the warm-up.

---

## Comparing Designs With One Run Each

Packing in 1.45 seconds instead of 1.5 is a real improvement, and 20 pairs of runs show how little one run of each can say about it.

```python title="docs/snippets/pages/common_mistakes.py:pairs"
def one_run_each():
    """Packing 1.45 s against 1.5 s: how often a single run of each says the faster design is not faster."""
    arrival = exponential(1 / 1.8)
    slow = waits(arrival, constant(1.5), 3, n=20, seed=1, until=3600)
    fast = waits(arrival, constant(1.45), 3, n=20, seed=2, until=3600)
    wrong = sum(s <= f for s, f in zip(slow, fast))
    return wrong, len(slow), mean(slow), mean(fast)
```

The mean wait is 1.51 seconds for the faster design and 1.99 seconds for the slower one over the 20 runs each. But in 4 of the 20 pairs the single run of the slower design was at or below the single run of the faster one, so one run each would have said the change did not help. Compare designs with the same number of replications each and an interval on the difference, using common random numbers to reduce the noise. [Comparing Designs](../tutorials/comparing-designs.md) does this on the shop.

---

## Treating One Run's Statistics as Independent

The mean wait of orders in one run is a mean of values that depend on each other: a long wait is followed by another long wait, because the second order stood in the line the first one made. The usual interval formula assumes independent values, so an interval built from the orders of one run is too narrow. Build the interval from the replications, each of which is independent of the others. [Output Analysis](output-analysis.md) shows how, and [Checking and Validating](checking-and-validating.md) lists the checks that catch the other mistakes before a result is reported.
