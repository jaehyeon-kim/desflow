# Output Analysis

A simulation run gives numbers, and a number from one run is a sample, not the answer. Output analysis is the work of turning runs into an answer with a stated precision. This page explains the ideas, and the tutorials and User Guide pages named below apply them to the packing shop and show the commands.

It follows the treatment of output analysis for a single model in Banks, Carson, Nelson and Nicol, *Discrete-Event System Simulation*, chapter 11, and the output-analysis pitfalls in Law, "Twenty-Three Critical Pitfalls in Simulation Modeling and How to Avoid Them" (Winter Simulation Conference, 2024). We read the chapter through [lecture notes on it](https://courses.grainger.illinois.edu/ece541/fa2016/OutputAnalysis.pdf) and the paper through [its published text](https://informs-sim.org/wsc24papers/inv198.pdf). The explanations are our own.

---

## Terminating or Steady State

First decide which question the study asks. A **terminating** run ends at a time or an event the system has by nature, and starts from conditions the system really has. A shop that opens empty and closes at the end of the day is terminating: the day's answer is the answer. A **steady-state** study asks how a system behaves in the long run, after the effect of its starting conditions has gone. The same model can be either, because the choice depends on the question and on the system, not on the model.

The two ask for different handling of the start of the run and of its length:

- **Terminating:** the start is part of the answer. Run the real opening state, run to the real closing event, and use replications to measure the spread.
- **Steady state:** the start is a distortion. Leave out a warm-up, and run long enough that the rest represents the long run.

The busy hour of [Trusting Results](../tutorials/trusting-results.md) is treated as steady state, because the question is what the shop's waits usually are. [End Conditions](../tutorials/end-conditions.md) treats the shop as terminating: orders stop at 3,600 seconds and the question is what happens to the work still in the shop.

---

## One Run Starts Empty

A run starts with an empty shop, and the first orders find every packer free. Their waits are shorter than the waits of the hour as a whole, so the mean wait of a run is biased low by its start. Look at the diagram for the two parts of a run: in the warm-up the shop fills from empty and the queue has not settled, so it is left out of the statistics, and the measured period, once the queue has settled, is what the statistics cover.

![One run from an empty shop: a warm-up (left out of the statistics), then the measured period, then the end of the run at until](../assets/diagrams/run-phases.png)

A **warm-up** is a time at the start of a run whose observations are discarded. In desflow the run's `warmup` resets every statistic once at that time, as [Run Length](../user-guide/time-and-pacing/run-length.md) and [Statistics](../user-guide/statistics.md) describe.

Choose the warm-up from evidence, not from habit. Plot a running statistic from the start of the run, such as the queue length or the mean wait so far, for several seeds, and pick a time after which they all move up and down with no trend. Law's paper shows a plot made with Welch's method for this, in which the warm-up is the time after which the average has levelled off, and advises choosing it conservatively. Compare a few candidate cutoffs as well: run the replications with each, and take the shortest after which a longer warm-up no longer moves the mean by more than its half-width. [Trusting Results](../tutorials/trusting-results.md#compare-warm-ups) does it for the shop: over five seeds, the comparison of 1, 2 and 4 minutes gives 1 minute, and the tutorial keeps 2 minutes, because the mean wait so far of one run climbs for about a minute and a half. A 2-minute warm-up moves the mean wait of one run of the busy hour only from 1.382 to 1.381 seconds, because an hour is long enough that its first minutes hardly count. A shorter run, or one that starts far from the usual state, needs the warm-up much more.

A warm-up is not a fix for a model that never settles. If utilisation is 1 or more, there is no steady state, and no warm-up will find one, as [Queueing Basics](queueing-basics.md) explains.

---

## Replications

The waits of orders in one run follow each other: after a busy period, many orders in a row wait long. They are not independent samples, so the usual formula for the spread of an average, which assumes independent samples, gives an interval that is too narrow. Law lists this as a pitfall, along with treating one run as the truth.

**Replications** avoid the problem. Each replication is the same run with its own random stream, so it has its own mean wait, and gives one independent value of each statistic. The mean of those values is the estimate, and a 95% interval is the mean plus or minus t × s ÷ √n, where s is the standard deviation of the n values and t is the Student t quantile for n − 1 degrees of freedom. Look at the diagram for the flow from runs to one interval.

![Four runs, each with its own stream, combine into one mean wait with a 95% interval](../assets/diagrams/replication-interval.png)

In the busy hour of the shop, with the empirical packing times and a 2-minute warm-up, 40 replications give a mean wait of 2.32 ± 0.20 seconds, and the one run with the seed 42 measured 1.38 seconds, lower than any of the 40. One run would have given a confident and wrong answer. [Common Mistakes](common-mistakes.md#taking-one-run-as-the-answer) explains why other pages give other figures for this wait. [Trusting Results](../tutorials/trusting-results.md) shows these runs, [Replications](../user-guide/replications.md) describes the commands and the seeds, which are spawned from one seed so that the set repeats exactly, and [Randomness and Seeds](../tutorials/randomness-and-seeds.md) explains the streams.

---

## How Many Replications

There is no fixed number that is enough. Decide the precision each output needs, such as a half-width within 10% of the mean, and run until the least precise output has it. In the shop, utilisation meets 10% with 5 replications, and the mean wait first meets it at 40, because waits vary far more from run to run than how busy the packers are. The half-width shrinks with √n, so a short run can suggest the number needed, and then the interval has to be read again, because the half-width is itself an estimate. [Trusting Results](../tutorials/trusting-results.md) shows the table.

A longer run is the other way to buy precision, and it has limits of its own: it reduces the effect of the start, but its mean is still one sample.

---

## Which Orders a Reading Covers

A mean is over some orders, and the page that reports it should say which. At a fixed horizon, some orders have started and not finished, and the mean wait covers those that started. With a drain, every order admitted finishes, at the cost of running past the hour. In [End Conditions](../tutorials/end-conditions.md) the fixed horizon ends at 3,600 seconds with 2,436 orders started, 2,398 finished and 38 in the shop, and the drain ends at 3,656.4 seconds with all 2,436 finished.

The warm-up adds a third group, orders that were in the shop when the warm-up ended. An order that started before the warm-up ended and finished after it is counted as finished and in the wait and service times, with its whole wait, but not as started. [Statistics](../user-guide/statistics.md) has the table of which orders each count covers. Report the counts beside the means.

---

## Comparing Designs

To tell whether a design is better, compare the designs with the same random numbers. Run each design with the same seeds, so that each pair of runs meets the same arrivals and the same service times, and take the difference seed by seed. The chance part partly cancels, so the interval on the differences is narrower than the interval an unpaired comparison of the same runs gives, as [Comparing Designs](../tutorials/comparing-designs.md) uses it: a fourth packer cuts the busy hour's mean wait by 2.21 ± 0.31 seconds.

The check below shows the effect on 40 runs of the busy hour with 3 packers, 20 packing in 1.5 seconds and 20 packing in 1.45 seconds, with the same 20 seeds. It builds two intervals for the difference of the mean waits from the same runs: one paired by seed, and one that treats the designs as independent.

```python title="docs/snippets/pages/output_analysis.py:paired"
def waits(service: float, n: int = 20) -> list[float]:
    """The mean wait of each replication; the same seed gives the same seeds to every design."""
    runs = replicate(lambda: shop(service), n, seed=42, until=3600, measures=[WAIT])
    return runs.values[WAIT]


def paired_and_unpaired(slow: list[float], fast: list[float]):
    """Half-widths of the difference of the means: paired by seed, and as if independent."""
    n = len(slow)
    paired = interval([a - b for a, b in zip(slow, fast)])
    # Welch's interval: the two designs' variances add, and the degrees of freedom shrink
    va, vb = statistics.variance(slow) / n, statistics.variance(fast) / n
    df = (va + vb) ** 2 / (va**2 / (n - 1) + vb**2 / (n - 1))
    unpaired = t_quantile(0.975, df) * math.sqrt(va + vb)
    return paired, unpaired
```

With the seeds spawned from 42, the mean wait of the 20 runs packing in 1.5 seconds is 0.69 seconds longer than that of the 20 runs packing in 1.45 seconds. [Common Mistakes](common-mistakes.md#comparing-designs-with-one-run-each) runs the same two designs with other seeds and without pairing, so its means differ from these. The paired interval is ± 0.12 seconds and the unpaired one is ± 0.29 seconds, so pairing more than halves the interval here.

Pairing helps only when the designs use the random numbers in the same way. desflow gives each distribution a stream of its own, so adding a step to one design does not shift the numbers the shared steps draw. The comparison report lists any stream that only one design used, because draws from such a stream are not paired. Check that list before trusting a paired interval. Law names comparing designs without a sound statistical approach as a pitfall. The [lecture notes](https://courses.grainger.illinois.edu/ece541/fa2016/OutputAnalysis.pdf) on Banks et al. give the interval across replications the same form as the one above.

---

## What to Report

State, for each answer:

- The question: terminating or steady state, and the period it covers.
- The warm-up, and the evidence for it.
- The number of replications and the seeds' source.
- Each output as a mean with its half-width, and the precision the decision needed.
- Which orders each count covers.

[Readiness and Reporting](readiness-and-reporting.md) lists what a report must contain.
