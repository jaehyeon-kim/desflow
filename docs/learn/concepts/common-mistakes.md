# Common Mistakes

Most wrong simulation results come from a short list of mistakes, and most of them are made before the first run. This page explains eight, in the order they happen in a study. Each one shows what the mistake does to the packing shop, why, how to avoid it, and what the result looks like once it is avoided. The checks behind the figures are in [common_mistakes.py](https://github.com/jaehyeon-kim/desflow/blob/main/docs/snippets/pages/common_mistakes.py), which runs each one with the builder and asserts each number the page quotes.

---

## What You Will Learn

- why a study needs one question, and a model only the detail that question needs;
- what replacing an input by its mean, or choosing a distribution by habit, does to the waits;
- why one run, or one run of each design, is not an answer;
- why a steady-state question needs a warm-up;
- why the orders of one run cannot give an interval.

---

## No Clear Question

### In the Tutorial

A shop model built to "see what happens" could include the pickers, the stock, the couriers and the breaks. Nothing says which of them matter, so nothing says when the model is finished.

### Why It Matters

A model with no question grows, and its results cannot be judged, because there is no measure to judge them by.

### How to Use It

Write one question before you write the model, with the measure that answers it, such as "how many packers keep the mean wait of the busy hour under 10 seconds?". [A Simulation Study](a-simulation-study.md#ask-one-question) shows how to write it.

### What the Result Means

With the question written, each part of the model can be judged by whether it changes the answer. The shop's question needs the packers and the orders, and not the couriers.

---

## Copying the Real System in Full

### In the Tutorial

The busy-hour question depends on the arrivals, the packing times and the number of packers. The pickers, the stock and delivery are outside the shop's first model, and [A Flow of Tasks](../shop-logic/flow-of-tasks.md) adds picking only when a question needs it.

### Why It Matters

A model with every detail of the real shop takes long to build, is hard to check, and needs data for each detail that nobody collected. More detail can also hide the factors that matter.

### How to Use It

Ask of each part: would the answer change if it were different? If not, leave it out. [A Simulation Study](a-simulation-study.md#conceptual-model-and-data) writes this down as the conceptual model, and lets the question decide how much detail goes in.

### What the Result Means

A small model that answers the question is easier to check and to explain. A part added later, when a new question needs it, is checked on its own.

---

## Replacing a Distribution by Its Mean

### In the Tutorial

Packing takes 1.5 seconds on average and an order arrives every 2.0 seconds on average, so it is tempting to put those two numbers in as constants. With one packer, constant gaps and constant packing times, the mean wait over 10 [replications](output-analysis.md#replications) is 0.00 seconds. With exponential gaps of the same mean and the same packing time, it is 2.45 seconds over the same 10 replications.

### Why It Matters

Real orders do not come at even gaps, and that variation is what makes a queue. With constant inputs, each order arrives after the last one is packed, so no order ever waits. [Kingman's formula](queueing-basics.md#variation-lengthens-waits) shows the same: with no variation in the gaps or the packing times, the wait is zero.

### How to Use It

Use the distribution you measured, not its mean. [Choosing Inputs](../inputs/choosing-inputs.md) compares candidate distributions for the packing time, and [Fitting the Inputs](../inputs/fit-the-inputs.md) fits them to measured times.

### What the Result Means

A model whose waits are near zero while utilisation is high has probably lost its variation. In Choosing Inputs, the same mean packing time gives a mean wait from 1.27 seconds with constant times to 2.37 seconds with exponential ones.

---

## Choosing a Normal or Uniform by Habit

### In the Tutorial

A normal distribution with a mean of 1.5 seconds and a standard deviation of 1.0 is a tempting packing time. About 6.7% of 100,000 draws from it are below zero.

### Why It Matters

A time cannot be negative. The model would have to clip the negative draws, which changes the mean, or fail. A uniform distribution has the opposite problem: it gives every value between its ends the same weight, which real times seldom have.

### How to Use It

Fit a distribution to the data, and let a goodness-of-fit check and a plot say which family it is. [Input Fitting](../../user-guide/model-parts/input-fitting.md) does this, and [Fitting the Inputs](../inputs/fit-the-inputs.md) works an example. [A Simulation Study](a-simulation-study.md#aic-and-the-ks-test) explains the scores.

### What the Result Means

With no data, a triangular distribution of the least, most likely and greatest values is a usual stand-in. Law's pitfall 18 warns against using one when data could be collected, because the greatest value is hard to choose and the long right tail of real times matters. Collect the data if you can.

---

## Taking One Run as the Answer

### In the Tutorial

Run the busy hour with three packers 10 times with different seeds. The mean wait of a run ranges from 1.52 seconds, 28% below the mean, to 2.75 seconds, 30% above it, and the mean of the 10 is 2.11 seconds.

### Why It Matters

One run is one sample. A report that gave one run alone could be that far off, and it would carry no sign of how far.

### How to Use It

Run replications and report the mean with an interval. [Output Analysis](output-analysis.md#replications) explains replications and how many to use, and [Trusting Results](../studies/trusting-results.md) runs them for the shop.

### What the Result Means

The pages of this site give several figures for the busy hour's mean wait, because each runs it in a different way. [Busy Hour Figures](../first-model/first-simulation.md#busy-hour-figures) lists each figure and how it was run.

---

## No Warm-up for a Steady State

### In the Tutorial

Take one packer and an order about every 1.58 seconds on average. With 1.5 seconds of packing, the packer is busy 1.5 ÷ 1.58 = 95% of the time. Forty runs of 30 minutes from an empty shop give a mean wait of 11.4 ± 2.1 seconds. The formula for the long-run wait of this shop (M/D/1) gives 14.25 seconds, above the whole interval.

### Why It Matters

A run starts with an empty shop, so the first orders find no queue, and the mean of a short run is biased low. The effect is largest when the shop is nearly full, because the queue then takes longest to reach its usual length.

### How to Use It

For a steady-state question, leave out a warm-up and run long enough. [Output Analysis](output-analysis.md#one-run-starts-empty) explains how to choose the warm-up, and shows the setting in each form of the model.

### What the Result Means

Forty runs of 20,000 seconds that each leave out their first 4,000 seconds give 14.2 ± 1.3 seconds. The interval now contains 14.25 seconds, so the bias is gone. Terminating questions do not need a warm-up, because the empty start is part of the question.

---

## Comparing Designs With One Run Each

### In the Tutorial

Packing in 1.45 seconds instead of 1.5 is a real improvement. Over 20 runs of each, with seeds spawned from 1 for the slower design and from 2 for the faster one, the mean wait is 1.99 seconds for the slower design and 1.51 seconds for the faster. Match the runs in order, first with first: in 4 of the 20 matches, the slower design's run had a mean wait at or below the faster design's run.

### Why It Matters

In those 4 matches, one run of each design would have said the change did not help. The difference between two single runs is mostly the difference between their random numbers.

### How to Use It

Compare designs with the same number of replications each and an interval on the difference. Use [common random numbers](output-analysis.md#comparing-designs) to reduce the random variation in the difference. [Comparing Designs](../studies/comparing-designs.md) does this on the shop.

### What the Result Means

These runs are not paired, so they are not the runs of the paired comparison in Output Analysis, which runs both designs with the seeds spawned from 42. With pairing, the interval of the difference there is ± 0.12 seconds against ± 0.29 seconds without it.

---

## Treating One Run's Statistics as Independent

### In the Tutorial

In the busy hour, an order that waits long is usually followed by another that waits long, because the second waited in the same queue, behind the first.

### Why It Matters

The mean wait of one run is a mean of values that depend on each other. The usual interval formula assumes independent values, so an interval built from the orders of one run is too narrow.

### How to Use It

Build the interval from the replications, each of which is independent of the others. [Output Analysis](output-analysis.md#interval-and-half-width) shows how.

### What the Result Means

An interval from replications says how far the mean may be from the true one. An interval from one run's orders claims a precision the run does not have. [Checking and Validating](checking-and-validating.md) lists the checks that catch the other mistakes before a result is reported.

---

## Key Takeaways

- Write one question, and model only what its answer depends on.
- Use measured distributions, not their means, and not a normal or uniform by habit.
- Answer with replications and an interval, never with one run.
- Leave out a warm-up for a steady-state question.
- Compare designs with the same seeds and an interval on the difference.

---

## Sources

- Law, "Twenty-Three Critical Pitfalls in Simulation Modeling and How to Avoid Them" (Winter Simulation Conference, 2024), which we read. The list follows its pitfalls.
- The examples are our own runs.
