# Experiment Results

This page shows what an [experiment](index.md) compares, checks and writes: the comparison of designs, the checks, the observed values, the files and the report.

---

## How to Use It

### Compare Designs

For each measure and each sweep point, a `compare` pair gives three things: the variant's value minus the baseline's in each [replication](../../des/output-analysis.md#replications), the mean of those differences, and the mean's [Student t](../../des/output-analysis.md#interval-and-half-width) half-width. An interval that does not contain 0 says the designs differ at that confidence level.

### Check Results

A check bounds one measure of one variant, or of every variant when `variant` is left out:

- **`max` and `min`:** the bound, or both for a range.
- **`on: interval`**, the default, compares the far end of the interval with the bound: the upper end with `max` and the lower end with `min`. A mean below the bound only by chance does not pass.
- **`on: mean`** compares the mean, for a quick look, not a decision.

A check whose measure is not in `measures` is refused. The experiment writes every file before it reports a failed check, so the report shows what failed.

### Compare With Observed Values

`observed` names a CSV of values measured in the real system, with the columns `measure`, `value`, `unit`, `population` and `horizon`. Each row is placed beside the simulated interval of `variant` (the first variant when it is not given), and the report says whether the value lies inside it. A value means something only beside a simulated value of the same kind, so the experiment refuses a row whose unit, population or horizon differs from the measure's:

| Statistic | Unit | Population |
|---|---|---|
| `wait_*` of a task, recorded when an instance starts work | `s` | `started instances` |
| `service_*` of a task, recorded when an instance finishes | `s` | `finished instances` |
| `utilization` and `utilization_avg` | `percent` | `end of run` and `time average` |
| other `*_avg`, such as `queue_length_avg` | `count`, or `amount` for a container | `time average` |
| task and flow counts, such as `finished` and `started` | `count` | `count over the horizon` |
| `in_system` of a flow and `in_progress` of a task | `count` | `end of run` |
| other readings, such as `queue_length` | `count`, or `amount` for a container | `end of run` |

The horizon is `until` minus the [warm-up](../../des/output-analysis.md#one-run-starts-empty), in seconds or a duration such as `57 min`. The horizon describes a run that ends at `until`, so with a `stop` or a `drain`, the observed values must cover the same entities as the simulated ones. The observed comparison checks the model's output against the real system's. It does not fit an input distribution, which [Input Fitting](../input-fitting.md) covers.

### Files

An experiment writes six files to its output folder:

| File | Holds |
|---|---|
| `experiment.json` | the settings, the question, assumptions, inputs and limits; the model's source and its SHA-256, its sim id and its compiled parameters; desflow, SimPy, NumPy and Python versions; each design's registry values, seeds and the streams each run drew from; the counts' names; the observed comparison |
| `results.csv` | one row per design, replication and statistic: `design`, `variant`, a column per swept path, `replication`, `seed`, `statistic`, `kind` (`measure` or `count`), `unit`, `population` and `value` |
| `summary.csv` | one row per design and statistic: `unit`, `population`, `n`, `mean`, `half_width`, `low`, `high` and `level` |
| `comparisons.csv` | one row per compared pair, sweep point and measure: both means, `difference`, `half_width`, `low`, `high`, and the streams in `streams_both`, `streams_baseline_only` and `streams_variant_only` |
| `checks.csv` | one row per check, design and bound: `on`, `bound`, `limit`, the checked `value` and `passed` |
| `report.md` | the report, built from the five files above |

### Read the Report

`report.md` has the sections below, and a section with nothing to report says so:

- Question, Model and Assumptions.
- Inputs and Provenance: every distribution of the model with its parameters, and its provenance from `inputs` or "not stated".
- Run Settings.
- Counts: the mean of each count the statistics publish, such as finished orders, for each design.
- Results, with the population each measure is taken over.
- Comparisons, Checks, Observed Comparison and Limits.
- Missing Inputs: what the experiment does not state, such as an input without a provenance. The report names it rather than filling it in.

The middle of the example's report follows. It is for the example's own shop, not the busy hour of the tutorials:

<!-- records: experiment-report -->
```text
## Results

Each measure's mean over the replications, with its 95% confidence interval.

| Design | Measure | Population | Mean | Half-width | Interval | n |
|---|---|---|---|---|---|---|
| two | `tasks.pack.wait_mean` | started instances | 4.872 | 0.5493 | 4.323 to 5.422 | 20 |
| two | `resources.packers.utilization_avg` | time average | 80.56 | 1.034 | 79.53 to 81.59 | 20 |
| three | `tasks.pack.wait_mean` | started instances | 0.5389 | 0.05297 | 0.4859 to 0.5919 | 20 |
| three | `resources.packers.utilization_avg` | time average | 53.74 | 0.6803 | 53.06 to 54.42 | 20 |

## Comparisons

Each difference is the variant's value minus the baseline's, seed by seed, with the confidence interval of the mean difference.

| Baseline | Variant | Measure | Difference | Half-width | Interval | Random streams |
|---|---|---|---|---|---|---|
| two | three | `tasks.pack.wait_mean` | -4.333 | 0.5105 | -4.844 to -3.823 | 2 shared |
| two | three | `resources.packers.utilization_avg` | -26.82 | 0.3642 | -27.18 to -26.45 | 2 shared |

## Checks

Checks passed: 1 of 1.

| Design | Measure | Checked | Bound | Value | Result |
|---|---|---|---|---|---|
| three | `tasks.pack.wait_mean` | the upper end | max 2 | 0.5919 | passed |

## Observed Comparison

| Design | Measure | Observed | Unit | Simulated interval | Inside |
|---|---|---|---|---|---|
| two | `tasks.pack.wait_mean` | 4.6 | s | 4.323 to 5.422 | yes |
```

`desflow report results/packers` builds the report again from the other five files, such as after editing `experiment.json` by hand.
