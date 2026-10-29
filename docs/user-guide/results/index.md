# Results

A run publishes records, and it can also measure itself. One run gives one sample, many runs give an interval, and several designs give a comparison. The pages of this section go from one run to a whole study.

---

## What It Is

- **Statistics** are collected during one run: how busy each resource was, how long tasks waited, how many entities finished. A warm-up period can be left out.
- **Replications** run the same model with many seeds and give each statistic a mean and a 95% interval.
- **Experiments** run replications of several designs, compare the designs seed by seed, check them against bounds and observed values, and write the results and a report to files.

---

## Pages in This Section

- [Built-in Statistics](statistics.md): how to switch the statistics on, what each level publishes and costs, how each value is computed, and how to add readings of your own.
- [Replications](replications.md): how to run replications from the command line and from Python, what they return and send to the sinks, and how the seeds and intervals are made.
- [Experiments](experiments.md): the experiment file, the Python call and every setting.
- [Experiment Results](experiment-results.md): what an experiment compares, checks and writes, and its report.

---

## How to Use It

- For one run, switch the statistics on with `stats` in YAML or `with_stats` in the builder.
- For an interval, run replications with `desflow run --replications`.
- To compare designs, run an experiment with `desflow experiment`.

---

## How It Works

Each layer uses the one below it. The statistics measure one run while it goes on. Replications build the model again for each seed, run it unpaced with no ingress and no egress, read the statistics at the end of each run, and give a Student t interval over the runs. An experiment runs replications for each design and compares the designs seed by seed.
