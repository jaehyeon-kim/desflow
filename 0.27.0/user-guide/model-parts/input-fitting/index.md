# Input Fitting

A model's distributions should come from data where data exists. Input fitting takes a column of measured times and fits every distribution family desflow has. It checks each result, and also offers the data itself as an empirical distribution. It writes the chosen settings in a form a model loads. This page describes the command and the Python call, every option, each outcome and how to deal with it, and how the fitting is done.

______________________________________________________________________

## What It Is

`desflow fit` and `fit_column` fit the families of [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/index.md) to service durations or to the gaps between arrivals, by maximum likelihood with scipy. Maximum likelihood chooses the parameters under which the measured values are most probable. Each family's fit becomes one row of a table, with its parameters in seconds and its diagnostics, and the best is marked. [Fitting the Inputs](https://jaehyeon.me/desflow/latest/learn/inputs/fit-the-inputs/index.md) fits the shop's 40 packing times and shows why the best by the numbers still needs checking against the model's answer. [Fit a Distribution](https://jaehyeon.me/desflow/latest/how-to/analysis/fit-a-distribution/index.md) fits the gaps of an order log.

Fitting needs the `fit` extra, which installs scipy and matplotlib: `pip install "desflow[fit]"`. A base install runs without the extra: desflow imports scipy and matplotlib only when a fit runs.

______________________________________________________________________

## How to Use It

### Fit a Column

```bash
# Fit the packing times, write the best fit and plot every candidate
desflow fit packing_times.csv --column packing_seconds --unit s --lower 0 --out packing_fit.yaml --plot fit.png
```

The command prints the table, writes the best candidate's settings to `--out` and the plot to `--plot`. It exits with status 1 when no candidate is usable, and with status 2 when the input is invalid.

```python
times = read_column(DATA, "packing_seconds")
result = fit_column(times, unit="s", lower=0)
print(result.text())  # every candidate, its status and diagnostics
```

```python
result = fit_column(read_column(DATA, "packing_seconds"), unit="s", lower=0)
print(result.text())  # every candidate, its status and diagnostics
PARAMS = SimParameter(
    sim_id="Shop",
    # An order every 1.25 s on average (rate 0.8)
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.8)},
    # The best candidate's settings, such as dist="triangular", low=0.546332
    service={"packing": DistributionConfig(**result.settings())},
    resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
)
```

`read_column(path, column)`, imported with `from desflow.analysis.fit import read_column`, reads one CSV column as text. `fit_column` takes any list of numbers or text, where an empty cell or None is a missing value.

### Options

| Command option       | `fit_column` argument | What it sets                                                                                                             | Default               |
| -------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------ | --------------------- |
| `--column`           |                       | the CSV column to fit                                                                                                    | required              |
| `--unit`             | `unit`                | the data's unit: `ms`, `s`, `min` or `h`; the settings are always in seconds                                             | `s`                   |
| `--candidates`       | `candidates`          | the families to try, such as `gamma,lognormal`; an unknown name is refused                                               | every family          |
| `--lower`, `--upper` | `lower`, `upper`      | the plausible range, in the data's unit: a value outside it is invalid, and a family that can draw outside it is flagged | none                  |
| `--bins`             | `bins`                | offer the empirical distribution as a histogram with this many bins                                                      | the values themselves |
| `--min-count`        | `min_count`           | the fewest values a family is fitted to                                                                                  | 20                    |
| `--tolerance`        | `tolerance`           | how far a fitted mean and standard deviation may be from the data's, as a share of the data's                            | 0.25                  |
| `--drop-invalid`     | `drop_invalid`        | leave invalid values out, counted, rather than stopping                                                                  | off                   |
| `--out`              | `result.yaml()`       | write the best candidate's settings as YAML                                                                              |                       |
| `--plot`             | `result.plot(path)`   | draw the data against the best candidate                                                                                 |                       |

A value is invalid when it is not a number, is not finite, or lies outside `--lower` and `--upper`. The table's first line counts the rows, and the values used, missing and invalid.

### Read the Result

`fit_column` returns a `FitResult`:

- **`table`:** one row per candidate, with `family`, `status`, `reason`, `parameters`, `log_likelihood`, `aic`, `ks_statistic`, `ks_p_value`, `bounds` and `best`. `aic` is the [AIC](https://jaehyeon.me/desflow/latest/learn/concepts/a-simulation-study/#aic-and-the-ks-test), where lower is better.
- **`best`:** the best candidate's family.
- **`settings(kind=None)`:** the settings of the best, or of the family `kind`, for `add_service(name, **settings)`. A family that is not usable is refused, and the error names those that are.
- **`text()`:** the table as the command prints it.
- **`yaml(kind=None)`:** the settings as the YAML file `--out` writes.
- **`plot(path, kind=None)`:** the PNG `--plot` draws: the data's histogram over values generated from the candidate, and a Q-Q plot, which puts each sorted data value against the generated value at the same quantile. Points on the diagonal mean that the two agree.

The settings have six significant digits, the same numbers the YAML file holds, so a script and a blueprint that load one fit draw the same values.

### How the Best Is Chosen

The best is the fitted family with the lowest [AIC](https://jaehyeon.me/desflow/latest/learn/concepts/a-simulation-study/#aic-and-the-ks-test) among those that cannot draw outside `--lower` and `--upper`. When every value is the same, it is `constant`. When no family within the bounds fitted, it is `empirical`, which never draws outside the data's range. Only when there is no empirical candidate is a fitted family that can draw outside the bounds chosen. A family that is rejected, did not fit or was skipped is never chosen.

When two families' AIC are too close to tell them apart, as [AIC and the KS Test](https://jaehyeon.me/desflow/latest/learn/concepts/a-simulation-study/#aic-and-the-ks-test) explains, check what the choice changes in the model's answer, as [Fitting the Inputs](https://jaehyeon.me/desflow/latest/learn/inputs/fit-the-inputs/#does-the-choice-matter) does.

### Outcomes

Each candidate ends with one status. Every one but `fitted` has a reason, and none stops the fit:

| Status                      | Cause                                                                                        | What to do                                                                                                                                                                                                                                                                                                                                                                                 |
| --------------------------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `skipped`                   | fewer values than `--min-count`                                                              | collect more data, or use the `empirical` candidate, which is still offered; lower `--min-count` only if the values are known to be typical                                                                                                                                                                                                                                                |
| `skipped`                   | a value of 0 or below, for `gamma`, `weibull` or `lognormal`, which are defined above 0 only | find out what the zeros are: rounding (record finer times), a different kind of event (remove it from the data), or real zero durations (use a family with no positive-only support, such as `exponential` or `truncated_normal`)                                                                                                                                                          |
| `skipped`                   | every value is the same, so only `constant` and `empirical` are fitted                       | use `constant`, or check that the column is the one meant                                                                                                                                                                                                                                                                                                                                  |
| `skipped`                   | a value below `low`, for `truncated_normal`, whose `low` is `--lower`, or 0                  | set `--lower` to the true lower bound                                                                                                                                                                                                                                                                                                                                                      |
| `did not fit`               | scipy's fit raised an error or its search did not settle; the reason is scipy's message      | the data is far from that family's shape: choose another family; more data can help                                                                                                                                                                                                                                                                                                        |
| `rejected`                  | a fitted parameter is not finite or is out of the family's range                             | as for `did not fit`                                                                                                                                                                                                                                                                                                                                                                       |
| `rejected`                  | the fitted mean or standard deviation is more than `--tolerance` from the data's             | look for outliers: correct or remove a value that is a recording error, and keep one that is real. `--upper` leaves out values above it only with `--drop-invalid`; without it, such a value stops the fit with status 2. With `--upper`, only the families bounded above, `uniform` and `triangular`, can be chosen as best, then `empirical`. Or use `--bins` for an empirical histogram |
| `rejected`                  | a value lies where the family cannot draw                                                    | as for the zeros above                                                                                                                                                                                                                                                                                                                                                                     |
| `fitted` with a bounds note | the family can draw below `--lower` or above `--upper`, such as `normal` below 0             | it is chosen only when nothing within the bounds fits; prefer a family with the right support                                                                                                                                                                                                                                                                                              |

The command fails only when no candidate is usable, `empirical` included. `empirical` is unusable only when there are no values, or when `--candidates` leaves it out.

### Load the Settings

`!include` reads a mapping from another file, relative to the blueprint, so the file `--out` wrote is loaded and checked as if it were written in place:

```yaml
services:
  # The fitted distribution, read from the file desflow fit wrote
  packing: !include packing_fit.yaml
```

An arrival also needs its `spawn`, so the include names the file and adds keys to it: `orders: !include {file: fitted/orders.yaml, spawn: pack}`. A YAML syntax error in the included file, or a file that is not a mapping, names that file and its line. A wrong key or value in it is reported at the blueprint's `!include` line, and so is a missing file. A file that includes itself, directly or through another, is refused at the `!include` that closes the loop.

```python
# The best candidate's settings, such as dist="triangular", low=0.546332
.add_service("packing", **result.settings())
```

`DistributionConfig(**result.settings())`, as the `fit` section above shows.

______________________________________________________________________

## How It Works

**Data first.** The values are read, and missing ones are counted and left out. Invalid ones are refused or, with `--drop-invalid`, counted and left out. They are converted to seconds. Then each family is checked against the data before it is fitted: enough values, positive values where the family needs them, and not a constant column.

**Maximum likelihood, location at 0.** desflow's families have no location parameter: a gamma or a Weibull starts at 0. scipy can also fit a location, which moves the start of the distribution to wherever the data suggests. For the tutorials' 40 packing times plus one outlier of 120 seconds, a free location puts a gamma's start at 0.70 seconds, the smallest value, with a shape of 0.21 and a scale of 81. desflow has no parameter for that start. So the location of `exponential`, `gamma`, `weibull` and `lognormal` is fixed at 0, and each fit is converted to desflow's parameters:

| Family             | Fitted with                                                                                        | desflow's parameters                                                      |
| ------------------ | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `exponential`      | `scipy.stats.expon.fit(x, floc=0)`                                                                 | `rate` = 1 ÷ scale                                                        |
| `normal`           | `scipy.stats.norm.fit(x)`                                                                          | `mean`, `std`                                                             |
| `lognormal`        | `scipy.stats.lognorm.fit(x, floc=0)`                                                               | the variable's `mean` = scale × e^(s²/2) and `std` = mean × √(e^(s²) − 1) |
| `truncated_normal` | the likelihood of `scipy.stats.truncnorm` with `low` fixed, maximised by `scipy.optimize.minimize` | `mean`, `std`, `low`                                                      |
| `uniform`          | `scipy.stats.uniform.fit(x)`                                                                       | `low`, `high`                                                             |
| `triangular`       | `scipy.stats.triang.fit(x)`                                                                        | `low`, `mode`, `high`                                                     |
| `gamma`            | `scipy.stats.gamma.fit(x, floc=0)`                                                                 | `shape`, `scale`                                                          |
| `weibull`          | `scipy.stats.weibull_min.fit(x, floc=0)`                                                           | `shape`, `scale`                                                          |
| `constant`         | every value is the same                                                                            | `value`                                                                   |
| `empirical`        | the values, or `numpy.histogram` with `--bins`                                                     | `values`, or `edges` and `counts`                                         |

Each fit runs on its own, so one that fails is a row, not a crash.

**Checks.** Every fitted result is checked before it can be chosen. Each parameter must be finite and within the family's rules, the same rules a blueprint's distribution is checked against. The family's mean and standard deviation, from desflow's own formulas, must be within `--tolerance` of the data's. A result that fails is `rejected` with the reason. Some bad fits still pass these checks. Fitted to a gamma, the three values 1.0, 1.0000001 and 1.0000002 give a shape of 1.4 × 10¹⁴ and a scale of 7.1 × 10⁻¹⁵, with the right mean and spread. `--min-count` guards against such fits: no family is fitted to fewer values than `--min-count`.

**Diagnostics.** For each fitted family, the table gives the log-likelihood of the data and the AIC. The k in the AIC counts only the fitted parameters: a fixed location does not count. The table also gives the [Kolmogorov-Smirnov statistic](https://jaehyeon.me/desflow/latest/learn/concepts/a-simulation-study/#aic-and-the-ks-test) and p-value from `scipy.stats.kstest`. The p-value assumes the parameters were known in advance. They were estimated from the same data, so the p-value is approximate and tends to be too high. `empirical` has no AIC.

**Plot.** The generated values are drawn through desflow's `Sampler`, seeded, with the same smallest delay a run applies, so the plot shows what a run draws and repeats exactly.

______________________________________________________________________

## Stability and Limits

- **Size of the extra.** These sizes were measured in new Python 3.12 environments on macOS. scipy 1.18.1 takes about 70 MB installed with uv, and about 100 MB with pip, which also compiles bytecode. Its download is 20 to 37 MB, depending on the platform. matplotlib and its dependencies, beyond the NumPy desflow already needs, add about 55 MB with uv and about 70 MB with pip. Fitting is not in the base install because of these sizes.
- **Local files only.** The command reads a CSV file. Reading from a database or an object store, and automated selection beyond the lowest AIC, come later.
- **A fit is not a validation.** A family that fits the data well can still change the model's answer, and a model whose inputs fit can still answer the question badly. Compare the model's output with the real system's, as [Experiment Results](https://jaehyeon.me/desflow/latest/user-guide/results/experiment-results/#compare-with-observed-values) does.
- **One column at a time.** Each column is fitted on its own, so a dependence between two inputs, such as larger orders taking longer to pack, is not captured.

______________________________________________________________________

## Example: Fitted Inputs

The `fitting` example fits the 40 packing times of the tutorials and runs one hour of a two-packer shop with the fitted packing time. It needs no Docker service and ends on its own.

The tutorial page [Fitting the Inputs](https://jaehyeon.me/desflow/latest/learn/inputs/fit-the-inputs/index.md) teaches these features step by step.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[fit]"

# 2. Download the example
desflow examples get fitting --api yaml
cd fitting/yaml

# 3. Run it
desflow fit ../packing_times.csv --column packing_seconds --lower 0 --out packing_fit.yaml && desflow run fitting.yaml
```

```bash
# 1. Install
pip install "desflow[fit]"

# 2. Download the example
desflow examples get fitting --api yaml
cd fitting/yaml

# 3. Run it
desflow fit ../packing_times.csv --column packing_seconds --lower 0 --out packing_fit.yaml && desflow run fitting.yaml
```

The file the fit writes, which the download already holds:

```yaml
# Fitted by desflow fit from packing_times.csv, column packing_seconds: triangular, from 40 values in s, written in seconds.
dist: triangular
low: 0.546332
mode: 1.2
high: 2.83766
```

The whole blueprint is [`examples/yaml/fitting.yaml`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/examples/yaml/fitting.yaml).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[fit]"

# 2. Download the example
desflow examples get fitting --api builder
cd fitting/builder

# 3. Run it
uv run --no-project --with "desflow[fit]" fitting_example.py
```

```bash
# 1. Install
pip install "desflow[fit]"

# 2. Download the example
desflow examples get fitting --api builder
cd fitting/builder

# 3. Run it
python fitting_example.py
```

The whole file is [`examples/builder/fitting_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/examples/builder/fitting_example.py).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install "desflow[fit]"

# 2. Download the example
desflow examples get fitting --api low-level
cd fitting/low-level

# 3. Run it
uv run --no-project --with "desflow[fit]" fitting_example.py
```

```bash
# 1. Install
pip install "desflow[fit]"

# 2. Download the example
desflow examples get fitting --api low-level
cd fitting/low-level

# 3. Run it
python fitting_example.py
```

The whole file is [`examples/low-level/fitting_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/examples/low-level/fitting_example.py).
