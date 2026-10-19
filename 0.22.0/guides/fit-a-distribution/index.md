# Fit a Distribution

**Level:** Intermediate

Fit a column of your own measured times, such as the gaps between orders in an order log, and load the result into a model. It builds on [Fit the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/index.md), which fits the shop's packing times and checks what the choice changes, and on [Input Fitting](https://jaehyeon.me/desflow/latest/user-guide/input-fitting/index.md), which describes every option and outcome. Fitting needs the `fit` extra: `pip install "desflow[fit]"`.

______________________________________________________________________

## Make the Column

A service is fitted to its durations. An arrival is fitted to the gaps between arrivals, not to the times they happened, so turn an order log's times into gaps first:

docs/snippets/guides/fit_a_distribution.py:gaps

```python
with LOG.open(newline="", encoding="utf-8") as handle:
    placed = [
        datetime.fromisoformat(row["placed_at"]) for row in csv.DictReader(handle)
    ]
# An arrival is fitted to the gaps between orders, not to the times themselves
return [
    (later - earlier).total_seconds() for earlier, later in zip(placed, placed[1:])
]
```

`order_log.csv` is an illustrative log of 300 orders placed one afternoon, with the time each was placed to a tenth of a second. Write the gaps to a CSV column to fit them with `desflow fit` instead.

______________________________________________________________________

## Fit It

State the unit and the bounds. The gaps are in seconds and none can be below 0:

docs/snippets/guides/fit_a_distribution.py:fit

```python
result = fit_column(gaps(), unit="s", lower=0)
print(result.text())  # every candidate, its status and diagnostics
```

On the command line, `desflow fit gaps.csv --column gap_seconds --unit s --lower 0 --out fitted/orders.yaml` does the same. The table for the 299 gaps:

```text
299 rows: 299 used, 0 missing, 0 invalid
family            status              AIC      KS p (approx.)  parameters or reason
exponential *     fitted             1205 0.03517      0.8404  rate=0.36388
normal            fitted             1487  0.1707   4.359e-08  mean=2.74816, std=2.88857 (can draw below the lower bound 0)
lognormal         skipped                                      lognormal needs every value above 0; 5 are not
truncated_normal  did not fit                                  RuntimeError: Maximum number of function evaluations has been exceeded.
constant          skipped                                      the values are not all equal
uniform           rejected                                     its mean is 12.75, the data's is 2.748: more than 25% apart
triangular        rejected                                     its mean is 7.702, the data's is 2.748: more than 25% apart
gamma             skipped                                      gamma needs every value above 0; 5 are not
weibull           skipped                                      weibull needs every value above 0; 5 are not
empirical         fitted                                       values: 299 numbers
* best: exponential
```

______________________________________________________________________

## Read the Table

- **Skipped** families were not fitted, and the reason says why. Here the log's times are to a tenth of a second, so 5 gaps are 0, and `gamma`, `weibull` and `lognormal`, which need every value above 0, are skipped.
- **Did not fit** means scipy's fit failed; its message is the reason. Here the truncated normal's search did not settle, because the gaps are far from a normal shape.
- **Rejected** families were fitted, but their mean or standard deviation is more than 25% from the data's, or a parameter is out of range. Here `uniform` and `triangular` reach far beyond most gaps.
- **Fitted** families have an AIC, lower is better, and a Kolmogorov-Smirnov p-value, which is approximate. The `exponential` has the lowest AIC and a p-value of 0.84, and its rate of 0.364 an order a second is one over the mean gap of 2.75 seconds. `normal` fits too, but it can draw below 0.

Two families within an AIC of 2 of each other fit about equally well; run the model with each and see whether the answer changes, as [Fit the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/#does-the-choice-matter) does. Read `--plot fit.png`, or `result.plot("fit.png")`, as well as the table.

______________________________________________________________________

## Load It

Write the best fit with `--out`, or from Python:

docs/snippets/guides/fit_a_distribution.py:write

```python
path = Path(folder) / "fitted" / "orders.yaml"
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(result.yaml(source="order_log.csv, gaps between placed_at"))
```

and include it. An arrival also needs the task it spawns, so the include names the file and adds `spawn`:

docs/snippets/yaml/fit_a_distribution.yaml:include

```yaml
arrivals:
  # The fitted gaps, with the task each order starts added to them
  orders: !include {file: fitted/orders.yaml, spawn: pack_order}
```

The file:

docs/snippets/yaml/fitted/orders.yaml

```yaml
# Fitted by desflow fit from order_log.csv, gaps between placed_at: exponential, from 299 values in s, written in seconds.
dist: exponential
rate: 0.36388
```

docs/snippets/guides/fit_a_distribution.py:load

```python
# The best fit's settings, such as dist="exponential", rate=0.4
.add_arrival("orders", **result.settings())
```

`DistributionConfig(**result.settings())` makes the arrival's config for a `SimParameter`.

`result.settings("normal")` takes another fitted family instead of the best.

______________________________________________________________________

## When Nothing Fits

When every family is skipped, rejected or did not fit, the best is the `empirical` distribution: the data itself, which a run draws from at random. For many values, a histogram is shorter:

docs/snippets/guides/fit_a_distribution.py:empirical

```python
result = fit_column(gaps(), unit="s", candidates=["empirical"], bins=10)
```

On the command line, `--candidates empirical --bins 10`. An empirical distribution never draws a value outside the data's range, so it cannot produce a gap longer than the longest one in the log.

______________________________________________________________________

## Example

The whole script fits the gaps and runs the shop for an hour with the fitted arrivals; one packer packs 1,302 orders. Run it from `docs/snippets/guides/` with `python fit_a_distribution.py`:

docs/snippets/guides/fit_a_distribution.py

```python
"""Fit a Distribution: the gaps between orders in an order log, fitted and loaded.

order_log.csv is an illustrative log of 300 orders placed one afternoon, with the
time each was placed. The script turns the times into the gaps between orders,
fits them, prints the table of candidates, and runs the shop for an hour with the
fitted arrivals. `write` saves the settings for a blueprint's `!include`.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path

from desflow import SimulationContext, fit_column

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)
LOG = Path(__file__).resolve().parent / "order_log.csv"


def gaps():
    """The gaps between consecutive orders, in seconds."""
    with LOG.open(newline="", encoding="utf-8") as handle:
        placed = [
            datetime.fromisoformat(row["placed_at"]) for row in csv.DictReader(handle)
        ]
    # An arrival is fitted to the gaps between orders, not to the times themselves
    return [
        (later - earlier).total_seconds() for earlier, later in zip(placed, placed[1:])
    ]


def fit():
    """Every family fitted to the gaps."""
    result = fit_column(gaps(), unit="s", lower=0)
    print(result.text())  # every candidate, its status and diagnostics
    return result


def histogram():
    """The empirical alternative: the gaps as a histogram of 10 bins."""
    result = fit_column(gaps(), unit="s", candidates=["empirical"], bins=10)
    return result


def write(result, folder):
    """Writes the best fit where a blueprint includes it."""
    path = Path(folder) / "fitted" / "orders.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(result.yaml(source="order_log.csv, gaps between placed_at"))
    return path


def shop(result) -> SimulationContext:
    """One packer, with the fitted arrivals."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=2)
        .add_service("packing", dist="gamma", shape=4, scale=0.375)
        # The best fit's settings, such as dist="exponential", rate=0.4
        .add_arrival("orders", **result.settings())
        .with_stats("basic")
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_loop(context):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


if __name__ == "__main__":
    app = shop(fit())
    app.run(until="1 h")
    finished = app.env.stats.readings()["Shop.stats.flows.orders.finished"]
    print(f"orders packed in an hour: {finished}")
```
