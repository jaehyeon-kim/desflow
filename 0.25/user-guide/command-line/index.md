# Command Line

The `desflow` command runs a model without a line of Python, and also checks it, compares designs, fits measured times and converts a distribution's parameters. This page uses each command in the order a study uses it, on one shop, and says what each output tells you.

______________________________________________________________________

## What It Is

`desflow` is installed with the package: `uv tool install desflow`, or `pip install desflow`. Each task is a command of its own, such as `desflow run` or `desflow explain`. `desflow` on its own lists them by task, with examples. The `--help` of each command, such as `desflow run --help`, lists its options and ends with examples of that command. [CLI](https://jaehyeon.me/desflow/latest/reference/cli/index.md) gives the help of every command, with every option.

The shop on this page is the model of the `experiment` example: two packers, an order every 2.5 seconds on average, and 4 seconds of packing on average. Every command runs in one folder and needs no Docker service. `desflow fit` needs the `fit` extra: `uv tool install "desflow[fit]"`, or `pip install "desflow[fit]"`.

______________________________________________________________________

## How to Use It

### Get an Example

`desflow --version` prints the release you installed. `desflow examples list`, or `desflow examples` alone, lists every example with the service it needs, such as `(no service)` or `(odctl up kafka-lite)`:

```bash
desflow --version
desflow examples list
desflow examples
```

`desflow examples get` downloads one example at the release you installed, into a folder named after it:

```bash
desflow examples get experiment --api yaml
```

```text
wrote experiment/yaml/experiment.yaml
wrote experiment/yaml/experiment_shop.yaml
wrote experiment/observed_waits.csv

Find the bottleneck, with no run: cd experiment/yaml && desflow explain experiment_shop.yaml
Run the yaml version: cd experiment/yaml && desflow experiment experiment.yaml
```

The model is `experiment_shop.yaml`, and `experiment.yaml` is the experiment that compares two designs of it. `observed_waits.csv` holds the mean wait measured in the real shop. The line before the last runs `desflow explain` on the model, and the last line is the command that runs the example from its own folder. `--api builder` or `--api low-level` downloads the same example in Python.

`--dest` downloads into another folder, and `--ref` downloads from another tag or branch, such as `main` for an example added after your release:

```bash
desflow examples get experiment --api builder --dest shop --ref main
```

```text
wrote shop/experiment/builder/experiment_example.py
wrote shop/experiment/observed_waits.csv

Run the builder version: cd shop/experiment/builder && python experiment_example.py
```

The builder version goes under `shop/experiment/builder`, and the last line runs it from there. A file that is already there stops the download before anything is written, and `--force` replaces it instead. This line downloads the YAML version again, over the files of the first download, and prints the same lines as the first download:

```bash
desflow examples get experiment --api yaml --force
```

### Check It Before a Long Run

`desflow explain` reads the model without running it. It draws the model as a graph and works out each resource's [utilisation](https://jaehyeon.me/desflow/latest/des/queueing-basics/#utilisation), the share of its units that the arrivals keep busy:

```bash
desflow explain experiment/yaml/experiment_shop.yaml
```

The same output as text:

```text
Shop
`-- orders (arrival, rate 0.4/s)
    `-- spawn -> pack (task, rate 0.4/s)
        +-- uses packers (resource)
        `-- next (share 1) -> exit
Resources
+---------------------------------------------------+
| resource |   λ |    μ | c |   ρ | Wq | L | status |
|----------+-----+------+---+-----+----+---+--------|
| packers  | 0.4 | 0.25 | 2 | 0.8 |  - | - | OK     |
+---------------------------------------------------+
packers by units
+-----------------------+
| c |      ρ | status   |
|---+--------+----------|
| 1 |    1.6 | UNSTABLE |
| 2 |    0.8 | OK       |
| 3 | 0.5333 | OK       |
| 4 |    0.4 | OK       |
+-----------------------+
Bottleneck: packers
Not estimated:
  wait at packers: no closed formula for c > 1 with non-exponential times; run --verify to measure it
Hints for utilisation 0.85:
  packers with 2 units: utilisation 0.8, at most 0.85
  packers with 2 units: utilisation 0.85 at a request every 2.35 seconds (25.5 a minute, 0.425 a second)
```

The packers are at utilisation 0.8: they are busy 80% of the time. An order arrives every 2.5 seconds on average (24 a minute, `rate 0.4/s` in the graph). Each order needs 4 seconds of a packer, and 2 packers share the work. The resources table names its columns with the usual queueing symbols:

- λ (lambda) is the arrival rate, 0.4 orders a second.
- μ (mu) is the rate one packer serves, 0.25 orders a second (4 seconds each).
- ρ (rho) is the utilisation.
- Wq is the mean wait in the queue, and L the mean number of orders in the shop. Both show `-` when no formula applies.

The status is `OK` below 0.85, `CONGESTED` from 0.85 and `UNSTABLE` from 1, where the queue grows for as long as the run lasts. The table by units shows that one packer would be `UNSTABLE`. The hints give the units and the arrival rate that reach the target of 0.85. A hint of more units than the resource's `max_cap` also says to raise `max_cap`, because a `current_cap` above `max_cap` is lowered to it.

The formulas give no wait for two packers with gamma packing times, so the line under `Not estimated` says to measure it. [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/#explain-the-load) works the same numbers out by hand.

`--verify` measures it. It runs the model unpaced and compares each statistic with the analysis, as [Check Against Theory](https://jaehyeon.me/desflow/latest/guides/check-against-theory/index.md) shows. With `--replications`, it runs the model that many times from `--seed`, each to `--until`, and compares with each measure's 95% interval. `--target` sets the utilisation the hints aim for:

```bash
# Ten one-hour runs compared with the analysis, and hints for utilisation 0.9
desflow explain experiment/yaml/experiment_shop.yaml --verify --replications 10 --seed 42 --until 1h --target 0.9
```

```text
Shop
`-- orders (arrival, rate 0.4/s)
    `-- spawn -> pack (task, rate 0.4/s)
        +-- uses packers (resource)
        `-- next (share 1) -> exit
Resources
+---------------------------------------------------+
| resource |   λ |    μ | c |   ρ | Wq | L | status |
|----------+-----+------+---+-----+----+---+--------|
| packers  | 0.4 | 0.25 | 2 | 0.8 |  - | - | OK     |
+---------------------------------------------------+
packers by units
+-----------------------+
| c |      ρ | status   |
|---+--------+----------|
| 1 |    1.6 | UNSTABLE |
| 2 |    0.8 | OK       |
| 3 | 0.5333 | OK       |
| 4 |    0.4 | OK       |
+-----------------------+
Bottleneck: packers
Not estimated:
  wait at packers: no closed formula for c > 1 with non-exponential times
Hints for utilisation 0.9:
  packers with 2 units: utilisation 0.8, at most 0.9
  packers with 2 units: utilisation 0.9 at a request every 2.22 seconds (27 a minute, 0.45 a second)
Verify: 10 runs to 3600 s, seed 42
+------------------------------------------------------------------------+
| node    | measure      | analysis | measured |        ± | result       |
|---------+--------------+----------+----------+----------+--------------|
| packers | utilization  |      0.8 |   0.7953 |  0.01561 | agrees       |
| packers | queue_length |        - |    1.823 |   0.2856 | not compared |
| pack    | wait         |        - |    4.505 |   0.6483 | not compared |
| orders  | throughput   |      0.4 |   0.4002 | 0.007201 | agrees       |
+------------------------------------------------------------------------+
Not compared:
  packers queue_length: no closed formula for c > 1 with non-exponential times
  pack wait: no closed formula for c > 1 with non-exponential times
--verify checks the implementation against the queueing analysis where it applies; it does not show that the model is close enough to the real system
```

The measured utilisation of 0.7953 and throughput of 0.4002 agree with the analysis, so the model does what the formulas say. The wait has no formula here, so the runs only measure it: 4.505 seconds. With a target of 0.9, the hints say that two packers reach it at an order every 2.22 seconds. `--json` prints the same explanation as one JSON object, for a script to read. Its `schema_version` names the shape, which the [`Explanation`](https://jaehyeon.me/desflow/latest/api/#explaining-a-model) class describes:

```bash
desflow explain experiment/yaml/experiment_shop.yaml --json > explain.json
```

### Run It

`desflow run --sample` prints the first events as JSON lines. It runs the model unpaced, with no ingress, and with no sinks of the file's own, so a model that writes to Kafka or a database needs no service:

```bash
desflow run experiment/yaml/experiment_shop.yaml --sample 5
```

```text
{"stream_type":"event","sim_ts":3.19,"timestamp":"...","key":"orders-1","task":"pack","instance":0,"value":{"path_id":"Shop.service.packing","status":"queued"}}
{"stream_type":"event","sim_ts":3.19,"timestamp":"...","key":"orders-1","task":"pack","instance":0,"value":{"path_id":"Shop.service.packing","status":"started"}}
{"stream_type":"event","sim_ts":3.37,"timestamp":"...","key":"orders-2","task":"pack","instance":1,"value":{"path_id":"Shop.service.packing","status":"queued"}}
{"stream_type":"event","sim_ts":3.37,"timestamp":"...","key":"orders-2","task":"pack","instance":1,"value":{"path_id":"Shop.service.packing","status":"started"}}
{"stream_type":"event","sim_ts":4.672,"timestamp":"...","key":"orders-3","task":"pack","instance":2,"value":{"path_id":"Shop.service.packing","status":"queued"}}
```

Each order publishes `queued` when it joins the packers' queue and `started` when a packer takes it. The first two orders find a packer free, so each starts at the moment it queues. Without `--sample`, `desflow run` runs the model with its own settings and sinks.

One run is one sample, so measure the wait with [replications](https://jaehyeon.me/desflow/latest/user-guide/replications/index.md): the same model with ten seeds, each run for one hour:

```bash
desflow run experiment/yaml/experiment_shop.yaml --replications 10 --seed 42 --until 1h
```

```text
measure                                n          mean    half-width (95%)
resources.packers.capacity            10             2                   0
resources.packers.in_use              10           1.8              0.3016
resources.packers.queue_length        10             4               3.422
resources.packers.utilization         10            90               15.08
resources.packers.utilization_avg     10         79.53               1.561
resources.packers.queue_length_avg    10         1.823              0.2856
tasks.pack.in_progress                10           5.8               3.566
tasks.pack.finished                   10          1441               25.92
tasks.pack.wait_mean                  10         4.505              0.6483
tasks.pack.wait_max                   10         28.84               4.347
tasks.pack.service_mean               10         3.972             0.04102
tasks.pack.service_max                10         14.64              0.8328
flows.orders.finished                 10          1441               25.92
flows.orders.started                  10          1446               26.78
flows.orders.in_system                10           5.8               3.566

Seed 42; replication seeds: 2684470948, 4091952314, 233227757, 3276785861, 3644269654, 1206282609, 3543069911, 3479688010, 877132087, 1244265337
Repeat one replication with: desflow run experiment/yaml/experiment_shop.yaml --seed <its seed> --until 1h
```

The mean wait is 4.505 seconds, and its [half-width](https://jaehyeon.me/desflow/latest/des/output-analysis/#interval-and-half-width) of 0.6483 says that the true mean lies within about 0.65 seconds of it, with 95% confidence. The packers are busy 79.53% of the time, close to the 0.8 that `explain` worked out. The last line repeats any one replication, with its events. This shop has no sinks. A model with a sink, such as `type: Console`, also sends the result to it as telemetry: one reading for each replication and measure. A Console sink therefore prints those lines before the table, as [What the Sinks Receive](https://jaehyeon.me/desflow/latest/user-guide/replications/#what-the-sinks-receive) explains.

`--measure` names a statistic to summarise, and repeats for more. `--level` sets the confidence level of the intervals. `--workers` runs that many replications at once, each in its own process, and gives the same results, because each replication keeps its seed:

```bash
# Two statistics at a 99% level, two replications at a time
desflow run experiment/yaml/experiment_shop.yaml --replications 10 --seed 42 --until 1h --measure tasks.pack.wait_mean --measure resources.packers.utilization_avg --level 0.99 --workers 2
```

```text
measure                               n          mean    half-width (99%)
tasks.pack.wait_mean                 10         4.505              0.9314
resources.packers.utilization_avg    10         79.53               2.243

Seed 42; replication seeds: 2684470948, 4091952314, 233227757, 3276785861, 3644269654, 1206282609, 3543069911, 3479688010, 877132087, 1244265337
Repeat one replication with: desflow run experiment/yaml/experiment_shop.yaml --seed <its seed> --until 1h
```

The means are the same as before. At 99%, the half-width of the mean wait grows from 0.6483 to 0.9314 seconds, because more confidence needs a wider interval.

`--stdout` prints the run's records as JSON lines, instead of writing to the file's sinks. `--drain` stops the arrivals at the end time and runs on until every order has left. This run stops the arrivals at 20 seconds, and its last record is the ninth order packed, at 24.789 seconds:

```bash
# Every record of a 20-second run that drains, as JSON lines in a file
desflow run experiment/yaml/experiment_shop.yaml --seed 42 --until 20 --drain --stdout > records.jsonl
```

### Compare Designs

`desflow experiment` runs an [experiment](https://jaehyeon.me/desflow/latest/user-guide/experiments/index.md): here two packers against three, 20 replications of each with the same seeds, with a check that three packers keep the mean wait under 2 seconds. `--output` names the folder for its files, here `results/packers` in the current folder. Without it, they go to the folder the file's `output` names, beside the file:

```bash
desflow experiment experiment/yaml/experiment.yaml --output results/packers
```

```text
Wrote the artifacts and report.md to results/packers
passed: three: tasks.pack.wait_mean max 2, checked 0.5919
```

The check passed: the upper end of the interval of the mean wait with three packers is 0.5919 seconds, under the bound of 2. The command exits with status 1 when a check fails, so a CI job that runs it fails too. The six files in `results/packers` hold every value, and `report.md` compares the designs, as [Experiment Results](https://jaehyeon.me/desflow/latest/user-guide/experiments/experiment-results/index.md) describes. After editing `experiment.json` by hand, `desflow report` builds the report again from the files, without running anything:

```bash
desflow report results/packers
```

```text
Wrote results/packers/report.md
```

### Fit Measured Times

A packing time in a model comes from measured times. The `fitting` example holds 40 packing times measured at a bench, for a shop of its own, and `desflow fit` fits every distribution family to them:

```bash
desflow examples get fitting --api yaml
desflow fit fitting/packing_times.csv --column packing_seconds --lower 0
```

```text
40 rows: 40 used, 0 missing, 0 invalid
family            status              AIC  KS stat KS p (approx.)  parameters or reason
exponential       rejected                                         its std is 1.5, the data's is 0.4832: more than 25% apart
normal            fitted            59.33    0.157         0.2504  mean=1.5, std=0.483218 (can draw below the lower bound 0)
lognormal         fitted            57.33  0.09406         0.8385  mean=1.50251, std=0.511854
truncated_normal  fitted            59.25   0.1558         0.2579  mean=1.49836, std=0.48579, low=0
constant          skipped                                          the values are not all equal
uniform           fitted            59.45    0.225        0.02923  low=0.7, high=2.7
triangular *      fitted            56.68   0.1258          0.511  low=0.546332, mode=1.2, high=2.83766
gamma             fitted            56.79   0.1157         0.6165  shape=9.55777, scale=0.15694
weibull           fitted            58.99   0.1502         0.2972  shape=3.33522, scale=1.67203
empirical         fitted                                           values: 40 numbers
* best: triangular
```

The first line counts the values. Each row is one family: `fitted` with its [AIC](https://jaehyeon.me/desflow/latest/des/a-simulation-study/#aic-and-the-ks-test), where lower is better, or the reason it was not fitted. The `triangular` has the lowest AIC, 56.68, and the `gamma` is within 0.11 of it, so the data cannot tell the two apart. [Fitting the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/#does-the-choice-matter) checks what the choice changes.

The other options shape the fit, as [Input Fitting](https://jaehyeon.me/desflow/latest/user-guide/input-fitting/index.md) describes:

- `--unit` names the data's unit, `ms`, `s`, `min` or `h`, and the settings are written in seconds.
- `--upper` is the highest plausible value, and a family that can draw above it is marked.
- `--candidates` names the families to try.
- `--bins` offers the empirical distribution as a histogram with that many bins.
- `--min-count` is the fewest values a family is fitted to.
- `--tolerance` is how far a fitted mean and standard deviation may be from the data's, as a share of the data's.
- `--drop-invalid` leaves out values that are not numbers or lie outside the bounds, instead of stopping.
- `--out` writes the best fit as YAML that a blueprint includes, and `--plot` draws the data against it as a PNG.

```bash
# Three families, a histogram of 8 bins, and the best fit written for the blueprint
desflow fit fitting/packing_times.csv --column packing_seconds --unit s --lower 0 --upper 10 --candidates triangular,gamma,empirical --bins 8 --min-count 30 --tolerance 0.2 --drop-invalid --out fitting/yaml/packing_fit.yaml --plot packing_fit.png
```

```text
40 rows: 40 used, 0 missing, 0 invalid
family            status              AIC  KS stat KS p (approx.)  parameters or reason
triangular *      fitted            56.68   0.1258          0.511  low=0.546332, mode=1.2, high=2.83766
gamma             fitted            56.79   0.1157         0.6165  shape=9.55777, scale=0.15694 (can draw above the upper bound 10)
empirical         fitted                                           edges: 9 numbers, counts: 8 numbers
* best: triangular
Wrote fitting/yaml/packing_fit.yaml
Wrote packing_fit.png
```

The `empirical` row is now a histogram of 8 bins, and the `gamma` row is marked because a gamma can draw above 10 seconds. `fitting/yaml/packing_fit.yaml` is the file the fitting example's blueprint includes, so `fitting/yaml/fitting.yaml` now draws its packing times from this triangular.

### Convert Parameters

A gamma takes a `shape` and a `scale`, not a mean and a standard deviation. `desflow dist` converts the measured times' mean of 1.5 seconds and standard deviation of 0.4832 seconds into them:

```bash
desflow dist gamma --mean 1.5 --std 0.4832
```

```text
parameters: shape 9.63671, scale 0.155655
mean 1.5, std 0.4832
variability: low (std / mean 0.322)
with no random generator, every draw is 1.5
YAML: {dist: gamma, shape: 9.63671, scale: 0.155655}
Python: dist="gamma", shape=9.63671, scale=0.155655
```

The shape and scale are close to the gamma that `desflow fit` found from the 40 values. The last two lines are the service in a blueprint and in the builder.

Given a family's own parameters, `desflow dist` converts the other way. Each of these lines prints the family's mean and standard deviation, as in the output above, for each kind of parameter: a rate, a constant's value, a low and a high, and a shape and a scale:

```bash
desflow dist exponential --rate 0.4
desflow dist constant --value 4
desflow dist uniform --low 0.7 --high 2.7
desflow dist gamma --shape 9.63671 --scale 0.155655
```

The shop's arrivals, `rate: 0.4`, are 2.5 seconds apart on average. A triangular also takes a mode, its most likely value. The one `desflow fit` found has a mean of 1.53 seconds, close to the measured 1.5:

```bash
desflow dist triangular --low 0.55 --mode 1.2 --high 2.84
```

```text
parameters: low 0.55, mode 1.2, high 2.84
mean 1.53, std 0.481785
variability: low (std / mean 0.315)
with no random generator, every draw is 1.53
YAML: {dist: triangular, low: 0.55, mode: 1.2, high: 2.84}
Python: dist="triangular", low=0.55, mode=1.2, high=2.84
```

`desflow dist` alone prints a table of every family with its parameters and a one-line summary, and [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/#measured-times) explains the variability line:

```bash
desflow dist
```

### Set Up an Editor

`desflow schema` prints the JSON Schema of a blueprint, for the release you installed. An editor that reads it offers the keys as you type and marks a misspelt one:

```bash
desflow schema > blueprint.schema.json
```

[Set Up Your Editor](https://jaehyeon.me/desflow/latest/guides/set-up-your-editor/index.md) names the file in the editor's settings.

______________________________________________________________________

## How It Works

Each command reads its file and calls the library function a Python script would call. `desflow run` loads the model with `load_context` and runs it once, or calls `replicate` when `--replications` is given. `desflow experiment` calls `run_experiment`, `desflow report` calls `write_report`, and `desflow fit` reads the column and calls `fit_column`. `desflow dist`, `desflow schema` and `desflow examples` read the distribution table, the blueprint schema and the list of examples. A command therefore gives the same result as the Python call with the same arguments.

`desflow explain` takes the most steps. Look at them in order:

**Graph.** `explain` builds the graph from what the model declares: its arrivals, tasks, resources, stores, containers and `next` shares. It builds and runs nothing. Code the model does not declare, such as a hand-written arrival loop or an `add_process` function, becomes an opaque node.

**Rates.** Each task's arrival rate comes from flow balance: the rate from its arrivals plus the share of each task that sends work to it. A `next` function or a hand-written arrival loop makes the rates unknown, because the graph cannot see where the work goes.

**Coverage.** The formulas hold for a plain network of queues:

- Each task holds at most one resource.
- Each queue serves in arrival order, `fifo` or `lifo`, which give the same mean wait.
- Nothing blocks, removes, delays or reorders work by priority.

Anything else marks tasks, and the rate of a marked task is unknown. These mark tasks:

- A task feature, such as `put`, `patience` or `fail`, marks that task and every task after it.
- A resource feature, such as `queue_limit` or breakdowns, marks the tasks that use it and every task after them.
- A priority queue whose tasks have different priorities marks every task after its tasks. The resource keeps its utilisation but has no wait.
- An unstable resource marks every task after its tasks. The resource keeps its status.
- A scenario step, live ingress or an `add_process` function marks every arrival and every task.

A resource used by a marked task has no supported utilisation, status or wait, and the output names the reason under `Not estimated`.

**Waits.** The utilisation ρ is λ ÷ (c × μ): the arrival rate divided by the units times the rate one unit serves. The status follows from it. The wait depends on the units and the input:

- With no unit limit, the wait is 0.
- At one unit, the wait comes from Kingman's formula. It is exact when the input is Poisson, and an approximation when one arrival with other gaps feeds the resource's only task.
- At more than one unit, the wait comes from Erlang C, which needs Poisson arrivals and one exponential service time shared by every task that uses the resource.
- With any other input or times there is no wait, and the output says to run `--verify`.

Little's law then gives the queue length and the number in the system. The bottleneck is the resource with the highest supported utilisation. The hints solve the utilisation alone for the units and the arrival rate that reach `--target`.

**Verify.** `--verify` runs the model as one replication does: unpaced, with no ingress or egress, and with the `typical` statistics. It measures from the end of the warm-up to `--until`, or to the blueprint's `run.until` when `--until` is not given. It compares four measures with the analysis: each resource's utilisation and queue length, the wait of each task that holds one resource, and each arrival's throughput. A measure agrees with the analysis:

- For one run, when it is within 10% of the analysis, or measures 0 where the analysis gives 0.
- For replications, when it is within the 95% interval's half-width or 1%, whichever is larger.

A measure is not compared in these cases:

- The analysis has no value for it.
- Its resource is unstable.
- Its wait comes from Kingman's formula as an approximation.
- For a throughput, the arrival starts no task the graph shows, or its flow passes a marked task or an unstable resource.

______________________________________________________________________

## Stability and Limits

- **Exit statuses.** A command exits with status 0 when it succeeds. `desflow run` and `desflow explain` exit with status 1 for a file that does not load, and print each error with its file and line. `desflow experiment` exits with status 1 when a check fails, after writing every file, and with status 2 when the experiment is invalid. `desflow fit` exits with status 1 when no family is usable. `desflow dist` exits with status 1 for a family with no formula from a mean and a standard deviation, such as `weibull`. A usage error exits with status 2: a missing file or folder, an option value that is not valid such as `--until abc`, an unknown `dist` family or a parameter that breaks its rule, and a `desflow fit` column with a value that is not a number, unless `--drop-invalid` drops it.
- **Files, not Python objects.** `desflow run` reads a blueprint. `desflow explain` also reads a builder Python file that defines a `SimulationContext` at module level. `desflow experiment` reads an experiment file, and `desflow fit` a CSV file. A builder or low-level script runs with Python, and `replicate` and `run_experiment` run its replications and experiments, as [Replications](https://jaehyeon.me/desflow/latest/user-guide/replications/index.md) shows.
- **JSON output.** The shape of `desflow explain --json` is experimental through 1.x, and its `schema_version` changes when the shape does.
- **Examples need the network.** `desflow examples get` downloads from GitHub, at the tag of the installed release. `--ref` names another tag or branch.
