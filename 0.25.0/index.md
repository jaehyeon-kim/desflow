# desflow

**Realistic event streams from a simulation you can steer while it runs.**

Most test data generators write random rows. desflow writes the events of a working process: orders queue for packers, waits grow at busy times, and a backlog builds when orders arrive faster than the packers can pack them. The data comes from a [SimPy](https://simpy.readthedocs.io/) discrete-event simulation, so each change in it has a cause in the model, as in a real system. [Why desflow](https://jaehyeon.me/desflow/latest/about/why-desflow/index.md) explains why it was built and what it is for.

You can change an arrival rate or a capacity while the simulation runs, from **Kafka**, **Redis**, **PostgreSQL**, a local schedule or a timed scenario, and the pipeline downstream sees the effect. One run can write a week of history at full speed, then keep streaming in real time.

Events and telemetry go to **Kafka**, **Redis**, **PostgreSQL**, **Parquet** or **JSONL** files (on local disk or S3-compatible storage), or **Apache Iceberg** tables. A model is a YAML file, a Python builder, or SimPy code.

**Used for:** testing streaming pipelines with realistic data, running demos and workshops, building digital twins that follow live data, generating training data for models that predict delays or load, and evaluating agents and detectors against ground truth. A seeded run can plant a fault and record what it planted as a label record. A detector's or an agent's answer is then scored against that known cause.

The three ways to write a model build one real-time run. Ingress connectors change its live parameters while it runs, and egress connectors write its events and telemetry:

The control dashboard of the [Kafka Digital Twin](https://jaehyeon.me/desflow/latest/examples/kafka/index.md) example changes simulation parameters during a run, and the telemetry shows the effect without a restart. The example's Quick Start runs the dashboard beside the simulation:

______________________________________________________________________

## Key Features

- **Real time or full speed**: `DynamicRealtimeEnvironment` runs SimPy in step with the system clock, or as fast as the machine allows, and reports how far the simulation lags behind real time.
- **Live parameters**: arrival rates, service times and capacities change mid-run through named parameters (registry paths) such as `Shop.arrival.orders.rate`. A resource grows at once. It shrinks only as busy units are released, so no work in progress is lost.
- **Several sinks per run**: each sink added with `add_egress` has its own `when` filter, `batch_size` and `flush_interval`, so one run can feed a stream and a data lake together.
- **Backfill then go live**: one run generates backdated history at full speed, then switches to real time at `go_live_at`.
- **Three ways to write a simulation**: the low-level `DynamicRealtimeEnvironment`, the `SimulationContext` builder, or a YAML blueprint run with `desflow run`.
- **Check before a long run**: `desflow explain` works out each resource's utilisation and waits from the model, without running it, and `--verify` compares them with runs.
- **Replications and experiments**: `desflow run --replications` gives each statistic with a 95% interval, and `desflow experiment` compares designs seed by seed and writes a report.
- **Inputs from data**: `desflow fit` fits every distribution family to measured times, and `desflow dist` turns a mean and a standard deviation into a family's parameters.
- **Serialisation**: records are JSON through `orjson` by default, or Avro through the Confluent or AWS Glue Schema Registry for chosen topics. A Pydantic model can be published as it is.
- **Kafka security**: extra keyword arguments go to the Kafka client, so SASL, mTLS, OAuth and AWS IAM clusters work.

## Commands

`desflow` is the command that comes with the package. It runs a blueprint, and checks, compares and fits models without a line of Python. [Command Line](https://jaehyeon.me/desflow/latest/user-guide/command-line/index.md) uses every command on one shop.

| Command              | What you use it for                                                         | How to use it                                                                                                        | Reference                                                                                                                                                                                  |
| -------------------- | --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `desflow examples`   | list the examples, and download one with `get`                              | [Get an Example](https://jaehyeon.me/desflow/latest/user-guide/command-line/#get-an-example)                         | [`desflow examples`](https://jaehyeon.me/desflow/latest/reference/cli/examples/index.md), [`desflow examples get`](https://jaehyeon.me/desflow/latest/reference/cli/examples-get/index.md) |
| `desflow explain`    | check a model for bottlenecks before a long run                             | [Check It Before a Long Run](https://jaehyeon.me/desflow/latest/user-guide/command-line/#check-it-before-a-long-run) | [`desflow explain`](https://jaehyeon.me/desflow/latest/reference/cli/explain/index.md)                                                                                                     |
| `desflow run`        | run a blueprint, or its replications with a 95% interval for each statistic | [Run It](https://jaehyeon.me/desflow/latest/user-guide/command-line/#run-it)                                         | [`desflow run`](https://jaehyeon.me/desflow/latest/reference/cli/run/index.md)                                                                                                             |
| `desflow experiment` | compare designs of a model and check them against bounds                    | [Compare Designs](https://jaehyeon.me/desflow/latest/user-guide/command-line/#compare-designs)                       | [`desflow experiment`](https://jaehyeon.me/desflow/latest/reference/cli/experiment/index.md)                                                                                               |
| `desflow report`     | build an experiment's report again from its files                           | [Compare Designs](https://jaehyeon.me/desflow/latest/user-guide/command-line/#compare-designs)                       | [`desflow report`](https://jaehyeon.me/desflow/latest/reference/cli/report/index.md)                                                                                                       |
| `desflow fit`        | fit every distribution family to measured times                             | [Fit Measured Times](https://jaehyeon.me/desflow/latest/user-guide/command-line/#fit-measured-times)                 | [`desflow fit`](https://jaehyeon.me/desflow/latest/reference/cli/fit/index.md)                                                                                                             |
| `desflow dist`       | turn a mean and a standard deviation into a family's parameters, and back   | [Convert Parameters](https://jaehyeon.me/desflow/latest/user-guide/command-line/#convert-parameters)                 | [`desflow dist`](https://jaehyeon.me/desflow/latest/reference/cli/dist/index.md)                                                                                                           |
| `desflow schema`     | print the blueprint JSON Schema, for an editor                              | [Set Up an Editor](https://jaehyeon.me/desflow/latest/user-guide/command-line/#set-up-an-editor)                     | [`desflow schema`](https://jaehyeon.me/desflow/latest/reference/cli/schema/index.md)                                                                                                       |

## Where to Start

| To                                                             | Start with                                                                                                                                                                         |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Learn why desflow exists and what it is for                    | [Why desflow](https://jaehyeon.me/desflow/latest/about/why-desflow/index.md)                                                                                                       |
| Build a first model and run it                                 | [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/index.md)                                                                                         |
| Find how many servers keep a queue short                       | [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md)                                                                                   |
| Stream events to Kafka                                         | [Kafka Digital Twin](https://jaehyeon.me/desflow/latest/examples/kafka/index.md)                                                                                                   |
| Write days of history to Parquet or Iceberg at full speed      | [Fast-Forward to Parquet](https://jaehyeon.me/desflow/latest/examples/parquet/index.md) or [Fast-Forward to Iceberg](https://jaehyeon.me/desflow/latest/examples/iceberg/index.md) |
| Write history, then keep streaming in real time                | [Parquet History, Kafka Live](https://jaehyeon.me/desflow/latest/examples/backfill-live/index.md)                                                                                  |
| Change parameters at set times during a run                    | [Changing Demand](https://jaehyeon.me/desflow/latest/tutorials/changing-demand/index.md)                                                                                           |
| Change parameters from Kafka, Redis or PostgreSQL during a run | [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-parameters/index.md)                                                                                          |
| Compare two designs and say which is better                    | [Comparing Designs](https://jaehyeon.me/desflow/latest/tutorials/comparing-designs/index.md)                                                                                       |
| Fit service times to measured data                             | [Fitting the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/index.md)                                                                                         |
| Model customers who give up waiting                            | [Patience](https://jaehyeon.me/desflow/latest/tutorials/patience/index.md)                                                                                                         |
| Model machines that break down                                 | [Model Breakdowns](https://jaehyeon.me/desflow/latest/guides/breakdowns/index.md)                                                                                                  |
| Look up a YAML key or a builder call                           | [Cheat Sheet](https://jaehyeon.me/desflow/latest/reference/cheat-sheet/index.md)                                                                                                   |
