# Live Runs and Connectors

A live run changes its parameters while it runs and sends its records out as they happen. Three parts do this: the registry of live parameters, the ingress connectors that change them, and the egress connectors that write the records. The diagram on [How desflow Works](https://jaehyeon.me/desflow/latest/user-guide/how-desflow-works/index.md) shows how one run moves through them.

______________________________________________________________________

## What It Is

- **Live parameters** sit in the registry under named paths, such as `Shop.service.packing.mean`. An arrival rate, a number of packers or a stock level can change while the run goes on.
- **Ingress** connectors bring those changes in from outside: from Kafka, Redis, PostgreSQL or a local schedule.
- **Egress** connectors, or sinks, write the records the run publishes: to the console, Kafka, Redis, PostgreSQL, Parquet or JSON Lines files, or Iceberg tables.

______________________________________________________________________

## Pages in This Section

- [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/index.md): how to name a parameter, the ways to change it, how to record what a change was for, and what happens to a value once it arrives.
- [Ingress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/ingress/index.md): each source connector, the message it reads, how to send one, and when the change lands.
- [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/index.md): each sink, how to choose which records each one gets, and how records travel from a task to a sink.

______________________________________________________________________

## How to Use It

- A value that should change during a run is a registry path, such as a service's mean or a resource's capacity, or a `variables` entry for any other value.
- An `ingress` entry in YAML, or `add_ingress` in the builder, adds a source of changes.
- An `egress` entry, or `add_egress`, adds a sink.

______________________________________________________________________

## How It Works

A run uses three threads. The ingress thread runs the sources, which put `(path, value)` pairs on a queue, and the ingress monitor applies them to the registry every 0.1 simulation seconds. The SimPy thread runs the model, which follows each change: a distribution uses the new parameters from its next draw, and a resource changes its capacity. Each record the model publishes goes into a buffer for each sink, and the egress thread writes full batches to the sinks. [Time and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#how-it-works) shows the threads and the queues between them.
