# How desflow Works

This page shows how desflow is built: the parts that run a model, why each one exists, and how a run moves through them. Each part links to the How It Works section that describes it in full.

One run moves through the parts of desflow like this:

Whichever way a model is written, it builds one environment, and one run goes the same way from there. The registry paths, the [records](https://jaehyeon.me/desflow/latest/reference/records-and-telemetry/index.md), the batching and the connectors are therefore the same in all three ways. [How It Works](#how-it-works) follows a model from its file or script to the environment.

______________________________________________________________________

## What It Is

desflow is built on [SimPy](https://simpy.readthedocs.io/). SimPy gives the event list, the clock, processes and resources, and desflow keeps all of them. SimPy leaves seven things to the modeller, and each part of desflow adds one of them:

| Left to the modeller in SimPy                                                                       | What desflow adds                                                                      | Read                                                                                                                    |
| --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| the code around a model: a class of parameters, an entity class, a model class and a loop over runs | three ways to write a model, each built on the one below                               | [Writing a Model](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/index.md)                               |
| one pacing `factor`, fixed when the environment is made                                             | one environment that runs history at full speed, then switches to the wall clock       | [Time and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#how-it-works)                          |
| random times, drawn by the model's own code                                                         | ten checked distributions, each with a random stream of its own                        | [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/#how-it-works)  |
| parameters that only the model's code changes                                                       | a registry of named paths that connectors, scenarios and processes change during a run | [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/#how-it-works) |
| resources, stores and containers that keep the capacity they were made with                         | resources, stores and containers whose capacity follows the registry                   | [Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/#how-it-works)                                      |
| output, which the model prints or logs                                                              | records of a fixed shape, written to sinks on a thread of their own                    | [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#how-it-works)                   |
| statistics, replications and confidence intervals                                                   | statistics collected during the run, replications, experiments and `explain`           | [Built-in Statistics](https://jaehyeon.me/desflow/latest/user-guide/results/statistics/#how-it-works)                   |

[Coming from SimPy](https://jaehyeon.me/desflow/latest/simpy/coming-from-simpy/#what-desflow-changes) compares each change with plain SimPy.

______________________________________________________________________

## How It Works

Look first at how a model gets from a file or a script to a run, then at the parts the run holds:

Each of the three ways ends in one `DynamicRealtimeEnvironment`. A YAML file is read by `blueprint`: `loader` reads the file, including `!python` and `!include`, `models` define and check each section, and `build` turns the sections into calls on `SimulationContext`; the arrow labelled calls the builder shows them. A builder script makes those calls itself, with the builder's methods and decorators. A low-level script makes neither: it builds the environment by hand and writes SimPy processes against it. [Writing a Model](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/index.md) helps choose between them.

`SimulationContext` in `core.context` collects what a model declares. `run()` checks the declarations and builds the environment, then starts the model's loops and processes and runs it, in the order [What `run()` Does](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/#what-run-does) lists. The environment holds the parts in the row under it:

- **registry:** holds every live parameter ([Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/#how-it-works)).
- **statistics:** collects the measures of the run while it goes on ([Built-in Statistics](https://jaehyeon.me/desflow/latest/user-guide/results/statistics/#how-it-works)).
- **egress queue:** the records that tasks and processes publish are buffered for each sink, and full batches wait in a queue. `connectors/egress` writes from it, on a thread of its own, and the arrow points the way the records go ([Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#how-it-works)).
- **connectors/ingress:** read parameter changes from outside, on a thread of their own, and update the registry. The arrow points up, because a change reaches the model only through the registry; the environment applies each one from a queue ([Ingress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/ingress/#how-it-works)). [Threads and Queues](#threads-and-queues) shows both queues.

The group at the bottom holds the tools. `cli` is the `desflow` command and calls `loading`, the path by which `desflow run` and `desflow explain` turn a blueprint or a builder file into a context ([Command Line](https://jaehyeon.me/desflow/latest/cli/#how-it-works)). `analysis` runs a model many times for [replications](https://jaehyeon.me/desflow/latest/user-guide/results/replications/#how-it-works) and [experiments](https://jaehyeon.me/desflow/latest/user-guide/results/experiments/#how-it-works). `cli` and `explain` read a blueprint or a builder file; `analysis` also runs a low-level model, as a `LowLevelModel`, from Python. `explain` is in the last row of the table below.

Each part is described in full on its own page. In short:

| Part                             | How it works                                                                                                                                                                                                                | Read                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Writing a model                  | A YAML blueprint, the builder and the low-level API are layers. `from_yaml` makes the calls a builder script makes, and the builder's `run()` makes the calls a low-level script writes by hand.                            | [YAML Blueprints](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/blueprints/#how-it-works), [What `run()` Does](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/#what-run-does)                                                                                                                                                                                                                       |
| Environment and pacing           | `DynamicRealtimeEnvironment` is SimPy's `RealtimeEnvironment` with the registry, the connectors and the statistics added. It runs without waiting at `factor: 0` and switches to real time at `go_live_at`.                 | [Time and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#how-it-works)                                                                                                                                                                                                                                                                                                                                               |
| Random times                     | A distribution reads its parameters at each draw, so a change applies from the next draw. Each one has its own random stream, made from the run's seed and its registry path.                                               | [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/#how-it-works)                                                                                                                                                                                                                                                                                                                       |
| Registry                         | Every live value sits under a dot path, such as `Shop.service.packing.mean`. Each update is checked before it is stored.                                                                                                    | [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/#how-it-works)                                                                                                                                                                                                                                                                                                                      |
| Resources, stores and containers | Each waits on its registry path, so its capacity changes during a run. A resource that shrinks lets its busy units finish.                                                                                                  | [Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/#how-it-works), [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#how-it-works)                                                                                                                                                                                                                                   |
| Threads, ingress and egress      | The SimPy thread runs the model. Ingress and egress run their connectors on threads of their own, each on an asyncio loop, so a network wait never stalls a simulation step.                                                | [Threads and Queues](#threads-and-queues), which also compares these queues with the waiting lines of the model; [Ingress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/ingress/#how-it-works), [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#how-it-works), [Write a Connector](https://jaehyeon.me/desflow/latest/how-to/records-and-kafka/write-a-connector/index.md) |
| Measurement and explain          | One statistics object collects readings while the run goes on. Replications run the model again for each seed and give a Student t interval. `desflow explain` applies queueing formulas to the model before anything runs. | [Built-in Statistics](https://jaehyeon.me/desflow/latest/user-guide/results/statistics/#how-it-works), [Replications](https://jaehyeon.me/desflow/latest/user-guide/results/replications/#how-it-works), [Experiments](https://jaehyeon.me/desflow/latest/user-guide/results/experiments/#how-it-works), [Command Line](https://jaehyeon.me/desflow/latest/cli/#how-it-works)                                                                |

### How a YAML File Becomes Builder Calls

Follow the file down the left column, then look at the group on the right, which joins each section of the file to the builder call it becomes:

- **loader** reads the file, replaces `${VAR}` with an environment variable, and resolves `!python` and `!include` ([YAML Blueprints](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/blueprints/#how-it-works)). It keeps the line each key came from, so an error names the line.
- **models** checks each section and refuses an unknown key, so a misspelt key is reported, not ignored.
- **build** makes one builder call for each entry, so a YAML file and a builder script that make the same calls build the same model. From `SimulationContext` on, a run goes as the next diagram shows.

### How a Builder Script Becomes a Run

Look at the two halves: the top shows where the builder keeps what a script declares, and the group below shows what `run()` does with it, in order. `run()` is `start()`, which makes steps 1 to 4, then `finish()`, which makes step 5; a script that steps through a run itself calls `start()` alone:

1. **checks:** every name a declaration refers to is checked first, and a wrong model is refused with a `ValueError` before anything is built.
1. **keeps the run:** each `start()` makes a new run state, so a second run never starts from what the first left.
1. **builds:** the builder makes the parts a low-level script makes by hand, which the next diagram shows: the environment, its registry and statistics, the connectors and the resources.
1. **starts:** the arrival loops and the scenario run as SimPy processes. Each arrival starts a task instance, `TaskRun` runs it, and `flow` routes its entity to the next task. A task with a `batch` keeps its place in the batch in `batches`. [What `run()` Does](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/#what-run-does) lists these steps.
1. **runs:** the model runs to its `until`, its `stop` or the end of its drain, and the run closes: the last statistics are published and the connectors stop.

### What a Low-Level Script Uses

A low-level script uses the engine directly. Look at the top row for what the script calls, and at the row below it for what the environment holds:

- **environment:** the script creates it, registers its parameters, calls `setup_stats`, `setup_ingress` and `setup_egress`, starts its processes with `env.process`, and calls `run()`.
- **sampler:** draws every random time, with one random stream for each distribution ([Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/#how-it-works)).
- **DynamicResource:** a resource whose capacity follows its registry path ([Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/#how-it-works)).
- **rules:** the checks a blueprint and the builder share; the registry uses one of them to check a capacity before it stores it.
- **expressions, families and replay** serve the builder and YAML: conditions and amounts, resources and signals made from a computed key, and arrivals read from a recorded file.

[ARCHITECTURE.md](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc2/ARCHITECTURE.md) draws the same three diagrams as text, with every module.

______________________________________________________________________

## Threads and Queues

### What the Environment Holds

A `DynamicRealtimeEnvironment` is SimPy's `RealtimeEnvironment` with four additions: the registry at `env.registry`, the ingress connectors, the egress connectors with their buffers, and the statistics at `env.stats`. Whichever way a model is written, `SimulationContext.run()` builds this environment.

### Threads

A run uses three threads, with queues between them. Records flow from the processes to the sinks, and parameter changes flow from the sources to the registry:

- **SimPy thread:** runs the model. When a process publishes a record, the record goes into a buffer for each sink, and a full buffer is handed to that sink's queue.
- **Egress thread:** runs the sinks' asyncio code, which drains the queues and writes to Kafka, files or databases, so network waits never stall a simulation step.
- **Ingress thread:** runs the sources, which put `(path, value)` pairs on a queue.

### Two Kinds of Queue

The word queue means two different things in desflow:

- **A waiting line in the model** holds entities, such as orders waiting for a packer. A resource keeps its line in `queue`, ordered `fifo`, `lifo` or by `priority`, and a request past its `queue_limit` raises `QueueFull`. Stores and containers have lines of puts and gets that wait. These lines are what a study measures: their lengths and waits are in the records and the statistics.
- **A hand-off queue** holds data on its way between the threads above. The ingress queue has no limit, and the ingress monitor applies it. Each sink has its own queue of at most `max_queued_batches` batches. When a sink's queue is full, the simulation waits, so a slow sink slows the run instead of building a backlog. When it stays full for `drain_stall_seconds`, the run stops rather than lose records. These queues change how fast the records arrive, never what they hold.

### Processes the Environment Starts

Besides your own processes, the environment starts these, as events of its own that `step()` and `peek()` do not count:

- an ingress monitor, which applies queued changes every 0.1 simulation seconds;
- a lag monitor, which publishes `system.simulation.lag_seconds` every `lag_interval`, at a `factor` above 0 or after going live;
- a timed flush per sink, at a `factor` above 0;
- the statistics timer;
- the warm-up reset.

They share SimPy's one event queue with the model's events, so `run()` processes them in order with the model's events. A script that steps the model never sees them. `advance()` and `finish()` run them in the same way. An environment subclass that overrides `step()` sees every event inside `run()`, desflow's own included, and only the model's events when a script steps outside `run()`.

### SimPy and desflow

SimPy's `RealtimeEnvironment` takes one `factor` for the whole run, and raises an error when the model falls behind the wall clock. desflow runs without waiting at `factor: 0`, switches to real time at `go_live_at`, and stamps every record with a logical time. A paced run is strict by default, as SimPy's is, and `strict: false` reports falling behind as telemetry instead. [SimPy and desflow](https://jaehyeon.me/desflow/latest/simpy/index.md) compares the two.

______________________________________________________________________

## For Contributors

A contributor who changes desflow itself can read [ARCHITECTURE.md](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc2/ARCHITECTURE.md), which holds the code map and the rules for imports.
