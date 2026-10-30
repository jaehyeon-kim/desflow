# Model Parts

A model is made of a few kinds of parts. This page is the map: which block models what, and what it is called in each way of writing. Each block has a page of its own.

______________________________________________________________________

## What It Is

An **entity** is the thing that flows through a model: an order, a visitor, a support call. It enters through an arrival and does the work its task describes. While it works, it holds a resource, and it may take from or add to a store or container. Then it goes on to the next task. [What Is DES?](https://jaehyeon.me/desflow/latest/learn/what-is-des/index.md) explains discrete-event simulation, the kind of simulation these blocks build. Look at how the blocks connect:

| Block      | What it models                                                    | YAML                        | Builder                            | Low-level                                     | Page                                                                                                               |
| ---------- | ----------------------------------------------------------------- | --------------------------- | ---------------------------------- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Arrival    | how often an entity enters, and what it starts with               | `arrivals`                  | `add_arrival`, `@app.arrival_loop` | a process that waits for a drawn gap          | [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/index.md)  |
| Service    | how long a piece of work takes                                    | `services`                  | `add_service`                      | a `DistributionConfig` drawn with a `Sampler` | [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/index.md)  |
| Task       | what one entity does, with its events                             | `tasks`                     | `@app.task`                        | a SimPy process                               | [Tasks and Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/index.md)                  |
| Resource   | packers, benches, lanes: whole units held by a task               | `resources`                 | `add_resource`                     | `DynamicResource`                             | [Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/index.md)                                      |
| Container  | an amount, such as a stock of boxes, that tasks take and add      | `containers`                | `add_container`                    | `DynamicContainer`                            | [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/index.md)  |
| Store      | items, such as parcels on a shelf, that tasks put and get         | `stores`                    | `add_store`                        | `DynamicStore`                                | [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/index.md)  |
| Task flow  | the task that follows a finished one, and what the entity carries | `next`, `attributes`, `set` | `next=`, `attributes=`, `set=`     | `Router`, `Entity`                            | [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/index.md)                          |
| Process    | anything else that runs over time: a break, a drift, a schedule   | `processes`                 | `add_process`                      | `env.process`                                 | [Tasks and Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/index.md)                  |
| Statistics | what the run measures about itself                                | `stats`                     | `with_stats`                       | `env.setup_stats`, `env.stats`                | [Statistics](https://jaehyeon.me/desflow/latest/user-guide/results/statistics/index.md)                            |
| Variable   | any other live value, such as a speed                             | `variables`                 | `add_variable`                     | `SimParameter(variables=...)`                 | [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/live-parameters/index.md) |

The [shop tutorial](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md), in the Learn section, puts them together one at a time, in a small shop.

______________________________________________________________________

## How to Use It

A model declares its blocks, and its tasks use them by name. A task names its service and its resource, and an arrival names the task it starts. The table above gives each block's YAML key, builder call and low-level class. [Writing a Model](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/index.md) shows a whole model in each form.

______________________________________________________________________

## How It Works

When the run starts, `run()` builds the environment, registers every block's parameters in the registry, builds the resources, stores and containers, and starts the arrivals and processes. [What `run()` Does](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#what-run-does) lists the steps in order. From then on, each block reads its live values from the registry: a distribution reads its parameters at each draw, and a resource follows its capacity path.
