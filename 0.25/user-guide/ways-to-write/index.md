# Ways to Write a Simulation

desflow has three ways to write a simulation: a YAML blueprint, the builder API and the low-level API. This page says how they relate and which to choose. If you are new to desflow, start with [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/index.md), which shows the same model in all three APIs side by side. Return here to look up a specific component.

______________________________________________________________________

## What It Is

The three are layers, not alternatives. Each layer writes the one below it for you:

- **YAML blueprint:** a file. `SimulationContext.from_yaml`, which `desflow run` calls, reads it and makes the same builder calls a Python script would make, so the result is an ordinary `SimulationContext`.
- **Builder API:** `SimulationContext` collects resources, distributions, tasks and connectors through its methods and decorators. Its `run()` compiles them into a `SimParameter`, builds the environment, registers the parameters and starts the processes.
- **Low-level API:** you write that last step by hand: a `SimParameter`, a `DynamicRealtimeEnvironment` that registers it, and SimPy processes that use the environment's resources and publish records.

Because each layer ends in the one below, the layers mix: a blueprint can reference Python written for the builder with `!python`, and a builder process can use the environment directly.

______________________________________________________________________

## How to Use It

|                          | YAML Blueprint                                                                                           | Builder API                                                                                            | Low-level API                                                        |
| ------------------------ | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------- |
| **Entry point**          | `desflow run` or `SimulationContext.from_yaml`                                                           | `SimulationContext`                                                                                    | `DynamicRealtimeEnvironment`                                         |
| **What you write**       | the simulation, declared in a file; Python only for logic YAML cannot express, referenced with `!python` | what the system looks like, with decorators for each task's steps                                      | how every event and resource operates, step by step                  |
| **What it does for you** | everything the builder does, and no Python to run                                                        | publishing each task's events, requesting resources, sampling times, routing between tasks, statistics | nothing: you queue, start, time out and release yourself             |
| **Typical use**          | changing rates, capacities, connectors and timed experiments between runs without editing code           | models whose logic is most of the program, and configuration built in code, for example from a loop    | what the builder does not do, such as resources created during a run |

### Which to Choose

- **YAML blueprint** when the configuration is what changes between runs: a new arrival rate, another packer, a different broker, a scripted experiment. A file is easy to review and to diff, a run is one command, and a blueprint has a scenario of timed changes on the simulation clock built in. Logic that YAML cannot express goes in a Python module beside the file, as [Call Python from YAML](https://jaehyeon.me/desflow/latest/guides/yaml-advanced/index.md) shows.
- **Builder API** when most of the program is logic and you want it in one Python file, or when you build the configuration in code.
- **Low-level API** when you need what the builder does not do, or full control of every event.

[YAML Blueprints](https://jaehyeon.me/desflow/latest/user-guide/blueprints/index.md), [Builder API](https://jaehyeon.me/desflow/latest/user-guide/builder-api/index.md) and [Low-level API](https://jaehyeon.me/desflow/latest/user-guide/low-level-api/index.md), the next three pages, describe each way in full.

______________________________________________________________________

## How It Works

Whichever way a model is written, it builds the same environment, and one run goes the same way from there. [How desflow Works](https://jaehyeon.me/desflow/latest/user-guide/how-desflow-works/index.md) shows that run and each part it passes through.
