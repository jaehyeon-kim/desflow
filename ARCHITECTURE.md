# Architecture

This file is for contributors. It shows how the source under `src/desflow` is divided into packages, which package may import which, how `core` is divided into two layers, and where each part of the repository lives. [How desflow Works](https://jaehyeon.me/desflow/latest/user-guide/how-desflow-works/) shows the parts of a run for users.

## Overview

A model is written in one of three ways, and each way builds one `DynamicRealtimeEnvironment`. The diagram follows a model from its file or script to the environment and the parts the environment holds. The word on a line says what it does, and an arrow points the way the call or the data goes.

```
            YAML file                   Python (builder)                   Python (low-level)
                │                               │                                   │
                ▼                               │                                   │
            blueprint                           │                                   │
     (loader, models, build)                    │                                   │
                │ calls the builder             │                                   │
                └───────────────────────────────┤                                   │
                                                ▼                                   │
                                        SimulationContext                           │
                                         (core.context)                             │
                                                │ run() builds                      │ builds by hand
                                                ▼                                   ▼
                          DynamicRealtimeEnvironment (core.environment, a SimPy environment)
                                                │ holds
                ┌───────────────────────────────┼───────────────────────────────────┐
                ▼                               ▼                                   ▼
            registry                       statistics                         egress queue
        (live parameters)                  (measures)                     (batches of records)
                ▲ updates                                                           │ writes
                │                                                                   ▼
       connectors/ingress                                                   connectors/egress
       (its own thread)                                                     (its own thread)

 tools:   cli ──► loading   analysis (many runs)   explain (graph and formulas)
```

- `YAML file`: read by `blueprint` in `src/desflow/blueprint/`. `loader.py` reads the file, `models.py` defines and checks its sections, and `build.py` turns them into builder calls.
- `Python (builder)`: a script that calls `SimulationContext` itself, the builder in `src/desflow/core/context.py`. `SimulationContext` collects what a model declares, and its `run()` checks the declarations and builds the environment.
- `Python (low-level)`: a script that creates the environment by hand and writes SimPy processes against it.
- `DynamicRealtimeEnvironment`: in `src/desflow/core/environment.py`. It is SimPy's `RealtimeEnvironment` with the parts below added.
- `registry`: the live parameters, each under a dot path, in `core/registry.py`. Every change is checked before it is stored.
- `statistics`: the measures a run collects about itself, in `core/statistics.py`.
- `egress queue`: the records a run publishes, buffered for each sink, with full batches waiting in a queue. `core/sinks.py` holds what a model declares about its sinks.
- `connectors/ingress`: read parameter changes from outside, on a thread of their own. The environment applies each change to the registry.
- `connectors/egress`: write the batches to Kafka, files and databases on a thread of their own, so a slow sink never stalls a simulation step.
- The tools: `cli` is the `desflow` command and calls `loading`, whose `load_context` turns a blueprint or a builder file into a context. `analysis` runs a model many times, and `explain` reads the graph of a model and applies queueing formulas before a run. `cli` and `explain` read a blueprint or a builder file. `analysis` also runs a low-level model, as a `LowLevelModel`, from Python.

The Code Map below lists every module.

## Packages

The diagram lists the packages from the top of the source to the bottom, one layer of the layers contract in `pyproject.toml` on each row. `blueprint` and `explain` share a row because they are one layer, and so do `examples`, `internal`, `config` and `utils`. An arrow means that the package above imports the package at its head, and it shows the imports that matter most. A package may import any package below it and none above it, so the diagram can omit an arrow that the order already allows. `cli` also imports the package of each command it runs.

```
                          cli
             ┌─────────────┼──────────────────────────────┐
             ▼             │                              │
          loading          │                              │
             │             ▼                              │
             │          analysis                          │
             ├─────────────┘                              │
             ▼                                            ▼
         blueprint                                     explain
             └────────────────────┬───────────────────────┘
                                  ▼
                          core, model layer
                                  ├───────────┬───────────────┐
                                  ▼           │               │
                               values         │               │
                                  │           │               │
                                  ▼           │               │
                          core, run layer ◄───┘               │
                                  │                           ▼
                                  │                       resources
                                  ▼                           │
                               models ◄───────────────────────┘
                                  │
                                  ▼
        examples    internal    config        utils
                                                │
                                                ▼
                                            connectors

 Against the layers:
   ✗ desflow.core.context ──► desflow.blueprint.build        SimulationContext.from_yaml
   ✗ desflow.core.context ──► desflow.explain.analysis       SimulationContext.analyze
   ✗ desflow.core.context ──► desflow.explain.graph          SimulationContext.analyze
   ✗ desflow.core.context ──► desflow.explain.verify         SimulationContext.analyze
   ✗ desflow.explain.verify ──► desflow.analysis.intervals   explain --verify
   ✗ desflow.explain.verify ──► desflow.analysis.models      explain --verify
   ✗ desflow.explain.verify ──► desflow.analysis.runner      explain --verify
   ✗ desflow.blueprint.model_check ──► desflow.cli.check     check_model
   ✗ desflow.analysis.runner ──► desflow                     the version number
```

Each name is a package of `src/desflow`, or one of the two layers of `core`:

- `cli`: the `desflow` command, one module for each command. `loading`: `load_context`, which loads a blueprint or a builder file. `analysis`: replications, experiments, reports and fitting. `explain`: the graph of a model and its queueing formulas.
- `blueprint`: the YAML loader, its models and `build`. It is a layer with `explain`, so neither imports the other.
- `core, model layer`: `SimulationContext` and the modules that declare a model, check it and run its tasks. `core, run layer`: the environment, the registry, the sinks and the other modules that hold the clock, the live parameters and the output. [Inside Core](#inside-core) lists the modules of both.
- `values`: the `gen` value generators and `Template`. It sits between the two layers of `core`, so it may import the run layer and not the model layer.
- `resources`: `DynamicResource`, `DynamicContainer` and `DynamicStore`, with capacities that follow the registry.
- `models`: parameter configs, distributions, `Entity` and the record schemas.
- `config`, `examples`, `internal` and `utils`: settings, the list of bundled examples, the markers for desflow's own events, and shared helpers. These four are one layer, so none imports another.
- `connectors`: ingress and egress. It imports no other desflow package.

Each line marked `✗` is an import against the layers that the contract allows by name:

- `SimulationContext.from_yaml` and `SimulationContext.analyze` are methods of the builder that load a blueprint and explain the model. Each imports inside the method. `blueprint.build` imports `core.context`, so a top-level import would be circular, and `explain` is loaded only when `analyze` is called.
- `explain --verify` runs the model to check the formulas, so it needs `analysis`.
- `check_model` in `desflow.blueprint`, the entry the Agent Skill calls, checks a file as `desflow check` does.
- `analysis.runner` reads `desflow.__version__` for `experiment.json`, and the top package imports `loading`, which is above `analysis`.

Each of these imports sits inside a function.

## Inside Core

`core` holds a model layer, which declares a model, checks it and runs its tasks, above a run layer, which holds the clock, the live parameters and the output. The modules of one layer may import each other.

An arrow points at a module that the module above it imports. Most names have a short note in brackets.

```
 model layer
                                   context (SimulationContext)
       ┌───────────────┬──────────────┬─────────┼─────────┬───────────────┬───────────────┐
 model_checks        tasks        arrivals      │      holders         signals          runs
(before a run)   (task starts) (arrival loops)  │    (resources)       (fires)       (RunState)
       ▼               ▼                        │         ▼
 task_checks ──► task (TaskRun) ──► flow        │    breakdowns
                 scenario (steps)               │
                                                │ imports
                                                ▼
 run layer                  environment (DynamicRealtimeEnvironment)
                      ┌─────────────────────────┼─────────────────────────┐
                  registry                 statistics                   sinks
                (live params)              (measures)             (egress buffers)

 also in the run layer: expressions, families, rules, sampler, replay
```

The model layer, at the top, holds `context`, which is `SimulationContext`, and the modules that declare and check a model and run its tasks. The run layer, below it, holds `environment`, which is `DynamicRealtimeEnvironment`, and the modules that hold the clock, the live parameters and the output. The line from `context` down to `environment` is the one import from the model layer to the run layer that the diagram names: `run()` builds the environment. Every module of the model layer may import the run layer, and no module of the run layer imports the model layer. The Code Map lists what each module holds, and the notes in brackets give the short form:

- Under `context` are the modules a run uses: `model_checks` checks the declarations of a model before each run, `tasks` starts the instances of its tasks, `arrivals` holds the arrival loops, `holders` holds its resources, stores, containers and breakdowns, `signals` holds the named events that tasks fire and wait for, and `runs` holds `RunState`, the state of one run.
- Three chains sit under the row. `model_checks` imports `task_checks`, which checks the tasks, families and rules and imports `task`. `tasks` and `task_checks` both import `task`, which holds `TaskRun`, the life of one task instance, and `task` imports `flow`, which routes a task's entity to the next task. `holders` imports `breakdowns`, which fails a resource at random until it is repaired.
- `scenario` holds the steps of a scenario, such as a registry change at a set simulation time. `context`, `model_checks` and `tasks` import it; the diagram leaves those arrows out so the lines stay apart.
- Under `environment` are `registry`, the live parameters, `statistics`, the measures of the run, and `sinks`, a model's egress and ingress providers with their batching. The line at the bottom names the rest of the run layer: `expressions` (conditions and amounts), `families` (resources, stores, containers and signals made from a computed key), `rules` (the checks that a blueprint and the builder share), `sampler` (one random stream for each distribution) and `replay` (arrivals read from a recorded file).

## Rules the Contracts Hold

The contracts are in `pyproject.toml`, under `[tool.importlinter]`, and `uv run --all-extras lint-imports` checks them in the lint job:

- Each package imports only the layers below it, as the Packages diagram shows. So `connectors`, at the bottom, imports no other desflow package, and `values` may import the run layer of `core` but not its model layer.
- In `core`, the model layer imports the run layer, and not the other way. Every module of `core` is in one of the two layers, so a new module fails the lint until it is placed.

The contracts count an import made inside a function, so a new import against the layers fails even there. They leave out an import made only for a type annotation, under `if TYPE_CHECKING`. Each allowed import is an `ignore_imports` entry with a comment that gives its reason, and an entry that matches no import fails the lint, so the list is cut when an import goes. `tests/docs/test_architecture.py` compares the layers and the `✗` lines of the Packages and Inside Core diagrams with the contracts, so a change to one fails until the other matches.

To draw the graph of the imports for yourself, run `uv run --all-extras import-linter drawgraph desflow --show-lazy-imports > desflow.dot`, or name a package such as `desflow.core`. Rendering the file needs [Graphviz](https://graphviz.org/): `dot -Tsvg desflow.dot -o desflow.svg`.

## Code Map

| Path | Contents |
|---|---|
| `src/desflow/config.py` | settings shared by more than one module: queue orders, statistics levels, the `labels` sink value, `MIN_DELAY` (the shortest time a draw gives) |
| `src/desflow/internal.py` | marks the events desflow creates for its own work |
| `src/desflow/core/` | `environment.py` (`DynamicRealtimeEnvironment`, egress buffering, record building), `context.py` (the `SimulationContext` builder), `runs.py` (`RunState`: the state of one run, and `EndSets`), `arrivals.py` (`Arrivals`: a model's arrivals, and `ArrivalRun`: their loops in one run), `holders.py` (`Holders`: a model's resources, stores, containers and breakdowns, and `HolderRun`: those of one run), `sinks.py` (`Sinks`: a model's egress and ingress providers, their batching and its statistics, with one `Sink` record per egress), `registry.py` (live parameters), `sampler.py` (one random stream per distribution), `expressions.py` (conditions and amounts, and the name of a family's member), `flow.py` (`Router`, `Transfer`, `LiveScope`: what every scope of a run reads), `statistics.py` (built-in statistics), `task.py` (`TaskSpec`, `TaskRun`: the life of one task instance), `tasks.py` (`Tasks`: a model's tasks, and the start of their instances in a run), `model_checks.py` (`ModelChecks`: the checks of what a model's declarations name, which `start()` and `analyze()` make before anything is built), `task_checks.py` (the checks of a model's tasks, which `ModelChecks`, `SimulationContext.family_read_problem` and the blueprint loader call), `families.py` (`Family`, `Families`: a model's families, `FamilyRun`: their members in one run, `read_place`, which reads a place of a run: a number, an expression or a generator, and `whole`, which takes a count as `range` does: an integer, or a boolean as 1 or 0, and refuses any other value, even `2.0`), `signals.py` (`Signal`; `Signals`: a model's signals, and `SignalRun`: those of one run), `scenario.py` (`run_scenario`), `breakdowns.py` (`run_breakdowns`), `replay.py` (arrivals from a recorded file), `rules.py` (the checks a blueprint and the builder share, among them `check_capacity`, which a live write of a capacity also uses) |
| `src/desflow/models/` | `params.py` (`SimParameter`, `DistributionConfig`, `CapacityConfig`, `ContainerConfig`), `flow.py` (`Entity`, `Upstream`), `schemas.py` (`EventPayload`, `TelemetryPayload`, `LabelPayload`: the published record schema), `distributions.py` (every distribution family, `DISTRIBUTIONS`) |
| `src/desflow/resources/` | `DynamicResource`, `DynamicContainer`, `DynamicStore`, with capacities driven by the registry |
| `src/desflow/connectors/` | `ingress/` (parameter changes in), `egress/` (records out), `admin/`, `_schema_registry/` (the code of the Confluent and Glue serializers and deserializers: the two registry clients, the wire framing, the Avro, JSON Schema and Protobuf codecs, and the errors) |
| `src/desflow/values/` | the `gen` value generators: `models.py` (one model for each `gen`, and the `Value` type), `runtime.py` (`ValueRuntime`, which evaluates them in a run), `template.py` (`Template` and `BoundPayload`, for the builder and Low-level APIs), `check.py` (their checks, made before the run) |
| `src/desflow/blueprint/` | YAML blueprints: `models.py` (Pydantic models), `loader.py` (`!python`, `!include`), `build.py`, `model_check.py` (`check_model`, the entry the Agent Skill calls), `expectations.py` (writes the measured statistics into a file's `tests`, for `desflow test --fix`), `schema.py` (reads `blueprint.schema.json`, the committed blueprint JSON Schema that `scripts/gen_schema.py` writes) |
| `src/desflow/examples.py` | what `desflow examples` lists and downloads; a new example file must be added there, or `test_cli_examples.py` fails |
| `src/desflow/utils.py` | `time_to_seconds` (a time such as `10 min` in seconds) and `suggestion` (the "did you mean" text of an error) |
| `src/desflow/loading.py` | `load_context`: loads a blueprint or a builder Python file, the one path `desflow run` and `desflow explain` use |
| `src/desflow/cli/` | the `desflow` command: `__init__.py` (the Typer app), `_common.py` (logging, the `Error:` exits and `--until`, shared by the commands), then one module per command (`run.py`, `check.py`, `test.py`, `experiment.py`, `report.py`, `fit.py`, `schema.py`, `examples.py`, `dist.py`, `explain.py`), which parses the arguments and imports its package only when the command runs |
| `src/desflow/analysis/` | replications and experiments, which run models of all three APIs unpaced: `models.py` (the one adapter for a blueprint path, a builder factory or a `LowLevelModel`, the seeds of the runs and the reader of a measure from them), `runner.py` (`replicate`, `run_experiment`), `intervals.py` (the Student t quantile, without scipy), `spec.py` (the experiment file), `results.py` (designs, comparisons, checks, the artifact files), `report.py` (`report.md`, built from the artifacts), `fit.py` (`fit_column`, which needs the `fit` extra and imports scipy lazily) |
| `src/desflow/explain/` | what `desflow explain` and `context.analyze()` compute: `models.py` (the `Explanation` shape; `explain.schema.json` beside it is its committed schema, written by `scripts/gen_schema.py`), `graph.py` (`build_graph`), `edges.py` and `features.py` (what it reads from the builder's private state, each module listing the attributes it reads at its top), `arrivals.py` (arrival rates and a distribution's mean and variance), `rates.py` (task rates by flow balance), `poisson.py` (which inputs are Poisson), `formulas.py` (Kingman, Erlang C, the status thresholds), `waits.py` (which formula applies to a resource), `capacity.py` (capacity rows and hints), `loads.py` (each resource's load and the bottleneck), `holders.py` (stores and containers), `support.py` (the one pass that decides which values the formulas cover), `analysis.py` (`analyze`, which combines them), `verify.py` (`--verify`), `pairs.py` (what verify compares, or why not), `compare.py` (whether a measured value differs), `render.py` and `tables.py` (Rich text and JSON) |
| `examples/` | `low-level/` (low-level API), `builder/` (builder), `yaml/` (blueprints), `gallery/` (the Gallery's data entries, each as `<name>.yaml`, `<name>_builder.py` and `<name>_low_level.py`) and `generators/` (three files for each `gen` kind: `<kind>.yaml`, `<kind>_builder.py` and `<kind>_low_level.py`, and the records they print in `<kind>_output.txt`, run by `tests/docs/test_generator_examples.py`) |
| `docs/` | MkDocs Material site, versioned with mike, deployed from `main`. Each folder is a sidebar section, and each subfolder a group in it, so a page's address follows the sidebar: `installation.md` (Installation), `learn/` (Learn: `what-is-des.md`, one folder per tutorial group and `concepts/`), `user-guide/` (User Guide), `cli/` (Command Line), `how-to/` (How-To: one folder per group, `examples/` for the Runnable Examples, and `projects.md`), `simpy/` (SimPy and desflow), `reference/` (Reference) and `about/` (About). A moved page keeps a redirect from its old path in `mkdocs.yml`. The sidebar is at most three levels deep |
| `tests/unit/`, `tests/docs/`, `tests/integration/` | the library's own tests, among them the 1.0 guard tests, which fail on a change that 1.0 does not allow (`test_yaml_format_frozen.py`, `test_record_contract.py`, `test_cli_surface.py`, `test_public_api.py`, which fails on any change to its snapshot, regenerated for an allowed one, and also when a `.py` file under `docs/`, `examples/` or `skills/desflow/`, or a `python` block or sentence of a page under `docs/`, imports a desflow name from a path 1.x does not keep, or when a `:::` line of the API Reference page names an object at another path than its promised one (the snapshot names each object at the first promised path that exports it, `PROMISED_PACKAGES` in `scripts/public_api.py`), `test_extras.py::test_promised_extras_exist` and `test_registry_paths.py::test_path_forms_are_kept`); tests of the docs, snippets, examples and scripts; integration tests against containers that odctl or `docker run` starts |
| `tests/known_cases/` | a folder for each known case (`model.yaml`, `builder.py`, `low_level.py` and `original.py`), and `index.yaml`, which lists every case with its topic; `harness.py` runs them |
| `tests/conformance/` | each operation checked against plain SimPy, the version in `uv.lock`, in the low-level, builder and YAML APIs (`harness.py`, blueprints in `blueprints/`); `register.yaml` lists every operation with its forms, checks and docs page, `simpy_constructs.yaml` lists the SimPy features of the page's rows and their gaps, and `scripts/gen_pages.py` turns both into the SimPy Compatibility page. A new `test_*` function there must be listed in `register.yaml` |
| `tests/regression/` | the regression scenarios: `cases.py` (`SCENARIO_CASES`, the run length and statistics of each), and `scenarios/yaml/`, `builder/` and `low-level/`, five models in the three forms that use only features of the last release; `expected.yaml` lists each difference against that release with its changelog line. Run by `scripts/regression.py` in the `regression` job of `.github/workflows/pull-request.yml`, not by pytest |
| `tests/copied_projects/` | the code and data that `tests/benchtop` and the BT and HR known cases load from other projects, copied unchanged with their source commit in its `README.md`, so no test needs another repository |
| `scripts/` | `gen_pages.py` (the generated Reference pages, run by every docs build: Distributions, Statistic Names, SimPy Compatibility, Generators, and one CLI page per command; a module for each page in `reference_pages/`, among them `generators.py`; Generators is an overview page and one page for each `gen` kind; `python scripts/gen_pages.py` also rewrites the records of `examples/generators/<kind>_output.txt` from the YAML examples), `record_blocks.py` (the record blocks on pages; their generators in `blocks/`, a module for each group of pages), `charts.py` (the charts), `diagrams.py` (the diagram images, and the vhs tapes under `docs/assets/tapes/`), `gen_schema.py` (the blueprint and explain JSON Schemas in the package), `gen_skill.py` (the `skills/desflow/` folder, from `scripts/skill/SKILL.template.md` and the code), `public_api.py` (the snapshot `tests/unit/public_api.txt` that `tests/unit/test_public_api.py` compares with), `release_notes.py`, `blueprint_layout.py` (writes the YAML blueprint files in one layout, and checks them in pre-commit; see Blueprint Layout), `regression.py` (captures every example, blueprint and regression scenario run to a file, and compares two captures against `tests/regression/expected.yaml`; the `regression` job runs it on every pull request and every push to `main` against the newest `v*` tag, with that release from PyPI) |
| `overrides/` | the `custom_dir` of the docs theme: `main.html` fills Material's `outdated` block, the banner that links a docs version other than `latest` to the latest one |
| `hooks/` | MkDocs hooks: `llmstxt_sections.py` (the llms.txt sections, from the nav), `release_links.py` (rewrites links to `main` to the tag `v<version>`, tested by `tests/docs/test_docs_release_links.py`) |
| `skills/desflow/` | the Agent Skill for AI assistants, generated by `scripts/gen_skill.py`; `tests/docs/test_skill.py` fails when it is out of date |
| `benchmarks/` | `throughput.py` (see issue #24) |
