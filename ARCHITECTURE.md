# Architecture

This file is for contributors. It shows how the source under `src/desflow` is divided into packages, which package may import which, what is inside `core`, `builder` and `blueprint`, and where each part of the repository lives. [How desflow Works](https://jaehyeon.me/desflow/latest/user-guide/how-desflow-works/) shows the parts of a run for users.

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
                                        (builder.context)                           │
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
- `Python (builder)`: a script that calls `SimulationContext` itself, the builder in `src/desflow/builder/context.py`. `SimulationContext` collects what a model declares, and its `run()` checks the declarations and builds the environment.
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
                             builder
                                  ├───────────┬───────────────┐
                                  ▼           │               │
                               values         │               │
                                  │           │               │
                                  ▼           │               │
                                core ◄────────┘               │
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
   ✗ desflow.builder.context ──► desflow.blueprint.build     SimulationContext.from_yaml
   ✗ desflow.builder.context ──► desflow.explain.analysis    SimulationContext.analyze
   ✗ desflow.builder.context ──► desflow.explain.graph       SimulationContext.analyze
   ✗ desflow.builder.context ──► desflow.explain.verify      SimulationContext.analyze
   ✗ desflow.explain.verify ──► desflow.analysis.intervals   explain --verify
   ✗ desflow.explain.verify ──► desflow.analysis.models      explain --verify
   ✗ desflow.explain.verify ──► desflow.analysis.runner      explain --verify
   ✗ desflow.blueprint.model_check ──► desflow.cli.check     check_model
   ✗ desflow.analysis.runner ──► desflow                     the version number
```

Each name is a package of `src/desflow`:

- `cli`: the `desflow` command, one module for each command. `loading`: `load_context`, which loads a blueprint or a builder file. `analysis`: replications, experiments, reports and fitting. `explain`: the graph of a model and its queueing formulas.
- `blueprint`: the YAML loader, its models and `build`. It is a layer with `explain`, so neither imports the other.
- `builder`: `SimulationContext` and the modules that declare a model, check it and run its tasks. [Inside Builder](#inside-builder) shows them.
- `values`: the `gen` value generators and `Template`. It sits between `builder` and `core`, so it may import `core` and not `builder`.
- `core`: the environment, the registry, the statistics, the sinks and the other modules that hold the clock, the live parameters and the output. [Inside Core](#inside-core) shows them.
- `resources`: `DynamicResource`, `DynamicContainer` and `DynamicStore`, with capacities that follow the registry.
- `models`: parameter configs, distributions, `Entity` and the record schemas.
- `config`, `examples`, `internal` and `utils`: settings, the list of bundled examples, the markers for desflow's own events, and shared helpers. These four are one layer, so none imports another.
- `connectors`: ingress and egress. It imports no other desflow package.

Each line marked `✗` is an import against the layers that the contract allows by name:

- `SimulationContext.from_yaml` and `SimulationContext.analyze` are methods of the builder that load a blueprint and explain the model. Each imports inside the method. `blueprint.build` imports `builder.context`, so a top-level import would be circular, and `explain` is loaded only when `analyze` is called.
- `explain --verify` runs the model to check the formulas, so it needs `analysis`.
- `check_model` in `desflow.blueprint`, the entry the Agent Skill calls, checks a file as `desflow check` does.
- `analysis.runner` reads `desflow.__version__` for `experiment.json`, and the top package imports `loading`, which is above `analysis`.

Each of these imports sits inside a function.

## Inside Core

`core` is the engine, and a low-level script uses it directly. The diagram shows what such a script creates and calls. A word on a line says what is done, and an arrow points at what is created, held or called.

```
                                a low-level script
             │ creates and runs              │ draws times with          │ makes
             ▼                               ▼                           ▼
        environment                       sampler                DynamicResource
 (DynamicRealtimeEnvironment)            (Sampler)            (the resources package)
             │ holds
   ┌─────────┴────────────────┬───────────────────────────┐
   ▼                          ▼                           ▼
registry                  statistics                    sinks
(live parameters)         (measures)                    (one Sink for each egress)
   │ checks a capacity
   ▼ change with
rules

 used by the builder, not by a low-level script: expressions, families, replay
```

- `environment`: `DynamicRealtimeEnvironment`, SimPy's `RealtimeEnvironment` with the registry, the connectors and the statistics added. The script creates it, registers its parameters, calls `setup_stats`, `setup_ingress` and `setup_egress`, starts its own processes with `env.process`, and calls `run()`.
- `sampler`: `Sampler`, which draws every random time, with one random stream for each distribution, so a run with a seed repeats.
- `registry`: the live parameters, each under a dot path. `register_sim_parameter` adds them, and ingress changes them during a run. A write of a capacity is checked with `check_capacity` from `rules`, which also holds the checks that a blueprint and the builder share.
- `statistics`: the measures a run collects about itself, which `setup_stats` starts.
- `sinks`: one `Sink` for each egress that `setup_egress` adds, with its batching, and `Sinks`, what a builder model declares about its egress and ingress.
- `DynamicResource` and the other holders are in the `resources` package, below `core`; their capacities follow the registry.
- `expressions`, `families` and `replay` are used by the builder: conditions and amounts written as expressions, resources, stores, containers and signals made from a computed key, and arrivals read from a recorded file.

## Inside Builder

`builder` is the Builder API. `SimulationContext` collects what a script declares, and `run()` turns it into the low-level parts of [Inside Core](#inside-core), making the calls a low-level script would make by hand. `run()` is `start()`, which makes steps 1 to 4 of the diagram, then `finish()`, which makes step 5; a script that steps through a run itself calls `start()` alone. The arrows point at what is called or started.

```
                                a builder script
                                       │ @app.task, add_arrival, add_resource,
                                       │ add_signal, add_egress and the others
                                       ▼
                          context (SimulationContext)
                                       │ keeps each kind in its own module
          ┌───────────────┬────────────┴───┬────────────────┐
          ▼               ▼                ▼                ▼
        tasks          arrivals         holders          signals
                                           │ starts
                                           ▼
                                       breakdowns

 when the script calls run() (start() does steps 1 to 4, and finish() does step 5):
   1. checks         model_checks ──► task_checks
   2. keeps the run  runs (RunState)
   3. builds         environment, registry, statistics, connectors, resources
   4. starts         the arrival loops and scenario (steps), as SimPy processes
                             │ each arrival starts
                             ▼
                     task (TaskRun) ──► flow (routes the entity on)
                             │ keeps its place in a batch in
                             ▼
                     batches (BatchSeat)
   5. runs           to until, stop or drain, then closes the run
```

- Declaring: `context` holds `SimulationContext`, and it keeps each kind of declaration in its own module: `tasks` (the tasks and the start of their instances), `arrivals` (the arrivals and their loops), `holders` (resources, stores, containers and their breakdowns) and `signals` (the named events that tasks fire and wait for).
- Checking: `model_checks` checks every name a declaration refers to, and refuses the model with a `ValueError` before anything is built. It calls `task_checks` for the tasks, families and rules.
- Keeping the run: `runs` holds `RunState`, made new by each `start()`, so no run starts from what an earlier run left.
- Building: `start()` makes the environment, registers the parameters in its registry, starts the statistics, connects the ingress and egress connectors, and makes the resources, stores and containers.
- Starting: the arrival loops and the scenario run as SimPy processes. `scenario` applies registry changes at set times. Each arrival starts a task instance: `task` holds `TaskRun`, the life of one instance, and `flow` routes its entity to the next task. A task with a `batch` keeps its place in the batch in `batches`: `BatchSeat` is one instance's place, and `OpenBatch` is one batch, with its members in join order, when it closes and which member serves it. `holders` starts `breakdowns`, which fails a resource at random until it is repaired.
- Running: `finish()` runs the model to its `until`, its `stop` or the end of its drain, then closes the run: it publishes the last statistics, flushes and stops the egress connectors, and stops the ingress connectors.

## Inside Blueprint

`blueprint` is the YAML API. It reads a file and builds it through the public builder methods of [Inside Builder](#inside-builder), so a YAML file and a builder script that make the same calls make the same simulation.

```
   a YAML file
        │ read by
        ▼
      loader   (${VAR}, !python, !include)
        │ checked by
        ▼
      models   (refuses an unknown key)
        │ built by
        ▼
      build    (one call for each entry)
        │
        └──► one builder call for each entry:
                tasks ───────► @app.task
                arrivals ────► add_arrival
                resources ───► add_resource
                signals ─────► add_signal
                egress ──────► add_egress
                          │ calls
                          ▼
              SimulationContext (builder.context)

 also in blueprint, for the tools: schema, model_check, expectations
```

- `loader`: reads the file and replaces `${VAR}` with an environment variable, and resolves `!python` (a Python object named by its path) and `!include` (another YAML file). It keeps the line each key came from, for the error messages.
- `models`: the Pydantic models of the file. Each refuses an unknown key, and many values are checked by the same functions the builder uses, in `core/rules.py`.
- `build`: makes one builder call for each entry of the checked file, such as `add_resource` for a resource, `add_arrival` for an arrival and `@app.task` for a task, and returns the `SimulationContext`. From there a run goes as for a builder script.
- `schema`, `model_check` and `expectations` serve tools: the JSON Schema of a blueprint file for editors, `check_model` for the Agent Skill, and `desflow test --fix`, which writes measured statistics into a file's `tests`.

## Rules the Contract Holds

The contract is in `pyproject.toml`, under `[tool.importlinter]`, and `uv run --all-extras lint-imports` checks it in the lint job. Each package imports only the layers below it, as the Packages diagram shows. So `connectors`, at the bottom, imports no other desflow package, and `values` may import `core` but not `builder`.

The contract counts an import made inside a function, so a new import against the layers fails even there. It leaves out an import made only for a type annotation, under `if TYPE_CHECKING`. Each allowed import is an `ignore_imports` entry with a comment that gives its reason, and an entry that matches no import fails the lint, so the list is cut when an import goes. `tests/docs/test_architecture.py` compares the layers and the `✗` lines of the Packages diagram with the contract, and checks that the diagrams of Inside Core, Inside Builder and Inside Blueprint name every module of their package, so a change to one fails until the other matches.

To draw the graph of the imports for yourself, run `uv run --all-extras import-linter drawgraph desflow --show-lazy-imports > desflow.dot`, or name a package such as `desflow.core`. Rendering the file needs [Graphviz](https://graphviz.org/): `dot -Tsvg desflow.dot -o desflow.svg`.

## Code Map

| Path | Contents |
|---|---|
| `src/desflow/config.py` | settings shared by more than one module: queue orders, statistics levels, the `labels` sink value, `MIN_DELAY` (the shortest time a draw gives) |
| `src/desflow/internal.py` | marks the events desflow creates for its own work |
| `src/desflow/builder/` | `context.py` (the `SimulationContext` builder), `runs.py` (`RunState`: the state of one run, and `EndSets`), `arrivals.py` (`Arrivals`: a model's arrivals, and `ArrivalRun`: their loops in one run), `holders.py` (`Holders`: a model's resources, stores, containers and breakdowns, and `HolderRun`: those of one run), `flow.py` (`Router`, `Transfer`, `LiveScope`: what every scope of a run reads), `task.py` (`TaskSpec`, `TaskRun`: the life of one task instance), `batches.py` (`OpenBatch`: one batch of task instances served together, and `BatchSeat`: one instance's place in it), `tasks.py` (`Tasks`: a model's tasks, and the start of their instances in a run), `model_checks.py` (`ModelChecks`: the checks of what a model's declarations name, which `start()` and `analyze()` make before anything is built), `task_checks.py` (the checks of a model's tasks, which `ModelChecks`, `SimulationContext.family_read_problem` and the blueprint loader call), `signals.py` (`Signal`; `Signals`: a model's signals, and `SignalRun`: those of one run), `scenario.py` (`run_scenario`), `breakdowns.py` (`run_breakdowns`) |
| `src/desflow/core/` | `environment.py` (`DynamicRealtimeEnvironment`, egress buffering, record building), `sinks.py` (`Sinks`: a model's egress and ingress providers, their batching and its statistics, with one `Sink` record per egress), `registry.py` (live parameters), `sampler.py` (one random stream per distribution), `expressions.py` (conditions and amounts, and the name of a family's member), `statistics.py` (built-in statistics), `families.py` (`Family`, `Families`: a model's families, `FamilyRun`: their members in one run, `read_place`, which reads a place of a run: a number, an expression or a generator, and `whole`, which takes a count as `range` does: an integer, or a boolean as 1 or 0, and refuses any other value, even `2.0`), `replay.py` (arrivals from a recorded file), `rules.py` (the checks a blueprint and the builder share, among them `check_capacity`, which a live write of a capacity also uses) |
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
| `scripts/` | `gen_pages.py` (the generated Reference pages, run by every docs build: Distributions, Statistic Names, SimPy Compatibility, Generators, and one CLI page per command; a module for each page in `reference_pages/`, among them `generators.py`; Generators is an overview page and one page for each `gen` kind; `python scripts/gen_pages.py` also rewrites the records of `examples/generators/<kind>_output.txt` from the YAML examples), `record_blocks.py` (the record blocks on pages; their generators in `blocks/`, a module for each group of pages), `charts.py` (the charts), `diagrams.py` (the diagram images, and the vhs tapes under `docs/assets/tapes/`), `gen_schema.py` (the blueprint and explain JSON Schemas in the package), `gen_skill.py` (the `skills/desflow/` folder, from `scripts/skill/SKILL.template.md` and the code), `public_api.py` (the snapshot `tests/unit/public_api.txt` that `tests/unit/test_public_api.py` compares with), `release_notes.py`, `deploy_docs.sh` (the docs deploy of `pipeline.yml` and `docs.yml`: builds this version with mike and pushes it to `gh-pages` as `latest`, unless its commit is no longer the head of `main`), `pages_images.py` (run by the docs deploy: stores each image of the docs versions other than `latest` once, in a folder at the root of `gh-pages` that they share, and rewrites their links), `blueprint_layout.py` (writes the YAML blueprint files in one layout, and checks them in pre-commit; see Blueprint Layout), `regression.py` (captures every example, blueprint and regression scenario run to a file, and compares two captures against `tests/regression/expected.yaml`; the `regression` job runs it on every pull request and every push to `main` against the newest `v*` tag, with that release from PyPI) |
| `overrides/` | the `custom_dir` of the docs theme: `main.html` fills Material's `outdated` block, the banner that links a docs version other than `latest` to the latest one |
| `hooks/` | MkDocs hooks: `llmstxt_sections.py` (the llms.txt sections, from the nav), `release_links.py` (rewrites links to `main` to the tag `v<version>`, tested by `tests/docs/test_docs_release_links.py`) |
| `skills/desflow/` | the Agent Skill for AI assistants, generated by `scripts/gen_skill.py`; `tests/docs/test_skill.py` fails when it is out of date |
| `benchmarks/` | `throughput.py` (see issue #24) |
