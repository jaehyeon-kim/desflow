# Architecture

This file is for contributors. It shows how the source under `src/desflow` is divided into packages, which package may import which, how `core` is divided into two layers, and where each part of the repository lives. [How desflow Works](https://jaehyeon.me/desflow/latest/user-guide/how-desflow-works/) shows the parts of a run for users.

## Overview

A model is written in one of three ways. A YAML blueprint is read by `blueprint`, which calls the builder, `SimulationContext` in `core`. A builder script calls the builder itself. A Low-level script writes SimPy processes against the environment of `core`. Each way builds one `DynamicRealtimeEnvironment`, which holds the registry of live parameters and writes records to the sinks. `analysis` runs a model many times, `explain` reads the graph of a model before a run, and `cli` is the `desflow` command over all of them.

## Packages

Each box is a layer, from the top of the package to the bottom. A layer may import any layer below it and none above it. Two packages in one box, separated by `|`, may not import each other. `core` is two layers, with `values` between them.

```
 ┌─ cli ──────────────────────────────────────────────────────────┐
 │ the desflow command; each command imports its package          │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ loading ──────────────────────────────────────────────────────┐
 │ load_context: a blueprint or a builder file to a context       │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ analysis ─────────────────────────────────────────────────────┐
 │ replications, experiments, reports and fitting                 │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ blueprint | explain ──────────────────────────────────────────┐
 │ blueprint: the YAML loader, its models and build               │
 │ explain: the graph of a model and its queueing formulas        │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ core, model layer ────────────────────────────────────────────┐
 │ declares a model and runs its tasks: SimulationContext,        │
 │ TaskRun, flow, arrivals, holders, signals                      │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ values ───────────────────────────────────────────────────────┐
 │ the gen value generators and Template                          │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ core, run layer ──────────────────────────────────────────────┐
 │ the clock, live parameters and output:                         │
 │ DynamicRealtimeEnvironment, the registry, the sampler,         │
 │ statistics, sinks, expressions, families, shared checks        │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ resources ────────────────────────────────────────────────────┐
 │ SimPy's Resource, Store and Container, with live capacity      │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ models ───────────────────────────────────────────────────────┐
 │ parameter configs, distributions, Entity, record schemas       │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ config | examples | internal | utils ─────────────────────────┐
 │ settings, the bundled examples list and shared helpers         │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ connectors ───────────────────────────────────────────────────┐
 │ ingress and egress; imports no other desflow package           │
 └────────────────────────────────────────────────────────────────┘

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
   ✗ desflow.core.families ──► desflow.core.arrivals         inside core
   ✗ desflow.core.registry ──► desflow.core.holders          inside core
```

Each line marked `✗` is an import against the layers that the contract allows by name:

- `SimulationContext.from_yaml` and `SimulationContext.analyze` are methods of the builder that load a blueprint and explain the model. Each imports inside the method. `blueprint.build` imports `core.context`, so a top-level import would be circular, and `explain` is loaded only when `analyze` is called.
- `explain --verify` runs the model to check the formulas, so it needs `analysis`.
- `check_model` in `desflow.blueprint`, the entry the Agent Skill calls, checks a file as `desflow check` does.
- `analysis.runner` reads `desflow.__version__` for `experiment.json`, and the top package imports `loading`, which is above `analysis`.
- The two imports inside `core` go when `core` is split into its two layers. The next section lists them again.

Each of these imports but the one of `families` sits inside a function.

## Inside Core

`core` holds a model layer, which declares a model and runs its tasks, above a run layer, which holds the clock, the live parameters and the output. The modules of one layer may import each other.

```
 ┌─ model layer ──────────────────────────────────────────────────┐
 │ arrivals, breakdowns, context, flow, holders, runs, scenario,  │
 │ signals, task, task_checks, tasks                              │
 └────────────────────────────────┬───────────────────────────────┘
                                  ▼
 ┌─ run layer ────────────────────────────────────────────────────┐
 │ environment, expressions, families, registry, replay, rules,   │
 │ sampler, sinks, statistics                                     │
 └────────────────────────────────────────────────────────────────┘

 Against the layers:
   ✗ desflow.core.families ──► desflow.core.arrivals         at the top of families
   ✗ desflow.core.registry ──► desflow.core.holders          inside a function
```

## Rules the Contracts Hold

The contracts are in `pyproject.toml`, under `[tool.importlinter]`, and `uv run --all-extras lint-imports` checks them in the lint job:

- Each package imports only the layers below it, as the Packages diagram shows. So `connectors`, at the bottom, imports no other desflow package, and `values` may import the run layer of `core` but not its model layer.
- In `core`, the model layer imports the run layer, and not the other way. Every module of `core` is in one of the two layers, so a new module fails the lint until it is placed.

The contracts count an import made inside a function, so a new import against the layers fails even there. They leave out an import made only for a type annotation, under `if TYPE_CHECKING`. Each allowed import is an `ignore_imports` entry with a comment that gives its reason, and an entry that matches no import fails the lint, so the list is cut when an import goes. `tests/docs/test_architecture.py` compares the boxes and the `✗` lines of the two diagrams with the contracts, so a change to one fails until the other matches.

To draw the graph of the imports for yourself, run `uv run --all-extras import-linter drawgraph desflow --show-lazy-imports > desflow.dot`, or name a package such as `desflow.core`. Rendering the file needs [Graphviz](https://graphviz.org/): `dot -Tsvg desflow.dot -o desflow.svg`.

## Code Map

| Path | Contents |
|---|---|
| `src/desflow/config.py` | settings shared by more than one module: queue orders, statistics levels, the `labels` sink value, `MIN_DELAY` (the shortest time a draw gives) |
| `src/desflow/internal.py` | marks the events desflow creates for its own work |
| `src/desflow/core/` | `environment.py` (`DynamicRealtimeEnvironment`, egress buffering, record building), `context.py` (the `SimulationContext` builder), `runs.py` (`RunState`: the state of one run, and `EndSets`), `arrivals.py` (`Arrivals`: a model's arrivals, and `ArrivalRun`: their loops in one run), `holders.py` (`Holders`: a model's resources, stores, containers and breakdowns, and `HolderRun`: those of one run), `sinks.py` (`Sinks`: a model's egress and ingress providers, their batching and its statistics, with one `Sink` record per egress), `registry.py` (live parameters), `sampler.py` (one random stream per distribution), `expressions.py` (conditions and amounts), `flow.py` (`Router`, `Transfer`, `LiveScope`: what every scope of a run reads), `statistics.py` (built-in statistics), `task.py` (`TaskSpec`, `TaskRun`: the life of one task instance), `tasks.py` (`Tasks`: a model's tasks, and the start of their instances in a run), `task_checks.py` (the checks of a model's tasks, made before a run), `families.py` (`Family`, `Families`: a model's families, and `FamilyRun`: their members in one run), `signals.py` (`Signal`; `Signals`: a model's signals, and `SignalRun`: those of one run), `scenario.py` (`run_scenario`), `breakdowns.py` (`run_breakdowns`), `replay.py` (arrivals from a recorded file), `rules.py` (the checks a blueprint and the builder share) |
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
