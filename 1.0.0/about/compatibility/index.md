# Compatibility Policy

From 1.0, a model, a script or a pipeline written for one 1.x release runs on every later 1.x release. A 1.x release may add a key, a command, an option, a name or a record field. It does not remove one, and it does not change what one means. Each promise below has a test that fails when a change breaks it, so a release cannot break a promise by mistake. [Upgrading to 1.0](https://jaehyeon.me/desflow/latest/about/upgrading-to-1.0/index.md) lists what changed from 0.28.0 to 1.0.

## What 1.x Keeps

| Promise                                                                                                                                                                                                                                                                                                                                         | What a 1.x release may change                                                                                                        | Test that guards it                                                    |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| The YAML blueprint format: every key, its type and what it means                                                                                                                                                                                                                                                                                | add a key, a value or a form; widen a limit; make a required key optional. Every difference from the 1.0 schema must be one of these | `tests/unit/test_yaml_format_frozen.py`                                |
| The `desflow` command: every command, argument and option, and how a 1.0 command line is read                                                                                                                                                                                                                                                   | add a command, an option that is not required, or a choice; widen a limit. Every 1.0 command line parses to the same values          | `tests/unit/test_cli_surface.py`                                       |
| The public Python API: every name at the import paths listed below, with its signature, including the schema registry serializers and deserializers                                                                                                                                                                                             | add a name, or an argument with a default                                                                                            | `tests/unit/test_public_api.py` and `tests/unit/public_api.txt`        |
| The record format of [Records and Telemetry](https://jaehyeon.me/desflow/latest/reference/records-and-telemetry/index.md): each field with its name, type and position                                                                                                                                                                          | add a field after the last one of its kind, or a record kind that reaches only the sinks that ask for it, as `trace` does            | `tests/unit/test_record_contract.py`                                   |
| The extra names of `pip install "desflow[<extra>]"`: `kafka`, `redis`, `postgres`, `confluent`, `glue`, `parquet`, `iceberg`, `fit`, `watch`, `fake` and `all`                                                                                                                                                                                  | add an extra                                                                                                                         | `tests/unit/test_extras.py`                                            |
| The registry path forms, such as `<simulation>.resources.<name>.current_cap`, that an ingress message, a scenario step and `registry.update` write to                                                                                                                                                                                           | add a form                                                                                                                           | `tests/unit/test_registry_paths.py`                                    |
| The reserved payload keys: a key with two underscores on each side belongs to desflow, a sink drops the keys it does not route by, and `__topic__`, `__table__`, `__path__` and `__stream__` choose the destination, as [Reserved Payload Keys](https://jaehyeon.me/desflow/latest/reference/records-and-telemetry/#reserved-payload-keys) says | add a key                                                                                                                            | `tests/unit/test_record_contract.py` and the routing test of each sink |

The public Python API is every name exported at these import paths:

- `desflow`
- `desflow.values`
- `desflow.blueprint`
- `desflow.analysis`
- `desflow.explain`
- `desflow.models`
- `desflow.resources`
- the connector modules under `desflow.connectors`, such as `desflow.connectors.egress.kafka` and `desflow.connectors.ingress.file`

The [API Reference](https://jaehyeon.me/desflow/latest/reference/api/index.md) shows each name at its path. A name imported from a module inside a package, such as `desflow.builder.context` or `desflow.blueprint.build`, works in 1.0 and may stop working in a later 1.x release, so import it from the path the API Reference shows.

## What Is Not Covered

- **Seeded output.** The records of a seeded run can change between 1.x releases, such as when a fix changes a draw or the order of events at one instant. Such a change is never made without a note, as the next section says.
- **`desflow explain --json`.** Its shape can change in 1.x, and its `schema_version` changes with it.
- **A form marked experimental.** A key or a command added in 1.x while it is marked experimental can change until a release removes the mark.
- **A Python version.** A 1.x minor release may drop one, as [Supported Versions and Platforms](#supported-versions-and-platforms) says.
- **Simulation server.** The server, its MCP tools and its `pattern` blueprint block ship after 1.0 behind the `server` extra and start as experimental. Its REST API under `/api/v1` only adds once it is released.

## No Silent Change to Results

A change to the result of a valid blueprint is a break, even where seeded output is not promised. The release lists it under Upgrading in the [Changelog](https://jaehyeon.me/desflow/latest/about/changelog/index.md), with its reason. The `regression` job of the CI pipeline runs every example, conformance blueprint and regression scenario on the last release from PyPI and on each new commit, and fails on a difference that `tests/regression/expected.yaml` does not list with the release that made it. From 1.0 it also runs the files of `v1.0.0` on desflow 1.0.0 and on the new commit, so an edit to an example cannot hide a change in what a 1.0 blueprint does.

## Deprecation

A name, key or option that a 1.x release replaces keeps working with a `FutureWarning` for at least one minor release. The warning names its replacement. It is removed only in the next major version, 2.0, and the Upgrading section of that release says what to change.

## Breaks After 1.0

A break is made only in a major version. It needs a stated reason and an Upgrading line with the old behaviour, the new behaviour and what to change. Where it is cheap, the release also adds a setting that keeps the old rule for one major version.

## Supported Versions and Platforms

Fixes, including security fixes, ship in 1.x releases, and a release before 1.0 gets none. [SECURITY.md](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0/SECURITY.md) says how to report a vulnerability. 1.0 supports Python 3.11 to 3.15, the versions the CI pipeline tests. A 1.x minor release may drop a Python version that has reached its end of life, or that a dependency no longer supports. That is not a break: the release raises `requires-python`, so pip installs the last release that supports the older Python.

| Platform             | What is supported                                                                                           |
| -------------------- | ----------------------------------------------------------------------------------------------------------- |
| Linux                | every feature, including the examples that run services with [odctl](https://github.com/jaehyeon-kim/odctl) |
| Windows through WSL2 | the same as Linux, and the recommended way to run desflow on Windows                                        |
| macOS                | the core library and the command                                                                            |
| Windows              | the core library and the command; the service stacks of the examples are not supported                      |

## Older Docs

The docs of each published release stay on the site under its version. A version other than the latest shows a banner that links to the latest. The docs of 0.20.0 to 0.28.0 show no banner, because each was built with the templates of its release, before the banner existed. Those releases came before this policy, so their pages promise no compatibility.
