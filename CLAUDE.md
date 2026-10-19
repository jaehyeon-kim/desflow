# desflow

Real-time SimPy simulations whose parameters change while they run, writing events and telemetry to Kafka, Redis, PostgreSQL, Parquet and JSONL files, and Iceberg tables. The public roadmap is `docs/about/roadmap.md`.

## Layout

| Path | Contents |
|---|---|
| `src/desflow/config.py` | settings shared by more than one module: queue orders, statistics levels, the `labels` sink value |
| `src/desflow/core/` | `environment.py` (`DynamicRealtimeEnvironment`, egress buffering, record building), `context.py` (the `SimulationContext` builder), `registry.py` (live parameters), `distributions.py` (every distribution family, `DISTRIBUTIONS`), `sampler.py` (one random stream per distribution), `expressions.py` (conditions and amounts), `flow.py` (`Router`, `Transfer`), `statistics.py` (built-in statistics) |
| `src/desflow/models/` | `params.py` (`SimParameter`, `DistributionConfig`, `CapacityConfig`, `ContainerConfig`), `flow.py` (`Entity`, `Upstream`), `schemas.py` (`EventPayload`, `TelemetryPayload`, `LabelPayload`: the published record schema) |
| `src/desflow/resources/` | `DynamicResource`, `DynamicContainer`, `DynamicStore`, with capacities driven by the registry |
| `src/desflow/connectors/` | `ingress/` (parameter changes in), `egress/` (records out), `admin/` |
| `src/desflow/blueprint/` | YAML blueprints: `models.py` (Pydantic models), `loader.py` (`!python`, `!include`), `build.py`, `cli.py` (`desflow`), `examples.py` (what `desflow examples` lists and downloads; a new example file must be added there, or `test_cli_examples.py` fails) |
| `src/desflow/analysis/` | replications and experiments, which run models of all three APIs unpaced: `models.py` (the one adapter for a blueprint path, a builder factory or a `LowLevelModel`), `runner.py` (`replicate`, `run_experiment`), `intervals.py` (the Student t quantile, without scipy), `spec.py` (the experiment file), `results.py` (designs, comparisons, checks, the artifact files), `report.py` (`report.md`, built from the artifacts), `fit.py` (`fit_column`, which needs the `fit` extra and imports scipy lazily) |
| `src/dynamic_des/` | the import name before the rename to desflow in 0.20.0: warns with a `FutureWarning` and aliases every `dynamic_des.<module>` to `desflow.<module>`; `ddes` in `blueprint/cli.py` is the old command, which warns and runs `desflow`. Both are removed in a later release, named in the Changelog (`docs/about/changelog.md`) |
| `compat/dynamic-des/` | the last `dynamic-des` release: depends on `desflow` with the same extras and declares the `ddes` command, and its sdist copies `src/dynamic_des/__init__.py` and `py.typed` at build time, so a pip upgrade from 0.19.0 keeps both; published by hand with `publish-dynamic-des.yml` |
| `examples/` | `low-level/` (low-level API), `builder/` (builder), `yaml/` (blueprints) |
| `docs/` | MkDocs Material site, versioned with mike, deployed from `main`: `des/` and `tutorials/` (the Learn section), `user-guide/` (Ways to Write, YAML Blueprints, Builder API, Low-level API, then the parts), `guides/` (the How-To section), `examples/`, `reference/`, `about/` |
| `tests/unit/`, `tests/docs/`, `tests/integration/` | the library's own tests; tests of the docs, snippets, examples and scripts; integration tests against containers started with odctl |
| `tests/conformance/` | each operation checked against plain SimPy, the version in `uv.lock`, in the low-level, builder and YAML APIs (`harness.py`, blueprints in `blueprints/`); `register.yaml` lists every operation with its forms, checks and docs page, and `scripts/gen_pages.py` turns it into the SimPy Compatibility page. A new `test_*` function there must be listed in `register.yaml` |
| `scripts/` | `gen_pages.py` (the generated Reference pages, run by every docs build: Distributions, Statistic Names, SimPy Compatibility), `record_blocks.py` (the record blocks on pages), `charts.py` (the charts), `diagrams.py` (the diagram images), `release_notes.py` |
| `benchmarks/` | `throughput.py` (see issue #24) |

## Commands

```bash
uv run --all-extras pytest tests/unit tests/conformance -q  # unit and SimPy conformance tests, as CI runs them (--all-extras matters)
uv run --all-extras pytest tests/docs -q          # docs, snippet, example and script tests, run in the docs-build CI job
uv run --all-extras pytest tests/integration -ra  # needs Docker; starts odctl profiles on demand
uvx pre-commit run --all-files                    # ruff, ruff format, mypy, whitespace, YAML
DISABLE_MKDOCS_2_WARNING=true uv run mkdocs build --strict
uv run mkdocs serve -a 127.0.0.1:8001             # docs at http://127.0.0.1:8001/desflow/
```

A plain `uv run` without `--all-extras` removes the extras from the venv, and the connector tests then fail to import.

Run tests as often as a change needs. While fixing, rerun only the failed tests (`pytest --lf`) and the test files for the code changed; run `tests/unit tests/conformance` in full once before each commit, and not `tests/conformance` again on its own. `tests/docs` is the slowest suite: while working, run only the docs tests for the pages, snippets and examples you changed (`pytest tests/docs -k <name>`), plus `test_docs_record_blocks.py` and `test_example_twins.py` when a commit changes behaviour (not for a rename or a docstring); run the full `tests/docs` before a merge or a release. Run the strict build only when `docs/`, `mkdocs.yml`, a docstring or a `scripts/gen_pages.py` input (`tests/conformance/register.yaml`, `uv.lock`, the distribution list, the statistic names) changed, at most once per commit. CI runs everything on every push.

## Rules

- **Three APIs.** A feature works in the low-level API, the builder and YAML wherever it can.
- **Examples.** A feature's example is a shop model in each of `examples/yaml/`, `examples/builder/` and `examples/low-level/`, and the three give the same records (`tests/docs/test_example_twins.py`). It is registered in `src/desflow/blueprint/examples.py`, so `desflow examples get <name>` downloads it (`tests/docs/test_cli_examples.py` fails on a file that is not), and listed on `docs/examples/index.md`. The page that teaches it shows a short excerpt and its Quick Start (`tests/docs/test_docs_quick_starts.py`), not the full files in three tabs. An example that needs a service names its odctl profile in `examples.py`. An existing example changes only when a behaviour change requires it.
- **Docs with every feature.** A feature PR carries its docs pages, examples and nav entries. A feature without docs does not merge.
- **Docs code blocks are copies of files.** A Python or YAML block labelled `title="examples/..."` or `title="docs/snippets/..."` must match that file exactly; `tests/docs/test_docs_match_examples.py` checks it. A block that shows one part of a file is labelled `path:section`, and the file marks that part with `# --8<-- [start:section]` and `# --8<-- [end:section]` lines. Change the file, then copy it into the page again. Every YAML block must be labelled with a file. Pages use copies, not `--8<--` includes, so the Markdown reads correctly on GitHub.
- **Docs code runs.** Every Python block in the docs is a labelled copy of a file that a test runs; `tests/docs/test_docs_code_is_run.py` fails on an unlabelled block, on a script in `docs/snippets/guides/` without a test in `test_docs_guides.py`, and on a snippet folder no test runs. A new How-To page puts its script in `docs/snippets/guides/` and adds a test that checks what its page says the script does.
- **Generated files follow the code.** The record blocks on the pages are rewritten only when `tests/docs/test_docs_record_blocks.py` fails, and only the blocks it names (`uv run --all-extras python scripts/record_blocks.py`); a rename or a docstring change needs no run. Run `uv run --all-extras python scripts/charts.py` when a tutorial snippet or a distribution changes, and `uv run --all-extras python scripts/diagrams.py <name>` after editing a `.drawio` file. `scripts/gen_pages.py` needs no run: every docs build runs it.
- **Record contract.** `publish_event`, `publish_telemetry` and `publish_label` build plain dicts with the same keys and order as `EventPayload`, `TelemetryPayload` and `LabelPayload`. Changing a field, its type or the `timestamp` format breaks downstream consumers and needs an upgrade note.
- **Extras.** Each connector's dependencies are in an extra (`kafka`, `confluent`, `glue`, `redis`, `postgres`, `parquet`, `iceberg`, `all`), and fitting's in `fit` (scipy and matplotlib), which `all` does not include. The `extras-isolation` CI job installs each extra alone, so import optional packages lazily.
- **Infrastructure comes from odctl** (`odctl>=1.0,<2` in the dev group): `kafka-lite`, `postgres`, `valkey`, `storage`, `catalog`.
- **Python modules.** Shared settings in `config.py`, models in `models.py`, names used only inside a module start with `_`.
- **Writing.** Plain English for non-native readers. No em or en dashes. Headings do not start with "The". Markdown paragraphs are one line each, never hard-wrapped.
- **ShadowTraffic** may be named as inspiration in a sentence, but nothing is copied from its docs or examples, and its name never appears in a package, command, module or page title.

## Release

1. Read SimPy's changelog (`CHANGES.rst` in its sdist) for every release since the version in `uv.lock` at the last desflow release. For each behaviour change, add a conformance check that compares desflow with the new SimPy, and move `uv.lock` to it; the tests must pass at both ends of the `simpy` requirement.
2. Bump `version` in `pyproject.toml` and add the release to `docs/about/changelog.md`, with an Upgrading line for any rename or behaviour change; when it reaches `main`, the pipeline deploys the docs as that version, aliased `latest`, and copies its `404.html` to the site root.
3. Once the `main` pipeline for that commit is green, tag `vX.Y.Z` on it and publish a GitHub release whose text is that version's Changelog section, so the two never differ: `python scripts/release_notes.py X.Y.Z > /tmp/notes.md`, then `gh release create vX.Y.Z --title vX.Y.Z --notes-file /tmp/notes.md`. `publish.yml` runs on the published release and uploads to PyPI through the `pypi` environment.
4. Python 3.15 is not in the CI matrix yet: confluent-kafka has no 3.15 wheel (issue #40).

## Git

- Commit or push only when asked. Do not create branches unless asked.
- Merge a bot PR with the owner as author: `gh pr merge <n> --squash --author-email <maintainer email>`. A plain squash merge adds the bot to the contributors list.
