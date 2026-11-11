# Contributing to desflow

This file is for people who change desflow itself: the library, its tests and its documentation. To use desflow, start with the [documentation](https://jaehyeon.me/desflow/). For a change larger than a fix, open an issue first, so the design is agreed before the code is written. The [roadmap](https://jaehyeon.me/desflow/latest/about/roadmap/) shows what is planned. [How desflow Works](https://jaehyeon.me/desflow/latest/user-guide/how-desflow-works/) shows how the parts fit together.

## Set Up

desflow uses [uv](https://docs.astral.sh/uv/). Clone the repository and install the package with every extra and the development tools:

```bash
git clone https://github.com/jaehyeon-kim/desflow.git
cd desflow
uv sync --all-extras
```

A plain `uv sync` removes the extras, and the connector tests then fail to import, so pass `--all-extras` to every `uv sync` and `uv run`.

The integration tests and the examples that need a service get it from [odctl](https://github.com/jaehyeon-kim/odctl), which manages Docker Compose stacks. odctl is in the development group (`odctl>=1.0,<2`), so `uv sync` installs it. It needs Docker. The profiles in use are `kafka-lite`, `postgres`, `valkey`, `storage` and `catalog`. The `floci`, `azure` and `gcp` test groups need no odctl profile: they start an emulator with `docker run`, which is Floci for AWS, Azurite for Azure Blob Storage and fake-gcs-server for Google Cloud Storage.

## Code Map

[ARCHITECTURE.md](ARCHITECTURE.md) maps the source: the packages, which may import which, what is inside `core`, `builder` and `blueprint`, and what each folder of the repository holds.

## Tests

```bash
uv run --all-extras pytest tests/unit tests/conformance tests/benchtop tests/known_cases -m "not paced" -q -n auto  # unit, SimPy conformance and known case tests; CI splits them into the jobs of scripts/ci_plan.py
TEST_SHARD=2/3 uv run --all-extras pytest tests/unit tests/conformance tests/benchtop -m "not paced and not every_model" -q -n auto  # one part of them, as a CI job runs it, dealt by the times in tests/durations.json
gh run download <run id> -p 'junit-Linux-*' -D /tmp/junit && python scripts/test_durations.py /tmp/junit  # record those times again from a run of manual.yml, when tests are added or become much slower
uv run --all-extras pytest tests/known_cases -m paced -q  # the known cases that run against the wall clock, without -n auto, because other tests slow them down
uv run --all-extras pytest tests/docs -q -n auto  # docs, snippet, example and script tests, run in the docs-build CI job of a pull request in 5 groups
DOCS_STORE_DURATIONS=1 uv run --all-extras pytest tests/docs -q -n auto  # record the test times that balance the groups, in tests/docs/durations.json
uv run --all-extras pytest tests/integration -ra  # needs Docker; starts odctl profiles and emulators on demand, and also runs the service examples against the profiles the other tests started; -m <group> runs one group: services, storage, floci, azure, gcp or examples
uv run odctl up <profile> && uv run --all-extras pytest tests/integration/test_service_examples.py -m examples -k <example> -ra  # one service example in its three versions; start the profile first, and stop it with uv run odctl down <profile> --volumes
uvx pre-commit run --all-files                    # ruff, ruff format, mypy, whitespace, YAML
uv run --all-extras lint-imports                  # the import contracts in pyproject.toml, [tool.importlinter]
DISABLE_MKDOCS_2_WARNING=true NO_MKDOCS_2_WARNING=1 uv run --all-extras mkdocs build --strict
uv run --all-extras mkdocs serve -a 127.0.0.1:8001             # docs at http://127.0.0.1:8001/desflow/
```

`-n auto` runs the tests in one worker process per CPU (pytest-xdist). The integration tests share odctl containers, so they run in one process.

Each layer checks one thing:

- `tests/unit` checks each module of the library.
- `tests/conformance` checks that each SimPy operation gives the same result in desflow as in plain SimPy, in all three APIs.
- `tests/docs` checks that every code block on a page is a copy of a file, that every file runs and does what its page says, and that every feature has an example, a reference entry and a place in the skill.
- `tests/integration` checks each connector against a real service.
- The regression CI jobs run every example and scenario with this branch and with the last release, and fail on a difference that `tests/regression/expected.yaml` does not list.

Run the unit suite in full once before you push or open a pull request, not before each commit. While fixing, run only the failed tests (`pytest --lf`) and the tests of the files you changed. `tests/docs` is slow. While working, run the docs tests for the pages, snippets and examples you changed (`pytest tests/docs -k <name>`), plus `test_docs_record_blocks.py` and `test_example_twins.py` when a change alters behaviour. Run the strict build when `docs/`, `mkdocs.yml`, a docstring or an input of `scripts/gen_pages.py` changed: `tests/conformance/register.yaml`, `uv.lock`, the distribution list or the statistic names. CI runs everything on every pull request and every push to `main`.

## Rules

- **Three APIs.** A feature works in the low-level API, the builder and YAML wherever it can.
- **Examples.** A feature's example is a shop model in each of `examples/yaml/`, `examples/builder/` and `examples/low-level/`, and the three give the same records (`tests/docs/test_example_twins.py`). It is registered in `src/desflow/examples.py`, so `desflow examples get <name>` downloads it, and listed on `docs/gallery/index.md`. The page that teaches it shows a short excerpt and its Quick Start (`tests/docs/test_docs_quick_starts.py`), not the full files in three tabs. An example that needs a service names its odctl profile in `examples.py`. Its builder and low-level scripts that run until Ctrl + C read `EXAMPLE_UNTIL`, a number of simulation seconds, and stop at that time when it is set. The `service-examples` job runs its three versions against the profile and reads the records back. `tests/integration/test_service_examples.py` holds each example's run length and read-back. The `services` job's matrix in `.github/workflows/pull-request.yml` lists the examples with a profile, and `tests/unit/test_service_examples_matrix.py` fails when it differs from `EXAMPLES`. The examples of a `gen` kind are the exception: each is a small order model in `examples/generators/`, in three files with the same records. A new `gen` kind needs its three files, its `<kind>_output.txt` (written by `python scripts/gen_pages.py`) and an entry in `GROUPS` and `KINDS` of `scripts/reference_pages/generators.py`, and `test_every_kind_a_group_lists_is_in_the_schema_once` fails without them. An existing example changes only when a behaviour change requires it.
- **Docs with every feature.** A feature pull request carries its docs pages, examples and nav entries, and a feature without docs does not merge. `tests/docs/feature_lists.py` reads every command and option, every name in `desflow.__all__`, every blueprint key with the model that declares it (`tests/docs/blueprint_keys.py`), every connector, extra and distribution family from the code. The tests in `tests/docs/test_feature_coverage*.py` fail on one without a tested example, a reference entry and a place in the skill, or, for a command, connector or extra, a README line. A new command or option also gets a line on `docs/cli/index.md`, which `tests/docs/test_docs_command_line.py` runs.
- **Generated files follow the code.** Rewrite the record blocks on the pages only when `tests/docs/test_docs_record_blocks.py` fails, and only the blocks it names: `uv run --all-extras python scripts/record_blocks.py`. A rename or a docstring change needs no run. Run `scripts/charts.py` when a tutorial snippet or a distribution changes, and `scripts/diagrams.py <name>` after editing a `.drawio` file. Run `scripts/gen_schema.py` after a change to the blueprint models or the `Explanation` shape, `scripts/gen_skill.py` after a change to the skill template, the blueprint keys, the distributions or the connectors, and `scripts/public_api.py` after a change to the public API. Each runs with `uv run --all-extras python`. Every docs build runs `scripts/gen_pages.py`.
- **Record contract.** `publish_event`, `publish_telemetry` and `publish_label` build plain dicts with the same keys and order as `EventPayload`, `TelemetryPayload` and `LabelPayload`. Changing a field, its type or the `timestamp` format breaks downstream consumers and needs an upgrade note in the Changelog.
- **Extras.** Each connector's dependencies are in an extra (`kafka`, `confluent`, `glue`, `redis`, `postgres`, `parquet`, `iceberg`), fitting's in `fit` (scipy and matplotlib), and `all` installs every one of them. The `extras-isolation` CI job installs each extra alone, so import optional packages lazily. The core install and the extras list only packages desflow's own code imports (`tests/unit/test_dependencies.py`); a package used only by docs, examples or tests goes in the `dev` group, and the page that uses it says how to install it. An extra may name a feature extra of a package desflow imports when a connector's tested behaviour needs it, as `iceberg` names pyiceberg's `sql-postgres` and `glue` for the SQL and Glue catalogs; `FEATURE_EXTRAS` in `tests/unit/test_dependencies.py` lists each one with what needs it.
- **Python modules.** Shared settings go in `config.py`, models in `models.py`, and names used only inside a module start with `_`.

## Add a Connector

[Write a Connector](https://jaehyeon.me/desflow/latest/how-to/records-and-kafka/write-a-connector/) shows a connector outside the package. A connector inside the package takes these steps:

1. Write the class in `src/desflow/connectors/egress/` or `ingress/`, on `BaseEgress` or `BaseIngress`, with one `async run(queue)`.
2. Add its short name to `EGRESS_TYPES` or `INGRESS_TYPES` in `src/desflow/blueprint/build.py`, with its module, its class and the extra that installs what it imports.
3. Put its dependencies in an extra in `pyproject.toml`, add the extra to `all`, and add `scripts/install_checks/check_<extra>.py`, which imports and constructs the connector with that extra alone; the `install-checks` CI job runs it, and `tests/unit/test_ci_plan.py` fails until it exists. Export the class from `src/desflow/__init__.py` inside a `try` block, as the other optional connectors are, then run `scripts/public_api.py`.
4. Add an integration test in `tests/integration/` with a module-level `pytestmark = [pytest.mark.integration, pytest.mark.<group>]` (a module whose tests need different groups marks `integration` on the module and one group on each test), against an odctl profile (`services` for `kafka-lite`, `postgres` and `valkey`; `storage` for `storage` and `catalog`, and for a test that also uses PostgreSQL beside them, such as the SQL catalog test, because the `catalog` profile starts PostgreSQL itself) or an emulator fixture (`floci`, `azure`, `gcp`). CI runs one job per group, so keep the two odctl groups at about the same run time.
5. Document it: a section on the Ingress or Egress page with a labelled `type: <Name>` YAML block, a row in that page's `type` table, the extra on Installation (`docs/installation.md`) and in the README's extras table, and a README line naming the connector. Then run `scripts/gen_skill.py`.

## Add a Distribution

Every distribution family is one `Distribution` in `DISTRIBUTIONS`, in `src/desflow/models/distributions.py`. It declares the parameters, their rules and check, the draw, the mean and variance, the value with no random generator, and, where it has one, the formula from a mean and a standard deviation. The sampler, the registry, the builder, the YAML models and `desflow dist` all read that table, so nothing else needs registering. Add unit tests for the draw and the check, a labelled docs block that draws from it, then run `scripts/gen_skill.py` and `scripts/charts.py`. The Distributions reference page is generated from the table.

## Write the Docs

- **Code blocks are copies of files.** A Python or YAML block labelled `title="examples/..."` or `title="docs/snippets/..."` must match that file exactly (`tests/docs/test_docs_match_examples.py`). A block that shows one part of a file is labelled `path:section`, and the file marks that part with `# --8<-- [start:section]` and `# --8<-- [end:section]` lines. Every YAML block is labelled with a file. Change the file, then copy it into the page again. Pages use copies, not includes, so the Markdown reads correctly on GitHub.
- **Code runs.** Every Python block is a copy of a file that a test runs. `tests/docs/test_docs_code_is_run.py` fails on a block with no file label, on a script in `docs/snippets/guides/` without a test in `tests/docs/test_docs_guides.py`, and on a snippet folder that no test runs. A new How-To page puts its script in `docs/snippets/guides/` and adds a test to `tests/docs/test_docs_guides.py` that checks what the page says the script does.
- **Page shape.** Each page follows the shape of its kind: tutorial, concept, User Guide, How-To or reference. A runnable example has the How-To shape. `tests/docs/test_docs_page_shapes.py` checks the sections each kind requires. A User Guide page on one part of desflow has How It Works, then Stability and Limits. A page has at most 10 `##` headings and 15 headings in all (`tests/docs/test_docs_page_size.py`). Each diagram is a `.drawio` file in `docs/assets/diagrams/` with the image `scripts/diagrams.py` exports from it, and each image is used on a page (`tests/docs/test_docs_diagrams.py`).
- **Writing.** Plain English for readers whose first language is not English. Headings are in title case and do not start with "The". No em or en dashes. Each Markdown paragraph is one line, never hard-wrapped.
- **Names.** Commercial data generators, such as ShadowTraffic or Mockaroo, may be named as inspiration in a sentence. Nothing is copied from their docs or examples, and their names never appear in a package, command, module or page title.

## Blueprint Layout

Every YAML blueprint file (`model.yaml` under `tests/known_cases/`, and the YAML files under `examples/` and `docs/snippets/`) is written in one layout, so the catalogue, the gallery and the docs show readable YAML. A mapping of plain values that fits in 88 characters stays on one line:

```yaml
w: {initial: 1, spawn: wait_x}
```

A mapping that holds another mapping opens into one key per line, and its inner mapping of plain values stays on one line:

```yaml
arrivals:
  a:
    initial: 1
    spawn: deposit
    attributes: {balance: 0}
```

A mapping of plain values that would pass 88 characters opens into one key per line:

```yaml
simulation:
  sim_id: HotRolling
  factor: "${FACTOR:-1.0}"
  random_seed: "${SEED:-42}"
  logical_start_time: "2026-01-01T00:00:00+00:00"
  timezone: UTC
```

A blank line separates top-level sections. No line passes 88 characters, comments included. Quotes stay as written, because desflow reads a quoted `${VAR}` value as a string and a plain one as a number if it looks like one. A command in a comment is shortened by hand, with a `\` at the end of each line but the last.

`uv run --all-extras python scripts/blueprint_layout.py <files>` rewrites a file into the layout. Two pre-commit hooks check it: `blueprint-layout` (the script's `--check` mode) and `yamllint` (the rules in `.yamllint.yaml`: line length, blank lines, spacing and indentation). `desflow fit` writes its settings files in the layout, so the files it wrote (`examples/yaml/packing_fit.yaml` and `docs/snippets/yaml/fitted/`) are checked like the others.

## Pull Requests

Work on a branch from `main` and open a pull request against `main`. Before you push, run `uvx pre-commit run --all-files`, `uv run --all-extras lint-imports`, the unit suite, and the docs tests for what you changed. `pull-request.yml` then runs the unit and conformance tests on Python 3.11, the lint and the strict docs build, the install checks, the conformance tests at the lowest SimPy, the regression, and the docs tests and the service jobs when the change touches what they cover. A pull request must be up to date with `main` before it merges, so a push to `main` (`pipeline.yml`, the README badge) runs only what the pull request did not: the unit and conformance tests on Python 3.12 to 3.15, and the strict docs build before the docs deploy. A run has at most 20 jobs at once. The long tests are `manual.yml`: every test, with the tests marked `every_model` and the known cases, on Python 3.11, 3.12 and 3.15 on Ubuntu and 3.12 on Windows, the paced cases, the conformance tests at the lowest and newest SimPy, the lowest Pydantic and the core install's time zone on Ubuntu and Windows. It runs every day, by hand with `gh workflow run manual.yml --ref <branch>` (`-f python="3.13"` chooses the versions on Ubuntu), and on a pull request when it is given the label `manual tests`. A pull request merges when its jobs all pass. Describe each change to behaviour in the pull request, so that the release can add it to the Changelog (`docs/about/changelog.md`).

## Releases

The maintainer releases. These steps are written down so that each release is the same:

1. Read SimPy's changelog (`CHANGES.rst` in its sdist) for every release since the version in `uv.lock` at the last desflow release. For each behaviour change, add a conformance check that compares desflow with the new SimPy, and move `uv.lock` to it. The tests must pass at both ends of the `simpy` requirement.
2. Bump `version` in `pyproject.toml` and add the release to `docs/about/changelog.md`, with an Upgrading line for any rename or behaviour change. The same commit updates the public roadmap, `docs/about/roadmap.md`: the release moves from the timeline to Released, the next release becomes the current entry (`key: now`), and every planned entry names all the issues planned for it; `tests/docs/test_docs_roadmap.py` fails otherwise. A commit that changes a record on purpose adds an entry to `tests/regression/expected.yaml` with the version that ships it, or the `regression` job fails. When the commit reaches `main`, the pipeline deploys the docs as that version, aliased `latest`, and copies its `404.html` to the site root.
3. Once the `main` pipeline for that commit is green, run the manual tests on it (`gh workflow run manual.yml --ref main`, which runs every test on Python 3.11, 3.12 and 3.15) and wait until it is green too, then tag `vX.Y.Z` on it and publish a GitHub release whose text is that version's Changelog section: `python scripts/release_notes.py X.Y.Z > /tmp/notes.md`, then `gh release create vX.Y.Z --title vX.Y.Z --notes-file /tmp/notes.md`. `publish.yml` runs on the published release and uploads to PyPI through the `pypi` environment. The regression capture jobs install the newest `v*` tag from PyPI, so a tag pushed before the upload has finished fails them until the upload is done. Re-run them then.
