# desflow

Real-time SimPy simulations whose parameters change while they run, writing events and telemetry to Kafka, Redis, PostgreSQL, Parquet and JSONL files, and Iceberg tables. The public roadmap is `docs/about/roadmap.md`.

`CONTRIBUTING.md` holds the set-up, the code map, the test commands, the rules for code and docs, how to add a connector or a distribution, and the release steps. Read it before a change. This file holds only what applies to an agent.

## Running the Tests

- Always pass `--all-extras` to `uv sync` and `uv run`. A plain `uv sync` removes the extras from the venv.
- Run the unit suite, `tests/unit tests/conformance`, in full once before each commit, never after each step, and not `tests/conformance` again on its own.
- Run `test_docs_record_blocks.py` and `test_example_twins.py` when a commit changes behaviour, not for a rename or a docstring. Run the full `tests/docs` only before a merge or a release.
- Run the strict build at most once per commit.

## Git

- Commit or push only when asked. Do not create branches unless asked.
- Merge a bot PR with the owner as author: `gh pr merge <n> --squash --author-email <maintainer email>`. A plain squash merge adds the bot to the contributors list.
