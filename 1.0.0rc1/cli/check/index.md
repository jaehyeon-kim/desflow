# desflow check

The help of `desflow check`, generated from the command's code. [Command Line](https://jaehyeon.me/desflow/latest/cli/#check-it-before-a-long-run) uses this one on the shop, and its [What It Is](https://jaehyeon.me/desflow/latest/cli/#what-it-is) lists every command.

```text
Usage: desflow check [OPTIONS] {FILE...}

  Check that each blueprint or builder file in FILE... builds, without running
  it.

  It reports what `desflow run` reports before the clock starts: YAML errors,
  unknown keys, missing names, unset variables, `!python` references that cannot
  be imported, and connectors whose extra is missing or whose settings are
  refused. A Parquet, Jsonl or Iceberg connector, in a blueprint or a builder
  file, is refused when its extra is not installed. It also reports a `tests`
  entry with no seed in a file with none, as `desflow test` does. It warns about
  a run with no end: an arrival with a `dist` or a `gap` and no `max_events`, in
  a file with no `run.until`, `run.stop` or `run.segments`. It does not call
  `run.before`, contact a broker, a database or a catalog, or run a `!python`
  function. Importing a `!python` module runs its top-level code, as `desflow
  run` does. A builder file, a `.py` file with a module-level
  `SimulationContext`, is imported as `desflow test` imports it, so an import
  whose extra is missing is reported; its `if __name__ == "__main__":` block
  does not run. An error or a `sys.exit` of the file's code is one line, with
  the file, its line and the error's type. Prints `ok FILE (sim_id ID)` for each
  valid file and every error of an invalid one, and exits 1 if any file is
  invalid.

Arguments:
  FILE...  The blueprint files, or builder files.  [required]

Options:
  --var NAME=VALUE  Fill ${NAME} in the files and in the files they include,
                    before the environment does. Repeat it for more.
  --help            Show this message and exit.

  Examples:

    # Check one blueprint without running it

    $ desflow check shop.yaml

    # Check every blueprint in a folder, as a pre-commit hook does

    $ desflow check examples/*.yaml

    # Check a builder file, which defines its model in Python

    $ desflow check shop.py
```
