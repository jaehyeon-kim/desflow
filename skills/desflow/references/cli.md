# Commands

The help of every `desflow` command, generated from the command's code.

- [`desflow`](#desflow)
- [`desflow run`](#desflow-run)
- [`desflow check`](#desflow-check)
- [`desflow test`](#desflow-test)
- [`desflow explain`](#desflow-explain)
- [`desflow experiment`](#desflow-experiment)
- [`desflow report`](#desflow-report)
- [`desflow fit`](#desflow-fit)
- [`desflow dist`](#desflow-dist)
- [`desflow schema`](#desflow-schema)
- [`desflow examples`](#desflow-examples)
- [`desflow examples list`](#desflow-examples-list)
- [`desflow examples get`](#desflow-examples-get)

## `desflow`

```text
Usage: desflow [OPTIONS] COMMAND [ARGS]...

  Run, explain and analyse desflow simulations, fit their inputs, and download
  the examples.

Options:
  --version  Print the installed version and exit.
  --help     Show this message and exit.

Commands:
  run         Build the simulation in FILE and run it, or run replications...
  check       Check that each blueprint or builder file in FILE...
  test        Run the tests of each file in FILE...
  explain     Show the model in SOURCE as a graph, with its utilisation,...
  experiment  Run the experiment in FILE and write its artifacts and report.
  report      Build the report of an experiment again from the artifacts in...
  fit         Fit a column of FILE to the distribution families, and exit 1...
  dist        List the distribution families, or convert parameters to a...
  schema      Print the blueprint JSON Schema.
  examples    List the examples, or download one at the version you have...

  Examples:

    # Run a blueprint, sending its records to its sinks

    $ desflow run shop.yaml

    # Print the first five events, with no services

    $ desflow run shop.yaml --sample 5

    # Find the bottleneck before running

    $ desflow explain shop.yaml

    # List the examples, then download one

    $ desflow examples list

    $ desflow examples get local
```

## `desflow run`

```text
Usage: desflow run [OPTIONS] {FILE}

  Build the simulation in FILE and run it, or run replications of it.

  FILE is a YAML blueprint. With --sample it may also be a builder file, a `.py`
  file with a module-level `SimulationContext`, which is imported as `desflow
  test` imports it, so its `if __name__ == "__main__":` block does not run.

Arguments:
  FILE <file>  The blueprint file, or a builder file with --sample.  [required]

Options:
  --until TIME      Simulation time to stop at, in seconds or as a duration such
                    as '10 min'. Overrides run.until in the file.
  --seed N          The random seed. Overrides simulation.random_seed in the
                    file. A run without one logs the seed it drew, with the
                    command that repeats it.
  --drain           At the end time, stop the arrivals and keep running until
                    every entity in the model has left. Sets run.drain in the
                    file to true.
  --replications N  Run N replications unpaced, with seeds spawned from the
                    seed, and print the mean and confidence interval of each
                    statistic.  [x>=2]
  --level <float>   The confidence level of the replications' intervals, between
                    0 and 1.  [default: 0.95]
  --measure NAME    A statistic to summarise, such as tasks.pack.wait_mean;
                    repeat it for more. Defaults to every statistic the run
                    publishes, at least up to typical.
  --workers N       Replications run at once, each in its own process.
                    [default: 1; x>=1]
  --sample N        Print the first N events as JSON lines, unpaced, with no
                    ingress and no real sinks.  [x>=1]
  --stdout          Print every record except the system.simulation.lag_seconds
                    readings as a JSON line, instead of writing to the file's
                    sinks. The lag readings describe the wall clock, not the
                    model.
  --var NAME=VALUE  Fill ${NAME} in the file and in the files it includes,
                    before the environment does. Repeat it for more.
  --watch           Run in a child process, and run again whenever the file or a
                    .py file beside it changes. Needs the watch extra.
  --help            Show this message and exit.

  Examples:

    # Run a blueprint, sending its records to its sinks

    $ desflow run shop.yaml

    # Print the first five events, with no services

    $ desflow run shop.yaml --sample 5

    # Print every record as a JSON line instead of sending it

    $ desflow run shop.yaml --stdout

    # Run 20 replications and print the mean and interval of each
    statistic

    $ desflow run shop.yaml --replications 20 --seed 7

    # Print the first five events again whenever the file changes

    $ desflow run shop.yaml --sample 5 --watch
```

## `desflow check`

```text
Usage: desflow check [OPTIONS] {FILE...}...

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
  FILE... <list[Path]>  The blueprint files, or builder files.  [required]

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

## `desflow test`

```text
Usage: desflow test [OPTIONS] {FILE...}...

  Run the tests of each file in FILE... and report the statistics that differ.

  A test names a seed, an end time and the statistics its run must give, such as
  tasks.pack_order.finished. It runs the model once, unpaced, with no ingress
  and no egress, so a test contacts no service. A test states its measures at
  one seed in one version of desflow: a new release may draw its random numbers
  differently, so exact seeded values are not promised across versions. Run it
  with --fix after an upgrade or a deliberate change and read the changes. A
  builder file reads its tests from a tests.yaml in its folder.

  Prints ok for each passing test, and each difference of a failing one. Exits 1
  if any test fails.

Arguments:
  FILE... <list[Path]>  The blueprint files, or builder files with a tests.yaml
                        beside them.  [required]

Options:
  --fix             Write the statistics the runs gave into the expect values
                    that differ, and into a test with none, instead of reporting
                    differences. Read the changes before you keep them.
  --var NAME=VALUE  Fill ${NAME} in a blueprint and in the files it includes,
                    before the environment does. Repeat it for more.
  --help            Show this message and exit.

  Examples:

    # Check the tests in a blueprint

    $ desflow test shop.yaml

    # Write the statistics the runs gave into the expect values, then read
    them

    $ desflow test shop.yaml --fix
```

## `desflow explain`

```text
Usage: desflow explain [OPTIONS] {SOURCE}

  Show the model in SOURCE as a graph, with its utilisation, status, waits and
  tuning hints where the queueing formulas apply, and why where they do not.

Arguments:
  SOURCE <str>  A YAML blueprint, or a builder Python file with a module-level
                SimulationContext, local or a URI such as
                s3://bucket/sims/shop.yaml.  [required]

Options:
  --json            Print the explanation as JSON (schema_version 0,
                    experimental).
  --verify          Also run the model unpaced and compare its statistics with
                    the analysis.
  --until TIME      With --verify: the end of the run, in seconds or as a
                    duration such as '10 min'. Overrides run.until in the file.
  --seed N          With --verify: the run's seed. Overrides the model's own.
  --replications N  With --verify: run N times and compare with each measure's
                    95% interval, instead of one run within 10 percent.  [x>=2]
  --target T        The utilisation the hints aim for, above 0 and at most 10;
                    at 1 or above the hints are for deliberate overload.
                    [default: 0.85]
  --help            Show this message and exit.

  Examples:

    # Show the graph, the utilisation and the bottleneck

    $ desflow explain shop.yaml

    # Size each resource for a utilisation of 0.7

    $ desflow explain shop.yaml --target 0.7

    # Compare the formulas with 10 replications of the model

    $ desflow explain shop.yaml --verify --replications 10[/bold
    cyan]

    # Print the result as JSON

    $ desflow explain shop.yaml --json
```

## `desflow experiment`

```text
Usage: desflow experiment [OPTIONS] {FILE}

  Run the experiment in FILE and write its artifacts and report.

  Exits 1 if a check fails.

Arguments:
  FILE <file>  The experiment file.  [required]

Options:
  --output FOLDER  Where to write the artifacts. Overrides output in the file.
  --help           Show this message and exit.

  Examples:

    # Run an experiment and write its results to a folder

    $ desflow experiment experiment.yaml --output results
```

## `desflow report`

```text
Usage: desflow report [OPTIONS] {FOLDER}

  Build the report of an experiment again from the artifacts in FOLDER.

Arguments:
  FOLDER <dir>  An experiment's output folder, with its experiment.json.
                [required]

Options:
  --help  Show this message and exit.

  Examples:

    # Build the report again from an experiment's results

    $ desflow report results
```

## `desflow fit`

```text
Usage: desflow fit [OPTIONS] {FILE}

  Fit a column of FILE to the distribution families, and exit 1 if none is
  usable.

Arguments:
  FILE <file>  A CSV file with a header row.  [required]

Options:
  --column NAME                   The column to fit.  [required]
  --unit <str>                    The data's unit: ms, s, min or h. Settings are
                                  in seconds.  [default: s]
  --candidates NAMES              The families to try, separated by commas.
                                  Defaults to every family.
  --lower <float>                 The lowest plausible value, in the data's
                                  unit.
  --upper <float>                 The highest plausible value, in the data's
                                  unit.
  --bins <int range>              Offer the empirical distribution as a
                                  histogram with this many bins.  [x>=1]
  --min-count <int range>         The fewest values a family is fitted to.
                                  [default: 20; x>=1]
  --tolerance <float>             How far a fitted mean and standard deviation
                                  may be from the data's, as a share of the
                                  data's.  [default: 0.25]
  --drop-invalid / --no-drop-invalid
                                  Leave out invalid values rather than stopping.
                                  [default: no-drop-invalid]
  --out FILE                      Write the best candidate's settings here, for
                                  !include in a blueprint.
  --plot FILE                     Draw the data against the best candidate, as a
                                  PNG.
  --help                          Show this message and exit.

  Examples:

    # Fit measured packing times, in seconds

    $ desflow fit times.csv --column packing_seconds

    # Write the best fit's settings for a blueprint to load

    $ desflow fit times.csv --column packing_seconds --lower 0 --out
    packing.yaml
```

## `desflow dist`

```text
Usage: desflow dist [OPTIONS] [FAMILY]

  List the distribution families, or convert parameters to a mean and std and
  back.

  Exits 1 for a family with no formula from a mean and std.

Arguments:
  FAMILY <str>  A family, such as gamma. Leave it out to list every family.

Options:
  --rate X   Events per second.
  --mean X   The mean, a parameter or, with --std, the mean to convert from.
  --std X    The std, a parameter or, with --mean, the std to convert from.
  --low X    The lowest value.
  --value X  A constant's time.
  --high X   The highest value.
  --mode X   The most likely value.
  --shape X  The shape.
  --scale X  The scale.
  --p X      The chance of success on each try.
  --help     Show this message and exit.

  Examples:

    # List the distribution families

    $ desflow dist

    # Show the parameters and rules of one family

    $ desflow dist gamma

    # Convert a mean and standard deviation to gamma parameters

    $ desflow dist gamma --mean 1.5 --std 0.4832
```

## `desflow schema`

```text
Usage: desflow schema [OPTIONS]

  Print the blueprint JSON Schema.

Options:
  --help  Show this message and exit.

  Examples:

    # Save the blueprint schema for an editor

    $ desflow schema > blueprint.schema.json
```

## `desflow examples`

```text
Usage: desflow examples [OPTIONS] COMMAND [ARGS]...

  List the examples, or download one at the version you have installed.

Options:
  --help  Show this message and exit.

Commands:
  list  List the examples, with what each shows and what it needs.
  get   Download an example into <dest>/<name>/<api>/, at the installed...

  Examples:

    # List the examples

    $ desflow examples list

    # Download the YAML version of one

    $ desflow examples get local --api yaml
```

## `desflow examples list`

```text
Usage: desflow examples list [OPTIONS]

  List the examples, with what each shows and what it needs.

Options:
  --help  Show this message and exit.

  Examples:

    # List the examples, with what each shows and the service it
    needs

    $ desflow examples list
```

## `desflow examples get`

```text
Usage: desflow examples get [OPTIONS] {name}

  Download an example into <dest>/<name>/<api>/, at the installed release.

Arguments:
  name <str>  The example, as `desflow examples` lists it.  [required]

Options:
  --api <str>           Only this version: yaml, builder or low-level.
  --dest <path>         The folder to download into.  [default: .]
  --ref <str>           A git tag or branch. Defaults to the installed release's
                        tag.
  --force / --no-force  Replace files that are already there.  [default: no-
                        force]
  --help                Show this message and exit.

  Examples:

    # Download the three versions of an example into ./local/

    $ desflow examples get local

    # Download only the YAML version into another folder

    $ desflow examples get local --api yaml --dest work

    # Download it from the main branch

    $ desflow examples get local --ref main
```
