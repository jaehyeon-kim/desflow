# CLI

The `desflow` command and each of its commands, generated from their code, so each page shows the help the installed release prints. [Command Line](https://jaehyeon.me/desflow/latest/user-guide/command-line/index.md) uses every command on the shop. [Cheat Sheet](https://jaehyeon.me/desflow/latest/reference/cheat-sheet/index.md) shows the commands in use.

| Command                                                                                            | What it does                                                                                                                                            |
| -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`desflow run`](https://jaehyeon.me/desflow/latest/reference/cli/run/index.md)                     | Build the simulation in FILE and run it, or run replications of it.                                                                                     |
| [`desflow explain`](https://jaehyeon.me/desflow/latest/reference/cli/explain/index.md)             | Show the model in SOURCE as a graph, with its utilisation, status, waits and tuning hints where the queueing formulas apply, and why where they do not. |
| [`desflow experiment`](https://jaehyeon.me/desflow/latest/reference/cli/experiment/index.md)       | Run the experiment in FILE and write its artifacts and report.                                                                                          |
| [`desflow report`](https://jaehyeon.me/desflow/latest/reference/cli/report/index.md)               | Build the report of an experiment again from the artifacts in FOLDER.                                                                                   |
| [`desflow fit`](https://jaehyeon.me/desflow/latest/reference/cli/fit/index.md)                     | Fit a column of FILE to the distribution families, and exit 1 if none is usable.                                                                        |
| [`desflow dist`](https://jaehyeon.me/desflow/latest/reference/cli/dist/index.md)                   | List the distribution families, or convert parameters to a mean and std and back.                                                                       |
| [`desflow schema`](https://jaehyeon.me/desflow/latest/reference/cli/schema/index.md)               | Print the blueprint JSON Schema.                                                                                                                        |
| [`desflow examples`](https://jaehyeon.me/desflow/latest/reference/cli/examples/index.md)           | List the examples, or download one at the version you have installed.                                                                                   |
| [`desflow examples list`](https://jaehyeon.me/desflow/latest/reference/cli/examples-list/index.md) | List the examples, with what each shows and what it needs.                                                                                              |
| [`desflow examples get`](https://jaehyeon.me/desflow/latest/reference/cli/examples-get/index.md)   | Download an example into <dest>/<name>/<api>/, at the installed release.                                                                                |

```text
Usage: desflow [OPTIONS] COMMAND [ARGS]...

  Run, explain and analyse desflow simulations, fit their inputs, and download
  the examples.

Options:
  --version  Print the installed version and exit.
  --help     Show this message and exit.

Commands:
  run         Build the simulation in FILE and run it, or run replications...
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
