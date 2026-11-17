# desflow experiment

The help of `desflow experiment`, generated from the command's code. [CLI](https://jaehyeon.me/desflow/latest/reference/cli/index.md) lists every command, and [Command Line](https://jaehyeon.me/desflow/latest/user-guide/command-line/#compare-designs) uses this one on the shop.

```text
Usage: desflow experiment [OPTIONS] {FILE}

  Run the experiment in FILE and write its artifacts and report.

  Exits 1 if a check fails.

Arguments:
  FILE  The experiment file.  [required]

Options:
  --output FOLDER  Where to write the artifacts. Overrides output in the file.
  --help           Show this message and exit.

  Examples:

    # Run an experiment and write its results to a folder

    $ desflow experiment experiment.yaml --output results
```
