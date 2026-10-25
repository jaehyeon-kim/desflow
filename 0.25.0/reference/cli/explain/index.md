# desflow explain

The help of `desflow explain`, generated from the command's code. [CLI](https://jaehyeon.me/desflow/latest/reference/cli/index.md) lists every command, and [Command Line](https://jaehyeon.me/desflow/latest/user-guide/command-line/#check-it-before-a-long-run) uses this one on the shop.

```text
Usage: desflow explain [OPTIONS] {SOURCE}

  Show the model in SOURCE as a graph, with its utilisation, status, waits and
  tuning hints where the queueing formulas apply, and why where they do not.

Arguments:
  SOURCE  A YAML blueprint, or a builder Python file with a module-level
          SimulationContext, local or a URI such as s3://bucket/sims/shop.yaml.
          [required]

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
