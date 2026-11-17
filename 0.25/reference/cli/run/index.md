# desflow run

The help of `desflow run`, generated from the command's code. [CLI](https://jaehyeon.me/desflow/latest/reference/cli/index.md) lists every command, and [Command Line](https://jaehyeon.me/desflow/latest/user-guide/command-line/#run-it) uses this one on the shop.

```text
Usage: desflow run [OPTIONS] {FILE}

  Build the simulation in FILE and run it, or run replications of it.

Arguments:
  FILE  The blueprint file.  [required]

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
```
