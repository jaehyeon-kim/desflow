# desflow test

The help of `desflow test`, generated from the command's code. [Command Line](https://jaehyeon.me/desflow/latest/cli/#check-it-before-a-long-run) uses this one on the shop, and its [What It Is](https://jaehyeon.me/desflow/latest/cli/#what-it-is) lists every command.

```text
Usage: desflow test [OPTIONS] {FILE...}

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
  FILE...  The blueprint files, or builder files with a tests.yaml beside them.
           [required]

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
