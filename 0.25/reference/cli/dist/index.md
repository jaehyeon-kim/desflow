# desflow dist

The help of `desflow dist`, generated from the command's code. [CLI](https://jaehyeon.me/desflow/latest/reference/cli/index.md) lists every command, and [Command Line](https://jaehyeon.me/desflow/latest/user-guide/command-line/#convert-parameters) uses this one on the shop.

```text
Usage: desflow dist [OPTIONS] [FAMILY]

  List the distribution families, or convert parameters to a mean and std and
  back.

  Exits 1 for a family with no formula from a mean and std.

Arguments:
  FAMILY  A family, such as gamma. Leave it out to list every family.

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
  --help     Show this message and exit.

  Examples:

    # List the distribution families

    $ desflow dist

    # Show the parameters and rules of one family

    $ desflow dist gamma

    # Convert a mean and standard deviation to gamma parameters

    $ desflow dist gamma --mean 1.5 --std 0.4832
```
