# desflow fit

The help of `desflow fit`, generated from the command's code. [Command Line](https://jaehyeon.me/desflow/latest/cli/#fit-measured-times) uses this one on the shop, and its [What It Is](https://jaehyeon.me/desflow/latest/cli/#what-it-is) lists every command.

```text
Usage: desflow fit [OPTIONS] {FILE}

  Fit a column of FILE to the distribution families, and exit 1 if none is
  usable.

Arguments:
  FILE  A CSV file with a header row.  [required]

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
