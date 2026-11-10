# Fit a Distribution

Ask for a fit when you have measured times, such as packing times taken at the bench, and want the model to draw from a distribution that matches them. An assistant with the skill fits every family with `desflow fit` and loads the best fit into the model.

---

## Ask

```text
packing_times.csv holds 40 packing times measured at the bench, in seconds, in the
column packing_seconds. Fit a distribution to them, and use it for packing in
drafted_shop.yaml.
```

---

## What the Assistant Does

The skill gives the assistant one command. It fits every family to the column, ranks the fits, and writes the best as YAML:

```bash
desflow fit packing_times.csv --column packing_seconds --lower 0 --out fitted/packing.yaml
```

`--lower 0` says a time cannot be below 0. The command needs the `fit` extra, which adds scipy and matplotlib: `uv tool install "desflow[fit]"`. The assistant then puts the file where the service was, with `packing: !include fitted/packing.yaml` under `services`.

---

## Example

[Fitting the Inputs](../learn/inputs/fit-the-inputs.md) fits the shop's 40 packing times with this command, and shows the table of families it ranks, a plot of the fit, and the file it writes.

The best fit is not always the right choice. On that page the triangular has the lowest AIC, and the gamma is 0.11 behind, too close to tell apart. The two give different mean waits, and only the gamma keeps the data's mean.

---

## Check the Reply

Ask which families came close to the best fit, and whether the choice changes the answer. Look at the plot that `--plot` draws: the data and the fit should cover the same range. [Input Fitting](../user-guide/model-parts/input-fitting.md) describes every option of `desflow fit`.
