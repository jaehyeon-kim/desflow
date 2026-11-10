"""Imports and constructs what the `fit` extra exists to enable, installed alone."""

import numpy as np

from desflow import fit_column

# fit_column and plot import scipy and matplotlib only when they run
values = np.random.default_rng(1).gamma(4.0, 0.4, 50)
result = fit_column(values, lower=0)
assert result.best is not None, result.text()
result.plot("fit.png")
