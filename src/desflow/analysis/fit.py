"""Input fitting: which distribution family fits a column of measured times.

`fit_column` checks the data, fits every candidate family by maximum likelihood with
scipy, checks each fitted result, and offers the empirical distribution of the data
as well, so a usable answer exists when every family fails. Each candidate ends as
one row of a table: `fitted`, `rejected`, `did not fit` or `skipped`, with its
reason. The best is the fitted family with the lowest AIC that cannot draw outside
the data's plausible bounds. Fitting needs the `fit` extra (scipy and matplotlib),
which is imported only here, when a fit runs.
"""

import csv
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np

from desflow.core.distributions import DISTRIBUTIONS, get_distribution
from desflow.core.sampler import Sampler
from desflow.models.params import DistributionConfig

# Seconds in one unit of the data; desflow's times are seconds.
_UNITS = {"ms": 0.001, "s": 1.0, "min": 60.0, "h": 3600.0}
# Families defined for positive values only; zeros or negatives skip them.
_POSITIVE_ONLY = ("gamma", "weibull", "lognormal")
_FIT_EXTRA = (
    "fitting needs the fit extra, which installs scipy and matplotlib: "
    "pip install 'desflow[fit]'"
)


def _scipy() -> Any:
    try:
        import scipy.optimize
        import scipy.stats
    except ImportError:
        raise ImportError(_FIT_EXTRA) from None
    return scipy


@dataclass
class Candidate:
    """
    One family's outcome: its parameters, diagnostics and status.

    Attributes:
        family (str): The family, as `dist` names it.
        status (str): `fitted`, `rejected`, `did not fit` or `skipped`.
        reason (str): Why it was rejected, did not fit or was skipped; empty when
            fitted.
        parameters (Dict[str, Any]): desflow's parameters, in seconds.
        log_likelihood (Optional[float]): The log-likelihood of the data.
        aic (Optional[float]): Akaike's information criterion, 2k minus twice the
            log-likelihood, where k counts the fitted parameters.
        ks_statistic (Optional[float]): The Kolmogorov-Smirnov distance between
            the data and the fitted family.
        ks_p_value (Optional[float]): Its p-value. It is approximate, because the
            parameters were estimated from the same data.
        bounds (str): Where the family can draw outside `lower` or `upper`; empty
            when it cannot.
    """

    family: str
    status: str
    reason: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    log_likelihood: Optional[float] = None
    aic: Optional[float] = None
    ks_statistic: Optional[float] = None
    ks_p_value: Optional[float] = None
    bounds: str = ""

    @property
    def usable(self) -> bool:
        """True when the family was fitted and its result passed every check."""
        return self.status == "fitted"


@dataclass
class FitResult:
    """
    The outcome of fitting one column: every candidate, the best and the data.

    Attributes:
        candidates (List[Candidate]): One per family tried, empirical last.
        values (List[float]): The values fitted, in seconds.
        unit (str): The unit the data was written in.
        rows (int): Rows read.
        missing (int): Empty cells, left out.
        invalid (int): Values that are not numbers, not finite, or outside the
            bounds; left out with `drop_invalid`.
        lower (Optional[float]): The lowest plausible value, in seconds, or None.
        upper (Optional[float]): The highest plausible value, in seconds, or None.
    """

    candidates: List[Candidate]
    values: List[float]
    unit: str = "s"
    rows: int = 0
    missing: int = 0
    invalid: int = 0
    lower: Optional[float] = None
    upper: Optional[float] = None

    @property
    def best(self) -> Optional[str]:
        """
        The fitted family with the lowest AIC that cannot draw outside the bounds;
        `constant` when every value is the same. When no family within the bounds
        fitted, the empirical distribution of the data, which stays within them;
        then the fitted family with the lowest AIC; None when nothing is usable.
        """

        def lowest(group: List[Candidate]) -> str:
            # constant, which fits only when every value is the same, has no AIC
            return min(
                group, key=lambda c: -math.inf if c.aic is None else c.aic
            ).family

        fitted = [c for c in self.candidates if c.usable and c.family != "empirical"]
        inside = [c for c in fitted if not c.bounds]
        if inside:
            return lowest(inside)
        if any(c.usable and c.family == "empirical" for c in self.candidates):
            return "empirical"
        return lowest(fitted) if fitted else None

    @property
    def table(self) -> List[Dict[str, Any]]:
        """One row per candidate, with its status, reason, parameters and
        diagnostics, and whether it is the best."""
        best = self.best
        return [
            {
                "family": c.family,
                "status": c.status,
                "reason": c.reason,
                "parameters": c.parameters,
                "log_likelihood": c.log_likelihood,
                "aic": c.aic,
                "ks_statistic": c.ks_statistic,
                "ks_p_value": c.ks_p_value,
                "bounds": c.bounds,
                "best": c.family == best,
            }
            for c in self.candidates
        ]

    def settings(self, kind: Optional[str] = None) -> Dict[str, Any]:
        """
        Returns a usable candidate's settings, for `add_service(name, **settings)`.

        Args:
            kind: The family; None takes the best.

        Returns:
            Dict[str, Any]: `dist` and the family's parameters, in seconds, with six
                significant digits, the numbers `yaml` writes, so a script and a
                blueprint that load the same fit draw the same values.

        Raises:
            ValueError: If no candidate is usable, or `kind` is not one that is.
        """
        name = kind or self.best
        for candidate in self.candidates:
            if candidate.family == name and candidate.usable:
                return {
                    "dist": name,
                    **{
                        key: [_rounded(v) for v in value]
                        if isinstance(value, list)
                        else _rounded(value)
                        for key, value in candidate.parameters.items()
                    },
                }
        usable = [c.family for c in self.candidates if c.usable]
        if not usable:
            raise ValueError(
                "no candidate is usable; the table says why for each. empirical "
                "fits any values, so give the column values, or keep empirical in "
                "the candidates (--candidates)"
            )
        raise ValueError(f"{name} is not usable; choose one of {', '.join(usable)}")

    def text(self) -> str:
        """The table as text: one line per candidate, the best marked."""
        lines = [
            f"{self.rows} rows: {len(self.values)} used, {self.missing} missing, "
            f"{self.invalid} invalid",
            f"{'family':<17} {'status':<12} {'AIC':>10} {'KS':>7} {'p (approx.)':>11}"
            "  parameters or reason",
        ]
        best = self.best
        for c in self.candidates:
            detail = (
                ", ".join(
                    f"{k}: {len(v)} numbers"
                    if isinstance(v, list)
                    else f"{k}={_short(v)}"
                    for k, v in c.parameters.items()
                )
                if c.usable
                else c.reason
            )
            if c.bounds:
                detail += f" ({c.bounds})"
            lines.append(
                f"{c.family + (' *' if c.family == best else ''):<17} {c.status:<12} "
                f"{_number(c.aic):>10} {_number(c.ks_statistic):>7} "
                f"{_number(c.ks_p_value):>11}  {detail}"
            )
        lines.append(f"* best: {best}" if best else "No candidate is usable.")
        return "\n".join(lines)

    def yaml(self, kind: Optional[str] = None, source: str = "") -> str:
        """
        The settings as a YAML mapping a blueprint loads with `!include`.

        Args:
            kind: The family; None takes the best.
            source: Where the data came from, for the comment at the top.

        Returns:
            str: A comment line, then `dist` and each parameter, one per line, with
                the numbers `settings` returns.
        """
        settings = self.settings(kind)
        lines = [
            f"# Fitted by desflow fit{' from ' + source if source else ''}: "
            f"{settings['dist']}, from {len(self.values)} values in {self.unit}, "
            "written in seconds."
        ]
        for key, value in settings.items():
            if isinstance(value, list):
                value = "[" + ", ".join(repr(v) for v in value) + "]"
            elif isinstance(value, float):
                value = repr(value)
            lines.append(f"{key}: {value}")
        return "\n".join(lines) + "\n"

    def plot(self, path: Union[str, Path], kind: Optional[str] = None) -> Path:
        """
        Draws the data against values generated from a candidate, as a PNG.

        The left panel puts the histogram of the data over the histogram of
        generated values; the right is a Q-Q plot, each sorted data value against
        the generated value at the same quantile. The generated values come from
        desflow's `Sampler`, as a run draws them, with the same smallest delay.

        Args:
            path: The image to write.
            kind: The family; None takes the best.

        Returns:
            Path: The image written.
        """
        try:
            from matplotlib.backends.backend_agg import FigureCanvasAgg
            from matplotlib.figure import Figure
        except ImportError:
            raise ImportError(_FIT_EXTRA) from None
        settings = self.settings(kind)
        generated = _generate(settings, max(10_000, 10 * len(self.values)))
        observed = np.sort(np.asarray(self.values))
        probabilities = (np.arange(1, len(observed) + 1) - 0.5) / len(observed)
        expected = np.quantile(generated, probabilities)

        figure = Figure(figsize=(9, 3.6))
        FigureCanvasAgg(figure)
        histogram, qq = figure.subplots(1, 2)
        bins = np.histogram_bin_edges(np.concatenate([observed, generated]), bins=30)
        histogram.hist(generated, bins=bins, density=True, alpha=0.5, label="generated")
        histogram.hist(observed, bins=bins, density=True, alpha=0.7, label="observed")
        histogram.set_xlabel("seconds")
        histogram.set_ylabel("density")
        histogram.legend(frameon=False)
        histogram.set_title(f"Observed and generated: {settings['dist']}")
        qq.plot(expected, observed, "o", markersize=3)
        ends = [min(expected[0], observed[0]), max(expected[-1], observed[-1])]
        qq.plot(ends, ends, linestyle="--", linewidth=1)
        qq.set_xlabel("generated quantile (s)")
        qq.set_ylabel("observed value (s)")
        qq.set_title("Q-Q")
        figure.tight_layout()
        path = Path(path)
        figure.savefig(path)
        return path


def _short(value: Any) -> str:
    """A float with six significant digits, as the YAML and the table write it."""
    return f"{float(value):.6g}"


def _rounded(value: Any) -> float:
    """A float with six significant digits, as a number."""
    return float(_short(value))


def _number(value: Optional[float]) -> str:
    return "" if value is None else f"{value:.4g}"


def _generate(settings: Dict[str, Any], n: int) -> np.ndarray:
    """Draws n values as a run draws them, through a `Sampler`, seeded so a plot
    repeats: the same family and the same smallest delay a run applies."""
    config = DistributionConfig(**settings)
    sampler = Sampler(seed=0)
    return np.array([sampler.sample(config) for _ in range(n)])


def read_column(path: Union[str, Path], column: str) -> List[str]:
    """
    Reads one column of a CSV file, as text, in row order.

    Args:
        path: A CSV file with a header row.
        column: The column's name in the header.

    Returns:
        List[str]: Each row's cell, empty where the cell is empty, for `fit_column`.

    Raises:
        ValueError: If the file has no such column.
    """
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if column not in (reader.fieldnames or []):
            raise ValueError(
                f"{path} has no column {column!r}; it has "
                f"{', '.join(reader.fieldnames or [])}"
            )
        return [row[column] for row in reader]


def _clean(
    values: Sequence[Any],
    scale: float,
    lower: Optional[float],
    upper: Optional[float],
    drop_invalid: bool,
) -> Tuple[List[float], int, int]:
    """Converts values to seconds; returns them, the missing count and the invalid
    count. Raises on an invalid value unless `drop_invalid`."""
    kept: List[float] = []
    missing = 0
    invalid: List[Tuple[int, Any, str]] = []
    for row, value in enumerate(values, start=1):
        if value is None or (isinstance(value, str) and not value.strip()):
            missing += 1
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            invalid.append((row, value, "not a number"))
            continue
        if not math.isfinite(number):
            invalid.append((row, value, "not finite"))
            continue
        if lower is not None and number < lower:
            invalid.append((row, value, f"below the lower bound {lower:g}"))
            continue
        if upper is not None and number > upper:
            invalid.append((row, value, f"above the upper bound {upper:g}"))
            continue
        kept.append(number * scale)
    if invalid and not drop_invalid:
        row, value, why = invalid[0]
        raise ValueError(
            f"{len(invalid)} invalid value(s); the first is row {row}, {value!r}: "
            f"{why}. Correct the data, or leave them out with drop_invalid "
            f"(--drop-invalid)"
        )
    return kept, missing, len(invalid)


def fit_column(
    values: Sequence[Any],
    *,
    unit: str = "s",
    candidates: Optional[Sequence[str]] = None,
    lower: Optional[float] = None,
    upper: Optional[float] = None,
    bins: Optional[int] = None,
    min_count: int = 20,
    tolerance: float = 0.25,
    drop_invalid: bool = False,
) -> FitResult:
    """
    Fits a column of measured times to desflow's distribution families.

    Args:
        values: The data, as numbers or text; an empty cell or None is missing.
        unit: The data's unit: `ms`, `s`, `min` or `h`. Settings are in seconds.
        candidates: The families to try; None tries every family. `empirical` is
            always offered unless the list leaves it out.
        lower: The lowest plausible value, in the data's unit; a value below it is
            invalid, and a family that can draw below it is flagged.
        upper: The highest plausible value, in the same way.
        bins: Offer the empirical distribution as a histogram with this many bins,
            rather than as the values themselves.
        min_count: The fewest values a family is fitted to. Defaults to 20.
        tolerance: How far a fitted family's mean and standard deviation may be from
            the data's, as a share of the data's. Defaults to 0.25.
        drop_invalid: Leave invalid values out rather than raising.

    Returns:
        FitResult: Every candidate's outcome, and the best.

    Raises:
        ValueError: If the unit or a candidate is unknown, or a value is invalid
            and `drop_invalid` is False.
        ImportError: If the fit extra is not installed.
    """
    if unit not in _UNITS:
        raise ValueError(f"unit must be one of {', '.join(_UNITS)}, got {unit!r}")
    names = list(candidates) if candidates is not None else list(DISTRIBUTIONS)
    unknown = [n for n in names if n not in DISTRIBUTIONS]
    if unknown:
        raise ValueError(
            f"no family called {', '.join(unknown)}; choose from "
            f"{', '.join(DISTRIBUTIONS)}"
        )
    scale = _UNITS[unit]
    data, missing, invalid = _clean(values, scale, lower, upper, drop_invalid)
    low = None if lower is None else lower * scale
    high = None if upper is None else upper * scale
    scipy = _scipy()
    x = np.asarray(data, dtype=float)
    found = []
    for name in names:
        if name == "empirical":
            continue
        reason = _skip(name, x, min_count, low)
        if reason:
            found.append(Candidate(name, "skipped", reason))
            continue
        try:
            parameters, k = _FITS[name](scipy, x, low)
        except Exception as exc:  # noqa: BLE001 - every failure is a table row
            found.append(Candidate(name, "did not fit", f"{type(exc).__name__}: {exc}"))
            continue
        found.append(_checked(scipy, name, parameters, k, x, tolerance, low, high))
    if "empirical" in names:
        found.append(_empirical(x, bins))
    return FitResult(
        candidates=found,
        values=list(data),
        unit=unit,
        rows=len(values),
        missing=missing,
        invalid=invalid,
        lower=low,
        upper=high,
    )


def _skip(name: str, x: np.ndarray, min_count: int, low: Optional[float]) -> str:
    """Why a family is not fitted to this data, or empty when it is."""
    if len(x) == 0:
        return "no values"
    constant = bool(np.all(x == x[0]))
    if name == "constant":
        return "" if constant else "the values are not all equal"
    if constant:
        return "every value is the same, so only constant fits"
    if len(x) < min_count:
        return f"{len(x)} values, fewer than the minimum count {min_count}"
    if name in _POSITIVE_ONLY and np.any(x <= 0):
        return f"{name} needs every value above 0; {int(np.sum(x <= 0))} are not"
    if name == "truncated_normal" and np.any(x < (low or 0.0)):
        return f"truncated_normal needs every value at or above low {low or 0.0:g}"
    return ""


def _fit_truncated_normal(scipy: Any, x: np.ndarray, low: Optional[float]):
    """Maximum likelihood for mean and std of a normal cut below at `low`."""
    floor = low or 0.0
    stats = scipy.stats

    def negative(params: np.ndarray) -> float:
        mean, log_std = params
        std = math.exp(log_std)
        a = (floor - mean) / std
        return -float(np.sum(stats.truncnorm.logpdf(x, a, np.inf, loc=mean, scale=std)))

    start = np.array([x.mean(), math.log(x.std())])
    found = scipy.optimize.minimize(negative, start, method="Nelder-Mead")
    if not found.success:
        raise RuntimeError(found.message)
    mean, log_std = found.x
    return {"mean": float(mean), "std": math.exp(log_std), "low": floor}, 2


def _fit_lognormal(scipy: Any, x: np.ndarray, low: Optional[float]):
    s, _, scale = scipy.stats.lognorm.fit(x, floc=0)
    mean = scale * math.exp(s * s / 2)
    return {"mean": float(mean), "std": float(mean * math.sqrt(math.expm1(s * s)))}, 2


_FITS: Dict[str, Any] = {
    "exponential": lambda sp, x, low: (
        {"rate": 1.0 / float(sp.stats.expon.fit(x, floc=0)[1])},
        1,
    ),
    "normal": lambda sp, x, low: (
        dict(zip(("mean", "std"), map(float, sp.stats.norm.fit(x)))),
        2,
    ),
    "lognormal": _fit_lognormal,
    "truncated_normal": _fit_truncated_normal,
    "constant": lambda sp, x, low: ({"value": float(x[0])}, 1),
    "uniform": lambda sp, x, low: (
        (lambda loc, scale: {"low": float(loc), "high": float(loc + scale)})(
            *sp.stats.uniform.fit(x)
        ),
        2,
    ),
    "triangular": lambda sp, x, low: (
        (
            lambda c, loc, scale: {
                "low": float(loc),
                "mode": float(loc + c * scale),
                "high": float(loc + scale),
            }
        )(*sp.stats.triang.fit(x)),
        3,
    ),
    "gamma": lambda sp, x, low: (
        (lambda a, loc, scale: {"shape": float(a), "scale": float(scale)})(
            *sp.stats.gamma.fit(x, floc=0)
        ),
        2,
    ),
    "weibull": lambda sp, x, low: (
        (lambda c, loc, scale: {"shape": float(c), "scale": float(scale)})(
            *sp.stats.weibull_min.fit(x, floc=0)
        ),
        2,
    ),
}


def _frozen(scipy: Any, name: str, p: Dict[str, Any]) -> Any:
    """The scipy distribution with desflow's parameters, for the diagnostics."""
    stats = scipy.stats
    if name == "exponential":
        return stats.expon(scale=1.0 / p["rate"])
    if name == "normal":
        return stats.norm(p["mean"], p["std"])
    if name == "lognormal":
        sigma = math.sqrt(math.log1p(p["std"] ** 2 / p["mean"] ** 2))
        return stats.lognorm(sigma, scale=p["mean"] * math.exp(-sigma * sigma / 2))
    if name == "truncated_normal":
        a = (p["low"] - p["mean"]) / p["std"]
        return stats.truncnorm(a, np.inf, loc=p["mean"], scale=p["std"])
    if name == "uniform":
        return stats.uniform(p["low"], p["high"] - p["low"])
    if name == "triangular":
        width = p["high"] - p["low"]
        return stats.triang((p["mode"] - p["low"]) / width, p["low"], width)
    if name == "gamma":
        return stats.gamma(p["shape"], scale=p["scale"])
    if name == "weibull":
        return stats.weibull_min(p["shape"], scale=p["scale"])
    return None


def _support(name: str, p: Dict[str, Any]) -> Tuple[float, float]:
    """The range a family with these parameters can draw from."""
    if name == "normal":
        return -math.inf, math.inf
    if name in ("uniform", "triangular"):
        return p["low"], p["high"]
    if name == "truncated_normal":
        return p["low"], math.inf
    if name == "constant":
        return p["value"], p["value"]
    return 0.0, math.inf


def _bounds(
    name: str, p: Dict[str, Any], low: Optional[float], high: Optional[float]
) -> str:
    start, end = _support(name, p)
    notes = []
    if low is not None and start < low:
        notes.append(f"can draw below the lower bound {low:g}")
    if high is not None and end > high:
        notes.append(f"can draw above the upper bound {high:g}")
    return "; ".join(notes)


def _checked(
    scipy: Any,
    name: str,
    parameters: Dict[str, Any],
    k: int,
    x: np.ndarray,
    tolerance: float,
    low: Optional[float],
    high: Optional[float],
) -> Candidate:
    """A fitted family's row: rejected when its parameters or moments are off."""
    if not all(math.isfinite(v) for v in parameters.values()):
        return Candidate(
            name, "rejected", "a fitted parameter is not finite", parameters
        )
    try:
        config = DistributionConfig(dist=name, **parameters)
    except ValueError as exc:
        return Candidate(
            name, "rejected", f"out of the family's range: {exc}", parameters
        )
    family = get_distribution(name)
    mean, std = x.mean(), x.std()
    fitted_mean, fitted_std = family.mean(config), math.sqrt(family.variance(config))
    for what, ours, theirs in (("mean", fitted_mean, mean), ("std", fitted_std, std)):
        if abs(ours - theirs) > tolerance * abs(theirs):
            return Candidate(
                name,
                "rejected",
                f"its {what} is {ours:.4g}, the data's is {theirs:.4g}: more than "
                f"{tolerance:.0%} apart",
                parameters,
            )
    candidate = Candidate(name, "fitted", parameters=parameters)
    if name != "constant":
        frozen = _frozen(scipy, name, parameters)
        candidate.log_likelihood = float(np.sum(frozen.logpdf(x)))
        candidate.aic = 2 * k - 2 * candidate.log_likelihood
        test = scipy.stats.kstest(x, frozen.cdf)
        candidate.ks_statistic = float(test.statistic)
        candidate.ks_p_value = float(test.pvalue)
        if not math.isfinite(candidate.log_likelihood):
            candidate.status = "rejected"
            candidate.reason = "a value lies outside where the family can draw"
    candidate.bounds = _bounds(name, parameters, low, high)
    return candidate


def _empirical(x: np.ndarray, bins: Optional[int]) -> Candidate:
    """The data itself, or its histogram: always offered when there is data."""
    if len(x) == 0:
        return Candidate("empirical", "skipped", "no values")
    if bins:
        counts, edges = np.histogram(x, bins=bins)
        parameters = {
            "edges": [float(e) for e in edges],
            "counts": [float(c) for c in counts],
        }
    else:
        parameters = {"values": [float(v) for v in x]}
    return Candidate("empirical", "fitted", parameters=parameters)
