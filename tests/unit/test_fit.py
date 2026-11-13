"""Input fitting: candidates, safeguards, the settings a model loads, and !include."""

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pytest
from typer.testing import CliRunner

from desflow import (
    DistributionConfig,
    Sampler,
    SimParameter,
    SimulationContext,
    fit_column,
)
from desflow.cli import app as cli
from desflow.blueprint.loader import BlueprintError
from desflow.models.distributions import get_distribution
from desflow.analysis import fit as fitting

pytest.importorskip("scipy")

# The 40 packing times of the tutorials, in seconds
PACKING = [
    1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
    2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
    1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
    2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
]  # fmt: skip


def _row(result, family):
    return next(r for r in result.table if r["family"] == family)


def test_every_family_is_tried_and_the_best_has_the_lowest_aic():
    result = fit_column(PACKING, lower=0)
    assert [c.family for c in result.candidates][-1] == "empirical"
    fitted = [r for r in result.table if r["status"] == "fitted" and r["aic"]]
    inside = [r for r in fitted if not r["bounds"]]
    assert result.best == min(inside, key=lambda r: r["aic"])["family"]
    assert _row(result, "normal")["bounds"] == "can draw below the lower bound 0"
    assert _row(result, "constant")["status"] == "skipped"
    for row in fitted:
        assert 0 <= row["ks_p_value"] <= 1 and row["log_likelihood"] < 0


def test_the_counting_families_are_not_tried_unless_named():
    """`poisson` and `geometric` count events, so a fit of times leaves them out; named,
    they are skipped with a reason and not reported as a failure to fit."""
    families = [c.family for c in fit_column(PACKING, lower=0).candidates]
    assert "poisson" not in families and "geometric" not in families
    named = fit_column(PACKING, candidates=["poisson", "geometric", "normal"])
    for family in ("poisson", "geometric"):
        row = _row(named, family)
        assert row["status"] == "skipped" and "counts whole numbers" in row["reason"]


def test_parameters_are_desflow_s_and_match_the_data():
    """Each fitted family's mean is close to the data's, in desflow's parameters."""
    result = fit_column(PACKING)
    gamma = _row(result, "gamma")["parameters"]
    assert gamma["shape"] * gamma["scale"] == pytest.approx(1.5, rel=0.01)
    lognormal = _row(result, "lognormal")["parameters"]
    config = DistributionConfig(dist="lognormal", **lognormal)
    family = get_distribution("lognormal")
    # desflow's lognormal takes the variable's mean and std, which the fit kept
    assert family.mean(config) == pytest.approx(np.mean(PACKING), rel=0.01)
    assert family.variance(config) ** 0.5 == pytest.approx(np.std(PACKING), rel=0.1)
    assert _row(result, "exponential")["status"] == "rejected"  # std far too large
    assert "std is 1.5, the data's is 0.4832" in _row(result, "exponential")["reason"]


def test_units_are_converted_to_seconds():
    minutes = fit_column([v / 60 for v in PACKING], unit="min", candidates=["gamma"])
    seconds = fit_column(PACKING, candidates=["gamma"])
    assert minutes.settings()["shape"] == pytest.approx(seconds.settings()["shape"])
    assert minutes.settings()["scale"] == pytest.approx(seconds.settings()["scale"])


def test_zeros_skip_the_positive_only_families():
    result = fit_column([0.0, 0.0] + PACKING)
    for family in ("gamma", "weibull", "lognormal"):
        row = _row(result, family)
        assert row["status"] == "skipped"
        assert row["reason"] == f"{family} needs every value above 0; 2 are not"
    assert result.best not in ("gamma", "weibull", "lognormal")


@pytest.mark.parametrize("value", [2.0, 0.1])
def test_a_constant_column_offers_constant_only(value):
    """30 copies of 0.1 have a float std of 2.8e-17, which is still no spread."""
    result = fit_column([value] * 30)
    assert result.best == "constant" and result.settings() == {
        "dist": "constant",
        "value": value,
    }
    others = [r for r in result.table if r["family"] not in ("constant", "empirical")]
    assert {r["status"] for r in others} == {"skipped"}
    assert {r["reason"] for r in others} == {
        "every value is the same, so only constant fits"
    }


def test_a_small_sample_skips_every_family_and_offers_the_values():
    result = fit_column(PACKING[:12])
    rows = [r for r in result.table if r["family"] not in ("constant", "empirical")]
    assert {r["reason"] for r in rows} == {"12 values, fewer than the minimum count 20"}
    assert result.best == "empirical"
    assert result.settings() == {"dist": "empirical", "values": PACKING[:12]}
    assert fit_column(PACKING[:12], min_count=10).best != "empirical"


def test_a_heavy_outlier_gives_table_rows_not_an_exception():
    """One value of 120 s among packing times near 1.5 s: the families that stay
    above 0 are rejected on their moments, and the data's own values are chosen
    over a normal that can draw below 0."""
    result = fit_column(PACKING + [120.0], lower=0)
    statuses = {r["family"]: r["status"] for r in result.table}
    for family in ("gamma", "weibull", "lognormal", "exponential"):
        assert statuses[family] == "rejected"
        assert "more than 25% apart" in _row(result, family)["reason"]
    assert statuses["normal"] == "fitted" and _row(result, "normal")["bounds"]
    assert result.best == "empirical"
    assert fit_column(PACKING + [120.0]).best == "normal"  # no bounds stated


def test_a_failed_fit_and_a_bad_result_are_rows(monkeypatch):
    scipy = pytest.importorskip("scipy.stats")

    def fails(sp, x, low):
        raise scipy.FitError(
            "Optimization converged to parameters that are outside the range"
        )

    monkeypatch.setitem(fitting._FITS, "gamma", fails)
    monkeypatch.setitem(
        fitting._FITS,
        "weibull",
        lambda sp, x, low: ({"shape": float("inf"), "scale": 1.0}, 2),
    )
    monkeypatch.setitem(
        fitting._FITS, "lognormal", lambda sp, x, low: ({"mean": -1.0, "std": 1.0}, 2)
    )
    result = fit_column(PACKING)
    gamma, weibull, lognormal = (
        _row(result, f) for f in ("gamma", "weibull", "lognormal")
    )
    assert gamma["status"] == "did not fit"
    assert gamma["reason"].startswith("FitError: Optimization converged")
    assert (weibull["status"], weibull["reason"]) == (
        "rejected",
        "a fitted parameter is not finite",
    )
    assert lognormal["status"] == "rejected"
    assert lognormal["reason"].startswith("out of the family's range")
    assert result.best not in ("gamma", "weibull", "lognormal")
    with pytest.raises(ValueError, match="gamma is not usable"):
        result.settings("gamma")


def test_nothing_usable_when_there_is_no_data():
    result = fit_column(["", None, " "])
    assert result.best is None and result.missing == 3
    with pytest.raises(
        ValueError, match="no candidate is usable.*keep empirical in the candidates"
    ):
        result.settings()


def test_invalid_values_stop_the_fit_unless_dropped():
    data = PACKING + ["n/a", "inf", "-1"]
    with pytest.raises(ValueError, match="3 invalid value\\(s\\); the first is row 41"):
        fit_column(data, lower=0)
    result = fit_column(data, lower=0, drop_invalid=True)
    assert (result.rows, result.invalid, len(result.values)) == (43, 3, 40)


def test_bounds_flag_and_refuse():
    result = fit_column(PACKING, lower=0, upper=3)
    assert "can draw above the upper bound 3" in _row(result, "gamma")["bounds"]
    assert _row(result, "uniform")["bounds"] == ""
    with pytest.raises(ValueError, match="above the upper bound 2"):
        fit_column(PACKING, upper=2)


def test_unknown_candidates_and_units_are_refused():
    with pytest.raises(ValueError, match="no family called 'beta'"):
        fit_column(PACKING, candidates=["gamma", "beta"])
    with pytest.raises(ValueError, match="unit must be one of"):
        fit_column(PACKING, unit="days")


def test_a_histogram_with_bins():
    result = fit_column(PACKING, candidates=["empirical"], bins=4)
    settings = result.settings()
    assert len(settings["edges"]) == 5 and sum(settings["counts"]) == 40


@pytest.mark.parametrize("bins", [10, None])
def test_the_empirical_settings_keep_values_that_six_digits_would_make_equal(bins):
    """200 values within 1 s of 100000 s: six digits leave 2 distinct values."""
    values = [str(100000 + v) for v in np.random.default_rng(1).uniform(0, 1, 200)]
    settings = fit_column(values, candidates=["empirical"], bins=bins).settings()
    DistributionConfig(**settings)
    if bins is None:
        assert len(set(settings["values"])) == 200


@pytest.mark.parametrize("family", ["uniform", "triangular", "normal"])
def test_a_parameter_keeps_every_digit_when_six_would_move_the_fit(family):
    """300 values within 0.3 s of 100000 s: six digits make `low` and `high` equal,
    and move a normal's mean by more than a thousandth of its std."""
    values = [str(100000.1 + v) for v in np.random.default_rng(1).uniform(0, 0.3, 300)]
    settings = fit_column(values, candidates=[family]).settings()
    DistributionConfig(**settings)
    assert any(v != float(f"{v:.6g}") for v in settings.values() if v != family)


def test_a_constant_keeps_every_digit_when_six_would_move_it():
    """A std of 0 allows no move of the mean: 1234567.8 rounds to 1234570."""
    settings = fit_column(["1234567.8"] * 30).settings()
    assert settings == {"dist": "constant", "value": 1234567.8}


def test_the_fit_extra_is_named_when_scipy_is_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "scipy", None)
    monkeypatch.setitem(sys.modules, "scipy.stats", None)
    with pytest.raises(ImportError, match="desflow\\[fit\\]"):
        fit_column(PACKING)


def _shop_yaml(tmp_path, packing):
    path = tmp_path / "shop.yaml"
    path.write_text(
        "simulation: {sim_id: Shop, factor: 0, random_seed: 42}\n"
        "resources: {packers: {current_cap: 1, max_cap: 1}}\n"
        "arrivals: {orders: {dist: exponential, rate: 0.5, spawn: pack}}\n"
        f"services:\n  packing: {packing}\n"
        "tasks: {pack: {service: packing, resource: packers, payload: {e: 1}}}\n"
        "stats: typical\n"
        "run: {until: 300}\n"
    )
    return path


@pytest.mark.parametrize("kind", ["triangular", "gamma", "empirical"])
def test_fitted_settings_load_in_all_three_apis(tmp_path, kind):
    """The file desflow fit writes loads with !include and draws as the builder and
    the low-level API draw with the same settings."""
    result = fit_column(PACKING)
    (tmp_path / "packing.yaml").write_text(result.yaml(kind))
    app = SimulationContext.from_yaml(_shop_yaml(tmp_path, "!include packing.yaml"))
    app.run()
    from_yaml = app.env.stats.readings()["Shop.stats.tasks.pack.service_mean"]

    builder = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_service("packing", **result.settings(kind))
        .compile_parameters()
    )
    written = SimParameter(
        sim_id="Shop",
        service={"packing": DistributionConfig(**result.settings(kind))},
    )
    loaded = app.compile_parameters().service["packing"]
    assert builder.service["packing"] == written.service["packing"] == loaded
    assert from_yaml > 0


_LAYOUT_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "blueprint_layout.py"
_layout_spec = importlib.util.spec_from_file_location(
    "blueprint_layout", _LAYOUT_SCRIPT
)
assert _layout_spec is not None and _layout_spec.loader is not None
blueprint_layout = importlib.util.module_from_spec(_layout_spec)
_layout_spec.loader.exec_module(blueprint_layout)


@pytest.mark.parametrize(
    "source",
    ["", "packing.csv, column packing_seconds", "observed_" * 8 + ".csv, column x"],
)
def test_the_fitted_settings_are_in_the_blueprint_layout(tmp_path, source):
    # Every usable family, an empirical list of 40 values among them, and a header
    # long enough to wrap: `blueprint_layout.py --check` leaves each file as written
    result = fit_column(PACKING)
    kinds = [c.family for c in result.candidates if c.usable]
    assert "empirical" in kinds
    files = []
    for kind in kinds:
        text = result.yaml(kind, source=source)
        assert max(map(len, text.splitlines())) <= 88
        files.append(tmp_path / f"{kind}.yaml")
        files[-1].write_text(text)
    assert blueprint_layout.main(["--check", *map(str, files)]) == 0


def test_read_column_drops_the_byte_order_mark_of_a_csv(tmp_path):
    """Excel's "CSV UTF-8" starts the file with a byte order mark, which is not part
    of the first column's name."""
    path = tmp_path / "gaps.csv"
    path.write_bytes("gap,sku\r\n1.5,a\r\n2,b\r\n".encode("utf-8-sig"))

    assert fitting.read_column(path, "gap") == ["1.5", "2"]


def test_include_errors_name_the_file_and_line(tmp_path):
    with pytest.raises(
        BlueprintError, match=r"shop.yaml:5: !include missing.yaml: no such file"
    ):
        SimulationContext.from_yaml(_shop_yaml(tmp_path, "!include missing.yaml"))
    (tmp_path / "broken.yaml").write_text("dist: gamma\nshape: [1,\n")
    with pytest.raises(BlueprintError, match=r"broken.yaml:3"):
        SimulationContext.from_yaml(_shop_yaml(tmp_path, "!include broken.yaml"))
    (tmp_path / "list.yaml").write_text("- 1\n- 2\n")
    with pytest.raises(
        BlueprintError, match=r"list.yaml:1: an included file must hold a mapping"
    ):
        SimulationContext.from_yaml(_shop_yaml(tmp_path, "!include list.yaml"))
    (tmp_path / "bad.yaml").write_text("dist: gamma\nshape: -1\nscale: 1\n")
    with pytest.raises(BlueprintError, match=r"shop.yaml:5: services.packing"):
        SimulationContext.from_yaml(_shop_yaml(tmp_path, "!include bad.yaml"))


def test_cli_writes_the_settings_and_the_plot(tmp_path):
    data = tmp_path / "packing.csv"
    data.write_text(
        "order,packing_seconds\n" + "".join(f"{i},{v}\n" for i, v in enumerate(PACKING))
    )
    out, plot = tmp_path / "fitted" / "packing.yaml", tmp_path / "fit.png"
    run = CliRunner().invoke(
        cli,
        [
            "fit",
            str(data),
            "--column",
            "packing_seconds",
            "--lower",
            "0",
            "--out",
            str(out),
            "--plot",
            str(plot),
        ],
    )
    assert run.exit_code == 0, run.output
    assert "* best: triangular" in run.output
    assert out.read_text() == fit_column(PACKING, lower=0).yaml(
        source="packing.csv, column packing_seconds"
    )
    assert plot.read_bytes()[:4] == b"\x89PNG"
    empty = tmp_path / "empty.csv"
    empty.write_text("packing_seconds\n\n\n")
    assert (
        CliRunner()
        .invoke(cli, ["fit", str(empty), "--column", "packing_seconds"])
        .exit_code
        == 1
    )
    unknown = CliRunner().invoke(
        cli, ["fit", str(data), "--column", "packing_seconds", "--candidates", "beta"]
    )
    assert unknown.exit_code == 2 and "no family called 'beta'" in unknown.output
    missing = CliRunner().invoke(cli, ["fit", str(data), "--column", "seconds"])
    assert missing.exit_code == 2 and "has no column 'seconds'" in missing.output


def test_plotted_values_are_drawn_as_a_run_draws_them():
    """The plot's values come through a Sampler, so a run's smallest delay applies:
    a normal centred on 0 gives no negative value."""
    settings = {"dist": "normal", "mean": 0.0, "std": 1.0}
    values = fitting._generate(settings, 2000)
    sampler = Sampler(seed=0)
    config = DistributionConfig(**settings)
    assert list(values) == [sampler.sample(config) for _ in range(2000)]
    assert values.min() == 0 and np.mean(values == 0) > 0.4


def test_an_include_of_itself_is_refused(tmp_path):
    (tmp_path / "a.yaml").write_text("dist: gamma\nshape: !include b.yaml\n")
    (tmp_path / "b.yaml").write_text("scale: !include a.yaml\n")
    with pytest.raises(
        BlueprintError,
        match=r"b.yaml:1: !include a.yaml: includes itself; include another file",
    ):
        SimulationContext.from_yaml(_shop_yaml(tmp_path, "!include a.yaml"))
    (tmp_path / "c.yaml").write_text("dist: gamma\nshape: !include c.yaml\n")
    with pytest.raises(
        BlueprintError, match=r"c.yaml:2: !include c.yaml: includes itself"
    ):
        SimulationContext.from_yaml(_shop_yaml(tmp_path, "!include c.yaml"))


def test_include_adds_keys_to_the_file(tmp_path):
    """An arrival's fitted gaps, with the task it spawns added by the blueprint."""
    (tmp_path / "gaps.yaml").write_text("dist: exponential\nrate: 0.5\n")
    blueprint = tmp_path / "shop.yaml"
    text = _shop_yaml(tmp_path, "{dist: constant, value: 1}").read_text()
    blueprint.write_text(
        text.replace(
            "arrivals: {orders: {dist: exponential, rate: 0.5, spawn: pack}}",
            "arrivals:\n  orders: !include {file: gaps.yaml, spawn: pack}",
        )
    )
    app = SimulationContext.from_yaml(blueprint)
    app.run()
    assert app.compile_parameters().arrival["orders"].rate == 0.5
    assert app.env.stats.readings()["Shop.stats.tasks.pack.finished"] > 100
    blueprint.write_text(blueprint.read_text().replace("file: gaps.yaml, ", ""))
    with pytest.raises(BlueprintError, match="a mapping with file and the keys to add"):
        SimulationContext.from_yaml(blueprint)


NAN = float("nan")


@pytest.mark.parametrize(
    "options,name",
    [
        ({"tolerance": NAN}, "tolerance"),
        ({"tolerance": -1}, "tolerance"),
        ({"tolerance": float("inf")}, "tolerance"),
        ({"tolerance": True}, "tolerance"),
        ({"lower": NAN}, "lower"),
        ({"upper": NAN}, "upper"),
        ({"lower": 5, "upper": 1}, "lower must be below upper"),
    ],
)
def test_fit_column_refuses_a_tolerance_or_bound_no_fit_can_use(options, name):
    with pytest.raises(ValueError, match=name):
        fit_column(PACKING, **options)


@pytest.mark.parametrize(
    "options,name",
    [
        (["--tolerance", "nan"], "tolerance"),
        (["--tolerance", "-1"], "tolerance"),
        (["--lower", "nan"], "lower"),
        (["--lower", "5", "--upper", "1"], "lower must be below upper"),
    ],
)
def test_the_fit_command_refuses_them_with_the_options_name(tmp_path, options, name):
    data = tmp_path / "packing.csv"
    data.write_text(
        "order,packing_seconds\n" + "".join(f"{i},{v}\n" for i, v in enumerate(PACKING))
    )
    run = CliRunner().invoke(
        cli, ["fit", str(data), "--column", "packing_seconds", *options]
    )
    assert run.exit_code == 2, run.output
    assert name in run.output


def test_candidates_are_stripped_of_spaces(tmp_path):
    data = tmp_path / "packing.csv"
    data.write_text(
        "order,packing_seconds\n" + "".join(f"{i},{v}\n" for i, v in enumerate(PACKING))
    )
    run = CliRunner().invoke(
        cli,
        [
            "fit",
            str(data),
            "--column",
            "packing_seconds",
            "--candidates",
            "normal, gamma",
        ],
    )
    assert run.exit_code == 0, run.output
    assert "normal" in run.output and "gamma" in run.output
