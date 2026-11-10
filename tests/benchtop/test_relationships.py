"""Batch 7 relationships against the benchtop projects' Python generators.

Each test runs the blueprint form of a benchtop row (the BT rows of the known-cases list)
in its smallest model. Where the project has a function that can be called (`next_hour`,
`daily_features`, `pages`, `generate_history`, `load_locations`) the test calls it. Where
the rule lives in a function that cannot be called alone, because it needs the running
context (live-dashboard's `visit`, ecommerce-cdc's `moves`, game-leaderboard's late
score) the test states the rule and compares the properties the project's own tests check. The projects are read only.

Rows compared here: BT-AQ2 and BT-AQ8 (air-quality `next_hour` and `daily_features`),
BT-LD7 (live-dashboard's returning visitor), BT-EC6 and BT-EC7 (ecommerce-cdc's moves and
`pages`), BT-GL7 (game-leaderboard's late score) and BT-PR3 and BT-PR4 (product-recommender's
`load_locations` and history). BT-WL7 has no Python generator, because web-and-logs is
planned and has no folder yet, so its test states the rule of the row: every record of
a visit carries the visit's tenant.
"""

import contextlib
import csv
import statistics
import sys
import textwrap
import types
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List

import numpy as np
import pytest

from tests.conformance.harness import run_yaml_text

HEAD = (
    "simulation: {sim_id: B, factor: 0, random_seed: %d, "
    'logical_start_time: "2026-01-01T00:00:00+00:00", timezone: UTC}\n'
)


def _events(outcome: Any, task: str) -> List[Dict[str, Any]]:
    assert outcome.error is None, outcome.error
    return [
        r["value"]
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["task"] == task
        and isinstance(r["value"], dict)
        and "status" not in r["value"]
    ]


def _lag1(values: List[float]) -> float:
    x = np.asarray(values, dtype=float)
    x = x - x.mean()
    return float((x[1:] * x[:-1]).sum() / (x * x).sum())


def test_air_quality_hidden_weather_follows_the_projects_chain(tmp_path, project):
    """BT-AQ2: temperature and wind deviations, the wet or dry chain, the rain on a wet
    hour and the direction walk, each carried from hour to hour with `previous` and a
    `default`, are compared with `next_hour` run from its calm first state (`_CALM`): lag 1
    and deviation of the temperature and the wind, the shares of wet after wet and after
    dry, the mean rain on a wet hour, and the deviation of the direction steps."""
    project("air-quality")
    from airq.feature.generator import _CALM, next_hour

    n = 12000
    text = HEAD % 4 + textwrap.dedent(
        f"""\
        arrivals: {{h: {{dist: constant, value: 1, spawn: hour, max_events: {n}}}}}
        tasks:
          hour:
            vars:
              wet: {{gen: boolean, p: {{gen: expr, expr: "0.78 if w else 0.06", names: {{w: {{gen: previous, path: wet, default: false}}}}}}}}
            payload:
              temperature: {{gen: expr, expr: "0.96 * p + e", names: {{p: {{gen: previous, path: temperature, default: 0}}, e: {{gen: normal, mean: 0, std: 0.8}}}}}}
              wind: {{gen: expr, expr: "0.9 * p + e", names: {{p: {{gen: previous, path: wind, default: 0}}, e: {{gen: normal, mean: 0, std: 1.8}}}}}}
              wet: {{gen: var, name: wet}}
              rain: {{gen: expr, expr: "r if wet else 0", names: {{r: {{gen: sample, dist: exponential, rate: 1.6666666666666667}}}}}}
              direction: {{gen: expr, expr: "(p + e) % 360", names: {{p: {{gen: previous, path: direction, default: 180}}, e: {{gen: normal, mean: 0, std: 27}}}}}}
        run: {{until: {n + 1}}}
        """
    )
    mine = _events(run_yaml_text(tmp_path, text, {}), "hour")
    assert len(mine) == n
    rng = np.random.default_rng(4)
    state, theirs = dict(_CALM), []
    for _ in range(n):
        state = next_hour(rng, state)
        theirs.append(state)
    for field in ("temperature", "wind"):
        a, b = [r[field] for r in mine], [r[field] for r in theirs]
        # Lag 1 of 12,000 hours has an error of 0.0025 (temperature, 0.96) or 0.004
        # (wind, 0.9), so the difference of two runs has 0.0036 or 0.0057 and 0.02 is
        # 3.5 times the larger. The deviation of an AR(1) series of this length has a
        # relative error of 4.5% (0.96) or 2.8% (0.9), so a difference of two has 6.4%
        # at most and 20% is over 3 times it.
        assert _lag1(a) == pytest.approx(_lag1(b), abs=0.02)
        assert statistics.pstdev(a) == pytest.approx(statistics.pstdev(b), rel=0.2)
    for rows, label in ((mine, "mine"), (theirs, "theirs")):
        wet = [bool(r["wet"]) for r in rows]
        after_wet = [b for a, b in zip(wet, wet[1:]) if a]
        after_dry = [b for a, b in zip(wet, wet[1:]) if not a]
        # About 2,570 wet hours give a share an error of 0.008 (0.03 is 3.7 times it) and
        # about 9,400 dry hours one of 0.0025 (0.01 is 4 times it)
        assert statistics.fmean(after_wet) == pytest.approx(0.78, abs=0.03), label
        assert statistics.fmean(after_dry) == pytest.approx(0.06, abs=0.01), label
        rain = [r["rain"] for r in rows if r["wet"]]
        # The mean of about 2,570 exponential draws has a relative error of 2% (7% is
        # 3.5 times it)
        assert statistics.fmean(rain) == pytest.approx(0.6, rel=0.07), label
        assert all(r["rain"] == 0 for r in rows if not r["wet"]), label
        steps = [
            (b["direction"] - a["direction"] + 180) % 360 - 180
            for a, b in zip(rows, rows[1:])
        ]
        # The deviation of 12,000 steps has a relative error of 0.65% (3% is 4.6 times it)
        assert statistics.pstdev(steps) == pytest.approx(27, rel=0.03), label
        assert all(0 <= r["direction"] < 360 for r in rows), label
    assert (
        mine[0]["temperature"] != 0 and mine[0]["direction"] != 180
    )  # the first hour uses the defaults


def test_air_quality_daily_rows_equal_the_projects_daily_features(tmp_path, project):
    """BT-AQ8: the daily mean of 24 readings and the previous day's mean, and the daily
    mean of the 24 forecasts of one lead for one day (`aggregate` with `where`), equal the
    rows `daily_features` makes from the same hourly rows: the air rows exactly, and the
    weather means within 0.0101, because both round to 2 places."""
    project("air-quality")
    from airq.feature.features import daily_features
    from airq.feature.generator import generate
    from airq.core.models import Observation, WeatherForecast

    days = 12
    origin = datetime(2026, 1, 1, tzinfo=timezone.utc)
    forecasts: List[Any] = []
    observations: List[Any] = []
    hours = generate(origin, 9)
    for _ in range(days * 24):
        for row in next(hours):
            (observations if isinstance(row, Observation) else forecasts).append(row)
    assert all(isinstance(f, WeatherForecast) for f in forecasts)
    with open(tmp_path / "obs.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["pm2_5"])
        writer.writerows([[o.pm2_5] for o in observations])
    with open(tmp_path / "fc.csv", "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "lead_days",
                "forecast_for",
                "temperature_2m",
                "precipitation",
                "wind_speed_10m",
            ]
        )
        writer.writerows(
            [
                [
                    f.lead_days,
                    f.forecast_for.isoformat(),
                    f.temperature_2m,
                    f.precipitation,
                    f.wind_speed_10m,
                ]
                for f in forecasts
            ]
        )
    weather_tasks = "".join(
        f"""\
  dw{lead}:
    vars:
      day: {{gen: format_time, time: {{gen: now, offset: {86400 * lead}}}, format: "%Y-%m-%d"}}
      tmean: {{gen: aggregate, task: forecast, path: temperature_2m, last: 24, function: mean, where: "lead_days == {lead} and forecast_for[:10] == day"}}
      wmean: {{gen: aggregate, task: forecast, path: wind_speed_10m, last: 24, function: mean, where: "lead_days == {lead} and forecast_for[:10] == day"}}
      wet: {{gen: aggregate, task: forecast, last: 24, function: count, where: "lead_days == {lead} and forecast_for[:10] == day and precipitation > 0"}}
    payload: {{day: {{gen: var, name: day}}, lead_days: {lead}, temperature_2m: {{gen: expr, expr: "round(tmean, 2)"}}, wind_speed_10m: {{gen: expr, expr: "round(wmean, 2)"}}, wet_hours: {{gen: var, name: wet}}}}
"""
        for lead in range(1, 8)
    )
    weather_arrivals = "".join(
        f"  d{lead}: {{dist: constant, value: 86400, first_at: 84600, spawn: dw{lead}}}\n"
        for lead in range(1, 8)
    )
    text = (
        HEAD % 1
        + "arrivals:\n"
        + "  hours: {dist: constant, value: 3600, first_at: 0, spawn: reading}\n"
        + "  forecasts: {dist: constant, value: 3600, first_at: 0, group: 7, spawn: forecast}\n"
        + "  air: {dist: constant, value: 86400, first_at: 84600, spawn: daily_air}\n"
        + weather_arrivals
        + "tasks:\n"
        + "  reading:\n    vars: {o: {gen: row, file: obs.csv, order: sequential}}\n"
        + "    payload: {pm2_5: {gen: var, name: o, path: pm2_5}}\n"
        + "  forecast:\n    vars: {f: {gen: row, file: fc.csv, order: sequential}}\n"
        + "    payload: {lead_days: {gen: var, name: f, path: lead_days}, forecast_for: {gen: var, name: f, path: forecast_for}, temperature_2m: {gen: var, name: f, path: temperature_2m}, precipitation: {gen: var, name: f, path: precipitation}, wind_speed_10m: {gen: var, name: f, path: wind_speed_10m}}\n"
        + "  daily_air:\n    vars:\n      m: {gen: aggregate, task: reading, path: pm2_5, last: 24, function: mean}\n"
        + "      epoch_s: {gen: format_time, format: epoch_s}\n"
        + '    payload:\n      day: {gen: format_time, format: "%Y-%m-%d"}\n'
        + '      pm2_5: {gen: expr, expr: "round(m, 2)"}\n'
        + '      is_weekend: {gen: expr, expr: "((epoch_s // 86400 + 3) % 7) >= 5"}\n'
        + "      pm2_5_lag1: {gen: previous, path: pm2_5, default: null}\n"
        + weather_tasks
        + f"run: {{until: {days * 86400 - 1}}}\n"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    weather, air = daily_features(forecasts, observations)
    mine_air = {r["day"]: r for r in _events(outcome, "daily_air")}
    assert len(air) == days - 1
    for row in air:
        got = mine_air[row.day.isoformat()]
        assert got["pm2_5"] == pytest.approx(row.pm2_5, abs=1e-9)
        assert got["pm2_5_lag1"] == pytest.approx(row.pm2_5_lag1, abs=1e-9)
        assert got["is_weekend"] == row.is_weekend
    mine_weather = {
        (r["day"], r["lead_days"]): r
        for lead in range(1, 8)
        for r in _events(outcome, f"dw{lead}")
    }
    assert len(weather) > 40
    for row in weather:
        got = mine_weather[(row.day.isoformat(), row.lead_days)]
        assert got["temperature_2m"] == pytest.approx(
            row.temperature_2m, abs=0.0101
        )  # a mean on a half cent rounds either way
        assert got["wind_speed_10m"] == pytest.approx(
            row.wind_speed_10m, abs=0.0101
        )  # a mean on a half cent rounds either way
        assert got["wet_hours"] == row.wet_hours


def _timed(outcome: Any, task: str) -> List[Any]:
    """The (simulation time, payload) of each finished event of a task, in order."""
    assert outcome.error is None, outcome.error
    return [
        (r["sim_ts"], r["value"])
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["task"] == task
        and isinstance(r["value"], dict)
        and "status" not in r["value"]
    ]


def test_a_returning_visitor_is_an_earlier_user_and_a_new_user_needs_a_purchase(
    tmp_path, project
):
    """BT-LD7: a visitor is a returning one with the chance `returning` once a user
    exists, a returning visitor who buys uses an earlier user chosen with equal chance,
    and a new user is made only when a visitor who is not a returning one buys. These
    are the rules of `visit` in `sales/simulation/run.py` (a closure of `build`, so it
    cannot be called alone), with the chances of `sales.core.config.CHANCES`."""
    project("live-dashboard")
    from sales.core.config import CHANCES

    visits = 6000
    text = HEAD % 11 + textwrap.dedent(
        f"""\
        arrivals: {{v: {{dist: constant, value: 1, max_events: {visits}, spawn: visit}}}}
        tasks:
          visit:
            vars:
              ret: {{gen: boolean, p: {CHANCES["returning"]}}}
              buys: {{gen: boolean, p: {CHANCES["buy"]}}}
              known: {{gen: aggregate, task: new_user, last: 1000000, function: count}}
            payload:
              visitor: {{gen: sequence}}
              returning: {{gen: expr, expr: "ret and known > 0"}}
              buys: {{gen: var, name: buys}}
              known: {{gen: var, name: known}}
            next:
              when:
                - {{condition: "buys and ret and known > 0", task: order_back}}
                - {{condition: "buys", task: new_user}}
              default: left
          order_back:
            payload:
              visitor: {{gen: upstream, path: visitor}}
              user_id: {{gen: lookup, task: new_user, path: user_id}}
          new_user:
            payload: {{visitor: {{gen: upstream, path: visitor}}, user_id: {{gen: sequence}}}}
          left: {{payload: {{event_type: left}}}}
        run: {{until: {visits + 2}}}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    visit = {v["visitor"]: v for _, v in _timed(outcome, "visit")}
    created = _timed(outcome, "new_user")
    back = _timed(outcome, "order_back")
    first_user = created[0][0]
    # a new user is made only for a visitor who bought and was not a returning one
    assert len(created) == sum(
        1 for v in visit.values() if v["buys"] and not v["returning"]
    )
    assert all(
        visit[v["visitor"]]["buys"] and not visit[v["visitor"]]["returning"]
        for _, v in created
    )
    # a returning visitor exists only after an earlier purchase, and uses an earlier user
    born = {v["user_id"]: t for t, v in created}
    assert all(not v["returning"] for v in visit.values() if v["known"] == 0)
    assert all(t >= first_user for t, _ in back)
    assert all(born[v["user_id"]] <= t for t, v in back)
    # the share of visitors who return, once a user exists (sampling error about 0.01)
    known = [v for v in visit.values() if v["known"] > 0]
    assert statistics.fmean(bool(v["returning"]) for v in known) == pytest.approx(
        CHANCES["returning"], abs=0.03
    )
    # the user is chosen with equal chance: where it stands among the users made so far
    # falls in each quarter with the share the project's `rng.integers(len(users))` gives
    rng = np.random.default_rng(3)
    quarters_mine: Counter = Counter()
    quarters_theirs: Counter = Counter()
    order = {user: n for n, user in enumerate(sorted(born, key=born.get))}
    for t, v in back:
        count = sum(1 for b in born.values() if b <= t)
        if count < 40:
            continue
        quarters_mine[int(4 * (order[v["user_id"]] + 0.5) / count)] += 1
        quarters_theirs[int(4 * (int(rng.integers(count)) + 0.5) / count)] += 1
    total = sum(quarters_mine.values())
    assert total > 500
    for quarter in range(4):
        # about 0.016 sampling error at 700 draws, so 0.06 is nearly 4 times it
        assert quarters_mine[quarter] / total == pytest.approx(0.25, abs=0.06)
        assert quarters_theirs[quarter] / sum(
            quarters_theirs.values()
        ) == pytest.approx(0.25, abs=0.06)


def _history_with_a_stub(monkeypatch: pytest.MonkeyPatch, add_cleanup: Any) -> Any:
    """Imports `recommender.data.history` of the product-recommender with a stand-in for
    `recommender.engine.simulation`, which imports scikit-learn, and which desflow does
    not need. It gives the module and the list of times `time_context` was called with.
    `add_cleanup` takes the function that puts `sys.modules` and the package back as
    they were, so a later import of the module is not bound to the stand-in."""
    name, package = "recommender.data.history", "recommender.data"
    prior = sys.modules.pop(name, None)
    parent = sys.modules.get(package)
    prior_attribute = getattr(parent, "history", None) if parent else None
    if parent is not None and hasattr(parent, "history"):
        delattr(parent, "history")

    def restore() -> None:
        sys.modules.pop(name, None)
        if prior is not None:
            sys.modules[name] = prior
        package_now = sys.modules.get(package)
        if package_now is not None:
            if prior_attribute is not None:
                setattr(package_now, "history", prior_attribute)
            elif hasattr(package_now, "history"):
                delattr(package_now, "history")

    add_cleanup(restore)
    seen: List[Any] = []
    stub = types.ModuleType("recommender.engine.simulation")

    def time_context(moment: Any) -> dict:
        seen.append(moment)
        return {}

    stub.will_click = lambda ctx, item, fake: 0  # type: ignore[attr-defined]
    stub.time_context = time_context  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "recommender.engine.simulation", stub)
    from recommender.data import history

    return history, seen


def test_the_stub_for_the_history_module_is_removed_afterwards(project):
    project("product-recommender")
    pytest.importorskip("faker")
    pytest.importorskip("pandas")
    name = "recommender.data.history"
    sys.modules.pop(name, None)
    with contextlib.ExitStack() as stack, pytest.MonkeyPatch.context() as patch:
        _history_with_a_stub(patch, stack.callback)
        assert name in sys.modules
    assert name not in sys.modules
    assert "recommender.engine.simulation" not in sys.modules


def test_history_events_pick_a_random_user_and_product_over_ninety_days(
    tmp_path, project, monkeypatch, request
):
    """BT-PR4: 10,000 events over 90 days, each with a user and a product chosen with
    equal chance. `generate_history` of the product-recommender runs with a stub for
    `will_click` and `time_context` (its click formula is not the row), and its users,
    products and times are compared with the blueprint's `lookup` and `row`."""
    project("product-recommender")
    pytest.importorskip("faker")
    pandas = pytest.importorskip("pandas")
    from faker import Faker

    history, seen = _history_with_a_stub(monkeypatch, request.addfinalizer)
    events, users, products = 10_000, 5, 4
    (tmp_path / "users.csv").write_text(
        "user_id,age\n" + "".join(f"{n},{20 + n}\n" for n in range(1, users + 1))
    )
    (tmp_path / "products.csv").write_text(
        "product_id,price\n" + "".join(f"{n},{n}.5\n" for n in range(1, products + 1))
    )
    monkeypatch.setattr(history, "USER_FEATURES", tmp_path / "users.csv")
    monkeypatch.setattr(history, "PRODUCT_FEATURES", tmp_path / "products.csv")
    monkeypatch.setattr(history, "TRAINING_LOG", tmp_path / "log.csv")
    monkeypatch.setattr(history, "NUM_PAST_EVENTS", events)
    fake = Faker()
    fake.seed_instance(12)
    history.generate_history(fake)
    theirs = pandas.read_csv(tmp_path / "log.csv")
    assert len(theirs) == events and len(seen) == events
    their_span = (max(seen) - min(seen)).total_seconds()
    text = HEAD % 12 + textwrap.dedent(
        f"""\
        arrivals:
          people: {{dist: constant, value: 1, max_events: {users}, spawn: user}}
          history: {{dist: exponential, rate: 0.001286, first_at: 10, spawn: event, max_events: {events}}}
        tasks:
          user: {{payload: {{user_id: {{gen: sequence, start: 1}}}}}}
          event:
            vars:
              u: {{gen: lookup, task: user}}
              product: {{gen: row, file: products.csv}}
            payload: {{user: {{gen: var, name: u, path: user_id}}, product: {{gen: var, name: product, path: product_id}}}}
        run: {{until: {100 * 86400}}}
        """
    )
    mine = _timed(run_yaml_text(tmp_path, text, {}), "event")
    assert len(mine) == events
    # 10,000 draws give a share an error of 0.004 at one in five and 0.0043 at one in four, so 0.015 is over 3 times it
    for ident in range(1, users + 1):
        assert sum(1 for _, v in mine if v["user"] == ident) / events == pytest.approx(
            1 / users, abs=0.015
        )
        # the log leaves out `user_id`, and the age of user n is 20 + n
        assert (theirs["age"] == 20 + ident).mean() == pytest.approx(
            1 / users, abs=0.015
        )
    for ident in range(1, products + 1):
        assert sum(
            1 for _, v in mine if v["product"] == ident
        ) / events == pytest.approx(1 / products, abs=0.015)
        assert (theirs["product_id"] == ident).mean() == pytest.approx(
            1 / products, abs=0.015
        )
    # the blueprint's 10,000 gaps sum to 90 days with a relative error of 0.01, so 0.05
    # is 5 times it; the project draws its times between two dates 90 days apart, so its
    # span is 90 days less about 90 days / 10,000 at each end
    mine_span = mine[-1][0] - mine[0][0]
    assert mine_span == pytest.approx(90 * 86400, rel=0.05)
    assert their_span == pytest.approx(90 * 86400, rel=0.01)


def test_a_move_picks_the_latest_row_of_a_uniform_user(tmp_path, project):
    """BT-EC6: `lookup` with `latest_by` picks one user in equal chance and gives the
    newest row of that user, as `moves` of the ecommerce-cdc run does with its list of
    current users (`state.users[index]`, which a move replaces). `moves` waits on the
    running context's arrival, so it cannot be called alone. No project code is called:
    the test states that rule, and skips when the project is absent to stay with the
    other project rows."""
    project("ecommerce-cdc")
    ids, reads = 4, 400
    text = HEAD % 5 + textwrap.dedent(
        f"""\
        arrivals:
          writes: {{dist: constant, value: 1, first_at: 1, spawn: write}}
          reads: {{dist: constant, value: 1, first_at: 1.5, max_events: {reads}, spawn: read}}
        tasks:
          write:
            vars: {{n: {{gen: sequence}}}}
            payload: {{n: {{gen: var, name: n}}, id: {{gen: expr, expr: "n % {ids}"}}}}
          read: {{payload: {{n: {{gen: lookup, task: write, path: n, latest_by: id}}}}}}
        run: {{until: {reads + 2}}}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    writes = [(t, v["id"], v["n"]) for t, v in _timed(outcome, "write")]
    chosen = Counter()
    newest = 0
    for when, value in _timed(outcome, "read"):
        latest: Dict[int, int] = {}
        for t, ident, n in writes:
            if t <= when:
                latest[ident] = n
        assert value["n"] in latest.values()
        newest += value["n"] == max(latest.values())
        chosen[[i for i, n in latest.items() if n == value["n"]][0]] += 1
    for count in chosen.values():
        # 400 draws give a share an error of 0.022 at one in four, so 0.08 is 3.6 times it
        assert count / reads == pytest.approx(1 / ids, abs=0.08)
    # the newest row of all is one user's row among four, not the one always given
    assert newest / reads == pytest.approx(1 / ids, abs=0.08)


def test_a_session_counts_its_pages_from_the_page_before(tmp_path, project):
    """BT-EC7: a visit has one id, browser and traffic source; its pages are the home
    page, one to four products (uniform), and the cart and purchase pages when it buys,
    as `pages` of the ecommerce-cdc run gives; and each page has the sequence number of
    the page before plus one, from 1, because `upstream` reads the record of the task
    that routed to it."""
    project("ecommerce-cdc")
    from types import SimpleNamespace

    from ecommerce.simulation.shop import pages

    visits = 4000
    text = HEAD % 2 + textwrap.dedent(
        f"""\
        arrivals: {{v: {{dist: constant, value: 1, max_events: {visits}, spawn: home}}}}
        tasks:
          home:
            vars:
              pages: {{gen: integer, bounds: [1, 4]}}
              browser: {{gen: weighted, options: {{Chrome: 1, Safari: 1, Firefox: 1}}}}
              source: {{gen: weighted, options: {{Organic: 1, Facebook: 1, Search: 1, Email: 1}}}}
              buys: {{gen: boolean, p: 0.5}}
            set:
              pages: {{gen: var, name: pages}}
              seen: 0
              buys: {{gen: var, name: buys}}
            payload:
              visit: {{gen: sequence}}
              sequence_number: 1
              page: home
              browser: {{gen: var, name: browser}}
              traffic_source: {{gen: var, name: source}}
            next: browse
          browse:
            set: {{seen: {{gen: expr, expr: "attr.seen + 1"}}}}
            payload:
              visit: {{gen: upstream, path: visit}}
              sequence_number: {{gen: expr, expr: "s + 1", names: {{s: {{gen: upstream, path: sequence_number}}}}}}
              page: product
              browser: {{gen: upstream, path: browser}}
              traffic_source: {{gen: upstream, path: traffic_source}}
            next: {{when: [{{condition: "attr.seen < attr.pages", task: browse}}, {{condition: "attr.buys", task: cart}}]}}
          cart:
            payload:
              visit: {{gen: upstream, path: visit}}
              sequence_number: {{gen: expr, expr: "s + 1", names: {{s: {{gen: upstream, path: sequence_number}}}}}}
              page: cart
              browser: {{gen: upstream, path: browser}}
              traffic_source: {{gen: upstream, path: traffic_source}}
            next: purchase
          purchase:
            payload:
              visit: {{gen: upstream, path: visit}}
              sequence_number: {{gen: expr, expr: "s + 1", names: {{s: {{gen: upstream, path: sequence_number}}}}}}
              page: purchase
              browser: {{gen: upstream, path: browser}}
              traffic_source: {{gen: upstream, path: traffic_source}}
        run: {{until: {visits + 2}}}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    by_visit: Dict[Any, List[Any]] = {}
    for task in ("home", "browse", "cart", "purchase"):
        for t, v in _timed(outcome, task):
            by_visit.setdefault(v["visit"], []).append(v)
    assert len(by_visit) == visits
    mine: Counter = Counter()
    for rows in by_visit.values():
        rows.sort(key=lambda row: row["sequence_number"])
        assert [r["sequence_number"] for r in rows] == list(range(1, len(rows) + 1))
        names = [r["page"] for r in rows]
        assert names[0] == "home"
        products = [n for n in names if n == "product"]
        assert 1 <= len(products) <= 4
        assert names[1 : 1 + len(products)] == products
        assert names[1 + len(products) :] in ([], ["cart", "purchase"])
        assert len({r["browser"] for r in rows}) == 1
        assert len({r["traffic_source"] for r in rows}) == 1
        mine[len(products)] += 1
    rng = np.random.default_rng(1)
    catalogue = [SimpleNamespace(id=i) for i in range(10)]
    theirs: Counter = Counter()
    for _ in range(visits):
        shown = pages(rng, catalogue, True)
        theirs[sum(1 for kind, _ in shown if kind == "product")] += 1
    assert set(theirs) == {1, 2, 3, 4} == set(mine)
    for count in (1, 2, 3, 4):
        # 4,000 visits give a share an error of 0.0068 at one in four, so 0.03 is over 4 times it
        assert mine[count] / visits == pytest.approx(0.25, abs=0.03)
        assert theirs[count] / visits == pytest.approx(0.25, abs=0.03)


def test_a_late_score_keeps_the_time_it_was_earned(tmp_path, project):
    """BT-GL7: a late score, 1 in 100 as in `run.py` (`late_share` 0.01), is sent again
    by the task `next` routes to, after a wait of Normal(420, 120) seconds (the
    `offline` service of `run.py`), with the `earned_ms` of the first record read by
    `upstream`. The share, the mean wait and the carried time are the properties checked by
    `test_late_scores_carry_the_time_they_were_earned` of the game-leaderboard, and the
    score itself carries the time it was earned (a lag of 0)."""
    project("game-leaderboard")
    scores, share = 30000, 0.01
    start_ms = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    text = HEAD % 6 + textwrap.dedent(
        f"""\
        services: {{offline: {{dist: normal, mean: 420, std: 120}}}}
        arrivals: {{s: {{dist: constant, value: 1, max_events: {scores}, spawn: score}}}}
        tasks:
          score:
            vars: {{late: {{gen: boolean, p: {share}}}}}
            payload: {{player: {{gen: sequence}}, earned_ms: {{gen: format_time, format: epoch_ms}}, event_type: normal}}
            next: {{when: [{{condition: late, task: send_late}}], default: done}}
          send_late:
            service: offline
            payload:
              player: {{gen: upstream, path: player}}
              earned_ms: {{gen: upstream, path: earned_ms}}
              event_type: late
          done: {{payload: {{event_type: done}}}}
        run: {{until: {scores + 3000}}}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    first = {v["player"]: (t, v["earned_ms"]) for t, v in _timed(outcome, "score")}
    assert all(earned == start_ms + round(t * 1000) for t, earned in first.values())
    late = _timed(outcome, "send_late")
    # 30,000 scores give a share an error of 0.0006, so 0.003 is 5 times it
    assert len(late) / scores == pytest.approx(share, abs=0.003)
    lags = []
    for t, v in late:
        sent, earned = first[v["player"]]
        assert v["earned_ms"] == earned
        lags.append(t - sent)
    # about 300 late scores: the mean has an error of 7 s and the deviation of 5 s, so
    # 42 s and 24 s are 6 and 5 times them
    assert statistics.fmean(lags) == pytest.approx(420, rel=0.1)
    assert statistics.pstdev(lags) == pytest.approx(120, rel=0.2)


def test_a_user_area_is_drawn_by_population(tmp_path, project):
    """BT-PR3: `row` with `where` and `weight` draws the postal areas of the city by
    population, with the shares `load_locations` of the product-recommender gives."""
    path = project("product-recommender")
    pytest.importorskip("faker")
    from recommender.data.locations import load_locations

    draws = 8000
    text = HEAD % 8 + textwrap.dedent(
        f"""\
        arrivals: {{u: {{dist: constant, value: 1, max_events: {draws}, spawn: user}}}}
        tasks:
          user:
            vars: {{place: {{gen: row, file: "{(path / "data" / "world_pop.csv").as_posix()}", where: "country == 'Australia' and city == 'Melbourne'", weight: population}}}}
            payload: {{postal_code: {{gen: var, name: place, path: postal_code}}}}
        run: {{until: {draws + 2}}}
        """
    )
    mine = Counter(
        str(v["postal_code"])
        for _, v in _timed(run_yaml_text(tmp_path, text, {}), "user")
    )
    locations = load_locations()
    expected = {
        str(a["postal_code"]): w for a, w in zip(locations.areas, locations.weights)
    }
    assert set(mine) <= set(expected) and sum(mine.values()) == draws
    for code, weight in expected.items():
        # 8,000 draws give a share an error of at most 0.0056 (at a weight of one half),
        # so 0.02 is over 3.5 times it
        assert mine[code] / draws == pytest.approx(weight, abs=0.02)


def test_every_record_of_a_visit_carries_the_visits_tenant(tmp_path):
    """BT-WL7: the requests and the confirmation of a visit carry its tenant by
    `upstream`. The web-and-logs project has no folder yet, so there is no Python
    original to compare with and the rule of the row is stated here."""
    visits = 300
    text = HEAD % 9 + textwrap.dedent(
        f"""\
        arrivals: {{v: {{dist: constant, value: 1, max_events: {visits}, spawn: hit}}}}
        tasks:
          hit:
            vars_once: {{tenant: {{gen: weighted, options: {{acme: 1, globex: 1, initech: 1}}}}}}
            payload: {{visit: {{gen: sequence}}, tenant: {{gen: var, name: tenant}}}}
            next: request
          request:
            payload: {{visit: {{gen: upstream, path: visit}}, tenant: {{gen: upstream, path: tenant}}}}
            next: confirmation
          confirmation:
            payload: {{visit: {{gen: upstream, path: visit}}, tenant: {{gen: upstream, path: tenant}}}}
        run: {{until: {visits + 2}}}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    seen: Dict[Any, set] = {}
    for task in ("hit", "request", "confirmation"):
        for _, v in _timed(outcome, task):
            seen.setdefault(v["visit"], set()).add(v["tenant"])
    assert len(seen) == visits
    assert all(len(tenants) == 1 for tenants in seen.values())
    assert len({next(iter(t)) for t in seen.values()}) == 3


def test_gallery_orders_belong_to_existing_customers(tmp_path):
    """GA-4: every order's customer is one of a sign-up with an earlier or equal time."""
    text = HEAD % 42 + textwrap.dedent(
        """\
        arrivals: {signups: {dist: exponential, rate: 0.1, spawn: sign_up}, orders: {dist: exponential, rate: 1, spawn: place_order}}
        tasks:
          sign_up: {payload: {customer_id: {gen: sequence}}}
          place_order: {payload: {customer_id: {gen: lookup, task: sign_up, path: customer_id}}}
        run: {until: 1000}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    first: Dict[Any, float] = {}
    for t, v in _timed(outcome, "sign_up"):
        first.setdefault(v["customer_id"], t)
    orders = _timed(outcome, "place_order")
    assert len(orders) > 100
    assert all(
        v["customer_id"] in first and first[v["customer_id"]] <= t for t, v in orders
    )


def test_gallery_sensors_drift_from_the_previous_value(tmp_path):
    """GA-6: exactly five sensors appear and a sensor's readings differ by at most 1."""
    text = HEAD % 42 + textwrap.dedent(
        """\
        arrivals: {reading: {dist: constant, value: 1, spawn: sensor_reading, count: 5, key: {gen: sequence, format: "sensor-{}"}}}
        tasks: {sensor_reading: {payload: {sensor_id: {gen: entity}, value: {gen: add, args: [{gen: previous, path: value, default: 0}, {gen: uniform, low: -1, high: 1}]}}}}
        run: {until: 100}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    by_sensor: Dict[Any, List[float]] = {}
    for _, v in _timed(outcome, "sensor_reading"):
        by_sensor.setdefault(v["sensor_id"], []).append(v["value"])
    assert len(by_sensor) == 5
    assert all(len(values) > 50 for values in by_sensor.values())
    steps = [
        abs(b - a) for values in by_sensor.values() for a, b in zip(values, values[1:])
    ]
    assert max(steps) <= 1
