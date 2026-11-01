"""Batch 4 generators against the benchtop projects' Python generators.

Each test runs the blueprint form of a benchtop row (the BT rows of the known-cases
list) and the project's own Python function, and compares them. Where the project draws
from numpy, the comparison is of the formula (equal numbers for equal inputs) or of the
shape of many draws (a mean and quantiles), as the property checks of the known-cases
list say. The projects are read only.

Rows compared here: BT-AQ3, BT-AQ4, BT-AQ5 (air-quality) and BT-EC1 to BT-EC4
(ecommerce-cdc). The other Batch 4 rows are not in a Python function of a project that
can be imported: order-streams generates in Java and the others in code that needs a
running stack.
"""

import math
import statistics
import textwrap
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, List

import numpy as np
import pytest

from tests.conformance.harness import run_yaml_text

HEAD = (
    "simulation: {sim_id: B, factor: 0, random_seed: %d, "
    'logical_start_time: "2026-01-01T00:00:00+00:00", timezone: UTC}\n'
)


def _events(outcome: Any) -> List[Any]:
    assert outcome.error is None, outcome.error
    return [
        r["value"]
        for r in outcome.records
        if r["stream_type"] == "event" and "status" not in r["value"]
    ]


def test_air_quality_cycles_and_weekday_equal_the_projects_formulas(tmp_path, project):
    """BT-AQ4 and BT-AQ5: the daily and yearly cycles and the weekday effect, for each
    hour of two weeks, equal `wave` and `hour.weekday() < 5` of the project."""
    project("air-quality")
    from airq.feature.generator import wave

    pi = "3.141592653589793"
    text = (
        HEAD % 1
        + "arrivals: {h: {replay: {path: h.csv}, spawn: t}}\n"
        + "tasks:\n  t:\n    vars:\n      epoch_s: {gen: format_time, format: epoch_s}\n"
        + "    payload:\n"
        + f"      temperature: {{gen: expr, expr: '18 + 5 * cos(2 * {pi} * (epoch_s / 3600 - 360) / 8766) + 3 * cos(2 * {pi} * (epoch_s / 3600 - 4.5) / 24)'}}\n"
        + f"      evening: {{gen: expr, expr: 'cos(2 * {pi} * (epoch_s / 3600 - 17) / 24)'}}\n"
        + "      weekday: {gen: expr, expr: '1 if ((epoch_s // 86400 + 3) % 7) < 5 else 0'}\n"
    )
    hours = 24 * 14
    found = _events(
        run_yaml_text(tmp_path, text, {"h": [3600.0 * i for i in range(hours)]})
    )
    assert len(found) == hours
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index, record in enumerate(found):
        hour = start + timedelta(hours=index)
        expected = 18 + 5 * wave(hour, 8766, 15 * 24) + 3 * wave(hour, 24, 4.5)
        assert record["temperature"] == pytest.approx(expected, abs=1e-6)
        assert record["evening"] == pytest.approx(wave(hour, 24, 17), abs=1e-9)
        assert record["weekday"] == (1 if hour.weekday() < 5 else 0)


def test_air_quality_rain_is_exponential_with_the_projects_mean(tmp_path, project):
    """BT-AQ3: `sample` of an exponential with rate 1/0.6 has the mean and quantiles of
    `rng.exponential(0.6)`, which `next_hour` draws for a wet hour."""
    project("air-quality")
    from airq.feature.generator import next_hour

    n = 4000
    text = (
        HEAD % 2
        + "arrivals: {h: {replay: {path: h.csv}, spawn: t}}\n"
        + "tasks:\n  t:\n    payload:\n"
        + "      rain: {gen: sample, dist: exponential, rate: 1.6666666666666667}\n"
    )
    mine = [
        r["rain"]
        for r in _events(
            run_yaml_text(tmp_path, text, {"h": list(map(float, range(n)))})
        )
    ]
    rng = np.random.default_rng(2)
    calm = {
        "temperature": 0.0,
        "wind": 0.0,
        "wet": True,
        "rain": 0.0,
        "direction": 180.0,
    }
    theirs: List[float] = []
    while len(theirs) < n:
        state = next_hour(rng, calm)
        if state["wet"]:
            theirs.append(state["rain"])
    assert statistics.fmean(mine) == pytest.approx(0.6, rel=0.06)
    assert statistics.fmean(mine) == pytest.approx(statistics.fmean(theirs), rel=0.08)
    for q in (0.5, 0.9):
        assert float(np.quantile(mine, q)) == pytest.approx(
            float(np.quantile(theirs, q)), rel=0.12
        )


def test_ecommerce_user_fields_follow_the_projects_rules(tmp_path, project):
    """BT-EC1 to BT-EC4: a user's city is chosen by population as `shop.city` does, the
    first name belongs to the user's gender, the email is the lowercased names, and the
    street address is a Faker address; the same holds for the project's `new_user`."""
    pytest.importorskip("faker")
    project("ecommerce-cdc")
    from faker import Faker

    from ecommerce.simulation import shop
    from ecommerce.simulation.catalogue import CITIES

    n = 3000
    rows = ["country,state,city,postal_code,latitude,longitude,population"]
    for c in CITIES:
        rows.append(
            f"{c.country},{c.state},{c.city},{c.postal_code},{c.latitude},{c.longitude},{c.population}"
        )
    (tmp_path / "cities.csv").write_text("\n".join(rows) + "\n")
    text = (
        HEAD % 3
        + "arrivals: {u: {replay: {path: u.csv}, spawn: new_user}}\n"
        + textwrap.dedent(
            """\
            tasks:
              new_user:
                vars_once:
                  city: {gen: row, file: cities.csv, weight: population}
                  person: {gen: one_of, choices: [{gender: F, first: {gen: fake, method: first_name_female}}, {gender: M, first: {gen: fake, method: first_name_male}}]}
                  last: {gen: fake, method: last_name}
                payload:
                  first_name: {gen: var, name: person, path: first}
                  last_name: {gen: var, name: last}
                  email: {gen: lower, value: {gen: format, template: "{f}.{l}@example.com", names: {f: {gen: var, name: person, path: first}, l: {gen: var, name: last}}}}
                  gender: {gen: var, name: person, path: gender}
                  street_address: {gen: fake, method: street_address}
                  city: {gen: var, name: city, path: city}
            """
        )
    )
    mine = _events(run_yaml_text(tmp_path, text, {"u": list(map(float, range(n)))}))
    assert len(mine) == n

    fake = Faker()
    fake.seed_instance(3)
    rng = np.random.default_rng(3)
    theirs = [shop.new_user(fake, rng, "2026-01-01") for _ in range(n)]

    from faker.providers.person.en_US import Provider

    male = set(Provider.first_names_male)
    female = set(Provider.first_names_female)
    for record in mine:
        assert record["first_name"] in (male if record["gender"] == "M" else female)
        assert (
            record["email"]
            == f"{record['first_name'].lower()}.{record['last_name'].lower()}@example.com"
        )
        assert record["street_address"]
    for user in theirs:
        assert user.first_name in (male if user.gender == "M" else female)
        assert user.email.startswith(
            f"{user.first_name.lower()}.{user.last_name.lower()}@"
        )

    total = sum(c.population for c in CITIES)
    mine_share = Counter(r["city"] for r in mine)
    their_share = Counter(u.city for u in theirs)
    weight = Counter()
    for c in CITIES:
        weight[c.city] += c.population / total
    for name, expected in weight.items():
        assert mine_share[name] / n == pytest.approx(expected, abs=0.03)
        assert their_share[name] / n == pytest.approx(expected, abs=0.03)
    assert Counter(r["gender"] for r in mine)["M"] / n == pytest.approx(0.5, abs=0.04)
    assert Counter(u.gender for u in theirs)["M"] / n == pytest.approx(0.5, abs=0.04)
    assert math.isclose(sum(c.population / total for c in CITIES), 1.0)
