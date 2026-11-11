"""BT-LD1 to BT-LD17: the live-dashboard shop.

The original is the project's own model, `sales.simulation.run.build`
(its copy in `tests/copied_projects/benchtop/live-dashboard`, read only), run unpaced to the document's end with a stand-in sampler that gives fixed
draws, and with `publish_event` recorded. Its imports of `asyncpg`, which only its
store needs, are answered with a stand-in module when it is not installed.

The fixed draws are numbers of the visitor (`v`, the order of the visit's start) or of
the order (`m`, the order of its creation), so the YAML and the project take the same
value at the same event whatever the order the events happen in. The project finds
`v` and `m` by the function that asks (`visit`, `fulfil`) and the calls it makes.

- a gap between visitors is `1 / rate` (the live rate);
- the pages of a visit are `2 + (3 * v) % 4`, and each lasts `mean + 0.0013 * v`;
- a visitor is a returning one when `v * 0.6180339887 % 1` is below the chance and a
  user exists, and picks the oldest user; buys when `v * 0.7548776662 % 1` is below the
  chance;
- the new user's age, gender, country and traffic source are a number of `v`, and the
  ids are numbers of `v` too (`00000000-0000-4000-8000-` and twelve digits: a role, `v`
  and the item's place), so the YAML makes the ids the project makes;
- the order's item count is read from `v * 0.5698402909 % 1`, its items are the first
  products (even `v`) or the last (odd `v`), its patience is 20 s for each third order
  and the live mean otherwise, its packing is 12 times the live mean for each fourth
  order, its transit and return are the live mean, and it is returned when
  `m * 0.3247179572 % 1` is below the live chance. Each time has a small part of `m`, so
  no two events meet.

Only the draws are replaced: the document's chances of a returning visitor and of a
purchase (the weights of `next`) and of a return (the `param` of `sent_back`) stay, so
a change of one of them changes the YAML's records.

`compare` checks that the rows of the four tables the YAML writes equal the rows the
project writes, in time, order, table and every field, and that the run reaches what
the rows name. A timestamp of the YAML has the milliseconds the engine's `now` gives,
where the project's has microseconds.
"""

import inspect
import re
import sys
import types
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest import mock

import numpy as np

from desflow import SimulationContext
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.sampler import Sampler

from tests.known_cases.bt_support import in_project, stand_in_modules

HERE = Path(__file__).parent
VARIABLES = {"FACTOR": "0"}
UNTIL = 300
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
CONFIGS = [{"variables": {}, "args": {}}]
RETURN = 0.6180339887
BUY = 0.7548776662
COUNT = 0.5698402909
SEND_BACK = 0.3247179572
PAGES = 0.0013
SLOW = 12
PREFIX = "00000000-0000-4000-8000-"
# the project's catalogue order of the keys of each `choose`
GENDERS = ["M", "F"]
COUNTRIES = [
    "China",
    "Brasil",
    "South Korea",
    "France",
    "United States",
    "United Kingdom",
]
SOURCES = ["Search", "Organic", "Facebook", "Email", "Display"]
# the live changes of the document's scenario, as `control.py` sends them: the path in
# the project's registry, and the same change in the document (the fixed document has a
# constant gap, so the rate is the gap)
CHANGES = [
    (100.1, "sales.arrival.visitor.rate", 6),
    (150.1, "sales.resources.pickers.current_cap", 6),
    (200.1, "sales.service.pick.mean", 8),
    (250.1, "sales.variables.buy", 0.4),
]


def _identifier(role: int, v: int, place: int = 0) -> str:
    """The id both sides make for a role (1 user, 2 order, 3 item), a visitor and an
    item's place."""
    return f"{PREFIX}{role}{v:09d}{place:02d}"


def _expression(text: str, **names: Any) -> dict[str, Any]:
    """An expression generator, with the names it reads."""
    node: dict[str, Any] = {"gen": "expr", "expr": text}
    if names:
        node["names"] = names
    return node


def _parameter(path: str) -> dict[str, Any]:
    """A generator that reads a registry value."""
    return {"gen": "param", "path": path}


def fix(document: dict[str, Any]) -> dict[str, Any]:
    """The document with the project's fixed draws (see the module's description).

    Args:
        document: The loaded `model.yaml`.

    Returns:
        dict[str, Any]: The same document with the draws fixed.
    """
    arrival = document["arrivals"]["visitor"]
    gap = 1.0 / arrival.pop("rate")
    arrival["dist"] = "constant"
    arrival["value"] = gap
    arrival["vars"] = {"seq": {"gen": "sequence", "start": 1}}
    arrival["attributes"]["seq"] = {"gen": "var", "name": "seq"}
    arrival["attributes"]["pages"] = _expression("2 + (3 * seq) % 4")
    for step in document["scenario"]:  # the gap is the constant's value
        if step["path"] == "sales.arrival.visitor.rate":
            step["path"] = "sales.arrival.visitor.value"
            step["value"] = 1.0 / step["value"]
    tasks = document["tasks"]
    seq = "attr.seq"
    # the chance of a returning visitor, from the document's weights of `next`
    returning = tasks["catalogue_check"]["next"]["weights"]["pick_user"]
    tasks["catalogue_check"]["vars"] = {"u": _expression(f"({seq} * {RETURN}) % 1")}
    tasks["catalogue_check"]["next"] = {
        "when": [{"condition": f"u < ({returning})", "task": "pick_user"}],
        "default": "no_user",
    }
    tasks["pick_user"]["set"]["user"]["strategy"] = "first"
    tasks["pages"]["service_time"] = _expression(
        f"m + {seq} * {PAGES}", m=_parameter("sales.service.page_view.mean")
    )
    buy = tasks["buy_choice"]["next"]["weights"]["buyer"]  # the chance of a purchase
    tasks["buy_choice"]["vars"] = {"u": _expression(f"({seq} * {BUY}) % 1")}
    tasks["buy_choice"]["next"] = {
        "when": [{"condition": f"u < ({buy})", "task": "buyer"}],
        "default": "walk_away",
    }
    user = tasks["new_user"]
    user["vars"]["uid"] = _expression(
        f'"{PREFIX}1" + str(1000000000 + {seq})[1:] + "00"'
    )
    payload = user["payload"]
    payload["age"] = _expression(f"12 + ({seq} * 7) % 59")
    payload["gender"] = _expression(f"{GENDERS!r}[{seq} % 2]")
    payload["country"] = _expression(f"{COUNTRIES!r}[({seq} * 5) % 6]")
    payload["traffic_source"] = _expression(f"{SOURCES!r}[({seq} * 3) % 5]")
    order = tasks["order"]
    order["vars"]["oid"] = _expression(
        f'"{PREFIX}2" + str(1000000000 + {seq})[1:] + "00"'
    )
    order["vars"]["n"] = _expression(
        "1 if u < 0.7 else 2 if u < 0.9 else 3 if u < 0.95 else 4",
        u=_expression(f"({seq} * {COUNT}) % 1"),
    )
    order["vars"]["ids"] = _expression(
        f'["{PREFIX}3" + str(1000000000 + {seq})[1:] + str(100 + j)[1:] for j in range(n)]'
    )
    products = order["set"]["products"]
    first = {**products, "value": {**products["value"], "strategy": "first"}}
    last = {**products, "value": {**products["value"], "strategy": "last"}}
    order["set"]["products"] = {
        "gen": "when",
        "condition": f"{seq} % 2 == 0",
        "then": first,
        "else": last,
    }
    order["set"]["ono"] = {"gen": "sequence", "start": 1}
    order["patience"] = _expression(
        "(20.0 if attr.ono % 3 == 0 else m) + attr.ono * 0.0005",
        m=_parameter("sales.service.patience.mean"),
    )
    pick = tasks["pick"]
    pick.pop("service")
    pick["service_time"] = _expression(
        f"(m * {SLOW} if attr.ono % 4 == 0 else m) + attr.ono * 0.001",
        m=_parameter("sales.service.pick.mean"),
    )
    tasks["pick"]["patience"] = order.pop("patience")
    for name, service, scale in (
        ("transit", "transit", 0.0007),
        ("returned", "return_after", 0.0003),
    ):
        tasks[name].pop("service")
        tasks[name]["service_time"] = _expression(
            f"m + attr.ono * {scale}", m=_parameter(f"sales.service.{service}.mean")
        )
    sent_back = tasks["items_complete"]["vars"]["sent_back"]  # the document's chance
    tasks["items_complete"]["vars"]["sent_back"] = _expression(
        f"(attr.ono * {SEND_BACK}) % 1 < p", p=sent_back["p"]
    )
    return document


class _Visit:
    """The project's `rng` for one visit (`v`): it answers the draws of `visit`,
    `new_user` and `basket` with the fixed values of this file."""

    def __init__(self, sampler: "_Sampler", v: int) -> None:
        self.sampler = sampler
        self.v = v
        self.ids = 0

    def random(self) -> float:
        name = sys._getframe(1).f_locals["name"]  # `_chance(app, name)`
        return (self.v * (RETURN if name == "returning" else BUY)) % 1.0

    def integers(self, low: int, high: int | None = None) -> int:
        if high is None:  # the oldest of the users
            return 0
        if sys._getframe(1).f_code.co_name == "visit":
            return 2 + (3 * self.v) % 4
        return low + (self.v * 7) % (high - low)  # the age of a new user

    def choice(
        self, count: int, p: Any = None, size: int | None = None, replace: bool = True
    ) -> Any:
        if size is not None:  # the products of the basket: the first or the last
            if self.v % 2 == 0:
                return np.array(range(size))
            return np.array([count - 1 - j for j in range(size)])
        if count == 2:
            return (self.v) % 2
        if count == 6:
            return (self.v * 5) % 6
        if count == 5:
            return (self.v * 3) % 5
        draw = (self.v * COUNT) % 1.0  # the number of items
        return 0 if draw < 0.7 else 1 if draw < 0.9 else 2 if draw < 0.95 else 3

    def bytes(self, length: int) -> bytes:
        caller = sys._getframe(2).f_code.co_name
        if caller == "new_user":
            return uuid.UUID(_identifier(1, self.v)).bytes
        place = self.ids
        self.ids += 1
        if place == 0:  # the order
            self.sampler.orders += 1
            text = _identifier(2, self.v)
            self.sampler.order_of[text] = self.sampler.orders
            return uuid.UUID(text).bytes
        return uuid.UUID(_identifier(3, self.v, place - 1)).bytes


class _Order:
    """The project's `rng` for one order (`m`): the chance that it is returned."""

    def __init__(self, m: int) -> None:
        self.m = m

    def random(self) -> float:
        return (self.m * SEND_BACK) % 1.0


class _Sampler:
    """The project's `sampler`: gaps are `1 / rate`, durations follow the fixed draws,
    and `rng` is the one of the visit or the order that asks."""

    def __init__(self, seed: Any = None) -> None:
        self.visits = 0
        self.orders = 0
        self.order_of: dict[str, int] = {}
        self._real = Sampler(seed=seed)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    @property
    def rng(self) -> Any:
        frame = sys._getframe(1)
        if frame.f_code.co_name == "_chance":
            frame = frame.f_back
        owner = frame.f_code.co_name
        if owner == "visit":
            rng = frame.f_locals.get("rng")
            if rng is None:
                self.visits += 1
                rng = _Visit(self, self.visits)
            return rng
        if owner == "fulfil":
            return _Order(self.order_of[frame.f_locals["rows"][0].id])
        raise AssertionError(f"a draw from {owner}")

    def sample(self, config: Any) -> float:
        if config.dist == "exponential":
            return 1.0 / config.rate
        frame = sys._getframe(2)
        service = sys._getframe(1).f_locals["service"]
        if frame.f_code.co_name == "visit":
            return config.mean + frame.f_locals["rng"].v * PAGES
        m = self.order_of[frame.f_locals["rows"][0].id]
        if service == "patience":
            return (20.0 if m % 3 == 0 else config.mean) + m * 0.0005
        if service == "pick":
            return (config.mean * SLOW if m % 4 == 0 else config.mean) + m * 0.001
        scale = 0.0007 if service == "transit" else 0.0003
        return config.mean + m * scale


@in_project("live-dashboard")
def generate(config: dict[str, Any]) -> types.SimpleNamespace:
    """Runs the project's `build` with the fixed draws to the end of the document's run.

    Args:
        config: An entry of `CONFIGS`.

    Returns:
        types.SimpleNamespace: `rows` (each row the project wrote: the time, the table
            and the row as a mapping), and its modules `project` (`run`), `store`
            (`postgres`), `settings` (`core.config`) and `shop` (`catalogue`).
    """
    sent: list[tuple[float, str, dict[str, Any]]] = []

    def record(env: Any, key: str, event: Any, **extra: Any) -> None:
        sent.append((env.now, key, event))

    with stand_in_modules("asyncpg"):
        from sales.core import config as settings
        from sales.simulation import catalogue as shop
        from sales.simulation import control
        from sales.simulation import run as project
        from sales.stores import postgres as store

    def context(sim_id: str, **options: Any) -> SimulationContext:
        return SimulationContext(sim_id, **{**options, "factor": 0.0})

    with (
        mock.patch.object(DynamicRealtimeEnvironment, "publish_event", record),
        mock.patch("desflow.builder.context.Sampler", _Sampler),
        mock.patch.object(project, "SimulationContext", context),
    ):
        app = project.build(seed=42, start=START)

        def scenario(ctx: Any) -> Any:
            at = 0.0
            for when, path, value in CHANGES:
                yield ctx.env.timeout(when - at)
                at = when
                ctx.env.registry.update(path, value)

        app.add_process(scenario)
        app.run(until=UNTIL)
    return types.SimpleNamespace(
        rows=sent,
        project=project,
        store=store,
        settings=settings,
        shop=shop,
        parameters=control.PARAMETERS,
    )


def _moment(text: str) -> datetime:
    return datetime.fromisoformat(text)


def compare(
    records: list[dict[str, Any]], result: types.SimpleNamespace, config: dict[str, Any]
) -> None:
    """Asserts that the rows of the YAML and of the project agree, and that the run
    reaches what BT-LD1 to BT-LD17 name.

    Args:
        records: The records of the YAML with the fixed draws.
        result: What `generate` returned.
        config: The entry of `CONFIGS` both ran.

    Raises:
        AssertionError: At the first check that fails.
    """
    want = [(t, table, dict(row)) for t, table, row in result.rows]
    got = [
        (
            r["sim_ts"],
            r["value"]["__table__"],
            {k: v for k, v in r["value"].items() if k != "__table__"},
        )
        for r in records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and "__table__" in r["value"]
    ]
    assert len(want) > 2000, len(want)
    for number, (a, b) in enumerate(zip(want, got)):
        assert abs(a[0] - b[0]) < 1e-9 and a[1] == b[1], (number, a, b)
        assert a[2].keys() == b[2].keys(), (number, a, b)
        for name, value in a[2].items():
            if name == "created_at":  # the project's text has microseconds
                assert abs(_moment(value) - _moment(b[2][name])) < timedelta(
                    milliseconds=1
                ), (number, a, b)
            else:
                assert value == b[2][name], (number, name, a, b)
    assert len(want) == len(got), (len(want), len(got))
    _reaches(records, got)
    _values(result)
    _sinks(result)


def _reaches(
    records: list[dict[str, Any]], rows: list[tuple[float, str, dict[str, Any]]]
) -> None:
    """The rules of the rows, reached by the fixed draws."""
    events = [
        r
        for r in records
        if r["stream_type"] == "event" and isinstance(r["value"], dict)
    ]
    tables: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    for t, table, row in rows:
        tables.setdefault(table, []).append((t, row))
    products, users, orders, items = (
        tables[n] for n in ("products", "users", "orders", "order_items")
    )
    assert len(products) == 260 and all(t == 0.0 for t, _ in products)  # BT-LD5
    assert [row["id"] for _, row in products] == list(range(1, 261))
    # BT-LD8: a user has the fields, the bounds and every choice of the weighted lists
    assert {row["gender"] for _, row in users} == set(GENDERS)
    assert {row["country"] for _, row in users} == set(COUNTRIES)
    assert {row["traffic_source"] for _, row in users} == set(SOURCES)
    assert all(12 <= row["age"] <= 70 for _, row in users)
    # BT-LD11 to BT-LD13: every order has its statuses, in order, with its own id and user
    statuses: dict[str, list[str]] = {}
    for _, row in orders:
        statuses.setdefault(row["id"], []).append(row["status"])
    steps = {tuple(s) for s in statuses.values()}
    assert ("Processing", "Cancelled") in steps
    assert ("Processing", "Shipped", "Complete") in steps
    assert ("Processing", "Shipped", "Complete", "Returned") in steps
    created = {row["id"]: row for _, row in orders if row["status"] == "Processing"}
    assert all(
        row["user_id"] == created[row["id"]]["user_id"]
        and row["created_at"] == created[row["id"]]["created_at"]
        for _, row in orders
    )
    # BT-LD7: a returning visitor orders for an earlier user, and users are made only by a purchase
    user_ids = [row["id"] for _, row in users]
    buyers = [row["user_id"] for row in created.values()]
    assert len(user_ids) < len(created) and set(user_ids) == set(buyers)
    assert len(set(user_ids)) == len(user_ids)
    born = {row["id"]: t for t, row in users}
    first = {}
    for t, row in orders:
        if row["status"] == "Processing":
            first.setdefault(row["user_id"], t)
            assert born[row["user_id"]] <= t
    assert (
        sum(
            1
            for t, row in orders
            if row["status"] == "Processing" and t > born[row["user_id"]]
        )
        > 10
    )
    # BT-LD9, BT-LD10: one row for each item, a different product each, one to four items
    by_order: dict[str, list[dict[str, Any]]] = {}
    for _, row in items:
        if row["status"] == "Processing":
            by_order.setdefault(row["order_id"], []).append(row)
    assert {len(v) for v in by_order.values()} == {1, 2, 3, 4}
    for order_id, rows_of in by_order.items():
        assert len(rows_of) == created[order_id]["num_of_item"]
        assert len({r["product_id"] for r in rows_of}) == len(rows_of)
    price = {row["id"]: row["retail_price"] for _, row in products}
    assert all(row["sale_price"] == price[row["product_id"]] for _, row in items)
    # BT-LD14: a status change writes the item rows again with the same ids
    seen: dict[str, int] = {}
    for _, row in items:
        seen[row["id"]] = seen.get(row["id"], 0) + 1
    assert max(seen.values()) >= 3
    # BT-LD6: a visit shows two to five pages
    pages: dict[str, int] = {}
    for r in events:
        if r["task"] == "pages" and r["value"].get("status") == "started":
            pages[r["key"]] = pages.get(r["key"], 0) + 1
    finished = {r["key"] for r in events if r["task"] == "buy_choice"}
    assert {pages[key] for key in finished} == {2, 3, 4, 5}
    _live(events)


def _live(events: list[dict[str, Any]]) -> None:
    """BT-LD1 to BT-LD4: the live changes show in the run."""
    visits = sorted(r["sim_ts"] for r in events if r["task"] == "catalogue_check")
    gaps = [(b, b - a) for a, b in zip(visits, visits[1:])]
    assert {round(g, 9) for t, g in gaps if t < 100.0} == {0.25}  # BT-LD1
    assert all(abs(g - 1.0 / 6) < 1e-9 for t, g in gaps if t > 101.0)
    started = {
        r["key"]: r["sim_ts"]
        for r in events
        if r["task"] == "pick" and r["value"].get("status") == "started"
    }
    ended = {
        r["key"]: r["sim_ts"]
        for r in events
        if r["task"] == "pick" and r["value"].get("status") == "Shipped"
    }
    spans = sorted((started[k], ended[k]) for k in ended)
    # BT-LD2: packing takes the live mean, 5 s and then 8 s (every fourth order takes 12 times it)
    quick = [(s, e - s) for s, e in spans if e - s < 20]
    assert max(d for s, d in quick if s < 200.0) < 6.0
    assert min(d for s, d in quick if s > 200.2) > 8.0
    busy = lambda lo, hi: max(  # noqa: E731
        sum(1 for s, e in spans if s <= t < e) for t in np.arange(lo, hi, 0.5)
    )
    assert (
        busy(50.0, 150.0) == 10 and busy(270.0, 300.0) <= 6
    )  # BT-LD3: the live number of pickers
    chosen = [r for r in events if r["task"] in ("new_user", "returning")]
    left = [r for r in events if r["task"] == "walk_away"]
    before = len([r for r in chosen if r["sim_ts"] < 250.0]) / max(
        1, len([r for r in chosen + left if r["sim_ts"] < 250.0])
    )
    after = len([r for r in chosen if r["sim_ts"] > 250.2]) / max(
        1, len([r for r in chosen + left if r["sim_ts"] > 250.2])
    )
    assert 0.25 < before < 0.35 and 0.4 < after < 0.5, (
        before,
        after,
    )  # BT-LD4: the live chance to buy


def _document() -> dict[str, Any]:
    """The document as it is written, with the draws the fixed run replaces."""
    from desflow.blueprint.loader import read_document

    return read_document(HERE / "model.yaml", variables=VARIABLES)[0]


def _values(result: types.SimpleNamespace) -> None:
    """The values of the document that the fixed run replaces or does not read, equal
    the project's: the services and chances (BT-LD1 to BT-LD4), the draws of a new user
    (BT-LD8), the item counts (BT-LD9) and the live changes of the scenario."""
    settings, shop = result.settings, result.shop
    document = _document()
    assert document["arrivals"]["visitor"]["rate"] == settings.ARRIVALS["visitor"]
    assert document["arrivals"]["visitor"]["attributes"]["pages"]["bounds"] == [
        2,
        5,
    ]  # `rng.integers(2, 6)`
    assert document["simulation"]["sim_id"] == settings.SIM_ID
    for name in settings.SERVICES:  # the document's names are the project's
        mean, std = settings.SERVICES[name]
        service = document["services"][name]
        assert (service["dist"], service["mean"], service["std"]) == (
            "lognormal",
            mean,
            std,
        ), name
    assert document["resources"]["pickers"] == {
        "current_cap": settings.PICKERS,
        "max_cap": settings.MAX_PICKERS,
    }
    for name, chance in settings.CHANCES.items():
        assert document["variables"][name] == chance, name
    payload = document["tasks"]["new_user"]["payload"]
    assert payload["age"]["bounds"] == [12, 70]
    assert sorted(payload["gender"]["choices"]) == sorted(GENDERS)
    assert payload["country"]["options"] == shop.COUNTRIES
    assert payload["traffic_source"]["options"] == shop.TRAFFIC_SOURCES
    shares = list(shop.ITEM_COUNTS.values())
    cut = [round(sum(shares[: i + 1]), 10) for i in range(len(shares) - 1)]
    count = document["tasks"]["order"]["vars"]["n"]["expr"]
    assert (
        count
        == f"1 if u < {cut[0]} else 2 if u < {cut[1]} else 3 if u < {cut[2]} else 4"
    ), count
    assert list(shop.ITEM_COUNTS) == [1, 2, 3, 4]
    steps = [(s["at"], s["path"], s["value"]) for s in document["scenario"]]
    assert all(path in result.parameters for _, path, _ in steps)  # the control tool's
    want = list(CHANGES)
    assert steps == want


def _sinks(result: types.SimpleNamespace) -> None:
    """BT-LD14 to BT-LD17: the sinks the project has, as the file builds them."""
    project, store = result.project, result.store
    app = SimulationContext.from_yaml(HERE / "model.yaml", variables=VARIABLES)
    tables = {}
    for sink in (each.provider for each in app._sinks.egress):
        (name,) = sink.tables  # one table made at start
        tables[sink.table_name] = (sink, name)
    assert set(tables) == set(store.TABLES)  # BT-LD15: four tables from one run
    for table, (sink, name) in tables.items():
        assert sink.upsert_keys == store.UPSERT_KEYS.get(table)  # BT-LD14
        assert sink.kwargs["server_settings"] == {"search_path": store.SCHEMA}
        assert (
            name == f"{store.SCHEMA}.{table}" and sink.table_policy == "create"
        )  # BT-LD16
    ddl = {
        name: dict(re.findall(r"(\w+) (TEXT|INT|BIGINT|FLOAT8)", body))
        for name, body in re.findall(
            r"CREATE TABLE IF NOT EXISTS \{SCHEMA\}\.(\w+) \((.*?)\);",
            store._DDL.replace(store.SCHEMA, "{SCHEMA}"),
            re.S,
        )
    }
    for table, (sink, name) in tables.items():
        columns, key = sink.tables[name]
        assert columns == ddl[table], (table, columns, ddl[table])
        assert key == ["id"]
    (ingress,) = app._sinks.ingress  # BT-LD17
    assert ingress.table_name == store.PARAMS_TABLE
    assert (app._sinks.batch_size, app._sinks.flush_interval) == (4, 1.0)
    source = inspect.getsource(project.run)
    assert re.search(r"with_batching\(batch_size=4, flush_interval=1\.0\)", source)
