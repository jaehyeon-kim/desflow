"""BT-EC1 to BT-EC9: the ecommerce-cdc project's own simulation.

The original is the project's `ecommerce.simulation.run.build` and its processes
(its copy in `tests/copied_projects/benchtop/ecommerce-cdc`, read only), run unpaced to the document's end with stand-ins for what is random:

- `_Sampler` gives every arrival gap and every service time a fixed value, and `rng` the
  fixed draws below;
- `_FixedFake` is the project's `Faker`, giving each method a fixed list of values;
- `publish_event` is recorded, so the rows each table receives are the project's own.

The fixed draws are a function of how many times a draw of its kind was made, so the k-th
user, session, order and delivery of the project has the k-th draw. `fix` gives the YAML
the same draws, so the two run the same shop. The draws are chosen so that the run reaches
what the rows name: a user of each gender and of every city in turn; sessions of one to
four product pages that buy and that leave; orders of one to four items; orders that
wait for a picker and are cancelled, that ship, that are delivered and that are returned.

`compare` holds the YAML's rows equal to the project's, table by table and in the order
each table's rows were made:

- BT-EC5: the ten distribution centres and the first 100 users;
- BT-EC1 to BT-EC4: the fields of each user, the six linked ones of its city included;
- BT-EC6: the moves, which look up the first user and give the project's street address;
- BT-EC7: each session's events: type, number, address and time;
- BT-EC8: each order's rows, status by status, with the time column of each status;
- BT-EC9: the control topic of the ingress and the topic call the project makes.

What differs between the document and the project, each stated by the row that chose it:
the document has no `uri`, `user_id` and per-event `traffic_source`, no order items, no
returning visitors and no products table; it writes the move row's address and time only;
a user's id is a count where the project's is a random hash, so users and orders are
paired by the order they were made in; the project's times are ISO text to the second, so
times are compared to the second.
"""

import csv
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest import mock

from desflow import SimulationContext
from desflow.blueprint.loader import read_document
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.sampler import Sampler

from tests.known_cases.bt_support import cycle, fix_draws, in_project, sequence_of

HERE = Path(__file__).parent
VARIABLES = {"FACTOR": "0"}
START = datetime(2026, 9, 1, tzinfo=timezone.utc)
UNTIL = 1200
CONFIGS = [{"variables": {}, "args": {}}]
GOLDEN = 0.6180339887
PHASE = {"buy": 0.0, "return": 0.31, "size": 0.57, "returning": 0.77}
GAPS = {
    1.0: 1.7,
    0.05: 23.3,
}  # a gap by its rate: a visit every 1.7 s, a move every 23.3 s
SERVICES = {  # a service's time by its mean and spread
    (4.0, 2.0): 4.0,  # a page
    (120.0, 60.0): 120.0,  # the patience of an order for a picker
    (5.0, 2.0): 100.0,  # packing, so that orders wait longer than their patience
    (60.0, 20.0): 60.0,  # transit
    (60.0, 30.0): 45.0,  # the return
}
MALE = ["Liam", "Noah", "Oliver", "Henry", "Jack", "McArthur", "O'Neil"]
FEMALE = ["Olivia", "Charlotte", "Amelia", "Mia", "Chloe", "Ava", "DeLeon", "Grace"]
LAST = [
    "Smith",
    "Jones",
    "Brown",
    "Wilson",
    "Taylor",
    "Evans",
    "McDonald",
    "Van Der Berg",
    "Lee",
]
STREETS = [
    "1 Park Road",
    "22 High Street",
    "303 Bay Lane",
    "4 Hill Court",
    "55 Mill Way",
]
MOVED = ["9 New Street", "10 Harbour Rise", "11 Orchard Close"]
BROWSERS = ["Chrome", "Safari", "Firefox"]
SIZES = [1, 2, 3, 4]
CHANCES = {"returning": 0.6, "buy": 0.3, "return": 0.1}
DOMAIN = "example.com"


def _draw(purpose: str, count: int) -> float:
    """The fixed draw of the `count`-th chance of a kind, counted from 1: a number in
    [0, 1) that takes every range, so each chance is above and below some draws."""
    return (count * GOLDEN + PHASE[purpose]) % 1.0


def _fraction(path: tuple[Any, ...]) -> dict[str, Any]:
    """The YAML's form of `_draw` for the node at `path`: the k-th draw of the node,
    counted from 1, in the phase of the node's purpose."""
    purpose = {"order_row": "size", "deliver_order": "return"}.get(str(path[1]))
    phase = PHASE[purpose] if purpose else 0.0
    return {
        "gen": "expr",
        "expr": f"(fix_k * {GOLDEN} + {phase}) % 1.0",
        "names": {"fix_k": sequence_of(1)},
    }


def fix(document: dict[str, Any]) -> dict[str, Any]:
    """The document with its draws fixed (`fix_draws`): each draw node takes the value
    of the project's stand-ins from its own choices, bounds or chance. What the
    project's stand-ins do that a node does not give (constant gaps and service times,
    the first user of a move, the places in the order of their file, and the chance of a
    purchase, which the document draws in the weights of `next`) is replaced with the
    same value. Each arrival and service is first checked by name against the one the
    project's `build` adds.

    Args:
        document: The loaded `model.yaml`.

    Returns:
        dict[str, Any]: The same document with the draws fixed.

    Raises:
        AssertionError: When an arrival or a service differs from the project's in its
            distribution, rate, mean or spread.
    """
    arrivals, services = _settings()
    for name, arrival in document["arrivals"].items():
        if "dist" in arrival:
            own = arrivals[name]
            assert (arrival["dist"], arrival["rate"]) == (own.dist, own.rate), (
                f"the document's arrival {name} is not the project's"
            )
            arrival["value"] = GAPS[arrival.pop("rate")]
            arrival["dist"] = "constant"
    assert document["services"].keys() == services.keys(), "the services differ"
    for name, service in document["services"].items():
        own = services[name]
        assert (service["dist"], service["mean"], service["std"]) == (
            own.dist,
            own.mean,
            own.std,
        ), f"the document's service {name} is not the project's"
        key = (service["mean"], service["std"])
        document["services"][name] = {"dist": "constant", "value": SERVICES[key]}
    tasks = document["tasks"]
    assert tasks["new_user"]["vars_once"]["city"].pop("weight", None) == "population"
    tasks["new_user"]["vars_once"]["city"]["order"] = (
        "circular"  # the k-th user, the k-th city
    )
    tasks["move"]["payload"]["id"]["strategy"] = (
        "first"  # the project moves its first user
    )

    def fake(node: dict[str, Any], path: tuple[Any, ...]) -> dict[str, Any]:
        if (
            node["method"] == "ipv4"
        ):  # the address of a page view is made from its number
            number = tasks[path[1]]["payload"]["sequence_number"]
            return {"gen": "format", "template": "10.0.0.{s}", "names": {"s": number}}
        table = {
            "first_name_male": MALE,
            "first_name_female": FEMALE,
            "last_name": LAST,
            "street_address": MOVED if path[1] == "move" else STREETS,
        }[node["method"]]
        return cycle(table)

    buy = tasks["buy_choice"]
    cart = buy["next"]["weights"]["cart"]  # the chance of a purchase, from the registry
    buy["next"] = {
        "when": [{"condition": f"u < {cart}", "task": "cart"}],
        "default": "leave",
    }
    buy["vars"] = {
        "u": {
            "gen": "expr",
            "expr": f"(int(e[8:]) * {GOLDEN} + {PHASE['buy']}) % 1.0",
            "names": {"e": {"gen": "entity"}},
        }
    }
    document["tasks"] = fix_draws(tasks, fraction=_fraction, fake=fake, path=("tasks",))
    document["arrivals"] = fix_draws(
        document["arrivals"], fraction=_fraction, path=("arrivals",)
    )
    return document


@in_project("ecommerce-cdc")
def _settings() -> tuple[dict[str, Any], dict[str, Any]]:
    """The arrivals and services the project's `build` adds, by name."""
    from ecommerce.simulation import run as project

    app = project.build(START, seed=7, factor=0.0)
    return app._arrivals.configs, app._services_config


class _FixedRng:
    """The project's `sampler.rng` (a NumPy generator) with the fixed draws: a draw of a
    kind (a chance, a list to choose from, a range) is a function of how many draws of
    that kind came before it."""

    def __init__(self) -> None:
        self.purpose = ""
        self.counts: dict[Any, int] = {}
        self.identifiers = 0

    def _count(self, kind: Any) -> int:
        before = self.counts.get(kind, 0)
        self.counts[kind] = before + 1
        return before

    def random(self) -> float:
        return _draw(self.purpose, self._count(("random", self.purpose)) + 1)

    def integers(
        self, low: int, high: int | None = None, size: int | None = None
    ) -> Any:
        if size is not None:  # the products a visit views, and an order's items
            count = self._count(("products", low))
            return [(count * 7 + i * 3) % low for i in range(size)]
        if high is None:  # a user from the list: the first
            return 0
        count = self._count(("range", low, high))
        return low + int(((count + 1) * GOLDEN % 1.0) * (high - low))

    def choice(self, options: Any, p: list[float] | None = None) -> Any:
        if isinstance(options, int):  # a city of the catalogue, by its place in it
            return self._count(("index", options)) % options
        count = self._count(("choice", tuple(options)))
        if p is None:
            if list(options) == ["M", "F"]:  # the document's list is F, M
                return options[(count + 1) % 2]
            return options[count % len(options)]
        chance, total = _draw("size", count + 1), 0.0
        for option, share in zip(options, p):
            total += share
            if chance < total:
                return option
        return options[-1]

    def bytes(self, length: int) -> bytes:
        self.identifiers += 1
        return self.identifiers.to_bytes(length, "big")


class _FixedFake:
    """The project's `Faker`, giving each method its fixed list in turn. A street address
    has one list for a new user and one for a move, and an address of a page view is
    made from the page's number."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def seed_instance(self, seed: Any) -> None:
        return None

    def _next(self, table: list[str], name: str) -> str:
        count = self.counts.get(name, 0)
        self.counts[name] = count + 1
        return table[count % len(table)]

    def first_name_male(self) -> str:
        return self._next(MALE, "male")

    def first_name_female(self) -> str:
        return self._next(FEMALE, "female")

    def last_name(self) -> str:
        return self._next(LAST, "last")

    def safe_domain_name(self) -> str:
        return DOMAIN

    def street_address(self) -> str:
        if sys._getframe(1).f_code.co_name == "moved":
            return self._next(MOVED, "moved")
        return self._next(STREETS, "street")

    def ipv4(self) -> str:
        return f"10.0.0.{sys._getframe(1).f_locals['n']}"


class _Sampler:
    """The project's `sampler`: gaps and service times are the fixed values."""

    def __init__(self, seed: Any = None) -> None:
        self.rng = _FixedRng()
        self._real = Sampler(seed=seed)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    def sample(self, config: Any) -> float:
        if config.dist == "exponential":
            return GAPS[config.rate]
        return SERVICES[(config.mean, config.std)]


def _project() -> types.SimpleNamespace:
    from ecommerce.core import config, models
    from ecommerce.simulation import catalogue, control, run, shop

    return types.SimpleNamespace(
        config=config,
        models=models,
        catalogue=catalogue,
        control=control,
        run=run,
        shop=shop,
    )


@in_project("ecommerce-cdc")
def generate(config: dict[str, Any]) -> types.SimpleNamespace:
    """Runs the project's `build` with the fixed draws to the end of the document's run.

    Args:
        config: An entry of `CONFIGS`.

    Returns:
        types.SimpleNamespace: `sent` (each row the project published: the time, the
            table and the row), `moved` (each user before and after a move), `project`
            (its modules).
    """
    project = _project()
    sent: list[tuple[float, str, dict[str, Any]]] = []
    moved: list[tuple[Any, Any]] = []
    real_moved, real_chance = project.shop.moved, project.run._chance

    def record(env: Any, key: str, event: Any, **extra: Any) -> None:
        sent.append((env.now, key, event))

    def moving(user: Any, fake: Any, rng: Any, now: str) -> Any:
        after = real_moved(user, fake, rng, now)
        moved.append((user, after))
        return after

    def chance(ctx: Any, name: str) -> bool:
        ctx.sampler.rng.purpose = name
        return real_chance(ctx, name)

    with (
        mock.patch.object(DynamicRealtimeEnvironment, "publish_event", record),
        mock.patch("desflow.builder.context.Sampler", _Sampler),
        mock.patch.object(project.run, "Faker", _FixedFake),
        mock.patch.object(project.run, "_chance", chance),
        mock.patch.object(project.shop, "moved", moving),
    ):
        app = project.run.build(START, seed=7, factor=0.0)
        app.run(until=UNTIL)
    return types.SimpleNamespace(sent=sent, moved=moved, project=project)


def _topic_calls(
    project: types.SimpleNamespace,
) -> tuple[list[Any], list[dict[str, Any]]]:
    """What the project's `run` asks of Kafka before the run: the topics it creates, and
    how it builds its ingress. Its other parts are replaced by recorders."""
    created: list[Any] = []
    ingress: list[dict[str, Any]] = []

    class Admin:
        def __init__(self, *args: Any, **settings: Any) -> None:
            pass

        def create_topics(self, topics: list[Any]) -> None:
            created.extend(topics)

    def make_ingress(**settings: Any) -> Any:
        ingress.append(settings)
        return mock.MagicMock()

    with (
        mock.patch.object(project.run.postgres, "create_tables"),
        mock.patch.object(project.run, "KafkaAdminConnector", Admin),
        mock.patch.object(project.run, "KafkaIngress", make_ingress),
        mock.patch.object(project.run, "PostgresEgress", mock.MagicMock()),
        mock.patch.object(project.run, "build", mock.MagicMock()),
    ):
        project.run.run(minutes=1, seed=7)
    return created, ingress


def _second(text: str) -> datetime:
    """A time of a row, to the second, as the project writes it."""
    return datetime.fromisoformat(text).replace(microsecond=0)


def _groups(
    rows: list[tuple[float, dict[str, Any]]], field: str
) -> list[list[tuple[float, dict[str, Any]]]]:
    """The rows grouped by the value of `field`, in the order each group first appears."""
    groups: dict[Any, list[tuple[float, dict[str, Any]]]] = {}
    for at, row in rows:
        groups.setdefault(row[field], []).append((at, row))
    return list(groups.values())


def compare(
    records: list[dict[str, Any]], result: types.SimpleNamespace, config: dict[str, Any]
) -> None:
    """Asserts that the YAML's rows and the project's agree, for BT-EC1 to BT-EC9.

    Args:
        records: The records of the YAML with the fixed draws.
        result: What `generate` returned.
        config: The entry of `CONFIGS` both ran.

    Raises:
        AssertionError: At the first check that fails.
    """
    project = result.project
    want: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    for at, table, row in result.sent:
        want.setdefault(table, []).append((at, row))
    got: dict[str, list[tuple[float, dict[str, Any]]]] = {}
    for record in records:
        value = record["value"]
        if (
            record["stream_type"] == "event"
            and isinstance(value, dict)
            and "__table__" in value
        ):
            got.setdefault(value["__table__"], []).append(
                (record["sim_ts"], {k: v for k, v in value.items() if k != "__table__"})
            )
    # BT-EC5: ten centres, numbered as the project's, and 100 users, all at the start
    assert (
        [r["id"] for _, r in got["dist_centers"]]
        == [r["id"] for _, r in want["dist_centers"]]
        == list(range(1, 11))
    )
    first_users = want["users"][:100]
    users = [row for row in got["users"] if row[1].get("first_name")]
    assert len(users) == 100 and {at for at, _ in users} == {0.0}
    # BT-EC1 to BT-EC4: every field of each user, the city's six linked fields included
    for (_, made), (_, row) in zip(first_users, users):
        for name in (
            "first_name", "last_name", "email", "age", "gender", "street_address",
            "postal_code", "city", "state", "country", "traffic_source",
        ):  # fmt: skip
            assert row[name] == made[name], (name, row, made)
        assert (
            row["latitude"] == made["latitude"]
            and row["longitude"] == made["longitude"]
        )
        assert _second(row["created_at"]) == _second(made["created_at"])
        assert _second(row["updated_at"]) == _second(made["updated_at"])
    assert {u["gender"] for _, u in users} == {"M", "F"}
    assert any(u["first_name"] != u["first_name"].lower() for _, u in users)
    assert all(
        u["email"] == f"{u['first_name']}.{u['last_name']}@{DOMAIN}".lower()
        for _, u in users
    )  # BT-EC3
    assert (
        len({u["city"] for _, u in users}) > 30
        and len({u["country"] for _, u in users}) >= 5
    )
    # the catalogue of cities in `cities.csv` is the project's
    with open(HERE / "cities.csv", encoding="utf-8", newline="") as handle:
        sheet = [tuple(row) for row in csv.reader(handle)][1:]
    assert sheet == [
        tuple(str(v) for v in city) for city in project.catalogue.CITIES
    ], "cities.csv is not the project's CITIES"
    # BT-EC6: the moves look up the first user and take the project's address and time
    moves = got["users"][100:]
    assert len(moves) == len(result.moved) > 10
    for (at, row), (before, after) in zip(moves, result.moved):
        assert row["id"] == users[0][1]["id"]
        assert row["street_address"] == after.street_address != before.street_address
        assert _second(row["updated_at"]) == _second(after.updated_at)
    # BT-EC7: each session's events in order: the type, the number, the address, the time
    sessions = _groups(got["events"], "session_id")
    visits = _groups(want["events"], "session_id")
    assert len(sessions) == len(visits) > 200
    lengths = set()
    for session, visit in zip(sessions, visits):
        assert [
            (r["event_type"], r["sequence_number"], r["ip_address"]) for _, r in session
        ] == [
            (r["event_type"], r["sequence_number"], r["ip_address"]) for _, r in visit
        ]
        assert [round(at, 6) for at, _ in session] == [round(at, 6) for at, _ in visit]
        assert session[0][1]["browser"] == visit[0][1]["browser"]
        assert all(r["session_id"] == session[0][1]["session_id"] for _, r in session)
        lengths.add(sum(r["event_type"] == "product" for _, r in session))
    assert {1, 2, 3, 4} <= lengths, lengths  # a visit of each length
    kinds = {tuple(r["event_type"] for _, r in v)[-2:] for v in visits}
    assert ("cart", "purchase") in kinds and any("cart" not in k for k in kinds)
    assert {r["browser"] for _, r in want["events"]} == set(BROWSERS)
    # BT-EC8: each order's rows, status by status, with the time column of each status
    orders = _groups(got["orders"], "id")
    made = _groups(want["orders"], "id")
    assert len(orders) == len(made) > 50
    created = [rows[0][1]["created_at"] for rows in orders]
    assert len(set(created)) == len(created), (
        "two orders share a time, so they cannot be paired"
    )
    trail = ("status", "num_of_items", "created_at", "updated_at", "shipped_at",
             "delivered_at", "cancelled_at", "returned_at")  # fmt: skip

    def shown(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(
            _second(row[n]) if n.endswith("_at") and row[n] is not None else row[n]
            for n in trail
        )

    for rows, mine in zip(orders, made):
        assert [shown(r) for _, r in rows] == [shown(r) for _, r in mine]
        assert [round(at, 6) for at, _ in rows] == [round(at, 6) for at, _ in mine]
    paths = {tuple(r["status"] for _, r in rows) for rows in made}
    assert paths == {
        ("Processing", "Cancelled"),
        ("Processing", "Shipped", "Delivered"),
        ("Processing", "Shipped", "Delivered", "Returned"),
        ("Processing", "Shipped"),
    } or paths >= {
        ("Processing", "Cancelled"),
        ("Processing", "Shipped", "Delivered"),
        ("Processing", "Shipped", "Delivered", "Returned"),
    }, paths
    assert {r["num_of_items"] for _, r in want["orders"]} == {1, 2, 3, 4}
    # BT-EC9: the control topic the project creates, and the one the document's ingress does
    created_topics, built = _topic_calls(project)
    app = SimulationContext.from_yaml(HERE / "model.yaml", variables=VARIABLES)
    (ingress,) = app._sinks.ingress
    assert ingress.topic == built[0]["topic"] == project.config.CONTROL_TOPIC
    assert (
        ingress.bootstrap_servers
        == built[0]["bootstrap_servers"]
        == project.config.KAFKA
    )
    # the document's names are the project's, so a change the project's control tool
    # sends reaches the YAML's registry
    document = read_document(HERE / "model.yaml", variables=VARIABLES)[0]
    sim_id = document["simulation"]["sim_id"]
    assert sim_id == project.config.SIM_ID
    names = [f"{sim_id}.variables.{name}" for name in document["variables"]]
    names += [
        f"{sim_id}.arrival.{name}.rate"
        for name, arrival in document["arrivals"].items()
        if "rate" in arrival
    ]
    names += [f"{sim_id}.service.{name}.mean" for name in document["services"]]
    names += [
        f"{sim_id}.resources.{name}.current_cap" for name in document["resources"]
    ]
    assert len(names) == 10 and all(n in project.control.PARAMETERS for n in names)
    asked: list[Any] = []

    class Admin:
        def __init__(self, **settings: Any) -> None:
            pass

        def create_topics(self, topics_config: list[Any]) -> None:
            asked.extend(topics_config)

    with mock.patch("desflow.connectors.ingress.kafka.KafkaAdminConnector", Admin):
        ingress._create_topic()
    assert asked == created_topics == [{"name": project.config.CONTROL_TOPIC}]
