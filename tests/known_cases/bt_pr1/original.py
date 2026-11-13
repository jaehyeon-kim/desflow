"""BT-PR1 to BT-PR10: the product-recommender's own Python.

The original is the project's `recommender` package (its copy in
`tests/copied_projects/benchtop/product-recommender`, read only):

- `generate` makes the users with `recommender.data.users.new_user` and a stand-in
  `Faker` that gives fixed draws (`_FixedFake`), so every user is known;
- `compare` has the YAML (with `fix` giving it the same draws) equal those users in all
  fourteen fields (BT-PR1 to BT-PR3), and has each event of the history agree with the
  project's `time_context`, `click_probability` and `will_click` on the inputs the
  event itself names (BT-PR4 to BT-PR6): the user, the product and the time. The click
  draw of an event is the fixed draw `_draw(k)` of the k-th event, for the YAML and
  for the project's `will_click` alike;
- the feedback events are valid `FeedbackEvent` rows of the project, keyed by product
  (BT-PR9), one every 0.1 s (BT-PR10); the Parquet sink writes the files (BT-PR7).

The ranking of BT-PR8 moved after 1.0 and is not built.
"""

import csv
import dataclasses
import inspect
import json
import re
import types
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from desflow import KafkaEgress, ParquetStorageEgress, SimulationContext
from desflow.connectors.egress.kafka import ConfluentAvroSerializer

from tests.known_cases.bt_support import (
    cycle,
    fix_draws,
    in_project,
    sequence_of,
    stand_in_modules,
)

HERE = Path(__file__).parent
VARIABLES = {"FACTOR": "0"}
START = datetime(2025, 10, 3)
GAP = 777.5  # the history's fixed gap, so 10,000 events end inside the 90 days
GOLDEN = 0.6180339887
# The phase of the click draws. It puts the draw of at least one event, a user of 30 to 32 years
# (0.25 to 0.3 scaled) shown a product of more than 0.8 of the price range, between the
# chance of a click without the budget rule and the chance with it.
CLICK = 0.05
CONFIGS = [{"variables": {}, "args": {"users": 1000}}]
MALE = ["Liam", "Noah", "Oliver", "Henry", "Jack"]
FEMALE = ["Olivia", "Charlotte", "Amelia", "Mia", "Chloe", "Ava", "Grace"]
LAST = ["Smith", "Jones", "Brown", "Wilson", "Taylor", "Evans"]
DOMAINS = ["example.net", "example.org", "example.com"]
STREETS = ["1 Park Road", "22 High Street", "303 Bay Lane", "4 Hill Court"]


def _draw(k: int, phase: float = 0.0) -> float:
    """The fixed draw of the k-th call: a number in [0, 1) that takes every value
    range, so every probability is above and below some draws."""
    return (k * GOLDEN + phase) % 1.0


class _FixedFake:
    """A `Faker` that gives a table's next value at each call of a method, as the
    project's `new_user` calls them (each method has its own count)."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.random = types.SimpleNamespace(choices=self._area)
        self.areas: list[Any] = []

    def _next(self, name: str) -> int:
        count = self.counts.get(name, 0)
        self.counts[name] = count + 1
        return count

    def _area(self, areas: list[Any], weights: Any = None, k: int = 1) -> list[Any]:
        return [areas[self._next("area") % len(areas)]]

    def random_element(self, elements: Any) -> Any:
        # the project lists "M" first and the document `F`, whose first choice is the
        # first of its own list: the project's pick is the one after
        return elements[(self._next("gender") + 1) % len(elements)]

    def first_name_male(self) -> str:
        return MALE[self._next("male") % len(MALE)]

    def first_name_female(self) -> str:
        return FEMALE[self._next("female") % len(FEMALE)]

    def last_name_nonbinary(self) -> str:
        return LAST[self._next("last") % len(LAST)]

    def safe_domain_name(self) -> str:
        return DOMAINS[self._next("domain") % len(DOMAINS)]

    def random_int(self, min: int, max: int) -> int:  # noqa: A002
        return min + int(_draw(self._next("age") + 1) * (max - min + 1))

    def street_address(self) -> str:
        return STREETS[self._next("street") % len(STREETS)]

    def random_choices(self, elements: Any, length: int) -> list[Any]:
        total, found = float(sum(elements.values())), []
        for _ in range(length):
            chance, before = _draw(self._next("source") + 1), 0.0
            for name, weight in elements.items():
                before += weight / total
                if chance < before:
                    break
            found.append(name)
        return found


FAKE = {
    "first_name_male": MALE,
    "first_name_female": FEMALE,
    "last_name_nonbinary": LAST,
    "safe_domain_name": DOMAINS,
    "street_address": STREETS,
}


def _fraction(path: tuple[Any, ...]) -> dict[str, Any]:
    """The YAML's form of `_draw`: the k-th draw of the node, counted from 1, with the
    phase `CLICK` for the click of an event."""
    phase = CLICK if path[-1] == "click" else 0.0
    return {
        "gen": "expr",
        "expr": f"(fix_k * {GOLDEN} + {phase}) % 1.0",
        "names": {"fix_k": sequence_of(1)},
    }


@in_project("product-recommender")
def _history() -> tuple[int, int]:
    """The number of events of the project's history, and the days it covers."""
    with stand_in_modules("sklearn", "sklearn.preprocessing", "textwiser"):
        from recommender.data import history
    days = re.search(
        r"timedelta\(days=(\d+)\)", inspect.getsource(history.generate_history)
    )
    assert days, "the project's history has no span of days"
    return history.NUM_PAST_EVENTS, int(days.group(1))


def fix(document: dict[str, Any]) -> dict[str, Any]:
    """The document with its draws fixed (`fix_draws`): each draw node takes the value
    of `_FixedFake` and `_draw` from its own choices, bounds, weights or chance, and a
    constant gap for the history. The structure of the document is kept, apart from the
    place, which is read in the order of its file.

    Args:
        document: The loaded `model.yaml`.

    Returns:
        dict[str, Any]: The same document with the draws fixed.

    Raises:
        AssertionError: When the history's number of events or its rate differs from
            the project's events over the days its history covers.
    """
    events, days = _history()
    history = document["arrivals"]["history"]
    assert history["max_events"] == events, "the history's events are not the project's"
    assert history.pop("rate") == round(events / (days * 86400), 6), (
        f"the history's rate is not {events} events over {days} days"
    )
    history.update(dist="constant", value=GAP)
    place = document["tasks"]["user"]["vars"]["place"]
    assert place.pop("weight", None) == "population"
    place["order"] = "circular"
    document["tasks"] = fix_draws(
        document["tasks"],
        fraction=_fraction,
        fake=lambda node, path: cycle(FAKE[node["method"]]),
    )
    return document


@in_project("product-recommender")
def generate(config: dict[str, Any]) -> types.SimpleNamespace:
    """The project's users, made by its `new_user` with the fixed draws, and the
    functions of its simulation.

    Args:
        config: An entry of `CONFIGS`.

    Returns:
        types.SimpleNamespace: `users` (the project's users, as mappings), `time_context`,
            `click_probability`, `will_click` (with `_draw`'s draw), `models`
            (the project's `FeedbackEvent` and its schema), `items` (the project's
            product features, by id) and `ages` (the project's scaled ages).
    """
    # the project imports `sklearn` and `textwiser` for its feature functions, which the
    # test does not call
    with stand_in_modules("sklearn", "sklearn.preprocessing", "textwiser"):
        from recommender.core import config as settings
        from recommender.core import models
        from recommender.core.config import PRODUCT_FEATURES, USER_FEATURES, USERS
        from recommender.data import history, users
        from recommender.data.locations import load_locations
        from recommender.engine import simulation

    fake = _FixedFake()
    locations = load_locations()
    made = [
        dataclasses.asdict(users.new_user(i + 1, locations, fake))
        for i in range(config["args"]["users"])
    ]
    with open(PRODUCT_FEATURES, encoding="utf-8") as handle:
        items = {int(r["product_id"]): r for r in csv.DictReader(handle)}
    with open(USER_FEATURES, encoding="utf-8") as handle:
        scaled = {int(r["user_id"]): float(r["age"]) for r in csv.DictReader(handle)}
    with open(USERS, encoding="utf-8") as handle:
        raw = {int(r["user_id"]): int(r["age"]) for r in csv.DictReader(handle)}

    def will_click(user_ctx: dict, item_ctx: dict, number: int) -> int:
        draw = types.SimpleNamespace(
            random=types.SimpleNamespace(random=lambda: _draw(number, CLICK))
        )
        return simulation.will_click(user_ctx, item_ctx, draw)

    return types.SimpleNamespace(
        users=made,
        time_context=simulation.time_context,
        click_probability=simulation.click_probability,
        will_click=will_click,
        models=models,
        config=settings,
        items=items,
        scaled_ages={uid: (raw[uid], scaled[uid]) for uid in raw},
        history=history,
    )


def _events(records: list[dict[str, Any]], task: str) -> list[dict[str, Any]]:
    return [
        r
        for r in records
        if r["stream_type"] == "event"
        and r["task"] == task
        and "status" not in r["value"]
    ]


def compare(
    records: list[dict[str, Any]], result: types.SimpleNamespace, config: dict[str, Any]
) -> None:
    """Asserts that the YAML's records and the project agree, for BT-PR1 to BT-PR10.

    Args:
        records: The records of the YAML with the fixed draws.
        result: What `generate` returned.
        config: The entry of `CONFIGS` both ran.

    Raises:
        AssertionError: At the first check that fails.
    """
    # BT-PR1 to BT-PR3: the users, in all fourteen fields
    made = [r["value"] for r in _events(records, "user")]
    assert len(made) == len(result.users) == 1000
    assert [u["user_id"] for u in made] == list(range(1, 1001))
    for got, want in zip(made, result.users):
        for name, value in want.items():
            if name in ("postal_code", "latitude", "longitude"):
                assert str(got[name]) == str(value) or float(got[name]) == float(
                    value
                ), (name, got, want)
            else:
                assert got[name] == value, (name, got, want)
    assert {u["gender"] for u in made} == {"M", "F"}
    assert len({u["email"] for u in made}) > 1 and all(
        u["email"]
        == f"{u['first_name'].lower()}.{u['last_name'].lower()}@{u['email'].split('@')[1]}"
        for u in made
    )
    assert {u["postal_code"] for u in made} == {
        int(a) if str(a).isdigit() else a for a in {u["postal_code"] for u in made}
    }
    # the YAML's scalers are the project's: age from 16 to 70, price from 3.0 to 38.99
    for uid, (age, scaled) in list(result.scaled_ages.items())[:50]:
        assert abs((age - 16) / 54 - scaled) < 1e-9
    prices = _prices()
    for pid, row in result.items.items():
        assert abs((prices[pid] - 3.0) / 35.99 - float(row["price"])) < 1e-6
    # BT-PR4: 10,000 events inside the 90 days the project's history covers
    events = _events(records, "event")
    assert len(events) == result.history.NUM_PAST_EVENTS == 10000
    assert events[-1]["sim_ts"] < 90 * 86400
    # BT-PR5 and BT-PR6: the flags and the click, on the event's own user, product and time
    by_id = {u["user_id"]: u for u in made}
    reached = dict.fromkeys(
        ("morning", "weekend", "budget", "band", "search", "click", "pass"), 0
    )
    for number, record in enumerate(events, start=1):
        value = record["value"]
        when = START + timedelta(seconds=record["sim_ts"])
        context = result.time_context(when)
        assert value["morning"] == context["is_morning"]
        assert value["evening"] == context["is_evening"]
        assert value["weekend"] == context["is_weekend"]
        user = by_id[value["user"]]
        item = result.items[value["product"]]
        user_ctx = {
            **context,
            "age": (user["age"] - 16) / 54,
            "traffic_source_Search": 1 if user["traffic_source"] == "Search" else 0,
        }
        item_ctx = {k: float(v) for k, v in item.items() if k != "product_id"}
        probability = result.click_probability(user_ctx, item_ctx)
        want = result.will_click(user_ctx, item_ctx, number)
        assert value["click"] == bool(want), (number, value, probability)
        reached["click" if want else "pass"] += 1
        reached["morning"] += bool(context["is_morning"] and item_ctx["is_coffee"])
        reached["weekend"] += bool(
            context["is_weekend"]
            and (item_ctx["cat_Pizzas"] or item_ctx["cat_Burgers & Sandwiches"])
        )
        reached["budget"] += bool(user_ctx["age"] < 0.25 and item_ctx["price"] > 0.8)
        # a user just above the budget rule's age, shown a costly product, whose draw
        # falls between the chances with and without the rule
        if 0.25 <= user_ctx["age"] < 0.3 and item_ctx["price"] > 0.8:
            young = result.click_probability({**user_ctx, "age": 0.0}, item_ctx)
            reached["band"] += young <= _draw(number, CLICK) < probability
        reached["search"] += user_ctx["traffic_source_Search"]
    assert all(reached.values()), reached  # every rule and both outcomes are reached
    # BT-PR9 and BT-PR10: feedback events of the project's model, keyed by product, every 0.1 s
    feedback = _events(records, "rank")
    fields = [f.name for f in dataclasses.fields(result.models.FeedbackEvent)]
    schema = [f["name"] for f in json.loads(result.models.FEEDBACK_SCHEMA)["fields"]]
    assert fields == schema
    assert len(feedback) == 50
    for number, record in enumerate(feedback, start=1):
        event = result.models.FeedbackEvent(**record["value"])
        assert record["key"] == event.product_id and isinstance(event.product_id, str)
        assert event.reward in (0, 1) and event.event_id.startswith("evt_")
        assert abs(record["sim_ts"] - 0.1 * number) < 1e-9
    # BT-PR7: the files for later steps are written by a Parquet sink
    app = SimulationContext.from_yaml(HERE / "model.yaml", variables=VARIABLES)
    parquet, kafka = [sink.provider for sink in app._sinks.egress]
    assert isinstance(parquet, ParquetStorageEgress)
    # BT-PR9: the feedback events go to Kafka in Avro, with the project's schema, topic
    # and key; the value is the event alone
    assert isinstance(kafka, KafkaEgress) and [
        sink.envelope for sink in app._sinks.egress
    ] == [True, False]
    assert kafka.event_topic == result.config.FEEDBACK_TOPIC
    serializer = kafka.default_serializer
    assert isinstance(serializer, ConfluentAvroSerializer)
    assert json.loads(serializer._schema_str) == json.loads(
        result.models.FEEDBACK_SCHEMA
    )
    assert kafka.producer_config["bootstrap_servers"] == result.config.BOOTSTRAP_SERVERS
    assert serializer._client._base_urls == [result.config.SCHEMA_REGISTRY]


def _prices() -> dict[int, float]:
    with open(HERE / "products.csv", encoding="utf-8") as handle:
        return {int(r["product_id"]): float(r["price"]) for r in csv.DictReader(handle)}
