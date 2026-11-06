"""BT-OS1 to BT-OS10: the order-streams JSON producer.

The project's generator is Kotlin (`orders-json-clients/.../ProducerApp.kt` of
`~/personal/benchtop/order-streams`, read only), so there is no Python function to run.
`check` states what that producer does, from its source, and the test runs it on the
records of the YAML form. The producer sends one `Order` a second, keyed by its id, with
a UUID, a bid time of the form `yyyy-MM-dd HH:mm:ss` up to `DELAY_SECONDS` seconds in
the past in the machine's own time zone (`ZoneId.systemDefault()`), for which the
document's `timezone: UTC` stands, a price from 1 to 150 with two decimals, a Datafaker product name and one of
eight supplier names, to a topic of three partitions, as JSON without an envelope.

The Avro producer (`orders-avro-clients/.../ProducerApp.kt`) sends the same order through
the schema registry with the credentials `admin:admin`. The document has one sink for
each producer, and `check` has each row follow its own producer: OS8 the JSON value, OS9
the Avro value and the registry's credentials, OS10 three partitions for both.

The run is 1,999 orders long, so that the prices reach the top of their range and the
shares of the suppliers show: a price above 149 and no price above 150, and the
suppliers' counts within a chi-square of 18.5 (7 degrees of freedom, p = 0.01; the
document's is 5.4) of the weights the project's `regexify` of eight names gives (OS6).
"""

import base64
import json
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from desflow import KafkaEgress, SimulationContext
from desflow.blueprint.loader import read_document
from desflow.connectors.egress.kafka import ConfluentAvroSerializer, JsonSerializer


HERE = Path(__file__).parent
VARIABLES = {"FACTOR": "0"}
ADJECTIVES = {
    "Small", "Ergonomic", "Rustic", "Intelligent", "Gorgeous", "Incredible",
    "Fantastic", "Practical", "Sleek", "Awesome", "Enormous", "Mediocre",
    "Synergistic", "Heavy Duty", "Lightweight", "Aerodynamic", "Durable",
}  # fmt: skip
MATERIALS = {
    "Steel", "Wooden", "Concrete", "Plastic", "Cotton", "Granite", "Rubber",
    "Leather", "Silk", "Wool", "Linen", "Marble", "Iron", "Bronze", "Copper",
    "Aluminum", "Paper",
}  # fmt: skip
PRODUCTS = {
    "Chair", "Car", "Computer", "Gloves", "Pants", "Shirt", "Table", "Shoes", "Hat",
    "Plate", "Knife", "Bottle", "Coat", "Lamp", "Keyboard", "Bag", "Bench", "Clock",
    "Watch", "Wallet",
}  # fmt: skip
# the weights, in 24ths, of `(Alice|Bob|Carol|Alex|Joe|James|Jane|Jack)` in the
# project's `regexify`: the first letters A, B, C and J take a quarter each, J takes Jo or
# Ja by half, and Ja takes James, Jane or Jack by a third
WEIGHTS = {
    "Alice": 3, "Alex": 3, "Bob": 6, "Carol": 6, "Joe": 3, "James": 1, "Jane": 1, "Jack": 1
}  # fmt: skip
SUPPLIERS = set(WEIGHTS)
DELAY_SECONDS = 5
START = datetime(2026, 1, 1)


def _product(name: str) -> tuple[str, str, str] | None:
    """The adjective, the material and the product of a name of Datafaker's
    `commerce.productName` lists (an adjective may hold two words), or None when the
    name is not one."""
    for adjective in ADJECTIVES:
        if name.startswith(adjective + " "):
            material, _, product = name[len(adjective) + 1 :].rpartition(" ")
            if material in MATERIALS and product in PRODUCTS:
                return adjective, material, product
    return None


def check(records: list[dict[str, Any]]) -> None:
    """The rules of the producer, on the YAML form's records.

    Args:
        records: The records of the YAML form.

    Raises:
        AssertionError: At the first rule the records break.
    """
    orders = [r for r in records if r["stream_type"] == "event"]
    assert len(orders) >= 10
    suppliers, prices, names = set(), [], []
    for number, record in enumerate(orders, start=1):
        value = record["value"]
        assert record["sim_ts"] == number  # BT-OS1: one order a second
        assert set(value) == {"order_id", "bid_time", "price", "item", "supplier"}
        assert str(uuid.UUID(value["order_id"], version=4)) == value["order_id"]  # OS2
        assert record["key"] == value["order_id"]  # OS7: the key is the order id
        bid = datetime.strptime(value["bid_time"], "%Y-%m-%d %H:%M:%S")  # OS3
        sent = START + timedelta(seconds=record["sim_ts"])
        assert sent - timedelta(seconds=DELAY_SECONDS + 1) <= bid <= sent
        assert 1 <= value["price"] <= 150  # OS4
        assert round(value["price"], 2) == value["price"]
        parts = _product(value["item"])
        assert parts, value["item"]  # OS5
        names.append(parts)
        assert value["supplier"] in SUPPLIERS  # OS6
        suppliers.add(value["supplier"])
        prices.append(value["price"])
    assert len(orders) == 1999, len(orders)
    assert len(suppliers) == 8 and len(set(prices)) > 5
    # OS5: every adjective, material and product of the lists is drawn
    assert [{n[i] for n in names} for i in range(3)] == [
        ADJECTIVES,
        MATERIALS,
        PRODUCTS,
    ]
    assert max(prices) > 149 and min(prices) < 2  # OS4: the range is 1 to 150
    expected = {n: w / 24 * len(orders) for n, w in WEIGHTS.items()}
    counts = {n: sum(r["value"]["supplier"] == n for r in orders) for n in WEIGHTS}
    chi = sum((counts[n] - expected[n]) ** 2 / expected[n] for n in WEIGHTS)
    assert chi < 18.5, (
        chi,
        counts,
    )  # OS6: the shares of the first letter and of Jo or Ja
    assert len({r["key"] for r in orders}) == len(orders)
    assert re.fullmatch(
        r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d", orders[0]["value"]["bid_time"]
    )
    # BT-OS3 and BT-OS6: the document's delay is the producer's default, and its
    # supplier weights are those of the project's `regexify`
    document = read_document(HERE / "model.yaml", variables=VARIABLES)[0]
    assert document["variables"]["delay_seconds"] == DELAY_SECONDS
    supplier = document["tasks"]["order"]["payload"]["supplier"]
    assert supplier["options"] == WEIGHTS, supplier
    # BT-OS8 to BT-OS10: the two sinks of the project's two producers, as the file builds them
    app = SimulationContext.from_yaml(HERE / "model.yaml", variables=VARIABLES)
    json_sink, avro_sink = [sink.provider for sink in app._sinks.egress]
    assert all(isinstance(s, KafkaEgress) for s in (json_sink, avro_sink))
    assert [sink.envelope for sink in app._sinks.egress] == [
        False,
        False,
    ]  # OS8: the value is the order alone
    assert (json_sink.event_topic, avro_sink.event_topic) == (
        "orders-json",
        "orders-avro",
    )
    assert json_sink.topic_policy["partitions"] == 3  # OS10: both producers
    assert avro_sink.topic_policy["partitions"] == 3
    assert isinstance(json_sink.default_serializer, JsonSerializer)  # OS8
    serializer = avro_sink.default_serializer
    assert isinstance(serializer, ConfluentAvroSerializer)  # OS9
    schema = json.loads(serializer._schema_str)
    assert [f["name"] for f in schema["fields"]] == [
        "order_id",
        "bid_time",
        "price",
        "item",
        "supplier",
    ]
    assert schema["name"] == "Order"
    # the registry's basic authentication is the project's `admin:admin`
    basic = serializer._client._client.auth
    assert basic._auth_header == "Basic " + base64.b64encode(b"admin:admin").decode()
