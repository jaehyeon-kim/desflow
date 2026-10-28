# Use Avro and Pydantic

`KafkaEgress` writes every record as JSON by default. That suits lifecycle events and telemetry, but a consumer that needs a compact, schema-checked format, such as a machine learning service reading predictions, can read Avro instead.

Each topic can have its own serializer, so JSON and Avro topics share one producer. A Pydantic model can be published as it is. It builds on [Egress](../user-guide/egress.md#kafka), which explains `KafkaEgress`, and on [Route Records to Topics](complex-routing.md), which explains a topic router.

The code below publishes to Kafka, so start a broker first with `odctl up kafka-lite`. See [Installation](../getting-started.md) for the one-time odctl install. Stop it with `odctl down kafka-lite --volumes`. Each serializer needs its extra: `pip install "desflow[confluent]"` for the Confluent Schema Registry, or `pip install "desflow[glue]"` for the AWS Glue Schema Registry. Both include `kafka`.

---

## Publish a Pydantic Model

`publish_event` turns a Pydantic V2 model into a JSON-ready dict before any egress sees it, so you do not convert it yourself. Pydantic V1 models, including `pydantic.v1`, are not supported and raise a `PydanticSerializationError`.

```python title="docs/snippets/guides/avro_and_pydantic.py:model"
from pydantic import BaseModel

from desflow import DynamicRealtimeEnvironment


class TaskEvent(BaseModel):
    path_id: str
    status: str


def work_task(env: DynamicRealtimeEnvironment, number: int):
    # The model is passed as it is; publish_event turns it into a dict.
    event = TaskEvent(path_id="Shop.service.packing", status="queued")
    env.publish_event(f"orders-{number + 1}", event, task="pack_order", instance=number)
    yield env.timeout(0)
```

---

## Avro for Some Topics

A topic router picks the topic of each record, and `topic_serializers` names the serializer of each topic. A topic with no serializer in the mapping uses the default, `JsonSerializer`.

![A Pydantic model published with publish_event becomes a record whose value is a dict; the KafkaEgress topic_router picks a topic, which is written as Avro if it is in topic_serializers and as JSON otherwise](../assets/diagrams/serializer-routing.png)

`KafkaEgress` serializes the whole record, not only its `value`: `key`, `task`, `instance`, `sim_ts`, `timestamp` and `value`, without `stream_type`. `task` and `instance` are null on an event published without them, so the schema makes both nullable. An Avro schema therefore describes that record, with your payload as the `value` field. `ConfluentAvroSerializer` uses a Confluent Schema Registry, and `GlueAvroSerializer` uses an AWS Glue Schema Registry.

```python title="docs/snippets/guides/avro_and_pydantic.py:router"
from desflow import KafkaEgress
from desflow.connectors.egress.kafka import ConfluentAvroSerializer

# An Avro serializer for the predictions topic. The schema describes the whole record
# KafkaEgress sends, with the prediction as its value.
prediction_serializer = ConfluentAvroSerializer(
    registry_url="http://127.0.0.1:8081",
    schema_str="""{
      "type": "record", "name": "PredictionEvent", "namespace": "com.desflow.ml",
      "fields": [
        {"name": "key", "type": "string"},
        {"name": "task", "type": ["null", "string"], "default": null},
        {"name": "instance", "type": ["null", "long"], "default": null},
        {"name": "sim_ts", "type": "double"},
        {"name": "timestamp", "type": "string"},
        {"name": "value", "type": {
          "type": "record", "name": "MLPrediction",
          "fields": [
            {"name": "event_type", "type": "string"},
            {"name": "task_id", "type": "string"},
            {"name": "confidence", "type": "double"}
          ]
        }}
      ]
    }""",
)


# A topic router that sends predictions to their own topic.
def ml_topic_router(data: dict) -> str:
    if data.get("stream_type") == "telemetry":
        return "sim-telemetry"
    value = data.get("value", {})
    if value.get("event_type") == "prediction":
        return "ml-predictions"
    return "sim-events"


egress = KafkaEgress(
    bootstrap_servers="127.0.0.1:9092",
    topic_router=ml_topic_router,
    # Only ml-predictions is Avro; sim-telemetry and sim-events use JSON.
    topic_serializers={"ml-predictions": prediction_serializer},
)
```

---

## Generate the Schema from the Model

A schema written by hand as a JSON string stops matching the model it describes when someone changes the model and not the schema. The `dataclasses-avroschema` package generates it from a Pydantic model instead. desflow's extras do not install it, so install it with `pip install "dataclasses-avroschema[pydantic]"`. `AvroBaseModel` is a Pydantic model that can also write its Avro schema:

```python title="docs/snippets/guides/avro_and_pydantic.py:schema"
from typing import Optional

from dataclasses_avroschema.pydantic import AvroBaseModel
from pydantic import Field


class MLPrediction(AvroBaseModel):
    """The prediction, published as the event value."""

    event_type: str = "prediction"
    task_id: str = Field(...)
    confidence: float = Field(...)


class PredictionRecord(AvroBaseModel):
    """The record KafkaEgress serializes for an event."""

    key: str
    task: Optional[str] = None
    instance: Optional[int] = None
    sim_ts: float
    timestamp: str
    value: MLPrediction

    class Meta:
        namespace = "com.desflow.ml"
        schema_name = "PredictionEvent"


schema_string = PredictionRecord.avro_schema()
```

Both `ConfluentAvroSerializer` and `GlueAvroSerializer` take the generated string as `schema_str`:

```python title="docs/snippets/guides/avro_and_pydantic.py:generated"
generated_serializer = ConfluentAvroSerializer(
    registry_url="http://127.0.0.1:8081",
    schema_str=PredictionRecord.avro_schema(),  # follows the model
)

egress = KafkaEgress(
    bootstrap_servers="127.0.0.1:9092",
    topic_router=ml_topic_router,
    topic_serializers={"ml-predictions": generated_serializer},
)
```

---

## Registry Connection

For a secured registry, `ConfluentAvroSerializer` and `ConfluentAvroDeserializer` take `auth` for a user name and password, `verify` for how to check the registry's certificate, `cert` for a client certificate, `headers` for extra headers such as a bearer token, and `proxy` for the URL of a proxy. `verify` is `True`, `False`, the path of a CA file or directory, or an `ssl.SSLContext`. `cert` is a certificate file, or a tuple of the certificate file, the key file and the key password.

Each step of a request to the registry (connecting, sending, reading) times out after 10 seconds by default. `timeout=` sets another number of seconds, and `timeout=None` waits without a limit. A request that times out is retried up to `max_retries` times (3 by default) with a short random wait. A registry that never answers therefore holds up every sink of the run (for the serializer) or every source (for the deserializer) for up to about 50 seconds for each registry URL, on each request. The Kafka sink then logs the error, drops the rest of that batch and reconnects, and the next batch asks the registry again. The Kafka source logs the error and skips that message, and the next message asks the registry again. The simulation goes on while each sink's queue has room. Once a queue stays full, the run stops with an error, as [Egress](../user-guide/egress.md#how-it-works) describes.

```python title="docs/snippets/guides/avro_and_pydantic.py:secured-registry"
import ssl

import certifi

from desflow.connectors.egress.kafka import ConfluentAvroSerializer

# A registry behind HTTPS and basic authentication, where each step of a request may take 30 seconds.
secured_serializer = ConfluentAvroSerializer(
    registry_url="https://registry.example.com:8081",
    schema_str=PredictionRecord.avro_schema(),
    auth=("registry-user", "registry-password"),
    # a private CA: cafile="ca.pem"
    verify=ssl.create_default_context(cafile=certifi.where()),
    timeout=30,
)
```

A YAML blueprint sets these arguments in the module its `!python` tag loads, as [Call Python from YAML](yaml-advanced.md#routers-serializers-and-connector-classes) shows.

---

## Run the Shop

In this shop, each packed order's finished event is a prediction: the task returns an `MLPrediction`, and the router sends it to `ml-predictions` as Avro. The `queued` and `started` events go to `sim-events` as JSON. Before the run, the script creates the three topics, because `KafkaEgress` creates none when it has a router:

```python title="docs/snippets/guides/avro_and_pydantic.py:shop"
import logging

from desflow import KafkaAdminConnector, SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(egress)  # predictions as Avro, the rest as JSON
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int, entity):
    # The finished event is a prediction for the order, published as the model
    return MLPrediction(task_id=entity.key, confidence=0.9)


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    # With a router, KafkaEgress creates no topics, so create them first
    admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
    topics = ["sim-events", "sim-telemetry", "ml-predictions"]
    admin.create_topics([{"name": topic} for topic in topics])
    app.run(until="1 min")
```

Run it with `python avro_and_pydantic.py` while `kafka-lite` is up. The serializer registers the generated schema with the schema registry of `kafka-lite` on its first send.

---

## Example

??? example "Full script"

    The complete file is below. It needs the `kafka-lite` profile, which runs a Kafka broker and a schema registry:

    ```python title="docs/snippets/guides/avro_and_pydantic.py"
    """The Pydantic models, topic router and Avro serializers of the Use Avro and Pydantic page.

    The shop at the end publishes predictions as Avro and every other record as JSON. It
    needs a Kafka broker and a schema registry: start both with `odctl up kafka-lite`, then
    run `python avro_and_pydantic.py`.
    """

    # Each section carries its own imports, as the page shows it.
    # ruff: noqa: E402

    from pydantic import BaseModel

    from desflow import DynamicRealtimeEnvironment


    class TaskEvent(BaseModel):
        path_id: str
        status: str


    def work_task(env: DynamicRealtimeEnvironment, number: int):
        # The model is passed as it is; publish_event turns it into a dict.
        event = TaskEvent(path_id="Shop.service.packing", status="queued")
        env.publish_event(f"orders-{number + 1}", event, task="pack_order", instance=number)
        yield env.timeout(0)


    from desflow import KafkaEgress
    from desflow.connectors.egress.kafka import ConfluentAvroSerializer

    # An Avro serializer for the predictions topic. The schema describes the whole record
    # KafkaEgress sends, with the prediction as its value.
    prediction_serializer = ConfluentAvroSerializer(
        registry_url="http://127.0.0.1:8081",
        schema_str="""{
          "type": "record", "name": "PredictionEvent", "namespace": "com.desflow.ml",
          "fields": [
            {"name": "key", "type": "string"},
            {"name": "task", "type": ["null", "string"], "default": null},
            {"name": "instance", "type": ["null", "long"], "default": null},
            {"name": "sim_ts", "type": "double"},
            {"name": "timestamp", "type": "string"},
            {"name": "value", "type": {
              "type": "record", "name": "MLPrediction",
              "fields": [
                {"name": "event_type", "type": "string"},
                {"name": "task_id", "type": "string"},
                {"name": "confidence", "type": "double"}
              ]
            }}
          ]
        }""",
    )


    # A topic router that sends predictions to their own topic.
    def ml_topic_router(data: dict) -> str:
        if data.get("stream_type") == "telemetry":
            return "sim-telemetry"
        value = data.get("value", {})
        if value.get("event_type") == "prediction":
            return "ml-predictions"
        return "sim-events"


    egress = KafkaEgress(
        bootstrap_servers="127.0.0.1:9092",
        topic_router=ml_topic_router,
        # Only ml-predictions is Avro; sim-telemetry and sim-events use JSON.
        topic_serializers={"ml-predictions": prediction_serializer},
    )


    from typing import Optional

    from dataclasses_avroschema.pydantic import AvroBaseModel
    from pydantic import Field


    class MLPrediction(AvroBaseModel):
        """The prediction, published as the event value."""

        event_type: str = "prediction"
        task_id: str = Field(...)
        confidence: float = Field(...)


    class PredictionRecord(AvroBaseModel):
        """The record KafkaEgress serializes for an event."""

        key: str
        task: Optional[str] = None
        instance: Optional[int] = None
        sim_ts: float
        timestamp: str
        value: MLPrediction

        class Meta:
            namespace = "com.desflow.ml"
            schema_name = "PredictionEvent"


    schema_string = PredictionRecord.avro_schema()


    generated_serializer = ConfluentAvroSerializer(
        registry_url="http://127.0.0.1:8081",
        schema_str=PredictionRecord.avro_schema(),  # follows the model
    )

    egress = KafkaEgress(
        bootstrap_servers="127.0.0.1:9092",
        topic_router=ml_topic_router,
        topic_serializers={"ml-predictions": generated_serializer},
    )


    import ssl

    import certifi

    from desflow.connectors.egress.kafka import ConfluentAvroSerializer

    # A registry behind HTTPS and basic authentication, where each step of a request may take 30 seconds.
    secured_serializer = ConfluentAvroSerializer(
        registry_url="https://registry.example.com:8081",
        schema_str=PredictionRecord.avro_schema(),
        auth=("registry-user", "registry-password"),
        # a private CA: cafile="ca.pem"
        verify=ssl.create_default_context(cafile=certifi.where()),
        timeout=30,
    )


    import logging

    from desflow import KafkaAdminConnector, SimulationContext

    app = (
        SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        .add_egress(egress)  # predictions as Avro, the rest as JSON
    )


    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, entity):
        # The finished event is a prediction for the order, published as the model
        return MLPrediction(task_id=entity.key, confidence=0.9)


    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


    if __name__ == "__main__":
        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
        )
        # With a router, KafkaEgress creates no topics, so create them first
        admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
        topics = ["sim-events", "sim-telemetry", "ml-predictions"]
        admin.create_topics([{"name": topic} for topic in topics])
        app.run(until="1 min")
    ```
