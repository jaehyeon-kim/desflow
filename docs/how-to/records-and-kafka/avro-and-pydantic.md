# Use Avro and Pydantic

`KafkaEgress` writes every record as JSON by default. That suits lifecycle events and telemetry, but a consumer that needs a compact, schema-checked format, such as a machine learning service reading predictions, can read Avro instead.

Each topic can have its own serializer, so JSON and Avro topics share one producer. A Pydantic model can be published as it is.

---

## Before You Start

- [Egress](../../user-guide/live-runs-and-connectors/egress.md#kafka) explains `KafkaEgress`.
- [Route Records to Topics](complex-routing.md) explains a topic router.
- The code publishes to Kafka, so start a broker first with `odctl up kafka-lite`. See [Installation](../../installation.md) for the one-time odctl install. Stop it with `odctl down kafka-lite --volumes`.
- The models need `dataclasses-avroschema` with its Pydantic support, in every form: `pip install "dataclasses-avroschema[pydantic]"`.
- Each serializer needs its extra: `pip install "desflow[confluent]"` for the Confluent Schema Registry, or `pip install "desflow[glue]"` for the AWS Glue Schema Registry. Both include `kafka`.

---

## Steps

### Publish a Pydantic Model

`publish_event` turns a Pydantic V2 model into a JSON-ready dict before any egress sees it, so you do not convert it yourself. Pydantic V1 models, including `pydantic.v1`, are not supported and raise a `PydanticSerializationError`. A task publishes a model as its finished event in every form. `MLPrediction` is the model that [Generate the Schema from the Model](#generate-the-schema-from-the-model) defines:

=== "YAML"

    ```python title="docs/snippets/yaml/avro_and_pydantic_logic.py:prediction"
    def prediction(task_id, context, entity):
        """The finished event of a packed order: a prediction, published as the model."""
        return MLPrediction(task_id=entity.key, confidence=0.9)
    ```

    The blueprint names it as the task's `payload`, as [Run the Shop](#run-the-shop) shows.

=== "Builder"

    ```python title="docs/snippets/guides/avro_and_pydantic.py:task"
    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, entity):
        # The finished event is a prediction for the order, published as the model
        return MLPrediction(task_id=entity.key, confidence=0.9)
    ```

=== "Low-level"

    `publish_event` takes the model as the value, in the last two lines of `pack_order`:

    ```python title="docs/snippets/guides/avro_and_pydantic_low_level.py:model"
    def pack_order(env, packers, sampler, key, task_id):
        path_id = "Shop.service.packing"
        env.publish_event(
            key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
        )
        with packers.request() as unit:
            yield unit
            env.publish_event(
                key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
            )
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            # The finished event is a prediction for the order, published as the model;
            # publish_event turns it into a dict
            prediction = MLPrediction(task_id=key, confidence=0.9)
            env.publish_event(key, prediction, "pack_order", task_id)
    ```

### Avro for Some Topics

From here on, the code is the same for every form. A topic router picks the topic of each record, and `topic_serializers` names the serializer of each topic. A topic with no serializer in the mapping uses the default, `JsonSerializer`.

![A Pydantic model published with publish_event becomes a record whose value is a dict; the KafkaEgress topic_router picks a topic, which is written as Avro if it is in topic_serializers and as JSON otherwise](../../assets/diagrams/serializer-routing.png)

`KafkaEgress` serializes the whole record, not only its `value`: `key`, `task`, `instance`, `sim_ts`, `timestamp` and `value`, without `stream_type`. `task` and `instance` are null on an event published without them, so the schema makes both nullable. An Avro schema therefore describes that record, with your payload as the `value` field. `ConfluentAvroSerializer` uses a Confluent Schema Registry, and `GlueAvroSerializer` uses an AWS Glue Schema Registry.

```python title="docs/snippets/guides/avro_and_pydantic_models.py:router"
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

### Generate the Schema from the Model

A schema written by hand as a JSON string stops matching the model it describes when someone changes the model and not the schema. The `dataclasses-avroschema` package generates it from a Pydantic model instead. desflow's extras do not install it, so install it with `pip install "dataclasses-avroschema[pydantic]"`. `AvroBaseModel` is a Pydantic model that can also write its Avro schema:

```python title="docs/snippets/guides/avro_and_pydantic_models.py:schema"
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

```python title="docs/snippets/guides/avro_and_pydantic_models.py:generated"
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

### Registry Connection

For a secured registry, `ConfluentAvroSerializer` and `ConfluentAvroDeserializer` take `auth` for a user name and password, `verify` for how to check the registry's certificate, `cert` for a client certificate, `headers` for extra headers such as a bearer token, and `proxy` for the URL of a proxy. `verify` is `True`, `False`, the path of a CA file or directory, or an `ssl.SSLContext`. `cert` is a certificate file, or a tuple of the certificate file, the key file and the key password.

Each step of a request to the registry (connecting, sending, reading) times out after 10 seconds by default. `timeout=` sets another number of seconds, and `timeout=None` waits without a limit. A request that times out is retried up to `max_retries` times (3 by default) with a short random wait. A registry that never answers therefore holds up every sink of the run (for the serializer) or every source (for the deserializer) for up to about 50 seconds for each registry URL, on each request. The Kafka sink then logs the error, drops the rest of that batch and reconnects, and the next batch asks the registry again. The Kafka source logs the error and skips that message, and the next message asks the registry again. The simulation goes on while each sink's queue has room. Once a queue stays full, the run stops with an error, as [Egress](../../user-guide/live-runs-and-connectors/egress.md#how-it-works) describes.

`PredictionRecord` is the model from the previous section, imported from `avro_and_pydantic_models.py`:

```python title="docs/snippets/guides/avro_and_pydantic_registry.py:secured-registry"
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

The Confluent JSON Schema and Protobuf classes of [JSON Schema and Protobuf](json-schema-and-protobuf.md) take the same arguments.

A YAML blueprint sets these arguments in the module its `!python` tag loads, as [Call Python from YAML](../tools/yaml-advanced.md#routers-serializers-and-connector-classes) shows.

### Run the Shop

In this shop, each packed order's finished event is a prediction: the task returns an `MLPrediction`, and the router sends it to `ml-predictions` as Avro. The `queued` and `started` events go to `sim-events` as JSON. Before the run, each form creates the three topics, because `KafkaEgress` creates none when it has a router. The blueprint does it in `run.before`:

=== "YAML"

    ```yaml title="docs/snippets/yaml/avro_and_pydantic.yaml:shop"
    simulation:
      sim_id: Shop
      factor: 1
      random_seed: 42

    resources:
      packers: {current_cap: 1, max_cap: 1}

    services:
      packing: {dist: constant, value: 1.5}

    arrivals:
      orders: {dist: exponential, rate: 0.5, spawn: pack_order}

    tasks:
      pack_order:
        service: packing
        resource: packers
        # The finished event is a prediction for the order, published as the model
        payload: !python avro_and_pydantic_logic.prediction

    egress:
      # Predictions as Avro, the rest as JSON
      - type: Kafka
        config:
          bootstrap_servers: 127.0.0.1:9092
          topic_router: !python avro_and_pydantic_models.ml_topic_router
          topic_serializers:
            ml-predictions: !python avro_and_pydantic_models.generated_serializer

    run:
      until: 1 min
      before:
        # With a router, KafkaEgress creates no topics, so create them first
        - !python avro_and_pydantic_logic.create_topics
    ```

=== "Builder"

    ```python title="docs/snippets/guides/avro_and_pydantic.py:shop"
    import logging

    from avro_and_pydantic_models import MLPrediction, egress

    from desflow import KafkaAdminConnector, SimulationContext

    app = (
        SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        .add_egress(egress)  # predictions as Avro, the rest as JSON
    )
    ```

    ```python title="docs/snippets/guides/avro_and_pydantic.py:task"
    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, entity):
        # The finished event is a prediction for the order, published as the model
        return MLPrediction(task_id=entity.key, confidence=0.9)
    ```

    ```python title="docs/snippets/guides/avro_and_pydantic.py:run"
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

=== "Low-level"

    The shop uses `pack_order` from [Publish a Pydantic Model](#publish-a-pydantic-model):

    ```python title="docs/snippets/guides/avro_and_pydantic_low_level.py:shop"
    def orders(env, packers, sampler):
        gap = env.registry.get_config("Shop.arrival.orders")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


    def shop(sinks: list, factor: float = 1.0) -> DynamicRealtimeEnvironment:
        """The shop, publishing to the sinks given."""
        env = DynamicRealtimeEnvironment(factor=factor)
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress(sinks)
        packers = DynamicResource(env, "Shop", "packers")
        env.process(orders(env, packers, Sampler(seed=42)))
        return env
    ```

---

## Check the Result

Run `desflow run avro_and_pydantic.yaml`, `python avro_and_pydantic.py` or `python avro_and_pydantic_low_level.py` while `kafka-lite` is up. The serializer registers the generated schema with the schema registry of `kafka-lite` on its first send. In every form, the `queued` and `started` events go to `sim-events` as JSON, and each prediction goes to `ml-predictions` and fits the generated schema.

To read a prediction back, run the models file after a shop has run. Its last lines build a consumer of `ml-predictions` and a `ConfluentAvroDeserializer`, which fetches the schema that the message names from the registry:

```python title="docs/snippets/guides/avro_and_pydantic_models.py:check"
import asyncio

from aiokafka import AIOKafkaConsumer

from desflow.connectors.ingress.kafka import ConfluentAvroDeserializer


async def show_first_prediction() -> None:
    """Prints the first message of ml-predictions, decoded with the schema it names."""
    consumer = AIOKafkaConsumer(
        "ml-predictions",
        bootstrap_servers="127.0.0.1:9092",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=10)
        reader = ConfluentAvroDeserializer(registry_url="http://127.0.0.1:8081")
        print(reader.deserialize("ml-predictions", message.value))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(show_first_prediction())
```

```bash
python avro_and_pydantic_models.py
```

```text
{'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'sim_ts': 4.052, 'timestamp': '2026-10-31T00:51:56.940+00:00', 'value': {'event_type': 'prediction', 'task_id': 'orders-1', 'confidence': 0.9}}
```

The `timestamp` is the clock time of your run. The registry holds the subject `ml-predictions-value`, which `curl http://127.0.0.1:8081/subjects` lists.

---

## Full Files

Every form uses the models, the router and the serializer in `avro_and_pydantic_models.py`, which builds no shop, so keep the files in one folder. The registry file imports its model from it.

??? example "Full files"

    ```python title="docs/snippets/guides/avro_and_pydantic_models.py"
    """The Pydantic models, topic router and Avro serializers of the Use Avro and Pydantic page.

    Every form of the shop imports the models, the router and the serializers from this file:
    avro_and_pydantic.py (builder), avro_and_pydantic.yaml and avro_and_pydantic_low_level.py.
    It builds no simulation. Install `dataclasses-avroschema[pydantic]` for the models.

    After a shop has run, `python avro_and_pydantic_models.py` prints the first prediction
    of ml-predictions.
    """

    # Each section carries its own imports, as the page shows it.
    # ruff: noqa: E402

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


    import asyncio

    from aiokafka import AIOKafkaConsumer

    from desflow.connectors.ingress.kafka import ConfluentAvroDeserializer


    async def show_first_prediction() -> None:
        """Prints the first message of ml-predictions, decoded with the schema it names."""
        consumer = AIOKafkaConsumer(
            "ml-predictions",
            bootstrap_servers="127.0.0.1:9092",
            auto_offset_reset="earliest",
        )
        await consumer.start()
        try:
            message = await asyncio.wait_for(consumer.getone(), timeout=10)
            reader = ConfluentAvroDeserializer(registry_url="http://127.0.0.1:8081")
            print(reader.deserialize("ml-predictions", message.value))
        finally:
            await consumer.stop()


    if __name__ == "__main__":
        asyncio.run(show_first_prediction())
    ```

    ```python title="docs/snippets/guides/avro_and_pydantic_registry.py"
    """The Registry Connection section of the Use Avro and Pydantic page: a serializer for a
    schema registry behind HTTPS and basic authentication.

    It needs the models of avro_and_pydantic_models.py beside it and builds the serializer when
    it is imported, so no shop imports it.
    """

    # ruff: noqa: E402

    from avro_and_pydantic_models import PredictionRecord

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

    === "YAML"

        ```yaml title="docs/snippets/yaml/avro_and_pydantic.yaml"
        # The Use Avro and Pydantic guide: each packed order publishes a prediction, sent to
        # ml-predictions as Avro; every other record goes to its topic as JSON. Needs
        # avro_and_pydantic_models.py and avro_and_pydantic_logic.py beside it, and a Kafka broker
        # with a schema registry: odctl up kafka-lite. Run it with:
        # desflow run avro_and_pydantic.yaml
        simulation:
          sim_id: Shop
          factor: 1
          random_seed: 42

        resources:
          packers: {current_cap: 1, max_cap: 1}

        services:
          packing: {dist: constant, value: 1.5}

        arrivals:
          orders: {dist: exponential, rate: 0.5, spawn: pack_order}

        tasks:
          pack_order:
            service: packing
            resource: packers
            # The finished event is a prediction for the order, published as the model
            payload: !python avro_and_pydantic_logic.prediction

        egress:
          # Predictions as Avro, the rest as JSON
          - type: Kafka
            config:
              bootstrap_servers: 127.0.0.1:9092
              topic_router: !python avro_and_pydantic_models.ml_topic_router
              topic_serializers:
                ml-predictions: !python avro_and_pydantic_models.generated_serializer

        run:
          until: 1 min
          before:
            # With a router, KafkaEgress creates no topics, so create them first
            - !python avro_and_pydantic_logic.create_topics
        ```

        ```python title="docs/snippets/yaml/avro_and_pydantic_logic.py"
        """Python for avro_and_pydantic.yaml: the prediction each packed order publishes, and
        the topics the run needs. The models, the router and the serializer are those of
        avro_and_pydantic_models.py, which must be beside the blueprint."""

        from avro_and_pydantic_models import MLPrediction

        from desflow import KafkaAdminConnector

        TOPICS = ["sim-events", "sim-telemetry", "ml-predictions"]


        def prediction(task_id, context, entity):
            """The finished event of a packed order: a prediction, published as the model."""
            return MLPrediction(task_id=entity.key, confidence=0.9)


        def create_topics():
            """With a router, KafkaEgress creates no topics, so create them first."""
            admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
            admin.create_topics([{"name": topic} for topic in TOPICS])
        ```

    === "Builder"

        ```python title="docs/snippets/guides/avro_and_pydantic.py"
        """The shop of the Use Avro and Pydantic page, built with the builder API.

        The shop publishes predictions as Avro and every other record as JSON. It needs a Kafka
        broker and a schema registry: start both with `odctl up kafka-lite`, then run
        `python avro_and_pydantic.py`. avro_and_pydantic.yaml and avro_and_pydantic_low_level.py
        are the same shop in YAML and the low-level API. All three import the models, router and
        serializer of avro_and_pydantic_models.py, which must be beside them.
        """

        # ruff: noqa: E402

        import logging

        from avro_and_pydantic_models import MLPrediction, egress

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

    === "Low-level"

        ```python title="docs/snippets/guides/avro_and_pydantic_low_level.py"
        """The Use Avro and Pydantic guide, low-level API: each packed order publishes a
        prediction, sent to ml-predictions as Avro, and every other record goes to its topic
        as JSON.

        The model, the router and the serializer are those of avro_and_pydantic_models.py, which
        must be beside this script. It needs a Kafka broker and a schema registry: start both with
        `odctl up kafka-lite`, then run `python avro_and_pydantic_low_level.py`.
        """

        import itertools
        import logging

        from avro_and_pydantic_models import MLPrediction, egress

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            KafkaAdminConnector,
            Sampler,
            SimParameter,
        )

        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
            service={"packing": DistributionConfig(dist="constant", value=1.5)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
        )


        def pack_order(env, packers, sampler, key, task_id):
            path_id = "Shop.service.packing"
            env.publish_event(
                key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
            )
            with packers.request() as unit:
                yield unit
                env.publish_event(
                    key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
                )
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                # The finished event is a prediction for the order, published as the model;
                # publish_event turns it into a dict
                prediction = MLPrediction(task_id=key, confidence=0.9)
                env.publish_event(key, prediction, "pack_order", task_id)


        def orders(env, packers, sampler):
            gap = env.registry.get_config("Shop.arrival.orders")
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


        def shop(sinks: list, factor: float = 1.0) -> DynamicRealtimeEnvironment:
            """The shop, publishing to the sinks given."""
            env = DynamicRealtimeEnvironment(factor=factor)
            env.registry.register_sim_parameter(PARAMS)
            env.setup_egress(sinks)
            packers = DynamicResource(env, "Shop", "packers")
            env.process(orders(env, packers, Sampler(seed=42)))
            return env


        if __name__ == "__main__":
            logging.basicConfig(
                level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
            )
            # With a router, KafkaEgress creates no topics, so create them first
            admin = KafkaAdminConnector(bootstrap_servers="127.0.0.1:9092")
            topics = ["sim-events", "sim-telemetry", "ml-predictions"]
            admin.create_topics([{"name": topic} for topic in topics])
            env = shop([egress])  # predictions as Avro, the rest as JSON
            try:
                env.run(until=60)
            finally:
                env.teardown()
        ```
