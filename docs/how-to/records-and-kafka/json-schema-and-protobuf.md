# Use Schema Serializers

Send orders to two Kafka topics, one as JSON checked against a JSON Schema and one as Protobuf, and read each back with the matching deserializer.

JSON Schema keeps a record as JSON and checks it against a schema before it is sent. Protobuf sends it as a compact binary message of a class generated from a `.proto` file. Either way, a consumer can rely on the shape of every record it reads. Both formats use the Confluent Schema Registry or the AWS Glue Schema Registry, and each topic can have its own serializer.

---

## Before You Start

- [Use Avro and Pydantic](avro-and-pydantic.md) shows the same steps for Avro, and [Route Records to Topics](complex-routing.md) explains `__topic__`, which this page uses to pick the topic.
- The code publishes to Kafka and to a schema registry, so start both with `odctl up kafka-lite`. See [Installation](../../installation.md) for the one-time odctl install. Stop it with `odctl down kafka-lite --volumes`.
- Each serializer needs its extra: `pip install "desflow[confluent]"` for the Confluent Schema Registry, or `pip install "desflow[glue]"` for the AWS Glue Schema Registry. Both include `kafka`.

---

## Steps

### Describe the Record in JSON Schema

As with [Avro](avro-and-pydantic.md#generate-the-schema-from-a-model), the schema describes the whole record `KafkaEgress` sends, with your payload as `value`. The serializer raises `SerializationError` for a record that does not match, and a deserializer checks each record it reads in the same way. A blueprint reads the schema from a file, `order_record.schema.json`, which holds the same schema as this string:

```python title="docs/snippets/guides/json_schema_and_protobuf_serializers.py:json-schema"
from desflow.connectors.egress.kafka import ConfluentJsonSchemaSerializer

# The schema describes the whole record KafkaEgress sends, with the order as its value.
json_schema = """{
  "type": "object",
  "properties": {
    "key": {"type": "string"},
    "task": {"type": ["string", "null"]},
    "instance": {"type": ["integer", "null"]},
    "sim_ts": {"type": "number"},
    "timestamp": {"type": "string"},
    "value": {
      "type": "object",
      "properties": {
        "order_id": {"type": "integer"},
        "amount": {"type": "number"}
      },
      "required": ["order_id", "amount"]
    }
  },
  "required": ["key", "sim_ts", "timestamp", "value"]
}"""

json_serializer = ConfluentJsonSchemaSerializer(
    registry_url="http://127.0.0.1:8081", schema_str=json_schema
)
```

`ConfluentJsonSchemaSerializer` registers the schema as you wrote it, under the subject `<topic>-value`, on the first record of each topic. It checks a schema with no `$schema` as Draft 2020-12. It registers no references, so the schema must hold every definition it uses, and a `$ref` points only inside the schema. `ConfluentJsonSchemaDeserializer` resolves a `$ref` to a schema that the writer registered as a reference. Neither fetches a schema from a web address.

### Describe the Record in Protobuf

Protobuf sends each record as a message of a class that `protoc` generates from a `.proto` file. This file describes the same record as the JSON schema:

```proto title="docs/snippets/guides/shop_record.proto"
// The record KafkaEgress sends for an order, as the JSON Schema and Protobuf
// guide publishes it. Generate shop_record_pb2.py from it with the command on that page.
syntax = "proto3";

package com.desflow.shop;

message Order {
  int64 order_id = 1;
  double amount = 2;
}

// `optional` keeps a missing task or instance apart from 0 and "", as null is in JSON.
message ShopRecord {
  string key = 1;
  optional string task = 2;
  optional int64 instance = 3;
  double sim_ts = 4;
  string timestamp = 5;
  Order value = 6;
}
```

The code forms use the Python module that `protoc` generates. This command uses the `protoc` that grpcio-tools 1.85.0 bundles, which is protoc 35.1, in the folder that holds the file:

```bash
uvx --from grpcio-tools==1.85.0 python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. shop_record.proto
```

The module checks the protobuf runtime when it is imported and refuses a runtime older than the `protoc` that wrote it. The header of `shop_record_pb2.py` names the oldest runtime that imports it: `Protobuf Python Version: 7.35.1`. For your own file, use a `protoc` whose header shows a version no newer than the protobuf you have installed. A blueprint cannot import a Python module, so it reads a descriptor file that `protoc` writes from the same `.proto` file:

```bash
uvx --from grpcio-tools==1.85.0 python -m grpc_tools.protoc -I. --descriptor_set_out=shop_record.desc --include_imports shop_record.proto
```

The serializer takes the generated class. It registers the file of the class under the subject `<topic>-value` on the first record of each topic, after the files that it imports. It builds the message from the record by field name and drops a key that the file does not have:

```python title="docs/snippets/guides/json_schema_and_protobuf_serializers.py:protobuf"
from shop_record_pb2 import ShopRecord

from desflow.connectors.egress.kafka import ConfluentProtobufSerializer

# message_type is the class that protoc generated. The serializer registers its file.
protobuf_serializer = ConfluentProtobufSerializer(
    registry_url="http://127.0.0.1:8081", message_type=ShopRecord
)
```

### Serialize Two Topics

`topics` gives the settings of each topic, and its `serializer` names the serializer of that topic. A topic with no serializer uses the default, JSON. In a blueprint the serializer is a mapping with a `type`, here `confluent_json_schema` and `confluent_protobuf`, and the registry address.

=== "YAML"

    ```yaml title="docs/snippets/yaml/json_schema_and_protobuf.yaml:egress"
    egress:
      - type: Kafka
        config:
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}
          topics:
            orders-json:
              serializer:
                type: confluent_json_schema
                registry_url: ${SCHEMA_REGISTRY_URL:-http://127.0.0.1:8081}
                schema_file: order_record.schema.json
            orders-protobuf:
              serializer:
                type: confluent_protobuf
                registry_url: ${SCHEMA_REGISTRY_URL:-http://127.0.0.1:8081}
                descriptor_file: shop_record.desc
                message: com.desflow.shop.ShopRecord
    ```

=== "Builder"

    ```python title="docs/snippets/guides/json_schema_and_protobuf.py:egress"
    import logging
    import os

    from json_schema_and_protobuf_serializers import json_serializer, protobuf_serializer

    from desflow import KafkaEgress, SimulationContext

    egress = KafkaEgress(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        topics={
            "orders-json": {"serializer": json_serializer},
            "orders-protobuf": {"serializer": protobuf_serializer},
        },
    )
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/json_schema_and_protobuf_low_level.py:egress"
    egress = KafkaEgress(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        topics={
            "orders-json": {"serializer": json_serializer},
            "orders-protobuf": {"serializer": protobuf_serializer},
        },
    )
    ```

### Publish Orders to Both Topics

Each packed order's finished event is the order record, and it names its topic with `__topic__`. In this shop the orders alternate between the formats, so one run shows both. The `queued` and `started` events name no topic, so they go to `sim-events` as JSON.

=== "YAML"

    ```yaml title="docs/snippets/yaml/json_schema_and_protobuf.yaml:shop"
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
        payload:
          # Orders alternate between the two formats, so one run shows both
          __topic__:
            gen: when
            condition: "process.instance % 2 == 1"
            then: orders-protobuf
            else: orders-json
          order_id: {gen: expr, expr: process.instance}
          amount: 19.99
    ```

=== "Builder"

    ```python title="docs/snippets/guides/json_schema_and_protobuf.py:shop"
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.5)
        .add_egress(egress)  # orders as JSON Schema or Protobuf, the rest as JSON
    )


    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int, entity):
        # Orders alternate between the two formats, so one run shows both
        topic = "orders-protobuf" if task_id % 2 else "orders-json"
        return {"__topic__": topic, "order_id": task_id, "amount": 19.99}


    @app.arrival_loop("orders")
    def orders(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))
    ```

=== "Low-level"

    `shop` takes the sinks as an argument, and the script passes it `[egress]`:

    ```python title="docs/snippets/guides/json_schema_and_protobuf_low_level.py:shop"
    def pack_order(env, packers, sampler, key, task_id):
        path_id = "Shop.service.packing"
        env.publish_event(
            key,
            {
                "path_id": path_id,
                "sim_id": "Shop",
                "service": "packing",
                "status": "queued",
            },
            "pack_order",
            task_id,
        )
        with packers.request() as unit:
            yield unit
            env.publish_event(
                key,
                {
                    "path_id": path_id,
                    "sim_id": "Shop",
                    "service": "packing",
                    "status": "started",
                },
                "pack_order",
                task_id,
            )
            yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
            # Orders alternate between the two formats, so one run shows both
            topic = "orders-protobuf" if task_id % 2 else "orders-json"
            order = {"__topic__": topic, "order_id": task_id, "amount": 19.99}
            env.publish_event(key, order, "pack_order", task_id)


    def orders(env, packers, sampler):
        gap = env.registry.get_config("Shop.arrival.orders")
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


    def shop(sinks: list, factor: float = 0.0) -> DynamicRealtimeEnvironment:
        """The shop, publishing to the sinks given."""
        env = DynamicRealtimeEnvironment(factor=factor)
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress(sinks)
        packers = DynamicResource(env, "Shop", "packers")
        env.process(orders(env, packers, Sampler(seed=42)))
        return env
    ```

### Use the AWS Glue Schema Registry

The same two formats work with an AWS Glue Schema Registry. The Glue classes need `desflow[glue]`; the rest of this page needs only `desflow[confluent]`, so this function builds the Glue serializers only when it is called. They replace the Confluent ones in `topics`. In a blueprint the types are `glue_json_schema` and `glue_protobuf`, with the key `registry_name` for the registry and the key `boto3` for the arguments of `boto3.client`, such as `region_name`. The classes take the same as keyword arguments (`region_name="us-east-1"`). Four rules differ from the Confluent classes:

- `GlueProtobufSerializer` takes one more argument, `schema_str`: the text of the `.proto` file, because Glue stores the text and protobuf cannot print it from the generated class. The text must declare the same messages, in the same file, as `message_type`. In a blueprint it is the key `schema_file`.
- `GlueJsonSchemaSerializer` compiles the schema with fastjsonschema when you build it, so a `$ref` to another document is fetched at that point.
- `GlueJsonSchemaSerializer` fills in the `default` of a property that a record leaves out.
- Glue checks a new version of a schema against the schema's compatibility mode before it is available. Both serializers wait for it: up to 10 checks, 3 seconds apart, which is about 30 seconds. If the version is still pending after that, the serializer raises an error that names the version.

=== "YAML"

    ```yaml title="docs/snippets/yaml/json_schema_and_protobuf_glue.yaml:egress"
    egress:
      - type: Kafka
        config:
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
          topics:
            orders-json:
              serializer:
                type: glue_json_schema
                registry_name: shop
                schema_file: order_record.schema.json
                boto3: {region_name: us-east-1}  # passed to boto3.client
            orders-protobuf:
              serializer:
                type: glue_protobuf
                registry_name: shop
                descriptor_file: shop_record.desc
                message: com.desflow.shop.ShopRecord
                schema_file: shop_record.proto  # the text of the .proto file
                boto3: {region_name: us-east-1}
    ```

=== "Builder"

    ```python title="docs/snippets/guides/json_schema_and_protobuf_serializers.py:glue"
    def glue_serializers():
        """The same two formats for an AWS Glue Schema Registry; they need `desflow[glue]`."""
        from pathlib import Path

        from desflow.connectors.egress.kafka import (
            GlueJsonSchemaSerializer,
            GlueProtobufSerializer,
        )

        glue_json = GlueJsonSchemaSerializer(
            registry_name="shop", schema_str=json_schema, region_name="us-east-1"
        )
        # Glue stores the text of the .proto file, so the serializer takes it as schema_str.
        # It must declare the same messages as message_type.
        glue_protobuf = GlueProtobufSerializer(
            registry_name="shop",
            message_type=ShopRecord,
            schema_str=Path(__file__).with_name("shop_record.proto").read_text(),
            region_name="us-east-1",
        )
        return glue_json, glue_protobuf


    def glue_egress():
        """The egress of the shop, sending the two order topics through the Glue serializers."""
        import os

        from desflow import KafkaEgress

        glue_json, glue_protobuf = glue_serializers()
        return KafkaEgress(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            topics={
                "orders-json": {"serializer": glue_json},
                "orders-protobuf": {"serializer": glue_protobuf},
            },
        )
    ```

    Pass `glue_egress()` to `add_egress` in place of the `egress` above.

=== "Low-level"

    ```python title="docs/snippets/guides/json_schema_and_protobuf_serializers.py:glue"
    def glue_serializers():
        """The same two formats for an AWS Glue Schema Registry; they need `desflow[glue]`."""
        from pathlib import Path

        from desflow.connectors.egress.kafka import (
            GlueJsonSchemaSerializer,
            GlueProtobufSerializer,
        )

        glue_json = GlueJsonSchemaSerializer(
            registry_name="shop", schema_str=json_schema, region_name="us-east-1"
        )
        # Glue stores the text of the .proto file, so the serializer takes it as schema_str.
        # It must declare the same messages as message_type.
        glue_protobuf = GlueProtobufSerializer(
            registry_name="shop",
            message_type=ShopRecord,
            schema_str=Path(__file__).with_name("shop_record.proto").read_text(),
            region_name="us-east-1",
        )
        return glue_json, glue_protobuf


    def glue_egress():
        """The egress of the shop, sending the two order topics through the Glue serializers."""
        import os

        from desflow import KafkaEgress

        glue_json, glue_protobuf = glue_serializers()
        return KafkaEgress(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            topics={
                "orders-json": {"serializer": glue_json},
                "orders-protobuf": {"serializer": glue_protobuf},
            },
        )
    ```

    Pass `glue_egress()` to the shop: `shop([glue_egress()])`.

### Read the Records Back

A consumer reads each order topic with the deserializer of its format. `read_order` takes the topic and the value of one message, from any Kafka consumer, and returns the record that was sent. Each deserializer fetches the schema that a message names from the registry. `ConfluentJsonSchemaDeserializer` checks every record against it, and `ConfluentProtobufDeserializer` decodes the message as `message_type`. For a Glue registry, `GlueJsonSchemaDeserializer("shop", region_name="us-east-1")` and `GlueProtobufDeserializer("shop", ShopRecord, region_name="us-east-1")` take their place: the registry name, for Protobuf the message class, and the arguments of `boto3.client`.

```python title="docs/snippets/guides/json_schema_and_protobuf_serializers.py:read-back"
from desflow.connectors.ingress.kafka import (
    ConfluentJsonSchemaDeserializer,
    ConfluentProtobufDeserializer,
    MessageDeserializer,
)

# A consumer of the order topics reads each one with the deserializer of its format.
order_readers: dict[str, MessageDeserializer] = {
    "orders-json": ConfluentJsonSchemaDeserializer(
        registry_url="http://127.0.0.1:8081"
    ),
    "orders-protobuf": ConfluentProtobufDeserializer(
        registry_url="http://127.0.0.1:8081", message_type=ShopRecord
    ),
}


def read_order(topic: str, payload: bytes) -> dict:
    """Decodes one message of an order topic into the record that was sent."""
    return order_readers[topic].deserialize(topic, payload)
```

---

## Check the Result

Run `desflow run json_schema_and_protobuf.yaml`, `python json_schema_and_protobuf.py` or `python json_schema_and_protobuf_low_level.py` while `kafka-lite` is up, beside the files of the page. Each runs for one simulated minute and ends at once. Each form sends 10 orders to `orders-json` and 10 to `orders-protobuf`, and the other events to `sim-events` as JSON.

To read the first order of each topic back, run the serializers file after a shop has run. It prints what `read_order` decodes:

```python title="docs/snippets/guides/json_schema_and_protobuf_serializers.py:check"
import asyncio

from aiokafka import AIOKafkaConsumer


async def show_first_order(topic: str) -> None:
    """Prints the first order of a topic, decoded by the reader of its format."""
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers="127.0.0.1:9092", auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        message = await asyncio.wait_for(consumer.getone(), timeout=10)
        print(topic, read_order(topic, message.value))
    finally:
        await consumer.stop()


if __name__ == "__main__":
    for topic in order_readers:
        asyncio.run(show_first_order(topic))
```

```bash
python json_schema_and_protobuf_serializers.py
```

```text
orders-json {'sim_ts': 2.1283113312939435, 'timestamp': '2026-11-09T00:00:00.000+00:00', 'key': 'orders-1', 'task': 'pack_order', 'instance': 0, 'value': {'order_id': 0, 'amount': 19.99}}
orders-protobuf {'key': 'orders-2', 'task': 'pack_order', 'instance': '1', 'sim_ts': 6.434338901219139, 'timestamp': '2026-11-09T00:00:00.000+00:00', 'value': {'order_id': '1', 'amount': 19.99}}
```

The `timestamp` is the clock time of your run. A 64 bit integer field of Protobuf, such as `order_id` and `instance`, comes back from a deserializer as a string, so the Protobuf record shows `'1'` for `order_id` and `instance` where the JSON record shows numbers.

---

## Full Files

Every code form uses the serializers in `json_schema_and_protobuf_serializers.py`, which builds no shop, so keep the files in one folder.

??? example "Full files"

    ```python title="docs/snippets/guides/json_schema_and_protobuf_serializers.py"
    """The serializers and order readers of the Use Schema Serializers page.

    The code forms of the shop import the serializers from this file:
    json_schema_and_protobuf.py (builder) and json_schema_and_protobuf_low_level.py. It builds
    no simulation. shop_record.proto and shop_record_pb2.py, generated from it, must be beside
    this file. It needs a Kafka broker and a schema registry: start both with
    `odctl up kafka-lite`.

    After a shop has run, `python json_schema_and_protobuf_serializers.py` reads the first
    order of each order topic back.
    """

    # Each section carries its own imports, as the page shows it.
    # ruff: noqa: E402

    from desflow.connectors.egress.kafka import ConfluentJsonSchemaSerializer

    # The schema describes the whole record KafkaEgress sends, with the order as its value.
    json_schema = """{
      "type": "object",
      "properties": {
        "key": {"type": "string"},
        "task": {"type": ["string", "null"]},
        "instance": {"type": ["integer", "null"]},
        "sim_ts": {"type": "number"},
        "timestamp": {"type": "string"},
        "value": {
          "type": "object",
          "properties": {
            "order_id": {"type": "integer"},
            "amount": {"type": "number"}
          },
          "required": ["order_id", "amount"]
        }
      },
      "required": ["key", "sim_ts", "timestamp", "value"]
    }"""

    json_serializer = ConfluentJsonSchemaSerializer(
        registry_url="http://127.0.0.1:8081", schema_str=json_schema
    )


    from shop_record_pb2 import ShopRecord

    from desflow.connectors.egress.kafka import ConfluentProtobufSerializer

    # message_type is the class that protoc generated. The serializer registers its file.
    protobuf_serializer = ConfluentProtobufSerializer(
        registry_url="http://127.0.0.1:8081", message_type=ShopRecord
    )


    def glue_serializers():
        """The same two formats for an AWS Glue Schema Registry; they need `desflow[glue]`."""
        from pathlib import Path

        from desflow.connectors.egress.kafka import (
            GlueJsonSchemaSerializer,
            GlueProtobufSerializer,
        )

        glue_json = GlueJsonSchemaSerializer(
            registry_name="shop", schema_str=json_schema, region_name="us-east-1"
        )
        # Glue stores the text of the .proto file, so the serializer takes it as schema_str.
        # It must declare the same messages as message_type.
        glue_protobuf = GlueProtobufSerializer(
            registry_name="shop",
            message_type=ShopRecord,
            schema_str=Path(__file__).with_name("shop_record.proto").read_text(),
            region_name="us-east-1",
        )
        return glue_json, glue_protobuf


    def glue_egress():
        """The egress of the shop, sending the two order topics through the Glue serializers."""
        import os

        from desflow import KafkaEgress

        glue_json, glue_protobuf = glue_serializers()
        return KafkaEgress(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            topics={
                "orders-json": {"serializer": glue_json},
                "orders-protobuf": {"serializer": glue_protobuf},
            },
        )


    from desflow.connectors.ingress.kafka import (
        ConfluentJsonSchemaDeserializer,
        ConfluentProtobufDeserializer,
        MessageDeserializer,
    )

    # A consumer of the order topics reads each one with the deserializer of its format.
    order_readers: dict[str, MessageDeserializer] = {
        "orders-json": ConfluentJsonSchemaDeserializer(
            registry_url="http://127.0.0.1:8081"
        ),
        "orders-protobuf": ConfluentProtobufDeserializer(
            registry_url="http://127.0.0.1:8081", message_type=ShopRecord
        ),
    }


    def read_order(topic: str, payload: bytes) -> dict:
        """Decodes one message of an order topic into the record that was sent."""
        return order_readers[topic].deserialize(topic, payload)


    import asyncio

    from aiokafka import AIOKafkaConsumer


    async def show_first_order(topic: str) -> None:
        """Prints the first order of a topic, decoded by the reader of its format."""
        consumer = AIOKafkaConsumer(
            topic, bootstrap_servers="127.0.0.1:9092", auto_offset_reset="earliest"
        )
        await consumer.start()
        try:
            message = await asyncio.wait_for(consumer.getone(), timeout=10)
            print(topic, read_order(topic, message.value))
        finally:
            await consumer.stop()


    if __name__ == "__main__":
        for topic in order_readers:
            asyncio.run(show_first_order(topic))
    ```

    === "YAML"

        ```yaml title="docs/snippets/yaml/json_schema_and_protobuf.yaml"
        # The Use Schema Serializers guide: each packed order publishes an order record, sent
        # to orders-json as JSON Schema or to orders-protobuf as Protobuf; every other record
        # goes to sim-events as JSON. Needs order_record.schema.json and shop_record.desc beside
        # it, and a Kafka broker with a schema registry: odctl up kafka-lite. Run it with:
        # desflow run json_schema_and_protobuf.yaml
        simulation: {sim_id: Shop, factor: 0, random_seed: 42}

        egress:
          - type: Kafka
            config:
              bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}
              topics:
                orders-json:
                  serializer:
                    type: confluent_json_schema
                    registry_url: ${SCHEMA_REGISTRY_URL:-http://127.0.0.1:8081}
                    schema_file: order_record.schema.json
                orders-protobuf:
                  serializer:
                    type: confluent_protobuf
                    registry_url: ${SCHEMA_REGISTRY_URL:-http://127.0.0.1:8081}
                    descriptor_file: shop_record.desc
                    message: com.desflow.shop.ShopRecord

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
            payload:
              # Orders alternate between the two formats, so one run shows both
              __topic__:
                gen: when
                condition: "process.instance % 2 == 1"
                then: orders-protobuf
                else: orders-json
              order_id: {gen: expr, expr: process.instance}
              amount: 19.99

        run: {until: 1 min}
        ```

        ```yaml title="docs/snippets/yaml/json_schema_and_protobuf_glue.yaml"
        # The Use Schema Serializers guide: the shop of json_schema_and_protobuf.yaml with the
        # AWS Glue Schema Registry in place of the Confluent one. Needs
        # order_record.schema.json, shop_record.desc and shop_record.proto beside it, and the
        # package desflow[glue]. Set KAFKA_BOOTSTRAP_SERVERS and give AWS credentials, then run
        # it with: desflow run json_schema_and_protobuf_glue.yaml
        simulation: {sim_id: Shop, factor: 0, random_seed: 42}

        egress:
          - type: Kafka
            config:
              bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
              topics:
                orders-json:
                  serializer:
                    type: glue_json_schema
                    registry_name: shop
                    schema_file: order_record.schema.json
                    boto3: {region_name: us-east-1}  # passed to boto3.client
                orders-protobuf:
                  serializer:
                    type: glue_protobuf
                    registry_name: shop
                    descriptor_file: shop_record.desc
                    message: com.desflow.shop.ShopRecord
                    schema_file: shop_record.proto  # the text of the .proto file
                    boto3: {region_name: us-east-1}

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
            payload:
              # Orders alternate between the two formats, so one run shows both
              __topic__:
                gen: when
                condition: "process.instance % 2 == 1"
                then: orders-protobuf
                else: orders-json
              order_id: {gen: expr, expr: process.instance}
              amount: 19.99

        run: {until: 1 min}
        ```

        ```json title="docs/snippets/yaml/order_record.schema.json"
        {
          "type": "object",
          "properties": {
            "key": {"type": "string"},
            "task": {"type": ["string", "null"]},
            "instance": {"type": ["integer", "null"]},
            "sim_ts": {"type": "number"},
            "timestamp": {"type": "string"},
            "value": {
              "type": "object",
              "properties": {
                "order_id": {"type": "integer"},
                "amount": {"type": "number"}
              },
              "required": ["order_id", "amount"]
            }
          },
          "required": ["key", "sim_ts", "timestamp", "value"]
        }
        ```

    === "Builder"

        ```python title="docs/snippets/guides/json_schema_and_protobuf.py"
        """The shop of the Use Schema Serializers page, built with the builder API.

        The shop publishes each packed order as JSON Schema or Protobuf and every other record as
        JSON. It needs a Kafka broker and a schema registry: start both with `odctl up kafka-lite`,
        then run `python json_schema_and_protobuf.py`. json_schema_and_protobuf.yaml and
        json_schema_and_protobuf_low_level.py are the same shop in YAML and the low-level API.
        The code forms import the serializers of json_schema_and_protobuf_serializers.py, which
        must be beside them.
        """

        # ruff: noqa: E402

        import logging
        import os

        from json_schema_and_protobuf_serializers import json_serializer, protobuf_serializer

        from desflow import KafkaEgress, SimulationContext

        egress = KafkaEgress(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            topics={
                "orders-json": {"serializer": json_serializer},
                "orders-protobuf": {"serializer": protobuf_serializer},
            },
        )

        app = (
            SimulationContext(sim_id="Shop", factor=0, random_seed=42)
            .add_resource("packers", current_cap=1, max_cap=1)
            .add_service("packing", dist="constant", value=1.5)
            .add_arrival("orders", dist="exponential", rate=0.5)
            .add_egress(egress)  # orders as JSON Schema or Protobuf, the rest as JSON
        )


        @app.task(service_id="packing", resource_id="packers")
        def pack_order(task_id: int, entity):
            # Orders alternate between the two formats, so one run shows both
            topic = "orders-protobuf" if task_id % 2 else "orders-json"
            return {"__topic__": topic, "order_id": task_id, "amount": 19.99}


        @app.arrival_loop("orders")
        def orders(context: SimulationContext):
            while True:
                order = yield context.wait_for_arrival("orders")
                context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            app.run(until="1 min")
        ```

    === "Low-level"

        ```python title="docs/snippets/guides/json_schema_and_protobuf_low_level.py"
        """The Use Schema Serializers guide, low-level API: each packed order publishes
        an order record, sent to orders-json as JSON Schema or to orders-protobuf as Protobuf, and
        every other record goes to its topic as JSON.

        The serializers are those of json_schema_and_protobuf_serializers.py, which must be
        beside this script, with shop_record.proto and shop_record_pb2.py. It needs a Kafka
        broker and a schema registry: start both with `odctl up kafka-lite`, then run
        `python json_schema_and_protobuf_low_level.py`.
        """

        import itertools
        import logging
        import os

        from json_schema_and_protobuf_serializers import json_serializer, protobuf_serializer

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            KafkaEgress,
            Sampler,
            SimParameter,
        )

        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
            service={"packing": DistributionConfig(dist="constant", value=1.5)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
        )


        egress = KafkaEgress(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
            topics={
                "orders-json": {"serializer": json_serializer},
                "orders-protobuf": {"serializer": protobuf_serializer},
            },
        )


        def pack_order(env, packers, sampler, key, task_id):
            path_id = "Shop.service.packing"
            env.publish_event(
                key,
                {
                    "path_id": path_id,
                    "sim_id": "Shop",
                    "service": "packing",
                    "status": "queued",
                },
                "pack_order",
                task_id,
            )
            with packers.request() as unit:
                yield unit
                env.publish_event(
                    key,
                    {
                        "path_id": path_id,
                        "sim_id": "Shop",
                        "service": "packing",
                        "status": "started",
                    },
                    "pack_order",
                    task_id,
                )
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                # Orders alternate between the two formats, so one run shows both
                topic = "orders-protobuf" if task_id % 2 else "orders-json"
                order = {"__topic__": topic, "order_id": task_id, "amount": 19.99}
                env.publish_event(key, order, "pack_order", task_id)


        def orders(env, packers, sampler):
            gap = env.registry.get_config("Shop.arrival.orders")
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


        def shop(sinks: list, factor: float = 0.0) -> DynamicRealtimeEnvironment:
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
            env = shop([egress])  # orders as JSON Schema or Protobuf, the rest as JSON
            try:
                env.run(until=60)
            finally:
                env.teardown()
        ```
