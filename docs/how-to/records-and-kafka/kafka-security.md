# Connect to Secure Kafka

Publish a simulation's events to a Kafka cluster that needs SASL, mutual TLS or Amazon MSK IAM, by passing the security settings in the sink's `config`.

The Kafka connectors pass any setting they do not use themselves to the Kafka client underneath, `aiokafka`, so SASL, mTLS, OAuth and AWS IAM work without code of your own. `KafkaAdminConnector` passes its settings to the `aiokafka` client of each method: the admin client for `create_topics`, the producer for `send_config` and the consumer for `collect_data`. Each setting must be one that client takes.

---

## Before You Start

- Install the Kafka connectors: `pip install "desflow[kafka]"`.
- [Egress](../../user-guide/live-runs-and-connectors/egress.md#kafka) and [Ingress](../../user-guide/live-runs-and-connectors/ingress.md#kafka) explain the Kafka connectors without security.
- Every example here connects to a managed or secured cluster. No odctl profile runs a secured broker, so the shop needs a cluster of your own, such as Confluent Cloud for SASL. To try the connectors locally without security, start a broker with `odctl up kafka-lite` and see [Installation](../../installation.md).

---

## Steps

### Connect with SASL

For Confluent Cloud, or any cluster that uses SASL PLAIN or SCRAM, pass `security_protocol`, `sasl_mechanism` and the credentials. The address and the credentials come from variables, so no secret is in the file. With no `topic_router`, `KafkaEgress` creates `sim-events` and `sim-telemetry` when it starts, and passes the security settings to the admin client that creates them.

This simulation publishes its events through a SASL sink:

=== "YAML"

    ```yaml title="docs/snippets/yaml/kafka_security.yaml:shop"
    simulation: {sim_id: Shop, factor: 0, random_seed: 42}

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
        payload: {event_type: order_packed}

    egress:
      - type: Kafka
        config:
          bootstrap_servers:
            ${KAFKA_BOOTSTRAP_SERVERS:-pkc-xxxx.us-east-1.aws.confluent.cloud:9092}
          # Passed as they are to the Kafka client
          security_protocol: SASL_SSL
          sasl_mechanism: PLAIN  # or SCRAM-SHA-512
          sasl_plain_username: ${KAFKA_API_KEY:-<YOUR_API_KEY>}
          sasl_plain_password: ${KAFKA_API_SECRET:-<YOUR_API_SECRET>}

    run: {until: 1 min}
    ```

=== "Builder"

    ```python title="docs/snippets/guides/kafka_security_connections.py:sasl"
    import os
    from typing import Any, Dict

    from desflow import KafkaEgress


    def sasl_connection() -> Dict[str, Any]:
        """The settings of a cluster that uses SASL. The environment holds the credentials."""
        return dict(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            # Passed as they are to the Kafka client
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
            sasl_plain_username=os.environ["KAFKA_API_KEY"],
            sasl_plain_password=os.environ["KAFKA_API_SECRET"],
        )


    def sasl_egress() -> KafkaEgress:
        """The egress of a cluster that uses SASL."""
        return KafkaEgress(**sasl_connection())
    ```

    ```python title="docs/snippets/guides/kafka_security.py:shop"
    def build(egress: KafkaEgress) -> SimulationContext:
        """Builds the shop, with `egress` as its one sink."""
        app = (
            SimulationContext(sim_id="Shop", factor=0, random_seed=42)
            .add_resource("packers", current_cap=1, max_cap=1)
            .add_service("packing", dist="constant", value=1.5)
            .add_arrival("orders", dist="exponential", rate=0.5)
            .add_egress(egress)
        )

        @app.task(service_id="packing", resource_id="packers")
        def pack_order(task_id: int):
            return {"event_type": "order_packed"}

        @app.arrival_loop("orders")
        def orders(context: SimulationContext):
            while True:
                order = yield context.wait_for_arrival("orders")
                context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

        return app


    if __name__ == "__main__":
        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
        )
        build(selected_egress()).run(until="1 min")
    ```

    ```python title="docs/snippets/guides/kafka_security.py:sasl"
    def sasl_app() -> SimulationContext:
        """The shop with the egress of a cluster that uses SASL."""
        return build(sasl_egress())
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/kafka_security_low_level.py:shop"
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
            env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)


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
        env = shop([selected_egress()])  # KAFKA_CONNECTION picks the cluster
        try:
            env.run(until=60)
        finally:
            env.teardown()
    ```

### Connect with mTLS

For a cluster that requires mutual TLS, `ssl` takes the certificate files: `cafile`, `certfile`, `keyfile` and, for an encrypted key, `password`. The sink builds the SSL context from them with `aiokafka.helpers.create_ssl_context`, so one setting serves the sink and the other Kafka connectors. Set `security_protocol` to `SSL` as well. The Builder and Low-level forms run the same shop as the SASL step with this egress: `build(mtls_egress())` and `shop([mtls_egress()])`.

=== "YAML"

    ```yaml title="docs/snippets/yaml/kafka_security_mtls.yaml:egress"
    egress:
      - type: Kafka
        config:
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
          security_protocol: SSL
          # The arguments of aiokafka.helpers.create_ssl_context
          ssl:
            cafile: ${KAFKA_CA_FILE}
            certfile: ${KAFKA_CERT_FILE}
            keyfile: ${KAFKA_KEY_FILE}
    ```

=== "Builder"

    ```python title="docs/snippets/guides/kafka_security_connections.py:mtls"
    import os


    def mtls_egress() -> KafkaEgress:
        """The egress of a cluster that requires mutual TLS."""
        return KafkaEgress(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            security_protocol="SSL",
            # The arguments of aiokafka.helpers.create_ssl_context
            ssl={
                "cafile": os.environ["KAFKA_CA_FILE"],
                "certfile": os.environ["KAFKA_CERT_FILE"],
                "keyfile": os.environ["KAFKA_KEY_FILE"],
            },
        )
    ```

    ```python title="docs/snippets/guides/kafka_security.py:mtls"
    def mtls_app() -> SimulationContext:
        """The shop with the egress of a cluster that requires mutual TLS."""
        return build(mtls_egress())
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/kafka_security_low_level.py:mtls"
    def mtls_shop() -> DynamicRealtimeEnvironment:
        """The shop with the egress of a cluster that requires mutual TLS."""
        return shop([mtls_egress()])
    ```

### Connect to AWS MSK

Amazon MSK with IAM access control uses `sasl_mechanism` `OAUTHBEARER` with a token provider. `sasl_oauth_token_provider: {type: aws_msk_iam, region: <region>}` builds one that signs a token with the AWS credentials of your environment, or the EC2 or EKS role. It needs the package `aws-msk-iam-sasl-signer-python`, which you install yourself. Set `security_protocol` to `SASL_SSL` as well. For Okta, Auth0 or another single sign-on provider, pass your own subclass of `aiokafka.abc.AbstractTokenProvider` with an `async` `token()` in the Builder and Low-level forms. The Builder and Low-level forms run the same shop as the SASL step with this egress: `build(msk_egress())` and `shop([msk_egress()])`.

=== "YAML"

    ```yaml title="docs/snippets/yaml/kafka_security_msk.yaml:egress"
    egress:
      - type: Kafka
        config:
          bootstrap_servers: b-1.my-msk.amazonaws.com:9098
          security_protocol: SASL_SSL
          sasl_mechanism: OAUTHBEARER
          # The token comes from the AWS credentials of your environment or role
          sasl_oauth_token_provider: {type: aws_msk_iam, region: us-east-1}
    ```

=== "Builder"

    ```python title="docs/snippets/guides/kafka_security_connections.py:msk"
    def msk_egress() -> KafkaEgress:
        """The egress of an Amazon MSK cluster with IAM access control."""
        return KafkaEgress(
            bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            # The token comes from the AWS credentials of your environment or role
            sasl_oauth_token_provider={"type": "aws_msk_iam", "region": "us-east-1"},
        )
    ```

    ```python title="docs/snippets/guides/kafka_security.py:msk"
    def msk_app() -> SimulationContext:
        """The shop with the egress of an Amazon MSK cluster."""
        return build(msk_egress())
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/kafka_security_low_level.py:msk"
    def msk_shop() -> DynamicRealtimeEnvironment:
        """The shop with the egress of an Amazon MSK cluster."""
        return shop([msk_egress()])
    ```

### Connect to a Secured Schema Registry

A schema registry that needs a user name, a certificate or a proxy takes its own settings, apart from the Kafka ones. The Confluent serializers (`confluent_avro`, `confluent_json_schema` and `confluent_protobuf`) and the Confluent deserializers take these arguments:

- `auth`: a user name and a password.
- `verify`: `true`, `false`, the path of a CA file or directory, or in code an `ssl.SSLContext`. A YAML blueprint cannot write a context.
- `cert`: a client certificate file, or a list of the certificate file, the key file and the key password.
- `headers`: extra headers, such as a bearer token.
- `proxy`: the URL of a proxy.
- `timeout`: the seconds each step of a request (connecting, sending, reading) may take. The default is 10. `null` waits without a limit.
- `max_retries`: how many times a request is repeated after a timeout or another transport error (3 by default).

This shop sends its predictions to `ml-predictions` as Avro, as [Use Avro and Pydantic](avro-and-pydantic.md) does, through a registry with all of these settings. The Builder and Low-level forms need `avro_and_pydantic_models.py` of that page beside them.

=== "YAML"

    ```yaml title="docs/snippets/yaml/kafka_security_registry.yaml:egress"
    egress:
      - type: Kafka
        config:
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
          topics:
            ml-predictions:
              serializer:
                type: confluent_avro
                registry_url: ${SCHEMA_REGISTRY_URL}
                schema_file: prediction_event.avsc
                auth:  # a user name and a password
                  - ${REGISTRY_USER}
                  - ${REGISTRY_PASSWORD}
                verify: ${REGISTRY_CA_FILE}  # true, false or a CA file or directory
                cert:  # a client certificate file and its key file
                  - ${REGISTRY_CERT_FILE}
                  - ${REGISTRY_KEY_FILE}
                headers: {X-Team: shop}  # added to every request
                proxy: http://proxy.example.com:3128
                timeout: 30  # seconds for each step of a request
                max_retries: 5
    ```

=== "Builder"

    ```python title="docs/snippets/guides/kafka_security_connections.py:registry"
    import os
    import ssl

    from avro_and_pydantic_models import PredictionRecord

    from desflow import KafkaEgress
    from desflow.connectors.egress.kafka import ConfluentAvroSerializer


    def registry_egress() -> KafkaEgress:
        """The egress of the Avro shop, with a serializer for a secured registry."""
        serializer = ConfluentAvroSerializer(
            registry_url=os.environ["SCHEMA_REGISTRY_URL"],
            schema_str=PredictionRecord.avro_schema(),
            auth=(os.environ["REGISTRY_USER"], os.environ["REGISTRY_PASSWORD"]),
            # True, False, a CA file or directory, or an ssl.SSLContext
            verify=ssl.create_default_context(cafile=os.environ["REGISTRY_CA_FILE"]),
            # A client certificate file and its key file
            cert=(os.environ["REGISTRY_CERT_FILE"], os.environ["REGISTRY_KEY_FILE"]),
            headers={"X-Team": "shop"},  # added to every request
            proxy="http://proxy.example.com:3128",
            timeout=30,  # seconds for each step of a request
            max_retries=5,
        )
        return KafkaEgress(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            topics={"ml-predictions": {"serializer": serializer}},
        )
    ```

    ```python title="docs/snippets/guides/kafka_security_registry.py:shop"
    def build(egress: KafkaEgress) -> SimulationContext:
        """Builds the shop of Use Avro and Pydantic, with `egress` as its one sink."""
        app = (
            SimulationContext(sim_id="Shop", factor=0, random_seed=42)
            .add_resource("packers", current_cap=1, max_cap=1)
            .add_service("packing", dist="constant", value=1.5)
            .add_arrival("orders", dist="exponential", rate=0.5)
            .add_egress(egress)
        )

        @app.task(service_id="packing", resource_id="packers")
        def pack_order(task_id: int, entity):
            prediction = MLPrediction(task_id=entity.key, confidence=0.9)
            return {**prediction.model_dump(), "__topic__": "ml-predictions"}

        @app.arrival_loop("orders")
        def orders(context: SimulationContext):
            while True:
                order = yield context.wait_for_arrival("orders")
                context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build(registry_egress()).run(until="1 min")
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/kafka_security_connections.py:registry"
    import os
    import ssl

    from avro_and_pydantic_models import PredictionRecord

    from desflow import KafkaEgress
    from desflow.connectors.egress.kafka import ConfluentAvroSerializer


    def registry_egress() -> KafkaEgress:
        """The egress of the Avro shop, with a serializer for a secured registry."""
        serializer = ConfluentAvroSerializer(
            registry_url=os.environ["SCHEMA_REGISTRY_URL"],
            schema_str=PredictionRecord.avro_schema(),
            auth=(os.environ["REGISTRY_USER"], os.environ["REGISTRY_PASSWORD"]),
            # True, False, a CA file or directory, or an ssl.SSLContext
            verify=ssl.create_default_context(cafile=os.environ["REGISTRY_CA_FILE"]),
            # A client certificate file and its key file
            cert=(os.environ["REGISTRY_CERT_FILE"], os.environ["REGISTRY_KEY_FILE"]),
            headers={"X-Team": "shop"},  # added to every request
            proxy="http://proxy.example.com:3128",
            timeout=30,  # seconds for each step of a request
            max_retries=5,
        )
        return KafkaEgress(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            topics={"ml-predictions": {"serializer": serializer}},
        )
    ```

    ```python title="docs/snippets/guides/kafka_security_registry_low_level.py:shop"
    if __name__ == "__main__":
        logging.basicConfig(
            level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
        )
        env = shop([registry_egress()])  # the shop of Use Avro and Pydantic
        try:
            env.run(until=60)
        finally:
            env.teardown()
    ```

A request to the registry that times out is retried up to `max_retries` times with a short random wait. With the defaults, `timeout: 10` and `max_retries: 3`, a request takes up to about 50 seconds for each registry URL. A larger `timeout` or `max_retries` makes it longer, and `timeout: null` sets no limit. The Kafka sink and the Kafka source wait for the registry in a worker thread, so the other sinks and sources of the run go on. When the registry fails for a time (a 5xx answer, a timeout or a lost connection) or throttles the call (a 408 or 429 answer, or the Glue error `ThrottlingException`), the Kafka sink keeps the batch and encodes it again after a wait that grows to one minute, with its connection to Kafka open. When the registry refuses the credentials or the access (a 401 or 403 answer, or the Glue error `AccessDeniedException`, or a Glue client that finds no credentials), it would refuse every record, so the Kafka sink or the Kafka source stops. The run logs a line at ERROR that starts with `Egress KafkaEgress stopped:` or `Ingress KafkaIngress stopped:` and gives the registry's error. When the registry refuses one record (any other 4xx answer, or a value the schema cannot encode), the sink skips that record with a warning that names its key and sends the rest of the batch. The Kafka source logs the error and skips that message, and the next message asks the registry again. The simulation goes on while each sink's queue has room. Once a queue stays full, the run stops with an error, as [Egress](../../user-guide/live-runs-and-connectors/egress.md#how-it-works) describes.

The JSON Schema and Protobuf serializers of [Use Schema Serializers](json-schema-and-protobuf.md) take the same arguments. The AWS Glue serializers do not: they take `registry_name` and `boto3`.

---

## Check the Result

Set `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_API_KEY` and `KAFKA_API_SECRET`, then run `desflow run kafka_security.yaml`, `python kafka_security.py` or `python kafka_security_low_level.py`. Each runs for one simulated minute, ends at once and publishes the same events to `sim-events`.

The mTLS and MSK steps run the same shop. Run `desflow run kafka_security_mtls.yaml` or `desflow run kafka_security_msk.yaml`, or set `KAFKA_CONNECTION=mtls` or `KAFKA_CONNECTION=msk` (with the variables of the mTLS step, `KAFKA_CA_FILE`, `KAFKA_CERT_FILE` and `KAFKA_KEY_FILE`) before `python kafka_security.py` or `python kafka_security_low_level.py`. The secured registry runs with `desflow run kafka_security_registry.yaml`, `python kafka_security_registry.py` or `python kafka_security_registry_low_level.py`.

The Kafka ingress and `KafkaAdminConnector` take the same security settings in their `config`: see [Ingress](../../user-guide/live-runs-and-connectors/ingress.md#kafka).

To read the events back, run the connections file with the SASL variables. Its last lines build a consumer from the same settings and print the first events of `sim-events`:

```python title="docs/snippets/guides/kafka_security_connections.py:check"
import asyncio

from aiokafka import AIOKafkaConsumer


async def show_events(count: int = 3) -> None:
    """Prints the first events of sim-events, read with the settings of the SASL egress."""
    consumer = AIOKafkaConsumer(
        "sim-events", **sasl_connection(), auto_offset_reset="earliest"
    )
    await consumer.start()
    try:
        for _ in range(count):
            message = await asyncio.wait_for(consumer.getone(), timeout=10)
            print(message.value.decode())
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(show_events())
```

```bash
python kafka_security_connections.py
```

```text
{"sim_ts":0.6283113312939436,"timestamp":"2026-11-09T00:00:00.628+00:00","key":"orders-1","task":"pack_order","instance":0,"value":{"path_id":"Shop.service.packing","sim_id":"Shop","service":"packing","status":"queued"}}
{"sim_ts":0.6283113312939436,"timestamp":"2026-11-09T00:00:00.628+00:00","key":"orders-1","task":"pack_order","instance":0,"value":{"path_id":"Shop.service.packing","sim_id":"Shop","service":"packing","status":"started"}}
{"sim_ts":2.1283113312939435,"timestamp":"2026-11-09T00:00:02.128+00:00","key":"orders-1","task":"pack_order","instance":0,"value":{"event_type":"order_packed"}}
```

Each line is one event as JSON. The `timestamp` is the clock time of your run.

---

## Full Files

The Builder and Low-level scripts import the egresses of `kafka_security_connections.py`, which builds no shop, so keep the files in one folder.

??? example "Full files"

    ```python title="docs/snippets/guides/kafka_security_connections.py"
    """The secured Kafka connections of the Connect to Secure Kafka page.

    The Builder and Low-level forms of the shop import their egress from this file:
    kafka_security.py (builder) and kafka_security_low_level.py use it directly, and the YAML
    files set the same keys in the `config` of their sink. It builds no simulation. Each
    function builds the egress of one kind of cluster, and `selected_egress()` picks one by the
    variable KAFKA_CONNECTION: sasl (the default), mtls or msk. The SASL egress reads
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET, the mTLS egress reads
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_CA_FILE, KAFKA_CERT_FILE and KAFKA_KEY_FILE, and the MSK
    egress calls AWS when the sink starts.

    `registry_egress()` needs avro_and_pydantic_models.py of Use Avro and Pydantic beside this
    file, and the registry variables of kafka_security_registry.yaml.

    After a shop has run with the SASL egress, `python kafka_security_connections.py` prints
    the first events of sim-events.
    """

    # Each section carries its own imports, as the page shows it.
    # ruff: noqa: E402, F811

    import os
    from typing import Any, Dict

    from desflow import KafkaEgress


    def sasl_connection() -> Dict[str, Any]:
        """The settings of a cluster that uses SASL. The environment holds the credentials."""
        return dict(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            # Passed as they are to the Kafka client
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
            sasl_plain_username=os.environ["KAFKA_API_KEY"],
            sasl_plain_password=os.environ["KAFKA_API_SECRET"],
        )


    def sasl_egress() -> KafkaEgress:
        """The egress of a cluster that uses SASL."""
        return KafkaEgress(**sasl_connection())


    import os


    def mtls_egress() -> KafkaEgress:
        """The egress of a cluster that requires mutual TLS."""
        return KafkaEgress(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            security_protocol="SSL",
            # The arguments of aiokafka.helpers.create_ssl_context
            ssl={
                "cafile": os.environ["KAFKA_CA_FILE"],
                "certfile": os.environ["KAFKA_CERT_FILE"],
                "keyfile": os.environ["KAFKA_KEY_FILE"],
            },
        )


    def msk_egress() -> KafkaEgress:
        """The egress of an Amazon MSK cluster with IAM access control."""
        return KafkaEgress(
            bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            # The token comes from the AWS credentials of your environment or role
            sasl_oauth_token_provider={"type": "aws_msk_iam", "region": "us-east-1"},
        )


    import os
    import ssl

    from avro_and_pydantic_models import PredictionRecord

    from desflow import KafkaEgress
    from desflow.connectors.egress.kafka import ConfluentAvroSerializer


    def registry_egress() -> KafkaEgress:
        """The egress of the Avro shop, with a serializer for a secured registry."""
        serializer = ConfluentAvroSerializer(
            registry_url=os.environ["SCHEMA_REGISTRY_URL"],
            schema_str=PredictionRecord.avro_schema(),
            auth=(os.environ["REGISTRY_USER"], os.environ["REGISTRY_PASSWORD"]),
            # True, False, a CA file or directory, or an ssl.SSLContext
            verify=ssl.create_default_context(cafile=os.environ["REGISTRY_CA_FILE"]),
            # A client certificate file and its key file
            cert=(os.environ["REGISTRY_CERT_FILE"], os.environ["REGISTRY_KEY_FILE"]),
            headers={"X-Team": "shop"},  # added to every request
            proxy="http://proxy.example.com:3128",
            timeout=30,  # seconds for each step of a request
            max_retries=5,
        )
        return KafkaEgress(
            bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            topics={"ml-predictions": {"serializer": serializer}},
        )


    import os


    def selected_egress() -> KafkaEgress:
        """The egress that KAFKA_CONNECTION names: sasl (the default), mtls or msk."""
        makers = {"sasl": sasl_egress, "mtls": mtls_egress, "msk": msk_egress}
        return makers[os.environ.get("KAFKA_CONNECTION", "sasl")]()


    import asyncio

    from aiokafka import AIOKafkaConsumer


    async def show_events(count: int = 3) -> None:
        """Prints the first events of sim-events, read with the settings of the SASL egress."""
        consumer = AIOKafkaConsumer(
            "sim-events", **sasl_connection(), auto_offset_reset="earliest"
        )
        await consumer.start()
        try:
            for _ in range(count):
                message = await asyncio.wait_for(consumer.getone(), timeout=10)
                print(message.value.decode())
        finally:
            await consumer.stop()


    if __name__ == "__main__":
        asyncio.run(show_events())
    ```

    === "YAML"

        ```yaml title="docs/snippets/yaml/kafka_security.yaml"
        # The Connect to Secure Kafka guide: a shop that publishes its events to a cluster
        # that uses SASL PLAIN, such as Confluent Cloud. Set KAFKA_BOOTSTRAP_SERVERS,
        # KAFKA_API_KEY and KAFKA_API_SECRET, then run it with:
        # desflow run kafka_security.yaml
        simulation: {sim_id: Shop, factor: 0, random_seed: 42}

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
            payload: {event_type: order_packed}

        egress:
          - type: Kafka
            config:
              bootstrap_servers:
                ${KAFKA_BOOTSTRAP_SERVERS:-pkc-xxxx.us-east-1.aws.confluent.cloud:9092}
              # Passed as they are to the Kafka client
              security_protocol: SASL_SSL
              sasl_mechanism: PLAIN  # or SCRAM-SHA-512
              sasl_plain_username: ${KAFKA_API_KEY:-<YOUR_API_KEY>}
              sasl_plain_password: ${KAFKA_API_SECRET:-<YOUR_API_SECRET>}

        run: {until: 1 min}
        ```

        ```yaml title="docs/snippets/yaml/kafka_security_mtls.yaml"
        # The Connect to Secure Kafka guide: a shop that publishes its events to a cluster that
        # requires mutual TLS. Set KAFKA_BOOTSTRAP_SERVERS, KAFKA_CA_FILE, KAFKA_CERT_FILE and
        # KAFKA_KEY_FILE, then run it with: desflow run kafka_security_mtls.yaml
        simulation: {sim_id: Shop, factor: 0, random_seed: 42}

        egress:
          - type: Kafka
            config:
              bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
              security_protocol: SSL
              # The arguments of aiokafka.helpers.create_ssl_context
              ssl:
                cafile: ${KAFKA_CA_FILE}
                certfile: ${KAFKA_CERT_FILE}
                keyfile: ${KAFKA_KEY_FILE}

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
            payload: {event_type: order_packed}

        run: {until: 1 min}
        ```

        ```yaml title="docs/snippets/yaml/kafka_security_msk.yaml"
        # The Connect to Secure Kafka guide: a shop that publishes its events to Amazon MSK with
        # IAM access control. Needs the package aws-msk-iam-sasl-signer-python and AWS
        # credentials in the environment. Run it with: desflow run kafka_security_msk.yaml
        simulation: {sim_id: Shop, factor: 0, random_seed: 42}

        egress:
          - type: Kafka
            config:
              bootstrap_servers: b-1.my-msk.amazonaws.com:9098
              security_protocol: SASL_SSL
              sasl_mechanism: OAUTHBEARER
              # The token comes from the AWS credentials of your environment or role
              sasl_oauth_token_provider: {type: aws_msk_iam, region: us-east-1}

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
            payload: {event_type: order_packed}

        run: {until: 1 min}
        ```

        ```yaml title="docs/snippets/yaml/kafka_security_registry.yaml"
        # The Connect to Secure Kafka guide: the shop of Use Avro and Pydantic, with its
        # predictions serialized through a schema registry that needs a user name, a client
        # certificate and more. Needs prediction_event.avsc from Use Avro and Pydantic beside
        # it. Set KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, REGISTRY_USER,
        # REGISTRY_PASSWORD, REGISTRY_CA_FILE, REGISTRY_CERT_FILE and REGISTRY_KEY_FILE, then
        # run it with: desflow run kafka_security_registry.yaml
        simulation: {sim_id: Shop, factor: 0, random_seed: 42}

        egress:
          - type: Kafka
            config:
              bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
              topics:
                ml-predictions:
                  serializer:
                    type: confluent_avro
                    registry_url: ${SCHEMA_REGISTRY_URL}
                    schema_file: prediction_event.avsc
                    auth:  # a user name and a password
                      - ${REGISTRY_USER}
                      - ${REGISTRY_PASSWORD}
                    verify: ${REGISTRY_CA_FILE}  # true, false or a CA file or directory
                    cert:  # a client certificate file and its key file
                      - ${REGISTRY_CERT_FILE}
                      - ${REGISTRY_KEY_FILE}
                    headers: {X-Team: shop}  # added to every request
                    proxy: http://proxy.example.com:3128
                    timeout: 30  # seconds for each step of a request
                    max_retries: 5

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
            # The finished event is a prediction for the order: the fields of MLPrediction
            payload:
              __topic__: ml-predictions
              event_type: prediction
              task_id: {gen: entity}
              confidence: 0.9

        run: {until: 1 min}
        ```

    === "Builder"

        ```python title="docs/snippets/guides/kafka_security.py"
        """The shop of the Connect to Secure Kafka page, built with the builder API.

        `build(egress)` gives the shop with the egress it is given. Run `python kafka_security.py`
        to publish to a cluster that uses SASL PLAIN, such as Confluent Cloud: set
        KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET. Set KAFKA_CONNECTION to mtls
        or msk to publish to a cluster of that kind instead, as kafka_security_connections.py sets
        out. That file, which holds the egresses, must be beside this one.
        """

        # ruff: noqa: E402

        import logging

        from kafka_security_connections import (
            msk_egress,
            mtls_egress,
            sasl_egress,
            selected_egress,
        )

        from desflow import KafkaEgress, SimulationContext


        def build(egress: KafkaEgress) -> SimulationContext:
            """Builds the shop, with `egress` as its one sink."""
            app = (
                SimulationContext(sim_id="Shop", factor=0, random_seed=42)
                .add_resource("packers", current_cap=1, max_cap=1)
                .add_service("packing", dist="constant", value=1.5)
                .add_arrival("orders", dist="exponential", rate=0.5)
                .add_egress(egress)
            )

            @app.task(service_id="packing", resource_id="packers")
            def pack_order(task_id: int):
                return {"event_type": "order_packed"}

            @app.arrival_loop("orders")
            def orders(context: SimulationContext):
                while True:
                    order = yield context.wait_for_arrival("orders")
                    context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

            return app


        if __name__ == "__main__":
            logging.basicConfig(
                level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
            )
            build(selected_egress()).run(until="1 min")


        def sasl_app() -> SimulationContext:
            """The shop with the egress of a cluster that uses SASL."""
            return build(sasl_egress())


        def mtls_app() -> SimulationContext:
            """The shop with the egress of a cluster that requires mutual TLS."""
            return build(mtls_egress())


        def msk_app() -> SimulationContext:
            """The shop with the egress of an Amazon MSK cluster."""
            return build(msk_egress())


        ```

        ```python title="docs/snippets/guides/kafka_security_registry.py"
        """The shop of Use Avro and Pydantic, built with the builder API, with its predictions
        serialized through a secured schema registry.

        The egress is `registry_egress()` of kafka_security_connections.py, which needs
        avro_and_pydantic_models.py of Use Avro and Pydantic beside it. Set the variables of
        kafka_security_registry.yaml, then run `python kafka_security_registry.py`. It runs for
        one minute.
        """

        import logging

        from avro_and_pydantic_models import MLPrediction
        from kafka_security_connections import registry_egress

        from desflow import KafkaEgress, SimulationContext


        def build(egress: KafkaEgress) -> SimulationContext:
            """Builds the shop of Use Avro and Pydantic, with `egress` as its one sink."""
            app = (
                SimulationContext(sim_id="Shop", factor=0, random_seed=42)
                .add_resource("packers", current_cap=1, max_cap=1)
                .add_service("packing", dist="constant", value=1.5)
                .add_arrival("orders", dist="exponential", rate=0.5)
                .add_egress(egress)
            )

            @app.task(service_id="packing", resource_id="packers")
            def pack_order(task_id: int, entity):
                prediction = MLPrediction(task_id=entity.key, confidence=0.9)
                return {**prediction.model_dump(), "__topic__": "ml-predictions"}

            @app.arrival_loop("orders")
            def orders(context: SimulationContext):
                while True:
                    order = yield context.wait_for_arrival("orders")
                    context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

            return app


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            build(registry_egress()).run(until="1 min")
        ```

    === "Low-level"

        ```python title="docs/snippets/guides/kafka_security_low_level.py"
        """The Connect to Secure Kafka guide, low-level API: a shop that publishes its events
        through the egress of kafka_security_connections.py, which must be beside this script.

        Set KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET, then run
        `python kafka_security_low_level.py`. It runs for one minute and publishes to a cluster
        that uses SASL. Set KAFKA_CONNECTION to mtls or msk to publish to a cluster of that kind
        instead, as kafka_security_connections.py sets out.
        """

        import itertools
        import logging

        from kafka_security_connections import msk_egress, mtls_egress, selected_egress

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
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
                env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)


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
            env = shop([selected_egress()])  # KAFKA_CONNECTION picks the cluster
            try:
                env.run(until=60)
            finally:
                env.teardown()


        def mtls_shop() -> DynamicRealtimeEnvironment:
            """The shop with the egress of a cluster that requires mutual TLS."""
            return shop([mtls_egress()])


        def msk_shop() -> DynamicRealtimeEnvironment:
            """The shop with the egress of an Amazon MSK cluster."""
            return shop([msk_egress()])


        ```

        ```python title="docs/snippets/guides/kafka_security_registry_low_level.py"
        """The shop of Use Avro and Pydantic, low-level API, with its predictions serialized
        through a secured schema registry.

        The egress is `registry_egress()` of kafka_security_connections.py, and the shop is that of
        avro_and_pydantic_low_level.py. Both files, and avro_and_pydantic_models.py, must be beside
        this script. Set the variables of kafka_security_registry.yaml, then run
        `python kafka_security_registry_low_level.py`. It runs for one minute.
        """

        import logging

        from avro_and_pydantic_low_level import shop
        from kafka_security_connections import registry_egress

        if __name__ == "__main__":
            logging.basicConfig(
                level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
            )
            env = shop([registry_egress()])  # the shop of Use Avro and Pydantic
            try:
                env.run(until=60)
            finally:
                env.teardown()
        ```
