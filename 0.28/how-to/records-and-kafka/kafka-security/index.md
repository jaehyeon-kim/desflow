# Connect to Secure Kafka

The Kafka connectors pass any keyword argument they do not use themselves to the Kafka client underneath, so SASL, mTLS, OAuth and AWS IAM work without code of your own. The connectors use `aiokafka`. `KafkaAdminConnector` passes its settings to the `aiokafka` client of each method: the admin client for `create_topics`, the producer for `send_config` and the consumer for `collect_data`. Each setting must be one that client takes.

______________________________________________________________________

## Before You Start

- Install the Kafka connectors: `pip install "desflow[kafka]"`.
- [Egress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#kafka) and [Ingress](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/ingress/#kafka) explain the Kafka connectors without security.
- Every example here connects to a managed or secured cluster. No odctl profile runs a secured broker, so the shop at the end needs a cluster of your own that uses SASL PLAIN, such as Confluent Cloud. To try the connectors locally without security, start a broker with `odctl up kafka-lite` and see [Installation](https://jaehyeon.me/desflow/latest/installation/index.md).

______________________________________________________________________

## Steps

### Confluent Cloud (SASL PLAIN)

The connector setups below are the same for the builder and the low-level API. A blueprint gives the same keys in the connector's `config`. It names an object such as an SSL context with `!python`, as [Call Python from YAML](https://jaehyeon.me/desflow/latest/how-to/tools/yaml-advanced/#routers-serializers-and-connector-classes) shows for a serializer. For Confluent Cloud, or any cluster that uses SASL PLAIN or SCRAM, pass `security_protocol`, `sasl_mechanism` and the credentials:

```python
import os
from typing import Any, Dict

from desflow import KafkaEgress

# The address and the credentials come from the environment, so no secret is in the file
connection: Dict[str, Any] = dict(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    # Passed as they are to the Kafka client
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
    sasl_plain_username=os.environ["KAFKA_API_KEY"],
    sasl_plain_password=os.environ["KAFKA_API_SECRET"],
)
egress = KafkaEgress(**connection)
```

With no `topic_router`, `KafkaEgress` creates `sim-events` and `sim-telemetry` when it starts, and passes the security settings to the admin client that creates them.

### On-Premise Cluster (mTLS)

For a cluster that requires mutual TLS, build an SSL context from your certificate files. `aiokafka` takes only `ssl_context`, not the certificate paths, so one context serves every connector.

```python
from aiokafka.helpers import create_ssl_context

from desflow import KafkaIngress

ssl_context = create_ssl_context(
    cafile="/path/to/ca.pem",
    certfile="/path/to/service.cert",
    keyfile="/path/to/service.key",
)

ingress = KafkaIngress(
    topic="sim-commands",
    bootstrap_servers="secure-broker.internal.company.com:9093",
    security_protocol="SSL",
    ssl_context=ssl_context,
)
```

### AWS MSK (IAM)

Amazon MSK with IAM access control uses `sasl_mechanism="OAUTHBEARER"` with a token provider. The token comes from the `aws-msk-iam-sasl-signer-python` package, which you install yourself.

`aiokafka` requires a subclass of `aiokafka.abc.AbstractTokenProvider` with an `async` `token()`. One provider serves every connector.

```python
from aiokafka.abc import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from desflow import KafkaAdminConnector, KafkaEgress

class MSKTokenProvider(AbstractTokenProvider):
    async def token(self):
        # Uses the AWS credentials of your environment, or the EC2 or EKS role
        token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
        return token

msk = dict(
    bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
    security_protocol="SASL_SSL",
    sasl_mechanism="OAUTHBEARER",
)

# KafkaIngress, KafkaEgress and KafkaAdminConnector all use aiokafka
egress = KafkaEgress(**msk, sasl_oauth_token_provider=MSKTokenProvider())

admin = KafkaAdminConnector(**msk, sasl_oauth_token_provider=MSKTokenProvider())
admin.create_topics([{"name": "sim-events", "partitions": 3}])
```

The same `OAUTHBEARER` setting works with a token provider of your own for Okta, Auth0 or another single sign-on provider.

### Use It in a Simulation

A connector with security settings is added like any other. This simulation publishes its events through the SASL egress above. The blueprint sets the same keys in its `config`:

```yaml
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
    payload: {event_type: order_packed}

egress:
  - type: Kafka
    config:
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-pkc-xxxx.us-east-1.aws.confluent.cloud:9092}
      # Passed as they are to the Kafka client
      security_protocol: SASL_SSL
      sasl_mechanism: PLAIN  # or SCRAM-SHA-512
      sasl_plain_username: ${KAFKA_API_KEY:-<YOUR_API_KEY>}
      sasl_plain_password: ${KAFKA_API_SECRET:-<YOUR_API_SECRET>}

run:
  until: 1 min
```

The credentials come from the variables `KAFKA_API_KEY` and `KAFKA_API_SECRET`, so they stay out of the file.

```python
app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(egress)  # the SASL egress above
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 min")
```

```python
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
        env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)


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
    env = shop([egress])  # the SASL egress of kafka_security_connections.py
    try:
        env.run(until=60)
    finally:
        env.teardown()
```

______________________________________________________________________

## Check the Result

Set `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_API_KEY` and `KAFKA_API_SECRET`, then run `python kafka_security.py` or `python kafka_security_low_level.py`. For the blueprint, run `desflow run kafka_security.yaml` with the same variables. Each runs for one minute and publishes the same events to `sim-events`.

To read them back, run the connections file with the same variables. Its last lines build a consumer from the same `connection` settings and print the first three events of `sim-events`:

```python
import asyncio

from aiokafka import AIOKafkaConsumer


async def show_events(count: int = 3) -> None:
    """Prints the first events of sim-events, read with the settings of the egress."""
    consumer = AIOKafkaConsumer(
        "sim-events", **connection, auto_offset_reset="earliest"
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
{"sim_ts":2.552,"timestamp":"2026-10-31T00:51:55.440+00:00","key":"orders-1","task":"pack_order","instance":0,"value":{"path_id":"Shop.service.packing","status":"queued"}}
{"sim_ts":2.552,"timestamp":"2026-10-31T00:51:55.440+00:00","key":"orders-1","task":"pack_order","instance":0,"value":{"path_id":"Shop.service.packing","status":"started"}}
{"sim_ts":2.696,"timestamp":"2026-10-31T00:51:55.584+00:00","key":"orders-2","task":"pack_order","instance":1,"value":{"path_id":"Shop.service.packing","status":"queued"}}
```

Each line is one event as JSON. The `timestamp` is the clock time of your run. No odctl profile runs a secured cluster, so this output comes from the same code against a local `kafka-lite` broker with the security settings left out. It was not run against Confluent Cloud.

______________________________________________________________________

## Full Files

The Builder and Low-level scripts import the egress of `kafka_security_connections.py`, which builds no shop, so keep the files in one folder.

Full files

```python
"""The secured Kafka connections of the Connect to Secure Kafka page.

The Builder and Low-level forms of the shop import the SASL egress from this file:
kafka_security.py (builder) and kafka_security_low_level.py use it directly, and
kafka_security.yaml sets the same keys in its `config`. It builds no simulation. Set
KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET before importing it. The mTLS
and MSK examples read certificate files or call AWS, so they sit in functions this file
defines and does not call.

After a shop has run, `python kafka_security_connections.py` prints the first events of
sim-events.
"""

# Each section carries its own imports, as the page shows it.
# ruff: noqa: E402

import os
from typing import Any, Dict

from desflow import KafkaEgress

# The address and the credentials come from the environment, so no secret is in the file
connection: Dict[str, Any] = dict(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    # Passed as they are to the Kafka client
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",  # or SCRAM-SHA-512
    sasl_plain_username=os.environ["KAFKA_API_KEY"],
    sasl_plain_password=os.environ["KAFKA_API_SECRET"],
)
egress = KafkaEgress(**connection)


def mtls_ingress():
    from aiokafka.helpers import create_ssl_context

    from desflow import KafkaIngress

    ssl_context = create_ssl_context(
        cafile="/path/to/ca.pem",
        certfile="/path/to/service.cert",
        keyfile="/path/to/service.key",
    )

    ingress = KafkaIngress(
        topic="sim-commands",
        bootstrap_servers="secure-broker.internal.company.com:9093",
        security_protocol="SSL",
        ssl_context=ssl_context,
    )
    return ingress


def msk_connectors():
    from aiokafka.abc import AbstractTokenProvider
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    from desflow import KafkaAdminConnector, KafkaEgress

    class MSKTokenProvider(AbstractTokenProvider):
        async def token(self):
            # Uses the AWS credentials of your environment, or the EC2 or EKS role
            token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
            return token

    msk = dict(
        bootstrap_servers="b-1.my-msk.amazonaws.com:9098",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
    )

    # KafkaIngress, KafkaEgress and KafkaAdminConnector all use aiokafka
    egress = KafkaEgress(**msk, sasl_oauth_token_provider=MSKTokenProvider())

    admin = KafkaAdminConnector(**msk, sasl_oauth_token_provider=MSKTokenProvider())
    admin.create_topics([{"name": "sim-events", "partitions": 3}])
    return egress, admin


import asyncio

from aiokafka import AIOKafkaConsumer


async def show_events(count: int = 3) -> None:
    """Prints the first events of sim-events, read with the settings of the egress."""
    consumer = AIOKafkaConsumer(
        "sim-events", **connection, auto_offset_reset="earliest"
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

```yaml
# The Connect to Secure Kafka guide: a shop that publishes its events to a cluster
# that uses SASL PLAIN, such as Confluent Cloud. Set KAFKA_BOOTSTRAP_SERVERS,
# KAFKA_API_KEY and KAFKA_API_SECRET, then run it with:
# desflow run kafka_security.yaml
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
    payload: {event_type: order_packed}

egress:
  - type: Kafka
    config:
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-pkc-xxxx.us-east-1.aws.confluent.cloud:9092}
      # Passed as they are to the Kafka client
      security_protocol: SASL_SSL
      sasl_mechanism: PLAIN  # or SCRAM-SHA-512
      sasl_plain_username: ${KAFKA_API_KEY:-<YOUR_API_KEY>}
      sasl_plain_password: ${KAFKA_API_SECRET:-<YOUR_API_SECRET>}

run:
  until: 1 min
```

```python
"""The shop of the Connect to Secure Kafka page, built with the builder API.

The simulation publishes to a cluster that uses SASL PLAIN, such as Confluent Cloud: set
KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET, then run
`python kafka_security.py`. kafka_security_connections.py, which holds the egress, must be
beside this file.
"""

# ruff: noqa: E402

import logging

from kafka_security_connections import egress

from desflow import SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=1.0, random_seed=42)
    .add_resource("packers", current_cap=1, max_cap=1)
    .add_service("packing", dist="constant", value=1.5)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(egress)  # the SASL egress above
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack_order(context.next_instance("pack_order"), entity=order))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
    )
    app.run(until="1 min")
```

```python
"""The Connect to Secure Kafka guide, low-level API: a shop that publishes its events
through the SASL egress of kafka_security_connections.py, which must be beside this
script.

Set KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY and KAFKA_API_SECRET, then run
`python kafka_security_low_level.py`. It runs for one minute.
"""

import itertools
import logging

from kafka_security_connections import egress

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
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)


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
    env = shop([egress])  # the SASL egress of kafka_security_connections.py
    try:
        env.run(until=60)
    finally:
        env.teardown()
```
