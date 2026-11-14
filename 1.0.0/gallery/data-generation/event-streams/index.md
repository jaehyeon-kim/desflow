# Event Streams

Five streams whose records depend on each other: orders that carry their customer's tier, a stolen card that makes three large payments, stock queries that read the latest level, requests that become traces of nested spans, and customer changes that end in Kafka tombstones. Each entry is a complete file in three forms.

______________________________________________________________________

## Before You Start

The commands run from the root of a clone of the repository, where the files are in `examples/gallery/`:

```bash
git clone https://github.com/jaehyeon-kim/desflow.git
cd desflow
```

An entry that needs a service or an extra says so. [Services for the Examples](https://jaehyeon.me/desflow/latest/gallery/#services-for-the-examples) shows how to start the services. A file with a fixed `random_seed` and `factor: 0` prints the same records on every run, and each output block below holds the first events of the YAML file. The Builder and Low-level files publish the same records.

______________________________________________________________________

## Entries

### Each order carries the tier of the customer who placed it

Customers sign up with a tier, and each order reads one sign-up row, so its customer id and tier agree. An order placed before any customer has signed up is dropped.

Run it with the sample option, which stops after the first events and prints them:

```bash
desflow run examples/gallery/orders_carry_the_customers_tier.yaml --sample 5
```

```yaml
# Each order carries the tier of the customer who placed it.
#
# Customers sign up with a tier, and each order reads one sign-up row, so its customer
# id and tier agree. An order placed before any customer has signed up is dropped.
#
# Needs: nothing
# Run it with: desflow run examples/gallery/orders_carry_the_customers_tier.yaml

simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  signups: {dist: exponential, rate: 0.1, spawn: sign_up}
  orders: {dist: exponential, rate: 1, spawn: place_order}

tasks:
  sign_up:
    payload:
      customer_id: {gen: sequence}
      tier:
        gen: weighted
        options: {basic: 70, plus: 25, pro: 5}
  place_order:
    vars:
      customer: {gen: lookup, task: sign_up}
    payload:
      order_id: {gen: sequence}
      customer_id: {gen: var, name: customer, path: customer_id}
      tier: {gen: var, name: customer, path: tier}
      amount: {gen: uniform, low: 5, high: 200, decimals: 2}

run: {until: 300}
```

```text
{"stream_type":"event","sim_ts":42.95160161372071,"timestamp":"...","key":"signups-1","task":"sign_up","instance":0,"value":{"customer_id":0,"tier":"plus"}}
{"stream_type":"event","sim_ts":44.816407163479376,"timestamp":"...","key":"orders-33","task":"place_order","instance":32,"value":{"order_id":0,"customer_id":0,"tier":"plus","amount":48.5}}
{"stream_type":"event","sim_ts":45.43549590251013,"timestamp":"...","key":"orders-34","task":"place_order","instance":33,"value":{"order_id":1,"customer_id":0,"tier":"plus","amount":121.18}}
{"stream_type":"event","sim_ts":45.58801673322658,"timestamp":"...","key":"orders-35","task":"place_order","instance":34,"value":{"order_id":2,"customer_id":0,"tier":"plus","amount":192.09}}
{"stream_type":"event","sim_ts":45.75519547391547,"timestamp":"...","key":"orders-36","task":"place_order","instance":35,"value":{"order_id":3,"customer_id":0,"tier":"plus","amount":154.2}}
```

The Builder file builds the same model with the builder API and prints every record of its run:

```bash
python examples/gallery/orders_carry_the_customers_tier_builder.py
```

```python
"""Each order carries the tier of the customer who placed it.

The Builder form of orders_carry_the_customers_tier.yaml, with the same records.
Customers sign up with a tier, and each order reads one sign-up row, so its customer id
and tier agree. An order placed before any customer has signed up is dropped.

Needs: nothing
Run it with: python examples/gallery/orders_carry_the_customers_tier_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 300


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Shop",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_arrival("signups", "exponential", rate=0.1, spawn="sign_up")
        .add_arrival("orders", "exponential", rate=1, spawn="place_order")
    )

    sign_up_payload = Template(
        {
            "customer_id": {"gen": "sequence"},
            "tier": {
                "gen": "weighted",
                "options": {"basic": 70, "plus": 25, "pro": 5},
            },
        },
        name="tasks.sign_up",
    )
    place_order_payload = Template(
        {
            "order_id": {"gen": "sequence"},
            "customer_id": {"gen": "var", "name": "customer", "path": "customer_id"},
            "tier": {"gen": "var", "name": "customer", "path": "tier"},
            "amount": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
        },
        name="tasks.place_order",
        vars={"customer": {"gen": "lookup", "task": "sign_up"}},
    )

    # history is the number of rows a blueprint keeps (1_000_000 here, as in the YAML);
    # place_order looks up the sign-up rows
    @app.task(name="sign_up", history=1_000_000)
    def sign_up(task_id):
        return sign_up_payload

    @app.task(name="place_order")
    def place_order(task_id):
        return place_order_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

```bash
python examples/gallery/orders_carry_the_customers_tier_low_level.py
```

The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `tasks.sign_up.payload.tier`, `tasks.place_order.vars.customer` and `tasks.place_order.payload.amount`. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's. The gaps between arrivals come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.signups` and `arrival.orders`, the sequences the blueprint uses for its arrivals.

```python
"""Each order carries the tier of the customer who placed it.

The Low-level form of orders_carry_the_customers_tier.yaml, with the same records, built
on `DynamicRealtimeEnvironment`. Customers sign up with a tier, and each order reads one
sign-up row, so its customer id and tier agree. An order placed before any customer has
signed up is dropped.

Needs: nothing
Run it with: python examples/gallery/orders_carry_the_customers_tier_low_level.py
"""

import itertools
import logging
from datetime import datetime

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

SEED = 42
UNTIL = 300
TIERS = {"basic": 70, "plus": 25, "pro": 5}


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def build(env):
    """Starts the sign-up and order sources on `env`."""
    sampler = Sampler(seed=SEED)
    tier = sampler.generator("tasks.sign_up.payload.tier")
    pick = sampler.generator("tasks.place_order.vars.customer")
    amount = sampler.generator("tasks.place_order.payload.amount")
    signup_gap = DistributionConfig(dist="exponential", rate=0.1)
    signup_gap.stream = "arrival.signups"
    order_gap = DistributionConfig(dist="exponential", rate=1)
    order_gap.stream = "arrival.orders"
    customers = []  # the sign-ups so far, oldest first
    order_ids = itertools.count()  # the `sequence` of the published orders

    def signups():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(signup_gap))
            customer = {"customer_id": number - 1, "tier": weighted(tier, TIERS)}
            customers.append(customer)
            env.publish_event(
                f"signups-{number}", customer, task="sign_up", instance=number - 1
            )

    def orders():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(order_gap))
            # The blueprint drops an order that arrives before the first sign-up, and
            # the dropped order keeps its number
            if not customers:
                continue
            customer = customers[int(pick.integers(len(customers)))]
            payload = {
                "order_id": next(order_ids),
                "customer_id": customer["customer_id"],
                "tier": customer["tier"],
                "amount": round(5 + (200 - 5) * amount.random(), 2),
            }
            env.publish_event(
                f"orders-{number}", payload, task="place_order", instance=number - 1
            )

    env.process(signups())
    env.process(orders())


def run():
    """Runs the model for 300 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```

### A stolen card makes three large payments seconds after a normal one

Payments arrive from twenty cards, and 3 percent of them come from a card that has just been stolen. Two seconds later the same card makes three large payments, and every record is keyed by its card.

Run it with the sample option, which stops after the first events and prints them:

```bash
desflow run examples/gallery/stolen_card_makes_three_large_payments.yaml --sample 5
```

```yaml
# A stolen card makes three large payments seconds after a normal one.
#
# Payments arrive from twenty cards, and 3 percent of them come from a card that has
# just been stolen. Two seconds later the same card makes three large payments, and
# every record is keyed by its card.
#
# Needs: nothing
# Run it with: desflow run examples/gallery/stolen_card_makes_three_large_payments.yaml

simulation:
  sim_id: Bank
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  payments:
    dist: exponential
    rate: 1
    vars:
      card: {gen: integer, bounds: [1, 20]}
    key: {gen: format, template: "card-{card}"}
    spawn: payment

tasks:
  payment:
    payload:
      card: {gen: expr, expr: process.key}
      amount: {gen: uniform, low: 5, high: 120, decimals: 2}
      stolen: {gen: boolean, p: 0.03}
    next:
      when:
        - {condition: payload.stolen, task: burst}
  burst:
    payload: null
    spawn:
      task: large_payment
      count: 3
      after: 2
      args:
        card: {gen: upstream, path: card}
  large_payment:
    key: {gen: expr, expr: arg.card}
    payload:
      card: {gen: expr, expr: arg.card}
      amount: {gen: uniform, low: 500, high: 2000, decimals: 2}
      fraud: true

run: {until: 600}
```

```text
{"stream_type":"event","sim_ts":2.081546583669206,"timestamp":"...","key":"card-4","task":"payment","instance":0,"value":{"card":"card-4","amount":46.73,"stolen":false}}
{"stream_type":"event","sim_ts":2.859436022312404,"timestamp":"...","key":"card-3","task":"payment","instance":1,"value":{"card":"card-3","amount":9.54,"stolen":false}}
{"stream_type":"event","sim_ts":2.9829212161369547,"timestamp":"...","key":"card-7","task":"payment","instance":2,"value":{"card":"card-7","amount":22.7,"stolen":false}}
{"stream_type":"event","sim_ts":3.93943343963651,"timestamp":"...","key":"card-20","task":"payment","instance":3,"value":{"card":"card-20","amount":36.17,"stolen":false}}
{"stream_type":"event","sim_ts":4.129047924723884,"timestamp":"...","key":"card-20","task":"payment","instance":4,"value":{"card":"card-20","amount":28.54,"stolen":false}}
```

The Builder file builds the same model with the builder API and prints every record of its run:

```bash
python examples/gallery/stolen_card_makes_three_large_payments_builder.py
```

```python
"""A stolen card makes three large payments seconds after a normal one.

The Builder form of stolen_card_makes_three_large_payments.yaml, with the same records.
Payments arrive from twenty cards, and 3 percent of them come from a card that has just
been stolen. Two seconds later the same card makes three large payments.

Needs: nothing
Run it with: python examples/gallery/stolen_card_makes_three_large_payments_builder.py
"""

import logging
from datetime import datetime

from desflow import NO_RECORD, ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 600


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Bank",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_arrival(
            "payments",
            "exponential",
            rate=1,
            vars={"card": generator({"gen": "integer", "bounds": [1, 20]})},
            key=generator({"gen": "format", "template": "card-{card}"}),
            spawn="payment",
        )
    )

    payment_payload = Template(
        {
            "card": {"gen": "expr", "expr": "process.key"},
            "amount": {"gen": "uniform", "low": 5, "high": 120, "decimals": 2},
            "stolen": {"gen": "boolean", "p": 0.03},
        },
        name="tasks.payment",
    )
    large_payment_payload = Template(
        {
            "card": {"gen": "expr", "expr": "arg.card"},
            "amount": {"gen": "uniform", "low": 500, "high": 2000, "decimals": 2},
            "fraud": True,
        },
        name="tasks.large_payment",
    )

    @app.task(
        name="payment",
        next={"when": [{"condition": "payload.stolen", "task": "burst"}]},
    )
    def payment(task_id):
        return payment_payload

    @app.task(
        name="burst",
        spawn={
            "task": "large_payment",
            "count": 3,
            "after": 2,
            "args": {"card": generator({"gen": "upstream", "path": "card"})},
        },
    )
    def burst(task_id):
        return NO_RECORD

    @app.task(name="large_payment", key=generator({"gen": "expr", "expr": "arg.card"}))
    def large_payment(task_id):
        return large_payment_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

```bash
python examples/gallery/stolen_card_makes_three_large_payments_low_level.py
```

The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `arrivals.payments.vars.card`, `tasks.payment.payload.amount` and `tasks.payment.payload.stolen` and 1 more. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's. The gaps between arrivals come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.payments`, the sequence the blueprint uses for its arrivals.

```python
"""A stolen card makes three large payments seconds after a normal one.

The Low-level form of stolen_card_makes_three_large_payments.yaml, with the same
records, built on `DynamicRealtimeEnvironment`. Payments arrive from twenty cards, and 3
percent of them come from a card that has just been stolen. Two seconds later the same
card makes three large payments.

Needs: nothing
Run it with: python examples/gallery/stolen_card_makes_three_large_payments_low_level.py
"""

import itertools
import logging
from datetime import datetime

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

SEED = 42
UNTIL = 600


def build(env):
    """Starts the payment source on `env`."""
    sampler = Sampler(seed=SEED)
    gap = DistributionConfig(dist="exponential", rate=1)
    gap.stream = "arrival.payments"
    pick_card = sampler.generator("arrivals.payments.vars.card")
    amount = sampler.generator("tasks.payment.payload.amount")
    stolen = sampler.generator("tasks.payment.payload.stolen")
    large_amount = sampler.generator("tasks.large_payment.payload.amount")
    instances = {"payment": itertools.count(), "large_payment": itertools.count()}

    def stolen_card(card):
        """Three large payments on the card, two seconds after the normal one."""
        yield env.timeout(2)
        for _ in range(3):
            payload = {
                "card": card,
                "amount": round(500 + (2000 - 500) * large_amount.random(), 2),
                "fraud": True,
            }
            env.publish_event(
                card,
                payload,
                task="large_payment",
                instance=next(instances["large_payment"]),
            )

    def payments():
        for _ in itertools.count():
            yield env.timeout(sampler.sample(gap))
            card = f"card-{int(pick_card.integers(1, 21))}"
            payload = {
                "card": card,
                "amount": round(5 + (120 - 5) * amount.random(), 2),
                "stolen": bool(stolen.random() < 0.03),
            }
            env.publish_event(
                card, payload, task="payment", instance=next(instances["payment"])
            )
            if payload["stolen"]:
                env.process(stolen_card(card))

    env.process(payments())


def run():
    """Runs the model for 600 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```

### Stock queries read the latest level of each product

Five products change their stock level by a few units every 5 seconds, each from its own last level, and the first change is at the start. Queries ask for the level of one product and get the level its latest update gave.

Run it with the sample option, which stops after the first events and prints them:

```bash
desflow run examples/gallery/stock_queries_read_the_latest_level.yaml --sample 5
```

```yaml
# Stock queries read the latest level of each product.
#
# Five products change their stock level by a few units every 5 seconds, each from its
# own last level, and the first change is at the start. Queries ask for the level of
# one product and get the level its latest update gave.
#
# Needs: nothing
# Run it with: desflow run examples/gallery/stock_queries_read_the_latest_level.yaml

simulation:
  sim_id: Warehouse
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  updates:
    count: 5
    key: {gen: sequence, start: 1, format: "sku-{}"}
    dist: constant
    value: 5
    first_at: 0
    spawn: update_stock
  queries: {dist: exponential, rate: 0.5, spawn: read_stock}

tasks:
  update_stock:
    payload:
      sku: {gen: entity}
      level:
        gen: max
        args:
          - 0
          - gen: add
            args:
              - {gen: previous, path: level, default: 100}
              - {gen: integer, bounds: [-12, 10]}
  read_stock:
    vars:
      row: {gen: lookup, task: update_stock, latest_by: sku}
    payload:
      sku: {gen: var, name: row, path: sku}
      level: {gen: var, name: row, path: level}

run: {until: 200}
```

```text
{"stream_type":"event","sim_ts":0.0,"timestamp":"...","key":"sku-1","task":"update_stock","instance":0,"value":{"sku":"sku-1","level":96}}
{"stream_type":"event","sim_ts":0.0,"timestamp":"...","key":"sku-2","task":"update_stock","instance":1,"value":{"sku":"sku-2","level":103}}
{"stream_type":"event","sim_ts":0.0,"timestamp":"...","key":"sku-3","task":"update_stock","instance":2,"value":{"sku":"sku-3","level":103}}
{"stream_type":"event","sim_ts":0.0,"timestamp":"...","key":"sku-4","task":"update_stock","instance":3,"value":{"sku":"sku-4","level":94}}
{"stream_type":"event","sim_ts":0.0,"timestamp":"...","key":"sku-5","task":"update_stock","instance":4,"value":{"sku":"sku-5","level":93}}
```

The Builder file builds the same model with the builder API and prints every record of its run:

```bash
python examples/gallery/stock_queries_read_the_latest_level_builder.py
```

```python
"""Stock queries read the latest level of each product.

The Builder form of stock_queries_read_the_latest_level.yaml, with the same records.
A blueprint's `count: 5` is five arrivals here, one for each product, each with the key
of its product. Queries look up the latest update of one product.

Needs: nothing
Run it with: python examples/gallery/stock_queries_read_the_latest_level_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 200


def build() -> SimulationContext:
    """Builds the model."""
    app = SimulationContext(
        sim_id="Warehouse",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1),
    ).add_egress(ConsoleEgress())

    for number in range(1, 6):
        app.add_arrival(
            f"updates{number}",
            "constant",
            value=5,
            first_at=0,
            spawn="update_stock",
            key=lambda n, number=number: f"sku-{number}",
        )
    app.add_arrival("queries", "exponential", rate=0.5, spawn="read_stock")

    update_payload = Template(
        {
            "sku": {"gen": "entity"},
            "level": {
                "gen": "max",
                "args": [
                    0,
                    {
                        "gen": "add",
                        "args": [
                            {"gen": "previous", "path": "level", "default": 100},
                            {"gen": "integer", "bounds": [-12, 10]},
                        ],
                    },
                ],
            },
        },
        name="tasks.update_stock",
    )
    read_payload = Template(
        {
            "sku": {"gen": "var", "name": "row", "path": "sku"},
            "level": {"gen": "var", "name": "row", "path": "level"},
        },
        name="tasks.read_stock",
        vars={"row": {"gen": "lookup", "task": "update_stock", "latest_by": "sku"}},
    )

    # history is the number of rows a blueprint keeps (1_000_000 here, as in the YAML);
    # read_stock looks up the update rows
    @app.task(name="update_stock", history=1_000_000)
    def update_stock(task_id):
        return update_payload

    @app.task(name="read_stock")
    def read_stock(task_id):
        return read_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

```bash
python examples/gallery/stock_queries_read_the_latest_level_low_level.py
```

The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `tasks.update_stock.payload.level.args.1.args.1` and `tasks.read_stock.vars.row`. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's. The gaps between arrivals come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.queries`, the sequence the blueprint uses for its arrivals.

```python
"""Stock queries read the latest level of each product.

The Low-level form of stock_queries_read_the_latest_level.yaml, with the same records,
built on `DynamicRealtimeEnvironment`. Each draw is made with `Sampler`, from the
random sequence the blueprint's generator at that place uses, so the draws are the
blueprint's. A product's new level is its last level plus the change. A query draws one
of the products, which is what `lookup` with `latest_by` does, and reads its last level.

Needs: nothing
Run it with: python examples/gallery/stock_queries_read_the_latest_level_low_level.py
"""

import itertools
import logging
from datetime import datetime

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

SEED = 42
UNTIL = 200


def build(env):
    """Starts the five product loops and the query source on `env`."""
    sampler = Sampler(seed=SEED)
    change = sampler.generator("tasks.update_stock.payload.level.args.1.args.1")
    pick = sampler.generator("tasks.read_stock.vars.row")
    gap = DistributionConfig(dist="exponential", rate=0.5)
    gap.stream = "arrival.queries"
    instances = itertools.count()
    levels = {}  # the last level of each product

    def product(number):
        sku = f"sku-{number}"
        for _ in itertools.count():
            level = max(0, levels.get(sku, 100) + int(change.integers(-12, 11)))
            levels[sku] = level
            env.publish_event(
                sku,
                {"sku": sku, "level": level},
                task="update_stock",
                instance=next(instances),
            )
            yield env.timeout(5)

    def queries():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            # The products first appear in the order 1 to 5, and each has a latest level
            sku = f"sku-{int(pick.integers(len(levels))) + 1}"
            env.publish_event(
                f"queries-{number}",
                {"sku": sku, "level": levels[sku]},
                task="read_stock",
                instance=number - 1,
            )

    for number in range(1, 6):
        env.process(product(number))
    env.process(queries())


def run():
    """Runs the model for 200 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```

### Each request becomes a trace of nested spans, and each child span names its parent

The gateway span covers an auth check and a database call that run inside it, so each child starts and ends within its parent's time. Every span has the trace id of its request, an id of its own, its parent's id (null for the gateway span), its start in seconds from the start of the run and its duration in milliseconds.

Run it with the sample option, which stops after the first events and prints them:

```bash
desflow run examples/gallery/requests_become_traces_of_nested_spans.yaml --sample 5
```

```yaml
# Each request becomes a trace of nested spans, and each child span names its parent.
#
# The gateway span covers an auth check and a database call that run inside it, so each
# child starts and ends within its parent's time. Every span has the trace id of its
# request, an id of its own, its parent's id (null for the gateway span), its start in
# seconds from the start of the run and its duration in milliseconds.
#
# Needs: nothing
# Run it with: desflow run examples/gallery/requests_become_traces_of_nested_spans.yaml

simulation:
  sim_id: Api
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  requests:
    dist: exponential
    rate: 0.5
    attributes:
      trace_id: {gen: uuid}
      gateway_span: {gen: uuid}
      began: {gen: expr, expr: t}
    spawn: auth

# A span's record is published when it ends, so the children come before the gateway
tasks:
  auth:
    vars:
      duration: {gen: integer, bounds: [5, 15]}
    service_time: duration / 1000
    payload:
      trace_id: {gen: attr, name: trace_id}
      span_id: {gen: uuid}
      parent_span_id: {gen: attr, name: gateway_span}
      name: check token
      start: {gen: expr, expr: "t - duration / 1000"}
      duration_ms: {gen: var, name: duration}
    next: orders_db
  orders_db:
    vars:
      duration: {gen: integer, bounds: [10, 120]}
    service_time: duration / 1000
    payload:
      trace_id: {gen: attr, name: trace_id}
      span_id: {gen: uuid}
      parent_span_id: {gen: attr, name: gateway_span}
      name: SELECT orders
      start: {gen: expr, expr: "t - duration / 1000"}
      duration_ms: {gen: var, name: duration}
    next: gateway
  gateway:
    vars:
      duration: {gen: integer, bounds: [20, 60]}
    service_time: duration / 1000
    payload:
      trace_id: {gen: attr, name: trace_id}
      span_id: {gen: attr, name: gateway_span}
      parent_span_id: null
      name: GET /orders
      start: {gen: attr, name: began}
      duration_ms: {gen: expr, expr: "round((t - attr.began) * 1000, 3)"}

run: {until: 120}
```

```text
{"stream_type":"event","sim_ts":0.9203554340231125,"timestamp":"...","key":"requests-1","task":"auth","instance":0,"value":{"status":"started"}}
{"stream_type":"event","sim_ts":0.9263554340231125,"timestamp":"...","key":"requests-1","task":"auth","instance":0,"value":{"trace_id":"fbf20686-5abf-4ade-9510-ae446054562c","span_id":"5f760756-dd54-4de5-aabd-4788a813c817","parent_span_id":"218acc9a-37ea-463c-b078-b0bdd04140aa","name":"check token","start":0.9203554340231125,"duration_ms":6}}
{"stream_type":"event","sim_ts":0.9263554340231125,"timestamp":"...","key":"requests-1","task":"orders_db","instance":0,"value":{"status":"started"}}
{"stream_type":"event","sim_ts":1.0103554340231125,"timestamp":"...","key":"requests-1","task":"orders_db","instance":0,"value":{"trace_id":"fbf20686-5abf-4ade-9510-ae446054562c","span_id":"3eb6d72c-3be1-4f2f-b392-061e4d0579ab","parent_span_id":"218acc9a-37ea-463c-b078-b0bdd04140aa","name":"SELECT orders","start":0.9263554340231125,"duration_ms":84}}
{"stream_type":"event","sim_ts":1.0103554340231125,"timestamp":"...","key":"requests-1","task":"gateway","instance":0,"value":{"status":"started"}}
```

The Builder file builds the same model with the builder API and prints every record of its run:

```bash
python examples/gallery/requests_become_traces_of_nested_spans_builder.py
```

```python
"""Each request becomes a trace of nested spans, and each child span names its parent.

The Builder form of requests_become_traces_of_nested_spans.yaml, with the same records.
The gateway span covers an auth check and a database call that run inside it, so each
child starts and ends within its parent's time.

Needs: nothing
Run it with: python examples/gallery/requests_become_traces_of_nested_spans_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 120


def duration(low, high):
    """The task's `duration` var, drawn once for each span, in milliseconds."""
    return {"duration": generator({"gen": "integer", "bounds": [low, high]})}


def child_span(task, name):
    """The payload of a span inside the gateway span, whose id is its parent's id."""
    return Template(
        {
            "trace_id": {"gen": "attr", "name": "trace_id"},
            "span_id": {"gen": "uuid"},
            "parent_span_id": {"gen": "attr", "name": "gateway_span"},
            "name": name,
            "start": {"gen": "expr", "expr": "t - duration / 1000"},
            "duration_ms": {"gen": "var", "name": "duration"},
        },
        name=f"tasks.{task}",
    )


gateway_payload = Template(
    {
        "trace_id": {"gen": "attr", "name": "trace_id"},
        "span_id": {"gen": "attr", "name": "gateway_span"},
        "parent_span_id": None,
        "name": "GET /orders",
        "start": {"gen": "attr", "name": "began"},
        "duration_ms": {"gen": "expr", "expr": "round((t - attr.began) * 1000, 3)"},
    },
    name="tasks.gateway",
)


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Api",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_arrival(
            "requests",
            "exponential",
            rate=0.5,
            attributes={
                "trace_id": generator({"gen": "uuid"}),
                "gateway_span": generator({"gen": "uuid"}),
                "began": generator({"gen": "expr", "expr": "t"}),
            },
            spawn="auth",
        )
    )

    # A span's record is published when it ends, so the children come before the gateway
    auth_payload = child_span("auth", "check token")
    orders_db_payload = child_span("orders_db", "SELECT orders")

    @app.task(
        name="auth",
        vars=duration(5, 15),
        service_time="duration / 1000",
        next="orders_db",
    )
    def auth(task_id):
        return auth_payload

    @app.task(
        name="orders_db",
        vars=duration(10, 120),
        service_time="duration / 1000",
        next="gateway",
    )
    def orders_db(task_id):
        return orders_db_payload

    @app.task(name="gateway", vars=duration(20, 60), service_time="duration / 1000")
    def gateway(task_id):
        return gateway_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

```bash
python examples/gallery/requests_become_traces_of_nested_spans_low_level.py
```

The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `arrivals.requests.attributes.trace_id` and `arrivals.requests.attributes.gateway_span`. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's. The gaps between arrivals come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.requests`, the sequence the blueprint uses for its arrivals.

```python
"""Each request becomes a trace of nested spans, and each child span names its parent.

The Low-level form of requests_become_traces_of_nested_spans.yaml, with the same
records, built on `DynamicRealtimeEnvironment`. The gateway span covers an auth check
and a database call that run inside it, so each child starts and ends within its
parent's time.

Needs: nothing
Run it with: python examples/gallery/requests_become_traces_of_nested_spans_low_level.py
"""

import itertools
import logging
import uuid
from datetime import datetime

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

SEED = 42
UNTIL = 120
# The tasks of a request in order: the task, the span's name and its duration range in ms
SPANS = [
    ("auth", "check token", 5, 15),
    ("orders_db", "SELECT orders", 10, 120),
    ("gateway", "GET /orders", 20, 60),
]


def build(env):
    """Starts the request source on `env`."""
    sampler = Sampler(seed=SEED)
    gap = DistributionConfig(dist="exponential", rate=0.5)
    gap.stream = "arrival.requests"
    duration = {
        task: sampler.generator(f"tasks.{task}.vars.duration") for task, *_ in SPANS
    }
    trace_id = sampler.generator("arrivals.requests.attributes.trace_id")
    gateway_span = sampler.generator("arrivals.requests.attributes.gateway_span")
    span_id = {
        task: sampler.generator(f"tasks.{task}.payload.span_id")
        for task, *_ in SPANS
        if task != "gateway"
    }
    # Each task numbers its own instances
    instances = {task: itertools.count() for task, *_ in SPANS}

    def uuid_of(rng):
        return str(uuid.UUID(bytes=rng.bytes(16), version=4))

    def request(key):
        # The trace and the gateway span are the request's own, and the children name them
        trace, gateway, began = uuid_of(trace_id), uuid_of(gateway_span), env.now
        for task, name, low, high in SPANS:
            instance = next(instances[task])
            milliseconds = int(duration[task].integers(low, high + 1))
            env.publish_event(key, {"status": "started"}, task=task, instance=instance)
            yield env.timeout(milliseconds / 1000)
            span = {
                "trace_id": trace,
                "span_id": gateway if task == "gateway" else uuid_of(span_id[task]),
                "parent_span_id": None if task == "gateway" else gateway,
                "name": name,
                "start": began if task == "gateway" else env.now - milliseconds / 1000,
                "duration_ms": (
                    round((env.now - began) * 1000, 3)
                    if task == "gateway"
                    else milliseconds
                ),
            }
            env.publish_event(key, span, task=task, instance=instance)

    def requests():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(request(f"requests-{number}"))

    env.process(requests())


def run():
    """Runs the model for 120 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```

### A customer table's changes are written to Kafka, and each delete is a tombstone

Ten customers exist at the start, then inserts, updates and deletes arrive in the shares 40, 45 and 15 percent, each keyed by its customer id. A delete has no value, which is how Kafka marks a deleted key (a tombstone), and the oldest customer is deleted first.

It needs a broker, started with `odctl up kafka-lite`, and the `kafka` extra. A delete is published as a Kafka tombstone, a record with a key and no value, so this entry shows no output block here.

Run it with the sample option, which stops after the first events and prints them:

```bash
desflow run examples/gallery/customer_deletes_become_kafka_tombstones.yaml --sample 5
```

```yaml
# A customer table's changes are written to Kafka, and each delete is a tombstone.
#
# Ten customers exist at the start, then inserts, updates and deletes arrive in the
# shares 40, 45 and 15 percent, each keyed by its customer id. A delete has no value,
# which is how Kafka marks a deleted key (a tombstone), and the oldest customer is
# deleted first.
#
# Needs: odctl up kafka-lite and the kafka extra
# Run it with: desflow run
# examples/gallery/customer_deletes_become_kafka_tombstones.yaml

simulation:
  sim_id: Customers
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - type: Kafka
    envelope: false
    config:
      bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:9092}
      event_topic: customers-cdc

arrivals:
  existing: {initial: 10, spawn: insert_customer}
  changes: {dist: exponential, rate: 1, spawn: change}

tasks:
  change:
    payload: null
    next:
      weights: {insert_customer: 40, update_customer: 45, delete_customer: 15}
  insert_customer:
    key_field: customer_id
    payload:
      op: insert
      customer_id: {gen: sequence}
      plan:
        gen: weighted
        options: {free: 60, pro: 40}
  # An update changes the newest customer, so no record names a customer that is gone
  update_customer:
    key_field: customer_id
    payload:
      op: update
      customer_id:
        gen: lookup
        task: insert_customer
        path: customer_id
        strategy: last
      plan:
        gen: weighted
        options: {free: 40, pro: 60}
  delete_customer:
    key: {gen: sequence}
    payload: {gen: literal, value: null}

run: {until: 120}
```

The Builder file builds the same model with the builder API and prints every record of its run:

```bash
python examples/gallery/customer_deletes_become_kafka_tombstones_builder.py
```

```python
"""A customer table's changes are written to Kafka, and each delete is a tombstone.

The Builder form of customer_deletes_become_kafka_tombstones.yaml, with the same
records. Every message is keyed by its customer id, and a delete has no value, so with
`envelope=False` the Kafka sink sends it as a tombstone.

Needs: odctl up kafka-lite and the kafka extra
Run it with: python examples/gallery/customer_deletes_become_kafka_tombstones_builder.py
"""

import os
from datetime import datetime

from desflow import NO_RECORD, KafkaEgress, SimulationContext
from desflow.values import Template, generator

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

UNTIL = 120


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Customers",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        # envelope=False sends each event's value alone, and a null value as a tombstone
        .add_egress(
            KafkaEgress(
                bootstrap_servers=BOOTSTRAP_SERVERS, event_topic="customers-cdc"
            ),
            envelope=False,
        )
        .add_initial_arrival("existing", 10, spawn="insert_customer")
        .add_arrival("changes", "exponential", rate=1, spawn="change")
    )

    insert_payload = Template(
        {
            "op": "insert",
            "customer_id": {"gen": "sequence"},
            "plan": {"gen": "weighted", "options": {"free": 60, "pro": 40}},
        },
        name="tasks.insert_customer",
    )
    update_payload = Template(
        {
            "op": "update",
            "customer_id": {
                "gen": "lookup",
                "task": "insert_customer",
                "path": "customer_id",
                "strategy": "last",
            },
            "plan": {"gen": "weighted", "options": {"free": 40, "pro": 60}},
        },
        name="tasks.update_customer",
    )
    # One generator that gives null: the task publishes a record whose value is null
    delete_payload = Template(
        {"gen": "literal", "value": None}, name="tasks.delete_customer"
    )

    @app.task(
        name="change",
        next={
            "weights": {
                "insert_customer": 40,
                "update_customer": 45,
                "delete_customer": 15,
            }
        },
    )
    def change(task_id):
        return NO_RECORD

    # history is the number of rows a blueprint keeps (1_000_000 here, as in the YAML);
    # update_customer looks up the insert rows
    @app.task(name="insert_customer", key_field="customer_id", history=1_000_000)
    def insert_customer(task_id):
        return insert_payload

    @app.task(name="update_customer", key_field="customer_id")
    def update_customer(task_id):
        return update_payload

    @app.task(name="delete_customer", key=generator({"gen": "sequence"}))
    def delete_customer(task_id):
        return delete_payload

    return app


if __name__ == "__main__":
    build().run(until=UNTIL)
```

The Low-level file builds the model on `DynamicRealtimeEnvironment`, with plain Python processes, and prints every record of its run:

```bash
python examples/gallery/customer_deletes_become_kafka_tombstones_low_level.py
```

The Low-level file draws each value with `Sampler.generator(<place>)`, where the place is the key path of the blueprint's generator that makes the same value, such as `tasks.insert_customer.payload.plan` and `tasks.update_customer.payload.plan`. Each draw therefore comes from the random sequence the blueprint uses at the same place, so the draws are the blueprint's. The branch of each step is chosen by a `Router` whose random sequence is `Sampler.generator("route.change")`, the sequence of the blueprint's `next`. The gaps between arrivals come from `Sampler.sample` on a `DistributionConfig` whose `stream` is `arrival.changes`, the sequence the blueprint uses for its arrivals.

```python
"""A customer table's changes are written to Kafka, and each delete is a tombstone.

The Low-level form of customer_deletes_become_kafka_tombstones.yaml, with the same
records, built on `DynamicRealtimeEnvironment`. `publish_event(key, None)` publishes a
delete, and a Kafka sink set up with `envelopes=[False]` sends it as a tombstone.

Needs: odctl up kafka-lite and the kafka extra
Run it with: python examples/gallery/customer_deletes_become_kafka_tombstones_low_level.py
"""

import itertools
import os
from datetime import datetime

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    KafkaEgress,
    Router,
    Sampler,
)

SEED = 42
UNTIL = 120
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def build(env):
    """Starts the ten customers and the source of changes on `env`."""
    sampler = Sampler(seed=SEED)
    insert_plan = sampler.generator("tasks.insert_customer.payload.plan")
    update_plan = sampler.generator("tasks.update_customer.payload.plan")
    router = Router(
        {
            "weights": {
                "insert_customer": 40,
                "update_customer": 45,
                "delete_customer": 15,
            }
        },
        rng=sampler.generator("route.change"),
    )
    gap = DistributionConfig(dist="exponential", rate=1)
    gap.stream = "arrival.changes"
    instances = {
        task: itertools.count()
        for task in ("insert_customer", "update_customer", "delete_customer")
    }
    customers = itertools.count()  # the customer ids, as `sequence` counts
    deleted = itertools.count()  # the oldest customer goes first
    inserted = []  # the customer ids of the inserts so far, oldest first

    def insert_customer():
        payload = {
            "op": "insert",
            "customer_id": next(customers),
            "plan": weighted(insert_plan, {"free": 60, "pro": 40}),
        }
        inserted.append(payload["customer_id"])
        env.publish_event(
            str(payload["customer_id"]),
            payload,
            task="insert_customer",
            instance=next(instances["insert_customer"]),
        )

    def update_customer():
        # An update changes the newest customer, as `lookup` with `strategy: last` does
        payload = {
            "op": "update",
            "customer_id": inserted[-1],
            "plan": weighted(update_plan, {"free": 40, "pro": 60}),
        }
        env.publish_event(
            str(payload["customer_id"]),
            payload,
            task="update_customer",
            instance=next(instances["update_customer"]),
        )

    def delete_customer():
        # A value of None is a tombstone for a Kafka sink with the envelope off
        env.publish_event(
            str(next(deleted)),
            None,
            task="delete_customer",
            instance=next(instances["delete_customer"]),
        )

    def changes():
        for _ in itertools.count():
            yield env.timeout(sampler.sample(gap))
            task = router.choose(None, env=env)
            {
                "insert_customer": insert_customer,
                "update_customer": update_customer,
                "delete_customer": delete_customer,
            }[task]()

    for _ in range(10):
        insert_customer()
    env.process(changes())


def run():
    """Runs the model for 120 simulation seconds."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress(
        [KafkaEgress(bootstrap_servers=BOOTSTRAP_SERVERS, event_topic="customers-cdc")],
        envelopes=[False],
    )
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
```
