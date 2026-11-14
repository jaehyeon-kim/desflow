# Build on the Last Value

Make each value depend on the one before it, as a price that drifts: five products are priced every second, and each price is the product's last price plus a small step.

______________________________________________________________________

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

  ```bash
  git clone https://github.com/jaehyeon-kim/desflow.git
  cd desflow
  ```

- [First Simulation](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md) explains arrivals, tasks and the records a run writes.

- [`previous`](https://jaehyeon.me/desflow/latest/generators/related-records/previous/index.md) lists the keys of the generator that reads the last value, and [`add`](https://jaehyeon.me/desflow/latest/generators/maths/add/index.md), [`min`](https://jaehyeon.me/desflow/latest/generators/maths/min/index.md) and [`max`](https://jaehyeon.me/desflow/latest/generators/maths/max/index.md) list the generators that combine values.

______________________________________________________________________

## Steps

### Start Five Products

`count: 5` starts five products from one arrival, and each is priced every second. Each keeps its own last payload, so `previous` reads the last value of the same field for the same product.

```yaml
arrivals:
  prices:
    dist: constant
    value: 1
    spawn: price
    count: 5
    key: {gen: sequence, format: "product-{}"}
```

```python
# A blueprint's `count: 5` is five arrivals here, each with the key of its product
for number in range(5):
    app.add_arrival(
        f"prices{number}",
        "constant",
        value=1,
        spawn="price",
        key=lambda n, number=number: f"product-{number}",
    )
```

A Builder arrival has no `count`, so the file adds one arrival for each product.

```python
sampler = Sampler(seed=42)
# The places of the two draws in the blueprint name their random sequences
step = sampler.generator("tasks.price.payload.price.args.1")
drift = sampler.generator("tasks.price.payload.stock.args.1.args.1.args.1")
last: dict = {}  # product key -> its last price and stock, 20 and 250 at first
counts = {"price": 0}
```

`Sampler(seed=42)` makes the two draws. Each generator is named by the place of its draw in the blueprint, so the steps are the blueprint's. A product is priced every second, so there is no gap to draw.

### Read the Last Value

`previous` with `path: price` reads the product's last price. `default: 20` is the value before the first record. [`add`](https://jaehyeon.me/desflow/latest/generators/maths/add/index.md) adds a draw between -1 and 1, so the price drifts by up to 1 each second. The stock level adds a normal step to its last value, and [`min`](https://jaehyeon.me/desflow/latest/generators/maths/min/index.md) and [`max`](https://jaehyeon.me/desflow/latest/generators/maths/max/index.md) keep it between 0 and 500 units.

```yaml
tasks:
  price:
    payload:
      product: {gen: entity}
      price:
        gen: add
        args:
          - {gen: previous, path: price, default: 20}
          - {gen: uniform, low: -1, high: 1}
        decimals: 2
      stock:
        gen: max
        args:
          - 0
          - gen: min
            args:
              - 500
              - gen: add
                args:
                  - {gen: previous, path: stock, default: 250}
                  - {gen: normal, mean: 0, std: 40}
        decimals: 0
```

```python
price_payload = Template(
    {
        "product": {"gen": "entity"},
        "price": {
            "gen": "add",
            "args": [
                {"gen": "previous", "path": "price", "default": 20},
                {"gen": "uniform", "low": -1, "high": 1},
            ],
            "decimals": 2,
        },
        "stock": {
            "gen": "max",
            "args": [
                0,
                {
                    "gen": "min",
                    "args": [
                        500,
                        {
                            "gen": "add",
                            "args": [
                                {
                                    "gen": "previous",
                                    "path": "stock",
                                    "default": 250,
                                },
                                {"gen": "normal", "mean": 0, "std": 40},
                            ],
                        },
                    ],
                },
            ],
            "decimals": 0,
        },
    },
    name="tasks.price",
)

@app.task(name="price")
def price(task_id):
    """Gives one price record."""
    return price_payload
```

```python
def product(number):
    key = f"product-{number}"
    while True:
        yield env.timeout(1)
        price, stock = last.get(key, (20, 250))
        price = round(price + step.uniform(-1, 1), 2)
        stock = int(round(max(0, min(500, stock + float(drift.normal(0, 40))))))
        last[key] = (price, stock)
        env.publish_event(
            key,
            {"product": key, "price": price, "stock": stock},
            task="price",
            instance=counts["price"],
        )
        counts["price"] += 1

for number in range(5):
    env.process(product(number))
```

A Low-level script keeps each product's last values itself, in the dictionary `last`, because `previous` cannot be read outside a task. It draws the two steps from `Sampler.generator` for the places the blueprint uses, so the draws are the blueprint's.

______________________________________________________________________

## Check the Result

Run the model for 30 simulated seconds and print its first 6 records. The five products are priced at 1 second, each near 20, and `product-0` is priced again at 2 seconds within 1 of its first price.

```bash
desflow run docs/snippets/generate-data/build_on_the_last_value.yaml --sample 6
```

```bash
python docs/snippets/generate-data/build_on_the_last_value_builder.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```bash
python docs/snippets/generate-data/build_on_the_last_value_low_level.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```text
{"stream_type":"event","sim_ts":1.0,"timestamp":"...","key":"product-0","task":"price","instance":0,"value":{"product":"product-0","price":20.07,"stock":260}}
{"stream_type":"event","sim_ts":1.0,"timestamp":"...","key":"product-1","task":"price","instance":1,"value":{"product":"product-1","price":19.78,"stock":218}}
{"stream_type":"event","sim_ts":1.0,"timestamp":"...","key":"product-2","task":"price","instance":2,"value":{"product":"product-2","price":20.29,"stock":323}}
{"stream_type":"event","sim_ts":1.0,"timestamp":"...","key":"product-3","task":"price","instance":3,"value":{"product":"product-3","price":20.99,"stock":219}}
{"stream_type":"event","sim_ts":1.0,"timestamp":"...","key":"product-4","task":"price","instance":4,"value":{"product":"product-4","price":19.28,"stock":271}}
{"stream_type":"event","sim_ts":2.0,"timestamp":"...","key":"product-0","task":"price","instance":5,"value":{"product":"product-0","price":20.15,"stock":243}}
```

The docs test runs the model for 5,000 simulated seconds: no price differs from the product's last price by more than 1, and the stock reaches both 0 and 500 but never leaves that range.

______________________________________________________________________

## Full Files

Full files

```yaml
# Build on the last value: prices that drift from the previous price.
#
# Five products are priced once a second. Each price is the product's last price plus a
# draw between -1 and 1, starting from 20. Each stock level adds a normal step to the
# last one and stays between 0 and 500 units. `previous` reads the last value of the
# same field of the same product, because with `count` each product keeps its own last
# payload.
#
# Needs: nothing.
# Run it with: desflow run docs/snippets/generate-data/build_on_the_last_value.yaml

simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  prices:
    dist: constant
    value: 1
    spawn: price
    count: 5
    key: {gen: sequence, format: "product-{}"}

tasks:
  price:
    payload:
      product: {gen: entity}
      price:
        gen: add
        args:
          - {gen: previous, path: price, default: 20}
          - {gen: uniform, low: -1, high: 1}
        decimals: 2
      stock:
        gen: max
        args:
          - 0
          - gen: min
            args:
              - 500
              - gen: add
                args:
                  - {gen: previous, path: stock, default: 250}
                  - {gen: normal, mean: 0, std: 40}
        decimals: 0

run: {until: 30}
```

```python
"""Build on the last value: prices that drift from the previous price, builder API.

The builder version of `build_on_the_last_value.yaml`: five products, each with the key
of its product, are priced once a second for 30 seconds. `previous` reads the last value
of the field that the same product published.

Needs: nothing.
Run it with: python docs/snippets/generate-data/build_on_the_last_value_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 30


def build() -> SimulationContext:
    """Builds the products."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    # A blueprint's `count: 5` is five arrivals here, each with the key of its product
    for number in range(5):
        app.add_arrival(
            f"prices{number}",
            "constant",
            value=1,
            spawn="price",
            key=lambda n, number=number: f"product-{number}",
        )
    price_payload = Template(
        {
            "product": {"gen": "entity"},
            "price": {
                "gen": "add",
                "args": [
                    {"gen": "previous", "path": "price", "default": 20},
                    {"gen": "uniform", "low": -1, "high": 1},
                ],
                "decimals": 2,
            },
            "stock": {
                "gen": "max",
                "args": [
                    0,
                    {
                        "gen": "min",
                        "args": [
                            500,
                            {
                                "gen": "add",
                                "args": [
                                    {
                                        "gen": "previous",
                                        "path": "stock",
                                        "default": 250,
                                    },
                                    {"gen": "normal", "mean": 0, "std": 40},
                                ],
                            },
                        ],
                    },
                ],
                "decimals": 0,
            },
        },
        name="tasks.price",
    )

    @app.task(name="price")
    def price(task_id):
        """Gives one price record."""
        return price_payload


    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Build on the last value: prices that drift from the previous price, low-level API.

The low-level version of `build_on_the_last_value.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. A
low-level model keeps each product's last price and stock itself, and draws each step
from the random sequence the blueprint's generator uses, with `Sampler`.

Needs: nothing.
Run it with: python docs/snippets/generate-data/build_on_the_last_value_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

UNTIL = 30
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the five products on `env`."""
    sampler = Sampler(seed=42)
    # The places of the two draws in the blueprint name their random sequences
    step = sampler.generator("tasks.price.payload.price.args.1")
    drift = sampler.generator("tasks.price.payload.stock.args.1.args.1.args.1")
    last: dict = {}  # product key -> its last price and stock, 20 and 250 at first
    counts = {"price": 0}

    def product(number):
        key = f"product-{number}"
        while True:
            yield env.timeout(1)
            price, stock = last.get(key, (20, 250))
            price = round(price + step.uniform(-1, 1), 2)
            stock = int(round(max(0, min(500, stock + float(drift.normal(0, 40))))))
            last[key] = (price, stock)
            env.publish_event(
                key,
                {"product": key, "price": price, "stock": stock},
                task="price",
                instance=counts["price"],
            )
            counts["price"] += 1

    for number in range(5):
        env.process(product(number))


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
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
