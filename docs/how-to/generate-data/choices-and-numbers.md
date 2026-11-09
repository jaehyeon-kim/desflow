# Choices and Numbers

Pick a value from a list with set odds and draw numbers from a range or a bell curve: a payment method that is card 60 times in 100, and a purchase with a price, an item count, a delivery time, a channel and a gift flag.

---

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

    ```bash
    git clone https://github.com/jaehyeon-kim/desflow.git
    cd desflow
    ```

- [First Simulation](../../learn/first-model/first-simulation.md) explains arrivals, tasks and the records a run writes.
- [Generators](../../generators/index.md) lists every generator kind and its keys.
- [Randomness and Seeds](../../learn/first-model/randomness-and-seeds.md) explains why a fixed seed gives the same draws in each run.

---

## Steps

### Describe the Arrivals

A payment arrives every second (`dist: constant`), and a purchase arrives about once a second (`dist: exponential` with `rate: 1`). Each arrival starts the task it names in `spawn`.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/choices_and_numbers.yaml:arrivals" hl_lines="2 3"
    arrivals:
      payments: {dist: constant, value: 1, spawn: payment}
      purchases: {dist: exponential, rate: 1, spawn: purchase}
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/choices_and_numbers_builder.py:arrival"
    app.add_arrival("payments", "constant", value=1, spawn="payment")
    app.add_arrival("purchases", "exponential", rate=1, spawn="purchase")
    ```

=== "Low-level"

    ```python title="docs/snippets/generate-data/choices_and_numbers_low_level.py:arrival"
    sampler = Sampler(seed=42)
    method = sampler.generator("tasks.payment.payload.method")
    price = sampler.generator("tasks.purchase.payload.price")
    items = sampler.generator("tasks.purchase.payload.items")
    delivery = sampler.generator("tasks.purchase.payload.delivery_minutes")
    channel = sampler.generator("tasks.purchase.payload.channel")
    gift = sampler.generator("tasks.purchase.payload.gift")
    payments_gap = DistributionConfig(dist="constant", value=1)
    # The random sequence the blueprint's draw uses
    payments_gap.stream = "arrival.payments"
    purchases_gap = DistributionConfig(dist="exponential", rate=1)
    purchases_gap.stream = "arrival.purchases"
    sent = {"payment": 0, "purchase": 0}
    ```

    `Sampler(seed=42)` supplies every draw: a random generator for each payload field and for the gap. The `stream` line names the random sequence the blueprint uses for this arrival, so the three forms draw the same gaps.

### Choose and Draw

Each field of a payload names one generator:

- [`weighted`](../../generators/choices/weighted.md) chooses from `options` in proportion to their weights, so a payment is paid by card 60 times in 100.
- [`uniform`](../../generators/random-numbers/uniform.md) draws a price between 1 and 250, and `decimals: 2` rounds it to cents.
- [`integer`](../../generators/random-numbers/integer.md) draws a whole number of items from 1 to 5, both ends included.
- [`normal`](../../generators/random-numbers/normal.md) draws a delivery time around 120 minutes with a standard deviation of 15.
- [`one_of`](../../generators/choices/one-of.md) chooses a channel with equal chance.
- [`boolean`](../../generators/random-numbers/boolean.md) with `p: 0.1` is true for 1 purchase in 10.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/choices_and_numbers.yaml:tasks" hl_lines="5 9 10 11 12 13"
    tasks:
      payment:
        payload:
          method:
            gen: weighted
            options: {card: 60, cash: 40}
      purchase:
        payload:
          price: {gen: uniform, low: 1, high: 250, decimals: 2}
          items: {gen: integer, bounds: [1, 5]}
          delivery_minutes: {gen: normal, mean: 120, std: 15, decimals: 1}
          channel: {gen: one_of, choices: [web, app, store]}
          gift: {gen: boolean, p: 0.1}
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/choices_and_numbers_builder.py:payload"
    payment_payload = Template(
        {"method": {"gen": "weighted", "options": {"card": 60, "cash": 40}}},
        name="tasks.payment",
    )

    @app.task(name="payment")
    def payment(task_id):
        """Gives one payment record."""
        return payment_payload

    purchase_payload = Template(
        {
            "price": {"gen": "uniform", "low": 1, "high": 250, "decimals": 2},
            "items": {"gen": "integer", "bounds": [1, 5]},
            "delivery_minutes": {
                "gen": "normal",
                "mean": 120,
                "std": 15,
                "decimals": 1,
            },
            "channel": {"gen": "one_of", "choices": ["web", "app", "store"]},
            "gift": {"gen": "boolean", "p": 0.1},
        },
        name="tasks.purchase",
    )

    @app.task(name="purchase")
    def purchase(task_id):
        """Gives one purchase record."""
        return purchase_payload
    ```

=== "Low-level"

    ```python title="docs/snippets/generate-data/choices_and_numbers_low_level.py:payload"
    def publish(task, payload):
        """Publishes a finished payload."""
        env.publish_event(
            f"{task}s-{sent[task] + 1}", payload, task=task, instance=sent[task]
        )
        sent[task] += 1

    def payments_source():
        while True:
            yield env.timeout(sampler.sample(payments_gap))
            # Weights 60 and 40: card takes the draws below 60 of 100
            card = method.random() * (60 + 40) < 60
            publish("payment", {"method": "card" if card else "cash"})

    def purchases_source():
        while True:
            yield env.timeout(sampler.sample(purchases_gap))
            publish(
                "purchase",
                {
                    "price": round(float(price.uniform(1, 250)), 2),
                    "items": int(items.integers(1, 5 + 1)),
                    "delivery_minutes": round(float(delivery.normal(120, 15)), 1),
                    "channel": CHANNELS[int(channel.integers(len(CHANNELS)))],
                    "gift": bool(gift.random() < 0.1),
                },
            )

    env.process(payments_source())
    env.process(purchases_source())
    ```

    The Low-level file writes the weighted choice out as the generator makes it: a point drawn in the total weight of 100 is card when it is below 60.

---

## Check the Result

Run the model for 30 simulated seconds and print its first 5 records. A payment is `card` or `cash`, and a purchase has a price from 1 to 250, 1 to 5 items, a delivery time in minutes, a channel and a gift flag.

=== "YAML"

    ```bash
    desflow run docs/snippets/generate-data/choices_and_numbers.yaml --sample 5
    ```

=== "Builder"

    ```bash
    python docs/snippets/generate-data/choices_and_numbers_builder.py
    ```

    This prints every record of the run as an `[EVT]` line with the same fields and values.

=== "Low-level"

    ```bash
    python docs/snippets/generate-data/choices_and_numbers_low_level.py
    ```

    This prints every record of the run as an `[EVT]` line with the same fields and values.

<!-- records: gd-choices-and-numbers -->
```text
{"stream_type":"event","sim_ts":1.0,"timestamp":"...","key":"payments-1","task":"payment","instance":0,"value":{"method":"card"}}
{"stream_type":"event","sim_ts":1.5718762452079822,"timestamp":"...","key":"purchases-1","task":"purchase","instance":0,"value":{"price":185.68,"items":4,"delivery_minutes":92.8,"channel":"web","gift":false}}
{"stream_type":"event","sim_ts":2.0,"timestamp":"...","key":"payments-2","task":"payment","instance":1,"value":{"method":"card"}}
{"stream_type":"event","sim_ts":2.3292479456462027,"timestamp":"...","key":"purchases-2","task":"purchase","instance":1,"value":{"price":68.32,"items":4,"delivery_minutes":115.8,"channel":"store","gift":false}}
{"stream_type":"event","sim_ts":3.0,"timestamp":"...","key":"payments-3","task":"payment","instance":2,"value":{"method":"card"}}
```

Over a long run the shares follow the settings. The docs test runs the model for 10,000 simulated seconds: about 60 payments in 100 are by card, the prices reach both ends of 1 to 250, the delivery times average 120 minutes with a standard deviation of 15, and about 1 purchase in 10 is a gift.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/generate-data/choices_and_numbers.yaml"
        # Choices and numbers: weighted choices and drawn numbers.
        #
        # A payment arrives every second. 60 percent are paid by card and the rest in cash. A
        # purchase arrives about once a second. A purchase has a price between 1 and 250, a
        # whole number of items from 1 to 5, a delivery time that is normal around 120 minutes,
        # a channel chosen with equal chance, and a `gift` flag that is true for 1 in 10.
        #
        # Needs: nothing.
        # Run it with: desflow run docs/snippets/generate-data/choices_and_numbers.yaml

        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42
          logical_start_time: 2026-01-01T00:00:00

        egress:
          - {type: Console}

        arrivals:
          payments: {dist: constant, value: 1, spawn: payment}
          purchases: {dist: exponential, rate: 1, spawn: purchase}

        tasks:
          payment:
            payload:
              method:
                gen: weighted
                options: {card: 60, cash: 40}
          purchase:
            payload:
              price: {gen: uniform, low: 1, high: 250, decimals: 2}
              items: {gen: integer, bounds: [1, 5]}
              delivery_minutes: {gen: normal, mean: 120, std: 15, decimals: 1}
              channel: {gen: one_of, choices: [web, app, store]}
              gift: {gen: boolean, p: 0.1}

        run: {until: 30}
        ```

    === "Builder"

        ```python title="docs/snippets/generate-data/choices_and_numbers_builder.py"
        """Choices and numbers: weighted choices and drawn numbers, builder API.

        The builder version of `choices_and_numbers.yaml`: a payment every second, 60 percent by
        card, and purchases with drawn numbers.

        Needs: nothing.
        Run it with: python docs/snippets/generate-data/choices_and_numbers_builder.py
        """

        import logging
        from datetime import datetime, timezone

        from desflow import ConsoleEgress, SimulationContext
        from desflow.values import Template

        UNTIL = 30


        def build() -> SimulationContext:
            """Builds the model."""
            app = SimulationContext(
                sim_id="Shop",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            app.add_egress(ConsoleEgress())
            app.add_arrival("payments", "constant", value=1, spawn="payment")
            app.add_arrival("purchases", "exponential", rate=1, spawn="purchase")
            payment_payload = Template(
                {"method": {"gen": "weighted", "options": {"card": 60, "cash": 40}}},
                name="tasks.payment",
            )

            @app.task(name="payment")
            def payment(task_id):
                """Gives one payment record."""
                return payment_payload

            purchase_payload = Template(
                {
                    "price": {"gen": "uniform", "low": 1, "high": 250, "decimals": 2},
                    "items": {"gen": "integer", "bounds": [1, 5]},
                    "delivery_minutes": {
                        "gen": "normal",
                        "mean": 120,
                        "std": 15,
                        "decimals": 1,
                    },
                    "channel": {"gen": "one_of", "choices": ["web", "app", "store"]},
                    "gift": {"gen": "boolean", "p": 0.1},
                },
                name="tasks.purchase",
            )

            @app.task(name="purchase")
            def purchase(task_id):
                """Gives one purchase record."""
                return purchase_payload


            return app


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            build().run(until=UNTIL)
        ```

    === "Low-level"

        ```python title="docs/snippets/generate-data/choices_and_numbers_low_level.py"
        """Choices and numbers: weighted choices and drawn numbers, low-level API.

        The low-level version of `choices_and_numbers.yaml`: plain SimPy on a
        `DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
        draw is made with `Sampler`, from the random sequence the blueprint's generator at that
        place uses, so the draws are the blueprint's. The weighted choice is written out as
        its generator makes it: a point drawn in the total weight of 100 is card when it is
        below 60.

        Needs: nothing.
        Run it with: python docs/snippets/generate-data/choices_and_numbers_low_level.py
        """

        import logging
        from datetime import datetime, timezone

        from desflow import (
            ConsoleEgress,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            Sampler,
        )

        UNTIL = 30
        START = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}
        CHANNELS = ["web", "app", "store"]


        def build(env: DynamicRealtimeEnvironment) -> None:
            """Starts the arrivals on `env`."""
            sampler = Sampler(seed=42)
            method = sampler.generator("tasks.payment.payload.method")
            price = sampler.generator("tasks.purchase.payload.price")
            items = sampler.generator("tasks.purchase.payload.items")
            delivery = sampler.generator("tasks.purchase.payload.delivery_minutes")
            channel = sampler.generator("tasks.purchase.payload.channel")
            gift = sampler.generator("tasks.purchase.payload.gift")
            payments_gap = DistributionConfig(dist="constant", value=1)
            # The random sequence the blueprint's draw uses
            payments_gap.stream = "arrival.payments"
            purchases_gap = DistributionConfig(dist="exponential", rate=1)
            purchases_gap.stream = "arrival.purchases"
            sent = {"payment": 0, "purchase": 0}

            def publish(task, payload):
                """Publishes a finished payload."""
                env.publish_event(
                    f"{task}s-{sent[task] + 1}", payload, task=task, instance=sent[task]
                )
                sent[task] += 1

            def payments_source():
                while True:
                    yield env.timeout(sampler.sample(payments_gap))
                    # Weights 60 and 40: card takes the draws below 60 of 100
                    card = method.random() * (60 + 40) < 60
                    publish("payment", {"method": "card" if card else "cash"})

            def purchases_source():
                while True:
                    yield env.timeout(sampler.sample(purchases_gap))
                    publish(
                        "purchase",
                        {
                            "price": round(float(price.uniform(1, 250)), 2),
                            "items": int(items.integers(1, 5 + 1)),
                            "delivery_minutes": round(float(delivery.normal(120, 15)), 1),
                            "channel": CHANNELS[int(channel.integers(len(CHANNELS)))],
                            "gift": bool(gift.random() < 0.1),
                        },
                    )

            env.process(payments_source())
            env.process(purchases_source())


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
