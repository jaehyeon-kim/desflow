# Look Up Other Records

Join records as they are made: an order names a customer written earlier, a payment reads the order that led to it, and a report summarises the last 10 orders.

---

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

    ```bash
    git clone https://github.com/jaehyeon-kim/desflow.git
    cd desflow
    ```

- [First Simulation](../../learn/first-model/first-simulation.md) explains arrivals, tasks and the records a run writes.
- [Task Flow](../../user-guide/model-parts/task-flow.md) explains `next`. [`lookup`](../../generators/related-records/lookup.md), [`upstream`](../../generators/related-records/upstream.md) and [`aggregate`](../../generators/related-records/aggregate.md) list the keys of the three generators.

---

## Steps

### Describe the Arrivals

Five customers are written when the run starts (`initial: 5`), and another about every 10 seconds. An order arrives about once a second, and a report every 10 seconds.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/look_up_other_records.yaml:arrivals" hl_lines="2"
    arrivals:
      customers: {initial: 5, dist: exponential, rate: 0.1, spawn: customer}
      orders: {dist: exponential, rate: 1, spawn: order}
      reports: {dist: constant, value: 10, spawn: report}
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/look_up_other_records_builder.py:arrival"
    app.add_arrival("customers", "exponential", rate=0.1, spawn="customer", initial=5)
    app.add_arrival("orders", "exponential", rate=1, spawn="order")
    app.add_arrival("reports", "constant", value=10, spawn="report")
    ```

=== "Low-level"

    ```python title="docs/snippets/generate-data/look_up_other_records_low_level.py:arrival"
    sampler = Sampler(seed=42)
    country = sampler.generator("tasks.customer.payload.country")
    pick = sampler.generator("tasks.order.payload.customer_id")
    total = sampler.generator("tasks.order.payload.total")
    customer_gap = DistributionConfig(dist="exponential", rate=0.1)
    # The random sequence the blueprint's draw uses
    customer_gap.stream = "arrival.customers"
    order_gap = DistributionConfig(dist="exponential", rate=1)
    order_gap.stream = "arrival.orders"
    report_gap = DistributionConfig(dist="constant", value=10)
    report_gap.stream = "arrival.reports"
    customer_rows: list = []  # the customers written so far, for the lookup
    order_rows: list = []  # the orders written so far, for the aggregate
    sent = {"customer": 0, "report": 0}
    ```

    `Sampler(seed=42)` supplies every draw: a random generator for each payload field and for the gap. The `stream` line names the random sequence the blueprint uses for this arrival, so the three forms draw the same gaps.

### Read Other Records

Three generators read the rows that other tasks have written:

- `lookup` gives an order the `customer_id` of a customer already written. The five customers written at the start make sure that every order finds one. An order that starts before any customer has a row is dropped, so no record is written for it.
- `upstream` gives the payment the `order_id` and `total` of the order that `next: payment` routed to it.
- `aggregate` gives a report the mean and the largest total of the last 10 orders (`last: 10`).

=== "YAML"

    ```yaml title="docs/snippets/generate-data/look_up_other_records.yaml:tasks" hl_lines="9 14 18"
    tasks:
      customer:
        payload:
          customer_id: {gen: sequence, start: 1}
          country: {gen: one_of, choices: [AU, NZ, SG]}
      order:
        payload:
          order_id: {gen: sequence, start: 1}
          customer_id: {gen: lookup, task: customer, path: customer_id}
          total: {gen: uniform, low: 5, high: 200, decimals: 2}
        next: payment
      payment:
        payload:
          order_id: {gen: upstream, path: order_id}
          amount: {gen: upstream, path: total}
      report:
        payload:
          mean_total: {gen: aggregate, task: order, path: total, last: 10, function: mean}
          largest_total: {gen: aggregate, task: order, path: total, last: 10, function: max}
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/look_up_other_records_builder.py:payload"
    customer_payload = Template(
        {
            "customer_id": {"gen": "sequence", "start": 1},
            "country": {"gen": "one_of", "choices": ["AU", "NZ", "SG"]},
        },
        name="tasks.customer",
    )
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"},
            "total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
        },
        name="tasks.order",
    )
    payment_payload = Template(
        {
            "order_id": {"gen": "upstream", "path": "order_id"},
            "amount": {"gen": "upstream", "path": "total"},
        },
        name="tasks.payment",
    )
    report_payload = Template(
        {
            "mean_total": {
                "gen": "aggregate",
                "task": "order",
                "path": "total",
                "last": 10,
                "function": "mean",
            },
            "largest_total": {
                "gen": "aggregate",
                "task": "order",
                "path": "total",
                "last": 10,
                "function": "max",
            },
        },
        name="tasks.report",
    )

    # A task that something looks up or aggregates keeps its rows: `history` is how
    # many. A builder task needs it because a `Template` payload is not searched for
    # them.
    @app.task(name="customer", history=1_000_000)
    def customer(task_id):
        """Gives one customer record."""
        return customer_payload

    @app.task(name="order", next="payment", history=10)
    def order(task_id):
        """Gives one order record."""
        return order_payload

    @app.task(name="payment")
    def payment(task_id):
        """Gives one payment record."""
        return payment_payload

    @app.task(name="report")
    def report(task_id):
        """Gives one report record."""
        return report_payload
    ```

    A task that something looks up or aggregates keeps its rows, and `history=` says how many. A Builder task needs it, because a `Template` payload is not searched for them. A blueprint keeps 1,000,000 rows by default.

=== "Low-level"

    ```python title="docs/snippets/generate-data/look_up_other_records_low_level.py:payload"
    def write_customer(key):
        """Publishes the next customer and keeps its row."""
        row = {
            "customer_id": len(customer_rows) + 1,
            "country": COUNTRIES[int(country.integers(len(COUNTRIES)))],
        }
        customer_rows.append(row)
        env.publish_event(key, row, task="customer", instance=sent["customer"])
        sent["customer"] += 1

    def customers():
        for number in itertools.count(6):  # 1 to 5 exist at the start
            yield env.timeout(sampler.sample(customer_gap))
            write_customer(f"customers-{number}")

    def orders():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(order_gap))
            key, instance = f"orders-{number}", number - 1
            # The lookup: one row of the customers written so far, chosen at random
            customer = customer_rows[int(pick.integers(len(customer_rows)))]
            order = {
                "order_id": number,
                "customer_id": customer["customer_id"],
                "total": round(float(total.uniform(5, 200)), 2),
            }
            order_rows.append(order)
            env.publish_event(key, order, task="order", instance=instance)
            # The upstream: the payment reads the order it follows
            payment = {"order_id": order["order_id"], "amount": order["total"]}
            env.publish_event(key, payment, task="payment", instance=instance)

    def reports():
        while True:
            yield env.timeout(sampler.sample(report_gap))
            # The aggregate: the last 10 orders, newest first, as the blueprint does
            recent = [row["total"] for row in reversed(order_rows[-10:])]
            report = {
                "mean_total": sum(recent) / len(recent),
                "largest_total": max(recent),
            }
            env.publish_event(
                f"reports-{sent['report'] + 1}",
                report,
                task="report",
                instance=sent["report"],
            )
            sent["report"] += 1

    def first(number):
        yield env.timeout(0)
        write_customer(f"customers-{number}")

    env.process(customers())
    env.process(orders())
    env.process(reports())
    for number in range(1, 6):
        env.process(first(number))
    ```

    A Low-level script keeps the rows that a `lookup` or an `aggregate` reads in lists of its own, and picks a customer and takes the last 10 orders itself.

---

## Check the Result

Run the model for 60 simulated seconds and print its first 40 records. The block keeps three of them: the first order, the payment that follows it and the first report. The order names a customer that exists, the payment repeats the order's id and total as `amount`, and the report, at 10 seconds, gives the mean and the largest total of the 7 orders written by then, because fewer than 10 have arrived.

=== "YAML"

    ```bash
    desflow run docs/snippets/generate-data/look_up_other_records.yaml --sample 40
    ```

=== "Builder"

    ```bash
    python docs/snippets/generate-data/look_up_other_records_builder.py
    ```

    This prints every record of the run as an `[EVT]` line with the same fields and values.

=== "Low-level"

    ```bash
    python docs/snippets/generate-data/look_up_other_records_low_level.py
    ```

    This prints every record of the run as an `[EVT]` line with the same fields and values.

<!-- records: gd-look-up-other-records -->
```text
{"stream_type":"event","sim_ts":0.3141556656469718,"timestamp":"...","key":"orders-1","task":"order","instance":0,"value":{"order_id":1,"customer_id":5,"total":161.55}}
{"stream_type":"event","sim_ts":0.3141556656469718,"timestamp":"...","key":"orders-1","task":"payment","instance":0,"value":{"order_id":1,"amount":161.55}}
{"stream_type":"event","sim_ts":10.0,"timestamp":"...","key":"reports-1","task":"report","instance":0,"value":{"mean_total":105.08285714285715,"largest_total":177.63}}
```

The docs test checks every order against the customers written by then, every payment against its order, and every report against the last 10 orders.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/generate-data/look_up_other_records.yaml"
        # Look up other records: an order names a customer, a payment reads its order, and a
        # report summarises the recent orders.
        #
        # Customers are written at the start and about every 10 seconds. `lookup` gives each
        # order the id of a customer already written. `next` routes each order to a payment, and
        # `upstream` gives the payment the order's id and total. A report every 10 seconds uses
        # `aggregate` to give the mean and the largest total of the last 10 orders.
        #
        # Needs: nothing.
        # Run it with: desflow run docs/snippets/generate-data/look_up_other_records.yaml

        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42
          logical_start_time: 2026-01-01T00:00:00

        egress:
          - {type: Console}

        arrivals:
          customers: {initial: 5, dist: exponential, rate: 0.1, spawn: customer}
          orders: {dist: exponential, rate: 1, spawn: order}
          reports: {dist: constant, value: 10, spawn: report}

        tasks:
          customer:
            payload:
              customer_id: {gen: sequence, start: 1}
              country: {gen: one_of, choices: [AU, NZ, SG]}
          order:
            payload:
              order_id: {gen: sequence, start: 1}
              customer_id: {gen: lookup, task: customer, path: customer_id}
              total: {gen: uniform, low: 5, high: 200, decimals: 2}
            next: payment
          payment:
            payload:
              order_id: {gen: upstream, path: order_id}
              amount: {gen: upstream, path: total}
          report:
            payload:
              mean_total: {gen: aggregate, task: order, path: total, last: 10, function: mean}
              largest_total: {gen: aggregate, task: order, path: total, last: 10, function: max}

        run: {until: 60}
        ```

    === "Builder"

        ```python title="docs/snippets/generate-data/look_up_other_records_builder.py"
        """Look up other records: an order names a customer, a payment reads its order, and a
        report summarises the recent orders, builder API.

        The builder version of `look_up_other_records.yaml`: customers are written at the start
        and about every 10 seconds. `lookup` gives each order the id of a customer already
        written. `next` routes each order to a payment, and `upstream` gives the payment the
        order's id and total. A report every 10 seconds uses `aggregate` to give the mean and
        the largest total of the last 10 orders.

        Needs: nothing.
        Run it with: python docs/snippets/generate-data/look_up_other_records_builder.py
        """

        import logging
        from datetime import datetime, timezone

        from desflow import ConsoleEgress, SimulationContext
        from desflow.values import Template

        UNTIL = 60


        def build() -> SimulationContext:
            """Builds the shop."""
            app = SimulationContext(
                sim_id="Shop",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            app.add_egress(ConsoleEgress())
            app.add_arrival("customers", "exponential", rate=0.1, spawn="customer", initial=5)
            app.add_arrival("orders", "exponential", rate=1, spawn="order")
            app.add_arrival("reports", "constant", value=10, spawn="report")
            customer_payload = Template(
                {
                    "customer_id": {"gen": "sequence", "start": 1},
                    "country": {"gen": "one_of", "choices": ["AU", "NZ", "SG"]},
                },
                name="tasks.customer",
            )
            order_payload = Template(
                {
                    "order_id": {"gen": "sequence", "start": 1},
                    "customer_id": {"gen": "lookup", "task": "customer", "path": "customer_id"},
                    "total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
                },
                name="tasks.order",
            )
            payment_payload = Template(
                {
                    "order_id": {"gen": "upstream", "path": "order_id"},
                    "amount": {"gen": "upstream", "path": "total"},
                },
                name="tasks.payment",
            )
            report_payload = Template(
                {
                    "mean_total": {
                        "gen": "aggregate",
                        "task": "order",
                        "path": "total",
                        "last": 10,
                        "function": "mean",
                    },
                    "largest_total": {
                        "gen": "aggregate",
                        "task": "order",
                        "path": "total",
                        "last": 10,
                        "function": "max",
                    },
                },
                name="tasks.report",
            )

            # A task that something looks up or aggregates keeps its rows: `history` is how
            # many. A builder task needs it because a `Template` payload is not searched for
            # them.
            @app.task(name="customer", history=1_000_000)
            def customer(task_id):
                """Gives one customer record."""
                return customer_payload

            @app.task(name="order", next="payment", history=10)
            def order(task_id):
                """Gives one order record."""
                return order_payload

            @app.task(name="payment")
            def payment(task_id):
                """Gives one payment record."""
                return payment_payload

            @app.task(name="report")
            def report(task_id):
                """Gives one report record."""
                return report_payload


            return app


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            build().run(until=UNTIL)
        ```

    === "Low-level"

        ```python title="docs/snippets/generate-data/look_up_other_records_low_level.py"
        """Look up other records: an order names a customer, a payment reads its order, and a
        report summarises the recent orders, low-level API.

        The low-level version of `look_up_other_records.yaml`: plain SimPy on a
        `DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
        draw is made with `Sampler`, from the random sequence the blueprint's generator at that
        place uses, so the draws are the blueprint's. The script keeps the customers and the
        orders in lists. The lookup is one draw of a row of the customers, the upstream is the
        order the payment follows, and the aggregate is the mean and the largest total of the
        last 10 orders.

        Needs: nothing.
        Run it with: python docs/snippets/generate-data/look_up_other_records_low_level.py
        """

        import itertools
        import logging
        from datetime import datetime, timezone

        from desflow import (
            ConsoleEgress,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            Sampler,
        )

        UNTIL = 60
        START = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}
        COUNTRIES = ["AU", "NZ", "SG"]


        def build(env: DynamicRealtimeEnvironment) -> None:
            """Starts the customers, the orders and the reports on `env`."""
            sampler = Sampler(seed=42)
            country = sampler.generator("tasks.customer.payload.country")
            pick = sampler.generator("tasks.order.payload.customer_id")
            total = sampler.generator("tasks.order.payload.total")
            customer_gap = DistributionConfig(dist="exponential", rate=0.1)
            # The random sequence the blueprint's draw uses
            customer_gap.stream = "arrival.customers"
            order_gap = DistributionConfig(dist="exponential", rate=1)
            order_gap.stream = "arrival.orders"
            report_gap = DistributionConfig(dist="constant", value=10)
            report_gap.stream = "arrival.reports"
            customer_rows: list = []  # the customers written so far, for the lookup
            order_rows: list = []  # the orders written so far, for the aggregate
            sent = {"customer": 0, "report": 0}

            def write_customer(key):
                """Publishes the next customer and keeps its row."""
                row = {
                    "customer_id": len(customer_rows) + 1,
                    "country": COUNTRIES[int(country.integers(len(COUNTRIES)))],
                }
                customer_rows.append(row)
                env.publish_event(key, row, task="customer", instance=sent["customer"])
                sent["customer"] += 1

            def customers():
                for number in itertools.count(6):  # 1 to 5 exist at the start
                    yield env.timeout(sampler.sample(customer_gap))
                    write_customer(f"customers-{number}")

            def orders():
                for number in itertools.count(1):
                    yield env.timeout(sampler.sample(order_gap))
                    key, instance = f"orders-{number}", number - 1
                    # The lookup: one row of the customers written so far, chosen at random
                    customer = customer_rows[int(pick.integers(len(customer_rows)))]
                    order = {
                        "order_id": number,
                        "customer_id": customer["customer_id"],
                        "total": round(float(total.uniform(5, 200)), 2),
                    }
                    order_rows.append(order)
                    env.publish_event(key, order, task="order", instance=instance)
                    # The upstream: the payment reads the order it follows
                    payment = {"order_id": order["order_id"], "amount": order["total"]}
                    env.publish_event(key, payment, task="payment", instance=instance)

            def reports():
                while True:
                    yield env.timeout(sampler.sample(report_gap))
                    # The aggregate: the last 10 orders, newest first, as the blueprint does
                    recent = [row["total"] for row in reversed(order_rows[-10:])]
                    report = {
                        "mean_total": sum(recent) / len(recent),
                        "largest_total": max(recent),
                    }
                    env.publish_event(
                        f"reports-{sent['report'] + 1}",
                        report,
                        task="report",
                        instance=sent["report"],
                    )
                    sent["report"] += 1

            def first(number):
                yield env.timeout(0)
                write_customer(f"customers-{number}")

            env.process(customers())
            env.process(orders())
            env.process(reports())
            for number in range(1, 6):
                env.process(first(number))


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
