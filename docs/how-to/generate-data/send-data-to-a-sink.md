# Send Data to a Sink

Generate orders and write them to the console, a Kafka topic, a PostgreSQL table or a JSON Lines file. The model is the same for every sink, and only the egress changes.

---

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

    ```bash
    git clone https://github.com/jaehyeon-kim/desflow.git
    cd desflow
    ```

- [First Simulation](../../learn/first-model/first-simulation.md) explains arrivals, tasks and the records a run writes.
- [Streaming Out](../../learn/running-live/streaming-out.md) shows the record each run writes, and [Egress, Batching and Delivery](../../user-guide/live-runs-and-connectors/egress.md) lists every sink and its settings.
- The console needs nothing. The Kafka sink needs `pip install "desflow[kafka]"` and a broker (`odctl up kafka-lite`), the PostgreSQL sink needs `pip install "desflow[postgres]"` and a database (`odctl up postgres`), and the file sink needs `pip install "desflow[parquet]"`. The Builder and Low-level files import all four sinks, so they need the `kafka` and `postgres` extras even for the console, and the `parquet` extra for the file sink.

---

## Steps

### Describe the Arrival

An arrival starts one order about every second: `dist: exponential` with `rate: 1` draws gaps that average 1 second, and `spawn: order` names the task each arrival starts.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/send_data_to_a_sink.yaml:arrivals" hl_lines="2"
    arrivals:
      orders: {dist: exponential, rate: 1, spawn: order}
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/send_data_to_a_sink_builder.py:arrival"
    app.add_arrival("orders", "exponential", rate=1, spawn="order")
    ```

=== "Low-level"

    ```python title="docs/snippets/generate-data/send_data_to_a_sink_low_level.py:arrival"
    sampler = Sampler(seed=42)
    customer = sampler.generator("tasks.order.payload.customer_id")
    total = sampler.generator("tasks.order.payload.total")
    orders_gap = DistributionConfig(dist="exponential", rate=1)
    # The random sequence the blueprint's draw uses
    orders_gap.stream = "arrival.orders"
    ```

    `Sampler(seed=42)` supplies every draw: a random generator for each payload field and for the gap. The `stream` line names the random sequence the blueprint uses for this arrival, so the three forms draw the same gaps.

### Generate the Payload

The `payload` of a task maps each field to a generator. [`sequence`](../../generators/text-and-ids/sequence.md) numbers the orders from 1, [`integer`](../../generators/random-numbers/integer.md) draws a customer from 1 to 100, and [`uniform`](../../generators/random-numbers/uniform.md) draws a total from 5 to 200 with 2 decimals.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/send_data_to_a_sink.yaml:tasks" hl_lines="4 5 6"
    tasks:
      order:
        payload:
          order_id: {gen: sequence, start: 1}
          customer_id: {gen: integer, bounds: [1, 100]}
          total: {gen: uniform, low: 5, high: 200, decimals: 2}
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/send_data_to_a_sink_builder.py:payload"
    order_payload = Template(
        {
            "order_id": {"gen": "sequence", "start": 1},
            "customer_id": {"gen": "integer", "bounds": [1, 100]},
            "total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
        },
        name="tasks.order",
    )

    @app.task(name="order")
    def order(task_id):
        """Gives one order record."""
        return order_payload
    ```

=== "Low-level"

    ```python title="docs/snippets/generate-data/send_data_to_a_sink_low_level.py:payload"
    def orders_source():
        number = 0
        while True:
            yield env.timeout(sampler.sample(orders_gap))
            number += 1
            payload = {
                "order_id": number,
                "customer_id": int(customer.integers(1, 100 + 1)),
                "total": round(float(total.uniform(5, 200)), 2),
            }
            env.publish_event(
                f"orders-{number}", payload, task="order", instance=number - 1
            )

    env.process(orders_source())
    ```

    A Low-level script has no payload mapping. It draws each value and passes the finished dictionary to `env.publish_event`.

### Choose the Sink

The egress decides where the records go. `send_data_to_kafka.yaml`, `send_data_to_postgres.yaml` and `send_data_to_a_file.yaml` are the console file with another `egress` section, and each Python file holds the four sinks as functions.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/send_data_to_a_sink.yaml:egress"
    egress:
      - {type: Console}
    ```

    ```yaml title="docs/snippets/generate-data/send_data_to_kafka.yaml:egress"
    egress:
      - type: Kafka
        config:
          bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
          event_topic: orders
    ```

    ```yaml title="docs/snippets/generate-data/send_data_to_postgres.yaml:egress"
    egress:
      - type: Postgres
        config:
          connection_dsn: postgresql://user:password@127.0.0.1:5432/odctl
          tables:
            orders:
              columns: {order_id: INT, customer_id: INT, total: REAL}
              primary_key: order_id
    ```

    ```yaml title="docs/snippets/generate-data/send_data_to_a_file.yaml:egress"
    egress:
      - type: Jsonl
        config:
          default_path: ${DEST_PATH:-data}/orders.jsonl
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/send_data_to_a_sink_builder.py:sinks"
    def console_sink() -> ConsoleEgress:
        """Prints each record."""
        return ConsoleEgress()


    def kafka_sink() -> KafkaEgress:
        """Writes the records to the topic `orders`."""
        return KafkaEgress(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            event_topic="orders",
        )


    def postgres_sink() -> PostgresEgress:
        """Writes each record as a row of the table `orders`."""
        return PostgresEgress(
            DSN,
            tables={
                "orders": {
                    "columns": {"order_id": "INT", "customer_id": "INT", "total": "REAL"},
                    "primary_key": "order_id",
                }
            },
        )


    def file_sink() -> JsonlStorageEgress:
        """Writes the records to a JSON Lines file."""
        return JsonlStorageEgress(
            default_path=f"{os.environ.get('DEST_PATH', 'data')}/orders.jsonl"
        )
    ```

    `build(sink)` takes one of these functions and uses the console when it gets none, so `build(kafka_sink).run(until=UNTIL)` writes to Kafka. The `SINK` environment variable of Check the Result picks one of them when the file runs as a script.

=== "Low-level"

    ```python title="docs/snippets/generate-data/send_data_to_a_sink_low_level.py:sinks"
    def console_sink() -> ConsoleEgress:
        """Prints each record."""
        return ConsoleEgress()


    def kafka_sink() -> KafkaEgress:
        """Writes the records to the topic `orders`."""
        return KafkaEgress(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            event_topic="orders",
        )


    def postgres_sink() -> PostgresEgress:
        """Writes each record as a row of the table `orders`."""
        return PostgresEgress(
            DSN,
            tables={
                "orders": {
                    "columns": {"order_id": "INT", "customer_id": "INT", "total": "REAL"},
                    "primary_key": "order_id",
                }
            },
        )


    def file_sink() -> JsonlStorageEgress:
        """Writes the records to a JSON Lines file."""
        return JsonlStorageEgress(
            default_path=f"{os.environ.get('DEST_PATH', 'data')}/orders.jsonl"
        )
    ```

    `run(sink)` takes one of these functions in the same way and calls `env.setup_egress` with it. The `SINK` environment variable of Check the Result picks one of them when the file runs as a script.

---

## Check the Result

Run the console model for 10 simulated seconds and print its first 5 records. Orders count from 1, every customer is from 1 to 100 and every total is from 5 to 200.

=== "YAML"

    ```bash
    desflow run docs/snippets/generate-data/send_data_to_a_sink.yaml --sample 5
    desflow run docs/snippets/generate-data/send_data_to_kafka.yaml
    desflow run docs/snippets/generate-data/send_data_to_postgres.yaml
    desflow run docs/snippets/generate-data/send_data_to_a_file.yaml
    ```

    The first command prints the records. Each of the other three runs one sink and prints log lines only.

    Read the records back from the sink. Run these after the run, with the service from [Before You Start](#before-you-start) still up for Kafka and PostgreSQL:

    ```bash
    # Kafka: the first three records of the topic orders
    docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 3

    # PostgreSQL: the number of rows of the table orders
    docker exec postgres psql -U user -d odctl -c "SELECT count(*) FROM orders;"

    # File: the number of records in the files of the folder data
    cat data/orders_*.jsonl | wc -l
    ```

    A run of 10 simulated seconds writes 10 records, so the PostgreSQL count and the file count are 10 after one run. A second run appends to the topic and adds a new file to the folder, and PostgreSQL skips an order number the table already holds, so the count stays at 10 until you drop the table.

=== "Builder"

    ```bash
    python docs/snippets/generate-data/send_data_to_a_sink_builder.py
    SINK=kafka python docs/snippets/generate-data/send_data_to_a_sink_builder.py
    SINK=postgres python docs/snippets/generate-data/send_data_to_a_sink_builder.py
    SINK=file python docs/snippets/generate-data/send_data_to_a_sink_builder.py
    ```

    `SINK` names one of the four functions of the Steps section, and the console is the default. The console run prints every record of the run as an `[EVT]` line with the same fields and values. The other three print log lines only.

    Read the records back from the sink. Run these after the run, with the service from [Before You Start](#before-you-start) still up for Kafka and PostgreSQL:

    ```bash
    # Kafka: the first three records of the topic orders
    docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 3

    # PostgreSQL: the number of rows of the table orders
    docker exec postgres psql -U user -d odctl -c "SELECT count(*) FROM orders;"

    # File: the number of records in the files of the folder data
    cat data/orders_*.jsonl | wc -l
    ```

    A run of 10 simulated seconds writes 10 records, so the PostgreSQL count and the file count are 10 after one run. A second run appends to the topic and adds a new file to the folder, and PostgreSQL skips an order number the table already holds, so the count stays at 10 until you drop the table.

=== "Low-level"

    ```bash
    python docs/snippets/generate-data/send_data_to_a_sink_low_level.py
    SINK=kafka python docs/snippets/generate-data/send_data_to_a_sink_low_level.py
    SINK=postgres python docs/snippets/generate-data/send_data_to_a_sink_low_level.py
    SINK=file python docs/snippets/generate-data/send_data_to_a_sink_low_level.py
    ```

    `SINK` names one of the four functions of the Steps section, and the console is the default. The console run prints every record of the run as an `[EVT]` line with the same fields and values. The other three print log lines only.

    Read the records back from the sink. Run these after the run, with the service from [Before You Start](#before-you-start) still up for Kafka and PostgreSQL:

    ```bash
    # Kafka: the first three records of the topic orders
    docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 3

    # PostgreSQL: the number of rows of the table orders
    docker exec postgres psql -U user -d odctl -c "SELECT count(*) FROM orders;"

    # File: the number of records in the files of the folder data
    cat data/orders_*.jsonl | wc -l
    ```

    A run of 10 simulated seconds writes 10 records, so the PostgreSQL count and the file count are 10 after one run. A second run appends to the topic and adds a new file to the folder, and PostgreSQL skips an order number the table already holds, so the count stays at 10 until you drop the table.

<!-- records: gd-send-data-to-a-sink -->
```text
{"stream_type":"event","sim_ts":1.2761899383482334,"timestamp":"...","key":"orders-1","task":"order","instance":0,"value":{"order_id":1,"customer_id":71,"total":100.17}}
{"stream_type":"event","sim_ts":1.3479226635723796,"timestamp":"...","key":"orders-2","task":"order","instance":1,"value":{"order_id":2,"customer_id":40,"total":126.8}}
{"stream_type":"event","sim_ts":1.8686592357447462,"timestamp":"...","key":"orders-3","task":"order","instance":2,"value":{"order_id":3,"customer_id":59,"total":48.33}}
{"stream_type":"event","sim_ts":2.0097703062717214,"timestamp":"...","key":"orders-4","task":"order","instance":3,"value":{"order_id":4,"customer_id":40,"total":26.1}}
{"stream_type":"event","sim_ts":3.09804773711297,"timestamp":"...","key":"orders-5","task":"order","instance":4,"value":{"order_id":5,"customer_id":76,"total":167.01}}
```

The Kafka, PostgreSQL and file models write these same records to their sink. Kafka receives them on the topic `orders` and PostgreSQL writes a row of the table `orders` for each, with `order_id` as the primary key. The file model writes the records as flat rows of JSON Lines files named `data/orders_<8 hex digits>.jsonl`, one file for each batch, as [Egress, Batching and Delivery](../../user-guide/live-runs-and-connectors/egress.md) sets out for file names. `DEST_PATH` sets another folder.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/generate-data/send_data_to_a_sink.yaml"
        # Send data to a sink: one arrival, one generated payload, written to the console.
        #
        # An order arrives about once a second. Its record holds an order number, a customer
        # and a total. The Console egress prints each record. The files send_data_to_kafka.yaml,
        # send_data_to_postgres.yaml and send_data_to_a_file.yaml are this file with another
        # egress.
        #
        # Needs: nothing.
        # Run it with: desflow run docs/snippets/generate-data/send_data_to_a_sink.yaml

        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42
          logical_start_time: 2026-01-01T00:00:00

        egress:
          - {type: Console}

        arrivals:
          orders: {dist: exponential, rate: 1, spawn: order}

        tasks:
          order:
            payload:
              order_id: {gen: sequence, start: 1}
              customer_id: {gen: integer, bounds: [1, 100]}
              total: {gen: uniform, low: 5, high: 200, decimals: 2}

        run: {until: 10}
        ```

        ```yaml title="docs/snippets/generate-data/send_data_to_kafka.yaml"
        # Send data to a sink: the orders of send_data_to_a_sink.yaml, written to a Kafka topic.
        #
        # Only the egress differs from that file.
        #
        # Needs: a broker, with odctl up kafka-lite, and the kafka extra.
        # KAFKA_BOOTSTRAP_SERVERS sets the broker address.
        # Run it with: desflow run docs/snippets/generate-data/send_data_to_kafka.yaml

        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42
          logical_start_time: 2026-01-01T00:00:00

        egress:
          - type: Kafka
            config:
              bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
              event_topic: orders

        arrivals:
          orders: {dist: exponential, rate: 1, spawn: order}

        tasks:
          order:
            payload:
              order_id: {gen: sequence, start: 1}
              customer_id: {gen: integer, bounds: [1, 100]}
              total: {gen: uniform, low: 5, high: 200, decimals: 2}

        run: {until: 10}
        ```

        ```yaml title="docs/snippets/generate-data/send_data_to_postgres.yaml"
        # Send data to a sink: the orders of send_data_to_a_sink.yaml, written to a PostgreSQL
        # table.
        #
        # Only the egress differs from that file.
        #
        # Needs: a database, with odctl up postgres, and the postgres extra.
        # Run it with: desflow run docs/snippets/generate-data/send_data_to_postgres.yaml

        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42
          logical_start_time: 2026-01-01T00:00:00

        egress:
          - type: Postgres
            config:
              connection_dsn: postgresql://user:password@127.0.0.1:5432/odctl
              tables:
                orders:
                  columns: {order_id: INT, customer_id: INT, total: REAL}
                  primary_key: order_id

        arrivals:
          orders: {dist: exponential, rate: 1, spawn: order}

        tasks:
          order:
            payload:
              order_id: {gen: sequence, start: 1}
              customer_id: {gen: integer, bounds: [1, 100]}
              total: {gen: uniform, low: 5, high: 200, decimals: 2}

        run: {until: 10}
        ```

        ```yaml title="docs/snippets/generate-data/send_data_to_a_file.yaml"
        # Send data to a sink: the orders of send_data_to_a_sink.yaml, written to a JSON
        # Lines file.
        #
        # Only the egress differs from that file.
        #
        # Needs: the parquet extra, which holds the file writers. DEST_PATH sets the folder
        # (data by default).
        # Run it with: desflow run docs/snippets/generate-data/send_data_to_a_file.yaml

        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42
          logical_start_time: 2026-01-01T00:00:00

        egress:
          - type: Jsonl
            config:
              default_path: ${DEST_PATH:-data}/orders.jsonl

        arrivals:
          orders: {dist: exponential, rate: 1, spawn: order}

        tasks:
          order:
            payload:
              order_id: {gen: sequence, start: 1}
              customer_id: {gen: integer, bounds: [1, 100]}
              total: {gen: uniform, low: 5, high: 200, decimals: 2}

        run: {until: 10}
        ```

    === "Builder"

        ```python title="docs/snippets/generate-data/send_data_to_a_sink_builder.py"
        """Send data to a sink: one arrival, one generated payload, builder API.

        The builder version of `send_data_to_a_sink.yaml`: an order about every second,
        written to the console, and to Kafka, PostgreSQL or a JSON Lines file when `sink` is
        another function of the file.

        Needs: the kafka and postgres extras, because the file imports every sink, and the
        parquet extra for the file sink. The console needs no service. The Kafka sink needs a
        broker (odctl up kafka-lite) and the PostgreSQL sink a database (odctl up postgres).
        Run it with: python docs/snippets/generate-data/send_data_to_a_sink_builder.py
        SINK sets the sink: console (the default), kafka, postgres or file.
        """

        import logging
        import os
        from datetime import datetime, timezone

        from desflow import (
            ConsoleEgress,
            JsonlStorageEgress,
            KafkaEgress,
            PostgresEgress,
            SimulationContext,
        )
        from desflow.values import Template

        UNTIL = 10
        DSN = "postgresql://user:password@127.0.0.1:5432/odctl"


        def console_sink() -> ConsoleEgress:
            """Prints each record."""
            return ConsoleEgress()


        def kafka_sink() -> KafkaEgress:
            """Writes the records to the topic `orders`."""
            return KafkaEgress(
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                event_topic="orders",
            )


        def postgres_sink() -> PostgresEgress:
            """Writes each record as a row of the table `orders`."""
            return PostgresEgress(
                DSN,
                tables={
                    "orders": {
                        "columns": {"order_id": "INT", "customer_id": "INT", "total": "REAL"},
                        "primary_key": "order_id",
                    }
                },
            )


        def file_sink() -> JsonlStorageEgress:
            """Writes the records to a JSON Lines file."""
            return JsonlStorageEgress(
                default_path=f"{os.environ.get('DEST_PATH', 'data')}/orders.jsonl"
            )


        SINKS = {
            "console": console_sink,
            "kafka": kafka_sink,
            "postgres": postgres_sink,
            "file": file_sink,
        }


        def build(sink=console_sink) -> SimulationContext:
            """Builds the model, with the egress that `sink` makes."""
            app = SimulationContext(
                sim_id="Shop",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            app.add_egress(sink())
            app.add_arrival("orders", "exponential", rate=1, spawn="order")
            order_payload = Template(
                {
                    "order_id": {"gen": "sequence", "start": 1},
                    "customer_id": {"gen": "integer", "bounds": [1, 100]},
                    "total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2},
                },
                name="tasks.order",
            )

            @app.task(name="order")
            def order(task_id):
                """Gives one order record."""
                return order_payload


            return app


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            build(SINKS[os.environ.get("SINK", "console")]).run(until=UNTIL)
        ```

    === "Low-level"

        ```python title="docs/snippets/generate-data/send_data_to_a_sink_low_level.py"
        """Send data to a sink: one arrival, one generated payload, low-level API.

        The low-level version of `send_data_to_a_sink.yaml`: plain SimPy on a
        `DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
        draw is made with `Sampler`, from the random sequence the blueprint's generator at that
        place uses, so the draws are the blueprint's. The four sinks are functions, and `run`
        takes one of them.

        Needs: the kafka and postgres extras, because the file imports every sink, and the
        parquet extra for the file sink. The console needs no service. The Kafka sink needs a
        broker (odctl up kafka-lite) and the PostgreSQL sink a database (odctl up postgres).
        Run it with: python docs/snippets/generate-data/send_data_to_a_sink_low_level.py
        SINK sets the sink: console (the default), kafka, postgres or file.
        """

        import logging
        import os
        from datetime import datetime, timezone

        from desflow import (
            ConsoleEgress,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            JsonlStorageEgress,
            KafkaEgress,
            PostgresEgress,
            Sampler,
        )

        UNTIL = 10
        DSN = "postgresql://user:password@127.0.0.1:5432/odctl"


        def console_sink() -> ConsoleEgress:
            """Prints each record."""
            return ConsoleEgress()


        def kafka_sink() -> KafkaEgress:
            """Writes the records to the topic `orders`."""
            return KafkaEgress(
                bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                event_topic="orders",
            )


        def postgres_sink() -> PostgresEgress:
            """Writes each record as a row of the table `orders`."""
            return PostgresEgress(
                DSN,
                tables={
                    "orders": {
                        "columns": {"order_id": "INT", "customer_id": "INT", "total": "REAL"},
                        "primary_key": "order_id",
                    }
                },
            )


        def file_sink() -> JsonlStorageEgress:
            """Writes the records to a JSON Lines file."""
            return JsonlStorageEgress(
                default_path=f"{os.environ.get('DEST_PATH', 'data')}/orders.jsonl"
            )


        START = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


        def build(env: DynamicRealtimeEnvironment) -> None:
            """Starts the arrivals on `env`."""
            sampler = Sampler(seed=42)
            customer = sampler.generator("tasks.order.payload.customer_id")
            total = sampler.generator("tasks.order.payload.total")
            orders_gap = DistributionConfig(dist="exponential", rate=1)
            # The random sequence the blueprint's draw uses
            orders_gap.stream = "arrival.orders"

            def orders_source():
                number = 0
                while True:
                    yield env.timeout(sampler.sample(orders_gap))
                    number += 1
                    payload = {
                        "order_id": number,
                        "customer_id": int(customer.integers(1, 100 + 1)),
                        "total": round(float(total.uniform(5, 200)), 2),
                    }
                    env.publish_event(
                        f"orders-{number}", payload, task="order", instance=number - 1
                    )

            env.process(orders_source())


        SINKS = {
            "console": console_sink,
            "kafka": kafka_sink,
            "postgres": postgres_sink,
            "file": file_sink,
        }


        def run(sink=console_sink) -> None:
            """Runs the model unpaced into the egress that `sink` makes."""
            env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
            env.setup_egress([sink()])
            build(env)
            try:
                env.run(until=UNTIL)
            finally:
                env.teardown()


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            run(SINKS[os.environ.get("SINK", "console")])
        ```
