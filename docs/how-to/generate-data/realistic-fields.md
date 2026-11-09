# Realistic Fields

Fill records with names, emails, cities and jobs that look real, and keep a product's sku, title and price together by drawing one row of a CSV file.

---

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

    ```bash
    git clone https://github.com/jaehyeon-kim/desflow.git
    cd desflow
    ```

- [First Simulation](../../learn/first-model/first-simulation.md) explains arrivals, tasks and the records a run writes.
- Install the Faker library: `pip install "desflow[fake]"`.
- [`fake`](../../generators/text-and-ids/fake.md) and [`row`](../../generators/related-records/row.md) list the keys of the two generators this page uses.

---

## Steps

### Describe the Arrival

A customer signs up about every 2 seconds: `rate: 0.5` is a gap that averages 2 seconds.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/realistic_fields.yaml:arrivals" hl_lines="2"
    arrivals:
      signups: {dist: exponential, rate: 0.5, spawn: signup}
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/realistic_fields_builder.py:arrival"
    app.add_arrival("signups", "exponential", rate=0.5, spawn="signup")
    ```

=== "Low-level"

    ```python title="docs/snippets/generate-data/realistic_fields_low_level.py:arrival"
    signups_gap = DistributionConfig(dist="exponential", rate=0.5)
    # The random sequence the blueprint's draw uses
    signups_gap.stream = "arrival.signups"
    ```

### Draw Names and Places

A `fake` field calls one method of the [Faker](https://faker.readthedocs.io/en/master/providers.html) library by name: `name`, `email`, `city` and `job` here. `locale: de_DE` draws a field in the German locale, so `german_city` is a German town. A Faker method that reads today's date, such as `date_of_birth`, would change the records from day to day, so this model uses none.

=== "YAML"

    ```yaml title="docs/snippets/generate-data/realistic_fields.yaml:tasks" hl_lines="5 6 7 8 9"
    tasks:
      signup:
        payload:
          customer_id: {gen: sequence, start: 1}
          name: {gen: fake, method: name}
          email: {gen: fake, method: email}
          city: {gen: fake, method: city}
          german_city: {gen: fake, method: city, locale: de_DE}
          job: {gen: fake, method: job}
          favourite: {gen: row, file: realistic_fields_products.csv, weight: popularity}
    ```

=== "Builder"

    ```python title="docs/snippets/generate-data/realistic_fields_builder.py:payload"
    signup_payload = Template(
        {
            "customer_id": {"gen": "sequence", "start": 1},
            "name": {"gen": "fake", "method": "name"},
            "email": {"gen": "fake", "method": "email"},
            "city": {"gen": "fake", "method": "city"},
            "german_city": {"gen": "fake", "method": "city", "locale": "de_DE"},
            "job": {"gen": "fake", "method": "job"},
            "favourite": {"gen": "row", "file": CSV, "weight": "popularity"},
        },
        name="tasks.signup",
    )

    @app.task(name="signup")
    def signup(task_id):
        """Gives one signup record."""
        return signup_payload
    ```

=== "Low-level"

    ```python title="docs/snippets/generate-data/realistic_fields_low_level.py:fields"
    sampler = Sampler(seed=42)

    def faker(place, locale="en_US"):
        """A Faker seeded from the random sequence of the generator at `place`."""
        fake = Faker(locale)
        fake.seed_instance(int(sampler.generator(place + "#fake").integers(2**32)))
        return fake

    name = faker("tasks.signup.payload.name")
    email = faker("tasks.signup.payload.email")
    city = faker("tasks.signup.payload.city")
    german_city = faker("tasks.signup.payload.german_city", "de_DE")
    job = faker("tasks.signup.payload.job")
    # A cell of digits is a number, as the `row` generator reads it
    with CSV.open(encoding="utf-8") as file:
        products: list = [
            {
                "sku": row["sku"],
                "title": row["title"],
                "price": float(row["price"]),
                "popularity": int(row["popularity"]),
            }
            for row in csv.DictReader(file)
        ]
    total = sum(product["popularity"] for product in products)
    shares = [product["popularity"] / total for product in products]
    favourite = sampler.generator("tasks.signup.payload.favourite")
    ```

    ```python title="docs/snippets/generate-data/realistic_fields_low_level.py:payload"
    def signups_source():
        number = 0
        while True:
            yield env.timeout(sampler.sample(signups_gap))
            number += 1
            payload = {
                "customer_id": number,
                "name": name.name(),
                "email": email.email(),
                "city": city.city(),
                "german_city": german_city.city(),
                "job": job.job(),
                "favourite": dict(
                    products[int(favourite.choice(len(products), p=shares))]
                ),
            }
            env.publish_event(
                f"signups-{number}", payload, task="signup", instance=number - 1
            )

    env.process(signups_source())
    ```

    Each `fake` field has a Faker of its own, seeded from the random sequence named by the field's place with `#fake` added. The Low-level file builds one Faker for each field in the same way, and a fixed seed gives the same names every run.

### Keep a Product Together

[`row`](../../generators/related-records/row.md) draws one row of a CSV file, so the sku, the title and the price always belong to the same product. `weight: popularity` draws the rows in proportion to the popularity column, so the notebook, with 12 of the 30 points in the file, is the favourite of 12 customers in 30.

```csv title="docs/snippets/generate-data/realistic_fields_products.csv"
sku,title,price,popularity
P-100,Desk lamp,24.90,5
P-101,Notebook,4.50,12
P-102,Water bottle,16.00,8
P-103,Backpack,59.00,3
P-104,Headphones,89.00,2
```

The Builder and Low-level files name the CSV file by a path beside the file, because a Builder `row` file is read from the working directory. The Low-level file reads the file itself with `csv.DictReader` and draws a row by its share of the popularity.

---

## Check the Result

Run the model for 20 simulated seconds and print its first 3 records. Each record has a name, an email, a city, a German city, a job and a product.

=== "YAML"

    ```bash
    desflow run docs/snippets/generate-data/realistic_fields.yaml --sample 3
    ```

=== "Builder"

    ```bash
    python docs/snippets/generate-data/realistic_fields_builder.py
    ```

    This prints every record of the run as an `[EVT]` line with the same fields and values.

=== "Low-level"

    ```bash
    python docs/snippets/generate-data/realistic_fields_low_level.py
    ```

    This prints every record of the run as an `[EVT]` line with the same fields and values.

<!-- records: gd-realistic-fields -->
```text
{"stream_type":"event","sim_ts":0.2343673678329999,"timestamp":"...","key":"signups-1","task":"signup","instance":0,"value":{"customer_id":1,"name":"Frederick Holmes","email":"hendersonrobert@example.net","city":"Hallborough","german_city":"Gräfenhainichen","job":"Surveyor, building control","favourite":{"sku":"P-104","title":"Headphones","price":89.0,"popularity":2}}}
{"stream_type":"event","sim_ts":1.056268358233068,"timestamp":"...","key":"signups-2","task":"signup","instance":1,"value":{"customer_id":2,"name":"Jordan Daniel","email":"matthew48@example.com","city":"West Nicole","german_city":"Großenhain","job":"Clinical biochemist","favourite":{"sku":"P-101","title":"Notebook","price":4.5,"popularity":12}}}
{"stream_type":"event","sim_ts":2.4217216156996133,"timestamp":"...","key":"signups-3","task":"signup","instance":2,"value":{"customer_id":3,"name":"Matthew King","email":"lewisdiane@example.com","city":"Tomland","german_city":"Ueckermünde","job":"Paediatric nurse","favourite":{"sku":"P-100","title":"Desk lamp","price":24.9,"popularity":5}}}
```

The names depend on the version of Faker that the lock file pins, so a different version writes other names. The records stay the same from run to run for one version.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/generate-data/realistic_fields.yaml"
        # Realistic fields: names, emails and places from Faker, and products from a CSV file.
        #
        # A customer signs up about every 2 seconds. Their name, email, city and job come
        # from Faker, one `fake` generator for each field, and `german_city` is a city drawn
        # in the German locale (`de_DE`). A Faker method that reads today's date would change
        # the records from day to day, so this file uses none. The product is one row of
        # realistic_fields_products.csv, chosen by the popularity column, so the sku, title and
        # price stay together.
        #
        # Needs: the fake extra.
        # Run it with: desflow run docs/snippets/generate-data/realistic_fields.yaml

        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42
          logical_start_time: 2026-01-01T00:00:00

        egress:
          - {type: Console}

        arrivals:
          signups: {dist: exponential, rate: 0.5, spawn: signup}

        tasks:
          signup:
            payload:
              customer_id: {gen: sequence, start: 1}
              name: {gen: fake, method: name}
              email: {gen: fake, method: email}
              city: {gen: fake, method: city}
              german_city: {gen: fake, method: city, locale: de_DE}
              job: {gen: fake, method: job}
              favourite: {gen: row, file: realistic_fields_products.csv, weight: popularity}

        run: {until: 20}
        ```

        ```csv title="docs/snippets/generate-data/realistic_fields_products.csv"
        sku,title,price,popularity
        P-100,Desk lamp,24.90,5
        P-101,Notebook,4.50,12
        P-102,Water bottle,16.00,8
        P-103,Backpack,59.00,3
        P-104,Headphones,89.00,2
        ```

    === "Builder"

        ```python title="docs/snippets/generate-data/realistic_fields_builder.py"
        """Realistic fields: names, emails and places from Faker, and products from a CSV file.

        The builder version of `realistic_fields.yaml`: a customer signs up about every 2
        seconds, with fields from Faker and a product from a CSV file.

        Needs: the fake extra.
        Run it with: python docs/snippets/generate-data/realistic_fields_builder.py
        """

        import logging
        from datetime import datetime, timezone
        from pathlib import Path

        from desflow import ConsoleEgress, SimulationContext
        from desflow.values import Template

        CSV = str(Path(__file__).with_name("realistic_fields_products.csv"))
        UNTIL = 20


        def build() -> SimulationContext:
            """Builds the model."""
            app = SimulationContext(
                sim_id="Shop",
                factor=0,
                random_seed=42,
                logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            app.add_egress(ConsoleEgress())
            app.add_arrival("signups", "exponential", rate=0.5, spawn="signup")
            signup_payload = Template(
                {
                    "customer_id": {"gen": "sequence", "start": 1},
                    "name": {"gen": "fake", "method": "name"},
                    "email": {"gen": "fake", "method": "email"},
                    "city": {"gen": "fake", "method": "city"},
                    "german_city": {"gen": "fake", "method": "city", "locale": "de_DE"},
                    "job": {"gen": "fake", "method": "job"},
                    "favourite": {"gen": "row", "file": CSV, "weight": "popularity"},
                },
                name="tasks.signup",
            )

            @app.task(name="signup")
            def signup(task_id):
                """Gives one signup record."""
                return signup_payload


            return app


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            build().run(until=UNTIL)
        ```

    === "Low-level"

        ```python title="docs/snippets/generate-data/realistic_fields_low_level.py"
        """Realistic fields: names, emails and places from Faker, and products from a CSV file.

        The low-level version of `realistic_fields.yaml`: plain SimPy on a
        `DynamicRealtimeEnvironment`, which publishes each record with `publish_event`. Each
        field has a Faker of its own, seeded from the random sequence the blueprint's generator
        at that place uses (its place with `#fake` added), and the product is one row of the
        CSV file read with the `csv` module, chosen by `Sampler.generator(<place>)` with the
        popularity column as the weights.

        Needs: the fake extra.
        Run it with: python docs/snippets/generate-data/realistic_fields_low_level.py
        """

        import csv
        import logging
        from datetime import datetime, timezone
        from pathlib import Path

        from faker import Faker

        from desflow import (
            ConsoleEgress,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            Sampler,
        )

        CSV = Path(__file__).with_name("realistic_fields_products.csv")
        UNTIL = 20
        START = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}


        def build(env: DynamicRealtimeEnvironment) -> None:
            """Starts the arrival on `env`."""
            sampler = Sampler(seed=42)

            def faker(place, locale="en_US"):
                """A Faker seeded from the random sequence of the generator at `place`."""
                fake = Faker(locale)
                fake.seed_instance(int(sampler.generator(place + "#fake").integers(2**32)))
                return fake

            name = faker("tasks.signup.payload.name")
            email = faker("tasks.signup.payload.email")
            city = faker("tasks.signup.payload.city")
            german_city = faker("tasks.signup.payload.german_city", "de_DE")
            job = faker("tasks.signup.payload.job")
            # A cell of digits is a number, as the `row` generator reads it
            with CSV.open(encoding="utf-8") as file:
                products: list = [
                    {
                        "sku": row["sku"],
                        "title": row["title"],
                        "price": float(row["price"]),
                        "popularity": int(row["popularity"]),
                    }
                    for row in csv.DictReader(file)
                ]
            total = sum(product["popularity"] for product in products)
            shares = [product["popularity"] / total for product in products]
            favourite = sampler.generator("tasks.signup.payload.favourite")
            signups_gap = DistributionConfig(dist="exponential", rate=0.5)
            # The random sequence the blueprint's draw uses
            signups_gap.stream = "arrival.signups"

            def signups_source():
                number = 0
                while True:
                    yield env.timeout(sampler.sample(signups_gap))
                    number += 1
                    payload = {
                        "customer_id": number,
                        "name": name.name(),
                        "email": email.email(),
                        "city": city.city(),
                        "german_city": german_city.city(),
                        "job": job.job(),
                        "favourite": dict(
                            products[int(favourite.choice(len(products), p=shares))]
                        ),
                    }
                    env.publish_event(
                        f"signups-{number}", payload, task="signup", instance=number - 1
                    )

            env.process(signups_source())


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
