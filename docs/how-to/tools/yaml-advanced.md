# Call Python from YAML

Read outside data during a run, or use a class of your own, from a blueprint. A reference of the form `!python module.attribute` takes the place of a value, and the blueprint uses the Python object it names. Most logic that once needed Python is a key of the blueprint now, so this page covers the two uses that stay.

---

## Before You Start

- [YAML Blueprints](../../user-guide/writing-a-model/blueprints.md) describes the blueprint file.
- Install the Redis extra for the example: `pip install "desflow[redis]"`. Start the Valkey of odctl with `odctl up valkey`. See [Installation](../../installation.md) for the one-time odctl install.
- Every file on this page is in the repository under [`docs/snippets/`](https://github.com/jaehyeon-kim/desflow/tree/main/docs/snippets).

### When to Use It

Use `!python` for one of these two things:

- **Data that only an outside system holds during a run**, such as a model that another program keeps in a store. No key of the blueprint reads a store while the model runs.
- **A class of your own**, such as a connector, a resource, a store or a container, which the blueprint builds from a `type`.

Everything else is a key. Look there first:

- Field values with logic, such as random or weighted choices, fields that depend on other fields and values that change from one event to the next, are the [generators](../../generators/index.md) and the expressions of [Task Flow](../../user-guide/model-parts/task-flow.md).
- **Keys of a task** that SimPy programs wrote as functions and processes, which [YAML Keys](../../reference/yaml-keys.md) lists:
    - `service_time`, a time computed for each entity, such as `"attr.litres / 2"`, and `next: {weights: {fast: "attr.w", slow: "1 - attr.w"}}`, routing weights from an expression.
    - `seize` and `release`, a unit held across several steps.
    - `wait: {task: child, args: {...}}` and `set_args`, a sub-process with arguments.
    - `wait: {timeout: "attr.limit"}`, a timeout computed for each entity, `wait: {when: ..., of: {...}}`, a wait with its own rule, and `running: {task, whole_flow: true}`, a wait for a whole flow.
    - `open` with `{handle}`, a wait that goes on after an interrupt.
- **Keys of an arrival**: `max_events`, a fixed number of arrivals, `gap: "1 + t / 4"`, a gap that depends on the time, `group`, `active`, `activate` and `off_mode`, `count` and `copy_attributes` of a spawn, and `initial`.
- [Processes](../../user-guide/tasks-and-processes/processes.md) and [Signals and Waiting](../../user-guide/tasks-and-processes/signals-and-waiting.md) explain the flow these keys sit in.
- Routing, priorities, preemption, breakdowns, several resources, stores and containers are keys too, as [Resources](../../user-guide/resources/index.md) and [Stores and Containers](../../user-guide/model-parts/stores-and-containers.md) show.
- Readings other than the built-in statistics are described in [Built-in Statistics](../../user-guide/results/statistics.md).
- Kafka topics, serializers, file and catalog settings and the setup of a connector are keys of the sink, as [Route Records to Topics](../records-and-kafka/complex-routing.md), [Use Schema Serializers](../records-and-kafka/json-schema-and-protobuf.md) and [Egress](../../user-guide/live-runs-and-connectors/egress.md) show.

### How a Reference Is Resolved

[YAML Blueprints](../../user-guide/writing-a-model/blueprints.md#how-it-works) describes how `!python module.attribute` is resolved, and [its Security section](../../user-guide/writing-a-model/blueprints.md#security) says why a blueprint with a reference needs the trust of a Python script.

---

## Steps

### Call Outside Code During a Run

A `{gen: call}` field calls a function each time the record is made. The function is called as `function(context, **kwargs)`, and the values in `kwargs` are worked out first. Its result becomes the value of the field.

The example is a product recommender. Each visit has three features, and the function ranks five products with the model of each, which another program keeps in Valkey under the key `linucb:<product>`. It reads all five in one `MGET`, scores a product as the expected reaction plus a bonus for how uncertain the model is, and gives the three best. A product with no model yet gets the untrained one, so it can still be shown:

```python title="docs/snippets/guides/call_python_ranking.py:rank"
def top_products(context, x):
    """The three products with the highest LinUCB score for the features x."""
    scores = {}
    for pid, stored in zip(PRODUCTS, CLIENT.mget([f"linucb:{p}" for p in PRODUCTS])):
        if stored:
            model = json.loads(stored)
        else:  # A product with no model yet gets the identity matrix and a zero vector
            identity = [
                [float(i == j) for j in range(FEATURES)] for i in range(FEATURES)
            ]
            model = {"A_inv": identity, "b": [0.0] * FEATURES}
        ax = [sum(a * v for a, v in zip(row, x)) for row in model["A_inv"]]
        expected = sum(a * b for a, b in zip(ax, model["b"]))
        uncertainty = math.sqrt(sum(a * v for a, v in zip(ax, x)))
        scores[pid] = expected + ALPHA * uncertainty
    return sorted(scores, key=scores.get, reverse=True)[:3]
```

The shop makes a visit every 0.1 seconds. The features are a var, so the record shows them and the function reads the same values:

In YAML, the `gen: call` field of the task payload names the function:

```yaml title="docs/snippets/yaml/call_python.yaml:task"
tasks:
  recommend:
    vars:
      # The visitor's three features, each between 0 and 1
      x:
        gen: repeat
        times: 3
        value: {gen: uniform, low: 0, high: 1, decimals: 3}
    payload:
      features: {gen: var, name: x}
      # call_python_ranking.top_products(context, x=...) gives the three products
      shown:
        gen: call
        function: !python call_python_ranking.top_products
        kwargs:
          x: {gen: var, name: x}
```

In the Builder, the field takes the function itself:

```python title="docs/snippets/guides/call_python.py:shop"
def build_shop() -> SimulationContext:
    """The shop: each visit calls the ranking function with the visitor's features."""
    app = SimulationContext(sim_id="Recommender", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress(format="json"))
    app.add_arrival("visits", dist="constant", value=0.1, spawn="recommend")
    x = {"gen": "var", "name": "x"}
    visit = Template(
        {
            "features": x,
            # top_products(context, x=...) gives the three products
            "shown": {"gen": "call", "function": top_products, "kwargs": {"x": x}},
        },
        name="tasks.recommend",
        # The visitor's three features, each between 0 and 1
        vars={
            "x": {
                "gen": "repeat",
                "times": 3,
                "value": {"gen": "uniform", "low": 0, "high": 1, "decimals": 3},
            }
        },
    )

    @app.task(name="recommend")
    def recommend(task_id):
        """Gives one visit record."""
        return visit

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_shop().run(until=1)
```

In the low-level API, which has no `gen`, the process draws the features with a `Sampler` and calls the function itself:

```python title="docs/snippets/guides/call_python_low_level.py:shop"
def visits(env, sampler):
    """A visit every 0.1 seconds, shown the products the ranking function picks."""
    features = sampler.generator("tasks.recommend.vars.x.value")
    for number in itertools.count(1):
        yield env.timeout(0.1)
        # The visitor's three features, each between 0 and 1
        x = [round(float(features.uniform(0, 1)), 3) for _ in range(3)]
        # top_products(context, x) gives the three products
        record = {"features": x, "shown": top_products(env, x)}
        env.publish_event(f"visits-{number}", record, "recommend", number - 1)


def shop() -> DynamicRealtimeEnvironment:
    """The shop, with its visits started."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.setup_egress([ConsoleEgress(format="json")])
    env.process(visits(env, Sampler(seed=42)))
    return env


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    env = shop()
    try:
        env.run(until=1)
    finally:
        env.teardown()
```

The function runs inside the simulation, so a slow call holds the run. This one asks for five keys in one call. A function that waits for a network call at each visit sets the pace of the run.

### Use Your Own Class

A `type: !python module.Class` entry builds your class from the `config` mapping. It works for `ingress`, `egress`, resources, stores and containers. The blueprint passes the keys of `config` to the class as keyword arguments:

```yaml title="docs/snippets/yaml/write_a_connector.yaml:connectors"
ingress:
  - type: !python write_a_connector_logic.JsonlIngress
    config: {path: changes.jsonl}

egress:
  - type: !python write_a_connector_logic.CsvEgress
    when: events  # events only; the readings of telemetry stay out of the file
    config: {path: events.csv, columns: [status, event_type]}
```

[Write a Connector](../records-and-kafka/write-a-connector.md) writes these two classes. [Custom Resource Types](../../user-guide/resources/custom-resource-types.md) and [Stores and Containers](../../user-guide/model-parts/stores-and-containers.md) show a resource, a store and a container. In the Builder and Low-level forms, pass the object itself, such as `app.add_egress(CsvEgress(path="events.csv"))`.

---

## Check the Result

Store the models, then run the shop from one folder that holds `call_python_ranking.py`, `call_python_seed.py` and the files of your form:

```bash
python call_python_seed.py
```

Three products get a model and two do not. Then run your form. In YAML:

```bash
desflow run call_python.yaml
```

In the Builder:

```bash
python call_python.py
```

In the low-level API:

```bash
python call_python_low_level.py
```

Each prints one JSON record for each visit, ten in all, and the three forms print the same `features` and `shown`. The first three are:

```text
{"stream_type":"event","sim_ts":0.1,"timestamp":"2026-11-08T22:36:15.282+00:00","key":"visits-1","task":"recommend","instance":0,"value":{"features":[0.6,0.288,0.277],"shown":["p1","p2","p4"]}}
{"stream_type":"event","sim_ts":0.2,"timestamp":"2026-11-08T22:36:15.382+00:00","key":"visits-2","task":"recommend","instance":1,"value":{"features":[0.107,0.018,0.831],"shown":["p3","p4","p5"]}}
{"stream_type":"event","sim_ts":0.30000000000000004,"timestamp":"2026-11-08T22:36:15.482+00:00","key":"visits-3","task":"recommend","instance":2,"value":{"features":[0.722,0.727,0.319],"shown":["p2","p1","p4"]}}
```

The `timestamp` of a record is the clock time of the run, so it differs from run to run. In the first visit, `p4` ranks third although it has no model: its score is only the bonus for not being known. When the models change in Valkey, the next visit ranks with the new models.

A reference that cannot be resolved stops the load with its line. [`python_error.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/docs/snippets/yaml/python_error.yaml) names `shop_logic.missing` as the payload of its task, and `desflow run python_error.yaml` prints:

<!-- records: python-error -->
```text
Error: python_error.yaml:15: !python shop_logic.missing: 'shop_logic' has no attribute 'missing'
```

A module that exists but fails on one of its own imports is reported with that error, rather than as a missing module.

---

## Full Files

??? example "Full files"

    ```python title="docs/snippets/guides/call_python_ranking.py"
    """The ranking function of the Call Python from YAML page.

    It reads the LinUCB model of each product from Valkey, scores the products for one
    visitor and returns the best three. Every form of the shop calls it. It needs the redis
    package (`pip install "desflow[redis]"`) and the Valkey of `odctl up valkey`.
    """

    import json
    import math
    import os

    import redis

    PRODUCTS = ["p1", "p2", "p3", "p4", "p5"]
    FEATURES = 3
    ALPHA = 1.0

    # The Valkey of `odctl up valkey`; VALKEY_URL points the function at another one
    CLIENT = redis.Redis.from_url(
        os.getenv("VALKEY_URL", "redis://user:password@127.0.0.1:6379/0"),
        decode_responses=True,
    )


    def top_products(context, x):
        """The three products with the highest LinUCB score for the features x."""
        scores = {}
        for pid, stored in zip(PRODUCTS, CLIENT.mget([f"linucb:{p}" for p in PRODUCTS])):
            if stored:
                model = json.loads(stored)
            else:  # A product with no model yet gets the identity matrix and a zero vector
                identity = [
                    [float(i == j) for j in range(FEATURES)] for i in range(FEATURES)
                ]
                model = {"A_inv": identity, "b": [0.0] * FEATURES}
            ax = [sum(a * v for a, v in zip(row, x)) for row in model["A_inv"]]
            expected = sum(a * b for a, b in zip(ax, model["b"]))
            uncertainty = math.sqrt(sum(a * v for a, v in zip(ax, x)))
            scores[pid] = expected + ALPHA * uncertainty
        return sorted(scores, key=scores.get, reverse=True)[:3]


    ```

    ```python title="docs/snippets/guides/call_python_seed.py"
    """Writes the models the Call Python from YAML page ranks with into Valkey.

    Three products have a model and two have none yet. Run it once before the shop with
    `python call_python_seed.py`, beside call_python_ranking.py.
    """

    import json

    from call_python_ranking import CLIENT

    MODELS = {
        "p1": {"A_inv": [[0.5, 0, 0], [0, 0.5, 0], [0, 0, 0.5]], "b": [2.0, 0.0, 0.0]},
        "p2": {"A_inv": [[0.5, 0, 0], [0, 0.5, 0], [0, 0, 0.5]], "b": [0.0, 2.0, 0.0]},
        "p3": {"A_inv": [[0.25, 0, 0], [0, 0.25, 0], [0, 0, 0.25]], "b": [0.0, 0.0, 3.0]},
    }


    def seed(client) -> None:
        """Stores the models of p1, p2 and p3; p4 and p5 keep none."""
        for pid, model in MODELS.items():
            client.set(f"linucb:{pid}", json.dumps(model))


    if __name__ == "__main__":
        seed(CLIENT)
    ```

    In YAML:

    ```yaml title="docs/snippets/yaml/call_python.yaml"
    # The Call Python from YAML guide: a visit every 0.1 seconds, each shown the three
    # products with the highest score from the models in Valkey. Needs
    # call_python_ranking.py beside it. Run it with: desflow run call_python.yaml
    simulation: {sim_id: Recommender, factor: 0, random_seed: 42}

    arrivals:
      visits: {dist: constant, value: 0.1, spawn: recommend}

    tasks:
      recommend:
        vars:
          # The visitor's three features, each between 0 and 1
          x:
            gen: repeat
            times: 3
            value: {gen: uniform, low: 0, high: 1, decimals: 3}
        payload:
          features: {gen: var, name: x}
          # call_python_ranking.top_products(context, x=...) gives the three products
          shown:
            gen: call
            function: !python call_python_ranking.top_products
            kwargs:
              x: {gen: var, name: x}

    egress:
      - type: Console
        config: {format: json}

    run: {until: 1 s}
    ```

    In the Builder:

    ```python title="docs/snippets/guides/call_python.py"
    """The Call Python from YAML guide, built with the builder API.

    A visit every 0.1 seconds, each shown the three products with the highest score from the
    models in Valkey. call_python.yaml and call_python_low_level.py are the same shop in YAML
    and the low-level API. Needs call_python_ranking.py beside it. Run it with:
    python call_python.py
    """

    import logging

    from call_python_ranking import top_products

    from desflow import ConsoleEgress, SimulationContext
    from desflow.values import Template


    def build_shop() -> SimulationContext:
        """The shop: each visit calls the ranking function with the visitor's features."""
        app = SimulationContext(sim_id="Recommender", factor=0, random_seed=42)
        app.add_egress(ConsoleEgress(format="json"))
        app.add_arrival("visits", dist="constant", value=0.1, spawn="recommend")
        x = {"gen": "var", "name": "x"}
        visit = Template(
            {
                "features": x,
                # top_products(context, x=...) gives the three products
                "shown": {"gen": "call", "function": top_products, "kwargs": {"x": x}},
            },
            name="tasks.recommend",
            # The visitor's three features, each between 0 and 1
            vars={
                "x": {
                    "gen": "repeat",
                    "times": 3,
                    "value": {"gen": "uniform", "low": 0, "high": 1, "decimals": 3},
                }
            },
        )

        @app.task(name="recommend")
        def recommend(task_id):
            """Gives one visit record."""
            return visit

        return app


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        build_shop().run(until=1)
    ```

    In the low-level API:

    ```python title="docs/snippets/guides/call_python_low_level.py"
    """The Call Python from YAML guide, low-level API.

    A visit every 0.1 seconds, each shown the three products with the highest score from the
    models in Valkey. The features come from `Sampler`, from the random sequence the
    blueprint's generator at that place uses, so the draws are the blueprint's. Needs
    call_python_ranking.py beside it. Run it with: python call_python_low_level.py
    """

    import itertools
    import logging

    from call_python_ranking import top_products

    from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler


    def visits(env, sampler):
        """A visit every 0.1 seconds, shown the products the ranking function picks."""
        features = sampler.generator("tasks.recommend.vars.x.value")
        for number in itertools.count(1):
            yield env.timeout(0.1)
            # The visitor's three features, each between 0 and 1
            x = [round(float(features.uniform(0, 1)), 3) for _ in range(3)]
            # top_products(context, x) gives the three products
            record = {"features": x, "shown": top_products(env, x)}
            env.publish_event(f"visits-{number}", record, "recommend", number - 1)


    def shop() -> DynamicRealtimeEnvironment:
        """The shop, with its visits started."""
        env = DynamicRealtimeEnvironment(factor=0)
        env.setup_egress([ConsoleEgress(format="json")])
        env.process(visits(env, Sampler(seed=42)))
        return env


    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        env = shop()
        try:
            env.run(until=1)
        finally:
            env.teardown()
    ```

---

## Sources

The recommender is cut down from [product-recommender](https://github.com/jaehyeon-kim/benchtop/tree/main/product-recommender), which keeps 200 products with about 15 features each. The 1.0 language has no key that reads a store during a run, which is why this call is written in Python.
