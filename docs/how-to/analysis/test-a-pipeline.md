# Test a Pipeline

Test a pipeline that reads a stream of records, such as a detector or an alert rule, against a fault that a seeded run plants and labels. The run is the same every time, so the test can say which fault the pipeline must find and where.

---

## Before You Start

- [Live Parameters](../../user-guide/live-runs-and-connectors/live-parameters.md#record-what-a-change-was-for) explains the scenario step and the label record it publishes.
- [Egress](../../user-guide/live-runs-and-connectors/egress.md) explains the `when` values of a sink.
- [Test a Model](test-a-model.md) tests the model itself. This page tests a program that reads the model's records.
- You need pytest: `pip install pytest`, and the JSON Lines sink, which needs the `parquet` extra: `pip install "desflow[parquet]"`.

---

## Steps

### Plant a Fault and Label It

The shop takes an order every 2 seconds on average. A scenario step at 300 seconds triples the rate, and its `record` publishes a ground-truth label of the fault. A second step at 360 seconds puts the rate back, with no label. Two sinks write what the run produces. The data sink gets the events only. The labels sink gets the labels only, so the pipeline under test never sees them. `OUT`, or the `folder` of the function, is where the two sinks write:

=== "YAML"

    ```yaml title="docs/snippets/yaml/test_a_pipeline.yaml:sinks"
    egress:
      # The data under test: events only, with no label
      - type: Jsonl
        config: {default_path: "${OUT:-output}/data/events.jsonl"}
        when: events
      # The labels only, for the scorer
      - type: Jsonl
        config: {default_path: "${OUT:-output}/labels/labels.jsonl"}
        when: labels
    ```

    ```yaml title="docs/snippets/yaml/test_a_pipeline.yaml:scenario"
    scenario:
      # The fault: the order rate triples at 300 seconds, and the label says so
      - at: 300
        path: Shop.arrival.orders.rate
        value: 1.5
        record: {fault: order_surge}
      # The end of the fault, which needs no label
      - {at: 360, path: Shop.arrival.orders.rate, value: 0.5}
    ```

    `run_shop` runs the blueprint, with `OUT` set to the folder:

    ```python title="docs/snippets/guides/pipeline_shop_yaml.py:shop"
    def run_shop(folder: Path) -> None:
        """Runs the shop of the blueprint, writing its data and labels under `folder`."""
        blueprint = Path(__file__).with_name("test_a_pipeline.yaml")
        SimulationContext.from_yaml(blueprint, variables={"OUT": str(folder)}).run()
    ```

=== "Builder"

    ```python title="docs/snippets/guides/pipeline_shop_builder.py:shop"
    def run_shop(folder: Path) -> None:
        """Runs the shop, writing its data and labels under `folder`."""
        app = (
            SimulationContext(sim_id="Shop", factor=0, random_seed=42)
            .add_resource("packers", current_cap=3, max_cap=5)
            .add_service("packing", dist="constant", value=1.5)
            .add_arrival("orders", dist="exponential", rate=0.5, spawn="pack_order")
            # The data under test: events only, with no label
            .add_egress(
                JsonlStorageEgress(default_path=f"{folder}/data/events.jsonl"),
                when=lambda record: record["stream_type"] == "event",
            )
            # The labels only, for the scorer
            .add_egress(
                JsonlStorageEgress(default_path=f"{folder}/labels/labels.jsonl"),
                when="labels",
            )
            .add_scenario(
                [
                    # The fault: the order rate triples at 300 seconds, and the label says so
                    {
                        "at": 300,
                        "path": "Shop.arrival.orders.rate",
                        "value": 1.5,
                        "record": {"fault": "order_surge"},
                    },
                    # The end of the fault, which needs no label
                    {"at": 360, "path": "Shop.arrival.orders.rate", "value": 0.5},
                ]
            )
        )

        @app.task(name="pack_order", service_id="packing", resource_id="packers")
        def pack_order(task_id):
            return {"event_type": "order_packed"}

        app.run(until="10 min")
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/pipeline_shop_low_level.py:shop"
    def run_shop(folder: Path) -> None:
        """Runs the shop, writing its data and labels under `folder`."""
        env = DynamicRealtimeEnvironment(factor=0)
        env.registry.register_sim_parameter(PARAMS)
        env.setup_egress(
            [
                # The data under test: events only, with no label
                JsonlStorageEgress(default_path=f"{folder}/data/events.jsonl"),
                # The labels only, for the scorer
                JsonlStorageEgress(default_path=f"{folder}/labels/labels.jsonl"),
            ],
            predicates=[lambda record: record["stream_type"] == "event", None],
            labels=[False, True],
        )
        packers = DynamicResource(env, "Shop", "packers")
        env.process(run_scenario(env, STEPS, until=600))
        env.process(orders(env, packers, Sampler(seed=42)))
        try:
            env.run(until=600)
        finally:
            env.teardown()
    ```

Each sink writes one file for each batch, named `events_<8 hex digits>.jsonl` and `labels_<8 hex digits>.jsonl`. The label file holds one line, with `sim_ts` 300 and the label `{"fault": "order_surge"}`.

### Write the Pipeline Under Test

The pipeline counts the finished orders in each window of 10 seconds and flags a window when its count is more than 2.5 times the median count. A surge shows as flagged windows:

```python title="docs/snippets/guides/pipeline_under_test.py:pipeline"
def count_orders(events: list[dict], window: int) -> list[int]:
    """Returns the number of finished orders in each window, from the first window."""
    packed: Counter = Counter()
    for event in events:
        if "event_type" in event:
            packed[int(event["sim_ts"] // window)] += 1
    return [packed[number] for number in range(max(packed) + 1)]


def find_surges(events: list[dict], window: int = 10, factor: float = 2.5) -> list[int]:
    """Returns the start of each window with more than `factor` times the usual orders."""
    counts = count_orders(events, window)
    limit = factor * statistics.median(counts)
    starts = []
    for number, count in enumerate(counts):
        if count > limit:
            starts.append(number * window)
    return starts
```

### Score It Against the Labels

A fault is caught when a window is flagged within 60 seconds of its label. A flagged window with no fault before it is a false alarm:

```python title="docs/snippets/guides/pipeline_under_test.py:score"
def score(found: list[int], labels: list[dict], within: int = 60) -> dict:
    """Returns how many planted faults the flagged windows caught and missed, and how
    many flagged windows had no fault before them."""
    faults = [label["sim_ts"] for label in labels]
    caught = 0
    for fault in faults:
        if any(fault <= start < fault + within for start in found):
            caught += 1
    false_alarms = 0
    for start in found:
        if not any(fault <= start < fault + within for fault in faults):
            false_alarms += 1
    return {
        "caught": caught,
        "missed": len(faults) - caught,
        "false": false_alarms,
    }
```

For this run the pipeline flags the windows that start at 330, 340 and 350 seconds. A window is flagged only when it holds more than 2.5 times the usual number of finished orders, so the first flagged window starts at 330 seconds, 30 seconds after the label. The score is one fault caught, none missed and no false alarm.

### Write the Tests

The tests run the shop once, and check the labels, the data and the pipeline. The last test shows that the check can fail: a pipeline that flags only a window with ten times the usual orders misses the fault:

```python title="docs/snippets/guides/test_a_pipeline.py:tests"
def test_the_labels_name_the_planted_fault(run):
    _, labels = run
    assert [label["label"] for label in labels] == [{"fault": "order_surge"}]
    assert [label["sim_ts"] for label in labels] == [300.0]


def test_the_data_carries_no_label(run):
    events, _ = run
    assert {event["stream_type"] for event in events} == {"event"}


def test_the_pipeline_catches_the_fault_without_a_false_alarm(run):
    events, labels = run
    assert score(find_surges(events), labels) == {"caught": 1, "missed": 0, "false": 0}


def test_a_pipeline_that_is_too_strict_misses_the_fault(run):
    events, labels = run
    assert score(find_surges(events, factor=10), labels)["missed"] == 1
```

---

## Check the Result

Save the shop of your form as `pipeline_shop.py`, in a folder with `pipeline_under_test.py` and `test_a_pipeline.py`. For the YAML form, add `test_a_pipeline.yaml` to the folder. Then run:

```bash
pytest test_a_pipeline.py
```

pytest reports 4 passed. The three forms flag the same windows and give the same score.

A test of this kind states what a pipeline does on one seeded run. A new release of desflow may draw its random numbers differently, so after an upgrade, run the tests again and read any change before you keep it.

---

## Full Files

??? example "Full files"

    ```python title="docs/snippets/guides/pipeline_under_test.py"
    """The pipeline under test and its scorer, for the Test a Pipeline page.

    The pipeline reads the events of a run and flags the windows with unusually many orders.
    The scorer compares what it flagged with the labels of the faults the run planted.
    """

    import glob
    import json
    import statistics
    from collections import Counter


    def read_lines(folder: str) -> list[dict]:
        """Every JSON line of every file under `folder`, in time order."""
        lines = []
        for path in glob.glob(f"{folder}/*.jsonl"):
            with open(path) as file:
                lines += [json.loads(line) for line in file]
        return sorted(lines, key=lambda line: line["sim_ts"])


    def count_orders(events: list[dict], window: int) -> list[int]:
        """Returns the number of finished orders in each window, from the first window."""
        packed: Counter = Counter()
        for event in events:
            if "event_type" in event:
                packed[int(event["sim_ts"] // window)] += 1
        return [packed[number] for number in range(max(packed) + 1)]


    def find_surges(events: list[dict], window: int = 10, factor: float = 2.5) -> list[int]:
        """Returns the start of each window with more than `factor` times the usual orders."""
        counts = count_orders(events, window)
        limit = factor * statistics.median(counts)
        starts = []
        for number, count in enumerate(counts):
            if count > limit:
                starts.append(number * window)
        return starts


    def score(found: list[int], labels: list[dict], within: int = 60) -> dict:
        """Returns how many planted faults the flagged windows caught and missed, and how
        many flagged windows had no fault before them."""
        faults = [label["sim_ts"] for label in labels]
        caught = 0
        for fault in faults:
            if any(fault <= start < fault + within for start in found):
                caught += 1
        false_alarms = 0
        for start in found:
            if not any(fault <= start < fault + within for fault in faults):
                false_alarms += 1
        return {
            "caught": caught,
            "missed": len(faults) - caught,
            "false": false_alarms,
        }


    ```

    ```python title="docs/snippets/guides/test_a_pipeline.py"
    """The Test a Pipeline page: tests of a pipeline against the labels of a planted fault.

    Save the shop of your form from the page as pipeline_shop.py in the folder of this file,
    with pipeline_under_test.py, and run: pytest test_a_pipeline.py
    """

    import pytest
    from pipeline_shop import run_shop
    from pipeline_under_test import find_surges, read_lines, score


    @pytest.fixture(scope="module")
    def run(tmp_path_factory):
        """One run of the shop: its events and its labels."""
        folder = tmp_path_factory.mktemp("run")
        run_shop(folder)
        return read_lines(f"{folder}/data"), read_lines(f"{folder}/labels")


    def test_the_labels_name_the_planted_fault(run):
        _, labels = run
        assert [label["label"] for label in labels] == [{"fault": "order_surge"}]
        assert [label["sim_ts"] for label in labels] == [300.0]


    def test_the_data_carries_no_label(run):
        events, _ = run
        assert {event["stream_type"] for event in events} == {"event"}


    def test_the_pipeline_catches_the_fault_without_a_false_alarm(run):
        events, labels = run
        assert score(find_surges(events), labels) == {"caught": 1, "missed": 0, "false": 0}


    def test_a_pipeline_that_is_too_strict_misses_the_fault(run):
        events, labels = run
        assert score(find_surges(events, factor=10), labels)["missed"] == 1


    ```

    === "YAML"

        ```yaml title="docs/snippets/yaml/test_a_pipeline.yaml"
        # The Test a Pipeline guide: a shop with a planted fault. Orders surge at 300 seconds
        # and the label of the fault goes only to the labels sink. OUT is the folder the two
        # sinks write under. Run it with: desflow run test_a_pipeline.yaml
        simulation: {sim_id: Shop, factor: 0, random_seed: 42}

        resources:
          packers: {current_cap: 3, max_cap: 5}

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
          # The data under test: events only, with no label
          - type: Jsonl
            config: {default_path: "${OUT:-output}/data/events.jsonl"}
            when: events
          # The labels only, for the scorer
          - type: Jsonl
            config: {default_path: "${OUT:-output}/labels/labels.jsonl"}
            when: labels

        scenario:
          # The fault: the order rate triples at 300 seconds, and the label says so
          - at: 300
            path: Shop.arrival.orders.rate
            value: 1.5
            record: {fault: order_surge}
          # The end of the fault, which needs no label
          - {at: 360, path: Shop.arrival.orders.rate, value: 0.5}

        run: {until: 10 min}
        ```

        ```python title="docs/snippets/guides/pipeline_shop_yaml.py"
        """The Test a Pipeline page, YAML form: runs test_a_pipeline.yaml, beside this file, with
        its sinks under `folder`. Save it as pipeline_shop.py for the tests."""

        from pathlib import Path

        from desflow import SimulationContext


        def run_shop(folder: Path) -> None:
            """Runs the shop of the blueprint, writing its data and labels under `folder`."""
            blueprint = Path(__file__).with_name("test_a_pipeline.yaml")
            SimulationContext.from_yaml(blueprint, variables={"OUT": str(folder)}).run()


        ```

    === "Builder"

        ```python title="docs/snippets/guides/pipeline_shop_builder.py"
        """The Test a Pipeline page, builder form: the shop with a planted fault. Save it as
        pipeline_shop.py for the tests."""

        from pathlib import Path

        from desflow import JsonlStorageEgress, SimulationContext


        def run_shop(folder: Path) -> None:
            """Runs the shop, writing its data and labels under `folder`."""
            app = (
                SimulationContext(sim_id="Shop", factor=0, random_seed=42)
                .add_resource("packers", current_cap=3, max_cap=5)
                .add_service("packing", dist="constant", value=1.5)
                .add_arrival("orders", dist="exponential", rate=0.5, spawn="pack_order")
                # The data under test: events only, with no label
                .add_egress(
                    JsonlStorageEgress(default_path=f"{folder}/data/events.jsonl"),
                    when=lambda record: record["stream_type"] == "event",
                )
                # The labels only, for the scorer
                .add_egress(
                    JsonlStorageEgress(default_path=f"{folder}/labels/labels.jsonl"),
                    when="labels",
                )
                .add_scenario(
                    [
                        # The fault: the order rate triples at 300 seconds, and the label says so
                        {
                            "at": 300,
                            "path": "Shop.arrival.orders.rate",
                            "value": 1.5,
                            "record": {"fault": "order_surge"},
                        },
                        # The end of the fault, which needs no label
                        {"at": 360, "path": "Shop.arrival.orders.rate", "value": 0.5},
                    ]
                )
            )

            @app.task(name="pack_order", service_id="packing", resource_id="packers")
            def pack_order(task_id):
                return {"event_type": "order_packed"}

            app.run(until="10 min")


        ```

    === "Low-level"

        ```python title="docs/snippets/guides/pipeline_shop_low_level.py"
        """The Test a Pipeline page, low-level form: the shop with a planted fault. Save it as
        pipeline_shop.py for the tests."""

        import itertools
        from pathlib import Path

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            JsonlStorageEgress,
            Sampler,
            SimParameter,
            run_scenario,
        )

        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
            service={"packing": DistributionConfig(dist="constant", value=1.5)},
            resources={"packers": CapacityConfig(current_cap=3, max_cap=5)},
        )

        # The fault: the order rate triples at 300 seconds, and the label says so
        STEPS = [
            {
                "at": 300,
                "path": "Shop.arrival.orders.rate",
                "value": 1.5,
                "record": {"fault": "order_surge"},
            },
            # The end of the fault, which needs no label
            {"at": 360, "path": "Shop.arrival.orders.rate", "value": 0.5},
        ]


        def pack_order(env, packers, sampler, key, task_id):
            with packers.request() as unit:
                yield unit
                yield env.timeout(
                    sampler.sample(env.registry.get_config("Shop.service.packing"))
                )
                env.publish_event(key, {"event_type": "order_packed"}, "pack_order", task_id)


        def orders(env, packers, sampler):
            gap = env.registry.get_config("Shop.arrival.orders")
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


        def run_shop(folder: Path) -> None:
            """Runs the shop, writing its data and labels under `folder`."""
            env = DynamicRealtimeEnvironment(factor=0)
            env.registry.register_sim_parameter(PARAMS)
            env.setup_egress(
                [
                    # The data under test: events only, with no label
                    JsonlStorageEgress(default_path=f"{folder}/data/events.jsonl"),
                    # The labels only, for the scorer
                    JsonlStorageEgress(default_path=f"{folder}/labels/labels.jsonl"),
                ],
                predicates=[lambda record: record["stream_type"] == "event", None],
                labels=[False, True],
            )
            packers = DynamicResource(env, "Shop", "packers")
            env.process(run_scenario(env, STEPS, until=600))
            env.process(orders(env, packers, Sampler(seed=42)))
            try:
                env.run(until=600)
            finally:
                env.teardown()


        ```
