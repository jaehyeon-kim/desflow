# Live Changes

The shop of [Changing Demand](changing-demand.md) has one packer, 1.5 seconds of packing and orders at 0.4 a second. At 20 minutes its label printer jams, and packing takes 3 seconds until the printer is fixed at 30 minutes. This page asks: **how long does the jam keep orders waiting after the fix, and how can someone who reads only the records tell what changed?** The decision is how long to keep a delay notice up after a fault on the bench is fixed. The measures are the mean wait of the orders queued in the 10 minutes of the jam and in the 10 minutes after it, and the records that name the change. The boundary is the hour. The assumptions are that the jam doubles the packing time and that orders arrive at random at the same rate throughout.

## What You Add

A **planned intervention**: two scenario steps that change the packing time and publish a **label**, a ground-truth record of each change. The events go to one file sink and the labels to another, and a second run without the jam gives the baseline. A last step sends the same change live from `LocalIngress`, with no broker.

---

## Record the Jam

A scenario step with a `record` mapping also publishes a label, as [Live Parameters](../user-guide/live-parameters.md#record-what-a-change-was-for) describes. The jam is two steps, each with its own label:

=== "YAML"

    ```yaml title="docs/snippets/yaml/live_changes.yaml:jam"
    # The jam: 3 seconds of packing from 20 to 30 minutes, each change recorded
    scenario:
      - at: 20 min
        path: Shop.service.packing.value
        value: 3.0
        record: {intervention: printer_jam}
      - at: 30 min
        path: Shop.service.packing.value
        value: 1.5
        record: {intervention: printer_fixed}
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/live_changes.py:jam"
    # The jam: 3 seconds of packing from 20 to 30 minutes, each change recorded
    app.add_scenario(
        [
            {
                "at": "20 min",
                "path": "Shop.service.packing.value",
                "value": 3.0,
                "record": {"intervention": "printer_jam"},
            },
            {
                "at": "30 min",
                "path": "Shop.service.packing.value",
                "value": 1.5,
                "record": {"intervention": "printer_fixed"},
            },
        ]
    )
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/live_changes_low_level.py:jam"
    # The jam: 3 seconds of packing from 20 to 30 minutes, each change recorded
    JAM = [
        {
            "at": 1200,
            "path": "Shop.service.packing.value",
            "value": 3.0,
            "record": {"intervention": "printer_jam"},
        },
        {
            "at": 1800,
            "path": "Shop.service.packing.value",
            "value": 1.5,
            "record": {"intervention": "printer_fixed"},
        },
    ]
    ```

    `run_scenario` is the SimPy process that applies the steps, started with `env.process`:

    ```python title="docs/snippets/tutorials/live_changes_low_level.py:run"
    # Each step applies before every other event at its time
    env.process(run_scenario(env, JAM, until=until))
    ```

    It waits on SimPy's clock with timeout events, as [SimPy and desflow](../des/simpy-and-desflow.md) describes.

The steps follow the simulation clock, so the jam lands at exactly 20 and 30 minutes on every run, as in Changing Demand. The path `Shop.service.packing.value` is the `value` of a `constant` distribution; [Live Parameters](../user-guide/live-parameters.md#find-the-path) shows how to list every path of a model.

---

## Write Events and Labels Apart

The labels go only to a sink with `when: labels`, for the reasons [Live Parameters](../user-guide/live-parameters.md#record-what-a-change-was-for) gives:

=== "YAML"

    ```yaml title="docs/snippets/yaml/live_changes.yaml:sinks"
    egress:
      # The events, one flat row per event
      - type: Jsonl
        config: {default_path: events/orders.jsonl}
      # The ground-truth records only
      - type: Jsonl
        config: {default_path: labels/labels.jsonl}
        when: labels
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/live_changes.py:sinks"
    # The events, one flat row per event
    .add_egress(JsonlStorageEgress(default_path=f"{out}/events/orders.jsonl"))
    # The ground-truth records only
    .add_egress(
        JsonlStorageEgress(default_path=f"{out}/labels/labels.jsonl"),
        when="labels",
    )
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/live_changes_low_level.py:sinks"
    env.setup_egress(
        [
            # The events, one flat row per event
            JsonlStorageEgress(default_path=f"{out}/events/orders.jsonl"),
            # The ground-truth records only
            JsonlStorageEgress(default_path=f"{out}/labels/labels.jsonl"),
        ],
        labels=[False, True],
    )
    ```

    `labels` takes one entry per sink, in the order of the list.

Each sink writes one file per batch, such as `labels/labels_1a2b3c4d.jsonl`. The run writes these two labels:

```text
{"stream_type":"label","sim_ts":1200.0,"timestamp":"...","path":"Shop.service.packing.value","old":1.5,"new":3.0,"label":{"intervention":"printer_jam"}}
{"stream_type":"label","sim_ts":1800.0,"timestamp":"...","path":"Shop.service.packing.value","old":3.0,"new":1.5,"label":{"intervention":"printer_fixed"}}
```

`path`, `old` and `new` say what changed and by how much, and `label` says why. [Records and Telemetry](../reference/records-and-telemetry.md#record-shape) lists every field.

---

## Measure Against a Baseline

The labels say when the packing time went back to 1.5 seconds, but not when the orders stopped waiting longer because of it. To measure that, run the hour again without the jam, on the same seed, and compare the waits. Each order's wait is its `started` time less its `queued` time, read from the event files:

```python title="docs/snippets/tutorials/live_changes.py:read"
def read_rows(folder: Path) -> list:
    """Every row of every JSONL file the sink wrote to folder."""
    return [
        json.loads(line)
        for path in sorted(folder.glob("*.jsonl"))
        for line in path.read_text().splitlines()
    ]


def waits(out: Path) -> dict:
    """The wait of each order, by the time it was queued, in seconds."""
    queued, started = {}, {}
    for row in read_rows(out / "events"):
        if row.get("status") == "queued":
            queued[row["instance"]] = row["sim_ts"]
        elif row.get("status") == "started":
            started[row["instance"]] = row["sim_ts"]
    return {queued[i]: started[i] - queued[i] for i in started}


def mean_wait(by_time: dict, start: float, end: float) -> float:
    """The mean wait of the orders queued from start to end, in seconds."""
    window = [wait for at, wait in by_time.items() if start <= at < end]
    return sum(window) / len(window)
```

In YAML, delete the `scenario` block, run the file again in another folder, and read both folders with these functions. The Builder and Low-level scripts run both and print the results. The constant packing time draws no random numbers, so both runs meet the same orders at the same times, and each order's two waits can be compared one to one.

---

## Send It Live

A scenario is planned in advance. A change from outside arrives while the run goes on, from an ingress connector. `LocalIngress` is the one that needs no broker: it applies each change after a delay in wall-clock seconds, so the run must be paced. Here 5 minutes run at 200 simulated seconds a wall-clock second:

=== "YAML"

    ```yaml title="docs/snippets/yaml/live_changes_live.yaml:live"
    # Half a wall-clock second after the start, set packing to 3 seconds
    ingress:
      - type: Local
        config:
          schedule: [[0.5, Shop.service.packing.value, 3.0]]
    ```

=== "Builder"

    ```python title="docs/snippets/tutorials/live_changes.py:live"
    # Half a wall-clock second after the start, set packing to 3 seconds
    app.add_ingress(LocalIngress(schedule=[(0.5, "Shop.service.packing.value", 3.0)]))
    ```

=== "Low-level"

    ```python title="docs/snippets/tutorials/live_changes_low_level.py:live"
    # Half a wall-clock second after the start, set packing to 3 seconds
    ingress = [LocalIngress(schedule=[(0.5, "Shop.service.packing.value", 3.0)])]
    ```

    `env.setup_ingress(ingress)` starts it, and a SimPy process checks the ingress queue every 0.1 simulated seconds.

Half a wall-clock second is 100 simulated seconds at this pace, and packing changes to 3 seconds about then. The exact time depends on how fast the machine runs, so it differs from run to run. The change publishes no label: an ingress update changes the registry and nothing else, so the `labels/` folder is never created. To keep the ground truth of a live change, the sender records it itself. Send it live with `desflow run live_changes_live.yaml`, `python live_changes.py live` or `python live_changes_low_level.py live`. The two scripts print `packing after the live change: 3.0 s`. [Ingress](../user-guide/ingress.md) describes the connectors that read Kafka, Redis and PostgreSQL in the same way.

---

## Result

How long does the jam keep orders waiting after the fix? The orders queued during the jam wait **80.85 seconds** on average, against **1.50** without it. The orders queued in the 10 minutes after the fix still wait **11.93 seconds** on average, against **1.04**. The last order that waits longer than in the baseline is queued at **1,987.8 seconds**, about 3 minutes after the fix, so the delay notice should stay up for about 3 minutes more. The two labels, at **1,200** and **1,800 seconds**, tell a reader what changed and when.

The Builder and Low-level scripts print:

```text
label at 1200.0 s: printer_jam, packing 1.5 s to 3.0 s
label at 1800.0 s: printer_fixed, packing 3.0 s to 1.5 s
with the jam: mean wait  80.85 s from 20 to 30 min,  11.93 s from 30 to 40 min
  without it: mean wait   1.50 s from 20 to 30 min,   1.04 s from 30 to 40 min
the last order that waits longer is queued at 1987.8 s
```

## Why It Works

During the jam the packer finishes one order every 3 seconds, 0.33 a second, while 0.4 arrive, so about 40 orders build up in 10 minutes. After the fix the packer works the queue off at 0.67 less 0.4 orders a second, which takes a few minutes, so the effect of a change lasts beyond the change itself. A label names the cause, and only a comparison with a baseline on the same orders measures the effect, as [Output Analysis](../des/output-analysis.md#comparing-designs) explains for any two designs.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/live_changes.yaml"
        # The Live Changes tutorial: a label printer jam, written as a scenario.
        # Run it with: desflow run live_changes.yaml
        # The events are written to events/ and the ground-truth records to labels/.
        simulation:
          sim_id: Shop
          factor: 0
          random_seed: 42

        resources:
          packers: {current_cap: 1, max_cap: 1}

        services:
          packing: {dist: constant, value: 1.5 s}

        arrivals:
          orders: {dist: exponential, rate: 0.4, spawn: pack_order}

        tasks:
          pack_order:
            service: packing
            resource: packers
            payload: {event_type: order_packed}

        # The jam: 3 seconds of packing from 20 to 30 minutes, each change recorded
        scenario:
          - at: 20 min
            path: Shop.service.packing.value
            value: 3.0
            record: {intervention: printer_jam}
          - at: 30 min
            path: Shop.service.packing.value
            value: 1.5
            record: {intervention: printer_fixed}

        egress:
          # The events, one flat row per event
          - type: Jsonl
            config: {default_path: events/orders.jsonl}
          # The ground-truth records only
          - type: Jsonl
            config: {default_path: labels/labels.jsonl}
            when: labels

        run:
          until: 1 h
        ```

        ```yaml title="docs/snippets/yaml/live_changes_live.yaml"
        # The Live Changes tutorial: the same jam sent live, from a LocalIngress schedule.
        # Run it with: desflow run live_changes_live.yaml
        # The run is paced at 200 simulated seconds a wall-clock second, so 5 minutes take 1.5 s.
        simulation:
          sim_id: Shop
          factor: 0.005
          random_seed: 42

        resources:
          packers: {current_cap: 1, max_cap: 1}

        services:
          packing: {dist: constant, value: 1.5 s}

        arrivals:
          orders: {dist: exponential, rate: 0.4, spawn: pack_order}

        tasks:
          pack_order:
            service: packing
            resource: packers
            payload: {event_type: order_packed}

        # Half a wall-clock second after the start, set packing to 3 seconds
        ingress:
          - type: Local
            config:
              schedule: [[0.5, Shop.service.packing.value, 3.0]]

        egress:
          - type: Jsonl
            config: {default_path: events/orders.jsonl}
          - type: Jsonl
            config: {default_path: labels/labels.jsonl}
            when: labels

        run:
          until: 5 min
        ```

    === "Builder"

        ```python title="docs/snippets/tutorials/live_changes.py"
        """The Live Changes tutorial: a label printer jam, written as a scenario, builder.

        One packer and 1.5 seconds of packing; orders arrive at 0.4 a second. From 20 to 30
        minutes a jammed label printer slows packing to 3 seconds. The events are written to
        events/ and the ground-truth records of the jam to labels/, and the script prints
        the mean waits with and without the jam: python live_changes.py
        With the argument live, it sends the jam live: python live_changes.py live
        """

        import json
        import logging
        import shutil
        import sys
        from pathlib import Path

        from desflow import JsonlStorageEgress, LocalIngress, SimulationContext

        logging.basicConfig(level=logging.WARNING)


        def shop(out: Path, jam: bool = True) -> SimulationContext:
            """The shop of Changing Demand, writing its events and labels under out."""
            app = (
                SimulationContext(sim_id="Shop", factor=0, random_seed=42)
                .add_resource("packers", current_cap=1, max_cap=1)
                .add_service("packing", dist="constant", value=1.5)
                .add_arrival("orders", dist="exponential", rate=0.4)
                # The events, one flat row per event
                .add_egress(JsonlStorageEgress(default_path=f"{out}/events/orders.jsonl"))
                # The ground-truth records only
                .add_egress(
                    JsonlStorageEgress(default_path=f"{out}/labels/labels.jsonl"),
                    when="labels",
                )
            )
            if jam:
                # The jam: 3 seconds of packing from 20 to 30 minutes, each change recorded
                app.add_scenario(
                    [
                        {
                            "at": "20 min",
                            "path": "Shop.service.packing.value",
                            "value": 3.0,
                            "record": {"intervention": "printer_jam"},
                        },
                        {
                            "at": "30 min",
                            "path": "Shop.service.packing.value",
                            "value": 1.5,
                            "record": {"intervention": "printer_fixed"},
                        },
                    ]
                )

            @app.task(service_id="packing", resource_id="packers")
            def pack_order(task_id: int):
                return {"event_type": "order_packed"}

            @app.arrival_loop("orders")
            def order_generator(context: SimulationContext):
                task_id = 0
                while True:
                    order = yield context.wait_for_arrival("orders")
                    context.spawn(pack_order(task_id, entity=order))
                    task_id += 1

            return app


        def read_rows(folder: Path) -> list:
            """Every row of every JSONL file the sink wrote to folder."""
            return [
                json.loads(line)
                for path in sorted(folder.glob("*.jsonl"))
                for line in path.read_text().splitlines()
            ]


        def waits(out: Path) -> dict:
            """The wait of each order, by the time it was queued, in seconds."""
            queued, started = {}, {}
            for row in read_rows(out / "events"):
                if row.get("status") == "queued":
                    queued[row["instance"]] = row["sim_ts"]
                elif row.get("status") == "started":
                    started[row["instance"]] = row["sim_ts"]
            return {queued[i]: started[i] - queued[i] for i in started}


        def mean_wait(by_time: dict, start: float, end: float) -> float:
            """The mean wait of the orders queued from start to end, in seconds."""
            window = [wait for at, wait in by_time.items() if start <= at < end]
            return sum(window) / len(window)


        def outcomes(out: Path) -> dict:
            """Runs the hour with and without the jam; returns the labels and mean waits."""
            result: dict = {}
            by_run: dict = {}
            for jam in (True, False):
                folder = out / ("jam" if jam else "baseline")
                shutil.rmtree(folder, ignore_errors=True)  # a run adds files; start empty
                shop(folder, jam).run(until="1 h")
                by_time = by_run[jam] = waits(folder)
                result[jam] = {
                    "jam": round(mean_wait(by_time, 1200, 1800), 2),
                    "after": round(mean_wait(by_time, 1800, 2400), 2),
                }
            # Both runs meet the same orders, so each order's two waits can be compared
            result["last_longer"] = max(
                at for at, wait in by_run[True].items() if wait > by_run[False][at] + 0.001
            )
            labels = read_rows(out / "jam" / "labels")
            result["labels"] = [
                (row["sim_ts"], row["label"]["intervention"], row["old"], row["new"])
                for row in sorted(labels, key=lambda row: row["sim_ts"])
            ]
            return result


        def live(out: Path) -> SimulationContext:
            """The same jam sent live, half a second of wall-clock time after the start."""
            app = shop(out, jam=False)
            app.factor = 0.005
            # Half a wall-clock second after the start, set packing to 3 seconds
            app.add_ingress(LocalIngress(schedule=[(0.5, "Shop.service.packing.value", 3.0)]))
            app.run(until="5 min")
            return app


        if __name__ == "__main__":
            here = Path("live_changes_output")
            if sys.argv[1:] == ["live"]:
                # python live_changes.py live: the jam sent live instead
                # A run adds files; start empty
                shutil.rmtree(here / "live", ignore_errors=True)
                env = live(here / "live").env
                packing = env.registry.get_config("Shop.service.packing").value
                print(f"packing after the live change: {packing} s")
            else:
                r = outcomes(here)
                for at, name, old, new in r["labels"]:
                    print(f"label at {at:6.1f} s: {name}, packing {old} s to {new} s")
                for jam, label in ((True, "with the jam"), (False, "without it")):
                    print(
                        f"{label:>12}: mean wait {r[jam]['jam']:6.2f} s from 20 to 30 min, "
                        f"{r[jam]['after']:6.2f} s from 30 to 40 min"
                    )
                print(f"the last order that waits longer is queued at {r['last_longer']:.1f} s")
        ```

    === "Low-level"

        ```python title="docs/snippets/tutorials/live_changes_low_level.py"
        """The Live Changes tutorial: a label printer jam, written as a scenario, low-level.

        One packer and 1.5 seconds of packing; orders arrive at 0.4 a second. From 20 to 30
        minutes a jammed label printer slows packing to 3 seconds. The events are written to
        events/ and the ground-truth records of the jam to labels/, and the script prints
        the mean waits with and without the jam: python live_changes_low_level.py
        With the argument live, it sends the jam live: python live_changes_low_level.py live
        """

        import copy
        import itertools
        import json
        import logging
        import shutil
        import sys
        from pathlib import Path

        from desflow import (
            CapacityConfig,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            DynamicResource,
            JsonlStorageEgress,
            LocalIngress,
            Sampler,
            SimParameter,
            run_scenario,
        )

        logging.basicConfig(level=logging.WARNING)

        PARAMS = SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
            service={"packing": DistributionConfig(dist="constant", value=1.5)},
            resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
        )

        # The jam: 3 seconds of packing from 20 to 30 minutes, each change recorded
        JAM = [
            {
                "at": 1200,
                "path": "Shop.service.packing.value",
                "value": 3.0,
                "record": {"intervention": "printer_jam"},
            },
            {
                "at": 1800,
                "path": "Shop.service.packing.value",
                "value": 1.5,
                "record": {"intervention": "printer_fixed"},
            },
        ]


        def pack_order(env, sampler, packers, key, task_id):
            path_id = "Shop.service.packing"

            def publish(value):
                env.publish_event(key, value, task="pack_order", instance=task_id)

            publish({"path_id": path_id, "status": "queued"})
            with packers.request() as request:
                yield request
                publish({"path_id": path_id, "status": "started"})
                yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
                publish({"event_type": "order_packed"})


        def order_generator(env, sampler, packers):
            arrival = env.registry.get_config("Shop.arrival.orders")
            for task_id in itertools.count():
                yield env.timeout(sampler.sample(arrival))
                key = f"orders-{task_id + 1}"
                env.process(pack_order(env, sampler, packers, key, task_id))


        def run(
            out: Path, jam: bool = True, factor: float = 0.0, until: float = 3600, ingress=None
        ):
            # strict=False lets a paced run fall behind the wall clock without stopping
            env = DynamicRealtimeEnvironment(factor=factor, strict=False)
            # A copy, because a change to the registry changes the parameters it holds
            env.registry.register_sim_parameter(copy.deepcopy(PARAMS))
            env.setup_egress(
                [
                    # The events, one flat row per event
                    JsonlStorageEgress(default_path=f"{out}/events/orders.jsonl"),
                    # The ground-truth records only
                    JsonlStorageEgress(default_path=f"{out}/labels/labels.jsonl"),
                ],
                labels=[False, True],
            )
            if ingress:
                env.setup_ingress(ingress)
            packers = DynamicResource(env, "Shop", "packers")
            if jam:
                # Each step applies before every other event at its time
                env.process(run_scenario(env, JAM, until=until))
            env.process(order_generator(env, Sampler(seed=42), packers))
            try:
                env.run(until=until)
            finally:
                env.teardown()
            return env


        def read_rows(folder: Path) -> list:
            """Every row of every JSONL file the sink wrote to folder."""
            return [
                json.loads(line)
                for path in sorted(folder.glob("*.jsonl"))
                for line in path.read_text().splitlines()
            ]


        def waits(out: Path) -> dict:
            """The wait of each order, by the time it was queued, in seconds."""
            queued, started = {}, {}
            for row in read_rows(out / "events"):
                if row.get("status") == "queued":
                    queued[row["instance"]] = row["sim_ts"]
                elif row.get("status") == "started":
                    started[row["instance"]] = row["sim_ts"]
            return {queued[i]: started[i] - queued[i] for i in started}


        def mean_wait(by_time: dict, start: float, end: float) -> float:
            """The mean wait of the orders queued from start to end, in seconds."""
            window = [wait for at, wait in by_time.items() if start <= at < end]
            return sum(window) / len(window)


        def outcomes(out: Path) -> dict:
            """Runs the hour with and without the jam; returns the labels and mean waits."""
            result: dict = {}
            by_run: dict = {}
            for jam in (True, False):
                folder = out / ("jam" if jam else "baseline")
                shutil.rmtree(folder, ignore_errors=True)  # a run adds files; start empty
                run(folder, jam)
                by_time = by_run[jam] = waits(folder)
                result[jam] = {
                    "jam": round(mean_wait(by_time, 1200, 1800), 2),
                    "after": round(mean_wait(by_time, 1800, 2400), 2),
                }
            # Both runs meet the same orders, so each order's two waits can be compared
            result["last_longer"] = max(
                at for at, wait in by_run[True].items() if wait > by_run[False][at] + 0.001
            )
            labels = read_rows(out / "jam" / "labels")
            result["labels"] = [
                (row["sim_ts"], row["label"]["intervention"], row["old"], row["new"])
                for row in sorted(labels, key=lambda row: row["sim_ts"])
            ]
            return result


        def live(out: Path):
            """The same jam sent live, half a second of wall-clock time after the start."""
            # Half a wall-clock second after the start, set packing to 3 seconds
            ingress = [LocalIngress(schedule=[(0.5, "Shop.service.packing.value", 3.0)])]
            return run(out, jam=False, factor=0.005, until=300, ingress=ingress)


        if __name__ == "__main__":
            here = Path("live_changes_output")
            if sys.argv[1:] == ["live"]:
                # python live_changes_low_level.py live: the jam sent live instead
                # A run adds files; start empty
                shutil.rmtree(here / "live", ignore_errors=True)
                env = live(here / "live")
                packing = env.registry.get_config("Shop.service.packing").value
                print(f"packing after the live change: {packing} s")
            else:
                r = outcomes(here)
                for at, name, old, new in r["labels"]:
                    print(f"label at {at:6.1f} s: {name}, packing {old} s to {new} s")
                for jam, label in ((True, "with the jam"), (False, "without it")):
                    print(
                        f"{label:>12}: mean wait {r[jam]['jam']:6.2f} s from 20 to 30 min, "
                        f"{r[jam]['after']:6.2f} s from 30 to 40 min"
                    )
                print(f"the last order that waits longer is queued at {r['last_longer']:.1f} s")
        ```

Next, [Streaming Out](streaming-out.md) sends the shop's records to the terminal and to files, and shows which records reach each.
