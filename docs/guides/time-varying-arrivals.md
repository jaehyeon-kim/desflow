# Time-varying Arrivals

Change the arrival rate and the staffing at set times, such as a rush at lunch or a second packer for an afternoon, and know exactly which events a change reaches. It builds on [Changing Demand](../tutorials/changing-demand.md), which asks what a rush does to waits, and on [Live Parameters](../user-guide/live-parameters.md), which explains registry paths.

---

## Steps

1. Find the registry path of each parameter that changes: `Shop.arrival.orders.rate` for an exponential arrival, `Shop.resources.packers.current_cap` for the number of packers.
2. Write one step for each change, with the simulation time it applies at, `at`, in seconds or as a duration such as `20 min`.
3. Return each parameter to its usual value with a later step. A schedule does not repeat on its own.

The blueprint of the same name in [Changing Demand](../tutorials/changing-demand.md) runs for an hour and packs in a constant 1.5 seconds. Its rush lasts from 20 to 40 minutes, with an order every 1.11 seconds on average (`rate: 0.9`). The example below, `examples/yaml/changing_demand.yaml`, runs for 5 minutes, and packing takes 1 to 3 seconds, most often 2 (a triangular distribution). From 120 to 180 seconds orders arrive twice as fast, an order every 1.25 seconds on average (`rate: 0.8`), and a second packer works for that time:

=== "YAML"

    ```yaml title="examples/yaml/changing_demand.yaml:schedule"
    scenario:
      # The rush: twice the orders, and a second packer, from 120 to 180 seconds
      - {at: 120, path: Shop.arrival.orders.rate, value: 0.8}
      - {at: 120, path: Shop.resources.packers.current_cap, value: 2}
      - {at: 180, path: Shop.arrival.orders.rate, value: 0.4}
      - {at: 180, path: Shop.resources.packers.current_cap, value: 1}
    ```

=== "Builder"

    ```python title="examples/builder/changing_demand_example.py:schedule"
    # The rush: twice the orders, and a second packer, from 120 to 180 seconds
    app.add_scenario(
        [
            {"at": 120, "path": "Shop.arrival.orders.rate", "value": 0.8},
            {"at": 120, "path": "Shop.resources.packers.current_cap", "value": 2},
            {"at": 180, "path": "Shop.arrival.orders.rate", "value": 0.4},
            {"at": 180, "path": "Shop.resources.packers.current_cap", "value": 1},
        ]
    )
    ```

    `add_step(at, path=..., value=...)` adds one step. A step can also carry a `record`, a ground-truth label, or an `interrupt`.

=== "Low-level"

    ```python title="examples/low-level/changing_demand_example.py:schedule"
    # The rush: twice the orders, and a second packer, from 120 to 180 seconds
    SCHEDULE = [
        {"at": 120, "path": "Shop.arrival.orders.rate", "value": 0.8},
        {"at": 120, "path": "Shop.resources.packers.current_cap", "value": 2},
        {"at": 180, "path": "Shop.arrival.orders.rate", "value": 0.4},
        {"at": 180, "path": "Shop.resources.packers.current_cap", "value": 1},
    ]
    ```

    `run_scenario(env, SCHEDULE, until=300)` is the SimPy process that applies the steps. Start it with `env.process`, beside the arrivals.

The steps change the registry, so everything that reads the path follows: the next arrival gap is drawn from the new rate, and the packers grant or owe units as on [Resources](../user-guide/resources/index.md#under-live-changes).

---

## Boundaries

A step that applies at the same time as another event has to come first or second, and a run that ends at a step's time has to apply it or not. Both are settled the way SimPy settles its own end time:

- **A step at time t applies before every other event at t.** It is scheduled as an urgent event, as SimPy's `run(until=t)` is, so an arrival at t draws its next gap from the new value.
- **Steps at the same time apply in the order written.**
- **A step at or after `until` never applies,** as an event due at exactly `until` does not run.

This function runs a shop with an order every 10 seconds, and a step at 30 seconds that makes the gap 5 seconds:

```python title="docs/snippets/guides/time_varying_arrivals.py:boundary"
def arrivals(until: float) -> tuple:
    """Orders every 10 s, every 5 s from 30 s: the arrival times, and the gap in
    force at the end."""
    app = (
        SimulationContext("Shop", factor=0)
        .add_arrival("orders", dist="constant", value=10)
        .add_step(30, path="Shop.arrival.orders.value", value=5)
    )
    times = []

    @app.arrival_loop("orders")
    def order_loop(context):
        while True:
            yield context.wait_for_arrival("orders")
            times.append(context.env.now)

    app.run(until=until)
    return times, app.env.registry.get("Shop.arrival.orders.value").value
```

`arrivals(50)` returns arrivals at 10, 20, **30, 35**, 40 and 45 seconds, and the gap in force at the end is 5 seconds: the order that arrived at 30 drew its gap after the step. `arrivals(30)` returns arrivals at 10 and 20 seconds, and the gap in force at the end is 10 seconds: the step at 30 is at `until`, so it never applies, and neither does the arrival due at 30.

---

## Example

The `changing_demand` example is the model above: one packer, an order every 2.5 seconds on average (`rate: 0.4`), and a rush with a second packer from 120 to 180 seconds. It needs no Docker service and ends on its own after 300 simulation seconds.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get changing-demand --api yaml
        cd changing-demand/yaml

        # 3. Run it
        desflow run changing_demand.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get changing-demand --api yaml
        cd changing-demand/yaml

        # 3. Run it
        desflow run changing_demand.yaml
        ```

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get changing-demand --api builder
        cd changing-demand/builder

        # 3. Run it
        uv run --no-project --with desflow changing_demand_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get changing-demand --api builder
        cd changing-demand/builder

        # 3. Run it
        python changing_demand_example.py
        ```

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get changing-demand --api low-level
        cd changing-demand/low-level

        # 3. Run it
        uv run --no-project --with desflow changing_demand_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get changing-demand --api low-level
        cd changing-demand/low-level

        # 3. Run it
        python changing_demand_example.py
        ```
