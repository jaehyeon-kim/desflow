# Serve Entities Together

Serve several entities with one run of a resource, such as an oven that bakes a tray of loaves at a time. The entities wait together in a batch, the resource serves the whole batch once, and each entity then goes on by itself.

---

## Before You Start

- [Several Resources](../../user-guide/resources/several-resources.md#several-resources) describes how a task holds a resource and what its queue does.
- [Tasks and Processes](../../user-guide/tasks-and-processes/index.md) describes `next`, which each member keeps. [Add Reneging and Balking](reneging-and-balking.md) shows `patience` and [Interruptions](../../learn/shop-logic/interruptions.md) shows `on_interrupt`, which each member keeps too.

---

## Steps

### Group the Entities in a Batch

`batch: {size, timeout, close_when, name}` makes the entities that start the task wait together. The batch closes when `size` members have joined, or `timeout` after its first member, whichever comes first. The task's `resource` and service then run once for the whole batch. Every member publishes `started` at the close, and goes on with its own payload and its own `next`, in the order the members joined.

`size` is a number, or text that is read as an expression for the first member when it joins. A size of 1 or less closes the batch at its first member. `timeout` is a duration.

Here seven orders arrive 1 second apart, and an oven bakes them in batches of 3, or sooner when 2.5 seconds pass. The oven takes 4 seconds for a batch:

=== "YAML"

    ```yaml title="docs/snippets/yaml/serve_together.yaml:batch"
    bake:
      # The batch closes with 3 members, or 2.5 seconds after its first member
      batch: {size: 3, timeout: 2.5}
      resource: oven  # one run of the oven serves the whole batch
      service_time: 4
      payload: {event_type: baked}
    ```

=== "Builder"

    ```python title="docs/snippets/guides/serve_together.py:batch"
    # The batch closes with 3 members, or 2.5 seconds after its first member
    @app.task(
        batch={"size": 3, "timeout": 2.5},
        resource_id="oven",  # one run of the oven serves the whole batch
        service_time=4,
    )
    def bake(task_id):
        return {"event_type": "baked"}
    ```

=== "Low-level"

    ```python title="docs/snippets/guides/serve_together_low_level.py:batch"
    def closes_by_size(batch):
        return len(batch["members"]) == SIZE

    def close(batch):
        if not batch["closed"].triggered:
            batch["closed"].succeed()
            open_batch.clear()

    def timer(batch):
        yield env.timeout(TIMEOUT)  # 2.5 seconds after the first member
        close(batch)

    def member(key, batch):
        batch["members"].append(key)
        if closes(batch):  # tested each time a member joins
            close(batch)
        yield batch["closed"]
        seen.append((env.now, key, "started"))  # every member, at the close
        if key == batch["members"][0]:
            # The first member runs the oven once for the whole batch
            with oven.request() as request:
                yield request
                yield env.timeout(SERVICE)
            batch["served"].succeed()
        yield batch["served"]
        seen.append((env.now, key, "baked"))  # every member goes on by itself
    ```

The Low-level form has no batch to call, so the script keeps a list of members and an event that closes it. `closes_by_size` is the test made each time a member joins. The first member runs the oven once, and every member waits for that run to end.

### Close a Batch by an Expression

`close_when` is an expression that is read each time a member joins. It reads `batch.size`, the number of members, and `batch.age`, the seconds since the first member joined. Neither name works anywhere else. Because the expression is read only when a member joins, a batch does not close by `batch.age` alone while no new member joins. Add a `timeout` to a batch that must close without one.

=== "YAML"

    ```yaml title="docs/snippets/yaml/serve_together_close_when.yaml:close"
    # Close with 2 members once the first has waited 2 seconds, or at 5 members
    batch: {size: 5, close_when: batch.size >= 2 and batch.age >= 2}
    ```

=== "Builder"

    `@app.task(batch=...)` takes the same mapping as a dict: `batch={"size": 5, "close_when": "batch.size >= 2 and batch.age >= 2"}`.

=== "Low-level"

    ```python title="docs/snippets/guides/serve_together_low_level.py:close"
    def closes_by_expression(batch):
        """batch.size >= 2 and batch.age >= 2, or 5 members."""
        size = len(batch["members"])
        age = env.now - batch["since"]  # seconds since the first member joined
        return size == 5 or (size >= 2 and age >= 2)
    ```

    `member` calls this function each time a member joins, in place of `closes_by_size`, and the batch has no timer.

Run the Builder script as `python serve_together.py close_when` to print these lines; the YAML file and the Low-level script give the same:

```text
check: (3.0, 'orders-1', 'started')
check: (3.0, 'orders-2', 'started')
check: (3.0, 'orders-3', 'started')
check: (6.0, 'orders-4', 'started')
check: (6.0, 'orders-5', 'started')
check: (6.0, 'orders-6', 'started')
check: (7.0, 'orders-1', 'baked')
check: (7.0, 'orders-2', 'baked')
check: (7.0, 'orders-3', 'baked')
check: (11.0, 'orders-4', 'baked')
check: (11.0, 'orders-5', 'baked')
check: (11.0, 'orders-6', 'baked')
```

The batch closes at the first join after its first member has waited 2 seconds, which is the third order, at 3 seconds. The second batch closes at 6 in the same way, and the oven bakes it from 7 to 11. The seventh order joins a new batch at 7 and waits for ever, because the batch has no `timeout` and no member joins after it.

### Share a Batch Between Tasks

`name` names the batch, and it defaults to the name of the task. Tasks that use the same `name` share one open batch, so a batch can hold bread and rolls together. They must have the same `size`, `timeout`, `close_when`, `resource` and service (`service` or `service_time`). A file in which they differ is refused when it loads, and the message names both tasks. Values are compared by what they mean, so a generator written the same way, an expression with other spacing and a number written as text agree.

### Know What Happens to a Member

Each member keeps its own `on_interrupt`, `patience`, payload and `next`.

- An interrupt before the close takes the member out of the batch. Under `resume` and `restart` it joins again at the back, its wait counts the time it spent in the batch, and it publishes `interrupted` and no `resumed`.
- After the close, the member that serves the batch publishes `interrupted` when it is interrupted while it waits for the unit, under `resume` or `restart`, and publishes `resumed` when it is granted the unit.
- Any other member of a closed batch publishes `interrupted` and `resumed` at the same instant, and goes on waiting to be served by the member that serves.
- Under `on_interrupt: abandon` a member leaves the batch, and `size` counts the members that are left. If the member that serves leaves, the next member serves, and its service starts again.
- A member's `patience` counts until the batch closes. A member that reneges leaves the batch. A closed batch does not renege while its service runs.
- A member that balks at a full line leaves, the next member asks for the unit, and it balks too.
- A failure of a member's task fails that member once, and it goes to its own `on_fail`.

A batch takes no `get`, `seize`, `open` or `cancel`.

---

## Check the Result

Run the Builder script, and keep the lines that start with `check:`. Each line is the time, the order and the status of one record:

```bash
# With uv
uv run --no-project --with desflow serve_together.py
# With pip
pip install desflow
python serve_together.py
```

```text
check: (3.0, 'orders-1', 'started')
check: (3.0, 'orders-2', 'started')
check: (3.0, 'orders-3', 'started')
check: (6.0, 'orders-4', 'started')
check: (6.0, 'orders-5', 'started')
check: (6.0, 'orders-6', 'started')
check: (7.0, 'orders-1', 'baked')
check: (7.0, 'orders-2', 'baked')
check: (7.0, 'orders-3', 'baked')
check: (9.5, 'orders-7', 'started')
check: (11.0, 'orders-4', 'baked')
check: (11.0, 'orders-5', 'baked')
check: (11.0, 'orders-6', 'baked')
check: (15.0, 'orders-7', 'baked')
```

The first batch closes at 3 seconds, when its third order joins. The second closes at 6, and its members publish `started` then, but the oven is busy until 7, so they finish at 11. The seventh order is alone, so its batch closes at 9.5, 2.5 seconds after it joined, and the oven bakes it from 11 to 15.

Run `serve_together_low_level.py` for the Low-level form. It prints the same rows, with whole numbers for the times it gets from whole-number timeouts, such as `(3, 'orders-1', 'started')`.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/serve_together.yaml"
        # The Serve Entities Together guide: seven orders arrive 1 second apart, and an
        # oven bakes them in batches of 3, or sooner when 2.5 seconds pass. Run it with:
        # desflow run serve_together.yaml --stdout
        simulation: {sim_id: Bakery, factor: 0, random_seed: 42}

        resources:
          oven: {current_cap: 1}

        arrivals:
          orders: {dist: constant, value: 1, max_events: 7, spawn: bake}

        tasks:
          bake:
            # The batch closes with 3 members, or 2.5 seconds after its first member
            batch: {size: 3, timeout: 2.5}
            resource: oven  # one run of the oven serves the whole batch
            service_time: 4
            payload: {event_type: baked}

        run: {until: 30}
        ```

    === "Builder"

        ```python title="docs/snippets/guides/serve_together.py"
        """The Serve Entities Together guide, builder API: seven orders arrive 1 second apart,
        and an oven bakes them in batches of 3, or sooner when 2.5 seconds pass.

        `batch_records(blueprint)` returns the (time, key, status) of each `started` and `baked`
        record; with a path, it runs that blueprint, and without one, it builds the model, with
        a batch that closes by an expression when `close_when` is true.
        """

        import asyncio
        import queue
        import sys

        from desflow import SimulationContext


        class Capture:
            """An egress that keeps every event it receives."""

            def __init__(self):
                self.events = []

            async def run(self, egress_queue):
                try:
                    while True:
                        try:
                            batch = egress_queue.get_nowait()
                            self.events += [r for r in batch if r["stream_type"] == "event"]
                        except queue.Empty:
                            await asyncio.sleep(0.005)
                except asyncio.CancelledError:
                    pass


        def _app() -> SimulationContext:
            app = SimulationContext("Bakery", factor=0)
            app.add_resource("oven", current_cap=1)
            app.add_arrival("orders", "constant", value=1, max_events=7, spawn="bake")
            return app


        def build() -> SimulationContext:
            app = _app()

            # The batch closes with 3 members, or 2.5 seconds after its first member
            @app.task(
                batch={"size": 3, "timeout": 2.5},
                resource_id="oven",  # one run of the oven serves the whole batch
                service_time=4,
            )
            def bake(task_id):
                return {"event_type": "baked"}

            return app


        def build_close_when() -> SimulationContext:
            app = _app()

            # Close with 2 members once the first has waited 2 seconds, or at 5 members
            @app.task(
                batch={"size": 5, "close_when": "batch.size >= 2 and batch.age >= 2"},
                resource_id="oven",
                service_time=4,
            )
            def bake(task_id):
                return {"event_type": "baked"}

            return app


        def batch_records(blueprint: str = "", close_when: bool = False) -> list:
            if blueprint:
                app = SimulationContext.from_yaml(blueprint)
            else:
                app = build_close_when() if close_when else build()
            capture = Capture()
            app.add_egress(capture)
            app.run(until=30)
            return sorted(
                (e["sim_ts"], e["key"], e["value"].get("status", "baked"))
                for e in capture.events
                if e["value"].get("status", "baked") in ("started", "baked")
            )


        if __name__ == "__main__":
            for row in batch_records(close_when="close_when" in sys.argv[1:]):
                print("check:", row)
        ```

    === "Low-level"

        ```python title="docs/snippets/guides/serve_together_low_level.py"
        """The Serve Entities Together guide, low-level API: seven orders arrive 1 second apart,
        and an oven bakes them in batches of 3, or sooner when 2.5 seconds pass.

        SimPy has no batch, so a batch is a list of members and an event that closes it.
        `batch_records()` returns the (time, key, status) of each `started` and `baked` record,
        for a batch that closes by size and timeout, or by an expression when `close_when` is
        true.
        """

        from simpy import Resource

        from desflow import DynamicRealtimeEnvironment

        SIZE = 3  # members that close a batch
        TIMEOUT = 2.5  # seconds after the first member that close it
        SERVICE = 4  # seconds the oven takes for a whole batch


        def batch_records(close_when: bool = False) -> list:
            env = DynamicRealtimeEnvironment(factor=0)
            oven = Resource(env, capacity=1)
            seen = []
            open_batch: list = []  # the batch that still takes members: none, or one

            def closes_by_size(batch):
                return len(batch["members"]) == SIZE

            def close(batch):
                if not batch["closed"].triggered:
                    batch["closed"].succeed()
                    open_batch.clear()

            def timer(batch):
                yield env.timeout(TIMEOUT)  # 2.5 seconds after the first member
                close(batch)

            def member(key, batch):
                batch["members"].append(key)
                if closes(batch):  # tested each time a member joins
                    close(batch)
                yield batch["closed"]
                seen.append((env.now, key, "started"))  # every member, at the close
                if key == batch["members"][0]:
                    # The first member runs the oven once for the whole batch
                    with oven.request() as request:
                        yield request
                        yield env.timeout(SERVICE)
                    batch["served"].succeed()
                yield batch["served"]
                seen.append((env.now, key, "baked"))  # every member goes on by itself


            def closes_by_expression(batch):
                """batch.size >= 2 and batch.age >= 2, or 5 members."""
                size = len(batch["members"])
                age = env.now - batch["since"]  # seconds since the first member joined
                return size == 5 or (size >= 2 and age >= 2)


            closes = closes_by_expression if close_when else closes_by_size

            def orders():
                for number in range(1, 8):
                    yield env.timeout(1)
                    if not open_batch:
                        batch = {
                            "members": [],
                            "since": env.now,
                            "closed": env.event(),
                            "served": env.event(),
                        }
                        open_batch.append(batch)
                        if not close_when:
                            env.process(timer(batch))
                    env.process(member(f"orders-{number}", open_batch[0]))

            env.process(orders())
            env.run(until=30)
            env.teardown()
            return sorted(seen)


        if __name__ == "__main__":
            for row in batch_records():
                print("check:", row)
        ```
