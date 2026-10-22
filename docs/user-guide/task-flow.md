# Task Flow: the Next Task by Condition or Probability

An entity rarely leaves after completing just one task. An order is checked, perhaps wrapped as a gift, packed, sometimes packed again, and shipped. This page shows how a finished task chooses the next one, and what the entity carries from task to task.

---

## What It Is

A **flow** is the sequence of tasks one entity passes through. Each task names what follows it: always the same task, the first of several rules whose condition holds, or a task drawn at random by weight. A rule can send the entity back to a task it has passed, for **rework**. A task that names nothing ends the flow. The entity keeps its key and its **attributes** through the whole flow, and every task can read them.

[A Flow of Tasks](../tutorials/flow-of-tasks.md) builds a pick, pack and ship flow step by step.

---

## How to Use It

### Choose the Next Task

`next` takes one of these forms:

- **A task name**, `next: pack_order`: always that task.
- **Rules**, `when` with an optional `default`: each rule is a condition and a task. Rules are tried in order. The first matching rule wins, and `default` applies if none matches. With no default and no matching rule, the flow ends.
- **Weights**, below.
- **In the builder, a function** that takes the `Upstream` and returns a task name or `None`.

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_flow.yaml:condition"
    check_order:
      payload: {total: 80}
      # Rules are tried in order; the first match wins. Otherwise, use the default.
      next:
        when:
          - {condition: "level.stock == 0", task: backorder}
          - {condition: "payload.total >= 100 or attr.gift == 1", task: wrap_gift}
        default: pack_order
    ```

=== "Builder"

    ```python title="docs/snippets/pages/task_flow.py:builder-condition"
    # Rules are tried in order; the first match wins. Otherwise, use the default.
    @app.task(
        next={
            "when": [
                {"condition": "level.stock == 0", "task": "backorder"},
                {
                    "condition": "payload.total >= 100 or attr.gift == 1",
                    "task": "wrap_gift",
                },
            ],
            "default": "pack_order",
        }
    )
    def check_order(task_id: int):
        return {"total": 80}
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/task_flow.py:low-level-condition"
    # The same rules as next, as a Router; choose() returns the next task's name
    route = Router(
        {
            "when": [{"condition": "attr.gift == 1", "task": "wrap_gift"}],
            "default": "pack_order",
        }
    )
    order = Entity("orders-1", "orders", {"gift": 0})  # key, arrival, attributes
    finished = Upstream(0, order.key, order.attributes, {"total": 80})
    assert route.choose(finished, env=None) == "pack_order"
    ```

    A `Router` takes the same rules as `next`. When a task finishes, its process builds the `Upstream`, asks `route.choose(upstream, env, levels=...)` for the next task's name, and starts that task with the same entity. `env` lets a condition read `registry[...]` and `t`, and `levels`, a mapping of store and container names to the objects, lets it read `level.<name>`.

The condition is evaluated when the task finishes, after its `set`. `payload.` reads the payload of the task that just finished, `attr.` the entity's attributes, `level.` a store's or container's level, and `registry["..."]` a live parameter. How It Works lists everything a condition can use.

### Weights

`weights` maps task names to numbers, and the next task is drawn in proportion to them. The draw comes from a random stream of the routing task's own, `route.<task>`, so a seeded run repeats it and it changes no other draw:

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_flow.yaml:weights" hl_lines="7 8"
    pack_order:
      service: packing
      resource: packers
      get: {container: stock, amount: 1}
      payload: {event_type: order_packed}
      # A task drawn in proportion to its weight: one in ten is packed again
      next:
        weights: {ship_order: 9, pack_order: 1}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/task_flow.py:builder-weights" hl_lines="6"
    # A task drawn in proportion to its weight: one in ten is packed again
    @app.task(
        service_id="packing",
        resource_id="packers",
        get={"container": "stock", "amount": 1},
        next={"weights": {"ship_order": 9, "pack_order": 1}},
    )
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/task_flow.py:low-level-weights"
    # Weights draw from a generator: the routing task's own stream, as the builder's do
    rework = Router({"weights": {"ship_order": 9, "pack_order": 1}})
    rework.rng = Sampler(seed=42).generator("route.pack_order")
    choices = [rework.choose(finished) for _ in range(1000)]
    ```

### Attributes

An arrival gives each new entity its starting `attributes`, and a task's `set` changes them when the task finishes. Each entity gets its own copy. Attributes are not published: a condition reads them as `attr.<name>`, and a payload includes them only when it asks.

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_flow.yaml:attributes"
    arrivals:
      # Each order starts with these attributes, read as attr.gift
      orders: {dist: exponential, rate: 0.5, spawn: check_order, attributes: {gift: 0}}
      gifts: {dist: exponential, rate: 0.1, spawn: check_order, attributes: {gift: 1}}
    ```

    ```yaml title="docs/snippets/yaml/task_flow.yaml:set"
    # Applied to the order's attributes when the task finishes
    set: {backordered: 1}
    ```

    A task's `attributes` list adds the named attributes to a mapping payload, as `ship_order` does in the full blueprint.

=== "Builder"

    ```python title="docs/snippets/pages/task_flow.py:builder-attributes"
    # Each order starts with these attributes, read as attr.gift
    app.add_arrival("orders", dist="exponential", rate=0.5, attributes={"gift": 0})
    app.add_arrival("gifts", dist="exponential", rate=0.1, attributes={"gift": 1})
    ```

    ```python title="docs/snippets/pages/task_flow.py:builder-set"
    # `set` is applied to the order's attributes when the task finishes
    @app.task(service_id="supplying", set={"backordered": 1}, next="check_order")
    def backorder(task_id: int):
        return {"event_type": "order_backordered"}
    ```

=== "Low-level"

    An `Entity(key, arrival, attributes)` carries them; the process updates `entity.attributes` itself after a task, as the low-level tab of [A Flow of Tasks](../tutorials/flow-of-tasks.md) shows.

### Entity Key

Each arrival starts a new entity, keyed by the arrival's name and a counter from 1: `orders-1`, `orders-2`. Every event of every task in its flow carries that key as its record key, so a sink can follow one order through the whole flow, and Kafka keeps its events in order on one partition. The builder can replace the key with a function of the counter:

=== "YAML"

    A blueprint uses the default key.

=== "Builder"

    ```python title="docs/snippets/pages/task_flow.py:builder-key"
    # Keys ret-00001, ret-00002 instead of returns-1, from the counter, which starts at 1
    app.add_arrival("returns", dist="exponential", rate=0.05, key=lambda n: f"ret-{n:05d}")
    ```

=== "Low-level"

    The process that creates each `Entity` gives it its key.

These are the finished events of `gifts-5` in the blueprint above: checked while the shelf was empty, backordered, checked again, wrapped, packed and shipped, all under one key:

<!-- records: gift-flow -->
```text
[EVT] {'sim_ts': 19.216, 'timestamp': '...', 'key': 'gifts-5', 'task': 'check_order', 'instance': 13, 'value': {'total': 80}}
[EVT] {'sim_ts': 49.216, 'timestamp': '...', 'key': 'gifts-5', 'task': 'backorder', 'instance': 1, 'value': {'event_type': 'order_backordered'}}
[EVT] {'sim_ts': 49.216, 'timestamp': '...', 'key': 'gifts-5', 'task': 'check_order', 'instance': 34, 'value': {'total': 80}}
[EVT] {'sim_ts': 52.216, 'timestamp': '...', 'key': 'gifts-5', 'task': 'wrap_gift', 'instance': 7, 'value': {'event_type': 'gift_wrapped'}}
[EVT] {'sim_ts': 54.362, 'timestamp': '...', 'key': 'gifts-5', 'task': 'pack_order', 'instance': 29, 'value': {'event_type': 'order_packed'}}
[EVT] {'sim_ts': 54.362, 'timestamp': '...', 'key': 'gifts-5', 'task': 'ship_order', 'instance': 27, 'value': {'event_type': 'order_shipped', 'gift': 1, 'backordered': 1}}
```

### Upstream

A task started by `next` receives what the task before it handed on, the `Upstream`: the finished task's instance id, the entity's key and attributes, and its payload. A builder function gets it by declaring a parameter called `upstream`, and the entity by declaring `entity`; a function that declares neither works as before. The first task of a flow gets `upstream=None`.

```python title="docs/snippets/pages/task_flow.py:builder-upstream"
# A function declaring `upstream` receives what the previous task handed on,
# and `entity` the order itself; a payload includes attributes only when it asks
@app.task()
def ship_order(task_id: int, upstream, entity):
    return {
        "event_type": "order_shipped",
        "packed_by": upstream.task_id,  # the instance of pack_order before it
        "gift": entity.attributes.get("gift"),
        "backordered": entity.attributes.get("backordered"),
    }


# A rule can also be a function of the Upstream, returning a task name or None
@app.task(next=lambda upstream: "check_order" if upstream.payload["ok"] else None)
def inspect_return(task_id: int):
    return {"ok": False}
```

### Example: A Flow of Tasks

The `flow` example: orders are picked, gift orders are wrapped, one parcel in ten is packed again, and every order is shipped. Wrapping and shipping are timed steps. It needs no Docker service and ends on its own.

=== "YAML"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get flow --api yaml
        cd flow/yaml

        # 3. Run it
        desflow run flow.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get flow --api yaml
        cd flow/yaml

        # 3. Run it
        desflow run flow.yaml
        ```

    ```yaml title="examples/yaml/flow.yaml:routes"
    tasks:
      pick:
        service: picking
        resource: pickers
        payload: {event_type: order_picked}
        # The first rule that holds wins; default catches the rest.
        next:
          when:
            - {condition: "attr.gift == 1", task: wrap}
          default: pack
      wrap:
        # A timed step: a service and no resource, so no queued event and no wait.
        service: wrapping
        payload: {event_type: gift_wrapped}
        # Applied to the entity's attributes when the task finishes.
        set: {wrapped: 1}
        next: pack
      pack:
        service: packing
        resource: packers
        payload: {event_type: order_packed}
        # Drawn from this task's own random sequence: one parcel in ten is packed again.
        next:
          weights: {ship: 9, pack: 1}
    ```

    The whole file is [`examples/yaml/flow.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/yaml/flow.yaml).

=== "Builder"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get flow --api builder
        cd flow/builder

        # 3. Run it
        uv run --no-project --with desflow flow_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get flow --api builder
        cd flow/builder

        # 3. Run it
        python flow_example.py
        ```

    ```python title="examples/builder/flow_example.py:routes"
    @app.task(
        service_id="picking",
        resource_id="pickers",
        # The first rule that holds wins; default catches the rest
        next={"when": [{"condition": "attr.gift == 1", "task": "wrap"}], "default": "pack"},
    )
    def pick(task_id, context):
        """Picks the order's items."""
        return {"event_type": "order_picked"}


    # A timed step: a service and no resource, so no queued event and no wait.
    # `set` is applied to the entity's attributes when the task finishes.
    @app.task(service_id="wrapping", set={"wrapped": 1}, next="pack")
    def wrap(task_id, context):
        """Wraps a gift."""
        return {"event_type": "gift_wrapped"}


    # Drawn from this task's own random sequence: one parcel in ten is packed again
    @app.task(
        service_id="packing",
        resource_id="packers",
        next={"weights": {"ship": 9, "pack": 1}},
    )
    def pack(task_id, context):
        """Packs the order."""
        return {"event_type": "order_packed"}
    ```

    The whole file is [`examples/builder/flow_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/builder/flow_example.py).

=== "Low-level"

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get flow --api low-level
        cd flow/low-level

        # 3. Run it
        uv run --no-project --with desflow flow_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get flow --api low-level
        cd flow/low-level

        # 3. Run it
        python flow_example.py
        ```

    ```python title="examples/low-level/flow_example.py:routes"
    ROUTES = {
        "pick": Router(
            {"when": [{"condition": "attr.gift == 1", "task": "wrap"}], "default": "pack"}
        ),
        "wrap": Router("pack"),
        # Weights draw from a generator, set in processes()
        "pack": Router({"weights": {"ship": 9, "pack": 1}}),
        "ship": Router(None),
    }
    SETS = {"wrap": {"wrapped": 1}}
    ```

    The whole file is [`examples/low-level/flow_example.py`](https://github.com/jaehyeon-kim/desflow/blob/main/examples/low-level/flow_example.py).

---

## How It Works

**Conditions.** A condition is a Python expression, parsed with Python's `ast` module and checked against a list of what is allowed before it is ever evaluated; nothing goes through `eval`. It may use:

- numbers, `True` and `False`, and the arithmetic `+`, `-`, `*`, `/`, `//` and `%`;
- the comparisons `<`, `<=`, `>`, `>=`, `==` and `!=`, strings compared with `==` or `!=`, and `in` and `not in` with `items(<store>)` on the right;
- `and`, `or`, `not`, and the conditional form `a if c else b`;
- names with their source as a prefix: `payload.<field>`, `attr.<name>`, `level.<name>`, `users.<name>` (how many units of a resource are held), `queue.<name>` (how many requests wait for it, [Resources](resources.md#read-the-queue-and-users)), `putters.<name>` and `getters.<name>` (how many puts and gets wait at a store or container), `count(<store>)` and `items(<store>)` (how many items a store holds, and the list of them; [Stores and Containers](stores-and-containers.md#registry-paths-readings-and-conditions)), `item.<field>` (the item the task's store `get` took, [Stores and Containers](stores-and-containers.md#pick-an-item-or-give-up-waiting)), `wait.<branch>.done`, `.ok` and `.value` (the task's wait) and `signal.<name>.count`, `.ok` and `.value` ([Tasks and Processes](tasks-and-processes.md#wait-signals-and-child-flows)), `process.<name>`, `.instance` and `.target` ([Tasks and Processes](tasks-and-processes.md#processes-and-the-low-level-api)), and `registry["<path>"]`;
- `t`, the simulation time in seconds.

A name carries its source so that adding a payload field cannot change what an attribute of the same name means, and so registry paths, which contain dots, can be written whole. Anything else is refused with a message that names it:

```python title="docs/snippets/pages/task_flow.py:refused"
for text in ("payload.total ** 2", "x > 1", "open('orders.csv')"):
    try:
        Expression(text)
    except ValueError as error:
        print(error)
```

<!-- records: refused-expressions -->
```text
'payload.total ** 2' uses the operator Pow, which an expression cannot
'x > 1' uses the name 'x' (write payload.<field>, attr.<name>, level.<name>, users.<name>, queue.<name>, putters.<name>, getters.<name>, item.<field>, count(<store>), items(<store>), wait.<branch>.<part>, signal.<name>.<part>, process.<name>, registry["<path>"] or t), which an expression cannot
'open('orders.csv')' uses a Call other than count(<store>) or items(<store>), which an expression cannot
```

A blueprint is checked when it loads: every condition parses, every task a rule names exists, every `level.<name>` names a store or container, every `users.<name>` and `queue.<name>` names a resource, every `putters.<name>` and `getters.<name>` a store or container, every `count(...)` and `items(...)` a store, and every `registry["..."]` is a path of the model, each with its YAML line. The builder makes the same checks when `run()` starts. A name that is missing when a condition is evaluated, such as a payload field, raises an error naming the expression.

**Weights.** The weights are turned into cumulative shares once. Each choice draws one uniform number from the routing task's stream and picks the task whose share it falls in.

**Starting the Next Task.** When a task finishes, it applies `set`, builds the `Upstream`, asks its rules for the next task and starts it as a new SimPy process, with the same entity and the next instance id from the task's counter. An arrival that spawns the task uses the same counter, so no two runs of a task share an id. In SimPy terms, one process starting the next is the whole mechanism; [SimPy and desflow](../des/simpy-and-desflow.md) describes processes.

---

## Stability and Limits

- **Flow balance.** The rate into each task follows from the rates into the tasks before it and the routing shares. With weights 9 to 1 back to packing, each order is packed 1 ÷ 0.9 = 1.11 times on average, so the packers see 11% more work than there are orders.
- **Rework raises utilisation.** Work the load out with the rework included, as [Queues and Capacity](../tutorials/queues-and-capacity.md) does, or a rework loop can push a resource past full use.
- **Refused expressions.** `**`, function calls, indexing other than `registry["..."]`, list and dictionary literals, and bare names other than `t` are refused.
- **One entity, one flow.** A task leads to at most one next task; splitting one order into several, or joining several into one, comes after 1.0.
- **`decide` is kept free.** The task key `decide` is kept for decisions sent from outside for one entity, planned after 1.0; today it is refused like any unknown key.
