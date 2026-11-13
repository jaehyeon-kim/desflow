# Task Flow

An entity rarely leaves after completing just one task. An order is checked, perhaps wrapped as a gift, packed, sometimes packed again, and shipped. This page shows how a finished task chooses the next one, what the entity carries from task to task, and which values a task draws and reads on the way.

---

## What It Is

A **flow** is the sequence of tasks one entity passes through. Each task names what follows it: always the same task, the first of several rules whose condition holds, or a task drawn at random by weight. A rule can send the entity back to a task it has passed, for **rework**. A task that names nothing ends the flow. The entity keeps its key and its **attributes** through the whole flow, and every task can read them.

Each task also has a **key** for its records, can draw **values** for its own use, and can read the **records** that earlier tasks published.

[A Flow of Tasks](../../learn/shop-logic/flow-of-tasks.md) builds a pick, pack and ship flow step by step.

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

    A `Router` takes the same rules as `next`. When a task finishes, its process builds the `Upstream`, asks `route.choose(upstream, env, levels=...)` for the next task's name, and starts that task with the same entity. `env` lets a condition read `registry[...]` and `t`. `levels`, a mapping from store and container names to the objects, lets it read `level.<name>`.

The condition is evaluated when the task finishes, after its `set`. `payload.` reads the payload of the task that just finished, `attr.` the entity's attributes, `level.` a store's or container's level, and `registry["..."]` a live parameter. [How It Works](#how-it-works) lists everything a condition can use.

### Weights

`weights` maps task names to numbers or expressions, and the next task is drawn in proportion to them. The draw comes from a random stream of the routing task's own, `route.<task>`, so a seeded run repeats it and it changes no other draw:

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

A weight written as an expression is read when the task finishes. It may use `attr.<name>`, `registry["..."]`, `level.<name>`, `t` and the task's vars, as `weights: {ship_order: 9, pack_order: "attr.rework"}` does. A weight of infinity or `true` is refused when the blueprint loads. A weight below 0, or text that is not a number, fails the run with the weight's name. Weights that add up to 0 fail the run.

### Attributes

An arrival gives each new entity its starting `attributes`, and a task's `set` changes them when the task finishes. Each entity gets its own copy. Attributes are not published: a condition reads them as `attr.<name>`, and a payload includes them only when it asks.

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_flow.yaml:attributes"
    arrivals:
      # Each order starts with these attributes, read as attr.gift
      # An order every 2 s on average (rate 0.5)
      orders:
        dist: exponential
        rate: 0.5
        spawn: check_order
        attributes: {gift: 0}
      # A gift order every 10 s on average (rate 0.1)
      gifts:
        dist: exponential
        rate: 0.1
        spawn: check_order
        attributes: {gift: 1}
    ```

    ```yaml title="docs/snippets/yaml/task_flow.yaml:set"
    # Applied to the order's attributes when the task finishes
    set: {backordered: 1}
    ```

    A task's `attributes` list adds the named attributes to a mapping payload, as `ship_order` does in the full blueprint, [`task_flow.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/docs/snippets/yaml/task_flow.yaml).

=== "Builder"

    ```python title="docs/snippets/pages/task_flow.py:builder-attributes"
    # Each order starts with these attributes, read as attr.gift
    # An order every 2 s on average (rate 0.5)
    app.add_arrival("orders", dist="exponential", rate=0.5, attributes={"gift": 0})
    # A gift order every 10 s on average (rate 0.1)
    app.add_arrival("gifts", dist="exponential", rate=0.1, attributes={"gift": 1})
    ```

    ```python title="docs/snippets/pages/task_flow.py:builder-set"
    # `set` is applied to the order's attributes when the task finishes
    @app.task(service_id="supplying", set={"backordered": 1}, next="check_order")
    def backorder(task_id: int):
        return {"event_type": "order_backordered"}
    ```

=== "Low-level"

    An `Entity(key, arrival, attributes)` carries them. The process updates `entity.attributes` itself after a task, as the low-level tab of [A Flow of Tasks](../../learn/shop-logic/flow-of-tasks.md) shows.

### Record Key and Fields

Each arrival starts a new entity, keyed by the arrival's name and a counter from 1: `orders-1`, `orders-2`. Every event of every task in its flow carries that key as its record key, so a sink can follow one order through the whole flow. Kafka sends records with the same key to one partition, so it keeps one order's events in order. The builder can replace the key with a function of the counter:

=== "YAML"

    A blueprint arrival uses the default key.

=== "Builder"

    ```python title="docs/snippets/pages/task_flow.py:builder-key"
    # Keys ret-00001, ret-00002 instead of returns-1, from the counter, which starts at 1
    # A return every 20 s on average (rate 0.05)
    app.add_arrival("returns", dist="exponential", rate=0.05, key=lambda n: f"ret-{n:05d}")
    ```

=== "Low-level"

    The process that creates each `Entity` gives it its key.

Below are the finished events of `gifts-5` in [`task_flow.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/docs/snippets/yaml/task_flow.yaml), the blueprint whose parts this page shows. The gift order is checked while the shelf is empty, backordered, checked again, wrapped, packed and shipped, all under one key:

<!-- records: gift-flow -->
```text
[EVT] {'sim_ts': 33.55388955498106, 'timestamp': '...', 'key': 'gifts-5', 'task': 'check_order', 'instance': 17, 'value': {'total': 80}}
[EVT] {'sim_ts': 36.55388955498106, 'timestamp': '...', 'key': 'gifts-5', 'task': 'wrap_gift', 'instance': 3, 'value': {'event_type': 'gift_wrapped'}}
[EVT] {'sim_ts': 38.05388955498106, 'timestamp': '...', 'key': 'gifts-5', 'task': 'pack_order', 'instance': 12, 'value': {'event_type': 'order_packed'}}
[EVT] {'sim_ts': 39.55388955498106, 'timestamp': '...', 'key': 'gifts-5', 'task': 'pack_order', 'instance': 13, 'value': {'event_type': 'order_packed'}}
[EVT] {'sim_ts': 39.55388955498106, 'timestamp': '...', 'key': 'gifts-5', 'task': 'ship_order', 'instance': 11, 'value': {'event_type': 'order_shipped', 'gift': 1, 'backordered': None}}
```

A task can also set the key of its own records. A blueprint task has these keys, and the builder's `@app.task` takes the same names:

- `key_field` names a payload field. Its value becomes the key of the task's `finished` record, and so the Kafka message key. The `queued` and `started` records keep the entity's key. A payload without the field fails the run.
- `key` is a value place for the key of all the task's records: text, a whole number or a generator. It is drawn once for each instance, at its first record, and written as text. Write `key` or `key_field`, not both.
- `id_field` adds the task's instance id to a mapping payload under that name, `item_field` adds the item the task's store `get` took, and `attributes` adds the named entity attributes. They need a mapping payload, and a null payload fails the task.
- `payload: null` publishes no `finished` record, and the task runs its other steps. It is refused together with a store `put`, `key_field`, `id_field`, `item_field`, `attributes` and a `payload.` read.

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_values.yaml:key"
    sign_up:
      payload:
        customer_id: {gen: sequence, format: "c-{}"}
      # The finished record carries the customer id as its key
      key_field: customer_id
    ```

=== "Builder"

    ```python title="docs/snippets/pages/task_values.py:key"
    customer = Template(
        {"customer_id": {"gen": "sequence", "format": "c-{}"}}, name="tasks.sign_up"
    )


    # The finished record carries the customer id as its key. A builder task keeps the
    # rows that a lookup reads only when it asks, with history
    @app.task(key_field="customer_id", history=1000)
    def sign_up(task_id):
        return customer
    ```

=== "Low-level"

    A process gives the key to `env.publish_event(key, value, ...)` itself, as the low-level tab of [Tasks](../tasks-and-processes/tasks.md#a-task) shows, so it can publish any record under any key.

### Upstream

A task started by `next` receives what the task before it handed on, the `Upstream`: the finished task's instance id, the entity's key and attributes, and the finished task's payload. A builder function gets it by declaring a parameter called `upstream`, and gets the entity by declaring `entity`. A function that declares neither is called without them. The first task of a flow gets `upstream=None`.

A task that `on_fail`, `on_renege` or `on_balk` reaches reads the record that sent it there as `upstream.status` and, after a failure, `upstream.reason`.

=== "YAML"

    A blueprint task has no function to declare `upstream`. Its `payload` names the attributes it carries, its generators read the finished task's payload as `{gen: upstream, path: <field>}`, and an expression reads it as `upstream.<field>`. Its `next` conditions read the finished task's payload as `payload.<field>` and the entity's attributes as `attr.<name>`.

=== "Builder"

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

=== "Low-level"

    A low-level process holds the order and its attributes itself. It can build an `Upstream` and call `Router.choose`, as in [Choose the Next Task](#choose-the-next-task), or decide the next step in plain Python.

### Values a Task Draws

A task can draw values for its own use. They come in three groups:

- `vars` are drawn in order, once for each instance of the task, and each can read the vars above it.
- `vars_once` are drawn once for each entity and task, at the first use. A task that an entity runs again, as a rework does, keeps its `vars_once`.
- `finish_vars` are drawn after the service and can read the vars.

`on_start` values are published as one record when the task starts. A task that has `on_start` draws its `vars` when it starts. Without `on_start`, the task draws them after the service, just before it builds its payload, and a `service_time` that reads a var draws it first.

A payload reads a var as `{gen: var, name: <name>}`. The `expr` and `format` generators and the `put`, `fail`, `next.when` and `next.weights` expressions of the same task read it by its bare name. A wait timeout `value` may read `vars_once` names but no `vars`. A var name is an identifier that is not a name of the expression language (`all`, `any`, `len`, `sum`, `cause`, `waiting` and the others) and not the name of a var that is visible there. It is in one group only. A task that can end early (`patience`, `balk`, `wait`, `on_interrupt: abandon` or `queue_limit`) refuses an `on:` value that reads a var.

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_values.yaml:draws"
    pack:
      service: packing
      resource: packers
      # Drawn once for each order, so a second packing keeps the box
      vars_once:
        box: {gen: one_of, choices: [small, large]}
      # Drawn for each packing, when it starts because on_start is written
      vars:
        weight: {gen: uniform, low: 0.5, high: 5, decimals: 1}
      # Published as one record when the packing starts
      on_start:
        weight: {gen: var, name: weight}
      # Drawn after the service, and able to read the values above
      finish_vars:
        fee: {gen: expr, expr: "round(weight * 0.4, 2)"}
      payload:
        box: {gen: var, name: box}
        weight: {gen: var, name: weight}
        fee: {gen: var, name: fee}
        # The total of the order that came here
        total: {gen: upstream, path: total}
      # One packing in ten is done again
      next:
        weights: {pack: 1, finish: 9}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/task_values.py:draws"
    packed = Template(
        {
            "box": {"gen": "var", "name": "box"},
            "weight": {"gen": "var", "name": "weight"},
            "fee": {"gen": "var", "name": "fee"},
            # The total of the order that came here
            "total": {"gen": "upstream", "path": "total"},
        },
        name="tasks.pack",
    )


    @app.task(
        service_id="packing",
        resource_id="packers",
        # Drawn once for each order, so a second packing keeps the box
        vars_once={"box": {"gen": "one_of", "choices": ["small", "large"]}},
        # Drawn for each packing, when it starts because on_start is written
        vars={"weight": {"gen": "uniform", "low": 0.5, "high": 5, "decimals": 1}},
        # Published as one record when the packing starts
        on_start={"weight": {"gen": "var", "name": "weight"}},
        # Drawn after the service, and able to read the values above
        finish_vars={"fee": {"gen": "expr", "expr": "round(weight * 0.4, 2)"}},
        # One packing in ten is done again
        next={"weights": {"pack": 1, "finish": 9}},
    )
    def pack(task_id):
        return packed
    ```

=== "Low-level"

    A process draws its values with its own `Sampler` generators and keeps them in Python variables, so it has no groups to order. A generator it names, such as `sampler.generator("pack.weight")`, draws from its own stream of the run's seed.

Every generator node draws from its own stream of the run's seed, so adding a value does not change the draws of the others.

### Read Earlier Records

A task can read the records that tasks finished before it. A generator reads them, in place of a function that keeps its own list:

- `lookup` takes a field of a row of another task's finished payloads, chosen at random unless `strategy` is `first` or `last`. `histogram` splits the rows, in the order they were written, into bins with a share each, `latest_by` keeps the newest row of each value of a field, and `distinct` draws without replacement inside one `repeat`.
- `previous` takes a field of the last finished payload of the same task on the same stream. The stream is the arrival, or the entity for an arrival with `count` or `keys`. It gives null when there is none and no `default` is written.
- `upstream` takes a field of the payload of the task that routed here.
- `aggregate` gives the `mean`, `sum`, `min`, `max` or `count` of a field over the last rows of a task. `where`, an expression over the row's fields, keeps some of them. It does not wait for a row.

A task keeps its last `history` finished payloads, one million by default, when a `lookup` or `aggregate` in the file names it or when `history` is written. [Look Up Other Records](../../how-to/generate-data/look-up-other-records.md) and [Build on the Last Value](../../how-to/generate-data/build-on-the-last-value.md) work through them.

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_values.yaml:earlier"
    place_order:
      # A value drawn here is read by name below
      vars:
        total: {gen: uniform, low: 5, high: 200, decimals: 2}
      payload:
        # A customer who signed up earlier
        customer_id: {gen: lookup, task: sign_up, path: customer_id}
        total: {gen: var, name: total}
        # This stream's last running_total plus this order's total
        running_total:
          gen: add
          args:
            - {gen: previous, path: running_total, default: 0}
            - {gen: var, name: total}
      next: pack
    ```

    A generator that reads records can also set how long a task takes. These tasks run one after another. `deliver` takes the mean weight of the last 5 packings over 1 kg, `restock` the newest weight of one customer, and `inspect` two past weights drawn without replacement, mostly from the older part of the rows. `settle` takes as long as the last settling, and 2 seconds for the first, and `finish` as long as the settling before it:

    ```yaml title="docs/snippets/yaml/task_reads.yaml:reads"
    deliver:
      # The mean weight of the last 5 packings over 1 kg, as seconds
      service_time:
        gen: aggregate
        task: pack
        path: weight
        last: 5
        where: weight > 1
        function: mean
      payload: {event_type: delivered}
      next: restock
    restock:
      # The newest weight of one customer, chosen from the newest row of each customer
      service_time:
        gen: lookup
        task: pack
        path: weight
        strategy: last
        latest_by: customer_id
      payload: {event_type: restocked}
      next: inspect
    inspect:
      # Two past weights, drawn without replacement, mostly from the older part of the
      # rows
      service_time:
        gen: expr
        expr: sum(weights)
        names:
          weights:
            gen: repeat
            times: 2
            value:
              gen: lookup
              task: pack
              path: weight
              distinct: true
              histogram:
                - {bin: 2, frequency: 8}
                - {bin: 8, frequency: 2}
      payload: {event_type: inspected}
      next: settle
    settle:
      # As long as the last settling took, and 2 s for the first
      service_time: {gen: previous, path: took, default: 2}
      payload:
        took: {gen: uniform, low: 1, high: 3, decimals: 1}
      next: finish
    finish:
      # As long as the settling that came before took
      service_time: {gen: upstream, path: took}
      payload: {event_type: finished}
    ```

=== "Builder"

    ```python title="docs/snippets/pages/task_values.py:earlier"
    order = Template(
        {
            # A customer who signed up earlier
            "customer_id": {"gen": "lookup", "task": "sign_up", "path": "customer_id"},
            "total": {"gen": "var", "name": "total"},
            # This stream's last running_total plus this order's total
            "running_total": {
                "gen": "add",
                "args": [
                    {"gen": "previous", "path": "running_total", "default": 0},
                    {"gen": "var", "name": "total"},
                ],
            },
        },
        name="tasks.place_order",
    )


    # A value drawn here is read by name below
    @app.task(
        history=10,
        vars={"total": {"gen": "uniform", "low": 5, "high": 200, "decimals": 2}},
        next="pack",
    )
    def place_order(task_id):
        return order
    ```

=== "Low-level"

    A process keeps the earlier values itself, in a list or a variable, and reads them where it needs them.

A task whose generators look up another task does not start before that task has a row. An arrival, a `spawn` or a `next` that would start it is dropped, with one INFO line that gives the wait in simulation time, and a task that never started gets one more line at the end of the run. A dropped start runs none of the task's `on:` actions. A lookup in an arrival place fails when there is no row. In `where`, a field of the row wins over `t`.

The blueprint is refused when it loads if no task starts the target of a lookup, if the path is not in the target's payload, if lookups form a cycle, if `previous` is outside a task, if `upstream` is in a task that no `next` reaches or that `on_fail` or `on_interrupt_next` also reaches, or if `distinct` is outside the value of a `repeat`.

### Carry Values and Switch Arrivals

A task can write names of its own process with `set_args`. This task and the tasks that follow in the same flow read them as `arg.<name>`. A child flow has its own, and a parent passes names to it with `args` on a `task` branch of its `wait` or on a `spawn`. A `spawn` evaluates `args` once for each child. A read of a name that was neither passed nor written is a run failure. `set_args` is read after `set`, so a payload reads the value from before the write.

`activate` and `deactivate` turn the sources of the named arrivals on and off when the task finishes, after `set_args`. Each must name an arrival that has a `spawn`, and the blueprint and the builder refuse a name that is not an arrival. A source with `count` or `keys` obeys them too. Under `off_mode: drop` an arrival that falls while its source is off is lost and takes no key number. Under `restart` the source starts a whole new gap when it goes on. [Arrivals and Services](arrivals-and-services.md) describes the arrival keys.

=== "YAML"

    ```yaml title="docs/snippets/yaml/task_values.yaml:carry"
    close_shop:
      payload: {event_type: shop_closed}
      # Written now, and read as arg.closed_at by this task and the tasks after it
      set_args:
        closed_at: {gen: expr, expr: t}
      # Turn on the source of the night sign-ups, as the task finishes
      activate: night
      next: day_report
    day_report:
      payload:
        closed_at: {gen: expr, expr: arg.closed_at}
        mean_total:
          gen: aggregate
          task: place_order
          path: total
          last: 10
          function: mean
    ```

=== "Builder"

    ```python title="docs/snippets/pages/task_values.py:carry"
    report = Template(
        {
            "closed_at": {"gen": "expr", "expr": "arg.closed_at"},
            "mean_total": {
                "gen": "aggregate",
                "task": "place_order",
                "path": "total",
                "last": 10,
                "function": "mean",
            },
        },
        name="tasks.day_report",
    )


    @app.task(
        # Written now, and read as arg.closed_at by this task and the tasks after it
        set_args={"closed_at": {"gen": "expr", "expr": "t"}},
        # Turn on the source of the night sign-ups, as the task finishes
        activate="night",
        next="day_report",
    )
    def close_shop(task_id):
        return {"event_type": "shop_closed"}


    @app.task()
    def day_report(task_id):
        return report
    ```

=== "Low-level"

    A process passes values to the next one as plain arguments, and decides in its own code which arrival processes keep running.

Below are the records of `task_values.yaml` that these sections show: the first sign-up, keyed by its customer id; the first order, which looks up customer `c-0` and starts its running total; the packing of that order, with the `weight` published at its start and the finished record with the box, the weight, the fee and the order's total; and the day report that the shop's closing starts at 100 seconds.

<!-- records: task-values -->
```text
[EVT] {'sim_ts': 42.95160161372071, 'timestamp': '...', 'key': 'c-0', 'task': 'sign_up', 'instance': 0, 'value': {'customer_id': 'c-0'}}
[EVT] {'sim_ts': 100.0, 'timestamp': '...', 'key': 'closing-1', 'task': 'day_report', 'instance': 0, 'value': {'closed_at': 100.0, 'mean_total': 126.093}}
```

---

## How It Works

**Conditions.** A condition is a Python expression, parsed with Python's `ast` module and checked against a list of what is allowed before it is ever evaluated. Nothing goes through `eval`. Each name carries its source as a prefix, so adding a payload field cannot change what an attribute of the same name means. [Names an Expression Reads](../writing-a-model/blueprints.md#names-an-expression-reads) lists every name by the place an expression sits in:

| Name | Value |
|---|---|
| `payload.<field>`, `attr.<name>`, `attr["<name>"]` | a field of the payload of the task that just finished, an attribute of the entity, and an attribute whose name Python reads as a word |
| `level.<name>` | the level of a store or container |
| `users.<name>`, `queue.<name>` | how many units of a resource are held, and how many requests wait for it ([Units and Queues](../resources/units-and-queues.md#read-the-queue-and-users)) |
| `putters.<name>`, `getters.<name>` | how many puts and gets wait at a store or container |
| `count(<store>)`, `items(<store>)` | how many items a store holds, and the list of them ([Stores and Containers](stores-and-containers.md#registry-paths-readings-and-conditions)) |
| `item`, `item.<field>`, `item_priority` | the item the task's store `get` took, a field of it at any depth, and the priority a priority store gave it ([Stores and Containers](stores-and-containers.md#pick-an-item-or-give-up-waiting)) |
| `arg.<name>`, `cause.<name>`, `upstream.<field>` | a name `set_args` wrote or a parent passed, why the task was interrupted, and a field of the payload of the task that routed here |
| `held(<resource>)`, `waiting(<resource>)` | the holders in grant order and the waiting requests in queue order, as mappings with `entity`, `rank`, `time`, `preempt` and `key`, and `since` for a holder |
| `putting(<store>)`, `getting(<store>)` | the puts and gets waiting at a store or container, with `entity` and, for a put, `item` or `amount` |
| `running(<task>)`, `running(<task>, <condition>)` | how many instances of a task run, or meet the condition |
| `wait.<branch>.done`, `.ok`, `.value`, `signal.<name>.count`, `.ok`, `.value`, `.processed`, `handle.<name>.triggered`, `.processed`, `.ok`, `.value`, `.alive`, `done_count` | the task's wait, the signals, the names the task opened, and in the `when` of a wait group how many branches are done ([Signals and Waiting](../tasks-and-processes/signals-and-waiting.md#wait-signals-and-child-flows)) |
| `process.name`, `.instance`, `.target`, `.key`, `.flow_name` | the running process ([Processes](../tasks-and-processes/processes.md#how-it-works)) |
| `registry["<path>"]` | a live parameter, written whole because its path contains dots |
| `t`, `peek()` | the simulation time in seconds, and the time of the next scheduled event |
| a var name | a var of the task, read bare in `put`, `fail`, `next.when`, `next.weights`, `expr` and `format` |

It may also use numbers, text, `True`, `False` and `None`; the arithmetic `+`, `-`, `*`, `/`, `//`, `%` and `**` (a power above 10,000, or one with a result of more than 1,000,000 bits, is refused); the comparisons `<`, `<=`, `>`, `>=`, `==`, `!=`, `is None`, `in` and `not in`; `and`, `or`, `not` and `a if c else b`; list and mapping literals, subscripts, slices and one-clause list comprehensions; and the functions `abs`, `min`, `max`, `round`, `floor`, `ceil`, `sqrt`, `exp`, `log`, `log10`, `sin`, `cos`, `tan`, `len`, `sum`, `sorted`, `range`, `any`, `all`, `str`, `int`, `float`, `bool`, `has(attr.<name>)`, `get_key`, `set_key` and `drop_key`. Text may be compared, indexed, added with `+` and passed to the key functions, and `%` on text is refused. One reading of an expression that makes more than 1,000,000 items in all is a named run failure. Anything else is refused with a message that names it:

=== "YAML"

    A blueprint condition that uses anything else is refused when the blueprint is built, with the same message.

=== "Builder"

    ```python title="docs/snippets/pages/task_flow.py:refused"
    for text in ("payload.total @ 2", "x > 1", "open('orders.csv')"):
        try:
            Expression(text)
        except ValueError as error:
            print(error)
    ```

=== "Low-level"

    A low-level `Router` takes these conditions, as in [Choose the Next Task](#choose-the-next-task), and a process can also decide in plain Python.

<!-- records: refused-expressions -->
```text
'payload.total @ 2' uses the operator MatMult, which an expression cannot
'x > 1' uses the name 'x' (write payload.<field>, attr.<name>, level.<name>, users.<name>, queue.<name>, putters.<name>, getters.<name>, item.<field>, item, item_priority, arg.<name>, cause.<name>, upstream.<field>, count(<store>), items(<store>), held(<resource>), waiting(<resource>), putting(<store>), getting(<store>), running(<task>), peek(), wait.<branch>.<part>, signal.<name>.<part>, process.<name>, registry["<path>"] or t), which an expression cannot
'open('orders.csv')' uses a Call other than count(<store>), items(<store>), has(attr.<name>), held, waiting, putting, getting, peek, running, members or abs, min, max, len, sum, sorted, range, any, all, str, int, float, bool, get_key, set_key, drop_key, round, floor, ceil, sqrt, exp, log, log10, sin, cos, tan, which an expression cannot
```

A blueprint is checked when it loads, and each error names its YAML line. Every condition must parse, and every task a rule names must exist. Every `level.<name>` must name a store or container, every `users.<name>` and `queue.<name>` a resource, every `putters.<name>` and `getters.<name>` a store or container, and every `count(...)` and `items(...)` a store. Every `registry["..."]` must be a path of the model. An `item.<field>` in a place that no `get` reaches is refused. The builder makes the same checks when `run()` starts. A name that is missing when a condition is evaluated, such as a payload field, raises an error naming the expression.

**Weights.** Weights that are numbers are turned into cumulative shares once. Weights that are expressions are read each time the task finishes. Each choice draws one uniform number from the routing task's stream and picks the task whose share it falls in.

**Starting the Next Task.** When a task finishes, it applies `set`, builds the `Upstream` and asks its rules for the next task. It runs that task next, in the same SimPy process and the same step, with the same entity and the next instance id from the task's counter. An arrival that spawns the task uses the same counter, so no two runs of a task share an id. In SimPy terms, the next task is the next statement of one process. [SimPy and desflow](../../simpy/index.md) describes processes.

---

## Stability and Limits

- **Flow balance.** The rate into each task follows from the rates into the tasks before it and the routing shares. With weights of 9 to ship and 1 back to packing, one packing in ten sends the order back to be packed again. Every order is packed once, one in ten a second time, one in a hundred a third time, and so on. An order is therefore packed 1 + 0.1 + 0.01 + ... = 1 ÷ 0.9 = 1.11 times on average, so the packers see 11% more work than there are orders. Weights that are expressions give `desflow explain` no shares, so it reports no flow balance for them.
- **Rework raises utilisation.** Work the load out with the rework included, as [Queues and Capacity](../../learn/first-model/queues-and-capacity.md) does, or a rework loop can push a resource past full use. [Queueing Basics](../../learn/concepts/queueing-basics.md#utilisation) explains utilisation.
- **Refused expressions.** A call other than the listed functions, a name without a source, `payload["total"]` (write `payload.total`), a comprehension with more than one clause or over a mapping, `lambda`, `~` and the shift operators are refused. `**` with a negative base and a fractional exponent, and `round(x, -n)` with a very large `n`, fail the run when the expression is evaluated, naming the expression. An expression nested more than 200 levels is refused when it is read.
- **A loop needs time.** A loop of tasks that take no service time, resource, wait or get never lets the clock move. The run stops with an error that names the tasks after the flow has run 10,000 instances at one instant, as a resource breakdown does. Give one task of the loop a `service_time` above 0, or a way out in its `next` rules.
- **One entity, one flow.** A task leads to at most one next task. Splitting one order into several, or joining several into one, comes after 1.0.
- **`decide` is kept free.** The task key `decide` is kept for decisions sent from outside for one entity, planned after 1.0. Today it is refused like any unknown key.

---

## Example: A Flow of Tasks

In the `flow` example, orders are picked, gift orders are wrapped, one parcel in ten is packed again, and every order is shipped. Wrapping and shipping are timed steps. It needs no Docker service and ends on its own.

The tutorial page [A Flow of Tasks](../../learn/shop-logic/flow-of-tasks.md) teaches these features step by step.

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

        # 3. Find the bottleneck, with no run
        desflow explain flow.yaml

        # 4. Run it
        desflow run flow.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get flow --api yaml
        cd flow/yaml

        # 3. Find the bottleneck, with no run
        desflow explain flow.yaml

        # 4. Run it
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
