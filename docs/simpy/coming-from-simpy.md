# Coming from SimPy

If you have written a SimPy model, you already know most of what desflow needs. This page takes a plain SimPy model in the structure most tutorials teach and shows where each part goes in desflow. Nothing you know is lost: a desflow model is SimPy underneath. [SimPy and desflow](index.md) teaches each SimPy construct, and the last two sections of this page list what desflow changes and the form each construct takes in desflow.

The structure comes from the open book [HSMA: the little book of DES](https://des.hsma.co.uk/) by Dan Chalk, Sammi Rosser and Amy Heather, which teaches SimPy to health-service modellers. Its chapter [The Recommended Structure for DES Models](https://des.hsma.co.uk/recommended_structure_classes_for_des_models.html) splits a model into four classes: a class of global parameters, an entity class, a model class and a trial class. We read that chapter and the chapter on running large numbers of scenarios. The book's text is under CC BY-NC-SA, so none of it is copied here. The code below is our own. It models the packing shop and follows the same four-class shape.

---

## Four Classes

Look at the table for where each class goes. The sections after it explain each row on the shop.

| In a plain SimPy model | In desflow |
|---|---|
| The global parameters class: arrival and packing times, packers, run length, number of runs | Arguments of `add_arrival`, `add_service` and `add_resource`, which are registry paths a run can change, and `add_variable` (`variables` in YAML) for values of your own |
| The entity class: the thing that moves through the model, with the facts it remembers | An entity with `attributes`, and statistics that record each order's wait and service for you |
| The model class: one run, with its environment, resources, arrival generator and journey generator | `SimulationContext`, with `@app.arrival_loop` for the arrival generator and `@app.task` for the journey |
| The trial class: a loop that builds a model for each run number and collects one result per run | `replicate`, which runs a model `n` times from spawned seeds and gives an interval over the runs |

---

## Global Parameters

In the usual structure one class holds every number the model reads, and the model reads them as `G.packers`. This is the shop's.

```python title="docs/snippets/pages/coming_from_simpy.py:globals"
class G:
    """Global parameters: read as G.packers, never instantiated."""

    arrival_mean = 2.0  # seconds between orders
    pack_time = 1.5  # seconds to pack an order
    packers = 1
    run_length = 3600  # seconds
    runs = 10
```

In desflow these numbers are arguments of the parts they describe:

| In `G` | In the builder |
|---|---|
| `arrival_mean = 2.0` | `add_arrival("orders", dist="exponential", rate=0.5)`, an order every 2 seconds on average |
| `pack_time = 1.5` | `add_service("packing", dist="constant", value=1.5)` |
| `packers = 1` | `add_resource("packers", current_cap=1, max_cap=10)` |
| `run_length` and `runs` | arguments of `run` and `replicate`, because they belong to the run and not to the system |
| a number of your own, such as a conveyor speed | `add_variable`, or `variables` in YAML |

Each value is stored in a registry under a path such as `Shop.resources.packers.current_cap`, and the model reads the registry whenever it needs the value. So a value can change while the run goes on, from a scenario or a connector, which a value in `G` cannot. Each change is checked before it is applied. [Live Parameters](../user-guide/live-runs-and-connectors/live-parameters.md) lists the paths.

---

## Entity Class

The entity class is what travels through the model. Here an order carries its number and the time it waited.

```python title="docs/snippets/pages/coming_from_simpy.py:entity"
class Order:
    """The entity: what travels through the model, and what it remembers."""

    def __init__(self, order_id):
        self.id = order_id
        self.wait = 0.0
```

In desflow an arrival creates an entity for you, and `attributes` on the arrival give it starting facts, such as `attributes={"express": 0}`. A task can read them to decide what comes next, and change them when it finishes. You do not store the wait yourself: the task records when the order queued, started and finished, and the statistics report the mean wait and service time of every task. [Task Flow](../user-guide/model-parts/task-flow.md#attributes) explains attributes.

---

## Model Class

The model class is one run. Its constructor makes the environment and the resources, one generator creates the arriving orders, and another is the journey of one order.

```python title="docs/snippets/pages/coming_from_simpy.py:model"
class Model:
    """One run of the shop."""

    def __init__(self, run_number):
        self.env = simpy.Environment()
        self.random = random.Random(run_number)
        self.packers = simpy.Resource(self.env, capacity=G.packers)
        self.waits = []

    def generate_orders(self):
        order_id = 0
        while True:
            order_id += 1
            self.env.process(self.pack_order(Order(order_id)))
            yield self.env.timeout(self.random.expovariate(1 / G.arrival_mean))

    def pack_order(self, order):
        arrived = self.env.now
        with self.packers.request() as request:
            yield request
            order.wait = self.env.now - arrived
            self.waits.append(order.wait)
            yield self.env.timeout(G.pack_time)

    def run(self):
        self.env.process(self.generate_orders())
        self.env.run(until=G.run_length)
        return statistics.mean(self.waits)
```

In desflow, `SimulationContext` is that class. It makes the environment, which is a SimPy environment, and the resources from the `add_` calls, so the constructor of your own class has nothing left to do. The arrival generator is `@app.arrival_loop`, and the journey is `@app.task`, which requests the resource, waits, holds it for the service time and releases it. The task replaces the `pack_order` method above, and it also publishes `queued`, `started` and a finished event as records. If the journey is more than one task, a task names the next one, as [A Flow of Tasks](../learn/shop-logic/flow-of-tasks.md) shows.

---

## Trial Class

The trial class runs the model once for each run number and keeps one result per run. Here the run number also seeds the random generator, so each run differs and each can be repeated.

```python title="docs/snippets/pages/coming_from_simpy.py:trial"
class Trial:
    """The runs of one design: one result for each."""

    def run_trial(self):
        return [Model(run).run() for run in range(G.runs)]
```

In desflow this loop is `replicate`. It runs the model `n` times, gives each run its own seed spawned from one seed, reads the statistics at the end of each run, and returns the [interval](../learn/concepts/output-analysis.md#interval-and-half-width) for each measure you name. You do not write the loop, the seeding or the interval. [Replications](../user-guide/results/replications.md) explains the options, and [Trusting Results](../learn/studies/trusting-results.md) uses it on the shop.

---

## Shop in desflow

Here is the whole shop in desflow, with the trial loop beside it. The model is one function that builds a context, and the loop is one call.

```python title="docs/snippets/pages/coming_from_simpy.py:desflow"
def shop() -> SimulationContext:
    """The same shop: parameters in the registry, the entity and the statistics built in."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=1 / 2.0)
    app.add_service("packing", dist="constant", value=1.5)
    app.add_resource("packers", current_cap=1, max_cap=10)
    app.with_stats("typical", interval=3600)

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def trial():
    """The trial loop is `replicate`: one seed for each run, and an interval over the runs."""
    result = replicate(
        shop, 10, seed=42, until=3600, measures=["tasks.pack_order.wait_mean"]
    )
    return result.summary[0]
```

Four parts of the desflow version have no line in the SimPy one:

- `context.wait_for_arrival("orders")` waits for the gap to the next order, drawn from the `orders` arrival's distribution, and returns the new order as an entity.
- `entity=order` hands that order to the task, so every record of the task carries the order's key, such as `orders-1`. Without it the task starts an entity of its own.
- The dictionary the task returns is the payload of its finished event, so `{"event_type": "order_packed"}` is published as a record.
- `with_stats("typical", interval=3600)` switches on the built-in statistics, such as the mean wait that `replicate` reads, and publishes them every hour of simulated time.

[Builder API](../user-guide/writing-a-model/builder-api.md#write-the-tasks) describes each call, and [Statistics](../user-guide/results/statistics.md#choose-a-level) lists the levels.

Both versions run 10 times for one hour from an empty shop, so the two answers estimate the same thing. Each line of the output below gives the mean wait over the 10 runs and the half-width of its 95% interval. The two versions use different random numbers, so the values differ, but the check in the script asserts that they agree within their intervals. This shop is not the busy hour of the tutorials: it has one packer and an order every 2 seconds on average, so its mean wait differs from theirs.

```text
plain SimPy: mean wait 2.30 ± 0.20 s over 10 runs
desflow:     mean wait 2.45 ± 0.27 s over 10 runs
```

What you take with you is the SimPy you wrote. A model can still be a SimPy generator inside desflow, through the [Low-level API](../user-guide/writing-a-model/low-level-api.md), and it keeps the SimPy constructs it uses. What you leave behind is the code around it: the parameters class, the result bookkeeping and the trial loop. What you gain is [records](../reference/records-and-telemetry.md), pacing, live parameters and the checks on [Output Analysis](../learn/concepts/output-analysis.md).

---

## What desflow Changes

desflow keeps SimPy's event list, clock, processes and resources. It changes the six things in the table:

| Part | In SimPy | In desflow |
|---|---|---|
| Clock and pacing | `Environment` runs without waiting; `RealtimeEnvironment(factor, strict=True)` follows the wall clock and raises `RuntimeError` when the model falls behind | one class, `DynamicRealtimeEnvironment`, does both: `factor=0` runs without waiting, `go_live_at` switches to real time partway through a run (SimPy's `factor` cannot change once set), `logical_start_time` gives every record a date and time, and falling behind is reported as telemetry; `strict` raises `RuntimeError` as SimPy's does, with the defaults that [Strict Pacing](../user-guide/time-and-pacing/clocks-and-pacing.md#strict-pacing) gives; `sync()` is SimPy's, and `resume_pacing()` also moves the simulation anchor after a pause; `advance(until)` runs a started run in segments, as repeated `env.run(until=...)` does |
| Processes | generators started with `env.process` | the same, and the builder adds `@app.task`, `@app.arrival_loop`, `@app.telemetry_loop` and `add_process`, which write the common ones for you |
| Random times | none built in: a model calls `random` or NumPy itself | ten named distributions with checked parameters, drawn by a `Sampler` from the current values each time; each distribution draws from a random sequence of its own, made from the run's seed and its registry path |
| Resources | `Resource`, `Container` and `Store` keep the capacity they were created with: it has no setter, so assigning a new one raises `AttributeError` (SimPy 4.1). `Resource` serves requests in the order they arrive, and `PriorityResource` by priority | `DynamicResource`, `DynamicContainer` and `DynamicStore` follow a capacity in the registry, and a resource that shrinks lets busy units finish. A resource serves its queue `fifo`, `lifo` or by `priority`, equal priorities first in, first out. `run()` builds the stores and containers a model declares, and tasks put to and get from them |
| Parameters | Python variables, changed only by the model's own code | a registry of named paths, changed by connectors, scenarios or processes, with each change checked |
| Output | whatever the model prints or logs | events and telemetry with a fixed shape, written to Kafka, Redis, PostgreSQL, files or Iceberg on background threads |

---

## Each Construct in desflow

Each row names a construct, its form in the three ways of writing a model, and the group of conformance checks on [SimPy Compatibility](compatibility.md) that runs it in plain SimPy and in all three forms and compares the results. A construct with no native form in one of the three is marked as a difference there, with the reason.

| SimPy construct | YAML | Builder | Low-level | Checks |
|---|---|---|---|---|
| `Environment`: `run`, `step`, `peek`, `now` | `run.until` | `app.run(until=)`, `app.step()`, `app.peek()` | `env.run(until=)` | [Run Control](compatibility.md#c6-run-control) |
| `timeout` | a task with `service` and no `resource` | `@app.task(service_id=...)` | `yield env.timeout(delay)` | [Events and Processes](compatibility.md#c1-events-and-processes) |
| Events and callbacks | `signals:`, `on:` | `add_signal`, `on=` | `Signal`, `event.callbacks.append(fn)` | [Events and Processes](compatibility.md#c1-events-and-processes) |
| Processes and waiting for one | `wait: {task: ...}` | `wait={"task": ...}`, `context.call(...)` | `yield env.process(child())` | [Events and Processes](compatibility.md#c1-events-and-processes) |
| Interrupts | `interrupt:`, `on_interrupt` | `context.interrupt(...)`, `on_interrupt=` | `process.interrupt(cause)` | [Interrupts and Preemption](compatibility.md#c3-interrupts-and-preemption) |
| `AnyOf`, `AllOf` | `wait: {any: ...}`, `wait: {all: ...}` | `wait={"any": ...}`, `wait={"all": ...}` | `yield a \| b`, `yield a & b` | [Wait for One or All](compatibility.md#c2-wait-for-one-or-all) |
| `Resource`, `PriorityResource` | `resource: packers`, `queue: priority` | `@app.task(resource_id=...)`, `add_resource(queue=...)` | `with packers.request() as req: yield req` | [Resources](compatibility.md#c4-resources) |
| `PreemptiveResource` | `preempt: true, queue: priority` | `add_resource(queue="priority", preempt=True)` | `DynamicResource(queue="priority", preempt=True)` | [Interrupts and Preemption](compatibility.md#c3-interrupts-and-preemption) |
| `Container` | `put` and `get` with `container` and `amount` | `put={"container": ...}`, `get={"container": ...}` | `yield stock.put(2)`, `yield stock.get(1)` | [Stores and Containers](compatibility.md#c5-stores-and-containers) |
| `Store`, `FilterStore`, `PriorityStore` | `put` and `get` with `store`, `filter`, `priority: true` | `put={"store": ...}`, `get={"store": ..., "filter": ...}` | `parcels.get(filter=...)` | [Stores and Containers](compatibility.md#c5-stores-and-containers) |
| `RealtimeEnvironment` | `simulation.factor`, `simulation.strict` | `SimulationContext(factor=, strict=)` | `DynamicRealtimeEnvironment(factor=)` | [Run Control](compatibility.md#c6-run-control) |

`!python` and `add_process` run SimPy code you write yourself. They are extension points, so a construct that has no native form in YAML or the builder is not covered by using them. User Guide pages that build on SimPy name the parts they use in their How It Works section, starting with [Arrivals and Services](../user-guide/model-parts/arrivals-and-services.md).
