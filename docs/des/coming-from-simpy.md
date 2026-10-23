# Coming from SimPy

If you have written a SimPy model, you already know most of what desflow needs. This page takes a plain SimPy model in the structure most tutorials teach and shows where each part goes in desflow. Nothing you know is lost: a desflow model is SimPy underneath, and [SimPy and desflow](simpy-and-desflow.md) shows each construct.

The structure comes from the open book [HSMA: the little book of DES](https://des.hsma.co.uk/) by Dan Chalk, Sammi Rosser and Amy Heather, which teaches SimPy to health-service modellers. Its chapter [The Recommended Structure for DES Models](https://des.hsma.co.uk/recommended_structure_classes_for_des_models.html) splits a model into four classes: a class of global parameters, an entity class, a model class and a trial class. We read that chapter and the chapter on running large numbers of scenarios. The book's text is under CC BY-NC-SA, so none of it is copied here: the code below is our own, on the packing shop, and follows the same four-class shape.

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

In desflow these numbers are arguments of the parts they describe: `add_arrival("orders", dist="exponential", rate=0.5)`, `add_service("packing", dist="constant", value=1.5)` and `add_resource("packers", current_cap=1, max_cap=10)`. Each is stored in a registry under a path such as `Shop.resources.packers.current_cap`, and the model reads the registry whenever it needs the value. So the difference from `G` is that a value can change while the run goes on, from a scenario or a connector, and each change is checked before it is applied. A number of your own, such as a conveyor speed, goes in `add_variable` in the builder and `variables` in YAML. The run length and the number of runs are arguments of `run` and `replicate`, because they belong to the run and not to the system. [Live Parameters](../user-guide/live-parameters.md) lists the paths.

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

In desflow an arrival creates an entity for you, and `attributes` on the arrival give it starting facts, such as `attributes={"express": 0}`. A task can read them to decide what comes next, and change them when it finishes. You do not store the wait yourself: the task records when the order queued, started and finished, and the statistics report the mean wait and service time of every task. [Task Flow](../user-guide/task-flow.md#attributes) explains attributes.

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

In desflow, `SimulationContext` is that class. It makes the environment, which is a SimPy environment, and the resources from the `add_` calls, so the constructor of your own class has nothing left to do. The arrival generator is `@app.arrival_loop`, and the journey is `@app.task`, which requests the resource, waits, holds it for the service time and releases it. The task replaces the `pack_order` method above, and it also publishes `queued`, `started` and a finished event as records. If the journey is more than one task, a task names the next one, as [A Flow of Tasks](../tutorials/flow-of-tasks.md) shows.

---

## Trial Class

The trial class runs the model once for each run number and keeps one result per run. Here the run number also seeds the random generator, so each run differs and each can be repeated.

```python title="docs/snippets/pages/coming_from_simpy.py:trial"
class Trial:
    """The runs of one design: one result for each."""

    def run_trial(self):
        return [Model(run).run() for run in range(G.runs)]
```

In desflow this loop is `replicate`. It runs the model `n` times, gives each run its own seed spawned from one seed, reads the statistics at the end of each run, and returns the [interval](output-analysis.md#interval-and-half-width) for each measure you name. You do not write the loop, the seeding or the interval. [Replications](../user-guide/replications.md) explains the options, and [Trusting Results](../tutorials/trusting-results.md) uses it on the shop.

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

Both versions run 10 times for one hour from an empty shop, so the two answers estimate the same thing. They use different random numbers, so the values differ, but the check in the script asserts that they agree within their intervals. This shop has one packer and an order every 2 seconds on average, so its wait is not the wait of the tutorials' busy hour; [Common Mistakes](common-mistakes.md#taking-one-run-as-the-answer) explains why the busy hour's figures differ between pages.

```text
plain SimPy: mean wait 2.30 ± 0.20 s over 10 runs
desflow:     mean wait 2.45 ± 0.27 s over 10 runs
```

What you take with you is the SimPy you wrote. A model can still be a SimPy generator inside desflow, through the low-level API, and it keeps the SimPy constructs it uses. What you leave behind is the code around it: the parameters class, the result bookkeeping and the trial loop. What you gain is [records](../reference/records-and-telemetry.md), pacing, live parameters and the checks on [Output Analysis](output-analysis.md).
