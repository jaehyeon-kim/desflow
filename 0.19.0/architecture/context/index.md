# Builder API

The `SimulationContext` acts as the entry point and configuration builder for standard Dynamic DES simulations. It implements the **Builder Pattern** to construct the simulation environment and register resources, statistical samplers, and ingress/egress connectors. [First Simulation](https://jaehyeon.me/dynamic-des/latest/tutorials/first-simulation/index.md) builds a first shop this way, in its Builder tabs; this page lists every builder call, with an example of each.

## Builder Lifecycle

Creating a simulation with `SimulationContext` follows a strict two-phase lifecycle to guarantee **thread safety** and **determinism**:

1. **Builder Phase (Pre-Compilation)**: You register resources, services, and connectors. Everything is held as passive configuration data structures (`SimParameter`, `DistributionConfig`, etc.) in memory.
1. **Compilation & Execution Phase (Post-Compilation)**: The moment you call `app.run(...)`, the builder compiles the configuration. It instantiates the `DynamicRealtimeEnvironment`, boots background network connector threads, and starts the SimPy event loop.

> **Architectural Guarantee**: You cannot call runtime helpers like `context.spawn()`, `context.get_resource()`, or `context.env` during the Builder Phase. Attempting to do so will raise a `RuntimeError`. This prevents partial state leaks and guarantees that the environment clock is strictly controlled.

______________________________________________________________________

## Builder API Reference

The fluent builder API allows chaining configurations:

### Resource Configuration

- `.add_resource(name, current_cap, max_cap)`: Stages a discrete token-based resource (such as packers or packing benches).
- `.add_container(name, current_cap, max_cap)`: Stages a continuous container (such as a stock of boxes or a battery charge level).
- `.add_variable(name, value)`: Stages generic variables or physical parameters (such as a conveyor speed).

docs/snippets/pages/builder.py:resources

```python
app.add_resource("packers", current_cap=2, max_cap=5)  # 2 packers now, room for 5
app.add_container("boxes", current_cap=100.0, max_cap=200.0)  # boxes in stock
app.add_variable("conveyor_speed", 1.5)  # any other live value
```

### Clock and Pacing

- `SimulationContext(sim_id, factor=1.0, random_seed=None, logical_start_time=None)`: `factor` sets the pacing (0.0 runs unpaced), `random_seed` seeds the shared `Sampler`, and `logical_start_time` sets the instant that simulation time 0 maps to in every record's `timestamp`.
- `SimulationContext(..., go_live_at)`: Logical instant, a datetime on the same clock as `logical_start_time`, at which `factor` gives way to real-time pacing. One run can therefore backfill unpaced and then tail live. See [Backfill Then Go Live in One Run](https://jaehyeon.me/dynamic-des/latest/guides/backfill-then-live/index.md).

docs/snippets/pages/builder.py:clock

```python
app = SimulationContext(
    sim_id="Shop",
    factor=0.0,  # 0.0 unpaced, 1.0 real time
    random_seed=42,  # the same draws on every run
    logical_start_time=datetime(2026, 10, 1, 9, 0),  # what simulation time 0 is
)
```

### Statistical Profiles

- `.add_arrival(name, dist, rate, mean, std, low, value)`: Configures an inter-arrival time distribution.
- `.add_service(name, dist, mean, std, rate, low, value)`: Configures a task processing duration distribution.
- Each `dist` reads only its own parameters, and an invalid value raises `ValueError` here, so a mistake fails before the run. [YAML Blueprints](https://jaehyeon.me/dynamic-des/latest/architecture/yaml/#sections) lists the distributions and their rules.

docs/snippets/pages/builder.py:distributions

```python
app.add_arrival("orders", dist="exponential", rate=0.5)  # an order every 2 s on average
app.add_service("packing", dist="truncated_normal", mean=3.0, std=1.0, low=0.5)
app.add_service("inspection", dist="constant", value=0.5)
```

### Decorators

- `@app.task(service_id, resource_id)`: Wraps a function into a task that emits `queued`, waits for the resource, emits `started`, waits a time sampled from the service, and emits the finished event. The finished event's value is exactly the dictionary the function returns, so return `status` or `path_id` yourself if consumers need them.
- `@app.arrival_loop(arrival_id)`: Starts a generator function with the context when the run starts, typically a loop over `context.wait_for_arrival(arrival_id)`.
- `@app.telemetry_loop(interval)`: Calls a function with the context every `interval` simulation seconds.

docs/snippets/pages/builder.py:decorators

```python
@app.task(service_id="packing", resource_id="packers")
def pack(order_id: int):
    # The finished event's value is exactly this dictionary
    return {"order_id": order_id, "status": "packed"}


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    order_id = 0
    while True:
        yield context.wait_for_arrival("orders")
        context.spawn(pack(order_id))
        order_id += 1


@app.telemetry_loop(interval=5.0)
def report(context: SimulationContext):
    # Publishes Shop.packers.in_use every 5 simulation seconds
    context.publish("packers.in_use", context.get_resource("packers").in_use)
```

### Processes

- `.add_process(func, **kwargs)`: Starts a generator function when the run starts, called as `func(context, **kwargs)`. Use it for a process that is not an arrival or telemetry loop, such as a drift engine. It is the builder form of `context.spawn()`, which only works once the run has started.
- `.compile_parameters()`: Returns the `SimParameter` that `run()` registers, so the registry paths a configuration creates can be checked before the run. [Registry paths](https://jaehyeon.me/dynamic-des/latest/architecture/registry/#registry-paths) lists them.

docs/snippets/pages/builder.py:process

```python
def speed_drift(context: SimulationContext, step: float):
    """Raises the conveyor speed a little every 10 simulation seconds."""
    path = "Shop.variables.conveyor_speed"
    while True:
        yield context.env.timeout(10.0)
        speed = context.env.registry.get(path).value
        context.env.registry.update(path, speed + step)


app.add_process(speed_drift, step=0.1)
```

docs/snippets/pages/builder.py:compile

```python
paths = app.compile_parameters()  # the SimParameter that run() registers
```

### Connectors & Ingestion

- `.add_ingress(provider)`: Attaches an ingress connector (such as `LocalIngress` or `KafkaIngress`) to stream live configuration updates into the registry.
- `.add_egress(provider, when=None, batch_size=None, flush_interval=None)`: Attaches an egress connector (such as `ConsoleEgress` or `KafkaEgress`) to publish event and telemetry streams. Every attached provider receives every record, so a stream sink and a lake sink can be written in one pass. Pass `when` to give a provider a predicate and route records instead, for example the hot tail to Kafka and cold history to Parquet. Pass `batch_size` or `flush_interval` to give one provider its own cadence, so a stream sink can flush small and often while a lake sink writes large files.
- `.with_batching(batch_size, flush_interval)`: Sets the default queue batching size and flush timeout for highly efficient I/O. Every provider uses these unless `add_egress` overrides them.
- `.with_batching(..., max_queued_batches, drain_stall_seconds)`: Bounds the egress queue and sets how long teardown keeps waiting for it. The queue is bounded so a sink that cannot keep up slows the simulation instead of building a backlog, and teardown drains until the queue stops shrinking rather than abandoning it on a fixed deadline. A sink that stops consuming altogether raises `RuntimeError` rather than losing events silently.

docs/snippets/pages/builder.py:connectors

```python
app.add_ingress(LocalIngress(schedule=[(5.0, "Shop.resources.packers.current_cap", 3)]))
app.add_egress(ConsoleEgress(), batch_size=100, flush_interval=1.0)
app.with_batching(batch_size=1_000, flush_interval=5.0)  # defaults for other sinks
```

______________________________________________________________________

## Building from YAML

`SimulationContext.from_yaml(path)` builds a context from a [YAML blueprint](https://jaehyeon.me/dynamic-des/latest/architecture/yaml/index.md). The file is validated first, and every error names the file and line. The context is then built with the builder methods above, in the order a script calls them, so the result is an ordinary `SimulationContext`. Here `local.yaml` is the local example, which `ddes examples get local --api yaml` downloads to `local/yaml/`.

docs/snippets/pages/yaml_loading.py:from-yaml

```python
from dynamic_des import SimulationContext

app = SimulationContext.from_yaml("local.yaml")
app.run()  # calls run.before, then runs until run.until
```

`run()` calls the blueprint's `run.before` functions first, and uses `run.until` when no `until` is passed. The `ddes run` command does the same from a shell.
