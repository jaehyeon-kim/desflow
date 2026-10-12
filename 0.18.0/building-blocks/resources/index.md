# Resources: `DynamicResource`, `DynamicContainer` and `DynamicStore`

Standard SimPy objects are static. Dynamic DES wraps SimPy objects so their capacity follows the registry in real time.

______________________________________________________________________

## Discrete Resources (`DynamicResource`)

`DynamicResource` is built from a SimPy `PriorityStore` request queue and a `Container` token pool, and represents discrete assets that process individual tasks (such as machines, bays, or operators).

A task's request waits in the queue. The dispatcher takes the next request by priority, waits for a token from the pool, and grants it; the registry decides how many tokens the pool may hold:

docs/snippets/pages/resources.py:resource

```python
lathe = DynamicResource(env, "Line_A", "lathe")  # capacity from the registry: 2


def part(env, part_id, priority=1):
    with lathe.request(priority=priority) as request:  # a lower number goes first
        yield request  # waits until a unit is free
        yield env.timeout(10.0)  # holds the unit for 10 s
    # leaving the with block releases the unit
```

### Shrinking Safety Guarantee

When capacity increases, tokens are immediately added to the pool. When capacity shrinks (for example, from 5 to 2 due to an unexpected machine breakdown): * If 4 machines are currently processing tasks, **Dynamic DES will not terminate active processes**. * Idle tokens leave the pool at once. The units still busy above the new capacity are owed: while tokens are owed, each released token is discarded instead of returning to the pool. * No waiting task starts until the busy units are within the new capacity (2). `in_use` counts the busy units, so it can be above `capacity` until they finish. * A capacity raised again before the owed tokens arrive first cancels them, then adds any remaining difference to the pool.

The animation steps through a drop from 5 to 2 with 4 units busy:

docs/snippets/pages/resources.py:shrink

```python
def breakdown(env):
    """Takes 3 of 5 lathes out of service at t=5 s. Running parts are not stopped."""
    env.registry.update("Line_A.resources.lathe.max_cap", 5)
    env.registry.update("Line_A.resources.lathe.current_cap", 5)
    yield env.timeout(5.0)
    env.registry.update("Line_A.resources.lathe.current_cap", 2)
    yield env.timeout(0.1)  # the resource applies the change in its own process
    print(f"capacity {lathe.capacity}, in use {lathe.in_use}")  # capacity 2, in use 4
```

______________________________________________________________________

## Continuous Containers (`DynamicContainer`)

`DynamicContainer` wraps a SimPy `Container` and represents continuous quantities (such as fuel tank levels, conveyor queues, or concept drift/physical wear).

- **Capacity**: The physical size limit of the tank or container. It follows the registry.
- **Level**: The current fluid level or quantity of material. It changes only through `put` and `get`.

docs/snippets/pages/resources.py:container

```python
coolant = DynamicContainer(env, "Line_A", "coolant", init=80.0)  # 80 of 100 litres


def cool(env):
    yield coolant.get(5.0)  # takes 5 litres, waiting if fewer are left
    yield coolant.put(5.0)  # puts them back, waiting if the tank is full
    print(f"coolant level {coolant.level}")
```

______________________________________________________________________

## Dynamic Stores (`DynamicStore`)

`DynamicStore` wraps a SimPy `Store`, or a `PriorityStore` when priorities are needed, and represents item-based collections (such as warehouses, buffer areas, or order books).

A container holds a quantity up to its capacity, and a store holds items up to its capacity. Both capacities follow the registry:

docs/snippets/pages/resources.py:store

```python
goods = DynamicStore(env, "Line_A", "finished_goods")  # at most 50 items


def pack(env, part_id):
    yield goods.put({"part_id": part_id})  # waits while the store is full
    item = yield goods.get()  # waits while the store is empty
    print(f"shipped {item['part_id']}")
```

______________________________________________________________________

## SimPy and Dynamic DES

SimPy's `Resource`, `Container` and `Store` take their capacity when they are created and keep it: the `capacity` property has no setter, so assigning a new value raises `AttributeError` (checked with SimPy 4.1.1). A SimPy model that needs a machine to break down has to model it some other way, such as a process that holds the resource.

Dynamic DES builds each one from SimPy parts and lets the capacity follow the registry. `DynamicResource` keeps requests in a SimPy `PriorityStore` and free units as tokens in a SimPy `Container`, and a dispatcher process gives tokens to requests; a change of capacity adds tokens, or removes idle ones at once and the rest as busy units finish. `DynamicContainer` and `DynamicStore` wrap SimPy's `Container` and `Store` and apply a new capacity in the same way. The request, `put` and `get` calls are SimPy's, so code written for SimPy resources works unchanged.

______________________________________________________________________

## Creating Them

`SimulationContext.run()` creates a `DynamicResource` for every `add_resource`. It does not create containers or stores: `add_container` registers the capacity paths only. A process that needs a container builds it from the registered paths. `DynamicContainer` and `DynamicStore` are imported from `dynamic_des`, like `DynamicResource`. Stores have no builder method; register them with `SimParameter(stores=...)` on the low-level API.

docs/snippets/pages/resources_builder.py:container

```python
from dynamic_des import DynamicContainer, SimulationContext

# run() creates the lathe; the container is only registered
app = (
    SimulationContext(sim_id="Line_A")
    .add_resource("lathe", current_cap=2, max_cap=5)
    .add_container("fuel_tank", current_cap=100.0, max_cap=500.0)
)


def refuel(context):
    # The container reads its capacity from Line_A.containers.fuel_tank
    tank = DynamicContainer(context.env, "Line_A", "fuel_tank", init=50.0)
    while True:
        yield context.env.timeout(10.0)
        yield tank.put(25.0)
        context.publish("fuel_tank.level", tank.level)


app.add_process(refuel)
```
