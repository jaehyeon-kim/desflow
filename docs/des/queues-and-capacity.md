# Queues and Capacity

A model is **stable** when, over time, its resources finish work as fast as it arrives. Then queues rise and fall but stay bounded, and the waits in the records look like a working system. When work arrives faster than it can be finished, the queue grows for as long as the simulation runs, every entity waits longer than the one before, and the data stops looking real. This page shows how to tell the two apart before you run, and what to change.

---

## Utilisation

**Utilisation** is the share of time a resource is busy. For one resource:

> utilisation = arrival rate × mean service time ÷ number of units

The factory in [First Simulation](../tutorials/first-simulation.md) has parts arriving at 0.5 per second and 1.5 seconds of machining on one lathe: 0.5 × 1.5 ÷ 1 = **0.75**, so the lathe is busy three quarters of the time.

- Below 1, the model is stable.
- At 1 or above, it is not: the queue grows without limit. In a run at utilisation 1.1, the lathe's queue held 7,151 parts halfway through 200,000 simulated seconds and 14,046 at the end.

The same calculation applies to every resource in the model. The one closest to 1 is the **bottleneck**: it decides how long everything waits.

---

## Waits Grow Before You Reach 1

A stable model can still wait a long time. Waits grow sharply as utilisation approaches 1, and more variable times make it worse. These are measured mean waits in the queue for one lathe with a mean service of 1.5 seconds, each over 200,000 simulated seconds:

![Measured mean wait in the queue for one lathe with mean service 1.5 s: at utilisation 0.5, 1.5 s with exponential service and 0.8 s with constant service; at 0.75, 4.4 and 2.3 s; at 0.9, 13.0 and 7.0 s; at 0.95, 31.3 and 14.8 s](../assets/diagrams/wait-vs-utilization.png)

Two rules follow, and the measurements match both:

- **With exponential service times**, the mean wait for one unit is `utilisation ÷ (1 − utilisation) × mean service time`. At 0.75 that is 3 × 1.5 = 4.5 seconds; the run measured 4.42. Going from 0.9 to 0.95 more than doubles it.
- **With constant service times** the wait is half of that, because there is no unusually long job holding up the queue. Spread in the service time, or in the gaps between arrivals, always lengthens waits.

You can measure any model the same way:

```python title="docs/snippets/pages/stability.py:measure"
def mean_wait(rate, service, lathes=1, until=200_000, seed=1):
    """Mean time a part waits for a lathe, over `until` simulated seconds."""
    env = DynamicRealtimeEnvironment(factor=0.0)  # unpaced: as fast as possible
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="F",
            arrival={"parts": DistributionConfig(dist="exponential", rate=rate)},
            service={"machining": service},
            resources={"lathe": CapacityConfig(current_cap=lathes, max_cap=lathes)},
        )
    )
    lathe = DynamicResource(env, "F", "lathe")
    sampler = Sampler(rng=np.random.default_rng(seed))
    waits = []

    def part():
        queued = env.now
        with lathe.request() as request:
            yield request
            waits.append(env.now - queued)  # time spent in the queue
            yield env.timeout(sampler.sample(env.registry.get_config("F.service.machining")))

    def arrivals():
        gap = env.registry.get_config("F.arrival.parts")
        while True:
            yield env.timeout(sampler.sample(gap))
            env.process(part())

    env.process(arrivals())
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return float(np.mean(waits))
```

```python title="docs/snippets/pages/stability.py:check"
utilisation = 0.75
rate = utilisation / 1.5  # parts per second, for 1.5 s of work each
exponential = DistributionConfig(dist="exponential", rate=1 / 1.5)
measured = mean_wait(rate, exponential)
expected = utilisation / (1 - utilisation) * 1.5  # 4.5 s, for one lathe
print(f"measured {measured:.2f} s, expected {expected:.2f} s")
```

---

## Making a Model Stable

To bring a resource below 1, change any term of the formula:

| Change | How | Example, from 0.75 |
|---|---|---|
| more units | raise `current_cap` (up to `max_cap`) | 2 lathes: 0.375 |
| faster work | lower the service `mean` | 1.0 s of machining: 0.5 |
| fewer arrivals | lower the arrival `rate` | 0.4 per second: 0.6 |

Each of these is a live parameter, so a running simulation can be stabilised, or pushed into overload on purpose, through the registry: a scenario step, a connector message or a process, as [Live Parameters](../architecture/registry.md) explains. A capacity change takes effect at once for idle units; [Resources](../building-blocks/resources.md) explains what happens to units still busy when it drops.

---

## Watching It While It Runs

Publish the queue and the busy units of each resource, and read them where the records go:

=== "YAML"

    ```yaml title="docs/snippets/yaml/tasks.yaml:telemetry"
    telemetry:
      - interval: 10
        publish: {drill_busy: drill.in_use, drill_waiting: drill.queue_length}
    ```

=== "Declarative"

    ```python title="docs/snippets/pages/tasks.py:builder-telemetry"
    @app.telemetry_loop(interval=10.0)
    def drill_stats(context: SimulationContext):
        drill = context.get_resource("drill")
        context.publish("drill_busy", drill.in_use)  # as Workshop.drill_busy
        context.publish("drill_waiting", len(drill.queue.items))  # requests waiting
    ```

=== "Low-level"

    ```python title="docs/snippets/pages/tasks.py:low-level-telemetry"
    def drill_stats_process(env, drill):
        """What @app.telemetry_loop does, by hand."""
        while True:
            env.publish_telemetry("Workshop.drill_busy", drill.in_use)
            env.publish_telemetry("Workshop.drill_waiting", len(drill.queue.items))
            yield env.timeout(10.0)
    ```

A queue that keeps climbing from one report to the next, while `in_use` stays at the capacity, is a resource above utilisation 1.

---

## Points to Remember

- **Start-up.** A run starts empty, so its first minutes have shorter queues than the rest. Leave them out of any measurement, or run long enough that they do not matter.
- **Long runs.** Averages from a short run are noisy, the more so near utilisation 1. The measurements above use 200,000 simulated seconds, which takes about a second unpaced.
- **Breakdowns lower capacity.** A machine out of service 5 seconds in every 50 has about 90% of its capacity, so include that in the formula.
- **Analysis tools are planned**: `ddes explain` will show a model as a graph, compute each resource's utilisation and point to the bottleneck before a run; see the [Roadmap](../about/roadmap.md).
