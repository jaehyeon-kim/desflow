# Change State Over Time

Write a state machine as tasks: three packing stations that move between running, slow and stopped, each state lasting a set time, and each record telling which state a station was in.

______________________________________________________________________

## Before You Start

- The commands run from the root of a clone of the repository, where the files are in `docs/snippets/generate-data/`. The Full Files block of this page holds the same files, for a reader who saves them by hand.

  ```bash
  git clone https://github.com/jaehyeon-kim/desflow.git
  cd desflow
  ```

- [First Simulation](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md) explains arrivals, tasks and the records a run writes.

- [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/index.md) explains `next` and its `weights`.

______________________________________________________________________

## Steps

### Start the Stations

`initial: 3` creates three stations when the run is built, and `spawn: running` starts each one in the task `running`. The `key` generator names them `station-1` to `station-3`.

```yaml
arrivals:
  stations:
    initial: 3
    spawn: running
    key: {gen: sequence, start: 1, format: "station-{}"}
```

```python
app.add_initial_arrival(
    "stations", 3, spawn="running", key=lambda n: f"station-{n}"
)
```

```python
sampler = Sampler(seed=42)
routers = {
    state: Router(rule, rng=sampler.generator(f"route.{state}"))
    for state, rule in NEXT.items()
    if isinstance(rule, dict)
}
counts = {state: 0 for state in DWELL}
```

### Write a Task for Each State

Each state is a task. `service_time` is how long a station stays in the state, the payload names the station and the state, and `next` chooses the state that follows. After running or slow the choice is by `weights`, and after stopped it is always slow, so a station never goes from stopped to running. A task writes its record when its time ends.

```yaml
tasks:
  running:
    service_time: 300
    payload:
      station: {gen: entity}
      state: running
    next:
      weights: {running: 85, slow: 15}
  slow:
    service_time: 120
    payload:
      station: {gen: entity}
      state: slow
    next:
      weights: {running: 55, slow: 25, stopped: 20}
  stopped:
    service_time: 240
    payload:
      station: {gen: entity}
      state: stopped
    next: slow
```

```python
def state(name):
    return Template(
        {"station": {"gen": "entity"}, "state": name}, name=f"tasks.{name}"
    )

running, slow, stopped = state("running"), state("slow"), state("stopped")

@app.task(
    name="running",
    service_time=300,
    next={"weights": {"running": 85, "slow": 15}},
)
def running_task(task_id):
    """The station is running."""
    return running

@app.task(
    name="slow",
    service_time=120,
    next={"weights": {"running": 55, "slow": 25, "stopped": 20}},
)
def slow_task(task_id):
    """The station is slow."""
    return slow

@app.task(name="stopped", service_time=240, next="slow")
def stopped_task(task_id):
    """The station is stopped."""
    return stopped
```

```python
DWELL = {"running": 300, "slow": 120, "stopped": 240}  # seconds in each state
NEXT = {
    "running": {"weights": {"running": 85, "slow": 15}},
    "slow": {"weights": {"running": 55, "slow": 25, "stopped": 20}},
    "stopped": "slow",
}
```

```python
def station(number):
    key = f"station-{number}"
    state = "running"
    while True:
        instance = counts[state]
        counts[state] += 1
        env.publish_event(key, {"status": "started"}, task=state, instance=instance)
        yield env.timeout(DWELL[state])
        env.publish_event(
            key, {"station": key, "state": state}, task=state, instance=instance
        )
        if state in routers:
            state = routers[state].choose(
                Upstream(task_id=0, key=key, attributes={})
            )
        else:
            state = NEXT[state]

for number in range(1, 4):
    env.process(station(number))
```

The Low-level file keeps the dwell times and the rules in two dictionaries. A `Router` draws the next state from the rule of the current one, from its own random sequence, `route.<state>`.

______________________________________________________________________

## Check the Result

Run the model for 2 simulated hours and print its first 8 records. Each station publishes a `started` record when it enters a state, and a record with its `station` and `state` when the state ends. The three stations start `running` at 0 seconds, and the first state ends at 300 seconds.

```bash
desflow run docs/snippets/generate-data/change_state_over_time.yaml --sample 8
```

```bash
python docs/snippets/generate-data/change_state_over_time_builder.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```bash
python docs/snippets/generate-data/change_state_over_time_low_level.py
```

This prints every record of the run as an `[EVT]` line with the same fields and values.

```text
{"stream_type":"event","sim_ts":0.0,"timestamp":"...","key":"station-1","task":"running","instance":0,"value":{"status":"started"}}
{"stream_type":"event","sim_ts":0.0,"timestamp":"...","key":"station-2","task":"running","instance":1,"value":{"status":"started"}}
{"stream_type":"event","sim_ts":0.0,"timestamp":"...","key":"station-3","task":"running","instance":2,"value":{"status":"started"}}
{"stream_type":"event","sim_ts":300.0,"timestamp":"...","key":"station-1","task":"running","instance":0,"value":{"station":"station-1","state":"running"}}
{"stream_type":"event","sim_ts":300.0,"timestamp":"...","key":"station-1","task":"slow","instance":0,"value":{"status":"started"}}
{"stream_type":"event","sim_ts":300.0,"timestamp":"...","key":"station-2","task":"running","instance":1,"value":{"station":"station-2","state":"running"}}
{"stream_type":"event","sim_ts":300.0,"timestamp":"...","key":"station-2","task":"running","instance":3,"value":{"status":"started"}}
{"stream_type":"event","sim_ts":300.0,"timestamp":"...","key":"station-3","task":"running","instance":2,"value":{"station":"station-3","state":"running"}}
```

The time between two records of a station is the dwell time of the later state. The docs test follows every station through the run: each move is one of the allowed ones, and each state lasts its set time.

______________________________________________________________________

## Full Files

Full files

```yaml
# Change state over time: packing stations move between running, slow and stopped.
#
# Each of 3 stations is a chain of tasks, one for each state. A task holds the station
# in its state for a time, writes a record of the state when the time ends, and `next`
# chooses the state that follows: by weights after running or slow, and always slow
# after stopped, so a station never goes from stopped to running. The run lasts 2 hours.
#
# Needs: nothing.
# Run it with: desflow run docs/snippets/generate-data/change_state_over_time.yaml

simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: 2026-01-01T00:00:00

egress:
  - {type: Console}

arrivals:
  stations:
    initial: 3
    spawn: running
    key: {gen: sequence, start: 1, format: "station-{}"}

tasks:
  running:
    service_time: 300
    payload:
      station: {gen: entity}
      state: running
    next:
      weights: {running: 85, slow: 15}
  slow:
    service_time: 120
    payload:
      station: {gen: entity}
      state: slow
    next:
      weights: {running: 55, slow: 25, stopped: 20}
  stopped:
    service_time: 240
    payload:
      station: {gen: entity}
      state: stopped
    next: slow

run: {until: 2 hours}
```

```python
"""Change state over time: stations move between three states, builder API.

The builder version of `change_state_over_time.yaml`: one task for each state, with a
dwell time as `service_time` and the next state in `next`.

Needs: nothing.
Run it with: python docs/snippets/generate-data/change_state_over_time_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 7200


def build() -> SimulationContext:
    """Builds the three stations."""
    app = SimulationContext(
        sim_id="Shop",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_initial_arrival(
        "stations", 3, spawn="running", key=lambda n: f"station-{n}"
    )

    def state(name):
        return Template(
            {"station": {"gen": "entity"}, "state": name}, name=f"tasks.{name}"
        )

    running, slow, stopped = state("running"), state("slow"), state("stopped")

    @app.task(
        name="running",
        service_time=300,
        next={"weights": {"running": 85, "slow": 15}},
    )
    def running_task(task_id):
        """The station is running."""
        return running

    @app.task(
        name="slow",
        service_time=120,
        next={"weights": {"running": 55, "slow": 25, "stopped": 20}},
    )
    def slow_task(task_id):
        """The station is slow."""
        return slow

    @app.task(name="stopped", service_time=240, next="slow")
    def stopped_task(task_id):
        """The station is stopped."""
        return stopped


    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
```

```python
"""Change state over time: stations move between three states, low-level API.

The low-level version of `change_state_over_time.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`. Each station is one process that holds a state for its
dwell time, publishes the record of the state, and asks a `Router` for the next state.

Needs: nothing.
Run it with: python docs/snippets/generate-data/change_state_over_time_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DynamicRealtimeEnvironment,
    Router,
    Sampler,
    Upstream,
)

UNTIL = 7200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}

DWELL = {"running": 300, "slow": 120, "stopped": 240}  # seconds in each state
NEXT = {
    "running": {"weights": {"running": 85, "slow": 15}},
    "slow": {"weights": {"running": 55, "slow": 25, "stopped": 20}},
    "stopped": "slow",
}


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the three stations on `env`."""
    sampler = Sampler(seed=42)
    routers = {
        state: Router(rule, rng=sampler.generator(f"route.{state}"))
        for state, rule in NEXT.items()
        if isinstance(rule, dict)
    }
    counts = {state: 0 for state in DWELL}

    def station(number):
        key = f"station-{number}"
        state = "running"
        while True:
            instance = counts[state]
            counts[state] += 1
            env.publish_event(key, {"status": "started"}, task=state, instance=instance)
            yield env.timeout(DWELL[state])
            env.publish_event(
                key, {"station": key, "state": state}, task=state, instance=instance
            )
            if state in routers:
                state = routers[state].choose(
                    Upstream(task_id=0, key=key, attributes={})
                )
            else:
                state = NEXT[state]

    for number in range(1, 4):
        env.process(station(number))


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```
