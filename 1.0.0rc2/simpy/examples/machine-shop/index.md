# Machine Shop

Ten machines make parts and break down, and one repairman repairs them before doing other jobs. This is SimPy's Machine Shop example, written in desflow.

______________________________________________________________________

## Before You Start

SimPy's shop has 10 machines. Each makes parts without end. A part takes a normal time with a mean of 10 minutes and a standard deviation of 2, never below 0. Each machine breaks down after an exponential time with a mean of 300 minutes, unless it is already broken. A broken machine waits for the one repairman, at priority 1, for a 30 minute repair, and then finishes its part. Between repairs the repairman does other jobs of 30 minutes at priority 2. A repair preempts such a job, and the job resumes afterwards. The run lasts 4 weeks, which is 40320 minutes. desflow reads each of SimPy's minutes as one second.

SimPy breaks a machine by calling `interrupt` on the process that makes parts. The repair is then a request for the repairman, who is a resource that the other jobs use too. The example writes the breakdowns as tasks, so that the repair is an ordinary request at priority 1: each machine has a `break_loop` task that waits for the next failure and interrupts the machine's `part` task, and the `part` task starts the repair when it is interrupted.

The example needs only desflow, which each Quick Start in [Run It](#run-it) installs, and no other service. `desflow examples get simpy-machine-shop --api yaml` downloads it, as [Get an Example](https://jaehyeon.me/desflow/latest/cli/#get-an-example) shows. [SimPy Examples](https://jaehyeon.me/desflow/latest/simpy/examples/index.md) says how these pages read SimPy's times and numbers. SimPy's own page for the example is [on its documentation site](https://simpy.readthedocs.io/en/4.1.2/examples/machine_shop.html).

______________________________________________________________________

## Steps

### Times Are Drawn

SimPy draws the times with two functions, one for the time of a part and one for the time to a failure. desflow draws each time where it is used: the part time on the `part` task and the time to a failure on the `break_loop` task, both shown in the next step. The shop's own settings come first.

```python
def time_per_part():
    """Return actual processing time for a concrete part."""
    t = random.normalvariate(PT_MEAN, PT_SIGMA)
    # The normalvariate can be negative, but we want only positive times.
    while t <= 0:
        t = random.normalvariate(PT_MEAN, PT_SIGMA)
    return t


def time_to_failure():
    """Return time until next failure for a machine."""
    return random.expovariate(BREAK_MEAN)
```

`repairman` is a resource with one unit that serves a priority queue and lets a request of higher priority preempt the unit's user. `repair_time` is a service of 30 seconds. `parts_made` counts the parts of each machine, and `target` names the machine that a failure breaks.

```yaml
variables:
  target: -1
  parts_made: {}

resources:
  repairman: {current_cap: 1, queue: priority, preempt: true}

services:
  repair_time: {dist: constant, value: 30}
```

`add_resource`, `add_variable` and `add_service` declare the same parts.

```python
app.add_resource("repairman", current_cap=1, queue="priority", preempt=True)
app.add_variable("target", -1)
app.add_variable("parts_made", {})
app.add_service("repair_time", dist="constant", value=30)
```

The low-level version has SimPy's `PreemptiveResource` for the repairman, and a `DistributionConfig` for each of the two times, so that it draws from the same streams as the other versions.

```python
sampler = Sampler(seed=42)
part = DistributionConfig(dist="truncated_normal", mean=10, std=2, low=0)
part.stream = "tasks.part.service_time"  # the sequence the blueprint draws from
failure = DistributionConfig(dist="exponential", rate=0.0033333333333333335)
failure.stream = "tasks.break_loop.wait.timeout"
repairman = simpy.PreemptiveResource(env, capacity=1)
names: dict = {}  # each machine's process, by the name its records carry
numbers = {task: itertools.count() for task in TASKS}
```

### Machine Works and Breaks

SimPy's `Machine` class makes parts in a loop. A second process, `break_machine`, interrupts the first after each failure time unless the machine is already broken. The interrupted machine asks for the repairman at priority 1, repairs, and goes on with the time it had left.

```python
class Machine:
    """A machine produces parts and may get broken every now and then.

    If it breaks, it requests a *repairman* and continues the production
    after the it is repaired.

    A machine has a *name* and a number of *parts_made* thus far.

    """

    def __init__(self, env, name, repairman):
        self.env = env
        self.name = name
        self.parts_made = 0
        self.broken = False

        # Start "working" and "break_machine" processes for this machine.
        self.process = env.process(self.working(repairman))
        env.process(self.break_machine())

    def working(self, repairman):
        """Produce parts as long as the simulation runs.

        While making a part, the machine may break multiple times.
        Request a repairman when this happens.

        """
        while True:
            # Start making a new part
            done_in = time_per_part()
            while done_in:
                start = self.env.now
                try:
                    # Working on the part
                    yield self.env.timeout(done_in)
                    done_in = 0  # Set to 0 to exit while loop.

                except simpy.Interrupt:
                    self.broken = True
                    done_in -= self.env.now - start  # How much time left?

                    # Request a repairman. This will preempt its "other_job".
                    with repairman.request(priority=1) as req:
                        yield req
                        yield self.env.timeout(REPAIR_TIME)

                    self.broken = False

            # Part is done.
            self.parts_made += 1

    def break_machine(self):
        """Break the machine every now and then."""
        while True:
            yield self.env.timeout(time_to_failure())
            if not self.broken:
                # Only break the machine if it is currently working.
                self.process.interrupt()
```

`machines` starts ten machines at time 0. `start_machine` starts each machine's `break_loop` and goes to `part`. `part` takes a truncated normal time and repeats with `next: part`. With `on_interrupt: resume` an interrupted part keeps the time it had left, and `on_interrupt_run: begin_repair` starts the repair. `repair` asks for the repairman at priority 1. `break_loop` waits an exponential time, and then interrupts the `part` of its machine, if that machine is not broken.

```yaml
machines:
  initial: 10
  spawn: start_machine
  key: {gen: sequence, format: "Machine {}"}
  attributes:
    id: {gen: sequence}
    parts_made: 0
    broken: 0
```

```yaml
start_machine:
  payload: {event_type: machine_started}
  spawn: {task: break_loop, copy_attributes: true}
  next: part
part:
  service_time: {gen: sample, dist: truncated_normal, mean: 10, std: 2, low: 0}
  on_interrupt: resume
  on_interrupt_run: begin_repair
  set:
    parts_made: {gen: expr, expr: "attr.parts_made + 1"}
  on:
    - path: MachineShop.variables.parts_made
      value:
        gen: expr
        expr: "set_key(registry['MachineShop.variables.parts_made'], process.key,
          get_key(registry['MachineShop.variables.parts_made'], process.key, 0) + 1)"
  payload: {event_type: part_made}
  next: part
begin_repair:
  payload: {event_type: broken}
  set: {broken: 1}
  next: repair
repair:
  resource: repairman
  priority: 1
  service: repair_time
  set: {broken: 0}
  payload: {event_type: repaired}
# The machine's failures: one wait for each, then an interrupt of its part
break_loop:
  wait:
    timeout: {gen: sample, dist: exponential, rate: 0.0033333333333333335}
  payload: {event_type: tick}
  on:
    - path: MachineShop.variables.target
      value: {gen: expr, expr: "attr.id"}
    - interrupt:
        task: part
        where: "attr.id == registry['MachineShop.variables.target'] and attr.broken ==
          0"
  next: break_loop
```

The same tasks, with the same keys on `add_task`.

```python
app.add_initial_arrival(
    "machines",
    size,
    key=lambda n: "Machine {}".format(n - 1),
    attributes={"id": generator({"gen": "sequence"}), "parts_made": 0, "broken": 0},
    spawn="start_machine",
)
```

```python
@app.task(
    name="start_machine",
    spawn={"task": "break_loop", "copy_attributes": True},
    next="part",
)
def start_machine(task_id):
    """Starts the machine and its failures."""
    return {"event_type": "machine_started"}

@app.task(
    name="part",
    service_time=generator(
        {
            "gen": "sample",
            "dist": "truncated_normal",
            "mean": 10,
            "std": 2,
            "low": 0,
        }
    ),
    on_interrupt="resume",
    on_interrupt_run="begin_repair",
    set={"parts_made": generator({"gen": "expr", "expr": "attr.parts_made + 1"})},
    on=[
        {
            "path": "MachineShop.variables.parts_made",
            "value": generator(
                {
                    "gen": "expr",
                    "expr": "set_key(registry['MachineShop.variables.parts_made'], process.key, get_key(registry['MachineShop.variables.parts_made'], process.key, 0) + 1)",
                }
            ),
        }
    ],
    next="part",
)
def part(task_id):
    """Makes a part."""
    return {"event_type": "part_made"}

@app.task(name="begin_repair", set={"broken": 1}, next="repair")
def begin_repair(task_id):
    """Marks the machine broken."""
    return {"event_type": "broken"}

@app.task(
    name="repair",
    resource_id="repairman",
    priority=1,
    service_id="repair_time",
    set={"broken": 0},
)
def repair(task_id):
    """Repairs the machine."""
    return {"event_type": "repaired"}

# The machine's failures: one wait for each, then an interrupt of its part
@app.task(
    name="break_loop",
    wait={
        "timeout": generator(
            {"gen": "sample", "dist": "exponential", "rate": 0.0033333333333333335}
        )
    },
    on=[
        {
            "path": "MachineShop.variables.target",
            "value": generator({"gen": "expr", "expr": "attr.id"}),
        },
        {
            "interrupt": {
                "task": "part",
                "where": "attr.id == registry['MachineShop.variables.target'] and attr.broken == 0",
            }
        },
    ],
    next="break_loop",
)
def break_loop(task_id):
    """Breaks the machine down."""
    return {"event_type": "tick"}
```

`Machine` is SimPy's class. Its `working` method catches the `Interrupt`, publishes the records and calls `repair`.

```python
class Machine:
    """A machine that makes parts and breaks down now and then."""

    def __init__(self, name, number):
        self.name = name
        self.number = number
        self.broken = False
        self.process = env.process(self.working())
        names[self.process] = name

    def working(self):
        env.process(self.break_machine())
        started = {"event_type": "machine_started"}
        publish(self.name, "start_machine", next(numbers["start_machine"]), started)
        while True:
            making = next(numbers["part"])
            publish(self.name, "part", making, {"status": "started"})
            done_in = sampler.sample(part)
            while done_in:
                start = env.now
                try:
                    yield env.timeout(done_in)
                    done_in = 0
                except simpy.Interrupt:
                    interrupted = {"status": "interrupted", "cause": None}
                    publish(self.name, "part", making, interrupted)
                    broken = {"event_type": "broken"}
                    publish(
                        self.name,
                        "begin_repair",
                        next(numbers["begin_repair"]),
                        broken,
                    )
                    self.broken = True
                    done_in -= env.now - start
                    yield from self.repair()
                    publish(self.name, "part", making, {"status": "resumed"})
            publish(self.name, "part", making, {"event_type": "part_made"})

    def repair(self):
        number = next(numbers["repair"])
        publish(
            self.name,
            "repair",
            number,
            {"path_id": REPAIR, **REPAIR_NAMES, "status": "queued"},
        )
        # The repair goes before the repairman's other jobs
        with repairman.request(priority=1) as req:
            yield req
            publish(
                self.name,
                "repair",
                number,
                {"path_id": REPAIR, **REPAIR_NAMES, "status": "started"},
            )
            yield env.timeout(REPAIR_TIME)
        publish(self.name, "repair", number, {"event_type": "repaired"})
        self.broken = False

    def break_machine(self):
        name = f"break_loop-{self.number + 1}"
        while True:
            number = next(numbers["break_loop"])
            publish(name, "break_loop", number, {"status": "queued"})
            yield env.timeout(sampler.sample(failure))
            publish(name, "break_loop", number, {"status": "started"})
            publish(name, "break_loop", number, {"event_type": "tick"})
            if not self.broken:
                self.process.interrupt()
```

### Repairman Does Other Jobs

SimPy's `other_jobs` asks for the repairman at priority 2 and works on a 30 minute job. A repair preempts it, and the job resumes with the time it had left.

```python
def other_jobs(env, repairman):
    """The repairman's other (unimportant) job."""
    while True:
        # Start a new job
        done_in = JOB_DURATION
        while done_in:
            # Retry the job until it is done.
            # Its priority is lower than that of machine repairs.
            with repairman.request(priority=2) as req:
                yield req
                start = env.now
                try:
                    yield env.timeout(done_in)
                    done_in = 0
                except simpy.Interrupt:
                    done_in -= env.now - start
```

The arrival `others` starts one `other_job` task at time 0. The task asks for the repairman at priority 2, takes 30 seconds, resumes after a preemption with `on_interrupt: resume`, and starts again with `next: other_job`.

```yaml
others: {initial: 1, spawn: other_job}
```

```yaml
other_job:
  resource: repairman
  priority: 2
  service_time: 30
  on_interrupt: resume
  payload: {event_type: other_done}
  next: other_job
```

`add_initial_arrival` starts the task, and the task takes the same keys.

```python
app.add_initial_arrival("others", 1, spawn="other_job")
```

```python
@app.task(
    name="other_job",
    resource_id="repairman",
    priority=2,
    service_time=30,
    on_interrupt="resume",
    next="other_job",
)
def other_job(task_id):
    """Does one of the repairman's other jobs."""
    return {"event_type": "other_done"}
```

`other_jobs` is SimPy's function. The last block starts the ten machines and the repairman's jobs.

```python
def other_jobs(name):
    while True:
        number = next(numbers["other_job"])
        publish(name, "other_job", number, {"status": "queued"})
        done_in = JOB_DURATION
        status = "started"
        while done_in:
            with repairman.request(priority=2) as req:
                yield req
                publish(name, "other_job", number, {"status": status})
                start = env.now
                try:
                    yield env.timeout(done_in)
                    done_in = 0
                except simpy.Interrupt as interrupt:
                    preempted = interrupt.cause
                    publish(
                        name,
                        "other_job",
                        number,
                        {
                            "status": "preempted",
                            "resource": "repairman",
                            "preempted_by": names[preempted.by],
                            "usage_since": preempted.usage_since,
                        },
                    )
                    done_in -= env.now - start
                    status = "resumed"
        publish(name, "other_job", number, {"event_type": "other_done"})
```

```python
for number in range(size):
    Machine(f"Machine {number}", number)
env.process(other_jobs("others-1"))
```

### Run It

`desflow run` loads the blueprint and runs it unpaced, because it sets `factor: 0`. Each record is printed to the terminal.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get simpy-machine-shop --api yaml
cd simpy-machine-shop/yaml

# 3. Run it
desflow run machine_shop.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get simpy-machine-shop --api yaml
cd simpy-machine-shop/yaml

# 3. Run it
desflow run machine_shop.yaml
```

The script builds the model with the builder API, runs it unpaced, and prints each record to the terminal.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get simpy-machine-shop --api builder
cd simpy-machine-shop/builder

# 3. Run it
uv run --no-project --with desflow machine_shop_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get simpy-machine-shop --api builder
cd simpy-machine-shop/builder

# 3. Run it
python machine_shop_example.py
```

The script wires the model by hand on `DynamicRealtimeEnvironment`, runs it unpaced, and prints each record to the terminal.

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get simpy-machine-shop --api low-level
cd simpy-machine-shop/low-level

# 3. Run it
uv run --no-project --with desflow machine_shop_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get simpy-machine-shop --api low-level
cd simpy-machine-shop/low-level

# 3. Run it
python machine_shop_example.py
```

______________________________________________________________________

## Check the Result

In the 40320 seconds the ten machines make 32637 parts. Machine 0 makes the most, 3329, and Machine 3 the fewest, 3194. In SimPy's output, below, the machines make between 3185 and 3387 parts each.

1124 machines break down and 1123 repairs finish, because the last repair is still running when the run ends. The repairman finishes 220 other jobs, and a repair preempts a job 223 times. SimPy's example does not print these counts. Here each one is a record with the machine's key, and the preempted job's record names the repair that preempted it. desflow draws each random value from its own stream, named by the place where the value is used, such as `MachineShop.service.patience`. A seed of 42 therefore gives different numbers from SimPy's `random.seed(42)`. The system is the same. The figures are not.

```text
Machine shop
Machine shop results after 4 weeks
Machine 0 made 3251 parts.
Machine 1 made 3273 parts.
Machine 2 made 3242 parts.
Machine 3 made 3343 parts.
Machine 4 made 3387 parts.
Machine 5 made 3244 parts.
Machine 6 made 3269 parts.
Machine 7 made 3185 parts.
Machine 8 made 3302 parts.
Machine 9 made 3279 parts.
```

The three versions in desflow print the same records, which [a test](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc2/tests/docs/test_example_twins.py) checks by running each version unpaced. Each record has `sim_ts`, the simulation time, and a key for the entity it belongs to. The statistics and the parameters that a live run can change are in the same stream, which SimPy's file does not have.

______________________________________________________________________

## Full Files

Full files

The blueprint is `machine_shop.yaml`.

```yaml
# Machine Shop: SimPy's machine_shop.py example, in YAML.
#
# The YAML version of machine_shop_example.py, which --api builder downloads. A shop
# has ten machines, each making parts without end; a part's time is normal with
# standard deviation 2, never below 0. Each machine breaks down after an exponential
# time, unless it is already broken. A broken machine waits for the one repairman, at
# priority 1, for its repair, and then finishes its part. Between repairs the
# repairman does other jobs at priority 2, which a repair preempts and which resume
# afterwards.
#
# SimPy's example counts minutes, and here each of its minutes is one second. So a
# part takes about 10 seconds, a machine breaks down about every 300 seconds, a repair
# and another job each take 30 seconds, and the run ends after 40320 seconds, SimPy's
# four weeks. Each value is drawn from desflow's random sequence for its place in the
# file, so the numbers differ from those SimPy prints.
#
# The breakdowns are written as tasks: each machine's `break_loop` waits for the
# next failure and interrupts the machine's `part`, whose `on_interrupt_run` starts the
# repair. `MachineShop.variables.parts_made` counts the parts of each machine.
#
# Needs no containers. Runs unpaced and ends after 40320 simulation seconds.
# Run it from its folder with: desflow run machine_shop.yaml

simulation: {sim_id: MachineShop, factor: 0, random_seed: 42}

egress:
  - {type: Console}

variables:
  target: -1
  parts_made: {}

resources:
  repairman: {current_cap: 1, queue: priority, preempt: true}

services:
  repair_time: {dist: constant, value: 30}

arrivals:
  machines:
    initial: 10
    spawn: start_machine
    key: {gen: sequence, format: "Machine {}"}
    attributes:
      id: {gen: sequence}
      parts_made: 0
      broken: 0
  others: {initial: 1, spawn: other_job}

tasks:
  start_machine:
    payload: {event_type: machine_started}
    spawn: {task: break_loop, copy_attributes: true}
    next: part
  part:
    service_time: {gen: sample, dist: truncated_normal, mean: 10, std: 2, low: 0}
    on_interrupt: resume
    on_interrupt_run: begin_repair
    set:
      parts_made: {gen: expr, expr: "attr.parts_made + 1"}
    on:
      - path: MachineShop.variables.parts_made
        value:
          gen: expr
          expr: "set_key(registry['MachineShop.variables.parts_made'], process.key,
            get_key(registry['MachineShop.variables.parts_made'], process.key, 0) + 1)"
    payload: {event_type: part_made}
    next: part
  begin_repair:
    payload: {event_type: broken}
    set: {broken: 1}
    next: repair
  repair:
    resource: repairman
    priority: 1
    service: repair_time
    set: {broken: 0}
    payload: {event_type: repaired}
  # The machine's failures: one wait for each, then an interrupt of its part
  break_loop:
    wait:
      timeout: {gen: sample, dist: exponential, rate: 0.0033333333333333335}
    payload: {event_type: tick}
    on:
      - path: MachineShop.variables.target
        value: {gen: expr, expr: "attr.id"}
      - interrupt:
          task: part
          where: "attr.id == registry['MachineShop.variables.target'] and attr.broken ==
            0"
    next: break_loop
  other_job:
    resource: repairman
    priority: 2
    service_time: 30
    on_interrupt: resume
    payload: {event_type: other_done}
    next: other_job

run: {until: 40320}
```

The script builds the model with the builder API.

```python
"""Machine Shop: SimPy's machine_shop.py example, builder API.

A shop has ten machines, each making parts without end; a part's time is normal with
standard deviation 2, never below 0. Each machine breaks down after an exponential
time, unless it is already broken. A broken machine waits for the one repairman, at
priority 1, for its repair, and then finishes its part. Between repairs the repairman
does other jobs at priority 2, which a repair preempts and which resume afterwards.

SimPy's example counts minutes, and here each of its minutes is one second. So a part
takes about 10 seconds, a machine breaks down about every 300 seconds, a repair and
another job each take 30 seconds, and the run ends after 40320 seconds, SimPy's four
weeks. Each value is drawn from desflow's random sequence for its place in the model,
so the numbers differ from those SimPy prints.

The breakdowns are written as tasks: each machine's `break_loop` waits for the next
failure and interrupts the machine's `part`, whose `on_interrupt_run` starts the
repair. `MachineShop.variables.parts_made` counts the parts of each machine.

Needs no containers. Runs unpaced and ends after 40320 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import generator


def build(size: int = 10) -> SimulationContext:
    """Builds the shop, with `size` machines."""
    app = SimulationContext(sim_id="MachineShop", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    app.add_resource("repairman", current_cap=1, queue="priority", preempt=True)
    app.add_variable("target", -1)
    app.add_variable("parts_made", {})
    app.add_service("repair_time", dist="constant", value=30)
    app.add_initial_arrival(
        "machines",
        size,
        key=lambda n: "Machine {}".format(n - 1),
        attributes={"id": generator({"gen": "sequence"}), "parts_made": 0, "broken": 0},
        spawn="start_machine",
    )
    app.add_initial_arrival("others", 1, spawn="other_job")

    @app.task(
        name="start_machine",
        spawn={"task": "break_loop", "copy_attributes": True},
        next="part",
    )
    def start_machine(task_id):
        """Starts the machine and its failures."""
        return {"event_type": "machine_started"}

    @app.task(
        name="part",
        service_time=generator(
            {
                "gen": "sample",
                "dist": "truncated_normal",
                "mean": 10,
                "std": 2,
                "low": 0,
            }
        ),
        on_interrupt="resume",
        on_interrupt_run="begin_repair",
        set={"parts_made": generator({"gen": "expr", "expr": "attr.parts_made + 1"})},
        on=[
            {
                "path": "MachineShop.variables.parts_made",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['MachineShop.variables.parts_made'], process.key, get_key(registry['MachineShop.variables.parts_made'], process.key, 0) + 1)",
                    }
                ),
            }
        ],
        next="part",
    )
    def part(task_id):
        """Makes a part."""
        return {"event_type": "part_made"}

    @app.task(name="begin_repair", set={"broken": 1}, next="repair")
    def begin_repair(task_id):
        """Marks the machine broken."""
        return {"event_type": "broken"}

    @app.task(
        name="repair",
        resource_id="repairman",
        priority=1,
        service_id="repair_time",
        set={"broken": 0},
    )
    def repair(task_id):
        """Repairs the machine."""
        return {"event_type": "repaired"}

    # The machine's failures: one wait for each, then an interrupt of its part
    @app.task(
        name="break_loop",
        wait={
            "timeout": generator(
                {"gen": "sample", "dist": "exponential", "rate": 0.0033333333333333335}
            )
        },
        on=[
            {
                "path": "MachineShop.variables.target",
                "value": generator({"gen": "expr", "expr": "attr.id"}),
            },
            {
                "interrupt": {
                    "task": "part",
                    "where": "attr.id == registry['MachineShop.variables.target'] and attr.broken == 0",
                }
            },
        ],
        next="break_loop",
    )
    def break_loop(task_id):
        """Breaks the machine down."""
        return {"event_type": "tick"}


    @app.task(
        name="other_job",
        resource_id="repairman",
        priority=2,
        service_time=30,
        on_interrupt="resume",
        next="other_job",
    )
    def other_job(task_id):
        """Does one of the repairman's other jobs."""
        return {"event_type": "other_done"}


    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs 40320 seconds, SimPy's four weeks counted in minutes."""
    model.run(until=4 * 7 * 24 * 60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
```

The script wires the model by hand on `DynamicRealtimeEnvironment`, with SimPy's own processes.

```python
"""Machine Shop: SimPy's machine_shop.py example, low-level API.

The low-level version of `builder/simpy/machine_shop_example.py`: SimPy's own
`Machine` and `other_jobs` on a `DynamicRealtimeEnvironment`. They publish with
`publish_event` the records that the YAML and builder versions publish: each task's
`queued`, `started`, `interrupted`, `preempted` and `resumed`, its own events, and the
number of the task's instance. A shop has ten machines, each making parts without end.
Each machine breaks down now and then, unless it is already broken. A broken machine
waits for the one repairman, at priority 1, and then finishes its part. Between
repairs the repairman does other jobs at priority 2, which a repair preempts and which
resume afterwards.

SimPy's example counts minutes, and here each of its minutes is one second. So a part
takes about 10 seconds, a machine breaks down about every 300 seconds, a repair and
another job each take 30 seconds, and the run ends after 40320 seconds, SimPy's four
weeks. Each value is drawn from desflow's `Sampler`, from the random sequence the YAML
and builder versions use for the same place, so the three versions publish the same
records.

Needs no containers. Runs unpaced and ends after 40320 simulation seconds.
"""

import itertools
import logging

import simpy

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

REPAIR_TIME = 30
JOB_DURATION = 30
NUM_MACHINES = 10
UNTIL = 4 * 7 * 24 * 60  # SimPy's four weeks in minutes, read as seconds
REPAIR = "MachineShop.service.repair_time"  # the service a repair's records name
REPAIR_NAMES = {"sim_id": "MachineShop", "service": "repair_time"}
TASKS = ("start_machine", "part", "begin_repair", "repair", "break_loop", "other_job")


def build(env: DynamicRealtimeEnvironment, size: int = NUM_MACHINES) -> None:
    """Starts `size` machines and the repairman's other jobs on `env`."""
    sampler = Sampler(seed=42)
    part = DistributionConfig(dist="truncated_normal", mean=10, std=2, low=0)
    part.stream = "tasks.part.service_time"  # the sequence the blueprint draws from
    failure = DistributionConfig(dist="exponential", rate=0.0033333333333333335)
    failure.stream = "tasks.break_loop.wait.timeout"
    repairman = simpy.PreemptiveResource(env, capacity=1)
    names: dict = {}  # each machine's process, by the name its records carry
    numbers = {task: itertools.count() for task in TASKS}

    def publish(name, task, number, value):
        env.publish_event(name, value, task=task, instance=number)

    class Machine:
        """A machine that makes parts and breaks down now and then."""

        def __init__(self, name, number):
            self.name = name
            self.number = number
            self.broken = False
            self.process = env.process(self.working())
            names[self.process] = name

        def working(self):
            env.process(self.break_machine())
            started = {"event_type": "machine_started"}
            publish(self.name, "start_machine", next(numbers["start_machine"]), started)
            while True:
                making = next(numbers["part"])
                publish(self.name, "part", making, {"status": "started"})
                done_in = sampler.sample(part)
                while done_in:
                    start = env.now
                    try:
                        yield env.timeout(done_in)
                        done_in = 0
                    except simpy.Interrupt:
                        interrupted = {"status": "interrupted", "cause": None}
                        publish(self.name, "part", making, interrupted)
                        broken = {"event_type": "broken"}
                        publish(
                            self.name,
                            "begin_repair",
                            next(numbers["begin_repair"]),
                            broken,
                        )
                        self.broken = True
                        done_in -= env.now - start
                        yield from self.repair()
                        publish(self.name, "part", making, {"status": "resumed"})
                publish(self.name, "part", making, {"event_type": "part_made"})

        def repair(self):
            number = next(numbers["repair"])
            publish(
                self.name,
                "repair",
                number,
                {"path_id": REPAIR, **REPAIR_NAMES, "status": "queued"},
            )
            # The repair goes before the repairman's other jobs
            with repairman.request(priority=1) as req:
                yield req
                publish(
                    self.name,
                    "repair",
                    number,
                    {"path_id": REPAIR, **REPAIR_NAMES, "status": "started"},
                )
                yield env.timeout(REPAIR_TIME)
            publish(self.name, "repair", number, {"event_type": "repaired"})
            self.broken = False

        def break_machine(self):
            name = f"break_loop-{self.number + 1}"
            while True:
                number = next(numbers["break_loop"])
                publish(name, "break_loop", number, {"status": "queued"})
                yield env.timeout(sampler.sample(failure))
                publish(name, "break_loop", number, {"status": "started"})
                publish(name, "break_loop", number, {"event_type": "tick"})
                if not self.broken:
                    self.process.interrupt()


    def other_jobs(name):
        while True:
            number = next(numbers["other_job"])
            publish(name, "other_job", number, {"status": "queued"})
            done_in = JOB_DURATION
            status = "started"
            while done_in:
                with repairman.request(priority=2) as req:
                    yield req
                    publish(name, "other_job", number, {"status": status})
                    start = env.now
                    try:
                        yield env.timeout(done_in)
                        done_in = 0
                    except simpy.Interrupt as interrupt:
                        preempted = interrupt.cause
                        publish(
                            name,
                            "other_job",
                            number,
                            {
                                "status": "preempted",
                                "resource": "repairman",
                                "preempted_by": names[preempted.by],
                                "usage_since": preempted.usage_since,
                            },
                        )
                        done_in -= env.now - start
                        status = "resumed"
            publish(name, "other_job", number, {"event_type": "other_done"})


    for number in range(size):
        Machine(f"Machine {number}", number)
    env.process(other_jobs("others-1"))


def run() -> None:
    """Runs 40320 seconds, SimPy's four weeks counted in minutes."""
    env = DynamicRealtimeEnvironment(factor=0)
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

This is SimPy 4.1.2's `machine_shop.py`, with SimPy's licence notice added at the top and comment lines that mark the pieces shown above. Run with Python, it prints the output shown under Check the Result.

```python
# SimPy 4.1.2, docs/examples/code/machine_shop.py, under SimPy's licence (LICENSE.rst):
#
# The MIT License (MIT)
#
# Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke (also see AUTHORS.txt)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
Machine shop example

Covers:

- Interrupts
- Resources: PreemptiveResource

Scenario:
  A workshop has *n* identical machines. A stream of jobs (enough to
  keep the machines busy) arrives. Each machine breaks down
  periodically. Repairs are carried out by one repairman. The repairman
  has other, less important tasks to perform, too. Broken machines
  preempt these tasks. The repairman continues them when he is done
  with the machine repair. The workshop works continuously.

"""

import random

import simpy

# fmt: off
RANDOM_SEED = 42
PT_MEAN = 10.0         # Avg. processing time in minutes
PT_SIGMA = 2.0         # Sigma of processing time
MTTF = 300.0           # Mean time to failure in minutes
BREAK_MEAN = 1 / MTTF  # Param. for expovariate distribution
REPAIR_TIME = 30.0     # Time it takes to repair a machine in minutes
JOB_DURATION = 30.0    # Duration of other jobs in minutes
NUM_MACHINES = 10      # Number of machines in the machine shop
WEEKS = 4              # Simulation time in weeks
SIM_TIME = WEEKS * 7 * 24 * 60  # Simulation time in minutes
# fmt: on


def time_per_part():
    """Return actual processing time for a concrete part."""
    t = random.normalvariate(PT_MEAN, PT_SIGMA)
    # The normalvariate can be negative, but we want only positive times.
    while t <= 0:
        t = random.normalvariate(PT_MEAN, PT_SIGMA)
    return t


def time_to_failure():
    """Return time until next failure for a machine."""
    return random.expovariate(BREAK_MEAN)


class Machine:
    """A machine produces parts and may get broken every now and then.

    If it breaks, it requests a *repairman* and continues the production
    after the it is repaired.

    A machine has a *name* and a number of *parts_made* thus far.

    """

    def __init__(self, env, name, repairman):
        self.env = env
        self.name = name
        self.parts_made = 0
        self.broken = False

        # Start "working" and "break_machine" processes for this machine.
        self.process = env.process(self.working(repairman))
        env.process(self.break_machine())

    def working(self, repairman):
        """Produce parts as long as the simulation runs.

        While making a part, the machine may break multiple times.
        Request a repairman when this happens.

        """
        while True:
            # Start making a new part
            done_in = time_per_part()
            while done_in:
                start = self.env.now
                try:
                    # Working on the part
                    yield self.env.timeout(done_in)
                    done_in = 0  # Set to 0 to exit while loop.

                except simpy.Interrupt:
                    self.broken = True
                    done_in -= self.env.now - start  # How much time left?

                    # Request a repairman. This will preempt its "other_job".
                    with repairman.request(priority=1) as req:
                        yield req
                        yield self.env.timeout(REPAIR_TIME)

                    self.broken = False

            # Part is done.
            self.parts_made += 1

    def break_machine(self):
        """Break the machine every now and then."""
        while True:
            yield self.env.timeout(time_to_failure())
            if not self.broken:
                # Only break the machine if it is currently working.
                self.process.interrupt()


def other_jobs(env, repairman):
    """The repairman's other (unimportant) job."""
    while True:
        # Start a new job
        done_in = JOB_DURATION
        while done_in:
            # Retry the job until it is done.
            # Its priority is lower than that of machine repairs.
            with repairman.request(priority=2) as req:
                yield req
                start = env.now
                try:
                    yield env.timeout(done_in)
                    done_in = 0
                except simpy.Interrupt:
                    done_in -= env.now - start


# Setup and start the simulation
print('Machine shop')
random.seed(RANDOM_SEED)  # This helps to reproduce the results

# Create an environment and start the setup process
env = simpy.Environment()
repairman = simpy.PreemptiveResource(env, capacity=1)
machines = [Machine(env, f'Machine {i}', repairman) for i in range(NUM_MACHINES)]
env.process(other_jobs(env, repairman))

# Execute!
env.run(until=SIM_TIME)

# Analysis/results
print(f'Machine shop results after {WEEKS} weeks')
for machine in machines:
    print(f'{machine.name} made {machine.parts_made} parts.')
```

______________________________________________________________________

## Sources

- [SimPy 4.1.2, Machine Shop](https://simpy.readthedocs.io/en/4.1.2/examples/machine_shop.html): the original code and its output.
- [SimPy's licence](https://gitlab.com/team-simpy/simpy/-/blob/master/LICENSE.rst): MIT, Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke. The SimPy file above carries this notice.
- [`examples/` in the desflow repository](https://github.com/jaehyeon-kim/desflow/tree/v1.0.0rc2/examples): the three versions in desflow, under `simpy/` in each folder.
