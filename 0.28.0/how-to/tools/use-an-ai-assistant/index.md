# Use an AI Assistant

Let an AI coding assistant write, check and run desflow blueprints with the rules of the library in front of it, instead of guessing them.

The skill is a folder, `skills/desflow/`, in the Agent Skills format that Claude Code and other assistants read. Its `SKILL.md` gives the assistant the rules assistants get wrong, and tells it how to:

- pick a way to write the model;
- write and run a blueprint;
- draft a model from a description of a process;
- check a model before a long run;
- run a study with a seed, a [warm-up](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#one-run-starts-empty) and [replications](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#replications), compare designs and fit measured times.

It links to files for every key, every distribution, every connector, every command, every name the package exports, the `local` example in all three APIs, and the blueprint JSON Schema. The folder is generated from the code, so it lists what the release has.

______________________________________________________________________

## Before You Start

- [YAML Blueprints](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/blueprints/index.md) describes what the assistant writes.
- [Set Up Your Editor](https://jaehyeon.me/desflow/latest/how-to/tools/set-up-your-editor/index.md) holds the schema the assistant also uses.

______________________________________________________________________

## Steps

1. Install desflow, so the assistant can run the `desflow` commands that [Command Line](https://jaehyeon.me/desflow/latest/cli/index.md) shows, such as `desflow explain` and `desflow run`.

1. Copy `skills/desflow/` into the skills folder of the assistant. Take it from the repository at the tag of the release you installed: `v` followed by what `desflow --version` prints. For Claude Code, `~/.claude/skills/desflow/` is for every project and `.claude/skills/desflow/` in a project is for that project. With git, these lines copy it for every project:

   ```bash
   # The source of the installed release, which holds the skill
   git clone --depth 1 --branch "v$(desflow --version)" https://github.com/jaehyeon-kim/desflow /tmp/desflow-src
   mkdir -p ~/.claude/skills
   cp -R /tmp/desflow-src/skills/desflow ~/.claude/skills/desflow
   ```

1. Ask the assistant for a model, such as "write a desflow blueprint for a shop with two packers and a lunch rush, and run it". It reads the skill when the request mentions desflow or a blueprint.

With the GitHub CLI, one command does the copy. `gh skill install` is a preview command of the GitHub CLI, so it needs `gh` 2.90.0 or later:

```bash
gh skill install jaehyeon-kim/desflow desflow --agent claude-code --scope user
```

`--scope user` installs for every project, in your home folder. `--scope project`, the default, installs for the current project, inside its git repository, so run it from that repository. Another assistant takes its own `--agent` value, listed in `gh skill install --help`. Without `--agent`, `gh` asks which assistant to install for. When it cannot ask, it installs for GitHub Copilot. At project scope, Copilot uses `.agents/skills`, a folder several other assistants also read. `gh` installs the latest release. `desflow@v<version>` in place of `desflow` pins one, with `<version>` the release you installed.

### Draft from a Description

When you describe a process in words and have no model yet, the skill tells the assistant to follow these steps. You can follow the same steps by hand:

1. Start from the nearest example: `desflow examples list`, then `desflow examples get <name> --api yaml`.
1. Build in stages, and run the file after each stage: the arrivals, then the tasks and their order, then the resources, then the service times, then the working hours.
1. Ask for the numbers the description does not give, such as the spread of each time, the share of each branch, the working hours and who does each task, before assuming them.
1. Mark every assumed number with a comment that says `assumed:`. The assistant lists the assumed numbers first in its reply, because you must confirm them.
1. Check the load by hand when `desflow explain` gives no estimate, as it does for a model with scenario steps. The work for one case, times the arrival rate, must fit inside the capacity of the working hours.
1. Close an arrival outside working hours with a first task that routes on a variable the scenario sets. A scenario step that sets an exponential rate to 0 is refused when the file loads.
1. Stop after three failed attempts at the same error, and read the file and the error yourself.

The repair desk is the worked example these steps were written from. The description is: "A laptop repair desk takes in about 30 laptops a day between 09:00 and 17:00. A technician diagnoses each one in about 20 minutes. Most need a part, which arrives in 1 to 3 days. Repair takes about an hour, then a test. About 1 in 10 fails the test and goes back to repair. There are 3 technicians."

The description does not give the spread of each time, the share that needs a part, how long a test takes, who tests, or the technicians' hours. The blueprint assumes them, and each assumption is marked in a comment:

```yaml
# Drafted from a description of a laptop repair desk. Each value says where it came
# from: "said" means the description gives it, and "assumed" means the user has not
# confirmed it.
# Run it with: desflow run repair_desk.yaml
simulation:
  sim_id: Repairs
  factor: 0
  random_seed: 42

variables:
  desk_open: 0  # 1 from 09:00 to 17:00, set by the scenario

resources:
  technicians: {current_cap: 0, max_cap: 3}  # said: 3 technicians; assumed: they work 09:00 to 17:00

services:
  diagnosing: {dist: lognormal, mean: 20 min, std: 10 min}  # said: about 20 minutes; assumed: the spread
  waiting_for_part: {dist: uniform, low: 1 day, high: 3 days}  # said: 1 to 3 days
  repairing: {dist: lognormal, mean: 1 hour, std: 30 min}  # said: about an hour; assumed: the spread
  testing: {dist: triangular, low: 10 min, mode: 15 min, high: 30 min}  # assumed: not described

arrivals:
  # said: about 30 a day between 09:00 and 17:00, one every 16 minutes on average while open.
  # Arrivals run all day at that rate, and check_open drops those that come while the desk is closed.
  laptops: {dist: exponential, rate: 0.0010417, spawn: check_open}

tasks:
  check_open:
    payload: {event_type: checked_open}
    next:
      when: [{condition: 'registry["Repairs.variables.desk_open"] == 1', task: diagnose}]
  diagnose:
    payload: {event_type: diagnosed}
    service: diagnosing
    resource: technicians
    next:
      weights: {order_part: 8, repair: 2}  # said: most need a part; assumed: 8 in 10
  order_part:
    payload: {event_type: part_arrived}
    service: waiting_for_part  # a timed step: no resource is held while the part is on its way
    next: repair
  repair:
    payload: {event_type: repaired}
    service: repairing
    resource: technicians
    next: test
  test:
    payload: {event_type: tested}
    service: testing
    resource: technicians  # assumed: a technician tests
    next:
      weights: {done: 9, repair: 1}  # said: about 1 in 10 fails and goes back to repair
  done:
    payload: {event_type: returned}

scenario:  # the desk opens at 09:00 and closes at 17:00 on the first day
  - {at: 32400, path: Repairs.variables.desk_open, value: 1}
  - {at: 32400, path: Repairs.resources.technicians.current_cap, value: 3}
  - {at: 61200, path: Repairs.variables.desk_open, value: 0}
  - {at: 61200, path: Repairs.resources.technicians.current_cap, value: 0}

run:
  until: 1 day
```

The skill drafts a blueprint, so the drafted model is the YAML tab. [Builder API](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/builder-api/index.md) shows the same model parts in Python.

The skill drafts a blueprint, so the drafted model is the YAML tab. [Low-level API](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/low-level-api/index.md) shows the same model parts in SimPy code.

The arrivals run all day at the open rate, because a rate of 0 is refused, and `check_open` ends the laptops that arrive while the desk is closed.

The load check follows from the means and the shares. A laptop needs about 107 technician-minutes: 20 to diagnose, 66.7 to repair (60 minutes, done 1 / 0.9 times on average because of the retests) and 20.4 to test. 30 laptops a day need about 53.5 technician-hours. 3 technicians for 8 hours give 24 hours, so the described desk cannot keep up. The assistant reports this before you read the records, and asks whether the numbers or the technicians are wrong.

### Other Ways to Teach an Assistant

- **Schema.** Give the assistant `https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json`, or the output of `desflow schema`, to validate what it writes.
- **Documentation as text.** `https://jaehyeon.me/desflow/latest/llms.txt` is an index of the pages, and `https://jaehyeon.me/desflow/latest/llms-full.txt` holds all of them in one file.

______________________________________________________________________

## Check the Result

An assistant can still write a model that runs and answers the wrong question. Read the blueprint before you trust its records, and check `desflow run` ends without an error. The skill asks the assistant to run a study rather than one run, so ask for the [95% interval](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#interval-and-half-width), and read [Trusting Results](https://jaehyeon.me/desflow/latest/learn/studies/trusting-results/index.md) for what a number from a simulation supports.

A model of the kind the assistant writes for a shop with a lunch rush is the one [Changing Demand](https://jaehyeon.me/desflow/latest/learn/inputs/changing-demand/index.md) builds. Its files are in [Full Files](#full-files): run `desflow run changing_demand.yaml`, `python changing_demand.py` or `python changing_demand_low_level.py`.

______________________________________________________________________

## Full Files

Full files

```yaml
# The Changing Demand tutorial: a 20-minute rush, with one packer or two.
# Run it with: desflow run changing_demand.yaml
# With a second packer for the rush: RUSH_PACKERS=2 desflow run changing_demand.yaml
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42

resources:
  packers: {current_cap: 1, max_cap: 2}

services:
  packing: {dist: constant, value: 1.5 s}

arrivals:
  orders: {dist: exponential, rate: 0.4, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

# The rush from 20 to 40 minutes: an order every 1.11 s on average (rate 0.9),
# and RUSH_PACKERS packers
scenario:
  - {at: 20 min, path: Shop.arrival.orders.rate, value: 0.9}
  - {at: 20 min, path: Shop.resources.packers.current_cap, value: "${RUSH_PACKERS:-1}"}
  - {at: 40 min, path: Shop.arrival.orders.rate, value: 0.4}
  - {at: 40 min, path: Shop.resources.packers.current_cap, value: 1}

# Orders packed, and the mean and longest wait, at the end
stats: {level: typical, interval: 2 h}

egress:
  - type: Console

run:
  until: 1 h
```

```python
"""The Changing Demand tutorial: a 20-minute rush, with one packer or two.

One packer and 1.5 seconds of packing. An order arrives every 2.5 seconds on average
(rate 0.4), and every 1.11 seconds (rate 0.9) from 20 to 40 minutes. Every record is
printed to the terminal, and the script prints the waits with one packer through the
rush and with a second packer for it. To see only those lines, run:
python changing_demand.py 2>/dev/null
"""

import logging

from desflow import ConsoleEgress, SimulationContext

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop(rush_packers: int) -> SimulationContext:
    """The shop of First Simulation, with a rush from 20 to 40 minutes."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=2)
        .add_service("packing", dist="constant", value=1.5)
        .add_arrival("orders", dist="exponential", rate=0.4)
        # The rush from 20 to 40 minutes: an order every 1.11 s on average (rate 0.9),
        # and rush_packers packers
        .add_scenario(
            [
                {"at": "20 min", "path": "Shop.arrival.orders.rate", "value": 0.9},
                {
                    "at": "20 min",
                    "path": "Shop.resources.packers.current_cap",
                    "value": rush_packers,
                },
                {"at": "40 min", "path": "Shop.arrival.orders.rate", "value": 0.4},
                {
                    "at": "40 min",
                    "path": "Shop.resources.packers.current_cap",
                    "value": 1,
                },
            ]
        )
        # Orders packed, and the mean and longest wait, at the end
        .with_stats("typical", interval=7200)
        .add_egress(ConsoleEgress())
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            # The gap is drawn from the rate in force now
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def outcomes(rush_packers: int) -> dict:
    """Runs the hour and returns the orders packed and their waits."""
    app = shop(rush_packers)
    app.run(until="1 h")
    r = app.env.stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "finished": r[name("finished")],
        "wait_mean": r[name("wait_mean")],
        "wait_max": r[name("wait_max")],
    }


if __name__ == "__main__":
    for packers in (1, 2):
        r = outcomes(packers)
        print(
            f"{packers} in the rush: {r['finished']} packed, "
            f"mean wait {r['wait_mean']:5.2f} s, longest {r['wait_max']:6.2f} s"
        )
```

```python
"""The Changing Demand tutorial: a 20-minute rush, with one packer or two.

One packer and 1.5 seconds of packing. An order arrives every 2.5 seconds on average
(rate 0.4), and every 1.11 seconds (rate 0.9) from 20 to 40 minutes. Every record is
printed to the terminal, and the script prints the waits with one packer through the
rush and with a second packer for it. To see only those lines, run:
python changing_demand_low_level.py 2>/dev/null
"""

import itertools
import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    run_scenario,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.4)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=2)},
)


def schedule(rush_packers):
    """The rush from 20 to 40 minutes, with rush_packers packers.

    During the rush an order arrives every 1.11 s on average (rate 0.9).
    """
    return [
        {"at": 1200, "path": "Shop.arrival.orders.rate", "value": 0.9},
        {
            "at": 1200,
            "path": "Shop.resources.packers.current_cap",
            "value": rush_packers,
        },
        {"at": 2400, "path": "Shop.arrival.orders.rate", "value": 0.4},
        {"at": 2400, "path": "Shop.resources.packers.current_cap", "value": 1},
    ]


def pack_order(env, sampler, packers, key, task_id):
    path_id = "Shop.service.packing"

    def publish(value):
        env.publish_event(key, value, task="pack_order", instance=task_id)

    publish({"path_id": path_id, "status": "queued"})
    env.stats.task_queued("pack_order", task_id)
    with packers.request() as request:
        yield request
        publish({"path_id": path_id, "status": "started"})
        env.stats.task_started("pack_order", task_id)
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        publish({"event_type": "order_packed"})
        env.stats.task_finished("pack_order", task_id)


def order_generator(env, sampler, packers):
    arrival = env.registry.get_config("Shop.arrival.orders")
    for task_id in itertools.count():
        # The gap is drawn from the rate in force now
        yield env.timeout(sampler.sample(arrival))
        key = f"orders-{task_id + 1}"
        env.process(pack_order(env, sampler, packers, key, task_id))


def run(rush_packers=1, factor=0.0):
    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    # Orders packed, and the mean and longest wait, at the end
    env.setup_stats("Shop", "typical", interval=7200)
    packers = DynamicResource(env, "Shop", "packers")
    # Each step applies before every other event at its time
    env.process(run_scenario(env, schedule(rush_packers), until=3600))
    env.process(order_generator(env, Sampler(seed=42), packers))
    try:
        env.run(until=3600)
    finally:
        env.teardown()
    return env


def outcomes(rush_packers: int) -> dict:
    """Runs the hour and returns the orders packed and their waits."""
    r = run(rush_packers).stats.readings()
    name = "Shop.stats.tasks.pack_order.{}".format
    return {
        "finished": r[name("finished")],
        "wait_mean": r[name("wait_mean")],
        "wait_max": r[name("wait_max")],
    }


if __name__ == "__main__":
    for packers in (1, 2):
        r = outcomes(packers)
        print(
            f"{packers} in the rush: {r['finished']} packed, "
            f"mean wait {r['wait_mean']:5.2f} s, longest {r['wait_max']:6.2f} s"
        )
```

______________________________________________________________________

## Sources

The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification). The folders Claude Code reads are in its [skills documentation](https://code.claude.com/docs/en/skills). The `gh skill install` flags are those of `gh skill install --help` in GitHub CLI 2.95.0.
