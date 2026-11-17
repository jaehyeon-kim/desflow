# Use an AI Assistant

Let an AI coding assistant write, check and run desflow blueprints with the rules of the library in front of it, instead of guessing them.

The skill is a folder, `skills/desflow/`, in the Agent Skills format that Claude Code and other assistants read. Its `SKILL.md` gives the assistant the rules assistants get wrong, and tells it how to:

- pick a way to write the model;
- write and run a blueprint;
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
