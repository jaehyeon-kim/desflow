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

   The block is for a first install. Before you run it again, delete `/tmp/desflow-src` and `~/.claude/skills/desflow`, because `git clone` stops at an existing folder and `cp -R` would put the new copy inside the old one. If `CLAUDE_CONFIG_DIR` is set, use that folder in place of `~/.claude`, here and in the check below.

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
1. Check the load by hand when `desflow explain` gives no estimate, as it does for a model with scenario steps or a task that routes on a variable. The work for one case, times the arrival rate, must fit inside the capacity of the working hours.
1. Close an arrival outside working hours with a first task that routes on a variable the scenario sets. A scenario step that sets an exponential rate to 0 is refused when the file loads.
1. Stop after three failed attempts at the same error, and read the file and the error yourself.

The repair desk is the worked example these steps were written from. The description is: "A laptop repair desk takes in about 30 laptops a day between 09:00 and 17:00. A technician diagnoses each one in about 20 minutes. Most need a part, which arrives in 1 to 3 days. Repair takes about an hour, then a test. About 1 in 10 fails the test and goes back to repair. There are 3 technicians."

The description does not give the spread of each time, the share that needs a part, how long a test takes, who tests, or the technicians' hours. The blueprint assumes them, and each assumption is marked in a comment. The resources, services and arrivals are below, and the whole file is in [Full Files](#full-files):

```yaml
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

Check first that the assistant has the skill. The folder must hold `SKILL.md`, and Claude Code lists the skills it has loaded with `/skills`, where `desflow` must appear:

```bash
ls ~/.claude/skills/desflow/SKILL.md
```

An assistant can still write a model that runs and answers the wrong question. Read the blueprint before you trust its records, and check that `desflow run` ends without an error. Save the repair desk from [Full Files](#full-files) as `repair_desk.yaml` and run it. The run ends with the teardown lines and no error:

```bash
desflow run repair_desk.yaml
```

```text
[INFO] desflow.core.context: Building SimulationContext for 'Repairs'...
[INFO] desflow.core.context: Simulation engine started.
[INFO] desflow.core.environment: Environment teardown initiated.
[INFO] desflow.core.environment: Tearing down egress connectors, flushing final events...
[INFO] desflow.core.environment: Tearing down ingress connectors...
[INFO] desflow.core.environment: Environment teardown complete.
```

Each line also starts with the wall-clock time. The first task routes on a variable and the model has scenario steps, so the formulas of `desflow explain` give no estimate and the load check above is by hand, as the steps say:

```bash
desflow explain repair_desk.yaml | grep Bottleneck
```

```text
Bottleneck: none can be named: no resource has a utilisation the formulas cover; technicians: task diagnose has no known rate: the split after check_open depends on the run; run --verify to measure it
```

The skill asks the assistant to run a study rather than one run, so ask for the [95% interval](https://jaehyeon.me/desflow/latest/learn/concepts/output-analysis/#interval-and-half-width), and read [Trusting Results](https://jaehyeon.me/desflow/latest/learn/studies/trusting-results/index.md) for what a number from a simulation supports.

______________________________________________________________________

## Full Files

Full files

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

______________________________________________________________________

## Sources

The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification). The folders Claude Code reads are in its [skills documentation](https://code.claude.com/docs/en/skills). The `gh skill install` flags are those of `gh skill install --help` in GitHub CLI 2.95.0.
