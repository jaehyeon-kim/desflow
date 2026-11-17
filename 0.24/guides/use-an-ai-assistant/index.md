# Use an AI Assistant

Let an AI coding assistant write, check and run desflow blueprints with the rules of the library in front of it, instead of guessing them. It builds on [YAML Blueprints](https://jaehyeon.me/desflow/latest/user-guide/blueprints/index.md) and on [Set Up Your Editor](https://jaehyeon.me/desflow/latest/guides/set-up-your-editor/index.md), whose schema the assistant also uses.

The skill is a folder, `skills/desflow/`, in the Agent Skills format that Claude Code and other assistants read. Its `SKILL.md` tells the assistant how to pick a way to write the model, how to write and run a blueprint, the rules assistants get wrong, and how to run a study with a seed, a warm-up and replications. It links to files for every key, every distribution, every connector, the `local` example in all three APIs, and the blueprint JSON Schema. The folder is generated from the code, so it lists what the release has.

______________________________________________________________________

## Steps

1. Install desflow, so the assistant can run `desflow run`, `desflow examples` and `desflow schema`.
1. Copy `skills/desflow/` from the repository at the tag of the release you installed, such as `v0.24.0`, into the skills folder of the assistant. For Claude Code, `~/.claude/skills/desflow/` is for every project and `.claude/skills/desflow/` in a project is for that project.
1. Ask the assistant for a model, such as "write a desflow blueprint for a shop with two packers and a lunch rush, and run it". It reads the skill when the request mentions desflow or a blueprint.

With the GitHub CLI, one command does the copy. `gh skill install` is a preview command of the GitHub CLI, so it needs `gh` 2.90.0 or later:

```bash
gh skill install jaehyeon-kim/desflow desflow --agent claude-code --scope user
```

`--scope user` installs for every project and `--scope project`, the default, for the current one. Another assistant takes its own `--agent` value, listed in `gh skill install --help`. Without `--agent`, `gh` asks which assistant to install for, and when it cannot ask it installs for GitHub Copilot, which at project scope uses `.agents/skills`, a folder several other assistants also read. `gh` installs the latest release, and `desflow@v0.24.0` after the skill name pins one.

______________________________________________________________________

## Other Ways to Teach an Assistant

- **Schema.** Give the assistant `https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json`, or the output of `desflow schema`, to validate what it writes.
- **Documentation as text.** `https://jaehyeon.me/desflow/latest/llms.txt` is an index of the pages, and `https://jaehyeon.me/desflow/latest/llms-full.txt` holds all of them in one file.

______________________________________________________________________

## What to Check

An assistant can still write a model that runs and answers the wrong question. Read the blueprint before you trust its records, and check `desflow run` ends without an error. The skill asks the assistant to run a study rather than one run, so ask for the confidence interval, and read [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/index.md) for what a number from a simulation supports.

______________________________________________________________________

## Example

The install command from the steps, then a blueprint of the kind the assistant writes for a shop with a lunch rush. Run it with `desflow run changing_demand.yaml`.

```bash
gh skill install jaehyeon-kim/desflow desflow --agent claude-code --scope user
```

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

# The rush: 0.9 orders a second from 20 to 40 minutes, and RUSH_PACKERS for it
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

______________________________________________________________________

## Sources

The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification). The folders Claude Code reads are in its [skills documentation](https://code.claude.com/docs/en/skills). The `gh skill install` flags are those of `gh skill install --help` in GitHub CLI 2.95.0.
