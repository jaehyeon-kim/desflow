# Agent Skill

The desflow skill gives an AI coding assistant the rules of the library, so that it writes, checks, tests and runs desflow models instead of guessing them.

It is one of the ways desflow makes a simulation quicker to build. A [YAML blueprint](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/blueprints/index.md) describes a whole model with no Python. The skill lets an assistant write that blueprint for you and check it. [From history to simulation](https://jaehyeon.me/desflow/latest/about/roadmap/#from-history-to-simulation), planned after 1.0, starts a model from an event log.

The skill is a folder, `skills/desflow/`, in the Agent Skills format that Claude Code and other assistants read. An assistant that supports the format loads the skill when a request mentions desflow, a blueprint, a test of a simulation model, or a SimPy model that streams records. The folder is generated from the code, so it lists what the release has.

______________________________________________________________________

## Tasks

Each page gives the request to type, what the skill tells the assistant to do, and the result on one example: the shop of the [Learn tutorial](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md), one packer and an order every 2 seconds, drafted from a description in words.

| Task                                                                                        | What the assistant gives you                                                                                  |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| [Draft a Model](https://jaehyeon.me/desflow/latest/ai/draft-a-model/index.md)               | a blueprint from a description of a process, with each assumed number marked, checked against queueing theory |
| [Port a SimPy Program](https://jaehyeon.me/desflow/latest/ai/port-a-simpy-program/index.md) | a desflow model that does what a SimPy program does, compared with the program's output                       |
| [Write Another Form](https://jaehyeon.me/desflow/latest/ai/write-another-form/index.md)     | the Builder and Low-level forms of a blueprint, which give the same records                                   |
| [Write Tests](https://jaehyeon.me/desflow/latest/ai/write-tests/index.md)                   | tests of the statistics with `desflow test`, with expected values taken from the request                      |
| [Run a Study](https://jaehyeon.me/desflow/latest/ai/run-a-study/index.md)                   | replications with a confidence interval, and a comparison of designs with `desflow experiment`                |
| [Fit a Distribution](https://jaehyeon.me/desflow/latest/ai/fit-a-distribution/index.md)     | a distribution fitted to measured times with `desflow fit`, loaded into the model                             |

______________________________________________________________________

## Install the Skill

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

______________________________________________________________________

## Check the Install

The folder must hold `SKILL.md`, and Claude Code lists the skills it has loaded with `/skills`, where `desflow` must appear:

```bash
ls ~/.claude/skills/desflow/SKILL.md
```

______________________________________________________________________

## Other Ways to Teach an Assistant

- **Schema.** Give the assistant `https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json`, or the output of `desflow schema`, to validate what it writes.
- **Documentation as text.** `https://jaehyeon.me/desflow/latest/llms.txt` is an index of the pages, and `https://jaehyeon.me/desflow/latest/llms-full.txt` holds all of them in one file.

______________________________________________________________________

## Sources

The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification). The folders Claude Code reads are in its [skills documentation](https://code.claude.com/docs/en/skills). The `gh skill install` flags are those of `gh skill install --help` in GitHub CLI 2.95.0.
