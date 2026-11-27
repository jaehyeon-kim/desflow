# Agent Skill

The desflow skill gives an AI coding assistant the rules of the library, so that it writes, checks, tests and runs desflow models instead of guessing them.

It is one of the ways desflow makes a simulation quicker to build. A [YAML blueprint](../../user-guide/writing-a-model/blueprints.md) describes a whole model with no Python. The skill lets an assistant write that blueprint for you and check it. [Models from history](../../about/roadmap.md#easier-to-create-simulations), planned after 1.0, start a model from an event log.

The skill is a folder, `skills/desflow/`, in the Agent Skills format that Claude Code and other assistants read. An assistant that supports the format loads the skill when a request mentions desflow, a blueprint, a test of a simulation model, or a SimPy model that streams records. The folder is generated from the code, so it lists what the release has.

[Plugin](../plugin.md) installs the skill in Claude Code, Codex or Antigravity, together with the [MCP server](../mcp-server.md), or copies the folder for another assistant.

---

## Tasks

Each page gives the request to type, what the skill tells the assistant to do, and the result on one example: the shop of the [Learn tutorial](../../learn/first-model/first-simulation.md), one packer and an order every 2 seconds, drafted from a description in words.

| Task | What the assistant gives you |
|---|---|
| [Draft a Model](../draft-a-model.md) | a blueprint from a description of a process, with each assumed number marked, checked against queueing theory |
| [Write Tests](../write-tests.md) | tests of the statistics with `desflow test`, with expected values taken from the request |
| [Run a Study](../run-a-study.md) | replications with a confidence interval, and a comparison of designs with `desflow experiment` |
| [Fit a Distribution](../fit-a-distribution.md) | a distribution fitted to measured times with `desflow fit`, loaded into the model |
| [Write Another Form](../write-another-form.md) | the Builder and Low-level forms of a blueprint, which give the same records |
| [Port a SimPy Program](../port-a-simpy-program.md) | a desflow model that does what a SimPy program does, compared with the program's output |

---

## Sources

The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification).
