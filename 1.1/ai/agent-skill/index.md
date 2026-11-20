# Agent Skill

The desflow skill gives an AI coding assistant the rules of the library, so that it writes, checks, tests and runs desflow models instead of guessing them.

It is one of the ways desflow makes a simulation quicker to build. A [YAML blueprint](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/blueprints/index.md) describes a whole model with no Python. The skill lets an assistant write that blueprint for you and check it. [Models from history](https://jaehyeon.me/desflow/latest/about/roadmap/#easier-to-create-simulations), planned after 1.0, start a model from an event log.

The skill is a folder, `skills/desflow/`, in the Agent Skills format that Claude Code and other assistants read. An assistant that supports the format loads the skill when a request mentions desflow, a blueprint, a test of a simulation model, or a SimPy model that streams records. The folder is generated from the code, so it lists what the release has.

[Plugin](https://jaehyeon.me/desflow/latest/ai/plugin/index.md) installs the skill in Claude Code, Codex or Antigravity, together with the [MCP server](https://jaehyeon.me/desflow/latest/ai/mcp-server/index.md), or copies the folder for another assistant.

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

## Sources

The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification).
