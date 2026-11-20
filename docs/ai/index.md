# AI Assistants

An AI coding assistant can write, check and run desflow models when it has three things: the rules of the library, the desflow commands as tools it can call, and desflow's documentation as text. This section describes each and how to install them.

The parts relate as follows:

| Part | What it gives the assistant | Page |
|---|---|---|
| Agent skill | a folder, `skills/desflow/`, of rules in the Agent Skills format: how to write, check, test and run a model, and how to run a study | [Agent Skill](agent-skill.md) |
| MCP server | `desflow mcp`, which offers the desflow commands to an MCP client as tools; MCP, the Model Context Protocol, is the standard an assistant uses to call a tool | [MCP Server](mcp-server.md) |
| Plugin | one install for Claude Code, Codex or Antigravity that adds the skill and the MCP server together | [Plugin](plugin.md) |
| Schema and `llms.txt` | the blueprint's JSON Schema and the pages of this site as text | below |

The plugin is a package of the other two parts. It starts the MCP server without `--allow-changes`, so the assistant gets the tools that write nothing. Install the skill without the plugin when your tool has no plugin support, and set up the MCP server by hand to allow the tools that write. The plugin does not install desflow, and its MCP server needs the `mcp` extra: see [Before You Install](plugin.md#before-you-install).

---

## Other Ways to Teach an Assistant

- **Schema.** Give the assistant `https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json`, or the output of `desflow schema`, to validate what it writes.
- **Documentation as text.** `https://jaehyeon.me/desflow/latest/llms.txt` is an index of the pages, and `https://jaehyeon.me/desflow/latest/llms-full.txt` holds all of them in one file.
