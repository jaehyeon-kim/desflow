# MCP Server

`desflow mcp` serves the desflow commands as tools to an AI assistant on the same machine. MCP, the Model Context Protocol, lets an assistant find the tools of a server and call them, each with named parameters. The assistant's MCP client starts `desflow mcp` as a process and exchanges messages with it over the process's standard input and output. Each tool runs the desflow command of the same name, in the server's process, so it checks and refuses its arguments as the command does.

The [plugin](ai-assistants.md) starts the server for Claude Code, Codex and Antigravity, with the [skill](../ai/index.md). This page sets the server up for another assistant, or for one without the plugin, and calls its tools from a Python script.

---

## Install

The server needs the `mcp` extra:

=== "uv"

    ```bash title="docs/snippets/integrations/commands.sh:install-uv"
    uv tool install "desflow[mcp]"
    ```

=== "pip"

    ```bash title="docs/snippets/integrations/commands.sh:install-pip"
    pip install "desflow[mcp]"
    ```

Without the extra, `desflow mcp` exits with an error that names the `pip install` command. The scripts on this page also import the `mcp` package, so run them where `pip install "desflow[mcp]"` installed it, such as a virtual environment of their own. `uv tool install` keeps desflow in an environment that a script cannot import from.

---

## Tools That Write Nothing

The server always offers these seven tools. Each takes the arguments and options of its command as parameters, with the same names, and `variables` fills `${NAME}` in a blueprint as `--var` does:

| Tool | Command | Returns |
|---|---|---|
| `check` | `desflow check` | what the command prints |
| `explain` | `desflow explain --json` | the JSON document that the command prints |
| `test` | `desflow test` | what the command prints |
| `fit` | `desflow fit`, which needs the `fit` extra | what the command prints |
| `dist` | `desflow dist` | what the command prints |
| `schema` | `desflow schema` | the blueprint JSON Schema |
| `list_examples` | `desflow examples list` | what the command prints |

A tool that returns a JSON document gives it as the result's structured content and as text. A tool that returns what the command prints gives its warnings first. A tool whose command fails returns an error with the command's message. The server runs one tool at a time.

---

## Tools That Write

`desflow mcp --allow-changes` also offers four tools, and two parameters of the tools above:

| Tool | Command | Returns |
|---|---|---|
| `run` | `desflow run --summary` | the JSON document that `--summary` writes: the run's statistics, or with `replications` the seeds, the summary and each replication's values |
| `experiment` | `desflow experiment` | what the command prints |
| `report` | `desflow report` | what the command prints |
| `get_example` | `desflow examples get` | what the command prints |

`test` gains `fix`, which writes the statistics into the expected values that differ, and `fit` gains `out` and `plot`, which write the best fit and a picture of the fits. A run sends its records to the blueprint's sinks, such as a Kafka topic or a table, and the other tools write files. The server marks these tools as destructive and the others as read-only, so a client can ask before it runs one. The plugin starts the server without the option. Add the option to an assistant's settings only for a project whose sinks it may write to. An option shows in those settings, where anyone can see it.

[Write the Result to a File](../user-guide/results/replications.md#write-the-result-to-a-file) lists the keys of the `run` tool's document.

---

## Add It to an Assistant

The server needs the command `desflow` and the argument `mcp`. It reads a relative path in a tool call from the folder the client starts it in, which the client chooses, so ask for absolute paths when a tool does not find a file.

In Claude Code, run this in the project's folder, to add the server for that project:

```bash title="docs/snippets/integrations/commands.sh:mcp-claude-code"
# Writes .mcp.json in this folder
claude mcp add --scope project desflow -- desflow mcp
```

In Codex, this adds the server:

```bash title="docs/snippets/integrations/commands.sh:mcp-codex"
codex mcp add desflow -- desflow mcp
```

Another assistant that reads MCP servers from a JSON file takes this entry, under the key its documentation names:

```json title="docs/snippets/integrations/mcp.json"
{
  "mcpServers": {
    "desflow": {
      "command": "desflow",
      "args": ["mcp"]
    }
  }
}
```

---

## List the Tools

This script starts the server command given in its arguments and prints the name and description of each tool. Save it as `mcp_client.py`:

```python title="docs/snippets/integrations/mcp_client.py"
import sys

import anyio
from mcp import Client, StdioServerParameters


async def main(command):
    """Print the name and description of each tool the server offers.

    Args:
        command: The command that starts the server, as a list of words.
    """
    server = StdioServerParameters(command=command[0], args=command[1:])
    async with Client(server) as client:
        for tool in (await client.list_tools()).tools:
            print(f"{tool.name}: {tool.description}")


anyio.run(main, sys.argv[1:])
```

This lists the tools that write nothing:

```bash title="docs/snippets/integrations/commands.sh:list-tools"
python mcp_client.py desflow mcp
```

```text
check: Check that each blueprint or builder file builds, without running it, as `desflow check` does. variables fill ${NAME} as --var does.
explain: Explain a model as `desflow explain --json` does: its graph, each resource's utilisation, status and wait where the queueing formulas apply, the bottleneck and the tuning hints. With verify, also run it unpaced and compare. The shape can change with its schema_version.
test: Run the tests of each blueprint, or of each builder file with a tests.yaml beside it, and report the statistics that differ, as `desflow test` does. Each test runs once, unpaced, with no ingress and no egress.
fit: Fit a column of a CSV file to the distribution families and rank them, as `desflow fit` does. Needs the fit extra.
dist: List the distribution families, or convert a family's parameters to its mean and standard deviation and back, as `desflow dist` does.
schema: Return the blueprint JSON Schema, as `desflow schema` prints it.
list_examples: List the examples, with what each shows and the service it needs, as `desflow examples list` does.
```

With `--allow-changes`, the four tools that write follow, and the descriptions of `test` and `fit` name their new parameters:

```bash title="docs/snippets/integrations/commands.sh:list-tools-changes"
python mcp_client.py desflow mcp --allow-changes
```

```text
check: Check that each blueprint or builder file builds, without running it, as `desflow check` does. variables fill ${NAME} as --var does.
explain: Explain a model as `desflow explain --json` does: its graph, each resource's utilisation, status and wait where the queueing formulas apply, the bottleneck and the tuning hints. With verify, also run it unpaced and compare. The shape can change with its schema_version.
test: Run the tests of each blueprint, or of each builder file with a tests.yaml beside it, and report the statistics that differ, as `desflow test` does. Each test runs once, unpaced, with no ingress and no egress. With fix, write the statistics the runs gave into the expect values that differ, as --fix does.
fit: Fit a column of a CSV file to the distribution families and rank them, as `desflow fit` does. Needs the fit extra. out writes the best fit as YAML, and plot draws the fits as an image, as --out and --plot do.
dist: List the distribution families, or convert a family's parameters to its mean and standard deviation and back, as `desflow dist` does.
schema: Return the blueprint JSON Schema, as `desflow schema` prints it.
list_examples: List the examples, with what each shows and the service it needs, as `desflow examples list` does.
run: Run a blueprint once, or its replications, as `desflow run --summary` does, and return the summary it writes: the statistics of the run, or the seeds, the summary and every replication's values. A run sends its records to the blueprint's sinks. measures, level and workers apply only with replications. variables fill ${NAME} as --var does.
experiment: Run an experiment file and write its artifacts and report.md, as `desflow experiment` does. output overrides the folder the file names.
report: Build an experiment's report.md again from the artifacts in its output folder, as `desflow report` does.
get_example: Download an example into <dest>/<name>/<api>/ at the installed release, as `desflow examples get` does. force replaces files that are already there.
```

---

## Run a Model Through the Tools

This script checks the shop of [Integrations](index.md#one-shop-for-every-orchestrator), reads the tuning hints from its explanation, and runs it with three packers for an hour. It starts the server with `--allow-changes`, because `run` writes. Save it as `mcp_example.py` beside `shop.yaml`:

```python title="docs/snippets/integrations/mcp_example.py"
import anyio
from mcp import Client, StdioServerParameters

# The rows of the run's statistics to print
FIGURES = (
    "tasks.pack.finished",
    "tasks.pack.wait_mean",
    "resources.packers.utilization_avg",
)


async def call(client, name, arguments):
    """Call one tool and stop on an error.

    A tool that fails returns its error as the result, so the call checks for it.

    Args:
        client: The connected MCP client.
        name: The tool to call.
        arguments: The tool's parameters.

    Returns:
        The tool's result.

    Raises:
        RuntimeError: When the tool returns an error.
    """
    result = await client.call_tool(name, arguments)
    if result.is_error:
        raise RuntimeError(f"{name}: {result.content[0].text}")
    return result


async def main():
    """Check the shop, explain it, then run it with three packers."""
    server = StdioServerParameters(command="desflow", args=["mcp", "--allow-changes"])
    async with Client(server) as client:
        checked = await call(client, "check", {"files": ["shop.yaml"]})
        # check returns what the command prints, warnings first
        print("--- check")
        print(checked.content[0].text.splitlines()[-1])

        explained = await call(client, "explain", {"source": "shop.yaml"})
        # explain returns the document of desflow explain --json
        print("--- explain")
        for hint in explained.structured_content["hints"]:
            print(hint["text"])

        ran = await call(
            client,
            "run",
            {
                "file": "shop.yaml",
                "until": "3600",
                "seed": 7,
                "variables": {"PACKERS": "3"},
            },
        )
        # run returns the summary that desflow run --summary writes
        print("--- run")
        statistics = ran.structured_content["statistics"]
        print(
            {
                row["statistic"]: row["value"]
                for row in statistics
                if row["statistic"] in FIGURES
            }
        )


anyio.run(main)
```

```bash title="docs/snippets/integrations/commands.sh:run-example"
python mcp_example.py
```

```text
--- check
ok shop.yaml (sim_id Shop)
--- explain
packers with 2 units: utilisation 0.75, at most 0.85
packers with 2 units: utilisation 0.85 at a request every 1.76 seconds (34 a minute, 0.5667 a second)
--- run
{'resources.packers.utilization_avg': 49.47475468873524, 'tasks.pack.finished': 1775, 'tasks.pack.wait_mean': 0.25614739363776756}
```

The explanation holds two packers, the blueprint's default. The run's figures are those of the command on the overview page, because the tool runs the same command.

---

## Sources

The protocol and the client's `Client` and `StdioServerParameters` are those of the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk) 2.3.0. The `claude mcp add` and `codex mcp add` lines follow `claude mcp add --help` in Claude Code 2.1.326 and `codex mcp add --help` in codex-cli 0.166.0, and Claude Code's [MCP documentation](https://code.claude.com/docs/en/mcp) for the `.mcp.json` file that project scope writes.
