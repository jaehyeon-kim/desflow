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
