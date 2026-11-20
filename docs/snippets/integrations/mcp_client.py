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
