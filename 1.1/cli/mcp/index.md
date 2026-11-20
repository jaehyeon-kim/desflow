# desflow mcp

The help of `desflow mcp`, generated from the command's code. [Command Line](https://jaehyeon.me/desflow/latest/cli/#serve-the-commands-to-an-assistant) uses this one on the shop, and its [What It Is](https://jaehyeon.me/desflow/latest/cli/#what-it-is) lists every command.

```text
Usage: desflow mcp [OPTIONS]

  Serve the desflow commands as MCP tools to an AI assistant on this machine.

  An MCP client starts the server as a process and exchanges messages with it
  over its standard input and output. Each tool runs the command of the same
  name. The tools that write are served only with --allow-changes, an option
  that shows in the client's configuration. Needs the mcp extra.

Options:
  --allow-changes  Also serve the tools that write: run, experiment, report and
                   get_example, test with fix, and fit with out and plot. A run
                   sends records to the blueprint's sinks, and the others write
                   files.
  --help           Show this message and exit.

  Examples:

    # Serve the tools that write nothing, as an MCP client starts the
    server

    $ desflow mcp

    # Also serve the tools that run models and write files

    $ desflow mcp --allow-changes
```
