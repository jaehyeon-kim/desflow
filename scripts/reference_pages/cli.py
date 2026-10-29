"""The CLI pages, one per command, and the help of each command."""

import re
from typing import Any, Dict, Iterator, Tuple

# The Rich tags of the Examples lists, which the plain help would print as text
_MARKUP = re.compile(r"\[/?(?:bold|dim|underline|cyan| )+\]")


def _help_text(command: Any, name: str, parent: Any = None) -> Tuple[str, Any]:
    """The help Click prints for `command`, without Rich's boxes, and its context.

    The context is made as a run makes it, without running the command, so each
    option's default is resolved; the width is fixed so the page does not depend
    on the terminal that builds it.
    """
    command.rich_markup_mode = None
    context = command.make_context(
        name, [], parent=parent, resilient_parsing=True, terminal_width=80
    )
    return _MARKUP.sub("", context.get_help()), context


def commands(command: Any, path: str, parent: Any = None) -> Iterator[Tuple[str, str]]:
    """Each command path under `command`, with its help, in the order help lists them.

    Args:
        command: The Click command or group.
        path: The command path, such as `desflow examples`.
        parent: The parent's Click context, or None for the top command.

    Yields:
        Tuple[str, str]: Each command path and its help text.
    """
    text, context = _help_text(command, path.split(" ")[-1], parent)
    yield path, text
    for name in getattr(command, "commands", {}):
        yield from commands(command.commands[name], f"{path} {name}", context)


# The section of the Command Line page that uses each command on the shop; a new
# command needs a section there, or the CLI pages fail to build
_CLI_SECTIONS = {
    "run": "run-it",
    "experiment": "compare-designs",
    "report": "compare-designs",
    "fit": "fit-measured-times",
    "schema": "set-up-an-editor",
    "dist": "convert-parameters",
    "explain": "check-it-before-a-long-run",
    "examples": "get-an-example",
    "examples-list": "get-an-example",
    "examples-get": "get-an-example",
}


def cli_overview() -> str:
    """The list of commands and the help of `desflow`, as the Command Line page shows
    them under What It Is. The page holds a copy, which `test_gen_pages.py` compares
    with this.
    """
    from typer.main import get_command

    from desflow.cli import app

    found = list(commands(get_command(app), "desflow"))
    rows = []
    for path, text in found[1:]:
        name = "-".join(path.split(" ")[1:])
        summary = text.split("\n\n")[1].strip().split("\n\n")[0]
        # A summary such as <dest>/<name>/ would read as HTML tags in a table
        cell = " ".join(summary.split()).replace("<", "&lt;").replace(">", "&gt;")
        rows.append(f"| [`{path}`]({name}.md) | {cell} |")
    help_lines = [f"    {line}".rstrip() for line in found[0][1].split("\n")]
    return "\n".join(
        [
            "| Command | What it does |",
            "|---|---|",
            *rows,
            "",
            '??? example "The help of `desflow`"',
            "",
            "    ```text",
            *help_lines,
            "    ```",
        ]
    )


def cli_pages() -> Dict[str, str]:
    """The CLI pages: `cli/<command>.md` for each command and
    sub-command, as Markdown. The help of `desflow` itself is on the Command Line
    page, from `cli_overview`."""
    from typer.main import get_command

    from desflow.cli import app

    found = list(commands(get_command(app), "desflow"))
    pages: Dict[str, str] = {}
    for path, text in found[1:]:
        name = "-".join(path.split(" ")[1:])
        pages[f"cli/{name}.md"] = "\n".join(
            [
                f"# {path}",
                "",
                f"The help of `{path}`, generated from the command's code. "
                f"[Command Line](index.md#{_CLI_SECTIONS[name]}) "
                "uses this one on the shop, and its "
                "[What It Is](index.md#what-it-is) "
                "lists every command.",
                "",
                "```text",
                text,
                "```",
                "",
            ]
        )
    return pages
