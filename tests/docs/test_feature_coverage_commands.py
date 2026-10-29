"""Every command and option runs on the Command Line page and has a reference page, a
place in the skill and a README line, and every command is used on a page that
teaches with it. feature_lists.py says how the features are read."""

import re

import pytest
from feature_lists import COMMANDS, DOCS, README, SKILL, SKILL_TEXT, gen_pages

_BASH = re.compile(r"^```bash\n(.*?)\n```$", re.S | re.M)


def _run_lines():
    """The command lines of the Command Line page, each split into words."""
    page = (DOCS / "cli" / "index.md").read_text(encoding="utf-8")
    return [
        line.split()
        for block in _BASH.findall(page)
        for line in block.splitlines()
        if line.startswith("desflow")
    ]


def _path_of(words):
    """The command path a command line runs, such as `desflow examples get`."""
    path = "desflow"
    for word in words[1:]:
        if f"{path} {word}" not in COMMANDS:
            break
        path = f"{path} {word}"
    return path


@pytest.mark.parametrize("command", sorted(COMMANDS))
def test_every_command_and_option_runs_on_the_command_line_page(command):
    lines = [words for words in _run_lines() if _path_of(words) == command]
    assert lines, f"{command}: no command line on docs/cli/index.md"
    used = {word.split("=")[0] for words in lines for word in words}
    missing = [name for name in COMMANDS[command] if name not in used]
    assert not missing, f"{command}: no command line uses {missing}"


@pytest.mark.parametrize("command", sorted(COMMANDS))
def test_every_command_has_a_reference_a_skill_line_and_a_readme_line(command):
    if command == "desflow":
        # Its help is on the Command Line page, which test_gen_pages.py checks
        assert "Usage: desflow [OPTIONS]" in gen_pages().cli_overview()
        return
    name = "-".join(command.split(" ")[1:])
    assert f"cli/{name}.md" in gen_pages().cli_pages()
    assert f"`{command}" in SKILL_TEXT, f"{command}: not in the skill"
    top = " ".join(command.split(" ")[:2])
    assert f"`{top}`" in README, f"{top}: not in the README"


def test_every_option_is_in_the_skill():
    """The skill's CLI reference lists every option of every command."""
    cli = (SKILL / "references" / "cli.md").read_text(encoding="utf-8")
    missing = [
        f"{command} {name}"
        for command, names in COMMANDS.items()
        for name in names
        if not re.search(
            rf"(?<![\w-]){re.escape(name)}\b",
            cli.split(f"## `{command}`\n")[1].split("\n## ")[0],
        )
    ]
    assert not missing, missing


# The sections whose pages use a command where its output answers the page's question
_TEACHING = (
    "learn/first-model",
    "learn/inputs",
    "learn/shop-logic",
    "learn/studies",
    "learn/running-live",
    "user-guide",
    "how-to",
)


@pytest.mark.parametrize("command", sorted(c for c in COMMANDS if c != "desflow"))
def test_every_command_is_used_beyond_the_command_line_page(command):
    """A command shown only on Command Line is easy to miss, so one other page of the
    tutorials, the User Guide, the How-To pages or the examples uses it too."""
    used = re.compile(re.escape(command) + r"(?![\w-])")
    pages = [
        page
        for section in _TEACHING
        for page in (DOCS / section).rglob("*.md")
        if page != DOCS / "cli" / "index.md"
        and used.search(page.read_text(encoding="utf-8"))
    ]
    assert pages, f"{command}: used only on the Command Line page"
