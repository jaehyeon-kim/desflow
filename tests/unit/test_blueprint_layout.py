"""Rule 6 of scripts/blueprint_layout.py keeps a comment that a tool reads whole.

yaml-language-server reads a modeline from one line, and takes the schema from that same
line, so a wrapped modeline names no schema. A `# yamllint` directive is read the same
way. A long comment above either must not carry words onto it.
"""

import importlib.util
import re
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "blueprint_layout.py"
_spec = importlib.util.spec_from_file_location("blueprint_layout", SCRIPT)
assert _spec is not None and _spec.loader is not None
blueprint_layout = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(blueprint_layout)

# What yaml-language-server matches (modelineUtil.ts): a line, then the schema from it
MODELINE = re.compile(r"^#\s+(?:yaml-language-server\s*:|\$schema:)")
SCHEMA = re.compile(r"\$schema(?:=|:[^\S\r\n]*)(\S+)")
URL = "https://example.com/desflow/latest/schema/blueprint.schema.json"
LONG = "# yaml-language-server: $schema=" + URL + "/with/a/long/path/to/reach/past/88"
BODY = "simulation: {sim_id: Shop, factor: 0}\n"


@pytest.mark.parametrize("space", [" ", "  "])
def test_a_long_modeline_stays_on_one_line(space):
    text = LONG.replace("# ", "#" + space, 1) + "\n" + BODY
    assert len(LONG) > blueprint_layout.MAX_WIDTH
    result = blueprint_layout._layout(text)
    assert result == text
    first = result.splitlines()[0]
    assert MODELINE.match(first) and SCHEMA.search(first)


def test_the_words_of_a_long_comment_are_not_carried_onto_a_modeline():
    comment = "# " + "a comment of many words that runs past the width " * 3
    assert len(comment) > blueprint_layout.MAX_WIDTH
    result = blueprint_layout._layout(f"{comment.rstrip()}\n{LONG}\n{BODY}")
    lines = result.splitlines()
    assert LONG in lines
    # the comment wrapped into its own lines, and the words it ends on stay with it
    index = lines.index(LONG)
    assert all(len(line) <= blueprint_layout.MAX_WIDTH for line in lines[:index])
    assert lines[index - 1].endswith("the width")


def test_a_yamllint_directive_takes_no_carried_words():
    comment = "# " + "a comment of many words that runs past the width " * 3
    directive = "# yamllint disable rule:line-length"
    result = blueprint_layout._layout(f"{comment.rstrip()}\n{directive}\n{BODY}")
    lines = result.splitlines()
    assert directive in lines
    assert lines[lines.index(directive) - 1].endswith("the width")


def test_a_long_ordinary_comment_is_still_wrapped():
    comment = "# " + "an ordinary comment that runs past the width " * 4
    result = blueprint_layout._layout(f"{comment.rstrip()}\n{BODY}")
    assert all(len(line) <= blueprint_layout.MAX_WIDTH for line in result.splitlines())
    assert len(result.splitlines()) > 2


@pytest.mark.parametrize(
    "text, message",
    [
        (
            "resources:\n  p: {current_cap: 1, # keep one\n      max_cap: 5}\n",
            "line 2: the layout would lose the comment `# keep one`",
        ),
        (
            "x:\n  y: [1, # first\n      2]\n",
            "line 2: the layout would lose the comment `# first`",
        ),
        (
            "x:\n  y: [1, #first\n      2]\n",
            "line 2: the layout would lose the comment `#first`",
        ),
    ],
    ids=["mapping", "sequence", "no space after the hash"],
)
def test_a_comment_inside_an_inline_collection_is_refused_not_dropped(text, message):
    with pytest.raises(ValueError, match=message):
        blueprint_layout._layout(text)
