"""Every example script shown in the documentation must match the file it came from.

These pages used to hold hand-copied source, and all six had drifted: one showed a
`TaskEvent` returned directly where the script returns `.model_dump(mode="json")`,
another documented a capacity schedule the script does not have, and two carried an S3
path that had been corrected in the script months earlier.

Snippet includes fixed that by never copying, but the raw markdown then showed a
`--8<--` marker rather than code to anyone reading the file on GitHub. So the source is
copied in again, and this test is what stops it drifting a second time.

A block can also show one part of a file: a title such as
`docs/snippets/factory.py:resource` names a section that the file marks with
`# --8<-- [start:resource]` and `# --8<-- [end:resource]` comment lines, and the block
must match the lines between them, without the indent they share. A block that shows
a whole file leaves out those marker lines.
"""

import re
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
# Python, YAML, CSV and JSON blocks labelled with a file from examples/ or docs/snippets/. A
# block inside a content tab is indented, so the fence's indent is captured and the
# closing fence must carry the same one. Options such as hl_lines follow the title.
BLOCK = re.compile(
    r'^( *)```(?:python|yaml|csv|json) title="((?:examples|docs/snippets)/[^"]+)"[^\n]*\n(.*?)\n\1```$',
    re.S | re.M,
)
# A named section of a file, between its start and end comment lines.
SECTION = r"^[ \t]*# --8<-- \[{kind}:{name}\][ \t]*$"
MARKER = re.compile(r"^[ \t]*(#|--) --8<-- \[(start|end):[\w-]+\][ \t]*$")
# Every YAML block, labelled or not.
YAML_BLOCK = re.compile(r"```yaml([^\n]*)\n")


def _dedent(code, indent):
    """Removes a content tab's indent. Blank lines carry none, so they are kept empty."""
    return "\n".join(
        line[len(indent) :] if line.startswith(indent) else line.strip()
        for line in code.split("\n")
    )


def _pages():
    """Every documentation page, and the README, which shows a blueprint too."""
    return sorted((ROOT / "docs").rglob("*.md")) + [ROOT / "README.md"]


def _blocks():
    for page in _pages():
        for indent, path, code in BLOCK.findall(page.read_text(encoding="utf-8")):
            yield page.relative_to(ROOT), path, _dedent(code, indent)


def test_at_least_one_page_shows_a_script():
    """A regex that silently matches nothing would make every other case vacuous."""
    assert len(list(_blocks())) >= 10


def _source(script, section):
    """The whole file, or the lines of one named section without its marker lines."""
    text = script.read_text(encoding="utf-8")
    if not section:
        # A whole file is shown without the comment lines that mark its sections. The
        # formatter keeps blank lines around a marker between two functions, so the
        # gap left by a removed marker is closed to the two blank lines PEP 8 uses.
        code = "\n".join(
            line for line in text.rstrip("\n").split("\n") if not MARKER.match(line)
        )
        return re.sub(r"\n{4,}", "\n\n\n", code)
    start = re.search(SECTION.format(kind="start", name=re.escape(section)), text, re.M)
    end = re.search(SECTION.format(kind="end", name=re.escape(section)), text, re.M)
    assert start and end, f"{script.relative_to(ROOT)} has no section '{section}'"
    # A section inside a function is shown without the function's indent
    return textwrap.dedent(text[start.end() : end.start()]).strip("\n")


def _pages_with_blocks():
    """Each page that shows a file, with all of its blocks."""
    pages = {}
    for page, path, code in _blocks():
        pages.setdefault(page, []).append((path, code))
    return sorted(pages.items())


@pytest.mark.parametrize(
    "page,blocks",
    _pages_with_blocks(),
    ids=[str(page) for page, _ in _pages_with_blocks()],
)
def test_documented_source_matches_the_script(page, blocks):
    """One case per page, naming every block of the page that differs."""
    drifted = []
    for path, code in blocks:
        file, _, section = path.partition(":")
        script = ROOT / file
        if not script.is_file():
            drifted.append(f"{path} (no such file)")
        elif code != _source(script, section):
            drifted.append(path)
    assert drifted == [], (
        f"{page} has drifted from {', '.join(drifted)}. Copy the code in again "
        f"rather than editing the page."
    )


def test_a_section_title_reads_its_section(tmp_path):
    """The section form must compare against the marked lines, not the whole file."""
    script = tmp_path / "factory.py"
    script.write_text(
        "import x\n# --8<-- [start:part]\na = 1\nb = 2\n# --8<-- [end:part]\nc = 3\n",
        encoding="utf-8",
    )
    assert _source(script, "part") == "a = 1\nb = 2"
    assert _source(script, "") == "import x\na = 1\nb = 2\nc = 3"  # markers left out


def test_every_yaml_block_is_a_file():
    """A YAML block typed into a page is never built, so nothing would catch it drifting."""
    unlabelled = [
        str(page.relative_to(ROOT))
        for page in _pages()
        for label in YAML_BLOCK.findall(page.read_text(encoding="utf-8"))
        if 'title="examples/' not in label and 'title="docs/snippets/' not in label
    ]
    assert unlabelled == [], (
        "These pages show YAML that is not a file. Put it under docs/snippets/ or "
        "examples/yaml/ and label the block with its path."
    )


def test_yaml_pages_show_yaml_files():
    """The YAML pages are what this test extends to, so a regex miss must fail."""
    # A User Guide page shows a section of an example, as path:section
    shown = {
        path.partition(":")[0]
        for _, path, _ in _blocks()
        if path.partition(":")[0].endswith(".yaml")
    }
    assert {
        str(p.relative_to(ROOT)) for p in (ROOT / "examples/yaml").rglob("*.yaml")
    } <= shown
