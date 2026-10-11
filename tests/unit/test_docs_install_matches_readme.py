"""The README and the docs Installation page give the same install commands.

GitHub and PyPI show the README, and the docs show the Installation page, so a reader
sees one or the other. The uv commands, the pip commands and the sentence on extras
must be the same in both.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _commands(text):
    """Every non-comment line of the bash blocks in the install section, and the extras line."""
    section = text[text.index("nstall") : text.index("| Extra |")]
    lines = []
    for block in re.findall(r"```bash\n(.*?)\n *```", section, re.S):
        lines += [l.strip() for l in block.split("\n") if l.strip()]
    extras = re.search(r"The connectors are optional extras[^\n]*", section).group(0)
    return lines, extras


def test_readme_and_docs_install_the_same_way():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    docs = (ROOT / "docs" / "getting-started.md").read_text(encoding="utf-8")
    readme = readme[readme.index("## Installation") :]
    assert _commands(readme) == _commands(docs)
