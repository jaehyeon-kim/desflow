"""The README's Learn More list names every top-level section of the docs sidebar.

GitHub and PyPI readers see the README, not the sidebar, so a section added to the
docs and not to that list would be invisible to them.
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_learn_more_names_every_sidebar_section():
    nav = yaml.safe_load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"))["nav"]
    sections = [next(iter(item)) for item in nav if isinstance(item, dict)]
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    learn_more = readme[readme.index("## Learn More") : readme.index("## Related reading")]
    missing = [s for s in sections if s != "Home" and f"- **{s}**" not in learn_more]
    assert missing == [], f"Add these sidebar sections to Learn More in README.md: {missing}"
