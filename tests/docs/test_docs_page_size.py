"""A page stays small enough to read in one sitting, and a moved section still has a way in.

A page has at most 10 `##` headings and at most 15 headings in all, code blocks left
out. The sections that every page of a kind has, such as Key Takeaways, are its frame,
not its subjects, so they are not counted. A page that is longer holds more than one subject, so it becomes a folder with an
index page and one page for each subject. A Reference page that lists every key, export
or field of one subject is exempt, because its length is the subject's length.

`docs/js/moved-anchors.js` sends an old section link to the page the section moved to.
Its targets must name a page and a heading that exist, with the id the site gives that
heading: a second heading of the same text gets `_1`, a third `_2`.
"""

import importlib.util
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
MAX_SECTIONS = 10
MAX_HEADINGS = 15

# Reference pages that list every key, export or field of one subject.
LISTS_ONE_SUBJECT = {"docs/reference/api.md"}

# Pages over the limit that this plan does not split. Empty: every page passes.
KNOWN_BREAKING: dict[str, str] = {}

FENCE = re.compile(r"^```.*?^```", re.S | re.M)
HEADING = re.compile(r"^(#{2,6}) (.+?)\s*$", re.M)
# The sections test_docs_page_shapes.py requires of a tutorial, concept or How-To
# page
SHAPE_SECTIONS = {
    "What You Will Learn",
    "What You Add",
    "What You Get and Why",
    "Key Takeaways",
    "Full Files",
    "Sources",
    "Before You Start",
    "Steps",
    "Check the Result",
    # The four parts of each idea on a concept page
    "In the Tutorial",
    "Why It Matters",
    "How to Use It",
    "What the Result Means",
}
MOVED_ANCHORS = DOCS / "js" / "moved-anchors.js"

_SCRIPT = ROOT / "scripts" / "gen_pages.py"
_spec = importlib.util.spec_from_file_location("gen_pages", _SCRIPT)
assert _spec is not None and _spec.loader is not None
gen_pages = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_pages)

# The Reference pages scripts/gen_pages.py writes into the build
GENERATED = {
    "reference/distributions": gen_pages.distributions_page,
    "reference/statistics": gen_pages.statistics_page,
    "reference/yaml-keys": gen_pages.yaml_keys_page,
    "simpy/compatibility": gen_pages.compatibility_page,
}


def _heading_counts(page):
    headings = HEADING.findall(FENCE.sub("", page.read_text(encoding="utf-8")))
    levels = [level for level, title in headings if title not in SHAPE_SECTIONS]
    return levels.count("##"), len(levels)


def _pages_over_the_limit():
    found = {}
    for page in sorted(DOCS.rglob("*.md")):
        name = str(page.relative_to(ROOT))
        sections, headings = _heading_counts(page)
        if name not in LISTS_ONE_SUBJECT and (
            sections > MAX_SECTIONS or headings > MAX_HEADINGS
        ):
            found[name] = f"{sections} ## headings, {headings} in all"
    return found


def _slug(heading):
    text = re.sub(r"[^\w\s-]", "", heading).strip().lower()
    return re.sub(r"[-\s]+", "-", text)


def _heading_ids(text):
    """The ids the site gives the headings a moved section can land on.

    A section that became a whole page lands on the title. When a section heading
    has the title's text, the title's id is ambiguous, so only the section counts.
    """
    ids, seen, title = set(), {}, None
    for hashes, heading in re.findall(r"^(#{1,6}) (.+)$", FENCE.sub("", text), re.M):
        slug = _slug(heading)
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        if hashes == "#":
            title = slug
        else:
            ids.add(slug if count == 0 else f"{slug}_{count}")
    if title is not None and seen[title] == 1:
        ids.add(title)
    return ids


def _page_text(page):
    if page in GENERATED:
        return GENERATED[page]()
    path = DOCS / page / "index.md"
    if not path.exists():
        path = DOCS / (page + ".md")
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _moved_map():
    text = MOVED_ANCHORS.read_text(encoding="utf-8")
    start = text.index("var MOVED = ") + len("var MOVED = ")
    end = text.index(";\n  function base")
    return json.loads(text[start:end])


def test_no_page_is_over_the_size_limit():
    over = _pages_over_the_limit()
    assert set(over) == set(KNOWN_BREAKING), (
        f"Pages over {MAX_SECTIONS} ## headings or {MAX_HEADINGS} headings in all: "
        f"{over}. Split the page into a folder with an index page."
    )


def test_known_breaking_pages_name_a_reason():
    assert all(reason.strip() for reason in KNOWN_BREAKING.values())


def test_exempt_pages_exist():
    assert {name for name in LISTS_ONE_SUBJECT if not (ROOT / name).exists()} == set()


def test_moved_anchors_point_at_real_headings():
    missing = []
    for old_page, anchors in _moved_map().items():
        if (
            old_page.rstrip("/") not in GENERATED
            and not (DOCS / old_page / "index.md").exists()
            and not (DOCS / (old_page.rstrip("/") + ".md")).exists()
        ):
            missing.append(old_page)
        for anchor, target in anchors.items():
            page, _, fragment = target.partition("#")
            if fragment not in _heading_ids(_page_text(page.rstrip("/"))):
                missing.append(f"{old_page}#{anchor} -> {target}")
    assert missing == [], "; ".join(missing)
