"""Each kind of page has its sections, its links and its code tabs.

The sidebar is at most three levels deep. A tutorial page and a How-To page have the
sections of their kind, and a concept page explains each idea from the shop. Only
the Learn pages, the tutorial and concept pages, say what they teach and end with key
takeaways. Every tutorial page links the concept pages it
rests on, and every concept page links back to the tutorial. All desflow code sits
in YAML, Builder and Low-level tabs, so a reader who follows one way of writing finds
it on every page.

A page the 0.26.0 docs revision has not reached yet is listed in PENDING with the
batch that brings it into line. The test expects it to fail, and a page that passes
while still listed fails the test, so each batch removes its pages from the list.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
MAX_DEPTH = 3

CONCEPT_PAGES = [
    "learn/what-is-des.md",
    "learn/concepts/queueing-basics.md",
    "learn/concepts/a-simulation-study.md",
    "learn/concepts/checking-and-validating.md",
    "learn/concepts/output-analysis.md",
    "learn/concepts/readiness-and-reporting.md",
    "learn/concepts/common-mistakes.md",
]
TUTORIAL_PAGES = sorted(
    p.relative_to(DOCS).as_posix()
    for p in (DOCS / "learn").glob("*/*.md")
    if p.parent.name != "concepts"
)
HOW_TO_PAGES = sorted(
    p.relative_to(DOCS).as_posix()
    for p in (DOCS / "how-to").glob("*/*.md")
    if p.parent.name != "examples"
)
# The Runnable Examples, and those whose example reads an ingress while it runs.
RUNNABLE_PAGES = sorted(
    p.relative_to(DOCS).as_posix()
    for p in (DOCS / "how-to" / "examples").glob("*.md")
    if p.name != "index.md"
)
LIVE_PAGES = {
    "how-to/examples/kafka.md",
    "how-to/examples/redis.md",
    "how-to/examples/postgres.md",
    "how-to/examples/advanced-postgres-orders.md",
}

TUTORIAL_SECTIONS = [
    "What You Will Learn",
    "What You Add",
    "What You Get and Why",
    "Key Takeaways",
    "Full Files",
]
CONCEPT_PAGE_SECTIONS = ["What You Will Learn", "Key Takeaways", "Sources"]
# The sections only a Learn page has.
LEARN_SECTIONS = {"What You Will Learn", "Key Takeaways"}
# Each other section of a concept page is one idea, explained in these parts.
CONCEPT_IDEA_PARTS = [
    "In the Tutorial",
    "Why It Matters",
    "How to Use It",
    "What the Result Means",
]
# What Is DES? is a general introduction, so it has no Learn sections.
INTRO_PAGE = "learn/what-is-des.md"
# Every How-To page, the Runnable Examples included, in this order; it then ends with
# Full Files, or with Example where a published example has its Quick Start, and may
# add Sources last.
HOW_TO_SECTIONS = ["Before You Start", "Steps", "Check the Result"]
HOW_TO_ENDINGS = (
    ["Full Files"],
    ["Example"],
    ["Full Files", "Sources"],
    ["Example", "Sources"],
)
# Every User Guide page except the section's index and How desflow Works, which is the
# overview of the whole section.
USER_GUIDE_PAGES = sorted(
    p.relative_to(DOCS).as_posix()
    for p in (DOCS / "user-guide").rglob("*.md")
    if p.relative_to(DOCS).as_posix()
    not in ("user-guide/index.md", "user-guide/how-desflow-works.md")
)
USER_GUIDE_SECTIONS = ["What It Is", "How to Use It", "How It Works"]

# Pages about one way of writing, pages about SimPy code, and lists.
NOT_TABBED = {
    "user-guide/writing-a-model/blueprints.md",
    "user-guide/writing-a-model/builder-api.md",
    "user-guide/writing-a-model/low-level-api.md",
    "how-to/tools/yaml-advanced.md",
    "how-to/tools/set-up-your-editor.md",
    "simpy/index.md",
    "simpy/coming-from-simpy.md",
    "reference/api.md",
}
NOT_TABBED_FOLDERS = ("reference/", "about/")
TABS = ["YAML", "Builder", "Low-level"]

# Page: the batch of the 0.26.0 docs revision that brings it into line.
PENDING: dict[str, dict[str, str]] = {
    "sections": {},
    "concept_parts": {},
    "tutorial_links": {},
    "concept_links": {},
    "tabs": {},
}

_FENCE = re.compile(r'^( *)```(\w*)(?:.*?title="([^"]+)")?')
_TAB = re.compile(r'^( *)=== "([^"]+)"')
# A Python block that names one of these builds or runs a model in one form. Code that
# every form uses unchanged, such as a connector class, is shown once above the tabs.
_FORM_NAMES = re.compile(
    r"\b(SimulationContext|DynamicRealtimeEnvironment|SimParameter|from_yaml|"
    r"load_context|app|context|env)\b"
)
_BLUEPRINT_KEY = re.compile(
    r"^\s*(simulation|tasks|resources|arrivals|services|processes|stores):",
    re.M,
)


def _params(check, pages):
    return [
        pytest.param(
            page,
            id=page,
            marks=pytest.mark.xfail(
                strict=True, reason=f"Batch {PENDING[check][page]} of 0.26.0"
            ),
        )
        if page in PENDING[check]
        else pytest.param(page, id=page)
        for page in pages
    ]


def _text(page):
    return (DOCS / page).read_text(encoding="utf-8")


def _headings(text, level):
    """The headings of one level, outside code blocks."""
    text = re.sub(r"^ *```.*?^ *```", "", text, flags=re.S | re.M)
    return re.findall(rf"^{'#' * level} (.+?)\s*$", text, re.M)


def _links_to(page, targets):
    """The pages in targets that page links to."""
    found = set()
    for link in re.findall(r"\]\(([^)#\s]+)", _text(page)):
        resolved = (DOCS / page).parent.joinpath(link).resolve()
        try:
            name = resolved.relative_to(DOCS).as_posix()
        except ValueError:
            continue
        if name in targets:
            found.add(name)
    return found


def _depth(items, level=1):
    deepest = level
    for item in items:
        value = next(iter(item.values())) if isinstance(item, dict) else item
        if isinstance(value, list):
            # An index page opens its group and adds no level.
            deepest = max(deepest, _depth(value, level + 1))
    return deepest


def test_sidebar_is_at_most_three_levels_deep(mkdocs_nav):
    assert _depth(mkdocs_nav) <= MAX_DEPTH


@pytest.mark.parametrize("page", _params("sections", TUTORIAL_PAGES))
def test_tutorial_page_has_its_sections(page):
    missing = [s for s in TUTORIAL_SECTIONS if s not in _headings(_text(page), 2)]
    assert missing == []


@pytest.mark.parametrize("page", _params("sections", CONCEPT_PAGES))
def test_concept_page_has_its_sections(page):
    required = ["Sources"] if page == INTRO_PAGE else CONCEPT_PAGE_SECTIONS
    missing = [s for s in required if s not in _headings(_text(page), 2)]
    assert missing == []


@pytest.mark.parametrize("page", _params("concept_parts", CONCEPT_PAGES))
def test_concept_page_explains_each_idea_from_the_shop(page):
    text = _text(page)
    incomplete = []
    for section in re.split(r"^## ", text, flags=re.M)[1:]:
        title = section.split("\n", 1)[0].strip()
        if title in CONCEPT_PAGE_SECTIONS:
            continue
        parts = _headings("\n" + section, 3)
        missing = [p for p in CONCEPT_IDEA_PARTS if p not in parts]
        if missing:
            incomplete.append(f"{title}: {missing}")
    assert incomplete == []


def test_only_learn_pages_have_what_you_will_learn_and_takeaways():
    learn = (set(TUTORIAL_PAGES) | set(CONCEPT_PAGES)) - {INTRO_PAGE}
    others = sorted(
        p.relative_to(DOCS).as_posix()
        for p in DOCS.rglob("*.md")
        if p.relative_to(DOCS).as_posix() not in learn
    )
    found = [p for p in others if LEARN_SECTIONS & set(_headings(_text(p), 2))]
    assert found == []


@pytest.mark.parametrize("page", _params("sections", USER_GUIDE_PAGES))
def test_user_guide_page_has_its_sections(page):
    missing = [s for s in USER_GUIDE_SECTIONS if s not in _headings(_text(page), 2)]
    assert missing == []


@pytest.mark.parametrize("page", _params("sections", HOW_TO_PAGES + RUNNABLE_PAGES))
def test_how_to_page_has_its_sections(page):
    sections = _headings(_text(page), 2)
    assert sections[:3] == HOW_TO_SECTIONS
    assert tuple(sections[3:]) in {tuple(e) for e in HOW_TO_ENDINGS}
    if page in LIVE_PAGES:
        assert "Change a Parameter Live" in _headings(_text(page), 3)


@pytest.mark.parametrize("page", _params("tutorial_links", TUTORIAL_PAGES))
def test_tutorial_page_links_a_concept_page(page):
    assert _links_to(page, set(CONCEPT_PAGES)), "link the concept pages it rests on"


@pytest.mark.parametrize("page", _params("concept_links", CONCEPT_PAGES))
def test_concept_page_links_the_tutorial_both_ways(page):
    assert _links_to(page, set(TUTORIAL_PAGES)), "explain from the tutorial's pages"
    linked_from = [t for t in TUTORIAL_PAGES if page in _links_to(t, {page})]
    assert linked_from, "no tutorial page links this concept page"


# Every hand-written page except Home, which every page reaches through the sidebar.
LINKED_PAGES = sorted(
    p.relative_to(DOCS).as_posix()
    for p in DOCS.rglob("*.md")
    if p.relative_to(DOCS).parts[0] != "snippets" and p != DOCS / "index.md"
)


def test_every_page_has_a_link_to_it():
    """A page found only through the sidebar is cut off from the pages around it."""
    pages = set(LINKED_PAGES) | {"index.md"}
    linked = set()
    for page in pages:
        linked |= _links_to(page, pages - {page})
    assert sorted(set(LINKED_PAGES) - linked) == []


def _figure_rows():
    """Each row of Busy Hour Figures: the figure and the pages that quote it."""
    text = _text("learn/first-model/first-simulation.md")
    table = text[text.index("## Busy Hour Figures") :].split("\n---", 1)[0]
    rows = []
    for line in table.splitlines():
        if line.startswith("| ") and not line.startswith("| Mean wait"):
            figure, pages = line.strip("|").split("|")[:2]
            rows.append((figure.split()[0], re.findall(r"\]\(([^)#]+)", pages)))
    return rows


def test_every_busy_hour_figure_is_on_the_pages_listed():
    missing = []
    for figure, pages in _figure_rows():
        for link in pages:
            page = (
                (DOCS / "learn" / "first-model" / link)
                .resolve()
                .relative_to(DOCS)
                .as_posix()
            )
            if figure not in _text(page):
                missing.append(f"{figure} on {page}")
    assert missing == []


def _desflow_names(source):
    """The names a Python file imports from desflow, and the objects that hold them."""
    names = {"desflow", "app", "context", "env"}
    for group in re.findall(
        r"from desflow[\w.]* import \(?([^)]*?)\)?$", source, re.M | re.S
    ):
        names.update(n.strip() for n in group.split(",") if n.strip())
    return names


def _is_desflow_code(language, title, body):
    """A Python block that builds or runs a model, or a YAML block with a blueprint key.

    A labelled block is judged by what it shows, so a section of plain Python that
    reads the files a run wrote is not desflow code, and neither is a connector class
    that every form uses unchanged.
    """
    if language in {"python", "py", "python3"}:
        if title is None:
            return "desflow" in body
        path = ROOT / title.split(":")[0]
        if not path.is_file():
            return False
        names = _desflow_names(path.read_text(encoding="utf-8"))
        uses_desflow = any(re.search(rf"\b{re.escape(n)}\b", body) for n in names)
        return uses_desflow and bool(_FORM_NAMES.search(body))
    if language in {"yaml", "yml"}:
        if title is not None:
            path = ROOT / title.split(":")[0]
            body = path.read_text(encoding="utf-8") if path.is_file() else body
        return bool(_BLUEPRINT_KEY.search(body))
    return False


def _untabbed_desflow_code(text):
    """Line numbers of desflow code blocks outside a YAML, Builder, Low-level set."""
    lines = text.split("\n")
    found, open_tabs, i = [], [], 0  # open_tabs: (indent, title)
    while i < len(lines):
        line = lines[i]
        indent = len(line) - len(line.lstrip(" "))
        tab = _TAB.match(line)
        fence = _FENCE.match(line)
        if tab:
            open_tabs = [t for t in open_tabs if t[0] < indent]
            open_tabs.append((indent, tab.group(2)))
        elif fence:
            start, close = i, " " * indent + "```"
            i += 1
            while i < len(lines) and not lines[i].startswith(close):
                i += 1
            body = "\n".join(lines[start + 1 : i])
            open_tabs = [t for t in open_tabs if t[0] < indent]
            in_tabs = any(title in TABS for _, title in open_tabs)
            if not in_tabs and _is_desflow_code(fence.group(2), fence.group(3), body):
                found.append(start + 1)
        elif line.strip():
            open_tabs = [t for t in open_tabs if t[0] < indent]
        i += 1
    return found


TABBED_PAGES = sorted(
    p.relative_to(DOCS).as_posix()
    for p in DOCS.rglob("*.md")
    if not p.relative_to(DOCS)
    .as_posix()
    .startswith(("snippets/",) + NOT_TABBED_FOLDERS)
    and p.relative_to(DOCS).as_posix() not in NOT_TABBED
)


@pytest.mark.parametrize("page", _params("tabs", TABBED_PAGES))
def test_desflow_code_is_in_tabs(page):
    assert _untabbed_desflow_code(_text(page)) == [], (
        "put desflow code in YAML, Builder and Low-level tabs; a tab that cannot "
        "show it says so in one line and links the form that can"
    )
