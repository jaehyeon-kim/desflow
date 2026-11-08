"""The Known Cases pages, from the case folders of `tests/known_cases/`.

`tests/known_cases/index.yaml` has one entry for each known case: its id, its topic and
its title, and either a folder of its own, a `runs_in` that names the case whose model it
shares, or a `reason` for a case that is left out. The catalogue has an index page and one page for
each of the 14 topics, in the order of `TOPICS`, with one entry for each case of the topic. A case
with a folder shows its three forms in the tabs YAML, Builder and Low-level, from the
files of the folder. A case of a registered or gallery example holds only `original.py`
and shows the three files of that example. A case with no folder shows its title and
the case it shares, or its reason.

Each entry is a heading, so the site's search holds one entry for each case: Material for
MkDocs splits a page at its headings and indexes the heading as the entry's title, which
the search weighs 1000 times the text below it.
"""

import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml

ROOT = Path(__file__).resolve().parents[2]
CASES = ROOT / "tests" / "known_cases"
EXAMPLES = ROOT / "examples"

# The 14 topics, in the order of the page. tests/known_cases/harness.py holds the same
# list, and a test in tests/docs/test_gen_pages.py checks that the two agree.
TOPICS = [
    "Environment and Time",
    "Events and Signals",
    "Processes and Tasks",
    "Conditions and Waits",
    "Interrupts and Failures",
    "Resources",
    "Containers",
    "Stores",
    "Real Time",
    "Generated Values",
    "Related Records",
    "Arrivals and Sources",
    "Statistics and Monitoring",
    "Sinks and Connectors",
]

# The tabs of every code set on the site, in this order, with the language of each
# form's file
FORMS = (
    ("YAML", "model.yaml", "yaml"),
    ("Builder", "builder.py", "python"),
    ("Low-level", "low_level.py", "python"),
)

# The pages are under this folder of the docs: an index page and one page for each topic
FOLDER = "reference/known-cases"


@dataclass(frozen=True)
class Case:
    """One row of `tests/known_cases/index.yaml`.

    Attributes:
        id: The case's id, such as `ST-C06`.
        topic: One of `TOPICS`.
        title: The case's title.
        runs_in: The id of the case whose model this row shares, or None.
        reason: Why the row is left out, or None.
    """

    id: str
    topic: str
    title: str
    runs_in: Optional[str] = None
    reason: Optional[str] = None

    @property
    def anchor(self) -> str:
        """The id of the case's heading, such as `st-c06`."""
        return self.id.lower()

    @property
    def folder(self) -> str:
        """The name of the case's folder: the id in lower case, with `-` as `_`."""
        return self.id.lower().replace("-", "_")


def _cases() -> List[Case]:
    """Every row of `tests/known_cases/index.yaml`, in the order of the file.

    Returns:
        List[Case]: One entry for each row.

    Raises:
        ValueError: A row names a topic that is not one of `TOPICS`.
    """
    rows = yaml.safe_load((CASES / "index.yaml").read_text(encoding="utf-8"))
    found = [Case(**row) for row in rows]
    for case in found:
        if case.topic not in TOPICS:
            raise ValueError(f"{case.id}: {case.topic!r} is not one of the 14 topics")
    return found


def topic_slug(topic: str) -> str:
    """The file name of a topic's page, such as `environment-and-time`.

    Args:
        topic: One of `TOPICS`.

    Returns:
        str: The topic in lower case, with `-` between the words.
    """
    return topic.lower().replace(" ", "-")


def form_files(case: Case) -> Dict[str, Path]:
    """The three files that show a case, by the name of the tab.

    A case of a registered or gallery example holds only `original.py`, whose `EXAMPLE`
    names the example. Its forms are the example's files, the gallery's when the gallery
    has the example and SimPy's otherwise, as `form_paths` of
    `tests/known_cases/harness.py` finds them.

    Args:
        case: A row with a folder.

    Returns:
        Dict[str, Path]: The file of the tab YAML, Builder and Low-level.

    Raises:
        ValueError: The folder has neither the three files nor an `EXAMPLE`.
    """
    folder = CASES / case.folder
    if (folder / "model.yaml").exists():
        return {tab: folder / name for tab, name, _ in FORMS}
    example = _example_name(folder / "original.py")
    if example is None:
        raise ValueError(f"{case.id}: no model.yaml and no EXAMPLE in original.py")
    gallery = EXAMPLES / "gallery"
    if (gallery / f"{example}.yaml").exists():
        return {
            "YAML": gallery / f"{example}.yaml",
            "Builder": gallery / f"{example}_builder.py",
            "Low-level": gallery / f"{example}_low_level.py",
        }
    return {
        "YAML": EXAMPLES / "yaml" / "simpy" / f"{example}.yaml",
        "Builder": EXAMPLES / "builder" / "simpy" / f"{example}_example.py",
        "Low-level": EXAMPLES / "low-level" / "simpy" / f"{example}_example.py",
    }


def _example_name(path: Path) -> Optional[str]:
    """The value of `EXAMPLE` in a Python file, read without running the file."""
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "EXAMPLE" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    return None


def _fenced(text: str, language: str, title: str) -> List[str]:
    """A fenced block of the file's text, with a fence longer than any run of backticks
    in the text, as lines indented for a tab."""
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    fence = "`" * max(3, longest + 1)
    lines = [
        f'{fence}{language} title="{title}"',
        *text.rstrip("\n").split("\n"),
        fence,
    ]
    return [f"    {line}" if line else "" for line in lines]


def _link(case: Case, topic: str) -> str:
    """The link to a case from the page of `topic`: an anchor on the same page, or the
    page of the case's own topic."""
    if case.topic == topic:
        return f"#{case.anchor}"
    return f"{topic_slug(case.topic)}.md#{case.anchor}"


def _entry(case: Case, by_id: Dict[str, Case], topic: str) -> List[str]:
    """The heading of a case and what follows it, as lines of Markdown.

    Args:
        case: The row.
        by_id: Every row, by id.
        topic: The topic of the page that holds the entry, for a link to a case on the
            same page.

    Returns:
        List[str]: The entry, ending with a blank line.
    """
    lines = [f"### {case.id}: {case.title} {{#{case.anchor}}}", ""]
    if case.reason is not None:
        lines += [f"Left out of the tests: {case.reason}.", ""]
    elif case.runs_in is not None:
        other = by_id[case.runs_in]
        link = _link(other, topic)
        lines += [
            f"The model of [{other.id}: {other.title}]({link}) runs this case.",
            "",
        ]
    else:
        files = form_files(case)
        for tab, _, language in FORMS:
            path = files[tab]
            lines += [f'=== "{tab}"', ""]
            lines += _fenced(
                path.read_text(encoding="utf-8"),
                language,
                path.relative_to(ROOT).as_posix(),
            )
            lines.append("")
    return lines


def _counts(found: List[Case]) -> str:
    """The sentence that counts the rows by how the page shows them."""
    own = sum(1 for c in found if c.runs_in is None and c.reason is None)
    shared = sum(1 for c in found if c.runs_in is not None)
    left = sum(1 for c in found if c.reason is not None)
    return (
        f"The list holds {len(found)} cases. {own} have a model of their own, "
        f"{shared} run on the model of another case, and {left} are left out, each "
        "with its reason."
    )


def _introduction(found: List[Case]) -> List[str]:
    """The lines that open the catalogue's first page."""
    return [
        "# Known Cases",
        "",
        "A known case is a behaviour that desflow must reproduce: a SimPy program, a "
        "project's own generator or a set of stated checks, written again as a desflow "
        "model. Its test runs the model as a YAML blueprint, with the Builder API and with "
        "the low-level API, and compares the records of each with the original.",
        "",
        "Each topic page lists its cases. A case with a model of its own shows it as a "
        "YAML blueprint, with the Builder API and with the low-level API, from the files "
        "its test runs. Search for a key, such as `seize` or `patience`, to find the "
        "cases that use it.",
        "",
        _counts(found),
        "",
    ]


def known_cases_files() -> Dict[str, str]:
    """The Known Cases pages: an index page and one page for each topic, under
    `reference/known-cases/`.

    Returns:
        Dict[str, str]: The text of each page, by its path in the docs.
    """
    found = _cases()
    by_id = {c.id: c for c in found}
    index = _introduction(found)
    index += ["The cases are on one page for each topic:", ""]
    pages: Dict[str, str] = {}
    for topic in TOPICS:
        mine = [c for c in found if c.topic == topic]
        index.append(f"- [{topic}]({topic_slug(topic)}.md): {len(mine)} cases")
        lines = [f"# Known Cases: {topic}", ""]
        for case in mine:
            lines += _entry(case, by_id, topic)
        pages[f"{FOLDER}/{topic_slug(topic)}.md"] = "\n".join(lines).rstrip("\n") + "\n"
    return {f"{FOLDER}/index.md": "\n".join(index) + "\n", **pages}
