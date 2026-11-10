"""Writes the folder the reviewer reads: the run's files, transcript, records and facts.

Every text is copied with the replacements that keep the version hidden: the skill copy's
path becomes `<skill>`, a page only one version has becomes `page-N.md`, a suite folder
under the p12e3 scratch folder becomes `<suite>`, and a kept run folder becomes `<run>`.
`skill/` holds only the pages both versions have.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Set

from grade_changes import ChangeRun
from grade_runs import Env, FORMS
from grade_tests import FLAKY, Steady, outcome_of


def tests_facts(run: Steady, changes: List[ChangeRun]) -> dict:
    """Returns `tests.json`: each test's outcome, unchanged and under each change, and its forms.

    A flaky test has `"outcome": "flaky"`, and the outcome of each run in each way in `runs`,
    in place of `unchanged`.
    """
    tests = {}
    for ident, outcome in sorted(run.outcomes.items()):
        under = {}
        for change in changes:
            if change.applied:
                under[change.name] = outcome_of(change.outcomes, ident)
        entry: Dict[str, Any] = (
            {"outcome": FLAKY, "runs": run.runs[ident]}
            if outcome == FLAKY
            else {"unchanged": outcome}
        )
        entry["forms"] = sorted(run.forms.get(ident, set()))
        entry["changes"] = under
        tests[ident] = entry
    listed = []
    for change in changes:
        listed.append(
            {
                "name": change.name,
                "applied": change.applied or None,
                "reason": change.reason,
            }
        )
    return {"tests": tests, "changes": listed}


# The files of a run the reviewer reads. A Python file gets `.txt`, so that the linters of
# the repository skip the review cases, which hold the same folder.
RUN_FILES = {
    "model.yaml": "model.yaml",
    "builder.py": "builder.py.txt",
    "low_level.py": "low_level.py.txt",
    "test_model.py": "test_model.py.txt",
    "reply.md": "reply.md",
}


class Hide(NamedTuple):
    """The texts replaced in what the reviewer reads, so that it does not tell the version."""

    old: str
    new: str


def page_names(skill: Optional[Path]) -> Set[str]:
    """Returns the names of a skill copy's reference pages."""
    if skill is None or not (skill / "references").is_dir():
        return set()
    return {page.name for page in (skill / "references").glob("*.md")}


def hidden_names(skill: Optional[Path], other: Optional[Path]) -> List[Hide]:
    """Returns the replacements: the skill copy's path, and each page only one version has.

    A page only one version has becomes `page-1.md`, `page-2.md` and so on, in the order of
    the names, so that a file citing it does not tell which version the run read.
    """
    hides = []
    if skill is not None:
        for form in {str(skill), str(skill.resolve())}:
            hides.append(Hide(form, "<skill>"))
            if form.startswith("/private/"):
                hides.append(Hide(form[len("/private") :], "<skill>"))
    if other is not None:
        one_version = sorted(page_names(skill) ^ page_names(other))
        for number, name in enumerate(one_version, 1):
            hides.append(Hide(name, f"page-{number}.md"))
    # Why: the longest first, so that a path is replaced before a part of it
    return sorted(hides, key=lambda hide: -len(hide.old))


# Why: each folder a run could name outside its workspace says which version it used: a
# suite folder (`before20`, `noskill`, `fix3/suite-a`) under the p12e3 scratch folder, and
# the kept run folder `claude plugin eval` made (`/private/tmp/e-XXXXXX`). A suite path
# becomes `<suite>` up to its `desflow`, `py`, `evals`, `sealed` or `home` folder; a run folder
# becomes `<run>`.
SCRATCH_ROOTS = [
    (
        re.compile(
            r"(?:/private)?/tmp/claude-\d+/[^/\s\"'\\]+/[^/\s\"'\\]+/scratchpad/p12e3"
            r"(?:/[^/\s\"'\\]+)*?(?=/(?:desflow|py|evals|sealed|home)\b|[\s\"'\\:;,)]|$)"
        ),
        "<suite>",
    ),
    (re.compile(r"(?:/private)?/tmp/e-[A-Za-z0-9_]+"), "<run>"),
]


def hide_text(text: str, hides: List[Hide]) -> str:
    """Applies the replacements to a text: the skill path and page names, then the
    scratch roots."""
    for hide in hides:
        if hide.old.endswith(".md"):
            text = re.sub(rf"(?<![\w-]){re.escape(hide.old)}", hide.new, text)
        else:
            text = text.replace(hide.old, hide.new)
    for pattern, new in SCRATCH_ROOTS:
        text = pattern.sub(new, text)
    return text


def copy_text(source: Path, target: Path, hides: List[Hide]) -> None:
    """Copies a text file with the replacements applied, when it exists."""
    if source.is_file():
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            hide_text(source.read_text("utf-8", "replace"), hides), "utf-8"
        )


class Sources(NamedTuple):
    """Where the reviewer's files come from."""

    root: Path
    task: Path
    case: Path
    original: Path
    skill: Optional[Path]
    other: Optional[Path]


def write_facts(
    env: Env, sources: Sources, hides: List[Hide], facts: dict, out: Path
) -> None:
    """Writes the folder the reviewer reads (`review_rubric.md`, What the reviewer gets)."""
    for name, kept in RUN_FILES.items():
        copy_text(env.run_dir / name, out / "files" / kept, hides)
    copy_text(sources.root / "out" / "trace.jsonl", out / "trace.jsonl", hides)
    (out / "records").mkdir(parents=True, exist_ok=True)
    for form in FORMS:
        result = env.cache.get((form, "plain", None))
        records = result.records if result is not None else []
        lines = [json.dumps(r, sort_keys=True) + "\n" for r in records]
        (out / "records" / f"{form}.jsonl").write_text("".join(lines), "utf-8")
    for name, value in facts.items():
        (out / name).write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", "utf-8"
        )
    copy_text(sources.case / "prompt.md", out / "task" / "prompt.md", hides)
    copy_text(sources.task / "records.yaml", out / "task" / "records.yaml", hides)
    copy_text(sources.original, out / "task" / "original.py.txt", hides)
    if sources.skill is not None:
        copy_text(sources.skill / "SKILL.md", out / "skill" / "SKILL.md", hides)
        # Why: a page only one version has would tell the version, so `skill/` holds the
        # pages both versions have; the agent's tool results in `trace.jsonl` show any
        # other page it read
        shared = page_names(sources.skill)
        if sources.other is not None:
            shared &= page_names(sources.other)
        for name in sorted(shared):
            page = sources.skill / "references" / name
            copy_text(page, out / "skill" / "references" / name, hides)
