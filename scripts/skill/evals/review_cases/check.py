"""Prepares the review cases for a reviewer, and checks the reviewers' lines against them.

Usage:

    python check.py --prepare OUT --skill DIR
    python check.py --reviews A B

Each folder here is one run as `grade.py --records-out` writes it for the reviewer, with its
transcript kept gzipped as `trace.jsonl.gz` and no `skill/` folder, and a `counts.json` that
the reviewer never reads: `known` (whether the right counts are known), `counts` (the right
count of each reviewer kind named, 0 or more) and `reason`. A run of the plan's pilot has
`known: false`: its right counts are what two reviewers agree on.

`--prepare` copies every case into `OUT/<case>/` as the reviewer reads it: the transcript
unpacked to `trace.jsonl`, `counts.json` left out, and `skill/` copied from the skill folder
`DIR` (`SKILL.md` and `references/*.md`).

`--reviews` takes two folders, one per reviewer, each with `<case>.jsonl`, the reviewer's
reply for the case. For each case it checks that the reply has one `REVIEWED` line; for a case
with known counts, that each reviewer finds a kind in the case's `counts` exactly when its
right count is not 0 (a kind counts once per run); and for a case without them, that the two
reviewers find the same kinds. It prints each difference and exits 1 when there is one.
"""

import argparse
import gzip
import json
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

HERE = Path(__file__).resolve().parent
REVIEW_KINDS = ["K3", "K5", "K8", "K9", "K10", "K14", "K15", "K17"]


def _cases() -> Dict[str, Path]:
    """Returns every case folder by its name."""
    return {p.parent.name: p.parent for p in sorted(HERE.glob("*/counts.json"))}


def prepare(out: Path, skill: Path) -> None:
    """Writes every case into `out` as the reviewer reads it.

    Args:
        out: The folder to write; a case already in it is replaced.
        skill: The skill folder to copy into each case's `skill/`.
    """
    for name, folder in _cases().items():
        target = out / name
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(
            folder, target, ignore=shutil.ignore_patterns("counts.json", "*.gz")
        )
        with gzip.open(folder / "trace.jsonl.gz", "rt", encoding="utf-8") as f:
            (target / "trace.jsonl").write_text(f.read(), "utf-8")
        (target / "skill" / "references").mkdir(parents=True)
        shutil.copy(skill / "SKILL.md", target / "skill" / "SKILL.md")
        for page in sorted((skill / "references").glob("*.md")):
            shutil.copy(page, target / "skill" / "references" / page.name)


def _found(path: Path) -> tuple:
    """Returns the kinds a reply finds and its problems."""
    if not path.is_file():
        return set(), [f"{path.name} is missing"]
    kinds: Set[str] = set()
    reviewed = 0
    problems = []
    for number, line in enumerate(path.read_text("utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            problems.append(f"line {number} is not JSON")
            continue
        if row.get("kind") == "REVIEWED":
            reviewed += 1
        elif row.get("kind") in REVIEW_KINDS:
            kinds.add(row["kind"])
        else:
            problems.append(f"line {number}: {row.get('kind')} is not a reviewer kind")
    if reviewed != 1:
        problems.append(f"{reviewed} REVIEWED lines, wanted 1")
    return kinds, problems


def check(reviews: List[Path]) -> List[str]:
    """Compares the reviewers' replies with the cases; returns each difference.

    Args:
        reviews: The two folders of replies, one per reviewer.

    Returns:
        One text per difference.
    """
    found = []
    for name, folder in _cases().items():
        spec = json.loads((folder / "counts.json").read_text("utf-8"))
        seen = []
        for reviewer in reviews:
            kinds, problems = _found(reviewer / f"{name}.jsonl")
            found += [f"{reviewer.name}/{name}: {p}" for p in problems]
            seen.append(kinds)
            if spec["known"]:
                for kind, count in spec["counts"].items():
                    if (kind in kinds) != bool(count):
                        found.append(
                            f"{reviewer.name}/{name}: {kind} "
                            f"{'found' if kind in kinds else 'not found'}, "
                            f"right count {count}"
                        )
        if not spec["known"] and len(seen) == 2 and seen[0] != seen[1]:
            found.append(
                f"{name}: {reviews[0].name} finds {sorted(seen[0])}, "
                f"{reviews[1].name} finds {sorted(seen[1])}"
            )
    return found


def main(argv: Optional[List[str]] = None) -> int:
    """Prepares the cases or checks the replies.

    Args:
        argv: The command line without the program name; None reads `sys.argv`.

    Returns:
        0, or 1 when a reply differs from a case or the two reviewers differ.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--prepare", type=Path)
    parser.add_argument("--skill", type=Path)
    parser.add_argument("--reviews", type=Path, nargs=2)
    args = parser.parse_args(argv)
    if args.prepare:
        if not args.skill:
            parser.error("--prepare needs --skill")
        prepare(args.prepare, args.skill)
        return 0
    if not args.reviews:
        parser.error("give --prepare or --reviews")
    differences = check(args.reviews)
    for text in differences:
        print(text)
    return 1 if differences else 0


if __name__ == "__main__":
    sys.exit(main())
