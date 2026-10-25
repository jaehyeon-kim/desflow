"""The SimPy Compatibility page, from `tests/conformance/register.yaml`."""

import re
import tomllib
from pathlib import Path
from typing import Any, Dict, List

from .compatibility_groups import GROUP_INTROS, GROUPS

ROOT = Path(__file__).resolve().parents[2]
REGISTER = ROOT / "tests" / "conformance" / "register.yaml"
LOCK = ROOT / "uv.lock"
SOURCE_URL = "https://github.com/jaehyeon-kim/desflow/blob/main/"


def register() -> List[Dict[str, Any]]:
    """The entries of the compatibility register, in file order."""
    import yaml

    return yaml.safe_load(REGISTER.read_text(encoding="utf-8"))


def _anchor(group: str) -> str:
    """The id MkDocs gives a group's heading: its text in lower case, joined by
    hyphens, with punctuation dropped."""
    title = f"{group}: {GROUPS[group]}" if group != "D" else GROUPS[group]
    return "-".join(re.sub(r"[^a-z0-9 ]", "", title.lower()).split())


def _check_link(check: str) -> str:
    path, name = check.split("::")
    return f"[`{name}`]({SOURCE_URL}{path})"


def simpy_version() -> str:
    """The SimPy version in `uv.lock`, the one the conformance tests run against.

    It is named here once, read from the lock, so a Dependabot update of the lock
    changes the page with no edit.
    """
    lock = tomllib.loads(LOCK.read_text(encoding="utf-8"))
    return next(p["version"] for p in lock["package"] if p["name"] == "simpy")


def compatibility_page() -> str:
    """The SimPy Compatibility reference page, as Markdown."""
    entries = register()
    native = [e for e in entries if e.get("status", "native") == "native"]
    kept = [e for e in entries if e.get("status") == "difference"]

    def count(rows: List[Dict[str, Any]]) -> int:
        return sum(len(row["inventory"]) for row in rows)

    version = simpy_version()
    total = count(entries)
    # A difference with no inventory item is named from its register row, so the
    # sentence follows the register; with no such row the clause is left out.
    alone = [
        e["operation"][0].lower() + e["operation"][1:]
        for e in kept
        if not e["inventory"]
    ]
    names = (
        ", ".join(alone[:-1]) + " and " + alone[-1]
        if len(alone) > 1
        else "".join(alone)
    )
    # The register's rows without an inventory item are not SimPy features, so the
    # count above leaves them out and this clause names them.
    one_more = (
        f" and one more, {names}, which is not a SimPy feature of its own"
        if len(alone) == 1
        else f" and {len(alone)} more, {names}, which are not SimPy features of "
        "their own"
        if alone
        else ""
    )
    lines: List[str] = [
        "# SimPy Compatibility",
        "",
        f"Every modelling feature of SimPy {version}, {total} in all, with its status "
        "in YAML, the builder and the low-level API: native and checked, or a "
        "difference kept by design. "
        f"A native feature is checked by conformance tests that run a plain SimPy "
        f"{version} model and the same model in all three APIs, and compare event "
        "times, outcomes and balances. The page is generated from "
        f"[`tests/conformance/register.yaml`]({SOURCE_URL}tests/conformance/register.yaml), "
        "the list the tests read, so a feature is native here only with its checks.",
        "",
        f"**{count(native)}** features are native and checked in all three APIs, "
        f"and **{count(kept)}** are differences kept by design. The last section "
        f"lists those{one_more}.",
        "",
        "The low-level API is SimPy with a few classes added, so most SimPy features "
        "work in it; "
        "[SimPy and desflow]"
        "(../des/simpy-and-desflow.md) explains how the two fit together. Where "
        "desflow differs from SimPy on purpose, the difference is listed under the "
        "table.",
        "",
        "The sections are numbered by kind. **C1 to C7** are SimPy's core "
        "operations, such as events, conditions, interrupts, resources and stores. "
        "**S1 to S3** are support features desflow builds from those operations: "
        "breakdowns, schedules and replay. SimPy has no single call for them, so "
        "their conformance tests compare with the same behaviour written in plain "
        "SimPy.",
    ]
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for entry in native:
        groups.setdefault(entry["id"], []).append(entry)
    lines += [
        "",
        "| Section | Operations native in all three APIs |",
        "|---|---|",
        *(
            f"| [{group}: {GROUPS[group]}](#{_anchor(group)}) | {len(rows)} |"
            for group, rows in groups.items()
        ),
        f"| [{GROUPS['D']}](#{_anchor('D')}) | {len(kept)} differences |",
        "",
        "Under each feature, API details opens to show its SimPy construct, "
        "its form in YAML, the builder and the low-level API, and the conformance "
        "tests that check it.",
    ]
    for group, rows in groups.items():
        lines += ["", f"## {group}: {GROUPS[group]}", "", GROUP_INTROS[group], ""]
        for row in rows:
            checks = ", ".join(_check_link(check) for check in row["checks"])
            lines += [
                f"**{row['operation']}**: {row['description']}",
                "",
                '??? note "API details"',
                "",
                f"    - **SimPy:** {row['simpy']}",
                f"    - **YAML:** {row['yaml']}",
                f"    - **Builder:** {row['builder']}",
                f"    - **Low-level:** {row['low_level']}",
                f"    - **Checks:** {checks}",
            ]
            if row.get("differences"):
                lines.append(f"    - **Difference:** {row['differences']}")
            lines += [
                f"    - **Taught on:** [{row['operation']}](../{row['page']})",
                "",
            ]
    lines += ["", f"## {GROUPS['D']}", ""]
    for row in kept:
        link = (
            f" **Taught on:** [{row['operation']}](../{row['page']})"
            if row.get("page")
            else ""
        )
        lines.append(
            f"- **{row['operation']}** (SimPy: {row['simpy']}): {row['differences']}"
            + link
        )
    return "\n".join(lines) + "\n"
