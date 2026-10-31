"""The SimPy Compatibility page, from `tests/conformance/register.yaml` and
`tests/conformance/simpy_constructs.yaml`."""

import tomllib
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[2]
REGISTER = ROOT / "tests" / "conformance" / "register.yaml"
CONSTRUCTS = ROOT / "tests" / "conformance" / "simpy_constructs.yaml"
LOCK = ROOT / "uv.lock"
SOURCE_URL = "https://github.com/jaehyeon-kim/desflow/blob/main/"
ISSUE_URL = "https://github.com/jaehyeon-kim/desflow/issues/105"
# The forms on Call Python from YAML, by the `form` of a gap
PYTHON_PAGE = "../how-to/tools/yaml-advanced.md"
PYTHON_FORMS: Dict[str, str] = {
    "computed": "write-a-process-for-simpy-behaviour",
    "held": "hold-a-resource-across-steps",
    "arrivals": "start-a-fixed-number-of-arrivals",
    "sub-process": "pass-arguments-to-a-sub-process-and-read-its-value",
}


def register() -> List[Dict[str, Any]]:
    """The entries of the compatibility register, in file order."""
    import yaml

    return yaml.safe_load(REGISTER.read_text(encoding="utf-8"))


def constructs() -> Dict[str, Any]:
    """The SimPy constructs and the gaps, as `simpy_constructs.yaml` holds them."""
    import yaml

    return yaml.safe_load(CONSTRUCTS.read_text(encoding="utf-8"))


def areas() -> List[str]:
    """The areas of the page, in SimPy's order."""
    return list(dict.fromkeys(c["area"] for c in constructs()["constructs"]))


def _anchor(area: str) -> str:
    """The id MkDocs gives an area's heading: its text in lower case, joined by
    hyphens."""
    return "-".join(area.lower().split())


def _cell(text: str) -> str:
    """Text for a table cell: a bar in it would start a new cell."""
    return text.replace("|", "\\|")


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


def _gap_lines(ids: List[str], gaps: Dict[str, Any]) -> List[str]:
    """One line for each gap: what differs, and how to get SimPy's behaviour."""
    lines = []
    for gap in ids:
        info = gaps[gap]
        line = f"**Gap {gap}:** {info['text']}"
        if info.get("fixed"):
            line += f" Planned for {info['fixed']}."
        if info.get("key"):
            line += f" Set {info['key']} for SimPy's behaviour."
        if info.get("form"):
            link = f"{PYTHON_PAGE}#{PYTHON_FORMS[info['form']]}"
            line += f" [Write it as a SimPy process]({link})."
        lines.append(line)
    return lines


def status(
    construct: Dict[str, Any], entry: Dict[str, Any], gaps: Dict[str, Any]
) -> str:
    """The kind of a construct: `gap`, `difference` or `native`."""
    if construct.get("gaps"):
        return "gap"
    return "difference" if entry.get("status") == "difference" else "native"


def _status_cell(
    construct: Dict[str, Any], entry: Dict[str, Any], gaps: Dict[str, Any]
) -> str:
    kind = status(construct, entry, gaps)
    parts: List[str] = []
    if kind == "gap":
        parts += _gap_lines(construct["gaps"], gaps)
    elif kind == "difference":
        parts.append(f"**Difference kept by design:** {entry['differences']}")
    else:
        parts.append("**Native**")
    if entry.get("checks"):
        checks = ", ".join(_check_link(check) for check in entry["checks"])
        parts.append(f"Checks: {checks}.")
    if entry.get("page"):
        parts.append(f"Taught on [{entry['operation']}](../{entry['page']}).")
    return _cell("<br>".join(parts))


def compatibility_page() -> str:
    """The SimPy Compatibility reference page, as Markdown."""
    data = constructs()
    gaps = data["gaps"]
    by_id = {i: e for e in register() for i in e["inventory"]}
    rows = data["constructs"]
    kinds = [status(c, by_id[c["id"]], gaps) for c in rows]
    version = simpy_version()
    lines: List[str] = [
        "# SimPy Compatibility",
        "",
        f"Every modelling feature of SimPy {version}, {len(rows)} in all, grouped by "
        "the part of SimPy it belongs to, with its form in YAML, the builder and the "
        "low-level API, and its status. The page is generated from "
        f"[`tests/conformance/register.yaml`]({SOURCE_URL}tests/conformance/register.yaml) "
        "and "
        f"[`tests/conformance/simpy_constructs.yaml`]({SOURCE_URL}tests/conformance/simpy_constructs.yaml), "
        "so a feature is native here only with its checks.",
        "",
        f"**{kinds.count('native')}** features are native. Each is checked by "
        f"conformance tests that run a plain SimPy {version} model and the same model "
        "in YAML, the builder or the low-level API, and compare "
        "event times, outcomes and balances.",
        "",
        f"**{kinds.count('gap')}** features have a gap: a key, a default or the "
        "engine gives other times, order or values than SimPy, or there is no key "
        "for the feature. Each gap has an id and is tracked in "
        f"[issue #105]({ISSUE_URL}). Where a SimPy process written under "
        "`processes:` gives SimPy's behaviour, the gap links its form on "
        "[Call Python from YAML](../how-to/tools/yaml-advanced.md). Where a setting "
        "gives it, the gap names the setting. Where a release is planned to remove the "
        "whole gap, it names the release.",
        "",
        f"**{kinds.count('difference')}** "
        + (
            "feature is a difference kept by design, with its reason."
            if kinds.count("difference") == 1
            else "features are differences kept by design, each with its reason."
        ),
        "",
        "The low-level API is SimPy with a few classes added, so most SimPy features "
        "work in it; "
        "[SimPy and desflow](index.md) explains how the two fit together. "
        "The register also holds checks of desflow's own features and of variants of "
        "a SimPy feature, such as breakdowns, schedules, replay and cancelling a put; "
        "this page lists only SimPy's features.",
        "",
        "Four gaps belong to no SimPy feature, because a SimPy program writes them "
        "in plain Python: "
        + "; ".join(
            f"**{gap}**, {info['text']}, which a blueprint writes as `!python`: "
            f"{info['python']}"
            for gap, info in data["unattached"].items()
        )
        + ". [Call Python from YAML]"
        f"({PYTHON_PAGE}#{PYTHON_FORMS['computed']}) shows how to write a process.",
    ]
    for area in areas():
        lines += [
            "",
            f"## {area}",
            "",
            "| SimPy | YAML | Builder | Low-level | Status |",
            "|---|---|---|---|---|",
        ]
        for construct in rows:
            if construct["area"] != area:
                continue
            entry = by_id[construct["id"]]
            lines.append(
                "| "
                + " | ".join(
                    [
                        _cell(construct["simpy"]),
                        _cell(entry["yaml"]),
                        _cell(entry["builder"]),
                        _cell(entry["low_level"]),
                        _status_cell(construct, entry, gaps),
                    ]
                )
                + " |"
            )
    return "\n".join(lines) + "\n"
