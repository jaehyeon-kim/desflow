"""The SimPy Compatibility page, from `tests/conformance/register.yaml` and
`tests/conformance/simpy_constructs.yaml`."""

import tomllib
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parents[2]
REGISTER = ROOT / "tests" / "conformance" / "register.yaml"
CONSTRUCTS = ROOT / "tests" / "conformance" / "simpy_constructs.yaml"
LOCK = ROOT / "uv.lock"
SOURCE_URL = "https://github.com/jaehyeon-kim/desflow/blob/main/"
# The section that explains each feature that is not native
EXCEPTIONS = "Where desflow Differs"
# The forms on Call Python from YAML, by the `form` of a gap
PYTHON_PAGE = "../how-to/tools/yaml-advanced.md"
PYTHON_FORMS: Dict[str, str] = {
    "computed": "write-a-process-for-simpy-behaviour",
    "held": "hold-a-resource-across-steps",
    "arrivals": "start-a-fixed-number-of-arrivals",
    "sub-process": "pass-arguments-to-a-sub-process-and-read-its-value",
    "wait-again": "wait-again-for-the-same-event-after-an-interrupt",
    "whole-flow": "wait-for-a-whole-flow-to-end",
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


def _checks(checks: List[str]) -> str:
    """The checks of a construct: how many tests, and a link to each file that holds
    them, in the order the register first names each file."""
    files = Counter(check.split("::")[0] for check in checks)
    # The file's name without `test_` and `.py`, so the cell does not break inside it
    links = [
        f"[`{Path(path).stem.removeprefix('test_')}`]({SOURCE_URL}{path})"
        + (f" ({count})" if len(files) > 1 else "")
        for path, count in files.items()
    ]
    tests = "1 test" if len(checks) == 1 else f"{len(checks)} tests"
    if len(links) > 1:
        links = [", ".join(links[:-1]) + " and " + links[-1]]
    return f"Checked by {tests} in {links[0]}."


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


def _item(
    construct: Dict[str, Any], entry: Dict[str, Any], gaps: Dict[str, Any]
) -> List[str]:
    """The entry for a SimPy feature: what it does in bold, with the page that explains
    it, then its form in SimPy and in each API as a list, with how desflow differs in a
    detail when the register says, then its checks."""
    kind = status(construct, entry, gaps)
    lead = f"**{construct['description']}**"
    if entry.get("page"):
        lead += f" Explained in [{entry['operation']}](../{entry['page']})."
    see = f"see [{EXCEPTIONS}](#{_anchor(EXCEPTIONS)})"
    if kind == "gap":
        lead += f" Gap {', '.join(construct['gaps'])}: {see}."
    elif kind == "difference":
        lead += f" Difference kept by design: {see}."
    lines = ["", lead, "", f"- SimPy: {construct['simpy']}"]
    lines += [
        f"- {label}: {entry[key]}"
        for label, key in (
            ("YAML", "yaml"),
            ("Builder", "builder"),
            ("Low-level", "low_level"),
        )
    ]
    # A native feature can still differ in a detail that its checks do not compare;
    # a gap or a difference gives its text under Where desflow Differs
    if kind == "native" and entry.get("differences"):
        lines.append(f"- Where desflow differs: {entry['differences']}")
    if entry.get("checks"):
        lines += ["", _checks(entry["checks"])]
    return lines


def compatibility_page() -> str:
    """The SimPy Compatibility reference page, as Markdown."""
    data = constructs()
    gaps = data["gaps"]
    by_id = {i: e for e in register() for i in e["inventory"]}
    rows = data["constructs"]
    kinds = [status(c, by_id[c["id"]], gaps) for c in rows]
    version = simpy_version()
    native, gap, difference = (
        kinds.count("native"),
        kinds.count("gap"),
        kinds.count("difference"),
    )
    lines: List[str] = [
        "# SimPy Compatibility",
        "",
        f"This page lists every modelling feature of SimPy {version}, {len(rows)} in "
        "all, and how to write each one in desflow. The features are grouped by the "
        "part of SimPy they belong to.",
        "",
        "Each feature starts with what it does in SimPy, in bold, and a link to the "
        "page that explains it. Below that are its form in SimPy, in YAML, in the "
        "Builder API and in the low-level API, how desflow differs in a detail where "
        "it does, and the tests that check it.",
        "",
        f"- **Native: {native}.** A conformance test runs a plain SimPy {version} model "
        "and the same model in desflow, and compares the event times, outcomes and "
        "balances.",
        f"- **{'Gap' if gap == 1 else 'Gaps'}: {gap}.** desflow gives other times, order "
        "or values than SimPy, or has no key for the feature. Each gap has an id and "
        "says how to get SimPy's behaviour.",
        f"- **{'Difference' if difference == 1 else 'Differences'} kept by design: "
        f"{difference}.** desflow behaves differently on purpose, for the reason given.",
        "",
        f"[{EXCEPTIONS}](#{_anchor(EXCEPTIONS)}), at the end of the page, explains each "
        "gap and difference.",
    ]
    for area in areas():
        lines += ["", f"## {area}"]
        for construct in rows:
            if construct["area"] == area:
                lines += _item(construct, by_id[construct["id"]], gaps)
    lines += ["", f"## {EXCEPTIONS}"]
    for construct in rows:
        entry = by_id[construct["id"]]
        kind = status(construct, entry, gaps)
        if kind == "native":
            continue
        area = construct["area"]
        lines += [
            "",
            f"**{construct['description']}** In [{area}](#{_anchor(area)}).",
            "",
            f"- SimPy: {construct['simpy']}",
        ]
        if kind == "gap":
            lines += [f"- {line}" for line in _gap_lines(construct["gaps"], gaps)]
        else:
            lines.append(f"- **Difference kept by design:** {entry['differences']}")
    return "\n".join(lines) + "\n"
