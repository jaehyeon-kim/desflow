"""Every example's Quick Start is the same template, filled from the example list.

`quick_start` builds the Quick Start of one example in one way of writing it, from
`desflow.blueprint.examples.EXAMPLES`: the files `desflow examples get` downloads, the
odctl profile, the extras and how the run ends. Every example page, and Getting
Started, must show exactly that text in each tab, so the steps, their names and their
numbers are the same everywhere.
"""

import re
from pathlib import Path

import pytest

from desflow.blueprint.examples import EXAMPLES, Example

ROOT = Path(__file__).resolve().parents[2]
PAGES = {
    "local": "docs/examples/local.md",
    "kafka": "docs/examples/kafka.md",
    "parquet": "docs/examples/parquet.md",
    "iceberg": "docs/examples/iceberg.md",
    "postgres": "docs/examples/postgres.md",
    "redis": "docs/examples/redis.md",
    "backfill-live": "docs/examples/backfill-live.md",
    "related-records": "docs/examples/advanced-postgres-orders.md",
    # The shop examples of step 1 are shown on the User Guide page of their area.
    "stock": "docs/user-guide/stores-and-containers.md",
    "flow": "docs/user-guide/task-flow.md",
    "measuring": "docs/user-guide/statistics.md",
    # The 0.22.0 examples are shown on the page that teaches their feature.
    "patience": "docs/guides/reneging-and-balking.md",
    "filtered-inventory": "docs/user-guide/stores-and-containers.md",
    "process-interaction": "docs/user-guide/tasks-and-processes.md",
    "interruptions": "docs/user-guide/resources.md",
    "competing-resources": "docs/user-guide/resources.md",
    "end-conditions": "docs/user-guide/time-and-pacing.md",
    "breakdowns": "docs/guides/breakdowns.md",
    "changing-demand": "docs/guides/time-varying-arrivals.md",
    "local-replay": "docs/user-guide/arrivals-and-services.md",
    "replications": "docs/user-guide/replications.md",
    "experiment": "docs/user-guide/experiments.md",
    "fitting": "docs/user-guide/input-fitting.md",
}
TABS = {"yaml": "YAML", "builder": "Builder", "low-level": "Low-level"}
ODCTL = '"odctl>=1.0,<2"'


def _package(example: Example) -> str:
    return f'"desflow[{example.extras}]"' if example.extras else "desflow"


def _run(example: Example, api: str, uv: bool) -> str:
    main = Path(example.files[api][0]).name
    if api == "yaml":
        return example.yaml_command or f"desflow run {main}"
    if uv:
        return f"uv run --no-project --with {_package(example)} {main}"
    return f"python {main}"


def quick_start(example: Example, api: str, tabs: bool = True) -> str:
    """The Quick Start of one example in one way of writing it, as Markdown.

    Nothing needs a virtual environment. With uv, `uv tool install` installs the
    `desflow` and `odctl` commands, and `uv run --with` runs a Python file without
    installing anything; with pip, `pip install` does both. The other steps are the
    same command in both ways.
    """
    folder = f"{example.name}/{api}"
    run_title = "Run it (Ctrl + C to stop)" if example.until_stopped else "Run it"

    # One command installs both: --with-executables-from also puts odctl on the path.
    uv_install = [f"uv tool install {_package(example)}"]
    if example.profile:
        uv_install = [
            f"uv tool install {_package(example)} --with-executables-from {ODCTL}"
        ]
    pip_packages = _package(example)
    if example.profile:
        pip_packages += f" {ODCTL}"
    if example.watch:
        pip_packages += f" {example.watch_needs}"

    def steps(uv: bool):
        lines = [
            "# 1. Install",
            *(uv_install if uv else [f"pip install {pip_packages}"]),
        ]
        rest = [
            [
                "Download the example",
                f"desflow examples get {example.name} --api {api}",
                f"cd {folder}",
            ]
        ]
        if example.profile:
            rest.append(["Start its service", f"odctl up {example.profile}"])
        rest.append([run_title, _run(example, api, uv)])
        if example.watch:
            watch = (
                f"uv run --no-project --with {_package(example)} --with {example.watch_needs} ../{example.watch}"
                if uv
                else f"python ../{example.watch}"
            )
            rest.append(
                [
                    "In a second terminal, in the same folder, watch and steer it at http://localhost:8080",
                    watch,
                ]
            )
        if example.profile:
            rest.append(
                [
                    "Stop its service when finished",
                    f"odctl down {example.profile} --volumes",
                ]
            )
        for number, (title, *commands) in enumerate(rest, start=2):
            lines += ["", f"# {number}. {title}", *commands]
        return lines

    def block(lines):
        return "```bash\n" + "\n".join(lines) + "\n```"

    intro = (
        "**Quick Start**\n\n"
        "`desflow examples get` downloads the example at the release you install, into a "
        "folder of its own. Pick uv or pip:\n\n"
    )
    if not tabs:  # the README: GitHub shows no tabs
        return (
            intro
            + "**With uv**\n\n"
            + block(steps(True))
            + "\n\n**With pip**\n\n"
            + block(steps(False))
        )

    def tab(title, lines):
        return f'=== "{title}"\n\n' + "\n".join(
            ("    " + row) if row else "" for row in block(lines).split("\n")
        )

    return intro + tab("uv", steps(True)) + "\n\n" + tab("pip", steps(False))


def _tab(text: str, title: str) -> str:
    """The bodies of every tab with this title, without their indent.

    A User Guide page has other tab sets before its example, so every tab of the
    title is searched.
    """
    bodies = re.findall(
        rf'^=== "{title}"\n(.*?)(?=^=== "|^---$|^#|\Z)', text, re.S | re.M
    )
    assert bodies, f"no {title} tab"
    return "\n".join(
        row[4:] if row.startswith("    ") else row
        for body in bodies
        for row in body.split("\n")
    )


@pytest.mark.parametrize("name", list(PAGES))
def test_example_page_quick_starts_follow_the_template(name):
    text = (ROOT / PAGES[name]).read_text(encoding="utf-8")
    for api, title in TABS.items():
        assert quick_start(EXAMPLES[name], api) in _tab(text, title), (
            f"{name}: {title} tab"
        )


def test_getting_started_uses_the_template():
    text = (ROOT / "docs" / "getting-started.md").read_text(encoding="utf-8")
    for api, title in TABS.items():
        assert quick_start(EXAMPLES["local"], api) in _tab(text, title), title


def test_every_example_has_a_page():
    assert set(PAGES) == set(EXAMPLES)


def test_readme_runs_the_template_commands():
    """The README runs the local example's YAML version with the template's commands.

    The README installs desflow once, in its Installation section, so its example
    block keeps the template's download and run commands and leaves out the install.
    """
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    template = quick_start(EXAMPLES["local"], "yaml", tabs=False)
    pip_block = template[template.index("**With pip**") :]
    commands = [
        line
        for line in pip_block.split("\n")
        if line and not line.startswith(("#", "`", "*", "pip install"))
    ]
    expected = "```bash\n" + "\n".join(commands) + "\n```"
    assert expected in readme[readme.index("## Run an Example") :]
