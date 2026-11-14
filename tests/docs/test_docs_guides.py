"""The guides' scripts run, unpaced, and behave as their pages say.

The pages show these files, and test_docs_match_examples.py keeps the copies equal to
them. A script that needs a broker runs here with a capturing sink in place of its
Kafka egress, so no service is needed.
"""

from dataclasses import replace
import ast
import asyncio
import importlib.util
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from desflow.connectors.egress.base import BaseEgress

ROOT = Path(__file__).resolve().parents[2]
GUIDES = ROOT / "docs" / "snippets" / "guides"


class _Capture(BaseEgress):
    """Keeps every record it receives, in place of a Kafka egress."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _captured(app, until):
    """Runs a builder app unpaced with its one egress replaced by a capture."""
    capture = _Capture()
    app.factor = 0.0
    app._sinks.egress = [
        replace(sink, provider=provider)
        for sink, provider in zip(app._sinks.egress, [capture])
    ]
    app.run(until=until)
    return capture.records


def _load(name):
    return _load_from(GUIDES, name)


def _load_from(folder, name):
    sys.path.insert(0, str(folder))
    try:
        spec = importlib.util.spec_from_file_location(name, folder / f"{name}.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(folder))


def _events(caplog):
    """(key, value) of every event the console egress logged, in order."""
    events = []
    for record in caplog.records:
        # One log call per batch, one line per record
        for line in record.getMessage().split("\n"):
            if line.startswith("[EVT] "):
                data = ast.literal_eval(line[len("[EVT] ") :])
                events.append((data["key"], data["value"]))
    return events


def test_breakdowns_downtime_matches_the_events():
    """The station of the breakdowns example fails five times and is down 38.6 s of
    300; eight orders are interrupted and all eight resume, as the page says."""
    module = _load("breakdowns")
    examples = ROOT / "examples"
    outcomes = [
        module.yaml_form(str(examples / "yaml" / "breakdowns.yaml")),
        module.builder_form(str(examples / "builder" / "breakdowns_example.py")),
        module.low_level_form(str(examples / "low-level" / "breakdowns_example.py")),
    ]
    assert outcomes[0] == outcomes[1] == outcomes[2]
    assert outcomes[0] == {
        "periods": [
            (5.6, 10.9),
            (63.5, 73.0),
            (98.7, 105.1),
            (187.4, 199.3),
            (217.4, 222.9),
        ],
        "downtime": 38.6,
        "share_down": 0.129,
        "interrupted": 8,
        "resumed": 8,
        "order_packed": 169,
    }


def test_dynamic_topology_scenario_gives_the_wrappers_their_units_in_every_form():
    """The wrappers exist from the start with no units and get 2 at 12 s: the packers
    finish at 7, 11, 15 and 19 s, and the wrappers at 15 and 19 s, in the YAML,
    builder and low-level forms."""
    from desflow import SimulationContext

    expected = [
        (7.0, "packers"),
        (11.0, "packers"),
        (15.0, "packers"),
        (15.0, "wrappers"),
        (19.0, "packers"),
        (19.0, "wrappers"),
    ]

    def finished(capture):
        return sorted(
            (r["sim_ts"], r["value"]["resource"])
            for r in capture.records
            if r["stream_type"] == "event" and r["value"].get("status") == "finished"
        )

    yaml = ROOT / "docs" / "snippets" / "yaml" / "dynamic_topology_scenario.yaml"
    app = SimulationContext.from_yaml(yaml)
    capture = _Capture()
    app.add_egress(capture)
    app.run()
    assert finished(capture) == expected
    app = _load("dynamic_topology_scenario_builder").build()
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=20)
    assert finished(capture) == expected
    low_level = _load("dynamic_topology_scenario_low_level")
    capture = _Capture()
    low_level.run([capture])
    assert finished(capture) == expected
    # The low-level step publishes the record the other forms give it: a label at 12 s
    from desflow import DynamicRealtimeEnvironment

    env = DynamicRealtimeEnvironment(factor=0, strict=False)
    env.registry.register_sim_parameter(low_level.PARAMS)
    labels = _Capture()
    env.setup_egress([labels], labels=[True])
    low_level.build(env)
    env.run(until=20)
    env.teardown()
    assert [
        (r["sim_ts"], r["label"]) for r in labels.records if r["stream_type"] == "label"
    ] == [(12.0, {"status": "wrappers_added", "capacity": 2})]


def test_switch_a_task_on_wraps_the_same_orders_in_every_form():
    """No order is wrapped before the step at 1 minute. From then on the weight read
    from the registry sends some orders to wrap_gift, the same 9 of the 48 packed in
    the YAML, builder and low-level forms. The step publishes its record at 1 minute."""
    from desflow import DynamicRealtimeEnvironment, SimulationContext

    expected = [
        (65.0, "orders-12"),
        (70.0, "orders-13"),
        (105.0, "orders-20"),
        (120.0, "orders-23"),
        (160.0, "orders-31"),
        (165.0, "orders-32"),
        (205.0, "orders-40"),
        (245.0, "orders-48"),
        (285.0, "orders-56"),
    ]

    def done(capture, task):
        return [
            (r["sim_ts"], r["key"])
            for r in capture.records
            if r["stream_type"] == "event"
            and r["task"] == task
            and "event_type" in r["value"]
        ]

    yaml = ROOT / "docs" / "snippets" / "yaml" / "switch_a_task_on.yaml"
    app = SimulationContext.from_yaml(yaml)
    yaml_capture = _Capture()
    app.add_egress(yaml_capture)
    app.run()
    app = _load("switch_a_task_on_builder").build()
    builder_capture = _Capture()
    app.add_egress(builder_capture)
    app.run(until=300)
    low_level = _load("switch_a_task_on_low_level")
    low_level_capture = _Capture()
    low_level.run([low_level_capture])
    for capture in (yaml_capture, builder_capture, low_level_capture):
        assert done(capture, "wrap_gift") == expected
        packed = done(capture, "pack_order")
        assert len([time for time, _ in packed if time >= 60]) == 48
    env = DynamicRealtimeEnvironment(factor=0, strict=False)
    env.registry.register_sim_parameter(low_level.PARAMS)
    labels = _Capture()
    env.setup_egress([labels], labels=[True])
    low_level.build(env)
    env.run(until=low_level.UNTIL)
    env.teardown()
    assert [
        (r["sim_ts"], r["label"]) for r in labels.records if r["stream_type"] == "label"
    ] == [(60.0, {"intervention": "gift_wrapping_on"})]


def test_serve_together_batches_agree_in_every_form():
    """Seven orders, 1 s apart: the first batch of 3 closes at 3 s, the second at 6 s
    (its members start then and the oven frees at 7 s), the seventh closes alone at
    9.5 s by its timeout. A `close_when` batch of
    `batch.size >= 2 and batch.age >= 2` closes at 3 s with orders 1 to 3 and at 6 s
    with orders 4 to 6, the oven bakes them at 7 s and 11 s, and the seventh order
    waits for ever. The YAML, builder and low-level forms agree for both batches, and
    the page shows the rows."""
    yaml = ROOT / "docs" / "snippets" / "yaml"
    builder = _load("serve_together")
    low_level = _load("serve_together_low_level")
    rows = builder.batch_records()
    assert rows[:3] == [(3.0, f"orders-{n}", "started") for n in (1, 2, 3)]
    assert (9.5, "orders-7", "started") in rows
    assert rows[-1] == (15.0, "orders-7", "baked")
    assert builder.batch_records(str(yaml / "serve_together.yaml")) == rows
    assert low_level.batch_records() == rows
    early = [
        (time, f"orders-{number}", status)
        for time, numbers, status in (
            (3.0, (1, 2, 3), "started"),
            (6.0, (4, 5, 6), "started"),
            (7.0, (1, 2, 3), "baked"),
            (11.0, (4, 5, 6), "baked"),
        )
        for number in numbers
    ]
    early.sort()
    assert builder.batch_records(str(yaml / "serve_together_close_when.yaml")) == early
    assert builder.batch_records(close_when=True) == early
    assert low_level.batch_records(close_when=True) == early
    page = (ROOT / "docs/how-to/modelling/serve-entities-together.md").read_text(
        "utf-8"
    )
    for shown_rows in (rows, early):
        shown = "\n".join(f"check: {row}" for row in shown_rows)
        assert f"```text\n{shown}\n```" in page


def test_breakdowns_busy_clock_counts_only_the_time_in_use():
    """A station that fails after 5 s of work fails once, 5 s into the order that
    starts at 10 s, with the busy clock; with the calendar clock it fails every 7 s,
    idle or not. The YAML, builder and low-level forms agree."""
    yaml = ROOT / "docs" / "snippets" / "yaml" / "breakdowns_busy.yaml"
    builder = _load("breakdowns_busy")
    low_level = _load("breakdowns_busy_low_level")
    busy = [(15.0, "down"), (17.0, "repaired")]
    calendar = [
        (time, status)
        for start in (5.0, 12.0, 19.0, 26.0)
        for time, status in ((start, "down"), (start + 2, "repaired"))
    ]
    for clock, expected in (("busy", busy), ("calendar", calendar)):
        assert builder.down_periods(clock) == expected
        assert builder.down_periods_yaml(clock, str(yaml)) == expected
        assert low_level.down_periods(clock) == expected
    page = (ROOT / "docs/how-to/modelling/breakdowns.md").read_text("utf-8")
    assert "`[(15.0, 'down'), (17.0, 'repaired')]`" in page


def test_breakdowns_check_runs_from_the_downloaded_files(tmp_path, monkeypatch):
    """The check finds the example where `desflow examples get breakdowns` writes it,
    and prints the lines the page shows."""
    for api, name in (
        ("yaml", "breakdowns.yaml"),
        ("builder", "breakdowns_example.py"),
        ("low-level", "breakdowns_example.py"),
    ):
        (tmp_path / "breakdowns" / api).mkdir(parents=True)
        shutil.copy(ROOT / "examples" / api / name, tmp_path / "breakdowns" / api)
    monkeypatch.chdir(tmp_path)
    module = _load("breakdowns")
    forms = (module.yaml_form, module.builder_form, module.low_level_form)
    printed = "\n".join(str(form()) for form in forms)
    page = (ROOT / "docs/how-to/modelling/breakdowns.md").read_text(encoding="utf-8")
    assert f"```text\n{printed}\n```" in page


def _script_from_page(page, module, script, import_line, tmp_path, import_first=False):
    """Writes `script` from the python blocks of docs/snippets/guides/<module>.py on
    a docs page, in the order the page shows them, with the desflow import line the
    page gives in its text, and returns the page text."""
    text = (ROOT / "docs" / page).read_text(encoding="utf-8")
    assert f"`{import_line}`" in text
    found = re.findall(
        rf'^( *)```python title="docs/snippets/guides/{module}\.py:[\w-]+"[^\n]*\n(.*?)\n\1```$',
        text,
        re.M | re.S,
    )
    parts = [
        "\n".join(line[len(indent) :] for line in code.split("\n"))
        for indent, code in found
    ]
    parts.insert(0 if import_first else 1, import_line)
    (tmp_path / script).write_text("\n\n\n".join(parts) + "\n", encoding="utf-8")
    return text


def _saved_file_from_page(page, name, tmp_path):
    """Writes `name` from the block of docs/snippets/yaml/<name> on a docs page, as a
    reader saves it, so a part the page leaves out fails the run."""
    text = (ROOT / "docs" / page).read_text(encoding="utf-8")
    found = re.findall(
        rf'^( *)```\w+ title="docs/snippets/yaml/{re.escape(name)}[:"][^\n]*\n(.*?)\n\1```$',
        text,
        re.M | re.S,
    )
    assert found, f"{page} shows no block of {name}"
    parts = [
        "\n".join(line[len(indent) :] for line in code.split("\n"))
        for indent, code in found
    ]
    (tmp_path / name).write_text("\n".join(parts) + "\n", encoding="utf-8")


def _printed_on_the_page(text, script):
    """The text block that follows the page's bash block for `script`, and the
    prefix its `grep "^..."` keeps ("" when the command does not filter). The block
    gives the uv command and the pip command, and both must run the script."""
    command, shown = re.search(
        rf"```bash\n((?:(?!```).)*?{re.escape(script)}(?:(?!```).)*?)\n```\n\n```text\n(.*?)\n```",
        text,
        re.S,
    ).groups()
    assert f"uv run --no-project --with desflow {script}" in command
    assert f"pip install desflow\npython {script}" in command
    prefix = re.search(r'grep "\^([^"]*)"', command)
    return (prefix.group(1) if prefix else ""), shown


def _example_files(tmp_path, name, files):
    """Where `desflow examples get <name>` writes the example's forms."""
    for api, file in files:
        (tmp_path / name / api).mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / "examples" / api / file, tmp_path / name / api)


_BREAKDOWNS = (
    "from desflow import DynamicRealtimeEnvironment, Sampler, SimulationContext"
)
_RENEGING = (
    "from desflow import CapacityConfig, DynamicRealtimeEnvironment, "
    "DynamicResource, Sampler, SimParameter, SimulationContext"
)
YAML_SNIPPETS = ROOT / "docs" / "snippets" / "yaml"


@pytest.mark.parametrize(
    "page,module,script,import_line,import_first",
    [
        (
            "how-to/modelling/breakdowns.md",
            "breakdowns",
            "check_breakdowns.py",
            _BREAKDOWNS,
            False,
        ),
        (
            "how-to/modelling/reneging-and-balking.md",
            "reneging_and_balking",
            "check_reneging.py",
            _RENEGING,
            False,
        ),
        (
            "how-to/modelling/wait-for-one-or-all.md",
            "wait_for_one_or_all",
            "wait_for_one_or_all.py",
            "from desflow import Entity, SimulationContext",
            False,
        ),
        (
            "how-to/modelling/wait-for-one-or-all.md",
            "wait_for_one_or_all_low_level",
            "wait_for_one_or_all_low_level.py",
            "from desflow import DynamicRealtimeEnvironment, Signal, SignalFailed",
            True,
        ),
    ],
)
def test_how_to_check_scripts_run_as_the_page_builds_them(
    page, module, script, import_line, import_first, tmp_path
):
    """The script a reader builds from the page's blocks, in the page's order and
    with the page's import line, runs in a folder of the saved files and prints the
    lines the page shows. Importing the snippet file cannot show a function that
    the page leaves out."""
    _example_files(
        tmp_path,
        "breakdowns",
        [
            ("yaml", "breakdowns.yaml"),
            ("builder", "breakdowns_example.py"),
            ("low-level", "breakdowns_example.py"),
        ],
    ) if module == "breakdowns" else None
    if module == "reneging_and_balking":
        _saved_file_from_page(page, "reneging_tie.yaml", tmp_path)
        _example_files(
            tmp_path,
            "patience",
            [
                ("yaml", "patience.yaml"),
                ("builder", "patience_example.py"),
                ("low-level", "patience_example.py"),
            ],
        )
    if module == "wait_for_one_or_all":
        for name in (
            "wait_first.yaml",
            "wait_tie.yaml",
            "wait_all.yaml",
            "wait_suspend.yaml",
            "wait_each.yaml",
            "wait_pool.yaml",
        ):
            _saved_file_from_page(page, name, tmp_path)
    text = _script_from_page(page, module, script, import_line, tmp_path, import_first)
    run = subprocess.run(
        [sys.executable, script], cwd=tmp_path, capture_output=True, text=True
    )
    assert run.returncode == 0, run.stderr[-800:]
    prefix, shown = _printed_on_the_page(text, script)
    printed = [line for line in run.stdout.splitlines() if line.startswith(prefix)]
    assert "\n".join(printed) == shown


def test_time_varying_arrivals_boundaries():
    """The gap drawn at the step's time uses the new value; a step at until never
    applies."""
    from desflow import SimulationContext

    for module in (
        _load("time_varying_arrivals"),
        _load("time_varying_arrivals_low_level"),
    ):
        assert module.arrivals(50) == ([10.0, 20.0, 30.0, 35.0, 40.0, 45.0], 5.0)
        assert module.arrivals(30) == ([10.0, 20.0], 10.0)
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "time_varying_arrivals.yaml"
    for until, times in (
        (50, [10.0, 20.0, 30.0, 35.0, 40.0, 45.0]),
        (30, [10.0, 20.0]),
    ):
        events = _event_rows(_captured(SimulationContext.from_yaml(blueprint), until))
        assert [row[0] for row in events] == times


def test_handoff_holds_the_picker_until_the_bench_is_free(caplog):
    module = _load("multi_resource_handoffs")
    module.app.factor = 0.0
    with caplog.at_level(logging.INFO, logger="desflow"):
        module.app.run(until=60)
    events = _events(caplog)
    assert any(value.get("event_type") == "order_packed" for _, value in events)

    # Each order goes through its steps in this order
    def step(value):
        if "status" in value:
            return f"{value['path_id'].rsplit('.', 1)[1]} {value['status']}"
        return value["event_type"]

    order = [
        "walking queued",
        "walking started",
        "order_picked",
        "packing queued",
        "packing started",
        "order_packed",
    ]
    seen: dict = {}
    for key, value in events:
        seen.setdefault(key, []).append(step(value))
    for key, steps in seen.items():
        assert steps == order[: len(steps)], key

    # One order at a time holds the picker, from its pick's start until its pack
    # starts, and one at a time is on the bench, from its pack's start until it is
    # packed
    with_picker, on_bench = None, None
    for key, value in events:
        current = step(value)
        if current == "walking started":
            assert with_picker is None, f"{key} picked while {with_picker} held it"
            with_picker = key
        elif current == "packing started":
            assert with_picker == key and on_bench is None
            with_picker, on_bench = None, key
        elif current == "order_packed":
            assert on_bench == key
            on_bench = None


def _event_rows(records):
    """(sim_ts, key, task, instance, value) of every event, in order."""
    return [
        (r["sim_ts"], r["key"], r["task"], r["instance"], r["value"])
        for r in records
        if r.get("stream_type") == "event"
    ]


def test_handoff_forms_publish_the_same_events():
    """The YAML, builder and low-level handoffs publish the same events at the same
    times, with seed 42."""
    from desflow import SimulationContext

    blueprint = ROOT / "docs" / "snippets" / "yaml" / "multi_resource_handoffs.yaml"
    yaml_events = _event_rows(_captured(SimulationContext.from_yaml(blueprint), 30))
    builder_events = _event_rows(_captured(_load("multi_resource_handoffs").app, 30))
    capture = _Capture()
    _load("multi_resource_handoffs_low_level").run(30, 0.0, [capture])
    low_level_events = _event_rows(capture.records)
    assert builder_events and any(
        row[4].get("event_type") == "order_packed" for row in builder_events
    )
    assert yaml_events == builder_events == low_level_events


def test_supervisor_adds_the_wrappers_mid_run(caplog):
    module = _load("dynamic_topology")
    with caplog.at_level(logging.INFO, logger="desflow"):
        resources = module.run(until=20, factor=0.0)
    assert set(resources) == {"packers", "wrappers"}
    assert resources["wrappers"].capacity == 2

    events = _events(caplog)
    added = [value for key, value in events if key == "supervisor"]
    assert added == [{"status": "wrappers_added", "capacity": 2}]
    started = [value["resource"] for _, value in events if value["status"] == "started"]
    assert "packers" in started and "wrappers" in started


def test_supervisor_adds_the_wrappers_in_every_form(tmp_path, monkeypatch, caplog):
    """The blueprint and the builder run the same supervisor as a process, and publish
    the events the low-level script publishes."""
    from desflow import SimulationContext

    # A reader copies the page's files into one folder and runs them there
    for source in (
        GUIDES / "dynamic_topology_builder.py",
        ROOT / "docs" / "snippets" / "yaml" / "dynamic_topology.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "dynamic_topology_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    with caplog.at_level(logging.INFO, logger="desflow"):
        _load("dynamic_topology").run(until=20, factor=0.0)
    low_level = _events(caplog)
    yaml_app = SimulationContext.from_yaml(tmp_path / "dynamic_topology.yaml")
    builder_app = _load_from(tmp_path, "dynamic_topology_builder").app
    for app in (yaml_app, builder_app):
        events = [(r[1], r[4]) for r in _event_rows(_captured(app, 20))]
        assert events == low_level
    assert ("supervisor", {"status": "wrappers_added", "capacity": 2}) in low_level


def test_reneging_and_balking_ties_and_balance(tmp_path, monkeypatch):
    """At a tie the patience's event comes first, so the order reneges, in every form:
    one arriving at 3 with a patience of 3 and one arriving at 0 with a patience of 6.
    An order whose patience ends after the grant is served. Every order of the patience
    example is packed, reneged, balked or still in the shop, in every form. The script
    runs from the folder a reader works in: the saved blueprint beside it and the files
    `desflow examples get patience` writes."""
    shutil.copy(ROOT / "docs" / "snippets" / "yaml" / "reneging_tie.yaml", tmp_path)
    for api, name in (
        ("yaml", "patience.yaml"),
        ("builder", "patience_example.py"),
        ("low-level", "patience_example.py"),
    ):
        (tmp_path / "patience" / api).mkdir(parents=True)
        shutil.copy(ROOT / "examples" / api / name, tmp_path / "patience" / api)
    monkeypatch.chdir(tmp_path)
    module = _load("reneging_and_balking")
    for tie in (module.tie, module.tie_yaml, module.tie_low_level):
        assert tie(3, 3) == "reneged"
        assert tie(0, 6) == "reneged"
        assert tie(3, 4) == "served"
    forms = (module.yaml_form, module.builder_form, module.low_level_form)
    for form in forms:
        assert form() == {"order_packed": 51, "balked": 5, "reneged": 13}
    printed = [
        "check: arrives at 3, patience 3: " + module.tie(3, 3),
        "check: arrives at 0, patience 6: " + module.tie(0, 6),
        "check: arrives at 3, patience 4: " + module.tie(3, 4),
        f"check: the same in YAML: {module.tie_yaml(3, 3)} {module.tie_yaml(0, 6)} "
        f"{module.tie_yaml(3, 4)}",
        f"check: the same, low-level: {module.tie_low_level(3, 3)} "
        f"{module.tie_low_level(0, 6)} {module.tie_low_level(3, 4)}",
        *(f"check: {dict(form())}" for form in forms),
    ]
    page = (ROOT / "docs/how-to/modelling/reneging-and-balking.md").read_text("utf-8")
    assert "```text\n" + "\n".join(printed) + "\n```" in page


def test_patience_example_uses_the_tie_form():
    """The Low-level patience example decides by `req not in result`, the form that
    gives a tie to the grant, and not by `req.triggered`, which gives it to the
    patience (row 187)."""
    source = (ROOT / "examples/low-level/patience_example.py").read_text("utf-8")
    assert "result = yield req | env.timeout(patience)" in source
    assert "if req not in result:" in source
    assert "req.triggered" not in source


def _numbers(keys):
    """The order numbers of keys such as `orders-7`, in order."""
    return sorted(int(key.split("-")[1]) for key in keys)


def test_reneging_follow_up_goes_on_to_a_task_in_every_form():
    """An order that reneges or balks goes to `follow_up`, which reads the status of the
    task it left. Of 12 orders 4 are packed, 3 renege (3, 5 and 6) and 5 balk (8 to 12),
    and the YAML, Builder and Low-level forms publish the same records."""
    from desflow import DynamicRealtimeEnvironment, SimulationContext

    def outcome(records):
        rows = [
            (r["sim_ts"], r["key"], r["task"], r["value"].get("how"))
            for r in records
            if r["stream_type"] == "event"
            and r["task"] in ("pack", "follow_up")
            and "status" not in r["value"]
        ]
        return sorted(rows, key=lambda row: (row[0], row[1]))

    yaml = SimulationContext.from_yaml(
        ROOT / "docs" / "snippets" / "yaml" / "reneging_follow_up.yaml"
    )
    capture = _Capture()
    yaml.add_egress(capture)
    yaml.run()
    expected = outcome(capture.records)

    builder = _load("reneging_follow_up").build()
    capture = _Capture()
    builder.add_egress(capture)
    builder.run(until=40)
    assert outcome(capture.records) == expected

    module = _load("reneging_follow_up_low_level")
    env = DynamicRealtimeEnvironment(factor=0)
    capture = _Capture()
    env.setup_egress([capture])
    module.build(env)
    env.run(until=40)
    env.teardown()
    assert outcome(capture.records) == expected

    by_how = {}
    for _, key, task, how in expected:
        by_how.setdefault(how if task == "follow_up" else "packed", []).append(key)
    assert _numbers(by_how["packed"]) == [1, 2, 4, 7]
    assert _numbers(by_how["reneged"]) == [3, 5, 6]
    assert _numbers(by_how["balked"]) == [8, 9, 10, 11, 12]
    page = (ROOT / "docs/how-to/modelling/reneging-and-balking.md").read_text("utf-8")
    shown = re.search(
        r"<!-- records: reneging-follow-up -->\n```text\n(.*?)\n```", page, re.S
    )
    assert shown is not None
    shown_rows = [json.loads(line) for line in shown.group(1).split("\n")]
    left = [row for row in expected if row[2] == "follow_up"]
    assert len(shown_rows) == len(left) == 8
    assert sorted(
        (r["sim_ts"], r["key"], r["value"]["how"]) for r in shown_rows
    ) == sorted((time, key, how) for time, key, _, how in left)


def test_wait_for_one_or_all_first_ties_and_failure():
    """The first branch wins; two signals at one instant are both done; a failed
    branch of an all fails the order at once while the child flow runs on. The
    builder, YAML and low-level forms agree."""
    module = _load("wait_for_one_or_all")
    low_level = _load("wait_for_one_or_all_low_level")
    yaml = ROOT / "docs" / "snippets" / "yaml"
    forms = [
        (module.first, module.two_at_once, module.all_with_a_failure),
        (
            lambda gap: module.first_yaml(gap, yaml / "wait_first.yaml"),
            lambda: module.two_at_once_yaml(yaml / "wait_tie.yaml"),
            lambda: module.all_with_a_failure_yaml(yaml / "wait_all.yaml"),
        ),
        (low_level.first, low_level.two_at_once, low_level.all_with_a_failure),
    ]
    for first, two_at_once, all_with_a_failure in forms:
        assert first(10) == {"stock": True, "limit": False}
        assert first(50) == {"stock": False, "limit": True}
        assert two_at_once() == {"boxes": True, "labels": True}
        assert all_with_a_failure() == [
            (0.0, "order", "queued"),
            (2.0, "order", "failed"),
            (4.0, "inspect", "finished"),
        ]


def test_wait_suspend_each_and_pool_in_every_form(tmp_path):
    """A truck wakes the two oldest of three parked orders with its value; an `each`
    wait with `at_least: 2` ends at the second box with the flags of the elements; a
    pool of three requests packs with the first and cancels the others, so a second
    order arriving at 1 s is served from 2 s to 3 s: without the `cancel` it never is.
    The builder, YAML and low-level forms agree, and a `when` wait ends when its expression holds."""
    module = _load("wait_for_one_or_all")
    low_level = _load("wait_for_one_or_all_low_level")
    yaml = ROOT / "docs" / "snippets" / "yaml"
    woken = [(5.0, "truck-1"), (5.0, "truck-1")]
    boxes = (5.0, [True, True, False])
    assert module.suspend_and_resume() == woken
    assert module.suspend_and_resume_yaml(yaml / "wait_suspend.yaml") == woken
    assert low_level.suspend_and_resume() == woken
    assert module.each_of_three() == boxes
    assert module.each_of_three_yaml(yaml / "wait_each.yaml") == boxes
    assert low_level.each_of_three() == boxes
    packed = (2.0, [True, False, False], 3.0)
    assert module.pool_of_requests() == packed
    assert module.pool_of_requests_yaml(yaml / "wait_pool.yaml") == packed
    assert low_level.pool_of_requests() == packed
    # Without the cancel the release hands the unit to a request nothing releases, so
    # the second order is never served: the YAML form shows it
    without_cancel = tmp_path / "wait_pool_without_cancel.yaml"
    text = (yaml / "wait_pool.yaml").read_text(encoding="utf-8")
    assert "    cancel: pool" in text
    without_cancel.write_text(text.replace("    cancel: pool", ""), encoding="utf-8")
    assert module.pool_of_requests_yaml(without_cancel) == (
        2.0,
        [True, False, False],
        None,
    )
    when = module.run_blueprint(yaml / "wait_when.yaml", {})
    approved = module._events("approve", when)[-1]
    assert (approved["sim_ts"], approved["value"]["address_done"]) == (3.0, False)


def test_an_each_when_group_reads_only_done_count(tmp_path):
    """The page says the `when` of an `each` group reads only `done_count` and that a
    `wait.<name>` reference is refused when the file loads: `done_count >= 2` loads and
    ends the wait at 5 s, and adding `wait.boxes.flags[0]` is refused."""
    from desflow import SimulationContext

    text = (ROOT / "docs/snippets/yaml/wait_each.yaml").read_text(encoding="utf-8")
    group = "group: {at_least: 2}"
    assert group in text
    allowed = tmp_path / "allowed.yaml"
    allowed.write_text(
        text.replace(group, 'group: {when: "done_count >= 2"}'), encoding="utf-8"
    )
    SimulationContext.from_yaml(allowed)
    refused = tmp_path / "refused.yaml"
    refused.write_text(
        text.replace(group, 'group: {when: "done_count >= 2 and wait.boxes.flags[0]"}'),
        encoding="utf-8",
    )
    with pytest.raises(Exception, match="when of an each group reads done_count"):
        SimulationContext.from_yaml(refused)
    page = (
        ROOT / "docs/user-guide/tasks-and-processes/signals-and-waiting.md"
    ).read_text("utf-8")
    assert "The `when` of an `each` group reads only `done_count`." in page
    assert "A `when` group reads `done_count` and `wait.<name>.values`" not in page


def test_wait_for_one_or_all_example_packs_and_refunds():
    """The page's example: each restock waits for all of its delivery (10 s) and its
    stock count (20 s), so it comes 20 s after each delivery starts, at 80 s to 560 s;
    22 orders are packed after a restock and 19 are refunded at the limit, in every
    form."""
    expected = {
        "pack": 22,
        "refund": 19,
        "restocked_at": [80.0 + 60 * n for n in range(9)],
    }
    assert _load("wait_for_one_or_all_example").outcome() == expected
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "wait_for_one_or_all_example.yaml"
    assert _load("wait_for_one_or_all").example_yaml(str(blueprint)) == expected
    assert _load("wait_for_one_or_all_example_low_level").outcome() == expected


def test_wait_for_one_or_all_checks_run_from_saved_files(tmp_path, monkeypatch):
    """The script runs in a folder with the saved blueprints and prints the lines the
    page shows; `desflow run --stdout` gives the counts and restock times the page
    reads from the YAML example."""
    from typer.testing import CliRunner

    from desflow.cli import app

    yaml = ROOT / "docs" / "snippets" / "yaml"
    for name in (
        "wait_first.yaml",
        "wait_tie.yaml",
        "wait_all.yaml",
        "wait_suspend.yaml",
        "wait_each.yaml",
        "wait_pool.yaml",
    ):
        shutil.copy(yaml / name, tmp_path)
    monkeypatch.chdir(tmp_path)
    module = _load("wait_for_one_or_all")
    printed = [
        f"check: restock after 10 s: {module.first(10)}",
        f"check: restock after 50 s: {module.first(50)}",
        f"check: two at once: {module.two_at_once()}",
        f"check: all, with a failure: {module.all_with_a_failure()}",
        f"check: the same in YAML: {module.first_yaml(10)} {module.first_yaml(50)}",
        f"check: two at once in YAML: {module.two_at_once_yaml()}",
        f"check: all, with a failure, in YAML: {module.all_with_a_failure_yaml()}",
        f"check: suspend and resume: {module.suspend_and_resume()}",
        f"check: suspend and resume, in YAML: {module.suspend_and_resume_yaml()}",
        f"check: two of three boxes: {module.each_of_three()}",
        f"check: two of three boxes, in YAML: {module.each_of_three_yaml()}",
        f"check: the first of three packers: {module.pool_of_requests()}",
        f"check: the first of three packers, in YAML: {module.pool_of_requests_yaml()}",
    ]
    page = (ROOT / "docs/how-to/modelling/wait-for-one-or-all.md").read_text("utf-8")
    assert "```text\n" + "\n".join(printed) + "\n```" in page

    result = CliRunner().invoke(
        app, ["run", str(yaml / "wait_for_one_or_all_example.yaml"), "--stdout"]
    )
    assert result.exit_code == 0, result.output
    records = result.stdout
    counts = re.findall(
        r'"path_id":"Shop.stats.tasks.(?:pack|refund).finished","value":\d+', records
    )[-2:]
    restocks = [
        re.search(r'"sim_ts":[0-9.]+', line).group()
        for line in records.splitlines()
        if '"event_type":"restocked"' in line
    ]
    shown = "\n".join("    " + line for line in counts + restocks)
    assert f"```text\n{shown}\n    ```" in page


def test_performance_tuning_counts_the_records():
    """An hour of the shop publishes 110,070 records at factor 0: no lag readings,
    which would add 3,600, and a rate above 0."""
    module = _load("performance_tuning")
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "performance_tuning.yaml"
    sys.path.insert(0, str(GUIDES))  # the low-level script imports Count from it
    try:
        low_level = _load("performance_tuning_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    for outcome in (
        module.blueprint(str(blueprint)),
        module.throughput(module.shop(), until=3600),
        low_level.throughput(until=3600),
    ):
        assert outcome["records"] == 110_070
        assert outcome["per_second"] > 0


def test_performance_tuning_commands_print_the_page_count(tmp_path):
    """Each command of the Check section, run from a folder that holds the saved
    files, prints the record count the page shows."""
    for source in (
        GUIDES / "performance_tuning.py",
        GUIDES / "performance_tuning_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "performance_tuning.yaml",
    ):
        shutil.copy(source, tmp_path)
    page = (ROOT / "docs/how-to/tools/performance-tuning.md").read_text("utf-8")
    assert "{'records': 110070, 'per_second': " in page
    for command in (
        ["performance_tuning.py", "yaml"],
        ["performance_tuning.py"],
        ["performance_tuning_low_level.py"],
    ):
        ran = subprocess.run(
            [sys.executable, *command], cwd=tmp_path, capture_output=True, text=True
        )
        assert ran.returncode == 0, ran.stderr
        assert ran.stdout.startswith("{'records': 110070, 'per_second': "), ran.stdout


def test_fit_a_distribution_gaps(tmp_path, capsys):
    """The exponential fits the 299 gaps best, at 0.364 a second; 5 zero gaps skip
    gamma, weibull and lognormal; the file it writes is the one the blueprint
    includes; and one packer packs 1,335 orders in the hour, in every form."""
    pytest.importorskip("scipy")
    from desflow import SimulationContext

    module = _load("fit_a_distribution")
    result = module.fit()
    capsys.readouterr()
    assert result.best == "exponential" and result.settings()["rate"] == 0.36388
    statuses = {row["family"]: (row["status"], row["reason"]) for row in result.table}
    for family in ("gamma", "weibull", "lognormal"):
        assert statuses[family] == (
            "skipped",
            f"{family} needs every value above 0; 5 are not",
        )
    assert statuses["truncated_normal"][0] == "did not fit"
    assert statuses["uniform"][0] == statuses["triangular"][0] == "rejected"
    histogram = module.histogram().settings()
    assert len(histogram["counts"]) == 10 and sum(histogram["counts"]) == 299
    written = module.write(result, tmp_path)
    blueprint = ROOT / "docs" / "snippets" / "yaml"
    assert written.read_text() == (blueprint / "fitted" / "orders.yaml").read_text()
    # The command line route reads the column the script writes
    from typer.testing import CliRunner

    from desflow.cli import app as cli

    column = module.write_gaps(tmp_path)
    assert column.read_text().splitlines()[0] == "gap_seconds"
    assert len(column.read_text().splitlines()) == 300
    out = tmp_path / "fitted" / "cli.yaml"
    ran = CliRunner().invoke(
        cli,
        ["fit", str(column), "--column", "gap_seconds", "--unit", "s", "--lower", "0"]
        + ["--out", str(out)],
    )
    assert ran.exit_code == 0, ran.output
    assert "exponential *" in ran.stdout and "rate=0.36388" in ran.stdout

    # The comment at the top names the source, which differs, so only the settings
    # are compared
    shipped = (blueprint / "fitted" / "orders.yaml").read_text()
    written = out.read_text()
    assert written[written.index("dist:") :] == shipped[shipped.index("dist:") :]
    app = module.shop(result)
    app.run(until="1 h")
    yaml_app = SimulationContext.from_yaml(blueprint / "fit_a_distribution.yaml")
    yaml_app.run()
    for run in (app, yaml_app):
        assert run.env.stats.readings()["Shop.stats.flows.orders.finished"] == 1335
    sys.path.insert(0, str(GUIDES))  # the low-level script imports fit from it
    try:
        low_level = _load("fit_a_distribution_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    assert low_level.run(result) == 1335


def test_test_a_model_tests_pass_and_the_saved_results_are_current(
    tmp_path, monkeypatch
):
    """The three kinds of test pass; the waits fall from 2.142 s with one packer to
    0.158 s with two and 0.016 s with three; saving the results writes the file the
    back test reads; and the YAML and low-level forms give the same results."""
    module = _load("test_a_model")
    module.test_more_packers_shorten_the_wait()
    module.test_results_match_the_saved_run()
    for packers, wait in ((1, 2.142251), (2, 0.157602), (3, 0.016406)):
        assert module.results(packers=packers)["wait_mean"] == wait
    assert module.results() == {"finished": 284, "wait_mean": 0.157602}
    for settings in (
        {"dist": "exponential", "rate": 0},
        {"dist": "constant", "value": -1},
    ):
        module.test_a_distribution_that_cannot_happen_is_refused(settings)
    monkeypatch.setattr(module, "SAVED", tmp_path / "saved.json")
    module.save_results()
    assert (tmp_path / "saved.json").read_text() == (
        GUIDES / "test_a_model_saved.json"
    ).read_text()
    # The YAML and low-level files run the same tests against the same saved results
    for source in (
        GUIDES / "test_a_model_yaml.py",
        GUIDES / "test_a_model_low_level.py",
        GUIDES / "test_a_model_saved.json",
        ROOT / "docs" / "snippets" / "yaml" / "test_a_model.yaml",
    ):
        shutil.copy(source, tmp_path)
    for form in (
        _load_from(tmp_path, "test_a_model_yaml"),
        _load_from(tmp_path, "test_a_model_low_level"),
    ):
        form.test_more_packers_shorten_the_wait()
        form.test_results_match_the_saved_run()
        for packers, wait in ((1, 2.142251), (2, 0.157602), (3, 0.016406)):
            assert form.results(packers=packers)["wait_mean"] == wait
    # The page shows "4 passed" for each form, and each file has four tests
    page = (ROOT / "docs/how-to/analysis/test-a-model.md").read_text("utf-8")
    assert page.count(" 4 passed in ") == 3
    shutil.copy(GUIDES / "test_a_model.py", tmp_path)
    for name in ("test_a_model", "test_a_model_yaml", "test_a_model_low_level"):
        ran = subprocess.run(
            [sys.executable, "-m", "pytest", f"{name}.py", "-p", "no:cacheprovider"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert " 4 passed in " in ran.stdout, ran.stdout


def test_analyse_the_event_log_waits_and_throughput(tmp_path):
    """Two minutes of one packer write 134 event rows, which become 223 log rows for
    45 orders; 44 leave the shop, the mean wait is 0.592 s and 22.0 orders leave each
    minute; every start is the later of the arrival and the last start plus 1.5 s;
    and the YAML and low-level forms write the same events."""
    module = _load("analyse_the_event_log")
    module.run(tmp_path)
    events = module.read_events(tmp_path)
    assert events.num_rows == 134
    statuses = [row for row in events["status"].to_pylist()]
    assert [statuses.count(s) for s in ("queued", "started", "finished")] == [
        45,
        45,
        44,
    ]
    log = module.event_log(events)
    assert log.column_names == ["entity_id", "event_type", "event", "time", "run"]
    assert log.num_rows == 223 and set(log["run"].to_pylist()) == {1}
    per_order = module.waits_and_times(log)
    assert len(per_order) == 45 and len(module.times(log, "depart")) == 44
    assert per_order["orders-1"] == {"wait": 0.0, "in_shop": 1.5}
    assert round(max(row["wait"] for row in per_order.values()), 3) == 3.63
    assert round(module.mean_wait(per_order), 3) == 0.592
    assert module.throughput_per_minute(log, module.RUN_SECONDS) == 22.0
    arrived = module.times(log, "arrival")
    started = module.times(log, "packing_start")
    last = None
    for order in sorted(started, key=started.get):
        expected = arrived[order] if last is None else max(arrived[order], last + 1.5)
        assert started[order] == pytest.approx(expected)
        last = started[order]
    # The log is sorted by time
    times = log["time"].to_pylist()
    assert times == sorted(times)
    # The YAML and low-level forms write the same events
    sys.path.insert(0, str(GUIDES))  # the low-level script imports from the module
    try:
        low_level = _load("analyse_the_event_log_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "analyse_the_event_log.yaml"
    module.run(tmp_path / "builder")
    module.run_yaml(tmp_path / "yaml", str(blueprint))
    low_level.run(tmp_path / "low_level")

    def rows(out):
        return sorted(
            (row["sim_ts"], row["key"], row["task"], row["status"])
            for row in module.read_events(out).to_pylist()
        )

    builder = rows(tmp_path / "builder")
    assert len(builder) == 134
    assert rows(tmp_path / "yaml") == rows(tmp_path / "low_level") == builder


def test_analyse_the_event_log_commands_print_the_page_counts(tmp_path):
    """Each command of the Check section, run from a folder that holds the saved
    files, prints the counts the page shows."""
    for source in (
        GUIDES / "analyse_the_event_log.py",
        GUIDES / "analyse_the_event_log_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "analyse_the_event_log.yaml",
    ):
        shutil.copy(source, tmp_path)
    page = (ROOT / "docs/how-to/analysis/analyse-the-event-log.md").read_text("utf-8")
    for command in (
        ["analyse_the_event_log.py", "yaml"],
        ["analyse_the_event_log.py"],
        ["analyse_the_event_log_low_level.py"],
    ):
        ran = subprocess.run(
            [sys.executable, *command], cwd=tmp_path, capture_output=True, text=True
        )
        assert ran.returncode == 0, ran.stderr
        assert f"```text\n{ran.stdout}```" in page


def test_animate_a_run_frames_and_entities(tmp_path):
    """The animation of the two-minute run has 61 frames, one for every 2 seconds
    from 0 to 120, and draws the 45 orders of Analyse the Event Log, in every form."""
    import vidigi  # noqa: F401  (a missing dev dependency fails the test)

    module = _load("animate_a_run")
    figure = module.make(tmp_path / "builder")
    assert len(figure.frames) == 61
    assert [frame.name for frame in figure.frames][:3] == ["0", "2", "4"]
    assert figure.frames[-1].name == "120"
    assert module.vidigi_log(tmp_path / "builder")["entity_id"].nunique() == 45
    # The YAML and low-level runs give the same animation
    analyse = _load("analyse_the_event_log")
    sys.path.insert(0, str(GUIDES))  # the low-level script imports from analyse
    try:
        low_level = _load("analyse_the_event_log_low_level")
    finally:
        sys.path.remove(str(GUIDES))
    blueprint = str(ROOT / "docs" / "snippets" / "yaml" / "analyse_the_event_log.yaml")
    for out, run_shop in (
        (tmp_path / "yaml", lambda out: analyse.run_yaml(out, blueprint)),
        (tmp_path / "low_level", low_level.run),
    ):
        other = module.make(out, run_shop)
        assert [frame.name for frame in other.frames] == [
            frame.name for frame in figure.frames
        ]
        assert module.vidigi_log(out).equals(module.vidigi_log(tmp_path / "builder"))


def test_share_by_weight_grants_by_finish_tag_in_every_form(tmp_path, monkeypatch):
    """Nine orders arrive at time 0: tier 2, six of tier 1, two of tier 2. Tier 1
    weighs 3 and tier 2 weighs 1, and the first order takes the packer, so the
    finish tags are worked out by hand here.

    The first order of tier 2 is granted at once with tag 1, and the queue's clock
    becomes 1. Tier 1 then gets 4/3, 5/3, 2, 7/3, 8/3 and 3 (1/3 more each time);
    tier 2 gets 2 and 3 (1 more each time). The smallest tag goes first, and at the
    tags 2 and 3 the order that arrived first goes first, which is tier 1. So the
    tags are granted 4/3, 5/3, 2 (tier 1), 2 (tier 2), 7/3, 8/3, 3 (tier 1), 3 (tier 2),
    and the tiers in grant order, including the first order, are 2, 1, 1, 1, 2, 1, 1, 1, 2.
    First in, first out would give
    2, 1, 1, 1, 1, 1, 1, 2, 2.
    """
    # A reader copies the page's files into one folder and runs the script there
    for source in (
        GUIDES / "share_by_weight.py",
        GUIDES / "share_by_weight_low_level.py",
        *(ROOT / "docs" / "snippets" / "yaml").glob("share_by_weight*"),
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    module = _load_from(tmp_path, "share_by_weight")
    low_level = _load_from(tmp_path, "share_by_weight_low_level")
    by_hand = [2, 1, 1, 1, 2, 1, 1, 1, 2]
    fifo = [2, 1, 1, 1, 1, 1, 1, 2, 2]
    assert module.TIERS == fifo  # the orders arrive in the first in, first out order
    assert by_hand != fifo
    for form in (module.yaml_form, module.builder_form, low_level.low_level_form):
        assert form() == by_hand


def test_editor_settings_hold_the_schema_and_the_three_tags():
    """Set Up Your Editor shows one settings object: the schema for a folder of
    blueprints and the three tags a blueprint uses."""
    settings = json.loads((ROOT / "docs/snippets/editor/settings.json").read_text())
    assert settings["yaml.customTags"] == [
        "!python scalar:string",
        "!include scalar:object",
        "!include mapping:object",
    ]
    assert list(settings["yaml.schemas"].values()) == ["blueprints/*.yaml"]


def test_check_against_theory_agrees_with_the_formulas(capsys, monkeypatch):
    """The wait by hand is 3 s for one packer and 1.286 s for two; explain gives the
    same values; 10 runs of the blueprint agree on every measure; and two packers
    with constant packing have no wait estimate, with D3's reason."""
    from desflow import SimulationContext

    module = _load("check_against_theory")
    blueprint = ROOT / "docs" / "snippets" / "yaml" / "check_against_theory.yaml"
    assert module.BLUEPRINT == blueprint
    assert round(module.by_hand(1, 0.75), 3) == 3.0
    assert round(module.by_hand(2, 1.5), 3) == 1.286
    packers = SimulationContext.from_yaml(blueprint).analyze().resources[0]
    assert packers.wait == pytest.approx(module.by_hand(1, 0.75), abs=1e-9)
    with monkeypatch.context() as patch:
        patch.setenv("PACKERS", "2")
        patch.setenv("RATE", "1.5")
        two = SimulationContext.from_yaml(blueprint).analyze().resources[0]
    assert two.wait == pytest.approx(module.by_hand(2, 1.5), abs=1e-9)
    explanation = module.verify()
    assert [c.differs for c in explanation.verification.comparisons] == [False] * 4
    shop = _load("check_against_theory_shop").app.analyze().resources[0]
    assert shop.wait == pytest.approx(module.by_hand(1, 0.75), abs=1e-9)
    # The builder file is the same model, so its runs give the same measures
    builder = module.verify_builder()
    assert [
        (c.measure, c.measured, c.half_width) for c in builder.verification.comparisons
    ] == [
        (c.measure, c.measured, c.half_width)
        for c in explanation.verification.comparisons
    ]
    reason = "no closed formula for c > 1 with non-exponential times; run --verify to measure it"
    unsupported = module.unsupported()
    assert (unsupported.utilization, unsupported.wait) == (0.75, None)
    assert unsupported.wait_reason == reason
    constant = (
        ROOT / "docs" / "snippets" / "yaml" / "check_against_theory_constant.yaml"
    )
    from_yaml = SimulationContext.from_yaml(constant).analyze().resources[0]
    assert (from_yaml.utilization, from_yaml.wait, from_yaml.wait_reason) == (
        0.75,
        None,
        reason,
    )
    capsys.readouterr()


def test_write_a_connector_applies_the_change_and_writes_the_events(
    tmp_path, monkeypatch
):
    """Nineteen orders arrive, one a second from 1 s, and each takes 1.5 s, so the
    18 that arrive by 18 s are packed by 20 s. The file's change gives the packers 2
    units; the ingress applies it within the first moments of the run, so from then
    on each order starts when it arrives. The three forms write the same events."""
    # A reader copies the page's files into one folder and runs the script there
    for source in (
        GUIDES / "write_a_connector.py",
        GUIDES / "write_a_connector_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "write_a_connector.yaml",
        ROOT / "docs" / "snippets" / "yaml" / "write_a_connector_logic.py",
    ):
        shutil.copy(source, tmp_path)
    monkeypatch.chdir(tmp_path)
    module = _load_from(tmp_path, "write_a_connector")
    low_level = _load_from(tmp_path, "write_a_connector_low_level")
    written = []
    for form in (module.yaml_form, module.builder_form, low_level.low_level_form):
        capacity, rows = form()
        written.append(rows)
        assert capacity == 2
        statuses = [row["status"] for row in rows if row["status"]]
        assert statuses.count("queued") == statuses.count("started") == 19
        packed = [row for row in rows if row["event_type"] == "order_packed"]
        assert len(packed) == 18
        last = [row for row in rows if row["key"] == "orders-19"]
        assert [(row["sim_ts"], row["status"]) for row in last] == [
            ("19.0", "queued"),
            ("19.0", "started"),
        ]
    assert written[0] == written[1] == written[2]
    # The page's YAML check writes the same change with `echo`, and shows these rows
    page = (ROOT / "docs/how-to/records-and-kafka/write-a-connector.md").read_text(
        "utf-8"
    )
    assert f"echo '{json.dumps(module.CHANGE)}' > changes.jsonl" in page
    shown = [
        f"{form.__name__:15} packers: 2, rows in events.csv: {len(rows)}"
        for form, rows in zip(
            (module.yaml_form, module.builder_form, low_level.low_level_form), written
        )
    ]
    assert all(line in page for line in shown)
    first_rows = ["sim_ts,key,task,status,event_type"] + [
        f"{r['sim_ts']},{r['key']},{r['task']},{r['status']},{r['event_type']}"
        for r in written[0][:5]
    ]
    assert "\n".join(first_rows) in page


_STORAGE_MODULES = (
    "cloud_storage",
    "cloud_storage_stores",
    "cloud_storage_low_level",
)


def _run_storage_form(form, tmp_path, monkeypatch, until=600):
    """Runs one form of the Write to Cloud Storage shop on the local disk, in tmp_path,
    for `until` simulated seconds.

    Returns the Parquet and the JSON Lines tables, read back with the page's
    read_events.
    """
    from pyarrow import fs

    from desflow import SimulationContext

    for source in (
        GUIDES / "cloud_storage.py",
        GUIDES / "cloud_storage_stores.py",
        GUIDES / "cloud_storage_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "cloud_storage.yaml",
    ):
        shutil.copy(source, tmp_path)
    shutil.copytree(
        ROOT / "docs" / "snippets" / "yaml" / "cloud_storage_stores",
        tmp_path / "cloud_storage_stores",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("STORAGE", "local")
    monkeypatch.setenv("STORAGE_BUCKET", "output")
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in _STORAGE_MODULES:
        monkeypatch.delitem(sys.modules, name, raising=False)
    try:
        if form == "builder":
            module = _load_from(tmp_path, "cloud_storage")
            module.app.run(until=until)
            module = sys.modules["cloud_storage_stores"]
        elif form == "yaml":
            SimulationContext.from_yaml(tmp_path / "cloud_storage.yaml").run(
                until=until
            )
            module = _load_from(tmp_path, "cloud_storage_stores")
        else:
            low_level = _load_from(tmp_path, "cloud_storage_low_level")
            env = low_level.shop(low_level.sinks)
            env.run(until=until)
            env.teardown()
            module = sys.modules["cloud_storage_stores"]
        store = fs.LocalFileSystem()
        folder = str(tmp_path / "output")
        return (
            module.read_events(store, f"{folder}/parquet"),
            module.read_events(store, f"{folder}/jsonl", format="json"),
        )
    finally:
        for name in _STORAGE_MODULES:
            sys.modules.pop(name, None)


@pytest.mark.parametrize("form", ["builder", "yaml", "low-level"])
def test_cloud_storage_shop_writes_the_same_events_as_parquet_and_jsonl(
    form, tmp_path, monkeypatch
):
    """Each form writes every event to both folders, and read_events reads them back:
    queued, started and finished events, the finished ones with the order record."""
    parquet, jsonl = _run_storage_form(form, tmp_path, monkeypatch)
    assert parquet.num_rows == jsonl.num_rows > 0
    rows = parquet.to_pylist()
    assert {row["status"] for row in rows} == {"queued", "started", None}
    finished = [row for row in rows if row["status"] is None]
    assert finished and all(row["amount"] == 19.99 for row in finished)
    assert {row["key"] for row in rows} == {row["key"] for row in jsonl.to_pylist()}


def test_cloud_storage_forms_write_the_same_rows(tmp_path, monkeypatch):
    """The seeded shop gives the same rows in YAML, Builder and Low-level, in the hour the
    scripts run, and the page shows the number of rows they hold."""
    rows = []
    for form in ("builder", "yaml", "low-level"):
        folder = tmp_path / form
        folder.mkdir()
        parquet, jsonl = _run_storage_form(form, folder, monkeypatch, until=3600)
        # The check block of the page: the number of rows each folder holds
        page = (ROOT / "docs/how-to/storage-and-tables/cloud-storage.md").read_text()
        assert (
            f"parquet: {parquet.num_rows} rows\njsonl: {jsonl.num_rows} rows\n```"
            in page
        )
        columns = ["sim_ts", "key", "status", "order_id"]
        rows.append(sorted(parquet.select(columns).to_pylist(), key=str))
    assert rows[0] == rows[1] == rows[2]


def test_cloud_storage_stores_make_their_filesystems(monkeypatch):
    """Each mapping of the page builds the PyArrow class of its store."""
    from pyarrow import fs

    from desflow.connectors.egress.storage import _build_filesystem

    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "myaccount")
    sys.modules.pop("cloud_storage_stores", None)
    try:
        stores = _load("cloud_storage_stores").STORES
    finally:
        sys.modules.pop("cloud_storage_stores", None)
    classes = {name: type(_build_filesystem(m, fs)) for name, m in stores.items()}
    assert classes == {
        "local": fs.LocalFileSystem,
        "s3": fs.S3FileSystem,
        "gcs": fs.GcsFileSystem,
        "azure": fs.AzureFileSystem,
    }


_ICEBERG_MODULES = (
    "iceberg_tables",
    "iceberg_tables_catalog",
    "iceberg_tables_low_level",
)


def _run_iceberg_form(form, tmp_path, monkeypatch):
    """Runs one form of the Write to Iceberg Tables shop on the SQLite catalog, in
    tmp_path, and returns the rows of shop.events, read back with the page's
    read_events."""
    from desflow import SimulationContext

    for source in (
        GUIDES / "iceberg_tables.py",
        GUIDES / "iceberg_tables_catalog.py",
        GUIDES / "iceberg_tables_low_level.py",
        ROOT / "docs" / "snippets" / "yaml" / "iceberg_tables.yaml",
    ):
        shutil.copy(source, tmp_path)
    shutil.copytree(
        ROOT / "docs" / "snippets" / "yaml" / "iceberg_tables_catalogs",
        tmp_path / "iceberg_tables_catalogs",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ICEBERG_CATALOG", raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))
    for name in _ICEBERG_MODULES:
        monkeypatch.delitem(sys.modules, name, raising=False)
    try:
        if form == "builder":
            module = _load_from(tmp_path, "iceberg_tables")
            module.app.run(until=600)
            module = sys.modules["iceberg_tables_catalog"]
        elif form == "yaml":
            # The SQLite database needs its folder, which the scripts make themselves
            (tmp_path / "iceberg").mkdir()
            SimulationContext.from_yaml(tmp_path / "iceberg_tables.yaml").run(until=600)
            module = _load_from(tmp_path, "iceberg_tables_catalog")
        else:
            low_level = _load_from(tmp_path, "iceberg_tables_low_level")
            env = low_level.shop(low_level.sink)
            env.run(until=600)
            env.teardown()
            module = sys.modules["iceberg_tables_catalog"]
        assert (
            module.catalog["uri"]
            == f"sqlite:///{tmp_path.resolve()}/iceberg/catalog.db"
        )
        return module.read_events(module.catalog)
    finally:
        for name in _ICEBERG_MODULES:
            sys.modules.pop(name, None)


def test_iceberg_shop_writes_the_same_rows_in_every_form(tmp_path, monkeypatch):
    """Each form appends every event to shop.events in one commit, and the seeded shop
    gives the same rows in YAML, Builder and Low-level."""
    rows = []
    for form in ("builder", "yaml", "low-level"):
        folder = tmp_path / form
        folder.mkdir()
        table = _run_iceberg_form(form, folder, monkeypatch)
        assert table.num_rows > 0
        columns = ["sim_ts", "key", "status", "order_id", "amount"]
        rows.append(sorted(table.select(columns).to_pylist(), key=str))
    assert rows[0] == rows[1] == rows[2]
    statuses = {row["status"] for row in rows[0]}
    assert statuses == {"queued", "started", None}
    assert all(row["amount"] == 19.99 for row in rows[0] if row["status"] is None)


def test_iceberg_catalogs_name_their_types(monkeypatch, tmp_path):
    """Each catalog of the page is the PyIceberg type its comment names, and
    ICEBERG_CATALOG picks one, with its name."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ICEBERG_CATALOG", "glue")
    sys.modules.pop("iceberg_tables_catalog", None)
    try:
        module = _load("iceberg_tables_catalog")
    finally:
        sys.modules.pop("iceberg_tables_catalog", None)
    types = {name: c["type"] for name, c in module.CATALOGS.items()}
    assert types == {"sqlite": "sql", "rest": "rest", "postgres": "sql", "glue": "glue"}
    assert module.catalog == {"name": "glue", **module.CATALOGS["glue"]}


def test_storage_checks_print_the_row_counts_the_pages_show(tmp_path):
    """After a full hour of the builder shop, the stores file and the catalog file
    print the counts that the Check the Result sections show."""
    for name in (
        "cloud_storage",
        "cloud_storage_stores",
        "iceberg_tables",
        "iceberg_tables_catalog",
    ):
        shutil.copy(GUIDES / f"{name}.py", tmp_path)

    def python(script):
        ran = subprocess.run(
            [sys.executable, script],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            env={k: v for k, v in os.environ.items() if k != "STORAGE"},
        )
        assert ran.returncode == 0, ran.stderr
        return ran.stdout

    python("cloud_storage.py")
    cloud = python("cloud_storage_stores.py")
    python("iceberg_tables.py")
    iceberg = python("iceberg_tables_catalog.py")
    docs = ROOT / "docs" / "how-to" / "storage-and-tables"
    assert f"```text\n{cloud}```" in (docs / "cloud-storage.md").read_text("utf-8")
    assert f"```text\n{iceberg}```" in (docs / "iceberg-tables.md").read_text("utf-8")


# Call Python from YAML: each form of SimPy behaviour the keys do not give yet runs
# beside a plain SimPy program and gives its times


def _simpy_form(name):
    """The (time, key) of every payload record a form's blueprint publishes, by task."""
    from desflow import SimulationContext

    sys.modules.pop("simpy_forms", None)
    app = SimulationContext.from_yaml(YAML_SNIPPETS / f"{name}.yaml")
    capture = _Capture()
    app.add_egress(capture)
    app.factor = 0.0
    app.run()
    return [
        (r["sim_ts"], r["value"])
        for r in capture.records
        if r["stream_type"] == "event" and "status" not in r["value"]
    ]


def test_computed_time_form_gives_simpy_times():
    import simpy

    env = simpy.Environment()
    pump = simpy.Resource(env, 1)
    expected = []

    def car(name, start, litres):
        yield env.timeout(start)
        with pump.request() as request:
            yield request
            yield env.timeout(litres / 2)
        expected.append((env.now, name))

    for name, start, litres in (("car-1", 0, 4), ("car-2", 1, 6), ("car-3", 2, 2)):
        env.process(car(name, start, litres))
    env.run(until=10)
    found = _simpy_form("simpy_computed_time")
    assert (
        [(t, v["car"]) for t, v in found]
        == expected
        == [
            (2, "car-1"),
            (5, "car-2"),
            (6, "car-3"),
        ]
    )


def test_held_unit_form_gives_simpy_times():
    import simpy

    env = simpy.Environment()
    machine = simpy.PriorityResource(env, 1)
    expected = []

    def job(name, start, priority, first, second):
        yield env.timeout(start)
        with machine.request(priority=priority) as request:
            yield request
            yield env.timeout(first)
            yield env.timeout(second)
        expected.append((env.now, name))

    for args in (
        ("job-1", 0, 2, 1, 2),
        ("job-2", 0, 1, 2, 1),
        ("job-3", 1, 0, 1, 1),
    ):
        env.process(job(*args))
    env.run(until=20)
    found = _simpy_form("simpy_held_unit")
    assert (
        [(t, v["job"]) for t, v in found]
        == expected
        == [
            (3, "job-1"),
            (5, "job-3"),
            (8, "job-2"),
        ]
    )


def test_fixed_arrivals_form_gives_simpy_times():
    import simpy

    env = simpy.Environment()
    counter = simpy.Resource(env, 1)
    expected = []

    def customer():
        with counter.request() as request:
            yield request
            yield env.timeout(3)
        expected.append(env.now)

    def customers():
        for _ in range(3):
            yield env.timeout(2)
            env.process(customer())

    env.process(customers())
    env.run(until=20)
    found = _simpy_form("simpy_fixed_arrivals")
    assert [t for t, _ in found] == expected == [5, 8, 11]


def test_sub_process_form_passes_an_argument_and_reads_the_value():
    import simpy

    env = simpy.Environment()
    expected = []

    def child(boxes):
        yield env.timeout(1)
        return {"total": boxes * 3}

    def order(boxes):
        packed = yield env.process(child(boxes))
        expected.append((env.now, packed["total"] + 1))

    env.process(order(2))
    env.run(until=10)
    found = _simpy_form("simpy_sub_process")
    assert (
        [(t, v["total"]) for t, v in found if "total" in v][-1:] == expected == [(1, 7)]
    )


_KEYS_WAIT_AGAIN = """
simulation: {sim_id: Shop, factor: 0}
resources: {r: {current_cap: 1}}
services: {two: {dist: constant, value: 2}}
tasks:
  work: {wait: {timeout: 5}, on_interrupt: resume, payload: {job: job-1}}
  boss: {service: two, interrupt: {task: work, cause: pause}, payload: {boss: 1}}
arrivals:
  w: {replay: {path: w.csv}, spawn: work}
  b: {replay: {path: b.csv}, spawn: boss}
run: {until: 20}
"""


def test_wait_again_form_ends_the_timeout_at_its_first_time(tmp_path):
    """After an interrupt at 2, a process that yields the same timeout again ends it
    at 5, as SimPy does. The keys (`wait: {timeout: 5}` with `on_interrupt: resume`)
    start the 5 seconds again, so the task ends at 7."""
    import simpy

    env = simpy.Environment()
    expected = []

    def job(name):
        timeout = env.timeout(5)
        while True:
            try:
                yield timeout
                break
            except simpy.Interrupt:
                pass
        expected.append((env.now, name))

    def pause(target):
        yield env.timeout(2)
        target.interrupt("pause")

    process = env.process(job("job-1"))
    env.process(pause(process))
    env.run(until=20)
    found = _simpy_form("simpy_wait_again")
    assert [(t, v["job"]) for t, v in found] == expected == [(5, "job-1")]

    (tmp_path / "w.csv").write_text("time\n0\n")
    (tmp_path / "b.csv").write_text("time\n0\n")
    blueprint = tmp_path / "keys.yaml"
    blueprint.write_text(_KEYS_WAIT_AGAIN, encoding="utf-8")
    from desflow import SimulationContext

    app = SimulationContext.from_yaml(blueprint)
    capture = _Capture()
    app.add_egress(capture)
    app.factor = 0.0
    app.run()
    ends = [
        r["sim_ts"]
        for r in capture.records
        if r["stream_type"] == "event" and r["value"].get("job") == "job-1"
    ]
    assert ends == [7]


def test_whole_flow_form_goes_on_when_the_flow_ends():
    """A process that yields the process of a two-task flow goes on at 6, when the
    flow ends, as SimPy's `yield process` does. A `running` wait on the first task
    goes on at 3, when that task ends."""
    import simpy

    env = simpy.Environment()
    expected = []

    def flow():
        yield env.timeout(3)
        yield env.timeout(3)

    def watch():
        yield env.process(flow())
        expected.append(env.now)

    env.process(watch())
    env.run(until=20)
    found = _simpy_form("simpy_whole_flow")
    assert [t for t, v in found if v.get("step") == "done"] == expected == [6]
    assert [t for t, v in found if v.get("step") == "relay"] == [3]


def _keys_queue_again(tmp_path):
    """The keys: `first` queues behind `holder`, is interrupted at 2 under
    `on_interrupt: resume`, and asks again."""
    text = """
simulation: {sim_id: Shop, factor: 0}
resources: {machine: {current_cap: 1}}
services:
  five: {dist: constant, value: 5}
  one: {dist: constant, value: 1}
  two: {dist: constant, value: 2}
tasks:
  holder: {resource: machine, service: five, payload: {job: holder}}
  first: {resource: machine, service: one, on_interrupt: resume, payload: {job: first}}
  second: {resource: machine, service: one, payload: {job: second}}
  boss: {service: two, interrupt: {task: first, cause: pause}, payload: {boss: 1}}
arrivals:
  h: {replay: {path: h.csv}, spawn: holder}
  f: {replay: {path: f.csv}, spawn: first}
  s: {replay: {path: s.csv}, spawn: second}
  b: {replay: {path: b.csv}, spawn: boss}
run: {until: 20}
"""
    for name, at in (("h", 0), ("f", 0), ("s", 1), ("b", 0)):
        (tmp_path / f"{name}.csv").write_text(f"time\n{at}\n")
    blueprint = tmp_path / "keys.yaml"
    blueprint.write_text(text, encoding="utf-8")
    from desflow import SimulationContext

    app = SimulationContext.from_yaml(blueprint)
    capture = _Capture()
    app.add_egress(capture)
    app.factor = 0.0
    app.run()
    return [
        (r["sim_ts"], r["value"]["job"])
        for r in capture.records
        if r["stream_type"] == "event" and "job" in r["value"]
    ]


def test_queue_again_form_keeps_its_place_in_the_queue(tmp_path):
    """`first` queues behind `holder`, which holds the machine to 5, and is interrupted
    at 2. A SimPy process that yields the same request again keeps its place, so it
    goes on at 5 and ends at 6, and `second` ends at 7. The keys (`on_interrupt:
    resume`) ask again at the back: `second` goes on at 5 and ends at 6, and `first`
    ends at 7."""
    import simpy

    env = simpy.Environment()
    machine = simpy.Resource(env, 1)
    expected = []

    def job(name, start, time, interrupt_at=None):
        yield env.timeout(start)
        if interrupt_at is not None:
            env.process(pause(env.active_process, interrupt_at))
        request = machine.request()
        while True:
            try:
                yield request
                break
            except simpy.Interrupt:
                pass
        yield env.timeout(time)
        machine.release(request)
        expected.append((env.now, name))

    def pause(target, at):
        yield env.timeout(at)
        target.interrupt("pause")

    env.process(job("holder", 0, 5))
    env.process(job("first", 0, 1, 2))
    env.process(job("second", 1, 1))
    env.run(until=20)
    assert expected == [(5, "holder"), (6, "first"), (7, "second")]
    found = _simpy_form("simpy_queue_again")
    assert [(t, v["job"]) for t, v in found] == expected
    assert _keys_queue_again(tmp_path) == [
        (5, "holder"),
        (6, "second"),
        (7, "first"),
    ]
