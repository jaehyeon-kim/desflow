"""Regenerate the docs blocks that show records, from a run of the code beside them.

A block of records on a page is marked by a comment line naming it, directly above
its fence:

    <!-- records: first-trace -->
    ```text
    [EVT] {...}
    ```

The comment can be indented, inside a content tab, and the fence then carries the
same indent. Each name in `BLOCKS` runs a snippet or an example unpaced and keeps the
records the page explains. The `timestamp` of a record is the wall-clock time the
run started plus its simulation time, so it differs on every run and is shown as
`'...'`.

Usage:
    python scripts/record_blocks.py          # rewrite every block
    python scripts/record_blocks.py --check  # list the blocks that are out of date
"""

import ast
import contextlib
import importlib.util
import logging
import os
import re
import sys
from pathlib import Path
from typing import Callable, Dict, List
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"

_MARKER = re.compile(
    r"^(?P<indent> *)<!-- records: (?P<name>[\w-]+) -->\n"
    r"(?P=indent)```text\n(?P<body>.*?)\n(?P=indent)```$",
    re.S | re.M,
)
_TIMESTAMP = re.compile(r"'timestamp': '[^']*'")


class _Capture(logging.Handler):
    """Keeps the lines ConsoleEgress logs, in order."""

    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.lines: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        if message[:6] in ("[EVT] ", "[TEL] ", "[LBL] "):
            self.lines.append(_TIMESTAMP.sub("'timestamp': '...'", message))


def _capture(run: Callable[[], object]) -> List[str]:
    """Runs `run` and returns the records ConsoleEgress printed."""
    handler = _Capture()
    logger = logging.getLogger("desflow.connectors.egress.local")
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        run()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
    return handler.lines


def _yaml(path: str, until: object = None) -> List[str]:
    """The records of a blueprint, run unpaced."""
    from desflow import SimulationContext

    def run() -> None:
        app = SimulationContext.from_yaml(ROOT / path)
        app.factor = 0.0
        app.run(until=until)

    return _capture(run)


def _module(path: str):
    """Imports a script by its path, with its folder on the import path."""
    file = ROOT / path
    sys.path.insert(0, str(file.parent))
    try:
        spec = importlib.util.spec_from_file_location(file.stem, file)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(file.parent))


def _fields(line: str) -> dict:
    """The record a console line prints, as a dict."""
    return ast.literal_eval(line[6:])


def _until(lines: List[str], end: float) -> List[str]:
    return [line for line in lines if _fields(line)["sim_ts"] <= end]


def _events(lines: List[str]) -> List[str]:
    return [line for line in lines if line.startswith("[EVT] ")]


# --- the blocks ---------------------------------------------------------------


def first_trace() -> List[str]:
    """First Simulation: the events of the first three orders."""
    return _until(_events(_yaml("docs/snippets/yaml/first_shop.yaml")), 6)


def queues_table() -> List[str]:
    """Queues and Capacity: the mean wait of the busy hour for 1 to 4 packers."""
    module = _module("docs/snippets/tutorials/queues_and_capacity.py")
    return [f"packers {n}: mean wait {module.mean_wait(n):8.2f} s" for n in range(1, 5)]


# A run without a seed draws one at random. The seed lines show this one, so the
# page does not change on every run; the line is otherwise the code's own.
_DRAWN_SEED = 3166812249


def _seed_line(run: Callable[[], object]) -> List[str]:
    """The line an unseeded run logs about its seed."""
    lines: List[str] = []

    class _Keep(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if "random_seed" in record.getMessage():
                lines.append(record.getMessage())

    handler = _Keep(logging.INFO)
    logger = logging.getLogger("desflow.core.context")
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        with mock.patch("secrets.randbelow", return_value=_DRAWN_SEED):
            _capture(run)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
    return lines


@contextlib.contextmanager
def _cwd(path: Path):
    before = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(before)


def seed_line_yaml() -> List[str]:
    """Randomness and Seeds: what `desflow run` logs without a seed."""
    from desflow import SimulationContext

    def run() -> None:
        with _cwd(DOCS / "snippets" / "yaml"):
            app = SimulationContext.from_yaml(Path("randomness_and_seeds.yaml"))
        app.random_seed = None
        app.run(until=1)

    return _seed_line(run)


def seed_line_builder() -> List[str]:
    """Randomness and Seeds: what a builder run logs without a seed."""

    def run() -> None:
        app = _module("docs/snippets/tutorials/randomness_and_seeds.py").app
        app.random_seed = None
        app.run(until=1)

    return _seed_line(run)


def twelfth_order() -> List[str]:
    """Randomness and Seeds: the events of orders-12 with the seed 42."""
    lines = _yaml("docs/snippets/yaml/randomness_and_seeds.yaml")
    return [line for line in lines if _fields(line).get("key") == "orders-12"]


def choosing_table() -> List[str]:
    """Choosing Inputs: the mean wait of the busy hour for each packing candidate."""
    module = _module("docs/snippets/tutorials/choosing_inputs.py")
    return [
        f"{name:<12} mean wait {module.mean_wait(name):5.2f} s"
        for name in module.CANDIDATES
    ]


def _stats(lines: List[str]) -> List[str]:
    return [line for line in lines if ".stats." in line and line.startswith("[TEL] ")]


def measuring_final() -> List[str]:
    """Measuring a Run: the statistics published at the end of the busy hour."""
    lines = _stats(_yaml("docs/snippets/yaml/measuring_a_run.yaml"))
    return [line for line in lines if _fields(line)["sim_ts"] == 3600]


def trusting_table() -> List[str]:
    """Trusting Results: the mean wait without and with the warm-up."""
    module = _module("docs/snippets/tutorials/trusting_results.py")
    lines: List[str] = []

    def run() -> None:
        lines.append(f"without a warm-up: mean wait {module.mean_wait(None):.3f} s")
        lines.append(
            f"with a 2 min warm-up: mean wait {module.mean_wait('2 min'):.3f} s"
        )

    _capture(run)
    return lines


def replications_telemetry() -> List[str]:
    """Replications: the telemetry a sink receives from three replications."""
    module = _module("docs/snippets/pages/replications.py")
    return _capture(module.builder)


def replications_summary() -> List[str]:
    """Replications: the summary of the same three replications."""
    module = _module("docs/snippets/pages/replications.py")
    result: List[object] = []
    _capture(lambda: result.append(module.builder()))
    return result[0].summary_text().split("\n")  # type: ignore[attr-defined]


def study_report() -> List[str]:
    """Local Studies: the results, comparison, check and observed part of the
    study example's report."""
    import tempfile

    module = _module("examples/builder/study_example.py")
    with tempfile.TemporaryDirectory() as folder:
        with contextlib.redirect_stdout(open(os.devnull, "w")):
            report = module.run(output=Path(folder)).report.split("\n")
    return report[report.index("## Results") : report.index("## Limits") - 1]


def fit_table() -> List[str]:
    """Fit the Inputs: the table of candidates for the 40 packing times."""
    import io

    module = _module("docs/snippets/tutorials/fit_the_inputs.py")
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        module.fitted()
    return printed.getvalue().rstrip("\n").split("\n")


def fit_gaps() -> List[str]:
    """Fit a Distribution: the table of candidates for the gaps between orders."""
    import io

    module = _module("docs/snippets/guides/fit_a_distribution.py")
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        module.fit()
    return printed.getvalue().rstrip("\n").split("\n")


def stock_out() -> List[str]:
    """Stock: the stock's readings around the second stock-out of 40-box deliveries."""
    module = _module("docs/snippets/tutorials/stock.py")
    lines = _capture(lambda: module.shop(40).run(until=210))
    keep = (".stock.level'", ".stock.getters_waiting'", ".packers.queue_length'")
    return [
        line
        for line in lines
        if any(k in line for k in keep) and 170 <= _fields(line)["sim_ts"] <= 200
    ]


def stock_table() -> List[str]:
    """Stock: the mean wait for 60-box and 40-box deliveries."""
    module = _module("docs/snippets/tutorials/stock.py")
    lines: List[str] = []

    def run() -> None:
        for delivery in (60, 40):
            wait = module.mean_wait(delivery)
            lines.append(f"deliveries of {delivery}: mean wait {wait:6.2f} s")

    _capture(run)
    return lines


def priorities_table() -> List[str]:
    """Priorities: the mean wait of each kind of order under each queue order."""
    module = _module("docs/snippets/tutorials/priorities.py")
    lines: List[str] = []

    def run() -> None:
        for queue in ("fifo", "priority"):
            for task, wait in module.mean_waits(queue).items():
                lines.append(f"{queue:<8} {task:<12} mean wait {wait:5.2f} s")

    _capture(run)
    return lines


def one_order_flow() -> List[str]:
    """A Flow of Tasks: every event of orders-172, backordered and packed twice."""
    lines = _events(_yaml("docs/snippets/yaml/flow_of_tasks.yaml"))
    return [line for line in lines if _fields(line)["key"] == "orders-172"]


def _logged(logger_name: str, run: Callable[[], object]) -> List[str]:
    """The messages one logger writes while `run` runs."""
    lines: List[str] = []

    class _Keep(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            lines.append(record.getMessage())

    handler = _Keep(logging.INFO)
    logger = logging.getLogger(logger_name)
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        run()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
    return lines


def _printed(run: Callable[[], object]) -> List[str]:
    """What `run` prints."""
    import io

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        run()
    return out.getvalue().rstrip("\n").split("\n")


def invalid_parameters() -> List[str]:
    """Arrivals and Services: the errors of three refused distributions."""
    return _printed(lambda: _module("docs/snippets/pages/arrivals_services.py"))


def refused_update() -> List[str]:
    """Arrivals and Services: the log line of a refused live update."""
    return _logged(
        "desflow.core.registry",
        lambda: _module("docs/snippets/pages/arrivals_services.py"),
    )


def task_events() -> List[str]:
    """Tasks and Processes: the events of orders-1, packed and then labelled."""
    lines = _events(_yaml("docs/snippets/yaml/tasks.yaml"))
    return [line for line in lines if _fields(line)["key"] == "orders-1"]


def _script_output(path: str) -> List[str]:
    """What a snippet prints when it runs as a script."""
    import runpy

    return _printed(lambda: runpy.run_path(str(ROOT / path), run_name="__main__"))


def resources_printed() -> List[str]:
    """Resources: the order each queue order serves, and a capacity drop."""
    return _script_output("docs/snippets/pages/resources.py")


def holder_readings() -> List[str]:
    """Stores and Containers: the readings before and after the stock count."""
    lines = _yaml("docs/snippets/yaml/stores_containers.yaml")
    keep = (
        "containers.stock.level'",
        "containers.stock.getters_waiting'",
        "stores.parcels.level'",
        "stores.parcels.putters_waiting'",
    )
    return [
        line
        for line in lines
        if any(k in line for k in keep) and _fields(line)["sim_ts"] in (30.0, 40.0)
    ]


def gift_flow() -> List[str]:
    """Task Flow: the finished events of gifts-5, backordered once, then wrapped."""
    lines = _events(_yaml("docs/snippets/yaml/task_flow.yaml"))
    return [
        line
        for line in lines
        if _fields(line)["key"] == "gifts-5" and "status" not in _fields(line)["value"]
    ]


def refused_expressions() -> List[str]:
    """Task Flow: the errors of three refused expressions."""
    return _printed(lambda: _module("docs/snippets/pages/task_flow.py"))


def statistics_final() -> List[str]:
    """Statistics: the statistics published at the end of the run."""
    lines = _stats(_yaml("docs/snippets/yaml/statistics.yaml"))
    return [line for line in lines if _fields(line)["sim_ts"] == 120]


def blueprint_error() -> List[str]:
    """Blueprints: what a blueprint with a mistake prints."""
    with _cwd(ROOT):
        return _printed(lambda: _module("docs/snippets/pages/blueprint_errors.py"))


def label_records() -> List[str]:
    """Live Parameters: the ground-truth records the labels sink wrote."""
    import tempfile

    with tempfile.TemporaryDirectory() as folder, _cwd(Path(folder)):
        _yaml("docs/snippets/yaml/ground_truth.yaml")
        lines: List[str] = []
        for file in sorted(Path("labels").glob("*.jsonl")):
            lines += file.read_text(encoding="utf-8").splitlines()
    return [re.sub(r'"timestamp":"[^"]*"', '"timestamp":"..."', line) for line in lines]


def change_lands() -> List[str]:
    """Live Parameters: the packers' capacity before and after a scenario step."""
    lines = _yaml("docs/snippets/yaml/scenario.yaml")
    return [
        line
        for line in lines
        if "packers.capacity'" in line and _fields(line)["sim_ts"] in (20.0, 30.0, 40.0)
    ]


def ingress_old_keys() -> List[str]:
    """Ingress: the line a message with the old keys is skipped with."""
    return _printed(lambda: _module("docs/snippets/pages/ingress_senders.py"))


def builder_first() -> List[str]:
    """Builder API: the first events of the guide's simulation."""
    import runpy

    path = str(ROOT / "docs/snippets/pages/builder.py")
    lines = _capture(lambda: runpy.run_path(path, run_name="__main__"))
    return _events(lines)[:8]


def record_examples() -> List[str]:
    """Records and Telemetry: one telemetry record and the events of one task."""
    readings = _yaml("docs/snippets/yaml/scenario.yaml")
    telemetry = [line for line in readings if "packers.in_use'" in line][:1]
    lines = _yaml("docs/snippets/yaml/tasks.yaml")
    events = [
        line
        for line in _events(lines)
        if _fields(line)["key"] == "orders-1" and _fields(line)["task"] == "pack_order"
    ]
    return telemetry + events


def local_records() -> List[str]:
    """Local Simulation: the first capacity reading, the first order, and the change."""
    lines = _yaml("examples/yaml/local.yaml")
    capacity = [line for line in lines if "packers.capacity'" in line]
    orders = [line for line in _events(lines) if _fields(line)["key"] == "orders-1"]
    changed = [line for line in capacity if _fields(line)["value"] == 3][:1]
    return sorted(
        capacity[:1] + orders + changed, key=lambda line: _fields(line)["sim_ts"]
    )


def hybrid_events() -> List[str]:
    """Python in a Blueprint: the first packed order and the first break."""
    lines = _events(_yaml("docs/snippets/yaml/hybrid.yaml"))
    packed = [line for line in lines if "'delivery'" in line][:1]
    breaks = [line for line in lines if "packers-break" in line][:2]
    return packed + breaks


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "hybrid-events": hybrid_events,
    "local-records": local_records,
    "record-examples": record_examples,
    "builder-first": builder_first,
    "ingress-old-keys": ingress_old_keys,
    "label-records": label_records,
    "change-lands": change_lands,
    "blueprint-error": blueprint_error,
    "statistics-final": statistics_final,
    "gift-flow": gift_flow,
    "refused-expressions": refused_expressions,
    "holder-readings": holder_readings,
    "resources-printed": resources_printed,
    "task-events": task_events,
    "invalid-parameters": invalid_parameters,
    "refused-update": refused_update,
    "one-order-flow": one_order_flow,
    "priorities-table": priorities_table,
    "stock-table": stock_table,
    "stock-out": stock_out,
    "trusting-table": trusting_table,
    "replications-telemetry": replications_telemetry,
    "replications-summary": replications_summary,
    "study-report": study_report,
    "fit-table": fit_table,
    "fit-gaps": fit_gaps,
    "measuring-final": measuring_final,
    "choosing-table": choosing_table,
    "first-trace": first_trace,
    "queues-table": queues_table,
    "seed-line-yaml": seed_line_yaml,
    "seed-line-builder": seed_line_builder,
    "twelfth-order": twelfth_order,
}


def _pages() -> List[Path]:
    return sorted(DOCS.rglob("*.md"))


def used() -> Dict[str, List[Path]]:
    """Every block name the pages use, with the pages that use it."""
    found: Dict[str, List[Path]] = {}
    for page in _pages():
        for match in _MARKER.finditer(page.read_text(encoding="utf-8")):
            found.setdefault(match["name"], []).append(page)
    return found


def _rewrite(page: Path, cache: Dict[str, List[str]]) -> str:
    text = page.read_text(encoding="utf-8")

    def replace(match: re.Match) -> str:
        indent, name = match["indent"], match["name"]
        if name not in cache:
            cache[name] = BLOCKS[name]()
        body = "\n".join(indent + line for line in cache[name])
        return f"{indent}<!-- records: {name} -->\n{indent}```text\n{body}\n{indent}```"

    return _MARKER.sub(replace, text)


def stale() -> List[str]:
    """The pages whose blocks differ from a new run, as `page: name` lines."""
    cache: Dict[str, List[str]] = {}
    out = []
    for page in _pages():
        text = page.read_text(encoding="utf-8")
        if not _MARKER.search(text):
            continue
        if _rewrite(page, cache) != text:
            names = sorted({m["name"] for m in _MARKER.finditer(text)})
            out.append(f"{page.relative_to(ROOT)}: {', '.join(names)}")
    return out


def main(argv: List[str]) -> int:
    if "--check" in argv:
        out = stale()
        print("\n".join(out) or "Every block is up to date.")
        return 1 if out else 0
    cache: Dict[str, List[str]] = {}
    for page in _pages():
        text = page.read_text(encoding="utf-8")
        if _MARKER.search(text):
            new = _rewrite(page, cache)
            if new != text:
                page.write_text(new, encoding="utf-8")
                print(f"rewrote {page.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
