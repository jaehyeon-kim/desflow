"""Runs the forms of a kept run in a copy of its workspace, and holds what the checks share.

`run_form` runs a form as its reader runs it (`desflow run model.yaml` or `python FILE`)
with the run's own prepared Python. `run_seeded` runs it at a seed set through desflow: the
YAML with `desflow run --seed`, a Python form through the run child of
`scripts/check_model.py` of the repository's current tree, which also records each seed that
the model's own code sets. Results are cached per form, way and seed.
"""

import ast
import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import yaml


# The run child that runs a Python form at a seed and records its draws
CHECK_MODEL = Path(__file__).resolve().parents[1] / "scripts" / "check_model.py"
KINDS = ["K1", "K2", "K4", "K6", "K7", "K9", "K11", "K12", "K13", "BASE"]
FORMS = ["yaml", "builder", "low_level"]


FILES = {"yaml": "model.yaml", "builder": "builder.py", "low_level": "low_level.py"}


ENGINE_STATUS = {"queued", "started", "resumed"}


# Why: a wait for a run is bounded; a model that never ends is a mistake of its own.
RUN_TIMEOUT = 180


class Done(NamedTuple):
    """The outcome of one command.

    `wall` is the wall time between the first and the last line of standard output that is
    a JSON object, as the lines arrive. A paced run prints its records over its simulated
    span; the start of Python and the half second desflow waits at teardown are outside it.
    """

    rc: int
    out: str
    err: str
    wall: float


class Seeded(NamedTuple):
    """A seed the model's own code set, as the run child of `check_model.py` reports it:
    `where` (its file and line, relative to the workspace when it is in it), the call that
    took it, and whether a `Sampler` was given the generator it made."""

    where: str
    call: str
    sampler: bool


class FormRun(NamedTuple):
    """The outcome of one form's run, with the JSON records it printed.

    `seeded` holds, for a run of a Python form through the run child of `check_model.py`,
    each seed the model's own code set (`Seeded`), and `seeds` the seeds the run used
    through desflow (`Seen.seeds` of `check_model.py`).
    """

    rc: int
    records: List[dict]
    stderr: str
    wall: float
    seeded: List["Seeded"]
    seeds: List[int]


class Env:
    """The prepared Python, the run's workspace and copies of it to run in."""

    def __init__(self, run_dir: Path, python_dir: Path, timeout: int, check_dir: Path):
        self.run_dir = run_dir
        self.venv = python_dir
        self.timeout = timeout
        self.python = str(python_dir / "bin" / "python")
        self.desflow = str(python_dir / "bin" / "desflow")
        # The Python the run child of check_model.py runs with, the same for every version
        self.check_python = str(check_dir / "bin" / "python")
        self.check_desflow = str(check_dir / "bin" / "desflow")
        self.cache: Dict[Tuple[Any, ...], Optional[FormRun]] = {}
        self.temps: List[Path] = []

    def copy(self, source: Optional[Path] = None) -> Path:
        """Makes a copy of the run folder, or of `source`, so that a run writes nothing into it."""
        tmp = Path(tempfile.mkdtemp(prefix="grade-"))
        self.temps.append(tmp)
        target = tmp / "run"
        shutil.copytree(
            source or self.run_dir,
            target,
            # Why: `py` is the run's own copy of the prepared Python, whose scripts name a
            # folder that no longer exists once the run is kept; `.claude` is Claude's state
            ignore=shutil.ignore_patterns(
                "__pycache__", ".pytest_cache", ".venv", "py", ".claude"
            ),
        )
        return target

    def close(self) -> None:
        """Deletes every copy."""
        for tmp in self.temps:
            shutil.rmtree(tmp, ignore_errors=True)

    def run(
        self,
        cmd: List[str],
        cwd: Path,
        timeout: Optional[int] = None,
        extra_env: Optional[Dict[str, str]] = None,
    ) -> Done:
        """Runs a command with the grading venv first on PATH; a timeout gives rc 124."""
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        env.update(extra_env or {})
        env["PATH"] = str(self.venv / "bin") + os.pathsep + env.get("PATH", "")
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        with tempfile.TemporaryFile("w+") as err:
            process = subprocess.Popen(
                cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=err, text=True
            )
            timer = threading.Timer(timeout or self.timeout, process.kill)
            timer.start()
            lines = []
            stamps = []
            assert process.stdout is not None
            for line in process.stdout:
                lines.append(line)
                if line.lstrip().startswith("{"):
                    stamps.append(time.monotonic())
            rc = process.wait()
            killed = not timer.is_alive()
            timer.cancel()
            err.seek(0)
            stderr = "timed out" if killed else err.read()
        wall = stamps[-1] - stamps[0] if stamps else 0.0
        return Done(124 if killed else rc, "".join(lines), stderr, wall)


def json_lines(text: str) -> List[dict]:
    """Returns each line of `text` that is a JSON object."""
    records = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if isinstance(value, dict):
                records.append(value)
    return records


class Loader(yaml.SafeLoader):
    """A safe loader that reads any `!tag` as null."""


Loader.add_multi_constructor("!", lambda loader, suffix, node: None)


def on_keys(node: Any) -> Any:
    """PyYAML reads a bare `on` key as true; turns it back into the text `on`."""
    if isinstance(node, dict):
        return {("on" if k is True else k): on_keys(v) for k, v in node.items()}
    if isinstance(node, list):
        return [on_keys(v) for v in node]
    return node


def model_document(path: Path) -> Optional[Any]:
    """Reads a blueprint with its `on` keys as text, or None when it does not parse."""
    try:
        return on_keys(yaml.load(path.read_text("utf-8"), Loader=Loader))
    except (OSError, yaml.YAMLError):
        return None


def has_run_end(document: Any) -> bool:
    """True when the blueprint ends its run itself: `run.until`, or a last segment's end."""
    run = document.get("run") if isinstance(document, dict) else None
    if not isinstance(run, dict):
        return False
    if run.get("until") is not None:
        return True
    segments = run.get("segments")
    last = segments[-1] if isinstance(segments, list) and segments else None
    return isinstance(last, dict) and (
        last.get("until") is not None or last.get("stop") is not None
    )


def yaml_command(
    env: Env, spec: dict, folder: Path, desflow: Optional[str] = None
) -> List[str]:
    """Returns the `desflow run` command of the YAML form in `folder`."""
    cmd = [desflow or env.desflow, "run", "model.yaml"]
    via = spec.get("via", "sample")
    if via == "sample":
        cmd += ["--sample", "100000"]
    elif via == "stdout":
        cmd += ["--stdout"]
    # Why: `--until` overrides the file's end, so a change to the end would not show
    if spec.get("until") is not None and not has_run_end(
        model_document(folder / "model.yaml")
    ):
        cmd += ["--until", str(spec["until"])]
    return cmd


def model_seed(document: Any) -> Optional[int]:
    """Returns `simulation.random_seed` of a blueprint when it is a number, else None."""
    simulation = document.get("simulation") if isinstance(document, dict) else None
    seed = simulation.get("random_seed") if isinstance(simulation, dict) else None
    return seed if isinstance(seed, int) and not isinstance(seed, bool) else None


def run_form(env: Env, spec: dict, form: str) -> Optional[FormRun]:
    """Runs one form as a reader runs it, once: `desflow run` or `python FILE`.

    Returns None when the file is missing.
    """
    key = (form, "plain", None)
    if key not in env.cache:
        result = None
        if (env.run_dir / FILES[form]).is_file():
            folder = env.copy()
            if form == "yaml":
                cmd = yaml_command(env, spec, folder)
            else:
                cmd = [env.python, FILES[form]]
            done = env.run(cmd, folder)
            result = FormRun(done.rc, json_lines(done.out), done.err, done.wall, [], [])
        env.cache[key] = result
    return env.cache[key]


def run_seeded(
    env: Env,
    spec: dict,
    form: str,
    seed: Optional[int],
    repeat: int = 0,
) -> Optional[FormRun]:
    """Runs one form at `seed`, set through desflow, or at its own seeds for None.

    The YAML form runs with `desflow run --seed`. A Python form runs in the run child of the
    repository's `scripts/skill/scripts/check_model.py`
    (`--run-form FILE REPORT SEED|-`), which sets the seed of each `SimulationContext` run
    and seeded `Sampler` the file makes, runs unpaced, and reports each seed the model's own
    code sets. `repeat` makes another run of the same settings. Both
    run with the check Python, so every version is run alike. Returns None when the file
    is missing.
    """
    key = (form, "seeded", seed, repeat)
    if key in env.cache:
        return env.cache[key]
    result = None
    if (env.run_dir / FILES[form]).is_file():
        folder = env.copy()
        report = folder.parent / "seen.json"
        if form == "yaml":
            cmd = yaml_command(env, spec, folder, env.check_desflow)
            cmd += ["--seed", str(seed)] if seed is not None else []
        else:
            cmd = [env.check_python, str(CHECK_MODEL), "--run-form", FILES[form]]
            cmd += [str(report), "-" if seed is None else str(seed)]
        done = env.run(cmd, folder)
        seeded: List[Seeded] = []
        seeds: List[int] = []
        if report.is_file():
            seen = json.loads(report.read_text("utf-8"))
            for where, call, sampler in seen["seeded"]:
                path, line = where.rsplit(":", 1)
                if folder.resolve() in Path(path).parents:
                    path = str(Path(path).relative_to(folder.resolve()))
                seeded.append(Seeded(f"{path}:{line}", call, sampler))
            for used in seen["used"] if seen["contexts"] else seen["samplers"]:
                if used is not None:
                    seeds.append(used)
        result = FormRun(
            done.rc, json_lines(done.out), done.err, done.wall, seeded, seeds
        )
    env.cache[key] = result
    return result


_CHECK_MODEL: List[Any] = []


def check_model() -> Any:
    """Returns `check_model.py` of the repository's current tree as a module, loaded once,
    so that a check reuses its rules (`_own_seeds`) rather than a copy."""
    if not _CHECK_MODEL:
        spec = importlib.util.spec_from_file_location("grade_check_model", CHECK_MODEL)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _CHECK_MODEL.append(module)
    return _CHECK_MODEL[0]


def form_ran(form: str, result: Optional[FormRun]) -> bool:
    """True when a form ran: exit 0, and for a Python form at least one record."""
    if result is None or result.rc != 0:
        return False
    return form == "yaml" or bool(result.records)


def plain_numbers(value: Any) -> Any:
    """Returns a value with each number as a float rounded to 6 places, so 1 equals 1.0."""
    if isinstance(value, dict):
        return {k: plain_numbers(v) for k, v in value.items()}
    if isinstance(value, list):
        return [plain_numbers(v) for v in value]
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return round(float(value), 6)
    return value


LAG_PATH = "system.simulation.lag_seconds"


def stream_records(records: List[dict]) -> List[str]:
    """Returns every record of the streams as comparable text, as `check_model.py` compares
    two runs of one file: status records included, the lag telemetry and the wall-clock
    `timestamp` left out."""
    rows = []
    for r in records:
        if "stream_type" not in r or r.get("path_id") == LAG_PATH:
            continue
        kept = {}
        for name, value in r.items():
            if name != "timestamp":
                kept[name] = value
        rows.append(json.dumps(kept, sort_keys=True))
    return rows


def canonical(records: List[dict]) -> List[str]:
    """Returns the event records without engine status records, as comparable text."""
    rows = []
    for r in records:
        if r.get("stream_type") != "event":
            continue
        value = r.get("value")
        if isinstance(value, dict) and value.get("status") in ENGINE_STATUS:
            continue
        rows.append(
            json.dumps(
                plain_numbers([r.get("sim_ts", 0), r.get("key"), r.get("task"), value]),
                sort_keys=True,
            )
        )
    return rows


class PythonFile(NamedTuple):
    """A Python file of the run: its text and its syntax tree (empty when it does not parse)."""

    text: str
    tree: ast.Module


class Score:
    """The evidence and the notes of each kind for one run."""

    def __init__(self) -> None:
        self.evidence: Dict[str, List[str]] = {kind: [] for kind in KINDS}
        self.notes: Dict[str, List[str]] = {kind: [] for kind in KINDS}
