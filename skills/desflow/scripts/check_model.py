"""Checks the files of a desflow model and compares the records of its forms.

Run it, do not read it:

    python check_model.py FILE... [--until T] [--timeout SECONDS]

Each FILE is one form of the model, in any order: a YAML blueprint, a Builder file
or a Low-level file. Give one form, or two or three. It needs only desflow. In
order, it:

1. checks every file with the function `desflow check` calls, and stops when it
   refuses one. A Python file refused only for having no module-level
   `SimulationContext` is left to its run, which shows whether it is a Low-level
   file. A YAML file is read with desflow's own loader, so `${VAR}` and `!include`
   are read as `desflow run` reads them;
2. reads the imports of each Python file: a module or a name from desflow that the
   skill's references do not document, a name that starts with `_`, and a name
   whose extra is not installed. Private attributes, such as `app._sinks`, are left
   to the reader, because only the code that runs knows what an object is;
3. runs each Python file as its reader runs it, `python FILE`, in a process of its
   own, with three changes made through desflow: no pacing, its sinks replaced by
   one JSON console (and one for labels and one for the trace when it has them),
   and, from the second run on, its seed replaced. The form of a Python file is
   what the run makes: a Builder file makes a `SimulationContext`, and a Low-level
   file makes a `DynamicRealtimeEnvironment` itself. Only the process `python
   FILE` starts is seen, so a model built and run in a child process is not. The
   seed is set and read through desflow: a `SimulationContext` run's seed, and
   each `Sampler` the file makes. The run also records each seed given to
   `random.Random`, `random.seed`, `numpy.random.seed`, `RandomState`,
   `default_rng`, a `SeedSequence` or NumPy's bit generators when the call
   stack holds a frame of the model's own code (a file that is not desflow's,
   the standard library's or an installed package's) with no desflow frame
   between it and the seeding. Each such place is a random generator the model
   seeds itself, rather than one desflow's `Sampler` made or was given, and is
   reported once with its count. A state set another way (`setstate`, the
   `state` setter, unpickling), a `SeedSequence` with a spawn key, a seeding
   function called through `functools.partial` or another callable with no
   Python code of the model's, a generator the model writes itself and one a C
   extension seeds are not found;
4. finds the model's seed: the YAML's `simulation.random_seed`, else the seed of
   the Builder run, else of the Low-level file's `Sampler`. A YAML file also needs
   a fixed `simulation.logical_start_time` and a run end. A run whose seed desflow
   drew is refused;
5. runs each Python file a second time at its own seed, so that a draw with no
   seed, such as Python's `random`, shows as two runs that differ. It compares the
   records of every form at the model's seed and the two after it with the
   YAML's, or with the Builder's when there is no YAML, and prints the first
   record that differs. The YAML runs with `desflow run FILE --sample 100000
   --seed S`, and `--until T` when it is given. One form alone runs at two seeds
   and is compared with nothing, to see that it runs and that its seed is used;
6. runs `desflow test` on the YAML, and on a Builder file with a `tests.yaml`
   beside it.

Each problem is printed with its file, and its line where it has one, and what to
write instead. It exits 1 when any step finds a problem, and prints `ok` lines for
the steps that ran.
"""

import argparse
import ast
import inspect
import json
import re
import runpy
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Set

import desflow

# The seed and the two after it: a form that draws differently can match at one
# seed by chance, and three seeds show it
RUNS_PER_FORM = 3
# More events than a model under test makes, so each run ends at its until
SAMPLE = 100000
# How long one unpaced run may take, in seconds, before it counts as endless
TIMEOUT = 300
# Readings of the wall clock, which differ between any two runs
LAG_PATH = "system.simulation.lag_seconds"
# The options that run one Python form and check one file; this script gives them
# to itself, so that the file's code runs in a process of its own
RUN_FORM = "--run-form"
CHECK_FILE = "--check-file"
# The skill's references, beside this script's folder. The source of the script in
# desflow's scripts/skill/ has no api.md, which scripts/gen_skill.py writes into
# skills/desflow/, so the source reads that folder's
REFERENCES = Path(__file__).resolve().parents[1] / "references"
if not (REFERENCES / "api.md").is_file():
    REFERENCES = (
        Path(__file__).resolve().parents[3] / "skills" / "desflow" / "references"
    )
BUILDER, LOW_LEVEL = "Builder", "Low-level"


class Problem(NamedTuple):
    """One thing to fix: where it is, and what is wrong with what to write instead.
    `where` is empty when the text already starts with the file."""

    where: str
    text: str


class Arguments(NamedTuple):
    """The command line, read."""

    yaml: Optional[Path]
    python: List[Path]
    until: Optional[str]
    timeout: float


class Seen(NamedTuple):
    """What one run of a Python file made, as its child process reports it.

    `contexts` counts the `SimulationContext` objects the file made, `given` holds
    the seed each context run was given (None when desflow drew one), `used` the
    seed each context run used, `environments` counts the environments the file
    made itself, `samplers` holds the seed of each `Sampler` the file made
    (None for one that draws nothing at random), and `seeded` holds each seed
    that code other than desflow set, as a `Seeded`."""

    contexts: int
    given: List[Optional[int]]
    used: List[Optional[int]]
    environments: int
    samplers: List[Optional[int]]
    seeded: List["Seeded"]

    def form(self) -> Optional[str]:
        """The form the run showed.

        Returns:
            Optional[str]: Builder when the file made a `SimulationContext`,
            Low-level when it made a `DynamicRealtimeEnvironment` itself, else None.
        """
        if self.contexts:
            return BUILDER
        if self.environments:
            return LOW_LEVEL
        return None

    def seeds(self) -> List[int]:
        """The seeds the run used, through desflow.

        Returns:
            List[int]: The seed of each context run of a Builder file, or of each
            seeded `Sampler` of a Low-level file, in the order they were made.
        """
        found = []
        for seed in self.used if self.contexts else self.samplers:
            if seed is not None:
                found.append(seed)
        return found


class Seeded(NamedTuple):
    """A seed the model's own code set: where, as its file and line, the call
    that took it, and whether a `Sampler` was given the generator it made."""

    where: str
    call: str
    sampler: bool


class Run(NamedTuple):
    """The records of one run, without the wall-clock readings, and what a Python
    file's run made (None for the YAML)."""

    records: List[Dict[str, Any]]
    seen: Optional[Seen]


class Checked(NamedTuple):
    """What `desflow check` found: the problems, the Python files it did not load
    because they define no module-level context, and the YAML's settings as
    desflow read them, or None when there is no YAML."""

    problems: List[Problem]
    unloaded: Set[Path]
    settings: Optional[Dict[str, Any]]


class FirstRuns(NamedTuple):
    """The forms the first runs showed, by kind, and the problems."""

    forms: Dict[str, "Form"]
    problems: List[Problem]


class Found(NamedTuple):
    """The model's seed, or None and why there is none."""

    seed: Optional[int]
    problems: List[Problem]


class RunFailed(Exception):
    """A run that failed or did not end, with why."""


class Form(NamedTuple):
    """One Python file, the form its first run showed, and that run."""

    path: Path
    kind: str
    first: Run


def main(argv: Optional[List[str]] = None) -> int:
    """Runs every step and prints its problems.

    Args:
        argv: The command line arguments, without the script's name.

    Returns:
        int: 1 when a step found a problem, else 0.
    """
    given = sys.argv[1:] if argv is None else argv
    if given[:1] == [RUN_FORM]:
        return _run_form(given[1:])
    if given[:1] == [CHECK_FILE]:
        return _check_one(given[1:])
    arguments = _arguments(given)
    files = ([arguments.yaml] if arguments.yaml else []) + arguments.python
    checked = _check_files(files)
    _report("desflow check", checked.problems)
    if checked.problems:
        return _not_compared()
    scanned: List[Problem] = []
    for path in arguments.python:
        scanned += _scan(path)
    first = FirstRuns({}, [])
    if arguments.python:
        _report("imports", scanned)
        first = _first_runs(arguments, checked.unloaded)
        _report("runs of the Python files", first.problems)
        if first.problems:
            return _not_compared()
    forms = first.forms
    found = _seed(arguments, forms, checked.settings)
    _report("seed and start time", found.problems)
    if found.seed is None:
        return _not_compared()
    compared = _compare(arguments, forms, found.seed)
    if len(forms) + (arguments.yaml is not None) > 1:
        _report("records of the forms", compared)
    else:
        _report("runs at two seeds", compared)
    tested = _test(arguments, forms)
    return 1 if scanned or compared or tested else 0


def _not_compared() -> int:
    print("The records were not compared: fix the problems above and run it again.")
    return 1


def _arguments(argv: List[str]) -> Arguments:
    parser = argparse.ArgumentParser(
        description="Check a desflow model and compare the records of its forms."
    )
    parser.add_argument("files", nargs="+", type=Path, metavar="FILE")
    parser.add_argument(
        "--until", metavar="T", help="the YAML's run end, such as 30 or '2 h'"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=TIMEOUT,
        metavar="SECONDS",
        help=f"how long one unpaced run may take (default {TIMEOUT})",
    )
    given = parser.parse_args(argv)
    blueprint = None
    python = []
    for path in given.files:
        if not path.is_file():
            parser.error(f"{path} is not a file")
        if path.suffix in (".yaml", ".yml"):
            if blueprint is not None:
                parser.error(f"{blueprint} and {path} are both YAML files; give one")
            blueprint = path
        elif path.suffix == ".py":
            python.append(path)
        else:
            parser.error(f"{path} is neither a YAML blueprint nor a Python file")
    return Arguments(blueprint, python, given.until, given.timeout)


def _report(step: str, problems: List[Problem]) -> None:
    if not problems:
        print(f"ok {step}")
        return
    for problem in problems:
        print(f"{problem.where}: {problem.text}" if problem.where else problem.text)


def _desflow() -> str:
    """The `desflow` command of the Python that runs this script."""
    found = shutil.which("desflow", path=str(Path(sys.executable).parent))
    return found or "desflow"


def _check_files(files: List[Path]) -> Checked:
    """Step 1: each file checked by `_check_one`, in a process of its own."""
    problems = []
    unloaded = set()
    settings = None
    for path in files:
        with tempfile.TemporaryDirectory() as folder:
            report = Path(folder) / "checked.json"
            done = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), CHECK_FILE]
                + [str(path), str(report)],
                capture_output=True,
                text=True,
            )
            if done.returncode != 0 or not report.is_file():
                text = done.stderr.strip()[-2000:] or "the check stopped"
                problems.append(Problem(str(path), f"desflow check failed:\n{text}"))
                continue
            checked = json.loads(report.read_text(encoding="utf-8"))
        if checked["no_context"]:
            unloaded.add(path)
            continue
        if checked["problem"] is None:
            settings = checked.get("settings", settings)
            continue
        for line in checked["problem"].splitlines():
            where = "" if line.startswith(str(path)) else str(path)
            problems.append(Problem(where, line))
    return Checked(problems, unloaded, settings)


def _check_one(argv: List[str]) -> int:
    """Checks one file with `desflow check`'s own function, and writes what it
    found to the report file. For a YAML file desflow accepts, it also writes the
    settings the comparison needs, read with desflow's loader: the seed, whether
    the run has an end, and the start time as the file gives it after `${VAR}` and
    `!include` are read, since the validated value of `now` is a time."""
    from desflow.blueprint import check_model

    file, report = Path(argv[0]), Path(argv[1])
    checked = check_model(file)
    found: Dict[str, Any] = {
        "problem": checked.problem,
        "no_context": checked.no_context,
    }
    if checked.context is not None and checked.run is not None:
        found["settings"] = {
            "seed": checked.context.random_seed,
            "start": checked.start,
            "ends": _has_end(checked.run),
        }
    report.write_text(json.dumps(found), encoding="utf-8")
    return 0


def _has_end(run: Any) -> bool:
    """Whether a blueprint's `run` section ends the run: an `until`, a `stop`, or
    a list of segments whose last entry has an `until` or a `stop`. A last entry
    `{}` runs to the end, which endless arrivals never reach."""
    if run.until is not None or run.stop is not None:
        return True
    segments = run.segments
    if not isinstance(segments, list) or not segments:
        return False
    return segments[-1].until is not None or segments[-1].stop is not None


def _documented() -> Dict[str, Optional[str]]:
    """Each import the skill's references document, as `module.name`, with the
    extra it needs, or None: the names of api.md's tables, and every full path
    such as `desflow.connectors.egress.kafka.ConfluentAvroSerializer`."""
    found: Dict[str, Optional[str]] = {}
    api = (REFERENCES / "api.md").read_text(encoding="utf-8")
    top, values = api.split("\n## Values\n", 1)
    for line in top.splitlines():
        cells = line.split("|")
        if len(cells) > 3 and cells[1].strip().startswith("`"):
            extra = cells[2].strip().strip("`")
            found[f"desflow.{cells[1].strip().strip('`')}"] = (
                None if extra == "none" else extra
            )
    for line in values.splitlines():
        cells = line.split("|")
        if len(cells) > 2 and cells[1].strip().startswith("`"):
            found[f"desflow.values.{cells[1].strip().strip('`')}"] = None
    for page in sorted(REFERENCES.glob("*.md")):
        for path in re.findall(r"`(desflow(?:\.\w+)+)`", page.read_text("utf-8")):
            found.setdefault(path, None)
    return found


def _scan(path: Path) -> List[Problem]:
    """Step 2: the imports from desflow that are not documented, private, or need an
    extra that is not installed."""
    documented = _documented()
    modules = set()
    for name in documented:
        modules.add(name.rsplit(".", 1)[0])
    problems = []
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        where = f"{path}:{getattr(node, 'lineno', 0)}"
        if isinstance(node, ast.ImportFrom) and _is_desflow(node.module or ""):
            for alias in node.names:
                text = _name_problem(node.module or "", alias.name, documented)
                if text:
                    problems.append(Problem(where, text))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _is_desflow(alias.name) and alias.name not in modules:
                    problems.append(Problem(where, _undocumented(alias.name)))
    return problems


def _is_desflow(module: str) -> bool:
    return module == "desflow" or module.startswith("desflow.")


def _name_problem(
    module: str, name: str, documented: Dict[str, Optional[str]]
) -> Optional[str]:
    """What is wrong with `from module import name`, or None."""
    full = f"{module}.{name}"
    if name.startswith("_"):
        return (
            f"{full} is a private name, which can change in any release; use a name "
            "references/api.md lists"
        )
    if full not in documented:
        return _undocumented(full)
    extra = documented[full]
    if module == "desflow" and extra and name not in desflow.__all__:
        return (
            f"{name} needs the {extra} extra, which is not installed: pip install "
            f'"desflow[{extra}]", and name the extra in the file\'s header'
        )
    return None


def _undocumented(name: str) -> str:
    return (
        f"{name} is not in desflow's documented API, which references/api.md lists "
        "and from desflow import gives; it can change in any release"
    )


def _first_runs(arguments: Arguments, unloaded: Set[Path]) -> FirstRuns:
    """Step 3: each Python file run once at its own seed."""
    forms: Dict[str, Form] = {}
    problems = []
    for path in arguments.python:
        try:
            run = _run_python(path, None, arguments.timeout)
        except RunFailed as failed:
            problems.append(Problem(str(path), f"python {path.name} failed:\n{failed}"))
            continue
        assert run.seen is not None
        kind = run.seen.form()
        if kind is None:
            problems.append(
                Problem(
                    str(path),
                    f"made no SimulationContext and no DynamicRealtimeEnvironment in "
                    f"the process python {path.name} starts; the script sees only "
                    "that process, so build and run the model there, not in a child "
                    "process",
                )
            )
        elif kind == BUILDER and path in unloaded:
            problems.append(
                Problem(
                    str(path),
                    "defines no SimulationContext at module level, so desflow cannot "
                    "load it: define the context at module level, such as "
                    "app = SimulationContext(...)",
                )
            )
        elif kind in forms:
            problems.append(
                Problem(
                    str(path), f"{forms[kind].path} is also the {kind} form; give one"
                )
            )
        elif not run.records:
            call = "app.run(until=...)" if kind == BUILDER else "env.run(until=...)"
            problems.append(
                Problem(
                    str(path),
                    f"printed no records when run with python {path.name}; call "
                    f'{call} under if __name__ == "__main__":, with a sink',
                )
            )
        else:
            forms[kind] = Form(path, kind, run)
    return FirstRuns(forms, problems)


def _seed(
    arguments: Arguments, forms: Dict[str, Form], settings: Optional[Dict[str, Any]]
) -> Found:
    """Step 4: the model's seed."""
    if arguments.yaml is not None:
        assert settings is not None
        return _yaml_settings(arguments.yaml, arguments.until, settings)
    first = forms.get(BUILDER) or forms[LOW_LEVEL]
    seen = first.first.seen
    assert seen is not None
    if first.kind == BUILDER and None in seen.given:
        drawn = ", ".join(str(seed) for seed in seen.used)
        return Found(
            None,
            [
                Problem(
                    str(first.path),
                    f"has no random_seed, so desflow drew one ({drawn}) and two runs "
                    "draw different numbers; write random_seed=42",
                )
            ],
        )
    seeds = seen.seeds()
    if not seeds:
        return Found(
            None,
            [
                Problem(
                    str(first.path),
                    "makes no Sampler with a seed, so its seed cannot be set; write "
                    "Sampler(seed=42)",
                )
            ],
        )
    return Found(seeds[0], [])


def _yaml_settings(path: Path, until: Optional[str], settings: Dict[str, Any]) -> Found:
    """The YAML's seed as desflow reads it, or why the records cannot be compared:
    no seed, no fixed start time, or no run end."""
    start = settings["start"]
    problems = []
    if settings["seed"] is None:
        problems.append(
            Problem(
                str(path),
                "simulation has no random_seed, so desflow draws one and two runs "
                "draw different numbers; write random_seed: 42",
            )
        )
    if start is None or start.strip().lower().startswith(("now", "-", "+")):
        problems.append(
            Problem(
                str(path),
                "simulation has no fixed logical_start_time, so the timestamps of two "
                "runs differ; write logical_start_time: '2026-01-05T09:00:00Z', and "
                "give the Builder and Low-level forms the same time",
            )
        )
    if until is None and not settings["ends"]:
        problems.append(
            Problem(
                str(path),
                "run has no end: give run.until, run.stop, or segments whose last "
                "entry has an until or a stop, or pass --until T",
            )
        )
    if problems:
        return Found(None, problems)
    return Found(int(settings["seed"]), [])


def _records(command: List[str], cwd: Path, timeout: float, path: Path) -> List[dict]:
    """The JSON records a command prints, without the wall-clock readings."""
    try:
        done = subprocess.run(
            command, capture_output=True, text=True, cwd=cwd, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise RunFailed(
            f"{path} did not end within {timeout:g} seconds, run unpaced; give the run "
            f"an end, such as until, or pass --timeout"
        ) from None
    if done.returncode != 0:
        raise RunFailed(done.stderr.strip()[-2000:])
    records = []
    for line in done.stdout.splitlines():
        if not line.startswith("{"):
            continue
        record = json.loads(line)
        if "stream_type" in record and record.get("path_id") != LAG_PATH:
            records.append(record)
    return records


def _run_yaml(path: Path, seed: int, until: Optional[str], timeout: float) -> Run:
    command = [_desflow(), "run", path.name, "--sample", str(SAMPLE)]
    command += ["--seed", str(seed)]
    if until is not None:
        command += ["--until", until]
    return Run(_records(command, path.parent.resolve(), timeout, path), None)


def _run_python(path: Path, seed: Optional[int], timeout: float) -> Run:
    """`python FILE` in a process of its own, with the changes of `_run_form`."""
    with tempfile.TemporaryDirectory() as folder:
        report = Path(folder) / "seen.json"
        command = [sys.executable, str(Path(__file__).resolve()), RUN_FORM]
        command += [str(path.resolve()), str(report)]
        command.append("-" if seed is None else str(seed))
        records = _records(command, path.parent.resolve(), timeout, path)
        found = json.loads(report.read_text(encoding="utf-8"))
        found["seeded"] = [Seeded(*entry) for entry in found["seeded"]]
        seen = Seen(**found)
    return Run(records, seen)


def _compare(arguments: Arguments, forms: Dict[str, Form], seed: int) -> List[Problem]:
    """Step 5: the records of every form at three seeds, against the first form's."""
    problems = _seeds_used(forms, seed) + _own_seeds(forms)
    problems += _unseeded_draws(forms, arguments.timeout)
    if problems:
        return problems
    runs = RUNS_PER_FORM if len(forms) + (arguments.yaml is not None) > 1 else 2
    for offset in range(runs):
        current = seed + offset
        try:
            streams = _streams(arguments, forms, current, offset)
        except RunFailed as failed:
            return [Problem("", f"a run at seed {current} failed:\n{failed}")]
        problems += _set_seeds(forms, streams, current, offset)
        names = list(streams)
        for name in names[1:]:
            problems += _difference(names[0], name, current, offset, streams)
        if problems:
            # The first seed that shows a difference is enough to fix
            return problems
    return problems


def _streams(
    arguments: Arguments, forms: Dict[str, Form], seed: int, offset: int
) -> Dict[Path, Run]:
    """Each form's run at `seed`: the YAML first, then the Builder, then the
    Low-level file. A Python file's first run is the one at its own seed."""
    streams: Dict[Path, Run] = {}
    if arguments.yaml is not None:
        streams[arguments.yaml] = _run_yaml(
            arguments.yaml, seed, arguments.until, arguments.timeout
        )
    for kind in (BUILDER, LOW_LEVEL):
        form = forms.get(kind)
        if form is None:
            continue
        if offset == 0:
            streams[form.path] = form.first
        else:
            streams[form.path] = _run_python(form.path, seed, arguments.timeout)
    return streams


def _seeds_used(forms: Dict[str, Form], seed: int) -> List[Problem]:
    """A Python file whose own run used another seed than the model's."""
    problems = []
    for form in forms.values():
        seen = form.first.seen
        assert seen is not None
        if form.kind == BUILDER and None in seen.given:
            problems.append(
                Problem(
                    str(form.path),
                    f"has no random_seed, so desflow drew one; write random_seed={seed}",
                )
            )
            continue
        for used in seen.seeds():
            if used != seed:
                problems.append(
                    Problem(
                        str(form.path),
                        f"runs at seed {used}, and the model's seed is {seed}; "
                        f"write {seed}",
                    )
                )
    return problems


def _own_seeds(forms: Dict[str, Form]) -> List[Problem]:
    """Each place where a Python file's own code seeds a random generator, rather
    than drawing from one desflow's `Sampler` made or was given, once with the
    number of seeds it set there."""
    problems = []
    for form in forms.values():
        seen = form.first.seen
        assert seen is not None
        counts: Dict[str, int] = {}
        calls: Dict[str, str] = {}
        for entry in seen.seeded:
            if not entry.sampler:
                counts[entry.where] = counts.get(entry.where, 0) + 1
                calls.setdefault(entry.where, entry.call)
        for where, count in counts.items():
            times = "1 seed" if count == 1 else f"{count} seeds"
            problems.append(
                Problem(
                    _place(form, where),
                    f"{calls[where]} seeds a random generator the model makes itself "
                    f"({times}), rather than one desflow's Sampler made; the skill's rule "
                    "is to draw only through desflow, so draw through "
                    'sampler.generator("<name>")',
                )
            )
    return problems


def _place(form: Form, where: str) -> str:
    """A file and line, relative to the form's folder when it is in it."""
    path, line = where.rsplit(":", 1)
    folder = form.path.parent.resolve()
    if folder in Path(path).parents:
        return f"{form.path.parent / Path(path).relative_to(folder)}:{line}"
    return where


def _unseeded_draws(forms: Dict[str, Form], timeout: float) -> List[Problem]:
    """A Python file whose two runs at its own seed differ, which only a draw with
    no seed gives."""
    problems = []
    for form in forms.values():
        try:
            again = _run_python(form.path, None, timeout)
        except RunFailed as failed:
            problems.append(
                Problem(str(form.path), f"its second run failed:\n{failed}")
            )
            continue
        problems += _differing_runs(
            form,
            again,
            "two runs at its own seed differ, so it draws numbers with no seed "
            "(Python's random or a NumPy generator with no seed); draw through "
            'sampler.generator("<name>") instead',
        )
    return problems


def _differing_runs(form: Form, again: Run, why: str) -> List[Problem]:
    """The first record where another run of a file at its own seed differs from
    its first run, with `why` that shows."""
    for index, (want, got) in enumerate(zip(form.first.records, again.records)):
        if want != got:
            return [
                Problem(
                    str(form.path),
                    f"{why}. Record {index} differs:\n"
                    f"  first: {json.dumps(want)}\n  again: {json.dumps(got)}",
                )
            ]
    if len(form.first.records) != len(again.records):
        return [
            Problem(
                str(form.path),
                f"{why}. The runs give {len(form.first.records)} and "
                f"{len(again.records)} records",
            )
        ]
    return []


def _set_seeds(
    forms: Dict[str, Form], streams: Dict[Path, Run], seed: int, offset: int
) -> List[Problem]:
    """A Python file whose seed the script could not set through desflow."""
    if offset == 0:
        return []
    problems = []
    for form in forms.values():
        seen = streams[form.path].seen
        assert seen is not None
        if seed not in seen.seeds():
            problems.append(
                Problem(
                    str(form.path),
                    "its seed could not be set: make its sampler with "
                    "Sampler(seed=42), or run a SimulationContext with random_seed",
                )
            )
    return problems


def _difference(
    first: Path, path: Path, seed: int, offset: int, streams: Dict[Path, Run]
) -> List[Problem]:
    """The first record where a form's stream differs from the first form's."""
    expected, found = streams[first].records, streams[path].records
    how = f"with its seed set to {seed}" if offset else "at its own seed"
    place = f"seed {seed} ({path.name} run {how})"
    for index, (want, got) in enumerate(zip(expected, found)):
        if want != got:
            fields = sorted(_differing_fields(want, got))
            return [
                Problem(
                    str(path),
                    f"differs from {first} at {place}, record {index} "
                    f"(fields {', '.join(fields)}):\n  {first}: "
                    f"{json.dumps(want)}\n  {path}: {json.dumps(got)}",
                )
            ]
    if len(expected) != len(found):
        shorter = path if len(found) < len(expected) else first
        return [
            Problem(
                str(path),
                f"at {place} {first} has {len(expected)} records and {path} has "
                f"{len(found)}; {shorter} ends first, so check the run end of both",
            )
        ]
    return []


def _differing_fields(want: Dict[str, Any], got: Dict[str, Any]) -> List[str]:
    """The fields two records hold different values in, one missing included."""
    fields = []
    for key in set(want) | set(got):
        if want.get(key) != got.get(key):
            fields.append(key)
    return fields


def _test(arguments: Arguments, forms: Dict[str, Form]) -> List[Problem]:
    """Step 6: `desflow test` on the YAML, and on the Builder file with tests.yaml."""
    paths = [] if arguments.yaml is None else [arguments.yaml]
    builder = forms.get(BUILDER)
    if builder is not None and (builder.path.parent / "tests.yaml").is_file():
        paths.append(builder.path)
    if not paths:
        print(
            "desflow test not run: there is no YAML, and no tests.yaml beside a Builder file"
        )
        return []
    problems = []
    for path in paths:
        done = subprocess.run(
            [_desflow(), "test", str(path)], capture_output=True, text=True
        )
        if done.returncode != 0:
            text = (done.stdout + done.stderr).strip()
            problems.append(Problem(str(path), f"desflow test failed:\n{text}"))
    _report("desflow test", problems)
    return problems


def _run_form(argv: List[str]) -> int:
    """Runs one Python file as `python FILE` runs it, with the changes of
    `_watch`, and writes what it made to the report file. The script runs this in
    a process of its own for each run."""
    file, report = Path(argv[0]), Path(argv[1])
    seed = None if argv[2] == "-" else int(argv[2])
    seen: Dict[str, Any] = {
        "contexts": 0,
        "given": [],
        "used": [],
        "environments": 0,
        "samplers": [],
        "seeded": [],
    }
    _watch(seen, seed)
    # As `python FILE` sets them: the file's folder first on the path, and its name
    sys.path[0] = str(file.parent)
    sys.argv = [str(file)]
    try:
        runpy.run_path(str(file), run_name="__main__")
    finally:
        seeded = [[r.where, r.call, r.sampler] for r in seen["seeded"]]
        report.write_text(json.dumps({**seen, "seeded": seeded}), encoding="utf-8")
    return 0


def _watch(seen: Dict[str, Any], seed: Optional[int]) -> None:
    """Wraps desflow's classes so that the run records what the file makes, runs
    unpaced with JSON consoles, and runs at `seed` when one is given. The seeds the
    model's own code sets are recorded (`_Seeds`).

    A `SimulationContext` run starts through `start`, which builds its environment
    and its `Sampler`; what is made while it runs belongs to the context, not to
    the file."""
    from desflow.blueprint import Preview, preview_builder

    seeds = _Seeds()
    seen["seeded"] = seeds.records
    seeds.watch()

    context = desflow.SimulationContext
    environment = desflow.DynamicRealtimeEnvironment
    sampler = desflow.Sampler
    inside = [0]
    files_own: Set[int] = set()
    previewed: Set[int] = set()

    made = context.__init__

    def init_context(self: Any, *args: Any, **kwargs: Any) -> None:
        seen["contexts"] += 1
        made(self, *args, **kwargs)

    start = context.start

    def start_context(self: Any, *args: Any, **kwargs: Any) -> Any:
        if seed is not None:
            self.random_seed = seed
            kwargs["seed"] = seed
        given = kwargs.get("seed")
        seen["given"].append(self.random_seed if given is None else given)
        if id(self) not in previewed:
            previewed.add(id(self))
            preview_builder(self, Preview(sample=SAMPLE))
        inside[0] += 1
        try:
            return start(self, *args, **kwargs)
        finally:
            inside[0] -= 1
            used = self.sampler.seed if self.sampler is not None else None
            seen["used"].append(used)

    built = environment.__init__
    built_signature = inspect.signature(built)

    def init_environment(self: Any, *args: Any, **kwargs: Any) -> None:
        if inside[0]:
            built(self, *args, **kwargs)
            return
        seen["environments"] += 1
        files_own.add(id(self))
        bound = built_signature.bind(self, *args, **kwargs)
        bound.arguments.update({"factor": 0, "go_live_at": None, "strict": None})
        built(*bound.args, **bound.kwargs)

    setup = environment.setup_egress
    setup_signature = inspect.signature(setup)

    def setup_egress(self: Any, providers: List, *args: Any, **kwargs: Any) -> Any:
        if id(self) not in files_own:
            return setup(self, providers, *args, **kwargs)
        bound = setup_signature.bind(self, providers, *args, **kwargs)
        labelled = sum(map(bool, bound.arguments.get("labels") or []))
        traced = sum(map(bool, bound.arguments.get("traces") or []))
        consoles = []
        for _ in range(1 + labelled + traced):
            consoles.append(desflow.ConsoleEgress(format="json"))
        bound.arguments["providers"] = consoles
        bound.arguments["labels"] = [False] + [True] * labelled + [False] * traced
        bound.arguments["traces"] = [False] * (1 + labelled) + [True] * traced
        for name in ("predicates", "batch_sizes", "flush_intervals", "envelopes"):
            bound.arguments.pop(name, None)
        return setup(*bound.args, **bound.kwargs)

    drawn = sampler.__init__

    def init_sampler(self: Any, *args: Any, **kwargs: Any) -> None:
        drawn(self, *args, **kwargs)
        if inside[0]:
            return
        # A generator given to a Sampler holds the model's seed
        seeds.given_to_sampler(self.rng)
        if seed is not None and self.seed is not None:
            drawn(self, seed=seed)
        seen["samplers"].append(self.seed)

    context.__init__ = init_context  # type: ignore[method-assign]
    context.start = start_context  # type: ignore[method-assign]
    environment.__init__ = init_environment  # type: ignore[method-assign]
    environment.setup_egress = setup_egress  # type: ignore[method-assign]
    sampler.__init__ = init_sampler  # type: ignore[method-assign]


class _Record:
    """A seed the model's own code set, which `given_to_sampler` can still mark."""

    def __init__(self, where: str, call: str) -> None:
        self.where = where
        self.call = call
        self.sampler = False


class _Made(NamedTuple):
    """An object a recorded seed made, kept so that its id stays its own, and the
    record of that seed."""

    made: Any
    record: _Record


class _Seeds:
    """Records each seed given to `random.Random`, `random.seed`,
    `numpy.random.seed`, `RandomState`, `default_rng`, a `SeedSequence` that a
    bit generator takes, or one of NumPy's bit generators, when the call stack
    holds a frame of the model's own code with no desflow frame between it and
    the seeding. That frame's file and line is the record's place. So the
    generators desflow's `Sampler` makes pass, and so do their copies, pickles
    and spawns, which take no seed. A state set another way (`setstate`, the
    `state` setter, unpickling), a `SeedSequence` with a spawn key, a seeding
    function called through `functools.partial` or another callable with no
    Python code of the model's, a generator the model writes itself and one a C
    extension seeds are not seen."""

    def __init__(self) -> None:
        import random
        import site
        import sysconfig

        import numpy as np

        self.records: List[_Record] = []
        self.made: Dict[int, _Made] = {}
        self.passed = {Path(__file__).resolve(), Path(random.__file__).resolve()}
        self.numpy = Path(np.__file__).resolve().parent
        self.desflow = Path(desflow.__file__).resolve().parent
        installed = set()
        for name in ("stdlib", "platstdlib", "purelib", "platlib"):
            installed.add(Path(sysconfig.get_paths()[name]).resolve())
        for folder in site.getsitepackages() + [site.getusersitepackages()]:
            installed.add(Path(folder).resolve())
        self.installed = installed

    def _is_installed(self, path: Path) -> bool:
        return path in self.installed or any(f in self.installed for f in path.parents)

    def _is_outside(self, path: Path) -> bool:
        return (
            path in self.passed
            or self.numpy in path.parents
            or self._is_installed(path)
        )

    def place(self) -> Optional[str]:
        """The file and line of the model's own frame nearest the seeding, or
        None when a desflow frame or an import comes first or no model frame led
        to it. Code run with `exec` from a model file counts as that file's: its
        place is the nearest frame of the file itself, else the file with the
        line inside the string."""
        frame: Any = sys._getframe(1)
        in_string: Optional[str] = None
        while frame is not None:
            name = frame.f_code.co_filename
            if name.startswith("<frozen importlib"):
                # An import runs the imported module's code, not the model's line
                return in_string
            if name.startswith("<"):
                module = frame.f_globals.get("__file__")
                if in_string is None and isinstance(module, str):
                    path = Path(module).resolve()
                    if self.desflow not in path.parents and not self._is_outside(path):
                        in_string = f"{path}:{frame.f_lineno}"
                frame = frame.f_back
                continue
            path = Path(name).resolve()
            if self.desflow in path.parents:
                return in_string
            if not self._is_outside(path):
                return f"{path}:{frame.f_lineno}"
            frame = frame.f_back
        return in_string

    def seed(self, value: Any, made: Any, call: str) -> Any:
        """Records a seed the model's own code sets, and returns it as it is."""
        if not _is_a_seed(value):
            return value
        where = self.place()
        if where is not None:
            record = _Record(where, call)
            self.records.append(record)
            self.made[id(made)] = _Made(made, record)
        return value

    def given_to_sampler(self, rng: Any) -> None:
        """Marks the seed of a generator given to a `Sampler`: it is the
        Sampler's, which the script's runs set."""
        if rng is None:
            return
        entry = self.made.get(id(rng.bit_generator))
        if entry is not None and entry.made is rng.bit_generator:
            entry.record.sampler = True

    def watch(self) -> None:
        """Replaces the places a seed is set with ones that call `seed` first."""
        import random

        import numpy as np
        import numpy.random._generator as generator_module
        import numpy.random.mtrand as mtrand

        seeds = self
        random_seed = random.Random.seed

        def seed_random(self: Any, a: Any = None, version: int = 2) -> None:
            call = "random.seed" if self is random._inst else "random.Random"  # type: ignore[attr-defined]
            random_seed(self, seeds.seed(a, self, call), version)

        random.Random.seed = seed_random  # type: ignore[method-assign]
        # The module's functions are the methods of one Random
        random.seed = random._inst.seed  # type: ignore[attr-defined]
        numpy_seed = np.random.seed

        def seed_numpy(seed: Any = None) -> None:
            numpy_seed(seeds.seed(seed, numpy_seed, "numpy.random.seed"))

        np.random.seed = seed_numpy
        mtrand.seed = seed_numpy  # type: ignore[attr-defined]
        for name in ("PCG64", "PCG64DXSM", "Philox", "SFC64", "MT19937"):
            replaced = _recording(getattr(np.random, name), self)
            setattr(np.random, name, replaced)
            if hasattr(generator_module, name):
                # default_rng makes its PCG64 through this module's name
                setattr(generator_module, name, replaced)
            if name == "MT19937":
                # RandomState makes its MT19937 through this name
                mtrand._MT19937 = replaced  # type: ignore[attr-defined]


def _is_a_seed(value: Any) -> bool:
    """Whether a value given as a seed is one: not None, and not a generator, a
    bit generator or a `SeedSequence` made by a spawn or a `Sampler`, which
    carry a seed made elsewhere. A `SeedSequence` with no spawn key holds a
    seed of its own."""
    import numpy as np

    if value is None:
        return False
    if isinstance(value, np.random.bit_generator.SeedSequence):
        return not value.spawn_key
    return not isinstance(value, (np.random.BitGenerator, np.random.Generator))


class _SameType(type):
    """The type of a replaced NumPy class: its base's objects are its own, so an
    `isinstance` check against the replaced name still holds for them. A model's
    own subclass of the replaced class checks as any class does."""

    def __instancecheck__(cls, instance: Any) -> bool:
        base = cls.__dict__.get("_replaces")
        if base is None:
            return type.__instancecheck__(cls, instance)
        return isinstance(instance, base)

    def __subclasscheck__(cls, subclass: Any) -> bool:
        base = cls.__dict__.get("_replaces")
        if base is None:
            return type.__subclasscheck__(cls, subclass)
        return issubclass(subclass, base)


def _recording(base: Any, seeds: _Seeds) -> Any:
    """A subclass of a NumPy bit generator whose seed goes through `seeds.seed`
    when it is made, and, for `MT19937`, when `RandomState` seeds it. NumPy's
    own modules keep their names, and the subclass keeps its base's module and
    name; it, and each object it makes, pickles as NumPy's class, through
    `copyreg`."""
    import copyreg

    call = f"numpy.random.{base.__name__}"

    class Recording(base, metaclass=_SameType):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            if args:
                args = (seeds.seed(args[0], self, call),) + args[1:]
            elif "seed" in kwargs:
                kwargs["seed"] = seeds.seed(kwargs["seed"], self, call)
            super().__init__(*args, **kwargs)

        def _legacy_seeding(self, seed: Any) -> None:
            super()._legacy_seeding(seeds.seed(seed, self, "numpy.random.RandomState"))

    Recording.__name__ = Recording.__qualname__ = base.__name__
    Recording.__module__ = base.__module__
    Recording._replaces = base  # type: ignore[attr-defined]

    def reduce(made: Any) -> Any:
        reduced = base.__reduce__(made)
        parts = tuple(base if part is Recording else part for part in reduced[1])
        return (reduced[0], parts) + tuple(reduced[2:])

    copyreg.pickle(Recording, reduce)
    copyreg.pickle(_SameType, _reduce_class)
    return Recording


def _reduce_class(cls: Any) -> Any:
    """Pickles a replacing class as the NumPy class it replaces, which pickle
    cannot find by the name the two share; a model's own subclass pickles by
    its name, as any class does."""
    import copy

    base = cls.__dict__.get("_replaces")
    if base is None:
        return cls.__qualname__
    return (copy.copy, (base,))


if __name__ == "__main__":
    sys.exit(main())
