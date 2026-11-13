"""The desflow skill in `skills/desflow/` follows the code and the Agent Skills
specification (https://agentskills.io/specification).

`scripts/gen_skill.py` writes the folder. These tests fail when the committed folder
differs from its output, when it leaves out a section, a distribution or a connector,
when its frontmatter breaks the specification, or when it stops teaching a study.
"""

import ast
import importlib.util
import inspect
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

from desflow.blueprint.build import EGRESS_TYPES, INGRESS_TYPES
from desflow.models.distributions import DISTRIBUTIONS

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "gen_skill", _ROOT / "scripts" / "gen_skill.py"
)
assert _spec is not None and _spec.loader is not None
gen_skill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_skill)

SKILL = _ROOT / "skills" / "desflow"


def _frontmatter(text: str) -> dict:
    match = re.match(r"---\n(.*?)\n---\n", text, re.S)
    assert match, "SKILL.md must start with YAML frontmatter"
    return yaml.safe_load(match.group(1))


def test_committed_skill_is_current():
    for name, text in gen_skill.files().items():
        assert (SKILL / name).read_text(encoding="utf-8") == text, (
            f"skills/desflow/{name} is out of date; run "
            "`uv run --all-extras python scripts/gen_skill.py`"
        )
    committed = {
        p.relative_to(SKILL).as_posix() for p in SKILL.rglob("*") if p.is_file()
    }
    assert committed == set(gen_skill.files())


def test_skill_names_every_section_distribution_and_connector():
    skill = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    for key in gen_skill.SECTIONS:
        assert f"| `{key}` |" in skill
    distributions = (SKILL / "references" / "distributions.md").read_text(
        encoding="utf-8"
    )
    for name in DISTRIBUTIONS:
        assert f"\n## {name}\n" in distributions
    connectors = (SKILL / "references" / "connectors.md").read_text(encoding="utf-8")
    for name in {*INGRESS_TYPES, *EGRESS_TYPES}:
        assert f"| `{name}` |" in connectors


def test_skill_names_every_serializer_and_deserializer():
    """The skill's serializer table is hand-kept, so a new class fails here."""
    import desflow.connectors.egress.kafka as egress
    import desflow.connectors.ingress.kafka as ingress

    text = (SKILL / "references" / "connectors.md").read_text(encoding="utf-8")
    for module, suffix in ((egress, "Serializer"), (ingress, "Deserializer")):
        for name in dir(module):
            if name.startswith(("Confluent", "Glue")) and name.endswith(suffix):
                assert f"`{module.__name__}.{name}`" in text, name


def test_skill_frontmatter():
    text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    meta = _frontmatter(text)
    name = meta["name"]
    assert name == SKILL.name
    assert 1 <= len(name) <= 64
    assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", name)
    assert 1 <= len(meta["description"]) <= 1024
    assert meta["license"] == "MIT"
    assert 1 <= len(meta["compatibility"]) <= 500
    assert meta["metadata"] == {"desflow-version": gen_skill.version()}
    assert gen_skill.version() in meta["compatibility"]
    assert len(text.splitlines()) < 500


def test_skill_teaches_a_study():
    skill = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "## Run a study, not a single run" in skill
    study = skill.split("## Run a study, not a single run")[1].split("\n## ")[0].lower()
    for word in ("seed", "warm", "replications", "confidence interval"):
        assert word in study


def test_skill_teaches_a_description():
    skill = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "## Draft a model from a description" in skill
    section = (
        skill.split("## Draft a model from a description")[1].split("\n## ")[0].lower()
    )
    for word in ("assumed", "stages", "ask", "load", "attempts"):
        assert word in section


def test_skill_reference_links_exist():
    for page in SKILL.rglob("*.md"):
        for target in re.findall(
            r"\]\(([^)#\s]+)(?:#[^)]*)?\)", page.read_text(encoding="utf-8")
        ):
            if "://" in target:
                continue
            assert (page.parent / target).resolve().exists(), f"{page}: {target}"


# The local example's three files, with what the script asks for before it compares
# them: a fixed logical_start_time in each form. The Low-level form stays paced and
# with the log console, which the script replaces. Each pair is the text replaced and
# its replacement
_START = "datetime(2026, 1, 5, 9, tzinfo=timezone.utc)"
_FIXED = {
    "yaml/local.yaml": [
        (
            "simulation: {sim_id: Shop, factor: 1.0, random_seed: 42}",
            "simulation:\n  sim_id: Shop\n  factor: 1.0\n  random_seed: 42\n"
            '  logical_start_time: "2026-01-05T09:00:00Z"',
        )
    ],
    "builder/local_example.py": [
        (
            "import logging\n",
            "import logging\nfrom datetime import datetime, timezone\n",
        ),
        ("random_seed=42)", f"random_seed=42, logical_start_time={_START})"),
    ],
    "low-level/local_example.py": [
        (
            "import logging\n",
            "import logging\nfrom datetime import datetime, timezone\n",
        ),
        (
            "DynamicRealtimeEnvironment(factor=1.0, strict=False)",
            "DynamicRealtimeEnvironment(factor=1.0, strict=False, "
            f"logical_start_time={_START})",
        ),
    ],
}
_NAMES = ("model.yaml", "builder.py", "low_level.py")
# A changed copy of one form, and the text the script must print for it
_BROKEN = {
    "private import": (
        "builder.py",
        "from desflow import ConsoleEgress, SimulationContext\n",
        "from desflow import ConsoleEgress, SimulationContext\n"
        "from desflow.builder.task import NO_RECORD  # noqa: F401\n",
        "desflow.builder.task.NO_RECORD is not in desflow's documented API",
    ),
    "hand-built distribution": (
        "low_level.py",
        "sampler.sample(env.registry.get_config(path_id))",
        'sampler.sample(DistributionConfig(dist="normal", mean=3.0, std=0.5))',
        "model.yaml at seed 42 (low_level.py run at its own seed)",
    ),
    "seed in the main block": (
        "builder.py",
        "    app.run(until=30)",
        "    app.random_seed = 7\n    app.run(until=30)",
        "runs at seed 7, and the model's seed is 42",
    ),
    "run end in the main block": (
        "builder.py",
        "    app.run(until=30)",
        "    app.run(until=20)",
        "builder.py ends first",
    ),
    # Equal at the file's seed, and not at the next one, which only a run at a
    # second seed shows
    # Python's random is seeded by nothing desflow sets, so two runs differ
    "unseeded draw": (
        "low_level.py",
        '"order_id": task_id}',
        '"order_id": task_id, "share": __import__("random").random()}',
        "two runs at its own seed differ, so it draws numbers with no seed",
    ),
    "draws at another seed": (
        "low_level.py",
        "    sampler = Sampler(rng=np.random.default_rng(42))\n",
        "    sampler = Sampler(rng=np.random.default_rng(42))\n"
        "    if sampler.seed == 43:\n"
        '        env.registry.update("Shop.resources.packers.current_cap", 2)\n',
        "at seed 43 (low_level.py run with its seed set to 43)",
    ),
}
_HISTORY_YAML = """\
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42
  logical_start_time: "2026-01-05T09:00:00Z"

arrivals:
  orders: {dist: exponential, rate: 1.0, spawn: order}
  reports: {dist: constant, value: 10, spawn: report}

tasks:
  order:
    payload: {size: 1}
  report:
    vars:
      orders: {gen: aggregate, task: order, function: count, last: 100}
    payload:
      orders: {gen: var, name: orders}

run: {until: 30}
"""
_HISTORY_BUILDER = """\
from datetime import datetime, timezone

from desflow import SimulationContext
from desflow.values import Template

app = SimulationContext(
    sim_id="Shop",
    factor=0,
    random_seed=42,
    logical_start_time=datetime(2026, 1, 5, 9, tzinfo=timezone.utc),
)
app.add_arrival("orders", dist="exponential", rate=1.0, spawn="order")
app.add_arrival("reports", dist="constant", value=10, spawn="report")


@app.task(name="order", history=5)
def order(task_id):
    return {"size": 1}


@app.task(
    name="report",
    vars={"orders": {"gen": "aggregate", "task": "order", "function": "count", "last": 100}},
)
def report(task_id):
    return Template({"orders": {"gen": "var", "name": "orders"}}, name="tasks.report")


if __name__ == "__main__":
    app.run(until=30)
"""


def _local_copy(folder, fixed=True):
    """The local example's three files in `folder` as model.yaml, builder.py and
    low_level.py, with the changes of `_FIXED` when `fixed`."""
    for source, name in zip(_FIXED, _NAMES, strict=True):
        text = (_ROOT / "examples" / source).read_text(encoding="utf-8")
        for old, new in _FIXED[source] if fixed else []:
            assert text.count(old) == 1, (source, old)
            text = text.replace(old, new)
        (folder / name).write_text(text, encoding="utf-8")
    return [folder / name for name in _NAMES]


def _change(path, old, new):
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1, (path, old)
    path.write_text(text.replace(old, new), encoding="utf-8")


def _check_model(files, *options, env=None):
    """The script run on `files` as the skill tells an assistant to run it, with
    `env` as its environment when one is given."""
    script = SKILL / "scripts" / "check_model.py"
    return subprocess.run(
        [sys.executable, str(script), *map(str, files), *options],
        capture_output=True,
        text=True,
        cwd=files[0].parent,
        env=env,
    )


def test_check_model_passes_on_the_local_example(tmp_path):
    """The three forms of the local example give the same records at three seeds
    once each has a fixed start time, which also shows that apis.md's claim holds.
    The Low-level file is paced at factor 1.0 for 30 seconds three times, so the
    time limit shows that the script runs it unpaced, as it runs the YAML."""
    started = time.monotonic()
    done = _check_model(_local_copy(tmp_path), "--timeout", "20")
    assert done.returncode == 0, done.stdout + done.stderr
    assert "ok records of the forms" in done.stdout
    assert time.monotonic() - started < 90


def test_check_model_refuses_a_model_without_a_fixed_start(tmp_path):
    done = _check_model(_local_copy(tmp_path, fixed=False))
    assert done.returncode == 1
    assert "no fixed logical_start_time" in done.stdout
    assert "The records were not compared" in done.stdout


@pytest.mark.parametrize("case", sorted(_BROKEN))
def test_check_model_finds_a_changed_form(tmp_path, case):
    name, old, new, message = _BROKEN[case]
    files = _local_copy(tmp_path)
    _change(tmp_path / name, old, new)
    done = _check_model(files)
    assert done.returncode == 1
    assert message in done.stdout, done.stdout


@pytest.mark.parametrize(
    "pair", [("model.yaml", "low_level.py"), ("low_level.py", "builder.py")]
)
def test_check_model_takes_any_two_forms_in_any_order(tmp_path, pair):
    """Each file's form is found from what it is, not from its place."""
    _local_copy(tmp_path)
    done = _check_model([tmp_path / name for name in pair])
    assert done.returncode == 0, done.stdout + done.stderr
    assert "ok records of the forms" in done.stdout


def test_check_model_finds_a_different_history(tmp_path):
    """The Builder keeps 5 rows of `order` where the YAML keeps the default, so the
    report's count differs."""
    (tmp_path / "model.yaml").write_text(_HISTORY_YAML, encoding="utf-8")
    (tmp_path / "builder.py").write_text(_HISTORY_BUILDER, encoding="utf-8")
    done = _check_model([tmp_path / "model.yaml", tmp_path / "builder.py"])
    assert done.returncode == 1
    assert 'builder.py: {"stream_type": "event"' in done.stdout
    assert '"value": {"orders": 5}' in done.stdout


@pytest.mark.parametrize(
    "name, old, new, message",
    [
        ("model.yaml", "resources:", "resourcez:", r":\d+: resourcez"),
        ("low_level.py", "def run():", "def run()):", r": unmatched '\)'"),
    ],
)
def test_check_model_stops_after_a_refused_file(tmp_path, name, old, new, message):
    """A file that desflow check refuses, a Python syntax error included, is
    reported as desflow reports it, and the script stops there rather than failing
    on the same file."""
    files = _local_copy(tmp_path)
    _change(tmp_path / name, old, new)
    done = _check_model(files)
    assert done.returncode == 1
    assert "Traceback" not in done.stdout + done.stderr
    assert "The records were not compared" in done.stdout
    path = str(tmp_path / name)
    assert f"{path}: {path}" not in done.stdout
    assert re.search(rf"^{re.escape(path)}{message}", done.stdout, re.M), done.stdout


def test_check_model_refuses_segments_that_run_to_the_end(tmp_path):
    """A last segment `{}` runs to the end, which endless arrivals never reach, so
    it is no run end; a last segment with an `until` is one."""
    model = _local_copy(tmp_path)[0]
    _change(model, "run: {until: 30}", "run:\n  segments:\n    - {until: 10}\n    - {}")
    done = _check_model([model])
    assert done.returncode == 1
    assert "run has no end" in done.stdout, done.stdout
    _change(model, "    - {}", "    - {until: 30}")
    done = _check_model([model])
    assert done.returncode == 0, done.stdout + done.stderr


def test_check_model_stops_an_endless_run(tmp_path):
    files = _local_copy(tmp_path)
    _change(files[2], "env.run(until=30)", "env.run()")
    done = _check_model(files, "--timeout", "5")
    assert done.returncode == 1
    assert "low_level.py did not end within 5 seconds" in done.stdout, done.stdout


_RUNS_OUT = """\
import datetime

from desflow import ConsoleEgress, SimulationContext

app = SimulationContext(
    sim_id="Out",
    factor=0,
    random_seed=42,
    logical_start_time=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
).add_egress(ConsoleEgress(format="json"))
app.add_arrival("a", gap=1, max_events=3, spawn="job")


@app.task(name="job", service_time=2)
def job(task_id):
    return {"event_type": "done"}


if __name__ == "__main__":
    app.run()
"""


def test_check_model_runs_a_builder_with_no_run_end_until_no_event_is_left(tmp_path):
    """A Builder run with no end stops when no event is left, as `python builder.py`
    and `desflow run builder.py --sample` stop it, rather than with SimPy's error
    that the sample's end event was never reached."""
    builder = tmp_path / "builder.py"
    builder.write_text(_RUNS_OUT, encoding="utf-8")
    done = _check_model([builder])
    assert done.returncode == 0, done.stdout + done.stderr
    assert "ok runs of the Python files" in done.stdout, done.stdout
    assert "No scheduled events left" not in done.stdout + done.stderr


def test_check_model_fails_a_builder_whose_own_stop_is_never_reached(tmp_path):
    """A stop of the file's own that never fires fails `python builder.py`, so the
    script fails it too, though a run with no end passes."""
    builder = tmp_path / "builder.py"
    text = _RUNS_OUT.replace(
        'app.add_arrival("a"', 'app.add_signal("never")\napp.add_arrival("a"'
    ).replace("    app.run()", '    app.run(stop="never")')
    assert 'stop="never"' in text and 'add_signal("never")' in text
    builder.write_text(text, encoding="utf-8")
    done = _check_model([builder])
    assert done.returncode == 1, done.stdout + done.stderr
    assert "No scheduled events left" in done.stdout, done.stdout


def test_check_model_reads_the_start_and_seed_as_desflow_does(tmp_path):
    """`NOW` and `${START:-now}` are `now`, so they are refused; a `!python` seed
    is read, not refused."""
    files = _local_copy(tmp_path)
    _change(files[0], '"2026-01-05T09:00:00Z"', "NOW")
    done = _check_model([files[0]])
    assert done.returncode == 1
    assert "no fixed logical_start_time" in done.stdout
    # A variable is read as desflow reads it, so its default `now` is refused
    _change(files[0], "NOW", "${START:-now}")
    done = _check_model([files[0]])
    assert done.returncode == 1
    assert "no fixed logical_start_time" in done.stdout, done.stdout
    _change(files[0], "${START:-now}", '"2026-01-05T09:00:00Z"')
    _change(files[0], "random_seed: 42", "random_seed: !python seed.SEED")
    (tmp_path / "seed.py").write_text("SEED = 42\n", encoding="utf-8")
    done = _check_model([files[0], files[1]])
    assert done.returncode == 0, done.stdout + done.stderr


@pytest.mark.parametrize(
    "egress, hidden",
    [
        ("ParquetStorageEgress(default_path='out')", "pyarrow"),
        ("IcebergStorageEgress(catalog=None, default_table='db.t')", "pyiceberg"),
    ],
)
def test_check_model_stops_on_a_sink_whose_extra_is_missing(tmp_path, egress, hidden):
    """A storage connector refuses to be built without its extra, so desflow check
    of the file names the extra, and the script stops there."""
    builder = tmp_path / "builder.py"
    builder.write_text(
        f"from desflow import {egress.split('(')[0]}, SimulationContext\n\n"
        "app = SimulationContext(sim_id='Shop', factor=0, random_seed=42)\n"
        f"app.add_egress({egress})\n",
        encoding="utf-8",
    )
    # Python imports sitecustomize from the path as it starts, so every process the
    # script starts sees the package as missing; find_spec returns None for a
    # module that sys.modules holds as None
    hide = tmp_path / "hide"
    hide.mkdir()
    (hide / "sitecustomize.py").write_text(
        f"import sys\nsys.modules[{hidden!r}] = None\n", encoding="utf-8"
    )
    done = _check_model([builder], env={**os.environ, "PYTHONPATH": str(hide)})
    assert done.returncode == 1
    assert f"needs {hidden}, which is not installed" in done.stdout, done.stdout
    assert "The records were not compared" in done.stdout


@pytest.mark.parametrize(
    "folder, files",
    [
        ("alias", ["model.yaml", "builder.py"]),
        ("helper", ["model.yaml", "builder.py"]),
        ("helper_in_low_level", ["model.yaml", "low_level.py"]),
        ("keyword_seed", ["model.yaml", "low_level.py"]),
        ("pcg64_seed", ["model.yaml", "low_level.py"]),
        ("seed_set_later", ["builder.py"]),
        ("seed_constant", ["builder.py", "low_level.py"]),
        ("warning_at_import", ["model.yaml", "low_level.py"]),
        ("yaml_variables", ["model.yaml", "builder.py"]),
        ("yaml_include", ["model.yaml", "builder.py"]),
    ],
)
def test_check_model_passes_on_correct_files_written_another_way(
    tmp_path, folder, files
):
    """A correct form passes however it names desflow's classes and gives its
    seed: the form and the seed come from what the run does, not from the text. A
    warning while desflow check imports a file is not a problem of the file, and a
    YAML is read as desflow reads it, with `${VAR}` and `!include`."""
    _local_copy(tmp_path)
    builder, low_level = tmp_path / "builder.py", tmp_path / "low_level.py"
    if folder == "alias":
        _change(builder, "ConsoleEgress, SimulationContext", "ConsoleEgress")
        _change(
            builder,
            "import logging\n",
            "import logging\n\nfrom desflow import SimulationContext as Context\n",
        )
        _change(builder, "    SimulationContext(", "    Context(")
    elif folder == "helper":
        text = builder.read_text(encoding="utf-8")
        (tmp_path / "shop_parts.py").write_text(text, encoding="utf-8")
        builder.write_text(
            "from shop_parts import app, run\n\n"
            'if __name__ == "__main__":\n    run()\n',
            encoding="utf-8",
        )
    elif folder == "helper_in_low_level":
        _change(
            low_level,
            "def run():",
            "def unused():\n    from desflow import SimulationContext\n\n"
            '    return SimulationContext(sim_id="Other")\n\n\ndef run():',
        )
    elif folder == "keyword_seed":
        _change(low_level, "default_rng(42)", "default_rng(seed=42)")
    elif folder == "pcg64_seed":
        _change(
            low_level,
            "np.random.default_rng(42)",
            "np.random.Generator(np.random.PCG64(42))",
        )
    elif folder == "seed_set_later":
        _change(builder, ", random_seed=42,", ",")
        _change(
            builder,
            "    app.run(until=30)",
            "    app.random_seed = 42\n    app.run(until=30)",
        )
    elif folder == "seed_constant":
        _change(builder, "random_seed=42,", "random_seed=SEED,")
        _change(builder, "import logging\n", "import logging\n\nSEED = 42\n")
        _change(low_level, "default_rng(42)", "default_rng(SEED)")
        _change(low_level, "import logging\n", "import logging\n\nSEED = 42\n")
    elif folder == "warning_at_import":
        _change(
            low_level,
            'logger = logging.getLogger("local_example")\n',
            'logger = logging.getLogger("local_example")\n'
            "__import__('warnings').warn('the old packing table is used')\n",
        )
    elif folder == "yaml_variables":
        _change(
            tmp_path / "model.yaml", "run: {until: 30}", "run:\n  until: ${UNTIL:-30}"
        )
    elif folder == "yaml_include":
        model = tmp_path / "model.yaml"
        text = model.read_text(encoding="utf-8")
        section = text[text.index("simulation:\n") : text.index("\negress:")]
        (tmp_path / "simulation.yaml").write_text(
            "\n".join(line[2:] for line in section.splitlines()[1:]) + "\n",
            encoding="utf-8",
        )
        _change(model, section, "simulation: !include simulation.yaml\n")
    done = _check_model([tmp_path / name for name in files])
    assert done.returncode == 0, done.stdout + done.stderr


def test_check_model_refuses_a_builder_whose_seed_desflow_drew(tmp_path):
    files = _local_copy(tmp_path)
    _change(files[1], ", random_seed=42,", ",")
    done = _check_model([files[1]])
    assert done.returncode == 1
    assert "has no random_seed, so desflow drew one" in done.stdout, done.stdout


_PAYLOAD = '"order_id": task_id}'
# One form alone, changed: each change in order, and the line the script must print
_ALONE = {
    "unseeded draw in a Builder": (
        "builder.py",
        [(_PAYLOAD, '"order_id": task_id, "share": __import__("random").random()}')],
        r"builder\.py: two runs at its own seed differ, so it draws numbers with no "
        "seed",
    ),
    "child process": (
        "low_level.py",
        [
            (
                "    run()\n",
                "    import multiprocessing\n\n"
                "    process = multiprocessing.Process(target=run)\n"
                "    process.start()\n    process.join()\n",
            )
        ],
        r"low_level\.py: made no SimulationContext and no DynamicRealtimeEnvironment "
        "in the process python low_level.py starts",
    ),
}


@pytest.mark.parametrize("case", sorted(_ALONE))
def test_check_model_finds_draws_and_processes_of_one_form_alone(tmp_path, case):
    """A form given alone is compared with nothing, so what makes its draws not
    follow the model's seed, and a model run where the script cannot see it, are
    found from its own runs."""
    name, changes, message = _ALONE[case]
    _local_copy(tmp_path)
    path = tmp_path / name
    for old, new in changes:
        _change(path, old, new)
    done = _check_model([path])
    assert done.returncode == 1, done.stdout + done.stderr
    assert re.search(rf"^{re.escape(str(tmp_path))}/{message}", done.stdout, re.M), (
        done.stdout + done.stderr
    )


# A seed that desflow does not set, one case for each place the script changes it
# and for each way of drawing that a rule on calls missed: the form, its imports,
# the lines at module level, and the payload's draw
_SEEDED = {
    "random.Random": ("builder.py", "", "R = random.Random(42)", "R.random()"),
    "random.seed": ("low_level.py", "", "random.seed(42)", "random.random()"),
    "numpy.random.seed": (
        "builder.py",
        "",
        "np.random.seed(1)",
        "float(np.random.random())",
    ),
    "default_rng": (
        "low_level.py",
        "",
        "G = np.random.default_rng(42)",
        "float(G.random())",
    ),
    "bit generator": (
        "builder.py",
        "",
        "G = np.random.Generator(np.random.Philox(42))",
        "float(G.random())",
    ),
    "RandomState": (
        "low_level.py",
        "",
        "RS = np.random.RandomState(42)",
        "float(RS.rand())",
    ),
    "SeedSequence": (
        "builder.py",
        "",
        "G = np.random.default_rng(np.random.SeedSequence(42))",
        "float(G.random())",
    ),
    "seed in a task": ("builder.py", "", "", "random.Random(task_id).random()"),
    "partial": (
        "builder.py",
        "import functools\n",
        "P = functools.partial(random.Random(42).random)",
        "P()",
    ),
    "map": (
        "builder.py",
        "",
        "R = random.Random(42)",
        "sum(map(R.uniform, [0], [1]))",
    ),
    "methodcaller": (
        "builder.py",
        "import operator\n",
        "R = random.Random(42)\nM = operator.methodcaller('random')",
        "M(R)",
    ),
    "thread target": (
        "builder.py",
        "from concurrent.futures import ThreadPoolExecutor\n",
        "R = random.Random(42)\nPOOL = ThreadPoolExecutor(1)",
        "POOL.submit(R.random).result()",
    ),
    "numpy.vectorize": (
        "builder.py",
        "",
        "V = np.vectorize(np.random.RandomState(42).uniform)",
        "float(V(0, 1))",
    ),
    "scipy random_state": (
        "builder.py",
        "from scipy import stats\n",
        "RS = np.random.RandomState(42)",
        "float(stats.norm.rvs(random_state=RS))",
    ),
    "pandas random_state": (
        "builder.py",
        "import pandas as pd\n",
        "FRAME = pd.DataFrame({'a': range(3)})",
        "int(FRAME.sample(1, random_state=42)['a'].iloc[0])",
    ),
    "eval": ("builder.py", "", "R = random.Random(42)", 'eval("R.random()")'),
    "numpy.random.mtrand.seed": (
        "builder.py",
        "import numpy.random.mtrand as mtrand\n",
        "mtrand.seed(42)",
        "float(np.random.random())",
    ),
    "bit_generator.SeedSequence": (
        "builder.py",
        "from numpy.random.bit_generator import SeedSequence\n",
        "G = np.random.Generator(np.random.PCG64(SeedSequence(42)))",
        "float(G.random())",
    ),
    # A seed made from the Sampler is still a generator the model seeds itself
    "seed drawn from a Sampler": (
        "low_level.py",
        "",
        "",
        'float(np.random.default_rng(int(sampler.generator("x").integers(2**32))).random())',
    ),
    "seed from the Sampler's seed": (
        "low_level.py",
        "",
        "",
        "random.Random(sampler.seed * 7 + task_id).random()",
    ),
    # A fixed and a drawn seed through one helper
    "helper with fixed and drawn seeds": (
        "low_level.py",
        "",
        "def make_rng(seed):\n    return random.Random(seed)\n",
        '(make_rng(42) if sampler.generator("k").random() < 0.1 else '
        'make_rng(int(sampler.generator("x").integers(2**31)))).random()',
    ),
    # A fixed seed in a branch that the run at the next seed does not reach
    "rare branch": (
        "low_level.py",
        "",
        "",
        '(random.Random(42).random() if sampler.generator("y3").random() < 0.04 else 0)',
    ),
}
# Draws that follow desflow's seed: each derives its sequence from a Sampler's
_DERIVED = {
    "copy": ("import copy\n", 'copy.deepcopy(sampler.generator("x")).random()'),
    "pickle": (
        "import pickle\n",
        'pickle.loads(pickle.dumps(sampler.generator("x"))).random()',
    ),
    "spawn": ("", 'sampler.generator("x").spawn(1)[0].random()'),
    "shared bit generator": (
        "",
        'np.random.Generator(sampler.generator("x").bit_generator).random()',
    ),
    "pickle of the Sampler's own generator": (
        "import pickle\n",
        "pickle.loads(pickle.dumps(sampler.rng)).random()",
    ),
}


def _seed_outside(path, imports, setup, draw):
    """Changes a copy of the Builder or Low-level form to draw its payload's
    `share` with `draw`, after `setup` at module level."""
    numpy = "\nimport numpy as np\n" if path.name == "builder.py" else ""
    _change(
        path, "import logging\n", f"import logging\nimport random\n{numpy}{imports}"
    )
    if setup:
        _change(path, "\nlogger = ", f"\n{setup}\nlogger = ")
    _change(path, _PAYLOAD, f'"order_id": task_id, "share": {draw}}}')


@pytest.mark.parametrize("case", sorted(_SEEDED))
def test_check_model_finds_a_seed_desflow_does_not_set(tmp_path, case):
    """A form given alone whose own code seeds a random generator, rather than
    drawing from one desflow's Sampler made, is refused at the place it seeds it,
    however the seed and the draws are made."""
    name, imports, setup, draw = _SEEDED[case]
    for package in ("scipy", "pandas"):
        if package in imports:
            pytest.importorskip(package)
    _local_copy(tmp_path)
    path = tmp_path / name
    _seed_outside(path, imports, setup, draw)
    done = _check_model([path])
    assert done.returncode == 1, done.stdout + done.stderr
    found = re.findall(
        rf"^{re.escape(str(path))}:\d+: \S+ seeds a random generator the model makes "
        r"itself \(\d+ seeds?\)",
        done.stdout,
        re.M,
    )
    assert found, done.stdout + done.stderr
    assert "failed" not in done.stdout, done.stdout


def test_check_model_finds_a_seed_desflow_does_not_set_beside_other_forms(tmp_path):
    """With the YAML beside it, the seed is reported before the records are
    compared."""
    files = _local_copy(tmp_path)
    _seed_outside(files[1], "", "R = random.Random(42)", "R.random()")
    done = _check_model(files)
    assert done.returncode == 1, done.stdout + done.stderr
    assert "random.Random seeds a random generator the model makes itself" in (
        done.stdout
    )


@pytest.mark.parametrize("case", sorted(_DERIVED))
def test_check_model_passes_a_sequence_derived_from_a_sampler(tmp_path, case):
    """A copy, a pickle, a spawn or a shared bit generator of a Sampler's
    generator takes no seed of its own, so it passes."""
    imports, draw = _DERIVED[case]
    _local_copy(tmp_path)
    low_level = tmp_path / "low_level.py"
    _seed_outside(low_level, imports, "", f"float({draw})")
    done = _check_model([low_level])
    assert done.returncode == 0, done.stdout + done.stderr


def test_check_model_finds_a_seed_set_in_code_run_with_exec(tmp_path):
    """A process written in a string, run with `exec` and started on SimPy, is
    the model's own code, though no frame of the model file is between it and
    desflow's run."""
    _local_copy(tmp_path)
    low_level = tmp_path / "low_level.py"
    setup = (
        "exec('def seeder(env):\\n    random.seed(7)\\n    yield env.timeout(0)\\n')"
    )
    _seed_outside(low_level, "", setup, "random.random()")
    _change(
        low_level,
        "    env.process(capacity_changes(env))\n",
        "    env.process(capacity_changes(env))\n    env.process(seeder(env))\n",
    )
    done = _check_model([low_level])
    assert done.returncode == 1, done.stdout + done.stderr
    assert re.search(
        rf"^{re.escape(str(low_level))}:\d+: random.seed seeds a random generator",
        done.stdout,
        re.M,
    ), done.stdout + done.stderr


def test_check_model_passes_a_model_that_imports_scipy_stats(tmp_path):
    """Importing `scipy.stats` seeds NumPy generators in SciPy's own code, which
    is not a seed of the model's."""
    pytest.importorskip("scipy")
    _local_copy(tmp_path)
    low_level = tmp_path / "low_level.py"
    _change(low_level, "import logging\n", "import logging\nimport scipy.stats\n")
    done = _check_model([low_level])
    assert done.returncode == 0, done.stdout + done.stderr


def test_check_model_leaves_numpy_classes_as_a_model_sees_them(tmp_path):
    """The classes the script puts in NumPy's place check, pickle and print as
    NumPy's own, a generator made from a seed pickles, and a model's own subclass
    of one checks as any class does. The seeds it sets are reported, and the run
    does not fail."""
    _local_copy(tmp_path)
    low_level = tmp_path / "low_level.py"
    setup = (
        "class MyBG(np.random.PCG64):\n    pass\n\n\n"
        "assert not isinstance(np.random.PCG64(1), MyBG)\n"
        "assert isinstance(np.random.PCG64(1), np.random.PCG64)\n"
        "import numpy.random._pcg64\n"
        "assert pickle.loads(pickle.dumps(np.random.PCG64)) is numpy.random._pcg64.PCG64\n"
        "pickle.loads(pickle.dumps(MyBG))\n"
        "pickle.loads(pickle.dumps(np.random.default_rng(1)))\n"
        "pickle.loads(pickle.dumps(np.random.PCG64(5)))\n"
        "assert repr(np.random.PCG64) == \"<class 'numpy.random._pcg64.PCG64'>\"\n"
        'assert repr(np.random.PCG64(1)).startswith("<numpy.random._pcg64.PCG64")'
    )
    _seed_outside(low_level, "import pickle\n", setup, "0")
    done = _check_model([low_level])
    assert "failed" not in done.stdout, done.stdout + done.stderr
    assert "numpy.random.PCG64 seeds a random generator" in done.stdout


def test_check_model_passes_a_pickle_of_a_builder_samplers_generator(tmp_path):
    """A Builder task that pickles its context's Sampler generator passes."""
    _local_copy(tmp_path)
    builder = tmp_path / "builder.py"
    _seed_outside(
        builder,
        "import pickle\n",
        "",
        "float(pickle.loads(pickle.dumps(context.sampler.rng)).random())",
    )
    done = _check_model([builder])
    assert done.returncode == 0, done.stdout + done.stderr


@pytest.mark.parametrize("name", ["builder.py", "low_level.py"])
def test_check_model_runs_one_python_form_alone(tmp_path, name):
    """One Python form alone is run, so a run that fails after its records is
    found, and a correct one passes at two seeds."""
    _local_copy(tmp_path)
    path = tmp_path / name
    done = _check_model([path])
    assert done.returncode == 0, done.stdout + done.stderr
    assert "ok runs at two seeds" in done.stdout
    _change(path, "    run()\n", "    run()\n    raise RuntimeError('after')\n")

    done = _check_model([path])
    assert done.returncode == 1
    assert f"python {name} failed" in done.stdout, done.stdout
    assert "RuntimeError: after" in done.stdout


def test_check_model_reads_an_upper_case_suffix_as_desflow_does(tmp_path):
    """`desflow check` reads `shop.PY` as Python, so the script does too."""
    _local_copy(tmp_path)
    path = (tmp_path / "builder.py").rename(tmp_path / "builder.PY")
    done = _check_model([path])
    assert done.returncode == 0, done.stdout + done.stderr
    assert "ok runs at two seeds" in done.stdout


def test_skill_texts_match_what_they_describe():
    """Sentences of the skill that the stage 2 review found wrong, each against
    the fact it states."""
    skill = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "Run python <skill folder>/scripts/check_model.py <files>" in skill
    assert "except a test of pacing, which runs alone" in skill
    assert "A Builder task read only from a `Template` keeps none" in skill
    testing = (SKILL / "references" / "testing.md").read_text(encoding="utf-8")
    assert "`python builder.py`, `python low_level.py`" in testing
    import simpy.util

    assert "interrupt((signaller, result))" in inspect.getsource(
        simpy.util.subscribe_at
    )
    simpy_md = (SKILL / "references" / "simpy.md").read_text(encoding="utf-8")
    assert "with the cause `(event, value)`, a tuple" in simpy_md
    assert (
        "against the YAML form, or against the Builder form when there is no YAML"
    ) in skill
    assert "on a Builder file that has a `tests.yaml` beside it" in skill
    assert "(one form alone runs at two seeds and is compared with nothing)" in skill
    assert (
        "It sees only the process `python FILE` starts, so each Python file builds "
        "and runs its model in that process, not a child process."
    ) in skill
    assert (
        "- a random generator the model seeds itself, other than one it gives to a "
        "`Sampler`;"
    ) in skill
    assert (
        "A state set another way (`setstate`, the `state` setter, unpickling), a "
        "`SeedSequence` with a spawn key, a seeding function called through "
        "`functools.partial` or another callable with no Python code of the "
        "model's, a generator the model writes itself and one a C extension seeds "
        "are not found"
    ) in skill
    assert "which makes two runs at one seed differ" in skill
    assert "two, not the four of the rule above" in testing
    # The script is read from its text: importing it would write a `__pycache__`
    # into the skill folder
    tree = ast.parse((SKILL / "scripts" / "check_model.py").read_text("utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "Seen":
            for method in node.body:
                if isinstance(method, ast.FunctionDef):
                    assert "Returns:" in (ast.get_docstring(method) or ""), method.name
    api = (SKILL / "references" / "api.md").read_text(encoding="utf-8")
    assert (
        "except `IcebergStorageEgress`, `JsonlStorageEgress` and "
        "`ParquetStorageEgress`: they are always there, and building one without "
        "its extra raises an `ImportError` that names the extra"
    ) in api


def _template(folder):
    """The pytest template of testing.md and its model, the shop of apis.md with a
    start time and the tests entry testing.md gives, written to `folder`."""
    testing = (SKILL / "references" / "testing.md").read_text(encoding="utf-8")
    template = testing.split("## Template")[1]
    tests = template.split("```yaml\n")[1].split("```")[0]
    code = template.split("```python\n")[1].split("```")[0]
    apis = (SKILL / "references" / "apis.md").read_text(encoding="utf-8")
    shop = apis.split("```yaml\n")[1].split("```")[0]
    old = "simulation: {sim_id: Shop, factor: 1.0, random_seed: 42}"
    assert shop.count(old) == 1
    shop = shop.replace(
        old,
        "simulation: {sim_id: Shop, factor: 1.0, random_seed: 42, "
        "logical_start_time: '2026-01-05T09:00:00Z'}",
    )
    (folder / "test_model.py").write_text(code, encoding="utf-8")
    return shop, tests


def _pytest(folder):
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "test_model.py",
        ],
        capture_output=True,
        text=True,
        cwd=folder,
    )


def test_testing_template_passes_and_its_statistics_test_can_fail(tmp_path):
    """The template passes on its model, and its statistics test fails on the shop
    without the tests entry, where desflow test has nothing to run."""
    shop, tests = _template(tmp_path)
    (tmp_path / "model.yaml").write_text(shop + "\n" + tests, encoding="utf-8")
    done = _pytest(tmp_path)
    assert done.returncode == 0, done.stdout + done.stderr
    (tmp_path / "model.yaml").write_text(shop, encoding="utf-8")
    done = _pytest(tmp_path)
    assert done.returncode == 1
    assert "1 failed" in done.stdout and "test_the_statistics" in done.stdout


@pytest.mark.parametrize("name", ["parquet_example.py", "iceberg_example.py"])
def test_check_model_finds_no_private_import_in_an_example(tmp_path, name):
    """`desflow examples get` hands these files to an assistant, which the skill
    tells to use documented names only. Only the imports are asked about: the
    Iceberg example needs its catalog to run, so the runs are cut short."""
    copy = tmp_path / name
    copy.write_text(
        (_ROOT / "examples" / "low-level" / name).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    done = _check_model([copy], "--timeout", "5")
    assert "ok imports" in done.stdout, done.stdout


@pytest.mark.parametrize("seed", [42, 43, 44])
def test_testing_template_tolerance_fails_a_longer_packing_time(tmp_path, seed):
    """The template's tests entry passes on the shop at seeds 42 to 44, and fails
    when the packing time is 10% longer, which is the change it is about."""
    shop, tests = _template(tmp_path)
    tests = tests.replace("    until: 300\n", f"    until: 300\n    seed: {seed}\n")
    desflow_command = Path(sys.executable).with_name("desflow")
    for mean, code in (("3.0", 0), ("3.3", 1)):
        model = shop.replace("mean: 3.0,", f"mean: {mean},") + "\n" + tests
        (tmp_path / "model.yaml").write_text(model, encoding="utf-8")
        done = subprocess.run(
            [desflow_command, "test", "model.yaml"],
            capture_output=True,
            text=True,
            cwd=tmp_path,
        )
        assert done.returncode == code, (mean, done.stdout + done.stderr)
