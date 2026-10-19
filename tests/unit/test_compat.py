"""The names before the rename keep working: `dynamic_des` and the `ddes` command.

Each check runs in a new interpreter, because a module warns only the first time it
is imported.
"""

import subprocess
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _python(code: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )


def test_import_warns_by_default_from_a_module():
    """A FutureWarning is shown with Python's default filters, outside __main__ too."""
    result = _python(
        "import types, sys\n"
        "m = types.ModuleType('user_code')\n"
        "exec('import dynamic_des', m.__dict__)\n"
    )
    assert "FutureWarning: dynamic_des is renamed desflow" in result.stderr


def test_submodules_are_the_desflow_modules():
    result = _python(
        "import warnings\n"
        "warnings.simplefilter('ignore', FutureWarning)\n"
        "import dynamic_des, desflow\n"
        "from dynamic_des.connectors.egress.local import ConsoleEgress\n"
        "import dynamic_des.core.context as old\n"
        "import desflow.connectors.egress.local as local, desflow.core.context as new\n"
        "assert ConsoleEgress is local.ConsoleEgress\n"
        "assert old is new and old.__spec__.name == 'desflow.core.context'\n"
        "assert dynamic_des.SimulationContext is desflow.SimulationContext\n"
        "assert dynamic_des.__version__ == desflow.__version__\n"
        "try:\n"
        "    import dynamic_des.no_such_module\n"
        "except ModuleNotFoundError:\n"
        "    print('missing')\n"
    )
    assert result.stdout.strip() == "missing"


def test_ddes_warns_then_runs_desflow():
    result = _python(
        "import sys\n"
        "sys.argv = ['ddes', 'examples']\n"
        "from desflow.blueprint.cli import ddes\n"
        "try:\n"
        "    ddes()\n"
        "except SystemExit as exc:\n"
        "    sys.exit(exc.code)\n"
    )
    assert "the ddes command is deprecated" in result.stderr
    assert "Download one with: desflow examples get <name>" in result.stdout


# Extras desflow added after the last dynamic-des release was published, so no one
# installs them under the old name.
_ADDED_AFTER_THE_LAST_RELEASE = {"fit"}


def test_last_dynamic_des_release_has_every_desflow_extra():
    """`pip install "dynamic-des[kafka]"` must install what `desflow[kafka]` does."""
    main = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    compat = tomllib.loads(
        (ROOT / "compat" / "dynamic-des" / "pyproject.toml").read_text(encoding="utf-8")
    )
    # The last dynamic-des release needs at least the desflow of the same version.
    version = compat["project"]["version"]
    assert compat["project"]["optional-dependencies"] == {
        extra: [f"desflow[{extra}]>={version}"]
        for extra in main["project"]["optional-dependencies"]
        if extra not in _ADDED_AFTER_THE_LAST_RELEASE
    }


def test_last_dynamic_des_release_has_the_ddes_command():
    """pip deletes 0.19.0's bin/ddes on upgrade, after desflow wrote it."""
    main = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    compat = tomllib.loads(
        (ROOT / "compat" / "dynamic-des" / "pyproject.toml").read_text(encoding="utf-8")
    )
    assert compat["project"]["scripts"] == {"ddes": main["project"]["scripts"]["ddes"]}
