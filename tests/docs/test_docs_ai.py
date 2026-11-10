"""The Agent Skill pages show what the files of docs/snippets/ai/ give.

The drafted shop is run with the commands its pages show: `desflow explain`, 20
replications, `desflow test`, the skill's check on its three forms, and the experiment.
Each output block on a page must be what the command prints, and the tests must fail
when the arrival rate they are about is 10% higher.
"""

import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGES = ROOT / "docs" / "ai"
SNIPPETS = ROOT / "docs" / "snippets" / "ai"
DESFLOW = shutil.which("desflow", path=str(Path(sys.executable).parent)) or "desflow"
CHECK = ROOT / "skills" / "desflow" / "scripts" / "check_model.py"


def _run(args, cwd=SNIPPETS):
    done = subprocess.run(args, cwd=cwd, capture_output=True, text=True, check=False)
    assert done.returncode == 0, done.stdout + done.stderr
    return done.stdout


def _block_after(page, command):
    """The text block that follows the bash block holding `command` on a page."""
    text = (PAGES / page).read_text(encoding="utf-8")
    after = text[text.index(f"```bash\n{command}\n```") :]
    return re.search(r"```text\n(.*?)\n```", after, re.S).group(1)


def test_explain_gives_what_draft_a_model_shows():
    shown = _block_after("draft-a-model.md", "desflow explain drafted_shop.yaml")
    assert _run([DESFLOW, "explain", "drafted_shop.yaml"]).rstrip("\n") == shown


def test_replications_give_what_draft_a_model_shows():
    command = (
        'desflow run drafted_shop.yaml --replications 20 | grep -E "^measure|wait_mean"'
    )
    shown = _block_after("draft-a-model.md", command)
    out = _run([DESFLOW, "run", "drafted_shop.yaml", "--replications", "20"])
    lines = [line for line in out.splitlines() if re.search("^measure|wait_mean", line)]
    assert "\n".join(lines) == shown


def test_the_skill_check_passes_on_the_three_forms():
    files = [
        "drafted_shop.yaml",
        "drafted_shop_builder.py",
        "drafted_shop_low_level.py",
    ]
    command = (
        "python <skill folder>/scripts/check_model.py drafted_shop.yaml "
        "drafted_shop_builder.py drafted_shop_low_level.py"
    )
    shown = _block_after("write-another-form.md", command)
    assert _run([sys.executable, str(CHECK), *files]).rstrip("\n") == shown


def test_desflow_test_passes_and_fails_on_a_faster_arrival(tmp_path):
    shown = _block_after("write-tests.md", "desflow test drafted_shop.yaml")
    assert _run([DESFLOW, "test", "drafted_shop.yaml"]).rstrip("\n") == shown
    faster = tmp_path / "drafted_shop.yaml"
    faster.write_text(
        (SNIPPETS / "drafted_shop.yaml")
        .read_text()
        .replace("rate: 0.5,", "rate: 0.55,")
    )
    done = subprocess.run(
        [DESFLOW, "test", str(faster)], capture_output=True, text=True, check=False
    )
    assert done.returncode == 1
    page = (PAGES / "write-tests.md").read_text(encoding="utf-8")
    measured = re.findall(r"measured ([\d.]+)", done.stdout + done.stderr)
    assert len(measured) == 2
    assert f"{int(float(measured[0])):,} orders" in page
    assert f"{round(float(measured[1]))}% busy" in page


def test_the_experiment_gives_the_comparison_run_a_study_shows(tmp_path):
    out = tmp_path / "study"
    _run([DESFLOW, "experiment", "drafted_shop_experiment.yaml", "--output", str(out)])
    report = (out / "report.md").read_text(encoding="utf-8")
    shown = _block_after(
        "run-a-study.md", "desflow experiment drafted_shop_experiment.yaml"
    )
    for line in shown.splitlines():
        assert line in report, line
