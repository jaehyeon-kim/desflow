"""Every command on the Command Line page runs, in order, in one folder.

The page shows `desflow` commands in bash blocks, and scripts/record_blocks.py shows
what each prints. This test runs each command as the page gives it, in a new folder,
with `desflow examples get` served from this repository's examples/ folder, so no
network is needed. It also checks the numbers the page reads from the output.
"""

import json
import re
import shlex
from pathlib import Path
from unittest import mock

from typer.testing import CliRunner

from desflow.blueprint import examples
from desflow.cli import app

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "docs" / "cli" / "index.md"
_BASH = re.compile(r"^```bash\n(.*?)\n```$", re.S | re.M)


def _fetch(url):
    """The file a download URL names, from this repository's examples/ folder."""
    return (ROOT / "examples" / url.split("/examples/", 1)[1]).read_bytes()


def _commands():
    """Each command line of the page's bash blocks, in page order."""
    text = PAGE.read_text(encoding="utf-8")
    return [
        line
        for block in _BASH.findall(text)
        for line in block.splitlines()
        if line.strip() and not line.startswith("#")
    ]


def test_every_command_on_the_page_runs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    outputs = {}
    commands = _commands()
    assert commands, "the page shows no commands"
    with mock.patch.object(examples, "_fetch", side_effect=_fetch):
        for line in commands:
            words = shlex.split(line)
            target = None
            if ">" in words:
                words, target = words[: words.index(">")], words[words.index(">") + 1]
            assert words[0] == "desflow", line
            result = CliRunner().invoke(app, words[1:])
            assert result.exit_code == 0, f"{line}\n{result.output}"
            if target:
                Path(target).write_text(result.stdout, encoding="utf-8")
            outputs[line] = result.stdout

    def output(start):
        return next(text for line, text in outputs.items() if line.startswith(start))

    # The numbers the page reads from each output
    dest = output("desflow examples get experiment --api builder --dest shop")
    assert "cd shop/experiment/builder && python experiment_example.py" in dest
    explain = output("desflow explain")
    assert re.search(r"\| packers\s+\|\s+0\.4 \|\s+0\.25 \| 2 \|\s+0\.8 \|", explain)
    assert re.search(r"\|\s+1 \|\s+1\.6 \| UNSTABLE", explain)
    replications = output("desflow run experiment/yaml/experiment_shop.yaml --rep")
    assert re.search(r"tasks\.pack\.wait_mean\s+10\s+4\.505\s+0\.6483", replications)
    assert re.search(r"utilization_avg\s+10\s+79\.53", replications)
    verify = output("desflow explain experiment/yaml/experiment_shop.yaml --verify")
    assert re.search(r"utilization\s+\|\s+0\.8 \|\s+0\.7953 \|.*agrees", verify)
    assert re.search(r"throughput\s+\|\s+0\.4 \|\s+0\.4002 \|.*agrees", verify)
    assert re.search(r"\| pack\s+\| wait\s+\|\s+- \|\s+4\.505 \|", verify)
    assert "utilisation 0.9 at a request every 2.22 seconds" in verify
    measure = output(next(line for line in outputs if "--level 0.99" in line))
    assert re.search(r"tasks\.pack\.wait_mean\s+10\s+4\.505\s+0\.9314", measure)
    records = (tmp_path / "records.jsonl").read_text("utf-8").splitlines()
    last = json.loads(records[-1])
    assert (last["sim_ts"], last["key"]) == (24.789, "orders-9")
    assert last["value"] == {"event_type": "order_packed"}
    assert "checked 0.5919" in output("desflow experiment")
    fit = output("desflow fit")
    assert re.search(r"triangular \*\s+fitted\s+56\.68", fit)
    assert re.search(r"gamma\s+fitted\s+56\.79", fit)
    options = output(next(line for line in outputs if "--bins 8" in line))
    assert "edges: 9 numbers, counts: 8 numbers" in options
    assert re.search(r"gamma .*can draw above the upper bound 10", options)
    assert "triangular" in (tmp_path / "fitting/yaml/packing_fit.yaml").read_text()
    assert "shape 9.63671, scale 0.155655" in output("desflow dist")
    assert "mean 2.5, std 2.5" in output("desflow dist exponential")
    assert "mean 1.53, std 0.481785" in output("desflow dist triangular")
    schema = json.loads((tmp_path / "blueprint.schema.json").read_text("utf-8"))
    assert "simulation" in schema["properties"]
