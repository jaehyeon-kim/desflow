"""Loading a blueprint from Python, as the YAML Blueprints and Builder API pages show it.

tests/docs runs this file. `local.yaml` is the local example, which `desflow examples get
local` downloads into the current folder. The example paces its run at factor 1.0 for
30 s, so this file runs an unpaced copy of it from the current folder.
"""

from pathlib import Path

EXAMPLE = Path(__file__).resolve().parents[3] / "examples" / "yaml" / "local.yaml"


def load_and_run():
    # --8<-- [start:from-yaml]
    from desflow import SimulationContext

    app = SimulationContext.from_yaml("local.yaml")
    app.run()  # calls run.before, then runs until run.until
    # --8<-- [end:from-yaml]
    return app


if __name__ == "__main__":
    text = EXAMPLE.read_text(encoding="utf-8")
    assert "factor: 1.0" in text
    Path("local.yaml").write_text(text.replace("factor: 1.0", "factor: 0.0"))
    app = load_and_run()
    assert app.env.now == 30
