"""Imports and constructs what the `all` extra exists to enable, installed alone: it
runs the check of each extra that `all` names in pyproject.toml, in this one
environment."""

import runpy
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parent
extras = tomllib.loads((HERE.parents[1] / "pyproject.toml").read_text())["project"][
    "optional-dependencies"
]
# `all` is the one requirement `desflow[kafka,redis,...]`
named = extras["all"][0].removeprefix("desflow[").removesuffix("]")
for name in named.split(","):
    print("all:", name)
    runpy.run_path(str(HERE / f"check_{name}.py"), run_name="__main__")
