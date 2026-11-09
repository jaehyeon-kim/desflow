"""Imports and constructs what the `watch` extra exists to enable, installed alone."""

from pathlib import Path

from watchfiles import DefaultFilter, watch  # noqa: F401

from desflow.cli.run import _BlueprintFilter

accept = _BlueprintFilter(Path("shop.yaml"))
assert accept(1, str(Path("shop.yaml").resolve()))
assert not accept(1, str(Path("other.yaml").resolve()))
