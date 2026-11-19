"""Imports and constructs what the `mcp` extra exists to enable, installed alone."""

from desflow.mcp_server import build_server

assert build_server(allow_changes=False)
assert build_server(allow_changes=True)
