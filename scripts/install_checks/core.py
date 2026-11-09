"""The core install alone imports desflow, with no extra.

The install-checks job also starts the `desflow` command and runs a YAML blueprint
with it, which proves the entry point, Typer and PyYAML need no extra.
"""

import desflow  # noqa: F401
