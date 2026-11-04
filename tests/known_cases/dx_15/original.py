"""DX-15: A wait for the whole flow of a process.

The original is the snippet's own Python: the docs snippet `docs/snippets/yaml/simpy_whole_flow.yaml`
with its `!python` process `watch_flow` of `docs/snippets/yaml/simpy_forms.py`. The test runs
it, and the YAML form, which has no Python, gives the same results at the same times:
the payload of each task's result (the records that only say a task was queued,
started, interrupted or resumed are not in the process's).
"""

from tests.known_cases.harness import finished_payloads

PYTHON_FORM = "docs/snippets/yaml/simpy_whole_flow.yaml"
LOGIC = "docs/snippets/yaml"  # the folder of simpy_forms.py
project = finished_payloads
