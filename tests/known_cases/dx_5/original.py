"""DX-5: A payload that reads which branches of a wait were done.

The original is the snippet's own Python: `original.yaml` is the model with
`payload: !python wait_for_one_or_all_logic.branches`. The test runs it, and the YAML form
has its shape: the same events at the same times, with the same fields.
"""

PYTHON_FORM = "tests/known_cases/dx_5/original.yaml"
LOGIC = "docs/snippets/yaml"  # the folder of wait_for_one_or_all_logic.py
