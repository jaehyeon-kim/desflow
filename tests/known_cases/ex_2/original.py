"""EX-2: Carwash where a car keeps its machine during the wash. The plain SimPy program: from the SimPy program.

The three forms are the files of the example `simpy-carwash`.
"""

from tests.conformance.yaml_only.programs import known8

HARNESS = known8
program = known8.EX_2
EXAMPLE = "carwash"  # the forms are the files of SimPy's Carwash example

# The draws are fixed, as in fixed_draws.py: the test applies these replacements to the
# example's YAML. They are the values `known8.EX_2` uses.
DRAWS = [
    ("gap: {gen: integer, bounds: [5, 9]}", "gap: 7"),
    ("dirt_removed_pct: {gen: integer, bounds: [50, 99]}", "dirt_removed_pct: 70"),
]
