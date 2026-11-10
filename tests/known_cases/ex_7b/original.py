"""EX-7b: One generator that broadcasts to several pipes. The plain SimPy program: from the SimPy program.

The three forms are the files of the example `simpy-process-communication`.
"""

from tests.conformance.yaml_only.programs import known11

HARNESS = known11
program = known11.EX_7b
EXAMPLE = "process_communication_broadcast"  # the forms are the files of SimPy's Process Communication example

# The draws are fixed, as in fixed_draws.py: the test applies these replacements to the
# example's YAML. They are the values `known11.EX_7b` uses.
DRAWS = [
    ("gap: {gen: integer, bounds: [6, 10]}", "gap: 8"),
    ("service_time: {gen: integer, bounds: [4, 8]}", "service_time: 6"),
]
