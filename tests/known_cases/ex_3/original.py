"""EX-3: Machine shop with breakdowns and a repairman. The plain SimPy program: from the SimPy program.

The three forms are the files of the example `simpy-machine-shop`.
"""

from tests.conformance.yaml_only.programs import known9

HARNESS = known9
program = known9.EX_3
EXAMPLE = "machine_shop"  # the forms are the files of SimPy's Machine Shop example

# The draws are fixed, as in fixed_draws.py: the test applies these replacements to the
# example's YAML. They are the two repeating sequences `known9.EX_3` uses: a part takes
# 8 to 12, and a breakdown comes 250 to 310 after the one before.
DRAWS = [
    (
        "service_time: {gen: sample, dist: truncated_normal, mean: 10, std: 2, low: 0}",
        'service_time: {gen: expr, expr: "8 + n % 5", names: {n: {gen: sequence}}}',
    ),
    (
        "timeout: {gen: sample, dist: exponential, rate: 0.0033333333333333335}",
        'timeout: {gen: expr, expr: "250 + n % 7 * 10", names: {n: {gen: sequence}}}',
    ),
]
