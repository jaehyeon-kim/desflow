"""EX-4: Movie tickets with a sold-out event that makes people leave. The plain SimPy program: from the SimPy program.

The three forms are the files of the example `simpy-movie-renege`.
"""

from tests.conformance.yaml_only.programs import known11

HARNESS = known11
program = known11.EX_4
EXAMPLE = "movie_renege"  # the forms are the files of SimPy's Movie Renege example

# The draws are fixed, as in fixed_draws.py: the test applies these replacements to the
# example's YAML. They are the values `known11.EX_4` uses: a gap of 0.5, the movie picked
# by the arrival number, and 1 + (n * 5) % 6 tickets.
DRAWS = [
    ("dist: exponential\n    rate: 2", "dist: constant\n    value: 0.5"),
    (
        "      movie:\n"
        "        gen: one_of\n"
        "        choices: [Python Unchained, Kill Process, Pulp Implementation]\n"
        "      tickets: {gen: integer, bounds: [1, 6]}\n",
        "      n: {gen: sequence, start: 0}\n"
        "      movie: {gen: expr, expr: \"['Python Unchained', 'Kill Process', 'Pulp Implementation']"
        '[(attr.n * 7) % 3 if attr.n % 5 else 0]"}\n'
        '      tickets: {gen: expr, expr: "1 + (attr.n * 5) % 6"}\n',
    ),
]
