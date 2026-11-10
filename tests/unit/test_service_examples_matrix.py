"""The `services` jobs run every example that has an odctl profile, each once.

The job's matrix in `pull-request.yml` lists the examples of each group by hand, and
`EXAMPLES` is the list of examples. An example registered with a profile and left out
of the matrix would never run in CI, and a name left in the matrix after its example
goes would select no test.
"""

from pathlib import Path

import yaml

from desflow.examples import EXAMPLES

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"


def test_matrix_lists_the_examples_with_a_profile():
    job = yaml.safe_load((WORKFLOWS / "pull-request.yml").read_text(encoding="utf-8"))[
        "jobs"
    ]["services"]
    listed = [
        name
        for group in job["strategy"]["matrix"]["include"]
        for name in group.get("examples", [])
    ]
    registered = sorted(name for name, e in EXAMPLES.items() if e.profile)
    assert sorted(listed) == registered, (
        "The services matrix in pull-request.yml and the examples with a profile "
        f"in EXAMPLES differ: matrix {sorted(listed)}, EXAMPLES {registered}."
    )


def test_every_service_example_has_a_plan_and_a_reader():
    """A service example added to `EXAMPLES` fails here until its test knows it.

    The integration test runs one example per CI job, selected with `-k`, so a check
    of the whole list there would run in no job.
    """
    from tests.integration.test_service_examples import PLANS, READERS

    registered = {name for name, e in EXAMPLES.items() if e.profile}
    assert set(PLANS) == registered == set(READERS)
