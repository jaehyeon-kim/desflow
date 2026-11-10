"""GA-5: One option gets 60 percent of votes.

The original is the test asserts of the section of yaml-only.md Part 4: `check` states
them and the test runs it on the records of the YAML form.
"""


def check(records):
    """Over the 9,999 votes of a run to 10,000 s, the first option's share is within 0.02
    of 0.60. The run ends at 10,000 s before the vote due then."""
    votes = [r["value"]["choice"] for r in records if r["stream_type"] == "event"]
    assert len(votes) == 9999
    assert abs(votes.count("a") / len(votes) - 0.60) < 0.02
