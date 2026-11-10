"""The Test a Pipeline page: tests of a pipeline against the labels of a planted fault.

Save the shop of your form from the page as pipeline_shop.py in the folder of this file,
with pipeline_under_test.py, and run: pytest test_a_pipeline.py
"""

import pytest
from pipeline_shop import run_shop
from pipeline_under_test import find_surges, read_lines, score


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    """One run of the shop: its events and its labels."""
    folder = tmp_path_factory.mktemp("run")
    run_shop(folder)
    return read_lines(f"{folder}/data"), read_lines(f"{folder}/labels")


# --8<-- [start:tests]
def test_the_labels_name_the_planted_fault(run):
    _, labels = run
    assert [label["label"] for label in labels] == [{"fault": "order_surge"}]
    assert [label["sim_ts"] for label in labels] == [300.0]


def test_the_data_carries_no_label(run):
    events, _ = run
    assert {event["stream_type"] for event in events} == {"event"}


def test_the_pipeline_catches_the_fault_without_a_false_alarm(run):
    events, labels = run
    assert score(find_surges(events), labels) == {"caught": 1, "missed": 0, "false": 0}


def test_a_pipeline_that_is_too_strict_misses_the_fault(run):
    events, labels = run
    assert score(find_surges(events, factor=10), labels)["missed"] == 1


# --8<-- [end:tests]
