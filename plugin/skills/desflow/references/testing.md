# Testing a model

How to test a desflow model so that each test fails when the behaviour it is about changes. Each section is one rule, with a wrong and a right example. The last section is a pytest template.

- [Keep the draws the request is about](#keep-the-draws-the-request-is-about)
- [Compare whole streams](#compare-whole-streams)
- [No wall-clock asserts in unpaced tests](#no-wall-clock-asserts-in-unpaced-tests)
- [A test must fail on the change it is about](#a-test-must-fail-on-the-change-it-is-about)
- [Expected values come from the request](#expected-values-come-from-the-request)
- [Each test stands alone](#each-test-stands-alone)
- [Template](#template)

## Keep the draws the request is about

A request that names a random choice is about the spread of its outcomes, so a test keeps the draw and runs at a seed.

Wrong: replace `next: {weights: {a: 3, b: 1}}` with `next: a`, so that the records are fixed.

Right: keep the weights, run at a seed, and assert that the records reach every branch:

```python
assert {"a", "b"} <= {record["task"] for record in records}
```

A fixed value is allowed only for a draw the request does not name. The test then asserts that each branch the request names is still reached.

## Compare whole streams

Two forms are the same model when their whole record streams and their statistics are equal at three seeds. One field, or the events alone, is not enough: a form can give the same events and different telemetry.

- Test each form through the entry point its reader runs: `desflow run model.yaml`, `python builder.py`, `python low_level.py`. `desflow run builder.py --sample N` prints a Builder file's first records, but it does not run the file's `if __name__ == "__main__":` block, so it misses a seed or a run end set there. A helper that only the test calls is not what the reader runs.
- A test named "every form" covers every form.
- `scripts/check_model.py` in this skill's folder makes this comparison: run it instead of writing your own.

## No wall-clock asserts in unpaced tests

An unpaced run (`factor: 0`, or `desflow run --sample`) takes as long as the computer takes, so an assert on its wall time fails on a slower computer. Only a paced test asserts on the wall clock, and it runs alone, not under `pytest -n`, because other tests slow it down.

A paced bound allows for the clock of Windows before Python 3.13, which moves in steps of 15.6 ms.

Wrong: `assert wall < 0.010`.

Right: `assert wall >= expected - 0.016`.

## A test must fail on the change it is about

Change the number the test is about (a rate, a weight, a capacity) by a small amount and run the test again: it must fail. A test that still passes checks nothing about that number.

- Set a tolerance from the standard error. Four standard errors of a share `p` over `n` draws is `4 * sqrt(p * (1 - p) / n)`: for a share of 0.25 over 400 draws that is 0.087.
- Do not catch `Exception` in a test, compare by type where the value is known, or assert a value the test itself set.
- A run that publishes no record passes every "for each record" assert, so assert the record count too.

Wrong: `assert all(r["value"]["shipping"] == "standard" for r in packed)` alone.

Right: `assert packed` first, then the same line.

## Expected values come from the request

Take each expected value from the request, from a hand calculation or from `desflow explain`, never from the model under test.

- `desflow test --fix` writes what the run gave into the expected values. Use it only after a deliberate change, and read each value it changed.
- A reference program is written from the request, not copied from the model.

Wrong: run the model, copy its mean wait into the test.

Right: the request says three packers keep the mean wait under 2 seconds, so the test asserts `wait_mean < 2`.

## Each test stands alone

A test that passes only after another test has run fails when it runs alone or in another order.

- Build a new context in each test.
- Restore `sys.path`, `sys.modules` and the environment variables a test changes, for example with pytest's `monkeypatch`.
- Collect the test ids with `pytest --co -q test_model.py`, pass them back to pytest in reverse order (`tail -r` on macOS, `tac` on Linux), and the file still passes.

## Template

A pytest file for the shop of `references/apis.md`, written as `model.yaml` with a `logical_start_time` beside the seed, so the timestamps of two runs are equal, and with this `tests` entry:

```yaml
tests:
  # From the request: in 300 s the packers can pack about 200 orders at 3 s an
  # order (10 s at 1 packer, 10 s at 3 and 280 s at 2), less about 2 still in
  # service at the end. 8 is a little over two standard deviations of the count
  # across seeds (3.6 over seeds 42 to 53): two, not the four of the rule above,
  # so that a packing time 10% longer, which packs 175 to 184, fails at every seed
  - name: packed_orders
    until: 300
    expect: {tasks.pack_order.finished: {value: 198, within: 8}}
```

It runs the model at three seeds with the `desflow` command, asserts the records the checklist of SKILL.md lists, and runs `desflow test`. It uses only the command line, so it does not change when desflow's internals do.

```python
"""Tests of the shop in model.yaml, run with the desflow command as a user runs it."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

MODEL = Path(__file__).with_name("model.yaml")
DESFLOW = shutil.which("desflow", path=str(Path(sys.executable).parent)) or "desflow"
UNTIL = "30"  # the request's run length, in seconds
SEEDS = (42, 43, 44)  # the file's seed and the two after it
CAPACITY = "Shop.stats.resources.packers.capacity"


def run_records(seed):
    """Every record of one unpaced run, in the order the run publishes them."""
    done = subprocess.run(
        # 100000 is more events than the run makes, so it ends at UNTIL
        [DESFLOW, "run", str(MODEL), "--sample", "100000"]
        + ["--seed", str(seed), "--until", UNTIL],
        capture_output=True,
        text=True,
        check=True,
    )
    return [json.loads(line) for line in done.stdout.splitlines() if line[:1] == "{"]


@pytest.mark.parametrize("seed", SEEDS)
def test_orders_are_packed(seed):
    packed = [
        record
        for record in run_records(seed)
        if record["stream_type"] == "event"
        and record["value"].get("event_type") == "order_packed"
    ]
    assert packed, "the run published no packed order"
    assert all(record["value"]["shipping"] == "standard" for record in packed)


@pytest.mark.parametrize("seed", SEEDS)
def test_the_scenario_changes_the_packers(seed):
    capacity = {
        record["sim_ts"]: record["value"]
        for record in run_records(seed)
        if record.get("path_id") == CAPACITY
    }
    assert capacity[8] == 1
    assert capacity[12] == 3  # the scenario adds two packers at 10 seconds
    assert capacity[22] == 2  # and removes one at 20 seconds


def test_the_statistics():
    # The tests entry of model.yaml holds a value taken from the request
    done = subprocess.run(
        [DESFLOW, "test", str(MODEL)], capture_output=True, text=True, check=True
    )
    # desflow test passes a file with no tests, so check that one ran
    assert "no tests" not in done.stdout + done.stderr
```
