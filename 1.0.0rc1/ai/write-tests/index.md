# Write Tests

Ask for tests when a model will change, so that a change that breaks it is found. An assistant with the skill tests the statistics with `desflow test` and the records with pytest, and makes each test fail when the behaviour it covers changes.

______________________________________________________________________

## Ask

```text
Write tests for drafted_shop.yaml.
Take the expected values from the request, not from a run of the model.
```

______________________________________________________________________

## What the Assistant Does

The skill's testing guide gives the assistant these rules, each with a wrong and a right example:

- Take each expected value from the request, a hand calculation or `desflow explain`, never from the model under test.
- Set each tolerance from the spread between seeds: four standard deviations.
- Change the number a test is about by a small amount and run the test again. It must fail.
- Run every test unpaced, at a fixed seed and a fixed `logical_start_time`.
- Keep each test independent of the others, so it passes alone and in any order.

For the drafted shop, the request gives two values. An order every 2 seconds is 18,000 orders in 10 hours. 1.5 seconds of packing for every 2 seconds keeps the packer busy 75% of the time. A run of an hour varies too much between seeds for a 10% change to stand out, so the tests run for 10 hours:

```yaml
tests:
  # From the request: an order every 2 seconds is 18,000 orders in 10 hours. The
  # count varies by about 134 between seeds (the square root of 18,000), so 540 is
  # four standard deviations, and an arrival rate 10% higher, 19,800, fails
  - name: orders_packed_in_10_hours
    until: 10 hours  # long enough that a 10% change is more than the spread
    expect:
      tasks.pack_order.finished: {value: 18000, within: 540}
  # From the request: 1.5 seconds of packing for every 2 seconds is 75% of the
  # packer's time. Over 10 hours it varies by about 0.5 between seeds, so 2 is four
  # standard deviations, and the same 10% higher rate, 82.5%, fails
  - name: packer_busy_three_quarters
    until: 10 hours
    expect:
      resources.packers.utilization_avg: {value: 75, within: 2}
```

`desflow test` runs a Builder file with the same entries in a `tests.yaml` beside it.

`desflow test` does not run a Low-level file. The skill's check in [Write Another Form](https://jaehyeon.me/desflow/latest/ai/write-another-form/index.md) compares its records with the YAML's.

`desflow test` runs each entry once, unpaced, with no ingress and no egress:

```bash
desflow test drafted_shop.yaml
```

```text
ok drafted_shop.yaml: orders_packed_in_10_hours
ok drafted_shop.yaml: packer_busy_three_quarters
```

With the arrival rate 10% higher, both tests fail and name what they measured, 19,450 orders and 81% busy, so the tests check the rate they are about.

For the records, the skill gives the assistant a pytest template that runs the model with the `desflow` command at three seeds and checks the records the request names. [Checking a Model](https://jaehyeon.me/desflow/latest/learn/studies/checking-a-model/index.md) shows the checks a model needs beyond tests.

______________________________________________________________________

## Check the Reply

Read where each expected value came from. A value copied from a run of the model passes whatever the model does. Ask the assistant to show a test failing on a small change to the number it covers.
