# Draft a Model

Describe a process in your own words, and an assistant with the skill turns it into a blueprint. It marks every number it had to assume, checks the load against queueing theory, and runs a study before it gives you an answer.

The GIF follows one request, the packing shop of the [Learn tutorial](https://jaehyeon.me/desflow/latest/learn/first-model/first-simulation/index.md), through the steps the skill teaches:

The sections below hold each step's text, so you can copy the request and check each answer.

______________________________________________________________________

## Ask

Paste the request into an assistant that has the skill:

```text
A shop packs customer orders.
An order arrives about every 2 seconds.
One packer packs each order in about 1.5 seconds.
How long does an order wait?
```

______________________________________________________________________

## Draft

The skill tells the assistant to start from the nearest example (`desflow examples list`), and to build the model in stages, running the file after each one. It asks you for the numbers the description does not give. When it has to assume one, it marks it with a comment and lists the assumed numbers first in its reply, because you must confirm them.

The description does not say whether orders arrive at random or how much the packing time varies. The draft assumes both:

```yaml
simulation:
  sim_id: Shop
  factor: 0  # no pacing: the run ends as soon as it is done
  random_seed: 42
  # a fixed date, so two runs give the same records
  logical_start_time: '2026-01-05T09:00:00Z'

resources:
  packers: {current_cap: 1, max_cap: 1}  # said: one packer

services:
  # said: 1.5 seconds; assumed: every order takes the same time
  packing: {dist: constant, value: 1.5}

arrivals:
  # said: an order every 2 seconds on average; assumed: orders arrive at random
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}
```

The skill drafts a blueprint, so the drafted model is the YAML tab. [Write Another Form](https://jaehyeon.me/desflow/latest/ai/write-another-form/index.md) has its Builder form.

The skill drafts a blueprint, so the drafted model is the YAML tab. [Write Another Form](https://jaehyeon.me/desflow/latest/ai/write-another-form/index.md) has its Low-level form.

______________________________________________________________________

## Check the Load

Before it runs anything, the assistant checks that the packer can keep up. `desflow explain` gives the queueing formulas' answer:

```bash
desflow explain drafted_shop.yaml
```

```text
Shop
`-- orders (arrival, rate 0.5/s)
    `-- spawn -> pack_order (task, rate 0.5/s)
        +-- uses packers (resource)
        `-- next (share 1) -> exit
Resources
+------------------------------------------------------------+
| resource |   λ |      μ | c |    ρ |   Wq |     L | status |
|----------+-----+--------+---+------+------+-------+--------|
| packers  | 0.5 | 0.6667 | 1 | 0.75 | 2.25 | 1.875 | OK     |
+------------------------------------------------------------+
Bottleneck: packers
Hints for utilisation 0.85:
  packers with 1 unit: utilisation 0.75, at most 0.85
  packers with 1 unit: utilisation 0.85 at a request every 1.76 seconds (34 a minute, 0.5667 a second)
```

The packer is busy 75% of the time (`ρ`), so the queue does not grow without limit, and the formulas give a mean wait of 2.25 seconds (`Wq`). When a model routes on a variable or has scenario steps, `desflow explain` gives no estimate, and the skill tells the assistant to check the load by hand: the work of one case, times the arrival rate, must fit inside the capacity. [Command Line](https://jaehyeon.me/desflow/latest/cli/#check-it-before-a-long-run) shows more of `desflow explain`, and [Run a Study](https://jaehyeon.me/desflow/latest/ai/run-a-study/index.md) compares designs.

______________________________________________________________________

## Answer From Replications

One run is one sample, so the skill tells the assistant to answer from replications. 20 runs of an hour, each with its own seed spawned from 42:

```bash
desflow run drafted_shop.yaml --replications 20 | grep -E "^measure|wait_mean"
```

```text
measure                                n          mean    half-width (95%)
tasks.pack_order.wait_mean            20         2.383              0.1828
```

The mean wait is 2.38 ± 0.18 seconds. The interval, 2.20 to 2.57, holds the 2.25 of the formulas, so the model and the theory agree. The run starts with an empty shop and has no warm-up, which [Trusting Results](https://jaehyeon.me/desflow/latest/learn/studies/trusting-results/index.md) explains how to choose.

______________________________________________________________________

## Check the Reply

The skill tells the assistant to copy a checklist into its reply and tick each line: every behaviour the request names, the key that makes it, the record that shows it, and a run of the skill's own check on the files. Read the assumed numbers first. Then check that the answer is an interval from replications, not the result of one run.

______________________________________________________________________

## Full Files

Full files

```yaml
# Drafted from a description of the packing shop. Each value says where it came from:
# "said" means the description gives it, and "assumed" means the user has not confirmed
# it.
# Run it with: desflow run drafted_shop.yaml
simulation:
  sim_id: Shop
  factor: 0  # no pacing: the run ends as soon as it is done
  random_seed: 42
  # a fixed date, so two runs give the same records
  logical_start_time: '2026-01-05T09:00:00Z'

resources:
  packers: {current_cap: 1, max_cap: 1}  # said: one packer

services:
  # said: 1.5 seconds; assumed: every order takes the same time
  packing: {dist: constant, value: 1.5}

arrivals:
  # said: an order every 2 seconds on average; assumed: orders arrive at random
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

egress:
  - {type: Console}

run: {until: 1 hour}  # assumed: one hour answers the question

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

[Write Another Form](https://jaehyeon.me/desflow/latest/ai/write-another-form/#full-files) has the Builder form.

[Write Another Form](https://jaehyeon.me/desflow/latest/ai/write-another-form/#full-files) has the Low-level form.
