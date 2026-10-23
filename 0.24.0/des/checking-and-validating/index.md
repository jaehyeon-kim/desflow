# Checking and Validating

A model that runs is not a model that is right. This page explains how to check a model in two ways that need different evidence: **verification** asks whether the program does what the model on paper says, and **validation** asks whether that model is a good enough picture of the real system for the question. Both are done on the way through a [simulation study](https://jaehyeon.me/desflow/latest/des/a-simulation-study/index.md), and neither is finished by one test.

The two terms, and the trace, the reasonableness checks, review by someone else, calibration and the three-step approach, follow Banks, Carson, Nelson and Nicol, *Discrete-Event System Simulation*, chapter 10, and Law, "How to Conduct a Successful Simulation Study" (Winter Simulation Conference, 2003). We read the chapter through [lecture notes on it](https://courses.ie.bilkent.edu.tr/ie324/wp-content/uploads/sites/4/2016/09/Validation.pdf), and [the paper](https://www.informs-sim.org/wsc03papers/009.pdf) itself. The checks with Little's law, with a formula and with fixed gaps, the extreme-condition tests, the explanations and the shop examples are our own.

______________________________________________________________________

## Verification and Validation

Banks et al., as the [lecture notes](https://courses.ie.bilkent.edu.tr/ie324/wp-content/uploads/sites/4/2016/09/Validation.pdf) give it, put the difference briefly: verification is building the model correctly, and validation is building the correct model. Look at the diagram for which link each one checks. Verification compares the program with the model on paper, which says the arrivals, the service times and the number of packers. Validation compares the program's results with the real shop.

A model can pass one and fail the other. A program can follow the model on paper exactly, and the model can leave out the thing that makes the real shop slow. A model can also describe the shop well and have a bug in its code. Law adds **credibility**: a model is credible when the people who will use its results accept them, and walking them through the assumptions is how a study earns it.

______________________________________________________________________

## Read the Trace

The cheapest check is to read what a run did. A **trace** is the record of each event in time order. Run a few orders and follow one by hand: it should arrive, wait while the packer is busy, start when the packer is free and finish after the service time. The records of a desflow run are a trace, and [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/index.md) reads the first of them. The [lecture notes](https://courses.ie.bilkent.edu.tr/ie324/wp-content/uploads/sites/4/2016/09/Validation.pdf) on Banks et al. list a trace, examining the output for reasonableness under a variety of input settings, and having someone else check the model among the ways to find errors in a program.

______________________________________________________________________

## Work a Case by Hand

Choose inputs for which the answer can be worked out on paper, run them, and compare. Constant gaps and constant service times make every number exact. This function runs the shop with them:

```python
def run_fixed(gap: float, service: float, until: float, packers: int = 1) -> dict:
    """The shop with constant gaps and constant packing times, so every number can be worked out by hand."""
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.add_arrival("orders", dist="constant", value=gap)
    app.add_service("packing", dist="constant", value=service)
    app.add_resource("packers", current_cap=packers, max_cap=packers)
    app.with_stats("typical", interval=3600)

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    app.run(until=until)
    return app.env.stats.readings()
```

With one order every 3 seconds and 1.5 seconds to pack, orders arrive at 3, 6 and on to 3,597 seconds, so 1,199 arrive in the hour. A packer is free each time, so no order waits, and the packer works 1,199 × 1.5 seconds of the 3,600, which is 49.96%. With one order every second the packer can never keep up: 3,599 orders arrive, 2,399 finish, and 1,200 are in the shop at the end. The code checks each of these numbers:

```python
# One order every 3 s and 1.5 s to pack: orders arrive at 3, 6, ... 3597 s, so 1,199 of
# them, and none waits. The packer works 1,199 x 1.5 s of the 3,600 s: 49.96%.
easy = run_fixed(gap=3, service=1.5, until=3600)
assert easy["Shop.stats.flows.orders.started"] == 1199
assert easy["Shop.stats.tasks.pack_order.wait_max"] == 0
assert (
    abs(easy["Shop.stats.resources.packers.utilization_avg"] - 1199 * 1.5 / 3600 * 100)
    < 0.01
)

# One order every second and 1.5 s to pack: the packer is never idle. The 3,599 orders
# that arrive leave 2,399 finished, so 1,200 are still in the shop.
overloaded = run_fixed(gap=1, service=1.5, until=3600)
assert overloaded["Shop.stats.flows.orders.started"] == 3599
assert overloaded["Shop.stats.tasks.pack_order.finished"] == 2399
assert overloaded["Shop.stats.flows.orders.in_system"] == 3599 - 2399
assert overloaded["Shop.stats.resources.packers.utilization_avg"] > 99.9
```

The second case is also an extreme-condition test: with demand above capacity, the queue must grow for as long as the run lasts. A model that cannot show this has a fault. Two more cases of this kind are no arrivals, which should give no orders started or finished, and a huge number of packers, which should give no waits. The code runs both, with one order every 10,000 seconds in an hour and with 100 packers for an order a second:

```python
# No order arrives within the hour: none starts and none finishes.
no_arrivals = run_fixed(gap=10_000, service=1.5, until=3600)
assert no_arrivals.get("Shop.stats.flows.orders.started", 0) == 0
assert no_arrivals.get("Shop.stats.tasks.pack_order.finished", 0) == 0

# A hundred packers for an order a second: a packer is always free, so no order waits.
many_packers = run_fixed(gap=1, service=1.5, until=3600, packers=100)
assert many_packers["Shop.stats.flows.orders.started"] == 3599
assert many_packers["Shop.stats.tasks.pack_order.wait_max"] == 0
```

______________________________________________________________________

## Check the Statistics Against Each Other

Some relations hold for every model that settles, so they test the statistics without any hand calculation. **Little's law** is the main one: the average number of orders in a system equals the arrival rate times the average time an order spends in it. The queue length, the arrival rate and the mean wait are measured separately, so if they disagree, one of them is wrong. [Queueing Basics](https://jaehyeon.me/desflow/latest/des/queueing-basics/index.md) runs this check on the shop's busy hour: a queue of 3.417 orders against 1.782 orders a second times a wait of 1.9175 seconds, which is 3.417. The code runs that busy hour and checks the figures:

```python
# Little's law on the busy hour of Queueing Basics: queue length = arrival rate x mean wait
rate, (queue, wait), _ = little()
print(
    f"queue {queue:.3f}, arrival rate {rate:.3f} x wait {wait:.4f} = {rate * wait:.3f}"
)
```

```text
queue 3.417, arrival rate 1.782 x wait 1.9175 = 3.417
```

[Measuring a Run](https://jaehyeon.me/desflow/latest/tutorials/measuring-a-run/index.md) shows the same idea on a shorter run, and the counts in [Statistics](https://jaehyeon.me/desflow/latest/user-guide/statistics/index.md) must also add up: when no order balks or reneges, the orders started over the whole run are those finished plus those still in the shop.

______________________________________________________________________

## Check Against a Formula

For a simple case there may be a formula. One packer with exponential gaps and service times at utilisation 0.75 has a mean wait of 4.5 seconds when the service time averages 1.5 seconds. A run of the model, repeated with several seeds, must produce an interval that contains it. [Queueing Basics](https://jaehyeon.me/desflow/latest/des/queueing-basics/index.md) gives the formulas for one and several servers, and shows runs of the shop within their intervals. A formula tests the part of the model it covers. It cannot test priorities, breakdowns or changing demand, which is why those are the parts to check by hand and by comparison with the real shop.

______________________________________________________________________

## Compare With the Real Shop

Validation needs evidence from the real system. Law suggests comparing the model's results with the existing system's, where there is one, along with a walk-through of the assumptions by the people who know the system, and sensitivity analysis. For the shop, give the model the inputs of a real day and compare its mean wait, queue length and utilisation with what happened. They need to agree closely enough for the decision, not exactly.

The [lecture notes](https://courses.ie.bilkent.edu.tr/ie324/wp-content/uploads/sites/4/2016/09/Validation.pdf) on Banks et al. describe calibration as iterative: compare the model with the real system, adjust it, compare again. They also have three steps, which are to build a model with high face validity, to validate the model's assumptions, and to compare input-output behaviour with the real system's data, so the assumptions and inputs are checked and not only the outputs. [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/index.md) and [Fit the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/index.md) cover the inputs, and [Local Replay](https://jaehyeon.me/desflow/latest/tutorials/local-replay/index.md) runs a model on the arrivals of a real trace. When the real system does not exist yet, as for a proposed design, there is nothing to compare with. Then validity rests on the assumptions and on sensitivity analysis.

______________________________________________________________________

## Test the Sensitivity

A model has inputs that are estimates. **Sensitivity analysis** changes them and sees whether the answer changes. If a 6% change in the arrival rate changes the decision, the estimate needs more work. If it does not, it does not. [Parameter Sweeps](https://jaehyeon.me/desflow/latest/tutorials/parameter-sweeps/index.md) does it for the shop: with three packers a rate 6% above the estimate more than doubles the mean wait to 5.55 seconds, and with four packers the wait is at most 0.35 seconds at every rate, so the choice of four packers holds whatever the demand within the range.

______________________________________________________________________

## What to Check, and When

Check as you build, not at the end. After each part is added, run a case with a known answer. Before the experiments, check the statistics against each other and against a formula where one exists. Before the results are used, compare with the real shop or say that no comparison was possible. [Checking a Model](https://jaehyeon.me/desflow/latest/tutorials/checking-a-model/index.md) applies these checks to the shop, written as tests. [Readiness and Reporting](https://jaehyeon.me/desflow/latest/des/readiness-and-reporting/index.md) lists what a model needs before its results are trusted, and [Common Mistakes](https://jaehyeon.me/desflow/latest/des/common-mistakes/index.md) lists the errors these checks find.
