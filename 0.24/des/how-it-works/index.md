# What Is Discrete-Event Simulation?

If you have never used simulation, start here. No maths is needed: a simulation is a way of playing out "what would happen if" on a computer, one step at a time, instead of guessing from averages.

______________________________________________________________________

## Simulation in Everyday Terms

Think of a packing shop. Orders arrive about every 2 seconds, and a packer takes 1.5 seconds to pack one. Will orders queue in the busy hour? What if a second packer starts at 9:30, or a delivery brings twenty orders at once?

You could answer by watching the shop with a notebook. You would not write something down every second, because most seconds nothing changes. You would write only the moments when something happens: an order arrives, a packer starts on it, the order is packed. Each of those moments is an **event**, and the notebook is enough to tell you who waited and for how long.

Look at the diagram for what the notebook holds: the shop is orders, a queue and packers, and the notebook lists only the moments when something changes.

A **discrete-event simulation** (DES) is that notebook, written by a computer. You describe the shop: how often orders come, how long packing takes, how many packers there are. The computer then plays out the events one after another, with random variation like in real life, and writes down what happens. Because it skips the quiet seconds, it can play out a day, or a year, in moments. Change the description, add a packer or double the orders, and run it again to see the difference.

desflow does this for any system with arrivals, work and limited capacity: a factory, a warehouse, a shop, a support desk, a network. What it adds is that every event is also sent out as a record, to Kafka, a database or files, so the simulation becomes a source of realistic data. [Building Blocks](https://jaehyeon.me/desflow/latest/user-guide/building-blocks/index.md) shows the parts you describe a model with, and [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md) explains why a queue grows the way it does.

Running a model is one step of a study. The question comes first, and the checking and the analysis of the runs come after. [A Simulation Study](https://jaehyeon.me/desflow/latest/des/a-simulation-study/index.md) walks through all of it.

______________________________________________________________________

## Events and the Event List

This section is the same idea in the terms the rest of the docs use. The terms follow chapter 3, General Principles, of Banks, Carson, Nelson and Nicol, *Discrete-Event System Simulation*, which we read through [lecture notes on that chapter](https://eg.bucknell.edu/~xmeng/Course/CS6337/Note/master/node15.html), and Ingalls, "Introduction to Simulation" (Winter Simulation Conference, 2001), which we read through [its published text](https://www.informs-sim.org/wsc01papers/002.PDF). The explanations are our own. A discrete-event simulation models a system as a series of **events**: an order arrives, a packer starts on it, the packer finishes. Each event happens at an instant, and between two events nothing changes. So the simulation keeps a list of what happens next, in time order, takes the earliest, moves its clock straight to that instant, and carries it out. Carrying out an event usually schedules more: an order that a packer starts on schedules its own finish 1.5 seconds later.

The clock jumps, so a simulated week with a few thousand events takes as long to compute as those events need, not a week. desflow runs on [SimPy](https://simpy.readthedocs.io/), which provides the event list and the clock. On top of SimPy it adds:

- **Pacing.** `factor` makes the clock follow the wall clock (`1.0`), run faster or slower, or not wait at all (`0.0`), so the same model can drive a live dashboard or write a month of history in seconds. [Time and Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/index.md) explains it.
- **Live values.** Every parameter is in a registry and can change while the simulation runs. [Live Parameters](https://jaehyeon.me/desflow/latest/user-guide/live-parameters/index.md) explains it.
- **Records.** What happens is published as events and telemetry to the sinks you attach. [Records and Telemetry](https://jaehyeon.me/desflow/latest/reference/records-and-telemetry/index.md) shows the formats.

Two events can fall at the same instant. They run by priority and then in the order they were scheduled. [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/index.md) shows the detail.

______________________________________________________________________

## Entities, Resources and Queues

The things that flow through a model are **entities**: orders, visitors, support calls. An entity arrives, needs something that is limited, such as a packer or a packing bench, holds it while it is served, and leaves. That limited thing is a **resource**. When every unit of a resource is busy, the next entity waits in its **queue**. Arrivals, service times, resources and queues are what a model is built from; [Building Blocks](https://jaehyeon.me/desflow/latest/user-guide/building-blocks/index.md) covers each one.

An entity can carry **attributes**: facts about it that stay with it as it moves, such as an order's priority or the area its items come from. A rule can read them to decide which task comes next or which order a packer takes first. [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/task-flow/#attributes) shows how, and [A Flow of Tasks](https://jaehyeon.me/desflow/latest/tutorials/flow-of-tasks/index.md) sets them as an order moves from step to step.

The time an entity spends in a model is of two kinds. An **activity** is a period of set length, such as the 1.5 seconds of packing. A **delay** is time spent waiting for something, such as a free packer, and its length is not set by the model: it comes out of the state of the system. These are Banks's meanings. Ingalls uses "delay" for a period of set length and calls the wait a queue. You describe the activities, and the simulation produces the delays. The wait in the queue is a delay, which is why it is a result and not an input. [Arrivals and Services](https://jaehyeon.me/desflow/latest/user-guide/arrivals-and-services/index.md) shows how to set an activity's length.

Queues are where the interesting behaviour comes from. Two models with the same average times can have very different waits, because a run of quick arrivals or one slow job holds everyone up. A simulation shows that directly, where an average cannot. [Queueing Basics](https://jaehyeon.me/desflow/latest/des/queueing-basics/index.md) explains the results of queueing theory that a model can be checked against.

______________________________________________________________________

## From Model to Records

A model on its own only moves its clock. What makes it useful here is what it publishes. A task publishes `queued`, `started` and a finished event for every entity, and telemetry reports what you choose, such as how many units of a resource are busy. A sink writes each record to Kafka, a database or files, so the simulation becomes a source of realistic data for whatever reads it.

Whether that data looks real depends on the model being **stable**: if work arrives faster than the resources can finish it, the queue grows without limit and every later event is delayed more. [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md) explains how to tell, and how to fix it.

How long to run depends on the question. A **terminating** run has a natural end: the shop opens empty and closes at the end of the day, and the day's results are the answer. A **steady-state** study asks how the shop behaves in the long run, after the effect of its empty start has gone, so the start of each run is left out as a warm-up. [Banks et al.'s lecture notes on output analysis](https://courses.grainger.illinois.edu/ece541/fa2016/OutputAnalysis.pdf) draw this line, and the same model can be either, depending on the question. [Output Analysis](https://jaehyeon.me/desflow/latest/des/output-analysis/index.md) explains both, and [Run Length](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/index.md) shows how to set the end of a run.
