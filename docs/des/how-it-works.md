# What Is Discrete-Event Simulation?

If you have never used simulation, start here. No maths is needed: a simulation is a way of playing out "what would happen if" on a computer, one step at a time, instead of guessing from averages.

---

## Simulation in Everyday Terms

Think of a coffee shop. Customers come in about once a minute, and the barista takes about 50 seconds per coffee. Will there be a queue at 9 in the morning? What if a second barista starts at 9:30, or a bus drops off twenty people at once?

You could answer by watching the shop with a notebook. You would not write something down every second, because most seconds nothing changes. You would write only the moments when something happens: a customer arrives, an order starts, a coffee is ready. Each of those moments is an **event**, and the notebook is enough to tell you who waited and for how long.

![A coffee shop: customers arrive about one a minute, queue, are served by a barista in about 50 seconds and leave; the notebook lists only the moments when something changes: Anna arrives at 9:00:00 and orders at once, Ben arrives at 9:00:40 and queues, Anna's coffee is ready at 9:00:50 and Ben orders, Ben's coffee is ready at 9:01:45](../assets/diagrams/coffee-shop.png)

A **discrete-event simulation** (DES) is that notebook, written by a computer. You describe the shop: how often customers come, how long a coffee takes, how many baristas there are. The computer then plays out the events one after another, with random variation like in real life, and writes down what happens. Because it skips the quiet seconds, it can play out a day, or a year, in moments. Change the description, add a barista or double the customers, and run it again to see the difference.

desflow does this for any system with arrivals, work and limited capacity: a factory, a warehouse, a shop, a support desk, a network. What it adds is that every event is also sent out as a record, to Kafka, a database or files, so the simulation becomes a source of realistic data. [Building Blocks](../user-guide/building-blocks.md) shows the parts you describe a model with, and [Queues and Capacity](../tutorials/queues-and-capacity.md) explains why a queue grows the way it does.

---

## Events and the Event List

This section is the same idea in the terms the rest of the docs use. A discrete-event simulation models a system as a series of **events**: an order arrives, a packer starts on it, the packer finishes. Each event happens at an instant, and between two events nothing changes. So the simulation keeps a list of what happens next, in time order, takes the earliest, moves its clock straight to that instant, and carries it out. Carrying out an event usually schedules more: an order that a packer starts on schedules its own finish 1.5 seconds later.

![The simulation clock jumps from event to event: order 1 arrives and starts at 0.0 s and finishes at 1.5 s, order 2 arrives and starts at 2.6 s and finishes at 4.1 s. Just after 0.0 s the event list holds order 1 finishing at 1.5 s and order 2 arriving at 2.6 s, so the clock jumps to 1.5 s](../assets/diagrams/event-clock.png)

The clock jumps, so a simulated week with a few thousand events takes as long to compute as those events need, not a week. desflow runs on [SimPy](https://simpy.readthedocs.io/), which provides the event list and the clock. On top of SimPy it adds:

- **Pacing.** `factor` makes the clock follow the wall clock (`1.0`), run faster or slower, or not wait at all (`0.0`), so the same model can drive a live dashboard or write a month of history in seconds. [Time and Pacing](../user-guide/time-and-pacing.md) explains it.
- **Live values.** Every parameter is in a registry and can change while the simulation runs. [Live Parameters](../user-guide/live-parameters.md) explains it.
- **Records.** What happens is published as events and telemetry to the sinks you attach. [Records and Telemetry](../reference/records-and-telemetry.md) shows the formats.

---

## Entities, Resources and Queues

The things that flow through a model are **entities**: orders, visitors, support calls. An entity arrives, needs something that is limited, such as a packer or a packing bench, holds it while it is served, and leaves. That limited thing is a **resource**. When every unit of a resource is busy, the next entity waits in its **queue**. Arrivals, service times, resources and queues are what a model is built from; [Building Blocks](../user-guide/building-blocks.md) covers each one.

Queues are where the interesting behaviour comes from. Two models with the same average times can have very different waits, because a run of quick arrivals or one slow job holds everyone up. A simulation shows that directly, where an average cannot.

---

## From Model to Records

A model on its own only moves its clock. What makes it useful here is what it publishes. A task publishes `queued`, `started` and a finished event for every entity, and telemetry reports what you choose, such as how many units of a resource are busy. A sink writes each record to Kafka, a database or files, so the simulation becomes a source of realistic data for whatever reads it.

Whether that data looks real depends on the model being **stable**: if work arrives faster than the resources can finish it, the queue grows without limit and every later event is delayed more. [Queues and Capacity](../tutorials/queues-and-capacity.md) explains how to tell, and how to fix it.
