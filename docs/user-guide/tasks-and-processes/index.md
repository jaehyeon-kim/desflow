# Tasks and Processes

This page is the map of the section: what a task and a process are, and which page shows what.

---

## What It Is

A **task** is what one entity does: an order being packed, a return being logged. A **process** is anything else that runs over time: packers going on a break every hour, a speed that drifts, a shift plan. Both are SimPy generators, functions that `yield` to wait. The builder and YAML write the common ones for you, and the low-level API writes them by hand.

---

## Pages in This Section

- [Tasks](tasks.md): write a task, its keys, how an order leaves the line, what a task does when it is interrupted, what `run()` does and what a task does step by step.
- [Processes](processes.md): run work alongside the tasks that belongs to no entity.
- [Signals and Waiting](signals-and-waiting.md): wait for signals, conditions and child flows, start flows, and run actions when an event happens.
- [Failures](failures.md): when a task fails, what a failure does, and what happens to one nobody handles.

---

## How to Use It

Pick the page by the job:

| To do this | Read |
|---|---|
| Write the work one order does, and the records it publishes | [Tasks](tasks.md) |
| Let an order give up waiting, or turn it away | [Tasks](tasks.md#leave-the-line) |
| Choose what an order does after a task | [Task Flow](../model-parts/task-flow.md) |
| Run a break, a drift or a schedule | [Processes](processes.md) |
| Make a task wait for another task, a signal or a time | [Signals and Waiting](signals-and-waiting.md) |
| Handle a task that cannot finish | [Failures](failures.md) |

---

## How It Works

`run()` builds the environment, registers the parameters, builds the resources, stores and containers, and starts the arrivals and processes. [What run() Does](tasks.md#what-run-does) lists the steps in order, and [What @app.task Does](tasks.md#what-apptask-does) lists the steps of one task.
