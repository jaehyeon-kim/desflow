# Tasks and Processes

This page is the map of the section: what a task and a process are, and which page shows what.

______________________________________________________________________

## What It Is

A **task** is what one entity does: an order being packed, a return being logged. A **process** is anything else that runs over time: packers going on a break every hour, a speed that drifts, a shift plan. Both are SimPy generators, functions that `yield` to wait. The builder and YAML write the common ones for you, and the low-level API writes them by hand.

______________________________________________________________________

## Pages in This Section

- [Tasks](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/index.md): write a task, its keys, how an order leaves the line, what a task does when it is interrupted, what `run()` does and what a task does step by step.
- [Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/index.md): run work alongside the tasks that belongs to no entity.
- [Signals and Waiting](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/index.md): wait for signals, conditions and child flows, start flows, and run actions when an event happens.
- [Failures](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/failures/index.md): when a task fails, what a failure does, and what happens to one nobody handles.

______________________________________________________________________

## How to Use It

Pick the page by the job:

| To do this                                                  | Read                                                                                                                  |
| ----------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Write the work one order does, and the records it publishes | [Tasks](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/index.md)                             |
| Let an order give up waiting, or turn it away               | [Tasks](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/#leave-the-line)                      |
| Choose what an order does after a task                      | [Task Flow](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/index.md)                             |
| Run a break, a drift or a schedule                          | [Processes](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/index.md)                     |
| Make a task wait for another task, a signal or a time       | [Signals and Waiting](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/index.md) |
| Handle a task that cannot finish                            | [Failures](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/failures/index.md)                       |

______________________________________________________________________

## How It Works

`run()` builds the environment, registers the parameters, builds the resources, stores and containers, and starts the arrivals and processes. [What run() Does](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/#what-run-does) lists the steps in order, and [What @app.task Does](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/#what-apptask-does) lists the steps of one task.
