# Resources

This page is the map of the section: what a resource is, and which page shows what.

______________________________________________________________________

## What It Is

A resource is a number of identical units: packers, packing benches, checkout lanes, loading bays. A task asks for one unit, waits in the resource's queue while every unit is busy, holds the unit while it works, and gives it back. How many units there are is the resource's capacity, and it can change while the run goes on, as when packers go on a break.

______________________________________________________________________

## Pages in This Section

- [Units and Queues](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/index.md): declare a resource, order its queue, limit the line, read who holds a unit, and see what a change of capacity does.
- [Priority and Preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/index.md): rank waiting requests by task priority or an expression, let a more urgent request take a unit from a less urgent task, and fail a resource at random. A lower `priority` value is more urgent.
- [Several Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/index.md): hold several resources in one task, and the order in which tasks acquire them.
- [Custom Resource Types](https://jaehyeon.me/desflow/latest/user-guide/resources/custom-resource-types/index.md): change how a resource grants units with your own class.

______________________________________________________________________

## How to Use It

Pick the page by the job:

| To do this                                                     | Read                                                                                                                                                                                                      |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Give a task a resource and set how many units there are        | [Units and Queues](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#declare-a-resource)                                                                                          |
| Let an order give up waiting, or turn it away from a long line | [Tasks](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/#leave-the-line) and [Queue Limit](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#queue-limit) |
| Serve urgent work first, or let it take a unit                 | [Priority and Preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/index.md)                                                                                       |
| Hold a picker and a bench together                             | [Several Resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/index.md)                                                                                                   |
| Take packers off the floor for a time                          | [Units and Queues](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#under-live-changes)                                                                                          |
| Grant units by a rule of your own                              | [Custom Resource Types](https://jaehyeon.me/desflow/latest/user-guide/resources/custom-resource-types/index.md)                                                                                           |

______________________________________________________________________

## How It Works

A resource keeps a line of waiting requests and a list of the requests that hold a unit, as SimPy's `Resource` does, and follows its capacity in the registry. [How It Works](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#how-it-works) on Units and Queues describes the grant. Amounts and items that tasks take and add, rather than hold, are [Stores and Containers](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/index.md).
