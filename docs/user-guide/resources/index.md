# Resources

This page is the map of the section: what a resource is, and which page shows what.

---

## What It Is

A resource is a number of identical units: packers, packing benches, checkout lanes, loading bays. A task asks for one unit, waits in the resource's queue while every unit is busy, holds the unit while it works, and gives it back. How many units there are is the resource's capacity, and it can change while the run goes on, as when packers go on a break.

---

## Pages in This Section

- [Units and Queues](units-and-queues.md): declare a resource, order its queue, limit the line, read who holds a unit, and see what a change of capacity does.
- [Priority and Preemption](priority-and-preemption.md): rank waiting requests by task priority or an expression, let a more urgent request take a unit from a less urgent task, and fail a resource at random. A lower `priority` value is more urgent.
- [Several Resources](several-resources.md): hold several resources in one task, and the order in which tasks acquire them.
- [Custom Resource Types](custom-resource-types.md): change how a resource grants units with your own class.

---

## How to Use It

Pick the page by the job:

| To do this | Read |
|---|---|
| Give a task a resource and set how many units there are | [Units and Queues](units-and-queues.md#declare-a-resource) |
| Let an order give up waiting, or turn it away from a long line | [Tasks](../tasks-and-processes/tasks.md#leave-the-line) and [Queue Limit](units-and-queues.md#queue-limit) |
| Serve urgent work first, or let it take a unit | [Priority and Preemption](priority-and-preemption.md) |
| Hold a picker and a bench together | [Several Resources](several-resources.md) |
| Take packers off the floor for a time | [Units and Queues](units-and-queues.md#under-live-changes) |
| Grant units by a rule of your own | [Custom Resource Types](custom-resource-types.md) |

---

## How It Works

A resource keeps a line of waiting requests and a list of the requests that hold a unit, as SimPy's `Resource` does, and follows its capacity in the registry. [How It Works](units-and-queues.md#how-it-works) on Units and Queues describes the grant. Amounts and items that tasks take and add, rather than hold, are [Stores and Containers](../model-parts/stores-and-containers.md).
