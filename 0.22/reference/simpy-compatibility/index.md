# SimPy Compatibility

Every modelling feature of SimPy 4.1.2, 98 in all, with its status in YAML, the builder and the low-level API: native and checked, or not yet. A native feature is checked by conformance tests that run a plain SimPy 4.1.2 model and the same model in all three APIs, and compare event times, outcomes and balances. The page is generated from [`tests/conformance/register.yaml`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/register.yaml), the list the tests read, so a feature is native here only with its checks.

**48** features are native and checked in all three APIs, **48** are not yet, and **2** are differences kept by design. Each feature that is not yet native becomes native in all three APIs in 0.23.0, as the [Roadmap](https://jaehyeon.me/desflow/latest/about/roadmap/index.md) says.

The low-level API is SimPy with a few classes added, so most SimPy features work in it, those not yet checked included; [SimPy and desflow](https://jaehyeon.me/desflow/latest/des/simpy-and-desflow/index.md) explains how the two fit together. Where desflow differs from SimPy on purpose, the difference is listed under the table.

The sections are numbered by kind. **C1 to C7** are SimPy's core operations, such as events, conditions, interrupts, resources and stores. **S1 to S3** are support features desflow builds from those operations: breakdowns, schedules and replay. SimPy has no single call for them, so their conformance tests compare with the same behaviour written in plain SimPy.

| Section                                                        | Operations native in all three APIs |
| -------------------------------------------------------------- | ----------------------------------- |
| [C1: Events and Processes](#c1-events-and-processes)           | 9                                   |
| [C2: Wait for One or All](#c2-wait-for-one-or-all)             | 2                                   |
| [C3: Interrupts and Preemption](#c3-interrupts-and-preemption) | 3                                   |
| [C4: Resources](#c4-resources)                                 | 5                                   |
| [C5: Stores and Containers](#c5-stores-and-containers)         | 11                                  |
| [C6: Run Control](#c6-run-control)                             | 10                                  |
| [C7: Patience](#c7-patience)                                   | 2                                   |
| [S1: Breakdowns](#s1-breakdowns)                               | 1                                   |
| [S2: Schedules](#s2-schedules)                                 | 1                                   |
| [S3: Replay](#s3-replay)                                       | 1                                   |
| [Not Yet](#not-yet)                                            | none; 48 SimPy features to come     |
| [Differences Kept by Design](#differences-kept-by-design)      | 2 differences                       |

Under each feature, API details opens to show its SimPy construct, its form in YAML, the builder and the low-level API, and the conformance tests that check it.

## C1: Events and Processes

Waiting a fixed time, events a task can fire or fail, and processes: starting one, waiting for it, reading its return value or its failure, and inspecting an event or a process while it runs.

**Timed wait**: A task waits a set or sampled time, such as the time packing takes, before it goes on.

API details

- **SimPy:** `simpy.events.Timeout`
- **YAML:** a task with `service` and no `resource`
- **Builder:** `@app.task(service_id=...)` with no resource
- **Low-level:** `yield env.timeout(delay)`
- **Checks:** [`test_c1_timed_wait`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_timed_wait.py)
- **Taught on:** [Timed wait](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#a-timed-step)

**Signal event success and value**: One task fires a named signal with a value, and every task waiting for it wakes up and reads that value.

API details

- **SimPy:** `simpy.events.Event.succeed(value)`
- **YAML:** `signals: [restocked]`, `signal: restocked` on the firing task and `wait: {signal: restocked}`
- **Builder:** `add_signal`, `@app.task(signal=...)`, `wait={"signal": ...}`, or `context.signal(name).fire(value)`
- **Low-level:** `Signal(env, name)`: `yield signal.wait()` and `signal.fire(value)`
- **Checks:** [`test_c1_signal_event_success_and_value`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Signal event success and value](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#wait-signals-and-child-flows)

**Signal event failure**: A signal fires as a failure, and every task waiting for it fails with the same reason.

API details

- **SimPy:** `simpy.events.Event.fail(exception)`
- **YAML:** a failing task with `signal`; its waiters fail with its reason
- **Builder:** the same, or `context.signal(name).fail(reason)`
- **Low-level:** `signal.fail(reason)`; waiters receive `SignalFailed`
- **Checks:** [`test_c1_signal_event_failure`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Signal event failure](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#wait-signals-and-child-flows)

**Start and wait for a process**: A task starts a child flow, such as an inspection, and waits until it ends.

API details

- **SimPy:** `yield env.process(child())`
- **YAML:** `wait: {task: inspect}`
- **Builder:** `wait={"task": "inspect"}`, or `yield context.call("inspect")`
- **Low-level:** `yield env.process(child())`
- **Checks:** [`test_c1_start_and_wait_for_a_process`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Start and wait for a process](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#wait-signals-and-child-flows)

**Process return value**: The task that waited for a child flow reads what the child produced.

API details

- **SimPy:** a process's `return` value
- **YAML:** `wait.inspect.value.<field>`, the last payload of the child flow
- **Builder:** the same, or the value of `context.call(...)`
- **Low-level:** `value = yield env.process(child())`
- **Checks:** [`test_c1_process_return_value`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Process return value](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#wait-signals-and-child-flows)

**Process failure**: A failure inside a child flow reaches the task waiting for it, which can handle it or fail in turn.

API details

- **SimPy:** an exception raised in a process reaches the process that waits for it
- **YAML:** `fail`, `fail_reason` and `on_fail` on a task; a failure no `on_fail` handles fails the waiting task
- **Builder:** the same, and a function that raises fails its task
- **Low-level:** `try: yield env.process(child())` and `except`
- **Checks:** [`test_c1_process_failure`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_process_interaction.py), [`test_c1_process_failure_handled_by_on_fail`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_process_interaction.py)
- **Difference:** In YAML and the builder a failure no task waits for ends the entity's flow, publishes `failed` and is counted, and the run goes on; in SimPy, and in the low-level API, an exception nobody handles stops the run.
- **Taught on:** [Process failure](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#wait-signals-and-child-flows)

**Event inspection**: A condition reads whether a signal has fired, whether it succeeded, and its value.

API details

- **SimPy:** `Event.triggered`, `.processed`, `.ok`, `.value`
- **YAML:** `signal.<name>.count`, `.ok` and `.value` in a condition
- **Builder:** `context.signal(name).wait()` and its SimPy attributes, or the signal's `count`, `ok` and `value`
- **Low-level:** the same attributes of SimPy events and `Signal`
- **Checks:** [`test_c1_event_inspection`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Event inspection](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#wait-signals-and-child-flows)

**Process inspection**: After a wait, a task reads whether a child flow has finished, whether it succeeded, and its value.

API details

- **SimPy:** `Process.is_alive`, `.triggered`, `.ok`, `.value`
- **YAML:** `wait.<branch>.done`, `.ok` and `.value` after the wait
- **Builder:** the process `context.spawn` and `context.call` return
- **Low-level:** the process `env.process` returns
- **Checks:** [`test_c1_process_inspection`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Process inspection](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#wait-signals-and-child-flows)

**Wait for a shared condition**: A task waits until a condition on the model's state holds, such as stock reaching 5.

API details

- **SimPy:** a loop over a fresh "changed" event
- **YAML:** `wait: {condition: level.stock >= 5}`
- **Builder:** `wait={"condition": "level.stock >= 5"}`
- **Low-level:** `env.watch_changes(check)`, or a SimPy loop
- **Checks:** [`test_c1_wait_for_a_shared_condition`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Wait for a shared condition](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#wait-signals-and-child-flows)

## C2: Wait for One or All

Waiting for the first of several events, or for all of them, as SimPy's `AnyOf` and `AllOf` conditions do.

**Wait for one event**: A task waits for whichever of several events happens first, such as a delivery or a time limit.

API details

- **SimPy:** `simpy.events.AnyOf`, `a | b`
- **YAML:** `wait: {any: {stock: {signal: restocked}, limit: {timeout: 3}}}`
- **Builder:** `wait={"any": {...}}`
- **Low-level:** `yield a | b`
- **Checks:** [`test_c2_wait_for_one_event`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c2_wait_for_one_or_all.py), [`test_c2_wait_for_one_event_two_at_one_instant`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c2_wait_for_one_or_all.py)
- **Difference:** Losing signal, timeout and condition branches are dropped when the wait ends; losing child flows keep running, as SimPy leaves processes running.
- **Taught on:** [Wait for one event](https://jaehyeon.me/desflow/latest/guides/wait-for-one-or-all/#wait-for-the-first)

**Wait for all events**: A task waits until every one of several events has happened.

API details

- **SimPy:** `simpy.events.AllOf`, `a & b`
- **YAML:** `wait: {all: {...}}`
- **Builder:** `wait={"all": {...}}`
- **Low-level:** `yield a & b`
- **Checks:** [`test_c2_wait_for_all_events`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c2_wait_for_one_or_all.py), [`test_c2_wait_for_all_events_with_a_failing_branch`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c2_wait_for_one_or_all.py)
- **Taught on:** [Wait for all events](https://jaehyeon.me/desflow/latest/guides/wait-for-one-or-all/#wait-for-all)

## C3: Interrupts and Preemption

Interrupting a task while it waits or while it is in service, and a higher-priority request taking a resource unit from a lower-priority user.

**Interrupt while waiting**: Another task or a scenario step interrupts a task while it is still in a queue.

API details

- **SimPy:** `simpy.Process.interrupt` while the process waits for a request
- **YAML:** `interrupt: {task: waiter}` on a scenario step or a task; `on_interrupt` on the task
- **Builder:** `context.interrupt("waiter")`; `@app.task(on_interrupt=...)`
- **Low-level:** `process.interrupt(cause)`, caught as `simpy.Interrupt`
- **Checks:** [`test_c3_interrupt_while_waiting`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c3_interrupts.py)
- **Taught on:** [Interrupt while waiting](https://jaehyeon.me/desflow/latest/tutorials/interruptions/#interrupt-a-task)

**Interrupt during service**: A task is interrupted while it holds a resource, and resumes, restarts or abandons its work.

API details

- **SimPy:** `simpy.Process.interrupt` while the process holds a unit
- **YAML:** `on_interrupt: resume`, `restart` or `abandon`
- **Builder:** `@app.task(on_interrupt=...)`
- **Low-level:** `except simpy.Interrupt`, then request again
- **Checks:** [`test_c3_interrupt_during_service`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c3_interrupts.py), [`test_c3_two_interrupts_at_one_instant`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c3_interrupts.py)
- **Taught on:** [Interrupt during service](https://jaehyeon.me/desflow/latest/tutorials/interruptions/#resume-restart-or-abandon)

**Priority preemption**: A more urgent request takes a resource unit away from a less urgent user, who is interrupted.

API details

- **SimPy:** `simpy.resources.resource.PreemptiveResource`
- **YAML:** `resources.<name>.preempt: true` with `queue: priority`; `preempt: false` on a task opts out
- **Builder:** `add_resource(..., queue="priority", preempt=True)`
- **Low-level:** `DynamicResource(..., queue="priority", preempt=True)` and `request(priority, preempt=True)`
- **Checks:** [`test_c3_priority_preemption`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c3_interrupts.py), [`test_c3_priority_preemption_equal_priorities`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c3_interrupts.py), [`test_c3_two_preemptions_at_one_instant`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c3_interrupts.py), [`test_c3_preemption_tie_between_users`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c3_interrupts.py), [`test_c3_priority_preemption_low_level`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c3_interrupts.py)
- **Taught on:** [Priority preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/#preemption)

## C4: Resources

Requesting and releasing resource units, in order of arrival or of priority, cancelling a request before or after its grant, and holding several resources at once.

**Ordinary request and release**: A task takes one unit of a resource, such as a packer, waits in line if none is free, and gives it back when done.

API details

- **SimPy:** `simpy.resources.resource.Resource`
- **YAML:** `resource: packers` on a task
- **Builder:** `@app.task(resource_id="packers")`
- **Low-level:** `with packers.request() as req: yield req`
- **Checks:** [`test_c4_ordinary_request_and_release`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c4_resources.py)
- **Taught on:** [Ordinary request and release](https://jaehyeon.me/desflow/latest/user-guide/resources/#declare-a-resource)

**Priority request**: Waiting requests are served by priority, the lowest number first, instead of in order of arrival.

API details

- **SimPy:** `simpy.resources.resource.PriorityResource`
- **YAML:** `queue: priority` on the resource and `priority` on a task
- **Builder:** `add_resource(..., queue="priority")` and `@app.task(priority=...)`
- **Low-level:** `DynamicResource(..., queue="priority")` and `request(priority=...)`
- **Checks:** [`test_c4_priority_resource_request`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c4_resources.py), [`test_c4_priority_request_with_mixed_preempt_flags`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c4_resources.py)
- **Difference:** Equal priorities are served first in, first out, as in SimPy; desflow also offers `fifo` and `lifo` queues, under which a request's priority is ignored.
- **Taught on:** [Priority request](https://jaehyeon.me/desflow/latest/user-guide/resources/#task-priority)

**Cancel before a grant**: A task leaves the queue before it is served, so it never takes a unit.

API details

- **SimPy:** `Request.cancel()` before it is granted
- **YAML:** `patience`, or an interrupt with `on_interrupt: abandon`
- **Builder:** the same
- **Low-level:** `request.cancel()`, or leaving the `with` block
- **Checks:** [`test_c4_cancel_before_a_grant`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c4_resources.py), [`test_c4_renege_ahead_of_a_preempting_request`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c4_resources.py)
- **Taught on:** [Cancel before a grant](https://jaehyeon.me/desflow/latest/user-guide/resources/#cancel-and-release)

**Cancel after a grant**: A task that already holds a unit stops early and gives the unit back.

API details

- **SimPy:** `Request.cancel()` after the grant releases nothing; `Resource.release(request)` does
- **YAML:** an interrupt with `on_interrupt: abandon` gives the unit back
- **Builder:** the same
- **Low-level:** `request.cancel()` releases nothing; `resource.release(request)`
- **Checks:** [`test_c4_cancel_after_a_grant`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c4_resources.py)
- **Taught on:** [Cancel after a grant](https://jaehyeon.me/desflow/latest/user-guide/resources/#cancel-and-release)

**Hold several resources**: A task holds more than one resource at once, taken in a fixed order, and can hand them to the next task.

API details

- **SimPy:** nested requests, each held while the next is awaited
- **YAML:** `resource: [picker, bench]`, acquired in that order; `keep` hands units to the next task
- **Builder:** `@app.task(resource_id=["picker", "bench"], keep=[...])`
- **Low-level:** nested `with` blocks
- **Checks:** [`test_c4_hold_several_resources`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c4_resources.py), [`test_c4_keep_hands_a_unit_to_the_next_task`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c4_resources.py)
- **Difference:** A model whose tasks acquire two resources in opposite orders is refused when it is built, because those tasks could each wait for ever; SimPy runs it.
- **Taught on:** [Hold several resources](https://jaehyeon.me/desflow/latest/user-guide/resources/#several-resources)

## C5: Stores and Containers

Putting and getting items in a store and amounts in a container, with capacity limits, priority items, filtered gets, cancelled puts and gets, and the order of puts and gets at the same time.

**Bounded store put**: A task adds an item to a store, and waits when the store is full.

API details

- **SimPy:** `simpy.resources.store.Store.put`
- **YAML:** `put: {store: parcels}` on a task
- **Builder:** `@app.task(put={"store": "parcels"})`
- **Low-level:** `yield parcels.put(item)`
- **Checks:** [`test_c5_bounded_store_put`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_bounded.py)
- **Taught on:** [Bounded store put](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#take-and-add)

**Bounded store get**: A task takes an item from a store, and waits when the store is empty.

API details

- **SimPy:** `simpy.resources.store.Store.get`
- **YAML:** `get: {store: parcels}` on a task
- **Builder:** `@app.task(get={"store": "parcels"})`
- **Low-level:** `item = yield parcels.get()`
- **Checks:** [`test_c5_bounded_store_get`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_bounded.py)
- **Taught on:** [Bounded store get](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#take-and-add)

**Equal-time puts and gets**: When items are added and taken at the same moment, waiting tasks are served in the same order as in SimPy.

API details

- **SimPy:** `simpy.resources.store.Store`, one waiting get granted per trigger
- **YAML:** `get: {store: parcels}` and `put: {store: parcels}` on tasks
- **Builder:** `@app.task(get={"store": "parcels"})`
- **Low-level:** `yield parcels.put(item)`, `item = yield parcels.get()`
- **Checks:** [`test_c5_equal_time_puts_and_gets_follow_simpy_store`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_store_order.py), [`test_c5_equal_time_puts_and_gets_as_tasks`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_store_order.py)
- **Difference:** While a get with a filter waits, the store grants as SimPy's `FilterStore` does, looking at every waiting get each time.
- **Taught on:** [Equal-time puts and gets](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#take-and-add)

**Bounded quantity put**: A task adds an amount to a container, such as boxes to stock, and waits when there is no room.

API details

- **SimPy:** `simpy.resources.container.Container.put`
- **YAML:** `put: {container: stock, amount: 2}` on a task
- **Builder:** `@app.task(put={"container": "stock", "amount": 2})`
- **Low-level:** `yield stock.put(2)`
- **Checks:** [`test_c5_bounded_quantity_put`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_bounded.py)
- **Taught on:** [Bounded quantity put](https://jaehyeon.me/desflow/latest/tutorials/stock/#take-and-restock)

**Bounded quantity get**: A task takes an amount from a container, and waits until there is enough.

API details

- **SimPy:** `simpy.resources.container.Container.get`
- **YAML:** `get: {container: stock, amount: 1}` on a task
- **Builder:** `@app.task(get={"container": "stock", "amount": 1})`
- **Low-level:** `yield stock.get(1)`
- **Checks:** [`test_c5_bounded_quantity_get`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_bounded.py)
- **Taught on:** [Bounded quantity get](https://jaehyeon.me/desflow/latest/tutorials/stock/#take-and-restock)

**Priority item storage**: Items in a store come out by priority, the lowest number first, instead of in the order they went in.

API details

- **SimPy:** `simpy.resources.store.PriorityStore`
- **YAML:** `stores.<name>.priority: true` and a `put` with `priority`
- **Builder:** `add_store(..., priority=True)` and `put={"store": ..., "priority": ...}`
- **Low-level:** `DynamicStore(..., priority=True)` with `simpy.PriorityItem`
- **Checks:** [`test_c5_priority_item_storage`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_priority_store.py)
- **Difference:** Equal priorities come out in put order in the builder and YAML, which add a rising put order to the priority; a plain SimPy `PriorityStore` gives no order among equal priorities.
- **Taught on:** [Priority item storage](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#start-with-items-and-serve-by-priority)

**Filtered retrieval**: A task takes only an item that matches a condition, such as a parcel for its own area.

API details

- **SimPy:** `simpy.resources.store.FilterStore.get(filter)`
- **YAML:** `get: {store: parcels, filter: item.area == attr.area}`
- **Builder:** `get={"store": "parcels", "filter": ...}`, a condition or a function of the item
- **Low-level:** `parcels.get(filter=lambda item: ...)`
- **Checks:** [`test_c5_filtered_retrieval`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_inventory.py)
- **Difference:** A priority store takes no filter, because SimPy has no store that both ranks and filters.
- **Taught on:** [Filtered retrieval](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#pick-an-item-or-give-up-waiting)

**Cancel store get**: A task stops waiting for an item, for example when its patience runs out.

API details

- **SimPy:** `simpy.resources.base.Get.cancel()`
- **YAML:** `patience` on a task with a store `get`
- **Builder:** `@app.task(get={"store": ...}, patience=...)`
- **Low-level:** `get.cancel()`
- **Checks:** [`test_c5_cancel_store_get`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_inventory.py)
- **Taught on:** [Cancel store get](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#pick-an-item-or-give-up-waiting)

**Cancel quantity get**: A task stops waiting for an amount from a container.

API details

- **SimPy:** `simpy.resources.base.Get.cancel()`
- **YAML:** `patience` on a task with a container `get`
- **Builder:** `@app.task(get={"container": ...}, patience=...)`
- **Low-level:** `get.cancel()`
- **Checks:** [`test_c5_cancel_quantity_get`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_inventory.py)
- **Taught on:** [Cancel quantity get](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#pick-an-item-or-give-up-waiting)

**Cancel store put**: A task stops waiting for room in a full store.

API details

- **SimPy:** `simpy.resources.base.Put.cancel()`
- **YAML:** an interrupt with `on_interrupt: abandon` while the put waits
- **Builder:** the same
- **Low-level:** `put.cancel()`
- **Checks:** [`test_c5_cancel_store_put`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_inventory.py)
- **Taught on:** [Cancel store put](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#pick-an-item-or-give-up-waiting)

**Cancel quantity put**: A task stops waiting for room in a full container.

API details

- **SimPy:** `simpy.resources.base.Put.cancel()`
- **YAML:** an interrupt with `on_interrupt: abandon` while the put waits
- **Builder:** the same
- **Low-level:** `put.cancel()`
- **Checks:** [`test_c5_cancel_quantity_put`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c5_inventory.py)
- **Taught on:** [Cancel quantity put](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#pick-an-item-or-give-up-waiting)

## C6: Run Control

Starting a run from a given time and state, ending it at a time, at an event or on a condition, draining it, and stepping through it one event at a time.

**Initial time**: The simulation clock starts at a given time instead of 0.

API details

- **SimPy:** `simpy.core.Environment(initial_time=...)`
- **YAML:** `simulation.initial_time`
- **Builder:** `SimulationContext(..., initial_time=100)`
- **Low-level:** `DynamicRealtimeEnvironment(initial_time=100)`
- **Checks:** [`test_c6_initial_time`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_run_control.py)
- **Taught on:** [Initial time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#initial-time)

**Initial resources**: Each resource starts with a given number of units.

API details

- **SimPy:** `simpy.resources.resource.Resource(capacity=...)`
- **YAML:** `resources.<name>.current_cap`
- **Builder:** `add_resource(name, current_cap=2, ...)`
- **Low-level:** `CapacityConfig(current_cap=2, ...)`
- **Checks:** [`test_c6_initial_resources`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_initial_state.py)
- **Taught on:** [Initial resources](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#initial-time)

**Initial inventory**: Containers start with a given level, and stores with given items.

API details

- **SimPy:** `Container(init=...)`; items in a `Store`
- **YAML:** `containers.<name>.level` and `stores.<name>.items`
- **Builder:** `add_container(..., level=6)` and `add_store(..., items=[...])`
- **Low-level:** `ContainerConfig(level=6)` and `DynamicStore(..., items=[...])`
- **Checks:** [`test_c6_initial_inventory`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_initial_state.py)
- **Taught on:** [Initial inventory](https://jaehyeon.me/desflow/latest/user-guide/stores-and-containers/#start-with-items-and-serve-by-priority)

**Initial entity state**: Each arriving entity, such as an order, starts with given attributes.

API details

- **SimPy:** the attributes a process starts with
- **YAML:** `arrivals.<name>.attributes`
- **Builder:** `add_arrival(..., attributes={...})`
- **Low-level:** the values a process is started with
- **Checks:** [`test_c6_initial_entity_state`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_initial_state.py)
- **Taught on:** [Initial entity state](https://jaehyeon.me/desflow/latest/user-guide/task-flow/#attributes)

**Stop at a time**: The run ends at a given simulation time.

API details

- **SimPy:** `simpy.core.Environment.run(until=...)`
- **YAML:** `run.until`
- **Builder:** `app.run(until=...)`
- **Low-level:** `env.run(until=...)`
- **Checks:** [`test_c6_stop_at_a_time`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_run_control.py)
- **Taught on:** [Stop at a time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#run-length)

**Stop at an event**: The run ends when a given event happens, such as a closing signal, even before its end time.

API details

- **SimPy:** `simpy.core.Environment.run(until=event)`
- **YAML:** `run.stop: {signal: closing}` beside `run.until`
- **Builder:** `app.run(until=..., stop={"signal": "closing"})`, a signal's name, or a function returning an event
- **Low-level:** `env.run(until=event)`
- **Checks:** [`test_c6_stop_at_an_event`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_end_conditions.py), [`test_c6_stop_at_an_event_at_the_same_instant_as_until`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_end_conditions.py)
- **Difference:** When the stop and `until` fall at the same time, `until` ends the run first, as SimPy's own `until` would.
- **Taught on:** [Stop at an event](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#stop-early)

**Explicit stop condition**: The run ends as soon as a condition on the model's state holds.

API details

- **SimPy:** `env.run(until=event)` with a process that succeeds the event when the condition holds
- **YAML:** `run.stop: {condition: level.stock >= 5}`
- **Builder:** `app.run(until=..., stop={"condition": "..."})`
- **Low-level:** `env.watch_changes(check)` and `env.run(until=event)`
- **Checks:** [`test_c6_explicit_stop_condition`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_end_conditions.py)
- **Taught on:** [Explicit stop condition](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#stop-early)

**Stop arrivals and drain**: New arrivals stop, and the run goes on until everything already in the model has finished.

API details

- **SimPy:** interrupt the source, then `env.run(until=env.all_of(entities))`
- **YAML:** `run.drain: true` or `{limit: 1 h}`; `desflow run --drain`
- **Builder:** `app.run(until=..., drain=True)`, or `context.stop_arrivals()`
- **Low-level:** `source.interrupt()`, then `env.run(until=env.all_of(processes))`
- **Checks:** [`test_c6_stop_arrivals_and_drain`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_end_conditions.py), [`test_c6_stop_arrivals_and_drain_with_a_limit`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_end_conditions.py)
- **Taught on:** [Stop arrivals and drain](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#drain)

**Step one event**: The run advances one event at a time, so a script can watch each step.

API details

- **SimPy:** `simpy.core.Environment.step()`
- **YAML:** `SimulationContext.from_yaml(path).start()`, then `.step()`
- **Builder:** `app.start()`, `app.step()`, `app.close()`
- **Low-level:** `env.step()`
- **Checks:** [`test_c6_step_one_event`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_end_conditions.py)
- **Difference:** A blueprint has no stepping key; its context, from `from_yaml`, steps as a builder context does.
- **Taught on:** [Step one event](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#step-and-peek)

**Inspect next event time**: A script reads when the next event will happen without running it.

API details

- **SimPy:** `simpy.core.Environment.peek()`
- **YAML:** `SimulationContext.from_yaml(path).start()`, then `.peek()`
- **Builder:** `app.peek()`
- **Low-level:** `env.peek()`
- **Checks:** [`test_c6_inspect_next_event_time`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_end_conditions.py)
- **Taught on:** [Inspect next event time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#step-and-peek)

## C7: Patience

Customers who give up after waiting too long (reneging) and customers who do not join a queue that is too long (balking).

**Reneging**: A customer leaves the queue after waiting longer than their patience.

API details

- **SimPy:** `simpy.events.AnyOf` of a request and a timeout
- **YAML:** `patience: 3 min` on a task
- **Builder:** `@app.task(..., patience=180)`
- **Low-level:** `yield req | env.timeout(patience)`, then `req.triggered`
- **Checks:** [`test_c7_reneging`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c7_patience.py), [`test_c7_reneging_tie_timeout_scheduled_after_the_grant`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c7_patience.py), [`test_c7_reneging_tie_timeout_scheduled_before_the_grant`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c7_patience.py), [`test_c7_reneged_request_leaves_the_queue`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c7_patience.py), [`test_c7_patience_pauses_during_service`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c7_patience.py)
- **Taught on:** [Reneging](https://jaehyeon.me/desflow/latest/tutorials/patience/#leave-after-a-wait)

**Balking**: A customer does not join the queue at all when a condition holds, such as the queue being too long.

API details

- **SimPy:** a check before `Resource.request`
- **YAML:** `balk: <condition>` on a task
- **Builder:** `@app.task(..., balk="...")`
- **Low-level:** an `if` before the request
- **Checks:** [`test_c7_balking`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c7_patience.py)
- **Taught on:** [Balking](https://jaehyeon.me/desflow/latest/tutorials/patience/#refuse-to-join)

## S1: Breakdowns

A resource that fails and is repaired, with the work it interrupts.

**Breakdown and repair**: A resource fails from time to time and is repaired, interrupting the work it was doing.

API details

- **SimPy:** a repair process holding the machine on a `PreemptiveResource`
- **YAML:** `resources.<name>.breakdown: {time_to_failure, repair, work}`
- **Builder:** `add_breakdown(resource, time_to_failure={...}, repair={...}, work=...)`
- **Low-level:** `run_breakdowns(env, resource, sampler, work=...)`
- **Checks:** [`test_s1_failure_during_service`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_s1_breakdowns.py), [`test_s1_failure_during_service_work_finishes`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_s1_breakdowns.py), [`test_s1_planned_change_during_repair`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_s1_breakdowns.py)
- **Difference:** The resource is down on a calendar clock, from the failure to the repair, even while it waits for the work in service to finish with `work: finish`.
- **Taught on:** [Breakdown and repair](https://jaehyeon.me/desflow/latest/guides/breakdowns/#steps)

## S2: Schedules

A schedule of times at which an arrival rate or a capacity changes during a run.

**Finite schedules**: Arrival rates or capacities change at set times during the run.

API details

- **SimPy:** a process that changes values at set times, started before the others
- **YAML:** `scenario` steps with `at`, `path` and `value`
- **Builder:** `add_scenario(steps)` or `add_step(at, path=..., value=...)`
- **Low-level:** `run_scenario(env, steps, until=...)`
- **Checks:** [`test_s2_step_at_an_arrival_time`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_s2_schedules.py)
- **Difference:** A step applies before every other event at its time, so the arrival gap drawn then uses the new value.
- **Taught on:** [Finite schedules](https://jaehyeon.me/desflow/latest/guides/time-varying-arrivals/#boundaries)

## S3: Replay

Arrivals read from a local CSV or Parquet file instead of drawn at random.

**Local replay**: Arrivals come from rows in a local CSV or Parquet file instead of a random distribution.

API details

- **SimPy:** a loop over recorded rows, each a timeout to its time
- **YAML:** `arrivals.<name>.replay: {path, time, unit, fields, on_invalid}`
- **Builder:** `add_arrival_replay(name, source, ...)`
- **Low-level:** `read_trace(source, ...)` and a SimPy loop over its rows
- **Checks:** [`test_s3_replay_equal_times_invalid_rows_and_the_horizon`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_s3_replay.py)
- **Taught on:** [Local replay](https://jaehyeon.me/desflow/latest/tutorials/local-replay/#replay-the-rows)

## Not Yet

These features are not yet native and checked in all three APIs. "Exists" marks an API that has a form, which no conformance test compares with SimPy yet, and "checked" one that a test in the Checks column compares already; the low-level API is SimPy, so many work there already.

| Feature                                    | SimPy                                                      | YAML    | Builder | Low-level | Checks                                                                                                                             | Release |
| ------------------------------------------ | ---------------------------------------------------------- | ------- | ------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------- |
| Current time                               | `core.Environment.now`                                     | exists  | exists  | exists    | none                                                                                                                               | 0.23.0  |
| Active process                             | `core.Environment.active_process`                          | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Start a process without waiting for it     | `core.Environment.process`                                 | not yet | exists  | exists    | none                                                                                                                               | 0.23.0  |
| Schedule an event with a priority          | `core.Environment.schedule`                                | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Urgent and normal event priorities         | `events.URGENT`, `events.NORMAL`                           | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| A step processes only the model's events   | `core.Environment.step`                                    | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Value of a run stopped at an event         | `core.Environment.run`                                     | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Run in segments                            | `core.Environment.run`, called again                       | not yet | not yet | checked   | [`test_c6_stopped_and_resumed_run`](https://github.com/jaehyeon-kim/desflow/blob/main/tests/conformance/test_c6_end_conditions.py) | 0.23.0  |
| Subclass the environment                   | `core.Environment`                                         | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Real-time factor                           | `rt.RealtimeEnvironment(factor)`                           | exists  | exists  | exists    | none                                                                                                                               | 0.23.0  |
| Strict pacing                              | `rt.RealtimeEnvironment.strict`                            | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Strict by default                          | `rt.RealtimeEnvironment(strict=True)`                      | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Sync with the wall clock                   | `rt.RealtimeEnvironment.sync`                              | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| One-shot event                             | `events.Event`                                             | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Trigger from another event                 | `events.Event.trigger`                                     | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Callbacks                                  | `events.Event.callbacks`                                   | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Defused failure                            | `events.Event.defused`                                     | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Failure nobody handles                     | `core.Environment.run`                                     | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Timeout value                              | `events.Timeout(delay, value)`                             | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Process target                             | `events.Process.target`                                    | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Process name                               | `events.Process.name`                                      | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Wait for a process already running         | `yield process`                                            | not yet | exists  | exists    | none                                                                                                                               | 0.23.0  |
| Nested and mixed conditions                | `events.Condition`, `&` and `\|`                           | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| A condition with a custom rule             | `events.Condition(env, evaluate, events)`                  | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| A condition over no events                 | `events.AnyOf(env, [])`                                    | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Request, put or get as a wait branch       | `resources.base.Put`, `resources.base.Get`, `events.AnyOf` | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Put and get queues of a store or container | `resources.base.BaseResource.put_queue`, `.get_queue`      | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Custom resource types                      | `resources.base.BaseResource._do_put`, `._do_get`          | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Custom put and get queue types             | `resources.base.BaseResource.PutQueue`, `.GetQueue`        | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Custom put and get event classes           | `resources.base.BaseResource.put`, `.get`                  | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| A put or get the process does not wait for | `resources.base.Put` without `yield`                       | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Resource capacity defaults to 1            | `resources.resource.Resource(capacity=1)`                  | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Resource count                             | `resources.resource.Resource.count`                        | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Resource queue                             | `resources.resource.Resource.queue`                        | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Resource users                             | `resources.resource.Resource.users`                        | not yet | exists  | exists    | none                                                                                                                               | 0.23.0  |
| Release event and its request              | `resources.resource.Release.request`                       | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Usage since                                | `resources.resource.Request.usage_since`                   | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Default request priority 0                 | `resources.resource.PriorityRequest(priority=0)`           | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Preemption cause                           | `resources.resource.Preempted`                             | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| A waiting line with a limit                | `resources.resource.SortedQueue(maxlen)`                   | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Subclass a resource to change its rules    | `resources.resource.PreemptiveResource`                    | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Container with no capacity limit           | `resources.container.Container(capacity=inf)`              | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Read a store's items                       | `resources.store.Store.items`                              | not yet | exists  | exists    | none                                                                                                                               | 0.23.0  |
| Store with no capacity limit               | `resources.store.Store(capacity=inf)`                      | not yet | not yet | not yet   | none                                                                                                                               | 0.23.0  |
| Priorities of any orderable type           | `resources.store.PriorityItem`                             | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Start a process after a delay              | `util.start_delayed`                                       | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |
| Be told when an event happens              | `util.subscribe_at`                                        | not yet | not yet | exists    | none                                                                                                                               | 0.23.0  |

## Differences Kept by Design

- **A run with no until** (SimPy: `core.Environment.run()` returns when no event is left): A run with a sink or an ingress connector and no `until` runs for ever, because a live run has no end: the connectors keep events coming. Give it an `until`, with a `drain` when it should finish its work, or a `stop` that is sure to happen, or stop it with Ctrl+C.
- **Capacity 0** (SimPy: `resources.resource.Resource(capacity=0)` raises `ValueError`, and so do `Store` and `Container`): A resource, store or container may have a capacity of 0, at the start or later, because its capacity changes during a run: a shift with no packers, a shelf that is closed. At capacity 0 a resource grants no request, and a store or container accepts no put, until the capacity rises; a get still takes what a store or container holds.
