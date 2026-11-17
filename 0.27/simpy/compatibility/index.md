# SimPy Compatibility

Every modelling feature of SimPy 4.1.2, 98 in all, with its status in YAML, the builder and the low-level API: native and checked, or a difference kept by design. A native feature is checked by conformance tests that run a plain SimPy 4.1.2 model and the same model in all three APIs, and compare event times, outcomes and balances. The page is generated from [`tests/conformance/register.yaml`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/register.yaml), the list the tests read, so a feature is native here only with its checks.

**96** features are native and checked in all three APIs, and **2** are differences kept by design. The last section lists those and one more, strict pacing at factor 0, which is not a SimPy feature of its own.

The low-level API is SimPy with a few classes added, so most SimPy features work in it; [SimPy and desflow](https://jaehyeon.me/desflow/latest/simpy/index.md) explains how the two fit together. Where desflow differs from SimPy on purpose, the difference is listed under the table.

The sections are numbered by kind. **C1 to C7** are SimPy's core operations, such as events, conditions, interrupts, resources and stores. **S1 to S3** are support features desflow builds from those operations: breakdowns, schedules and replay. SimPy has no single call for them, so their conformance tests compare with the same behaviour written in plain SimPy.

| Section                                                        | Operations native in all three APIs |
| -------------------------------------------------------------- | ----------------------------------- |
| [C1: Events and Processes](#c1-events-and-processes)           | 18                                  |
| [C2: Wait for One or All](#c2-wait-for-one-or-all)             | 6                                   |
| [C3: Interrupts and Preemption](#c3-interrupts-and-preemption) | 4                                   |
| [C4: Resources](#c4-resources)                                 | 12                                  |
| [C5: Stores and Containers](#c5-stores-and-containers)         | 20                                  |
| [C6: Run Control](#c6-run-control)                             | 21                                  |
| [C7: Patience](#c7-patience)                                   | 2                                   |
| [S1: Breakdowns](#s1-breakdowns)                               | 1                                   |
| [S2: Schedules](#s2-schedules)                                 | 1                                   |
| [S3: Replay](#s3-replay)                                       | 1                                   |
| [Differences Kept by Design](#differences-kept-by-design)      | 3 differences                       |

Under each feature, API details opens to show its SimPy construct, its form in YAML, the builder and the low-level API, and the conformance tests that check it.

## C1: Events and Processes

Waiting a fixed time, events a task can fire or fail, including an event that triggers once, and processes: starting one, with or without a delay and without waiting for it, waiting for it, reading its return value or its failure, being told when it ends, running actions when an event happens, and inspecting an event, a process or the running process while it runs.

**Timed wait**: A task waits a set or sampled time, such as the time packing takes, before it goes on.

API details

- **SimPy:** `simpy.events.Timeout`
- **YAML:** a task with `service` and no `resource`
- **Builder:** `@app.task(service_id=...)` with no resource
- **Low-level:** `yield env.timeout(delay)`
- **Checks:** [`test_c1_timed_wait`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_timed_wait.py)
- **Taught on:** [Timed wait](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/#a-timed-step)

**Signal event success and value**: One task fires a named signal with a value, and every task waiting for it wakes up and reads that value.

API details

- **SimPy:** `simpy.events.Event.succeed(value)`
- **YAML:** `signals: [restocked]`, `signal: restocked` on the firing task and `wait: {signal: restocked}`
- **Builder:** `add_signal`, `@app.task(signal=...)`, `wait={"signal": ...}`, or `context.signal(name).fire(value)`
- **Low-level:** `Signal(env, name)`: `yield signal.wait()` and `signal.fire(value)`
- **Checks:** [`test_c1_signal_event_success_and_value`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Signal event success and value](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**Signal event failure**: A signal fires as a failure, and every task waiting for it fails with the same reason.

API details

- **SimPy:** `simpy.events.Event.fail(exception)`
- **YAML:** a failing task with `signal`; its waiters fail with its reason
- **Builder:** the same, or `context.signal(name).fail(reason)`
- **Low-level:** `signal.fail(reason)`; waiters receive `SignalFailed`
- **Checks:** [`test_c1_signal_event_failure`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Signal event failure](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**Start and wait for a process**: A task starts a child flow, such as an inspection, and waits until it ends.

API details

- **SimPy:** `yield env.process(child())`
- **YAML:** `wait: {task: inspect}`
- **Builder:** `wait={"task": "inspect"}`, or `yield context.call("inspect")`
- **Low-level:** `yield env.process(child())`
- **Checks:** [`test_c1_start_and_wait_for_a_process`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Start and wait for a process](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**Process return value**: The task that waited for a child flow reads what the child produced.

API details

- **SimPy:** a process's `return` value
- **YAML:** `wait.inspect.value.<field>`, the last payload of the child flow
- **Builder:** the same, or the value of `context.call(...)`
- **Low-level:** `value = yield env.process(child())`
- **Checks:** [`test_c1_process_return_value`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Process return value](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**Process failure**: A failure inside a child flow reaches the task waiting for it, which can handle it or fail in turn.

API details

- **SimPy:** an exception raised in a process reaches the process that waits for it
- **YAML:** `fail`, `fail_reason` and `on_fail` on a task; a failure no `on_fail` handles fails the waiting task
- **Builder:** the same, and a function that raises fails its task
- **Low-level:** `try: yield env.process(child())` and `except`
- **Checks:** [`test_c1_process_failure`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py), [`test_c1_process_failure_handled_by_on_fail`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py)
- **Difference:** By default (`on_unhandled_failure` is `continue`) a failure no task waits for ends the entity's flow in YAML and the builder, publishes `failed` and is counted, and the run goes on; under `on_unhandled_failure` of `stop` it stops the run, matching SimPy and the low-level API, where an exception nobody handles stops the run.
- **Taught on:** [Process failure](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**Event inspection**: A condition reads whether a signal has fired, whether it succeeded, and its value.

API details

- **SimPy:** `Event.triggered`, `.processed`, `.ok`, `.value`
- **YAML:** `signal.<name>.count`, `.ok` and `.value` in a condition
- **Builder:** `context.signal(name).wait()` and its SimPy attributes, or the signal's `count`, `ok` and `value`
- **Low-level:** the same attributes of SimPy events and `Signal`
- **Checks:** [`test_c1_event_inspection`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Event inspection](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**Process inspection**: After a wait, a task reads whether a child flow has finished, whether it succeeded, and its value.

API details

- **SimPy:** `Process.is_alive`, `.triggered`, `.ok`, `.value`
- **YAML:** `wait.<branch>.done`, `.ok` and `.value` after the wait
- **Builder:** the process `context.spawn` and `context.call` return
- **Low-level:** the process `env.process` returns
- **Checks:** [`test_c1_process_inspection`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Process inspection](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**Wait for a shared condition**: A task waits until a condition on the model's state holds, such as stock reaching 5.

API details

- **SimPy:** a loop over a fresh "changed" event
- **YAML:** `wait: {condition: level.stock >= 5}`
- **Builder:** `wait={"condition": "level.stock >= 5"}`
- **Low-level:** `env.watch_changes(check)`, or a SimPy loop
- **Checks:** [`test_c1_wait_for_a_shared_condition`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [Wait for a shared condition](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**The running process**: `process.name`, `process.instance` and `process.target` read the running task's name, instance id and what it waits on now, as SimPy's active process, its name and its target.

API details

- **SimPy:** `core.Environment.active_process`, `events.Process.name`, `events.Process.target`
- **YAML:** `process.name`, `process.instance`, `process.target` in a condition
- **Builder:** `context.env.active_process`; `.name` and `.target` on a process
- **Low-level:** `env.active_process.name`, `.target`
- **Checks:** [`test_c1_active_process`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py), [`test_c1_process_name`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py), [`test_c1_process_target`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_process_interaction.py)
- **Taught on:** [The running process](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/#how-it-works)

**Start a flow without waiting for it**: A task spawns another task as a new entity, keyed `<task>-n`, with the spawning entity's key in `attr.parent`, and goes on without waiting.

API details

- **SimPy:** `core.Environment.process`
- **YAML:** `spawn: {task: courier}`
- **Builder:** `@app.task(spawn=...)`, or `context.spawn(generator)`
- **Low-level:** `env.process(generator)`
- **Checks:** [`test_c1_start_a_process_without_waiting_for_it`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_start_a_process_without_waiting_for_it_drain`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py)
- **Taught on:** [Start a flow without waiting for it](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#start-a-flow-without-waiting)

**One-shot event**: A one-shot signal fires once, as SimPy's `Event` does: it does not re-arm, a wait after the firing resumes at once, and a second firing is refused.

API details

- **SimPy:** `events.Event`
- **YAML:** `signals: {closing: {once: true}}`
- **Builder:** `add_signal(name, once=True)`
- **Low-level:** `Signal(env, name, once=True)`
- **Checks:** [`test_c1_one_shot_event`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_one_shot_event_fired_twice`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py)
- **Taught on:** [One-shot event](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#one-shot-signals)

**Actions when an event happens**: An `on:` list runs actions when a signal fires or a task ends: fire a signal (copying another outcome), spawn a flow, interrupt a task, or set a value, as SimPy's callbacks, `trigger` and defused failures.

API details

- **SimPy:** `events.Event.callbacks`, `events.Event.trigger`, `events.Event.defused`
- **YAML:** `on: [{fire: {signal, from}}, {spawn: {...}}]` on a task or a signal
- **Builder:** `on=[...]` on `@app.task` and `add_signal`
- **Low-level:** `event.callbacks.append(fn)`, `ev.trigger(other)`, `ev.defused = True`
- **Checks:** [`test_c1_callbacks`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_trigger_from_another_event`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_trigger_from_another_event_failure`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_defused_failure`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_drain_waits_for_an_on_spawn`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_drain_waits_for_a_signals_on_actions`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_drain_waits_through_a_balking_next`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_drain_waits_through_a_chain_of_firings`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_process_signal_fire_is_not_counted`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_on_spawn_upstream_after_each_end`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_losing_child_flow_fire_after_left_is_not_counted`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py)
- **Taught on:** [Actions when an event happens](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens)

**Failure nobody handles**: Under `on_unhandled_failure: stop` a task, child flow or one-shot signal failure nobody handles stops the run, as SimPy; under `continue`, the default, it is defused and the run goes on.

API details

- **SimPy:** `core.Environment.run`
- **YAML:** `run: {on_unhandled_failure: stop}`
- **Builder:** `run(on_unhandled_failure=...)` or `start(on_unhandled_failure=...)`
- **Low-level:** `DynamicRealtimeEnvironment(on_unhandled_failure=...)`
- **Checks:** [`test_c1_failure_nobody_handles`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_failure_nobody_handles_continue`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py), [`test_c1_failure_nobody_handles_losing_child_flow`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py)
- **Taught on:** [Failure nobody handles](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/failures/#failures)

**Timeout value**: A wait timeout carries a value the waiter receives, as SimPy's `env.timeout(delay, value)`.

API details

- **SimPy:** `events.Timeout(delay, value)`
- **YAML:** `wait: {timeout: {after: 40 s, value: ...}}`
- **Builder:** the same mapping in `wait=`
- **Low-level:** `env.timeout(delay, value)`
- **Checks:** [`test_c1_timeout_value`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_events.py)
- **Taught on:** [Timeout value](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**Wait for a process already running**: A `running` wait holds until the running instances of a task on the waiter's entity, and the entities it spawned, all end, as SimPy's `yield process`.

API details

- **SimPy:** `yield process`
- **YAML:** `wait: {running: {task: charge, where: ...}}`
- **Builder:** the same in `wait=`, or `yield` a process from `context.spawn` or `call`
- **Low-level:** `yield process`
- **Checks:** [`test_c1_wait_for_a_process_already_running`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_wait_for_a_process_that_has_ended`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_wait_for_a_process_that_fails`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_wait_for_a_process_two_instances`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_wait_for_a_process_that_hands_on`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_wait_for_a_process_that_it_spawned`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_wait_for_a_process_abandoned_in_a_child_flow`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py)
- **Taught on:** [Wait for a process already running](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows)

**Start a process after a delay**: A spawn with `after` starts the new entity that long after the task finishes, as SimPy's `start_delayed`.

API details

- **SimPy:** `util.start_delayed`
- **YAML:** `spawn: {task: courier, after: 3 s}`
- **Builder:** `spawn={"task", "after"}`, or `context.spawn(generator, delay=3)`
- **Low-level:** `simpy.util.start_delayed(env, generator, 3)`
- **Checks:** [`test_c1_start_a_process_after_a_delay`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_start_a_process_after_a_delay_at_a_tie`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_start_a_process_after_a_delay_refuses_zero`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py)
- **Taught on:** [Start a process after a delay](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#start-a-flow-without-waiting)

**Be told when an event happens**: A `subscribe` interrupts the task when an event happens, carrying the event and its value, as SimPy's `subscribe_at`.

API details

- **SimPy:** `util.subscribe_at`
- **YAML:** `subscribe: {task: charger}`
- **Builder:** `@app.task(subscribe=...)`, or `context.subscribe(event)`
- **Low-level:** `simpy.util.subscribe_at(event)`
- **Checks:** [`test_c1_be_told_when_an_event_happens`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py), [`test_c1_be_told_when_an_event_happens_after_the_subscriber_ended`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c1_processes.py)
- **Taught on:** [Be told when an event happens](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#be-told-when-a-flow-ends)

## C2: Wait for One or All

Waiting for the first of several events, or for all of them, as SimPy's `AnyOf` and `AllOf` conditions do, with conditions inside conditions, a rule of your own such as at least some of them (SimPy's condition with a count rule), a condition over no events, and a request, put or get as one of the events.

**Wait for one event**: A task waits for whichever of several events happens first, such as a delivery or a time limit.

API details

- **SimPy:** `simpy.events.AnyOf`, `a | b`
- **YAML:** `wait: {any: {stock: {signal: restocked}, limit: {timeout: 3}}}`
- **Builder:** `wait={"any": {...}}`
- **Low-level:** `yield a | b`
- **Checks:** [`test_c2_wait_for_one_event`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_wait_for_one_or_all.py), [`test_c2_wait_for_one_event_two_at_one_instant`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_wait_for_one_or_all.py)
- **Difference:** Losing signal, timeout and condition branches are dropped when the wait ends; losing child flows keep running, as SimPy leaves processes running.
- **Taught on:** [Wait for one event](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-the-first)

**Wait for all events**: A task waits until every one of several events has happened.

API details

- **SimPy:** `simpy.events.AllOf`, `a & b`
- **YAML:** `wait: {all: {...}}`
- **Builder:** `wait={"all": {...}}`
- **Low-level:** `yield a & b`
- **Checks:** [`test_c2_wait_for_all_events`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_wait_for_one_or_all.py), [`test_c2_wait_for_all_events_with_a_failing_branch`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_wait_for_one_or_all.py)
- **Taught on:** [Wait for all events](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-all)

**Nested and mixed conditions**: A wait nests `any`, `all` and `at_least` to any depth, as SimPy's conditions combine with `&` and `|`.

API details

- **SimPy:** `events.Condition`, `&`, `|`
- **YAML:** `wait: {all: {look: {task: inspect}, either: {any: {...}}}}`
- **Builder:** `wait={"all": {...}}` with nested groups
- **Low-level:** `yield (a | b) & c`
- **Checks:** [`test_c2_nested_and_mixed_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_nested_condition_with_a_failing_member`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_nested_condition_holds_leaves_of_an_untriggered_group`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py)
- **Taught on:** [Nested and mixed conditions](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#nest-any-and-all)

**A condition with a custom rule**: `at_least: n` of several branches is done when `n` of them are, as SimPy's `Condition` with a count rule.

API details

- **SimPy:** `events.Condition(env, evaluate, events)`
- **YAML:** `wait: {at_least: 2, of: {...}}`
- **Builder:** `wait={"at_least": 2, "of": {...}}`
- **Low-level:** `Condition(env, lambda evs, c: c >= 2, [...])`
- **Checks:** [`test_c2_condition_with_a_custom_rule`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_custom_rule_equal_to_the_branch_count_is_all`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_custom_rule_two_at_one_instant`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_custom_rule_with_a_failing_member`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py)
- **Taught on:** [A condition with a custom rule](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#at-least-some-of-them)

**A condition over no events**: An empty `any` or `all` is accepted and succeeds at once with an empty value, as SimPy does.

API details

- **SimPy:** `events.AnyOf(env, [])`, `events.AllOf(env, [])`
- **YAML:** `wait: {any: {}}` or `wait: {all: {}}`
- **Builder:** `wait={"any": {}}`
- **Low-level:** `env.any_of([])`
- **Checks:** [`test_c2_condition_over_no_events`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_wait_for_one_or_all.py)
- **Taught on:** [A condition over no events](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#an-empty-wait)

**Request, put or get as a wait branch**: A `request`, `get` or `put` can be a wait branch; a won request holds its unit through the service, and a losing branch is cancelled.

API details

- **SimPy:** `resources.base.Put`, `resources.base.Get`, `events.AnyOf`
- **YAML:** `wait: {request: packers}`, `wait: {get: {store: parcels}}`, `wait: {put: {store: ...}}`
- **Builder:** `wait={"request": "packers"}` or `wait={"get": {...}}`
- **Low-level:** `yield req | env.timeout(3)`
- **Checks:** [`test_c2_request_as_a_wait_branch`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_get_as_a_wait_branch`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_put_as_a_wait_branch`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_a_wait_branch_on_a_missing_store_is_refused`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_wait_branch_in_a_failing_all`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_request_wait_branch_cleaned_on_renege`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_request_wait_branch_reused_across_a_resume_in_the_wait`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_request_wait_branch_kept_across_a_resume_in_the_service`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_abandon_during_a_wait_with_a_won_branch_is_clean`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_request_wait_branch_reacquired_after_a_preemption`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_a_get_granted_as_the_wait_ends_counts_as_won`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py), [`test_c2_a_request_granted_as_the_wait_ends_counts_as_won`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c2_conditions.py)
- **Difference:** A request, get or put branch that is granted at the instant its wait ends, after the condition was processed, counts as won, and the task keeps its unit or item, as SimPy's `if not event.triggered: event.cancel()` leaves a granted event. A SimPy waiter that ignores such an event, because it is not in the condition's value, keeps the unit held or loses the item.
- **Taught on:** [Request, put or get as a wait branch](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-a-unit-an-item-or-room)

## C3: Interrupts and Preemption

Interrupting a task while it waits or while it is in service, and a higher-priority request taking a resource unit from a lower-priority user, with the cause that tells the user who took it and since when it held it.

**Interrupt while waiting**: Another task or a scenario step interrupts a task while it is still in a queue.

API details

- **SimPy:** `simpy.Process.interrupt` while the process waits for a request
- **YAML:** `interrupt: {task: waiter}` on a scenario step or a task; `on_interrupt` on the task
- **Builder:** `context.interrupt("waiter")`; `@app.task(on_interrupt=...)`
- **Low-level:** `process.interrupt(cause)`, caught as `simpy.Interrupt`
- **Checks:** [`test_c3_interrupt_while_waiting`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py)
- **Taught on:** [Interrupt while waiting](https://jaehyeon.me/desflow/latest/learn/shop-logic/interruptions/#interrupt-a-task)

**Interrupt during service**: A task is interrupted while it holds a resource, and resumes, restarts or abandons its work.

API details

- **SimPy:** `simpy.Process.interrupt` while the process holds a unit
- **YAML:** `on_interrupt: resume`, `restart` or `abandon`
- **Builder:** `@app.task(on_interrupt=...)`
- **Low-level:** `except simpy.Interrupt`, then request again
- **Checks:** [`test_c3_interrupt_during_service`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py), [`test_c3_two_interrupts_at_one_instant`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py)
- **Taught on:** [Interrupt during service](https://jaehyeon.me/desflow/latest/learn/shop-logic/interruptions/#resume-restart-or-abandon)

**Priority preemption**: A more urgent request takes a resource unit away from a less urgent user, who is interrupted.

API details

- **SimPy:** `simpy.resources.resource.PreemptiveResource`
- **YAML:** `resources.<name>.preempt: true` with `queue: priority`; `preempt: false` on a task opts out
- **Builder:** `add_resource(..., queue="priority", preempt=True)`
- **Low-level:** `DynamicResource(..., queue="priority", preempt=True)` and `request(priority, preempt=True)`
- **Checks:** [`test_c3_priority_preemption`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py), [`test_c3_priority_preemption_equal_priorities`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py), [`test_c3_two_preemptions_at_one_instant`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py), [`test_c3_preemption_tie_between_users`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py), [`test_c3_priority_preemption_low_level`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py)
- **Taught on:** [Priority preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#preemption)

**Preemption cause**: A task that loses its unit to a more urgent request learns who took it and since when it had held the unit.

API details

- **SimPy:** `resources.resource.Preempted`
- **YAML:** `preempted_by` and `usage_since` in the `preempted` record
- **Builder:** the same record
- **Low-level:** `simpy.Interrupt.cause` is a `Preempted` with `by`, `usage_since` and `resource`
- **Checks:** [`test_c3_preemption_cause`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py), [`test_c3_preemption_record_shape`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py), [`test_c3_preemption_by_a_process_that_is_not_a_task`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c3_interrupts.py)
- **Taught on:** [Preemption cause](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#preemption)

## C4: Resources

Requesting and releasing resource units, in order of arrival or of priority, with a default capacity of one unit or no limit, reading the count, users and queue, limiting the waiting line, cancelling a request before or after its grant, holding several resources at once, and resources of your own classes.

**Ordinary request and release**: A task takes one unit of a resource, such as a packer, waits in line if none is free, and gives it back when done.

API details

- **SimPy:** `simpy.resources.resource.Resource`
- **YAML:** `resource: packers` on a task
- **Builder:** `@app.task(resource_id="packers")`
- **Low-level:** `with packers.request() as req: yield req`
- **Checks:** [`test_c4_ordinary_request_and_release`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Taught on:** [Ordinary request and release](https://jaehyeon.me/desflow/latest/user-guide/resources/#declare-a-resource)

**Priority request**: Waiting requests are served by priority, the lowest number first, instead of in order of arrival.

API details

- **SimPy:** `simpy.resources.resource.PriorityResource`
- **YAML:** `queue: priority` on the resource and `priority` on a task
- **Builder:** `add_resource(..., queue="priority")` and `@app.task(priority=...)`
- **Low-level:** `DynamicResource(..., queue="priority")` and `request(priority=...)`
- **Checks:** [`test_c4_priority_resource_request`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py), [`test_c4_priority_request_with_mixed_preempt_flags`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Difference:** Equal priorities are served first in, first out, as in SimPy; desflow also offers `fifo` and `lifo` queues, under which a request's priority is ignored.
- **Taught on:** [Priority request](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#task-priority)

**Cancel before a grant**: A task leaves the queue before it is served, so it never takes a unit.

API details

- **SimPy:** `Request.cancel()` before it is granted
- **YAML:** `patience`, or an interrupt with `on_interrupt: abandon`
- **Builder:** the same
- **Low-level:** `request.cancel()`, or leaving the `with` block
- **Checks:** [`test_c4_cancel_before_a_grant`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py), [`test_c4_renege_ahead_of_a_preempting_request`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Taught on:** [Cancel before a grant](https://jaehyeon.me/desflow/latest/user-guide/resources/#cancel-and-release)

**Cancel after a grant**: A task that already holds a unit stops early and gives the unit back.

API details

- **SimPy:** `Request.cancel()` after the grant releases nothing; `Resource.release(request)` does
- **YAML:** an interrupt with `on_interrupt: abandon` gives the unit back
- **Builder:** the same
- **Low-level:** `request.cancel()` releases nothing; `resource.release(request)`
- **Checks:** [`test_c4_cancel_after_a_grant`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Taught on:** [Cancel after a grant](https://jaehyeon.me/desflow/latest/user-guide/resources/#cancel-and-release)

**Hold several resources**: A task holds more than one resource at once, taken in a fixed order, and can hand them to the next task.

API details

- **SimPy:** nested requests, each held while the next is awaited
- **YAML:** `resource: [picker, bench]`, acquired in that order; `keep` hands units to the next task
- **Builder:** `@app.task(resource_id=["picker", "bench"], keep=[...])`
- **Low-level:** nested `with` blocks
- **Checks:** [`test_c4_hold_several_resources`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py), [`test_c4_keep_hands_a_unit_to_the_next_task`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Difference:** A model whose tasks acquire two resources in opposite orders is refused when it is built, because those tasks could each wait for ever; SimPy runs it. Two requests under one `all` wait are made together, so another task that acquires the two one after the other, in either order, is refused too.
- **Taught on:** [Hold several resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/#several-resources)

**Resource capacity defaults to 1**: A resource declared with no capacity has one unit, and a capacity of infinity makes a resource that never queues.

API details

- **SimPy:** `resources.resource.Resource(capacity=1)`
- **YAML:** `resources: {packers: {}}`; `current_cap: .inf` for no limit
- **Builder:** `add_resource("packers")`; `current_cap=float("inf")` for no limit
- **Low-level:** `DynamicResource(env, sim_id, name)` with no registry entry; `CapacityConfig(current_cap=float("inf"))` for no limit
- **Checks:** [`test_c4_default_capacity`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py), [`test_c4_resource_with_no_capacity_limit`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Taught on:** [Resource capacity defaults to 1](https://jaehyeon.me/desflow/latest/user-guide/resources/#declare-a-resource)

**Resource count, queue and users**: A condition or a script reads how many units are held, which requests hold them and which wait.

API details

- **SimPy:** `resources.resource.Resource.count`, `.queue` and `.users`
- **YAML:** `users.<resource>` and `queue.<resource>` in a condition, such as `balk: queue.machine >= 2`
- **Builder:** `get_resource(name).count`, `.users` and `.queue`
- **Low-level:** `resource.count`, `.users` and `.queue`, the list of waiting requests in service order
- **Checks:** [`test_c4_count_queue_and_users`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py), [`test_c4_users_and_queue_in_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py), [`test_c4_a_condition_is_not_checked_between_the_steps_of_one_call`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Difference:** A `lifo` queue serves the newest request first, which SimPy has no counterpart of.
- **Taught on:** [Resource count, queue and users](https://jaehyeon.me/desflow/latest/user-guide/resources/#read-the-queue-and-users)

**Release event and its request**: Giving a unit back returns an event that holds the request it released and is triggered at once.

API details

- **SimPy:** `resources.resource.Release.request`
- **YAML:** implicit: a task releases its units when it ends
- **Builder:** `get_resource(name).release(request)`
- **Low-level:** `resource.release(request)` returns a `DynamicRelease` whose `request` is the request
- **Checks:** [`test_c4_release_event`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Taught on:** [Release event and its request](https://jaehyeon.me/desflow/latest/user-guide/resources/#cancel-and-release)

**Usage since**: A granted request records the time its unit was granted, and a preemption report carries it.

API details

- **SimPy:** `resources.resource.Request.usage_since`
- **YAML:** `usage_since` in the `preempted` record
- **Builder:** `get_resource(name).users[i].usage_since`, and the `preempted` record
- **Low-level:** `request.usage_since`
- **Checks:** [`test_c4_usage_since`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Taught on:** [Usage since](https://jaehyeon.me/desflow/latest/user-guide/resources/#read-the-queue-and-users)

**Default request priority 0**: A request that names no priority has priority 0, so it ties with priority 0 and goes ahead of priority 1.

API details

- **SimPy:** `resources.resource.PriorityRequest(priority=0)`
- **YAML:** a task with no `priority` on a `queue: priority` resource
- **Builder:** `@app.task(resource_id=...)` with no `priority`
- **Low-level:** `request()` with no priority
- **Checks:** [`test_c4_default_priority`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Taught on:** [Default request priority 0](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#task-priority)

**A waiting line with a limit**: A resource refuses a request when a set number of requests wait already, and a task that is refused balks.

API details

- **SimPy:** `resources.resource.SortedQueue(maxlen)`
- **YAML:** `queue_limit: 5` on the resource
- **Builder:** `add_resource(..., queue_limit=5)`
- **Low-level:** `DynamicResource(..., queue_limit=5)`: `request()` raises `RuntimeError`
- **Checks:** [`test_c7_queue_limit`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_queue_limit_on_the_second_of_two_resources`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_queue_limit_fails_a_request_wait_branch`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_a_type_with_its_own_queue_limit_balks_before_queued`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_a_preempted_request_wait_balks_when_the_line_is_full`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_a_interrupt_while_a_preempted_request_waits_leaves_no_stale_request`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_a_preempted_request_wait_reneges_when_the_patience_runs_out`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_a_preempted_request_wait_targets_the_resource_while_it_asks_again`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py)
- **Difference:** A task whose first resource refuses it publishes `balked` with the resource before it publishes `queued`, which SimPy has no counterpart of.
- **Taught on:** [A waiting line with a limit](https://jaehyeon.me/desflow/latest/learn/shop-logic/patience/#refuse-to-join)

**Subclass a resource to change its rules**: A resource of your own class changes when a request is granted, how the waiting line is kept and what a request or release does.

API details

- **SimPy:** `resources.resource.PreemptiveResource`
- **YAML:** `type: !python module.Class` on the resource
- **Builder:** `add_resource(..., type=Class)`
- **Low-level:** a subclass of `ResourceBase`, built as `Class(env, sim_id, name)`
- **Checks:** [`test_c4_custom_resource_type`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c4_resources.py)
- **Taught on:** [Subclass a resource to change its rules](https://jaehyeon.me/desflow/latest/user-guide/resources/custom-resource-types/#custom-resource-types)

## C5: Stores and Containers

Putting and getting items in a store and amounts in a container, with capacity limits or none, priority items of any orderable type, filtered gets, cancelled puts and gets, the order of puts and gets at the same time, reading what a store holds and its put and get queues, a put or get that the task does not wait for, and stores and containers of your own classes.

**Bounded store put**: A task adds an item to a store, and waits when the store is full.

API details

- **SimPy:** `simpy.resources.store.Store.put`
- **YAML:** `put: {store: parcels}` on a task
- **Builder:** `@app.task(put={"store": "parcels"})`
- **Low-level:** `yield parcels.put(item)`
- **Checks:** [`test_c5_bounded_store_put`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_bounded.py)
- **Taught on:** [Bounded store put](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#take-and-add)

**Bounded store get**: A task takes an item from a store, and waits when the store is empty.

API details

- **SimPy:** `simpy.resources.store.Store.get`
- **YAML:** `get: {store: parcels}` on a task
- **Builder:** `@app.task(get={"store": "parcels"})`
- **Low-level:** `item = yield parcels.get()`
- **Checks:** [`test_c5_bounded_store_get`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_bounded.py)
- **Taught on:** [Bounded store get](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#take-and-add)

**Equal-time puts and gets**: When items are added and taken at the same moment, waiting tasks are served in the same order as in SimPy.

API details

- **SimPy:** `simpy.resources.store.Store`, one waiting get granted per trigger
- **YAML:** `get: {store: parcels}` and `put: {store: parcels}` on tasks
- **Builder:** `@app.task(get={"store": "parcels"})`
- **Low-level:** `yield parcels.put(item)`, `item = yield parcels.get()`
- **Checks:** [`test_c5_equal_time_puts_and_gets_follow_simpy_store`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_store_order.py), [`test_c5_equal_time_puts_and_gets_as_tasks`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_store_order.py)
- **Difference:** While a get with a filter waits, the store grants as SimPy's `FilterStore` does, looking at every waiting get each time. Once no filtered get waits, it grants one get per trigger, as `Store` does, where `FilterStore` goes on to the next. With filtered and unfiltered gets in one queue, gets at the same instant can therefore resume in a different order than in `FilterStore`; each get receives the same item.
- **Taught on:** [Equal-time puts and gets](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#take-and-add)

**Bounded quantity put**: A task adds an amount to a container, such as boxes to stock, and waits when there is no room.

API details

- **SimPy:** `simpy.resources.container.Container.put`
- **YAML:** `put: {container: stock, amount: 2}` on a task
- **Builder:** `@app.task(put={"container": "stock", "amount": 2})`
- **Low-level:** `yield stock.put(2)`
- **Checks:** [`test_c5_bounded_quantity_put`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_bounded.py)
- **Taught on:** [Bounded quantity put](https://jaehyeon.me/desflow/latest/learn/shop-logic/stock/#take-and-restock)

**Bounded quantity get**: A task takes an amount from a container, and waits until there is enough.

API details

- **SimPy:** `simpy.resources.container.Container.get`
- **YAML:** `get: {container: stock, amount: 1}` on a task
- **Builder:** `@app.task(get={"container": "stock", "amount": 1})`
- **Low-level:** `yield stock.get(1)`
- **Checks:** [`test_c5_bounded_quantity_get`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_bounded.py)
- **Taught on:** [Bounded quantity get](https://jaehyeon.me/desflow/latest/learn/shop-logic/stock/#take-and-restock)

**Priority item storage**: Items in a store come out by priority, the lowest number first, instead of in the order they went in.

API details

- **SimPy:** `simpy.resources.store.PriorityStore`
- **YAML:** `stores.<name>.priority: true` and a `put` with `priority`
- **Builder:** `add_store(..., priority=True)` and `put={"store": ..., "priority": ...}`
- **Low-level:** `DynamicStore(..., priority=True)` with `simpy.PriorityItem`
- **Checks:** [`test_c5_priority_item_storage`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_priority_store.py)
- **Difference:** Equal priorities come out in put order in the builder and YAML, which add a rising put order to the priority; a plain SimPy `PriorityStore` gives no order among equal priorities.
- **Taught on:** [Priority item storage](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#start-with-items-and-serve-by-priority)

**Filtered retrieval**: A task takes only an item that matches a condition, such as a parcel for its own area.

API details

- **SimPy:** `simpy.resources.store.FilterStore.get(filter)`
- **YAML:** `get: {store: parcels, filter: item.area == attr.area}`
- **Builder:** `get={"store": "parcels", "filter": ...}`, a condition or a function of the item
- **Low-level:** `parcels.get(filter=lambda item: ...)`
- **Checks:** [`test_c5_filtered_retrieval`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_inventory.py)
- **Difference:** A priority store takes no filter, because SimPy has no store that both ranks and filters.
- **Taught on:** [Filtered retrieval](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#pick-an-item-or-give-up-waiting)

**Cancel store get**: A task stops waiting for an item, for example when its patience runs out.

API details

- **SimPy:** `simpy.resources.base.Get.cancel()`
- **YAML:** `patience` on a task with a store `get`
- **Builder:** `@app.task(get={"store": ...}, patience=...)`
- **Low-level:** `get.cancel()`
- **Checks:** [`test_c5_cancel_store_get`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_inventory.py)
- **Taught on:** [Cancel store get](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#pick-an-item-or-give-up-waiting)

**Cancel quantity get**: A task stops waiting for an amount from a container.

API details

- **SimPy:** `simpy.resources.base.Get.cancel()`
- **YAML:** `patience` on a task with a container `get`
- **Builder:** `@app.task(get={"container": ...}, patience=...)`
- **Low-level:** `get.cancel()`
- **Checks:** [`test_c5_cancel_quantity_get`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_inventory.py)
- **Taught on:** [Cancel quantity get](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#pick-an-item-or-give-up-waiting)

**Cancel store put**: A task stops waiting for room in a full store.

API details

- **SimPy:** `simpy.resources.base.Put.cancel()`
- **YAML:** an interrupt with `on_interrupt: abandon` while the put waits
- **Builder:** the same
- **Low-level:** `put.cancel()`
- **Checks:** [`test_c5_cancel_store_put`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_inventory.py)
- **Taught on:** [Cancel store put](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#pick-an-item-or-give-up-waiting)

**Cancel quantity put**: A task stops waiting for room in a full container.

API details

- **SimPy:** `simpy.resources.base.Put.cancel()`
- **YAML:** an interrupt with `on_interrupt: abandon` while the put waits
- **Builder:** the same
- **Low-level:** `put.cancel()`
- **Checks:** [`test_c5_cancel_quantity_put`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_inventory.py)
- **Taught on:** [Cancel quantity put](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#pick-an-item-or-give-up-waiting)

**Put and get queues of a store or container**: A condition reads how many puts or gets wait at a store or container, and is checked again when a waiter joins or leaves.

API details

- **SimPy:** `resources.base.BaseResource.put_queue`, `.get_queue`
- **YAML:** `wait: {condition: putters.dock > 2}`, `getters.dock == 0`
- **Builder:** `wait={"condition": "putters.dock > 2"}`
- **Low-level:** `dock.put_queue`, `dock.get_queue`
- **Checks:** [`test_c5_store_put_and_get_queues`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_queues.py), [`test_c5_container_put_and_get_queues`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_queues.py), [`test_c5_queue_count_falls_when_a_waiter_leaves`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_queues.py)
- **Taught on:** [Put and get queues of a store or container](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#registry-paths-readings-and-conditions)

**Custom store and container types**: A store or container of your own class, which changes how an item or amount is taken in or out.

API details

- **SimPy:** `resources.base.BaseResource._do_put`, `._do_get`
- **YAML:** `stores.<name>.type: !python pkg.module.MyStore`
- **Builder:** `add_store(..., type=MyStore)`
- **Low-level:** `class MyStore(StoreBase)`
- **Checks:** [`test_c5_custom_store_type`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_custom_types.py), [`test_c5_custom_store_type_statistics`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_custom_types.py), [`test_c5_custom_container_type`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_custom_types.py)
- **Taught on:** [Custom store and container types](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#custom-store-and-container-types)

**Custom put and get queue types**: A store or container whose waiting puts and gets are kept in a queue class of your own.

API details

- **SimPy:** `resources.base.BaseResource.PutQueue`, `.GetQueue`
- **YAML:** `stores.<name>.type: !python pkg.module.MyStore`, with `PutQueue` and `GetQueue` set on the class
- **Builder:** `add_store(..., type=MyStore)`
- **Low-level:** `class MyStore(StoreBase): PutQueue = MyQueue`
- **Checks:** [`test_c5_custom_queue_types`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_custom_types.py), [`test_c5_custom_container_broken_queue`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_custom_types.py)
- **Taught on:** [Custom put and get queue types](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#custom-store-and-container-types)

**Custom put and get event classes**: A store or container that creates its own put and get events, such as a get that carries extra data.

API details

- **SimPy:** `resources.base.BaseResource.put`, `.get`
- **YAML:** `stores.<name>.type: !python pkg.module.MyStore`, with `put` and `get` set on the class
- **Builder:** `add_store(..., type=MyStore)`
- **Low-level:** `class MyStore(StoreBase): put = BoundClass(MyPut)`
- **Checks:** [`test_c5_custom_event_classes`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_custom_types.py)
- **Taught on:** [Custom put and get event classes](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#custom-store-and-container-types)

**A put or get the process does not wait for**: A task makes a put or get and carries on at once, without waiting for it to be granted.

API details

- **SimPy:** `resources.base.Put` without `yield`
- **YAML:** `put: {store: dock, wait: false}` on a task
- **Builder:** `put={"store": "dock", "wait": False}`
- **Low-level:** `dock.put(item)` without `yield`
- **Checks:** [`test_c5_put_not_waited_for`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_no_wait.py), [`test_c5_get_not_waited_for`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_no_wait.py), [`test_c5_no_wait_puts_are_served_in_order`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_no_wait.py), [`test_c5_no_wait_put_ends_with_the_run`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_no_wait.py)
- **Taught on:** [A put or get the process does not wait for](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#take-and-add)

**Container with no capacity limit**: A container that has no upper level, so a put never waits.

API details

- **SimPy:** `resources.container.Container(capacity=inf)`
- **YAML:** `containers.<name>` with no `current_cap`, or `current_cap: .inf`
- **Builder:** `add_container("tank")`
- **Low-level:** `DynamicContainer(env, "Dock", "tank")`
- **Checks:** [`test_c5_container_with_no_capacity_limit`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_unbounded.py)
- **Taught on:** [Container with no capacity limit](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#declare-them)

**Read a store's items**: A condition counts the items in a store or asks whether an item is in it.

API details

- **SimPy:** `resources.store.Store.items`
- **YAML:** `count(parcels) > 3`, `payload.sku in items(shelf, item.sku)`
- **Builder:** `wait={"condition": "count(parcels) > 3"}`
- **Low-level:** `len(parcels.items)`, `sku in [i["sku"] for i in shelf.items]`
- **Checks:** [`test_c5_count_store_items`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_store_items.py), [`test_c5_item_in_store`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_store_items.py), [`test_c5_item_in_priority_store`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_store_items.py), [`test_c5_item_in_empty_store`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_store_items.py)
- **Taught on:** [Read a store's items](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#registry-paths-readings-and-conditions)

**Store with no capacity limit**: A store that has no upper size, so a put never waits.

API details

- **SimPy:** `resources.store.Store(capacity=inf)`
- **YAML:** `stores.<name>` with no `current_cap`, or `current_cap: .inf`
- **Builder:** `add_store("parcels")`
- **Low-level:** `DynamicStore(env, "Shop", "parcels")`
- **Checks:** [`test_c5_store_with_no_capacity_limit`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_unbounded.py)
- **Taught on:** [Store with no capacity limit](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#declare-them)

**Priorities of any orderable type**: A priority store ranks items by a number, a string, a tuple or any value that can be ordered.

API details

- **SimPy:** `resources.store.PriorityItem`
- **YAML:** `put: {store: queue, priority: [1, urgent]}`
- **Builder:** `put={"store": "queue", "priority": (1, "urgent")}`
- **Low-level:** `simpy.PriorityItem((1, "urgent"), item)`
- **Checks:** [`test_c5_priority_item_storage_any_orderable_type`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_priority_store.py), [`test_c5_priority_yaml_list_literal`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_priority_store.py), [`test_c5_priority_mixed_types_raise_type_error`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c5_priority_store.py)
- **Taught on:** [Priorities of any orderable type](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#start-with-items-and-serve-by-priority)

## C6: Run Control

Starting a run from a given time and state, reading the current time, ending it at a time, at an event or on a condition and reading the value that ended it, draining it, running it in segments, stepping through it one model event at a time, scheduling an event with a priority, an environment class of your own, and the pace of the run: its factor, strict pacing and syncing with the wall clock.

**Initial time**: The simulation clock starts at a given time instead of 0.

API details

- **SimPy:** `simpy.core.Environment(initial_time=...)`
- **YAML:** `simulation.initial_time`
- **Builder:** `SimulationContext(..., initial_time=100)`
- **Low-level:** `DynamicRealtimeEnvironment(initial_time=100)`
- **Checks:** [`test_c6_initial_time`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_run_control.py)
- **Taught on:** [Initial time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#initial-time)

**Initial resources**: Each resource starts with a given number of units.

API details

- **SimPy:** `simpy.resources.resource.Resource(capacity=...)`
- **YAML:** `resources.<name>.current_cap`
- **Builder:** `add_resource(name, current_cap=2, ...)`
- **Low-level:** `CapacityConfig(current_cap=2, ...)`
- **Checks:** [`test_c6_initial_resources`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_initial_state.py)
- **Taught on:** [Initial resources](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#initial-time)

**Initial inventory**: Containers start with a given level, and stores with given items.

API details

- **SimPy:** `Container(init=...)`; items in a `Store`
- **YAML:** `containers.<name>.level` and `stores.<name>.items`
- **Builder:** `add_container(..., level=6)` and `add_store(..., items=[...])`
- **Low-level:** `ContainerConfig(level=6)` and `DynamicStore(..., items=[...])`
- **Checks:** [`test_c6_initial_inventory`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_initial_state.py)
- **Taught on:** [Initial inventory](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#start-with-items-and-serve-by-priority)

**Initial entity state**: Each arriving entity, such as an order, starts with given attributes.

API details

- **SimPy:** the attributes a process starts with
- **YAML:** `arrivals.<name>.attributes`
- **Builder:** `add_arrival(..., attributes={...})`
- **Low-level:** the values a process is started with
- **Checks:** [`test_c6_initial_entity_state`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_initial_state.py)
- **Taught on:** [Initial entity state](https://jaehyeon.me/desflow/latest/user-guide/model-parts/task-flow/#attributes)

**Stop at a time**: The run ends at a given simulation time.

API details

- **SimPy:** `simpy.core.Environment.run(until=...)`
- **YAML:** `run.until`
- **Builder:** `app.run(until=...)`
- **Low-level:** `env.run(until=...)`
- **Checks:** [`test_c6_stop_at_a_time`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_run_control.py)
- **Taught on:** [Stop at a time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#run-length_1)

**Stop at an event**: The run ends when a given event happens, such as a closing signal, even before its end time.

API details

- **SimPy:** `simpy.core.Environment.run(until=event)`
- **YAML:** `run.stop: {signal: closing}` beside `run.until`
- **Builder:** `app.run(until=..., stop={"signal": "closing"})`, a signal's name, or a function returning an event
- **Low-level:** `env.run(until=event)`
- **Checks:** [`test_c6_stop_at_an_event`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_stop_at_an_event_at_the_same_instant_as_until`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py)
- **Difference:** When the stop and `until` fall at the same time, `until` ends the run first, as SimPy's own `until` would.
- **Taught on:** [Stop at an event](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#stop-early)

**Explicit stop condition**: The run ends as soon as a condition on the model's state holds.

API details

- **SimPy:** `env.run(until=event)` with a process that succeeds the event when the condition holds
- **YAML:** `run.stop: {condition: level.stock >= 5}`
- **Builder:** `app.run(until=..., stop={"condition": "..."})`
- **Low-level:** `env.watch_changes(check)` and `env.run(until=event)`
- **Checks:** [`test_c6_explicit_stop_condition`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py)
- **Taught on:** [Explicit stop condition](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#stop-early)

**Stop arrivals and drain**: New arrivals stop, and the run goes on until everything already in the model has finished.

API details

- **SimPy:** interrupt the source, then `env.run(until=env.all_of(entities))`
- **YAML:** `run.drain: true` or `{limit: 1 h}`; `desflow run --drain`
- **Builder:** `app.run(until=..., drain=True)`, or `context.stop_arrivals()`
- **Low-level:** `source.interrupt()`, then `env.run(until=env.all_of(processes))`
- **Checks:** [`test_c6_stop_arrivals_and_drain`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_stop_arrivals_and_drain_with_a_limit`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py)
- **Taught on:** [Stop arrivals and drain](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#drain)

**Step one event**: The run advances one event at a time, so a script can watch each step.

API details

- **SimPy:** `simpy.core.Environment.step()`
- **YAML:** `SimulationContext.from_yaml(path).start()`, then `.step()`
- **Builder:** `app.start()`, `app.step()`, `app.close()`
- **Low-level:** `env.step()`
- **Checks:** [`test_c6_step_one_event`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py)
- **Difference:** A blueprint has no stepping key; its context, from `from_yaml`, steps as a builder context does.
- **Taught on:** [Step one event](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#step-and-peek)

**Inspect next event time**: A script reads when the next event will happen without running it.

API details

- **SimPy:** `simpy.core.Environment.peek()`
- **YAML:** `SimulationContext.from_yaml(path).start()`, then `.peek()`
- **Builder:** `app.peek()`
- **Low-level:** `env.peek()`
- **Checks:** [`test_c6_inspect_next_event_time`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py)
- **Taught on:** [Inspect next event time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#step-and-peek)

**A step processes only the model's events**: `step()` and `peek()` count the events of the model, not the statistics, flush, lag, ingress and watcher processes that desflow adds, so a script steps a model as it would step it in SimPy. Inside `run()` the step processes every event, desflow's own included, because SimPy's `run()` calls `step()` for each event in its one queue.

API details

- **SimPy:** `simpy.core.Environment.step()` and `peek()`
- **YAML:** `SimulationContext.from_yaml(path).start()`, then `.step()` and `.peek()`; `processes:` entries are model events
- **Builder:** `app.start()`, `app.step()`, `app.peek()`; `add_process` functions are model events
- **Low-level:** `env.step()` and `env.peek()`
- **Checks:** [`test_c6_step_counts_model_events`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_step_counts_model_events_in_arrival_models`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_step_with_a_live_capacity_change`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_step_with_a_task_that_has_on_actions`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_run_after_steps_gives_the_records_of_one_run`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_finish_after_steps_that_reached_until`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_finish_after_steps_to_until_with_statistics_due_there`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py)
- **Difference:** With only desflow's own events left, `step()` raises `EmptySchedule` and `peek()` gives infinity, but the queue is not empty: `run()` and `finish()` still process those events.
- **Taught on:** [A step processes only the model's events](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#step-and-peek)

**Value of a run stopped at an event**: `run()` returns the value of the event that stopped it, and raises the exception of a stop that failed. The value is a signal's payload for a stop on a signal, True for a condition, and the event's own value for a function. It is None when `until` or a drain ends the run, or when `until` and the stop are due together. A stop on a signal that fails raises `SignalFailed`, the exception the failed event carries, which is what SimPy raises. The run closes before it returns, so the sinks have their records.

API details

- **SimPy:** `simpy.core.Environment.run(until=event)`
- **YAML:** `SimulationContext.from_yaml(path).run()` returns the value; `desflow run` exits with an error when the stop failed
- **Builder:** `app.run(until=..., stop=...)` and `app.finish()` return the value
- **Low-level:** `env.run(until=event)`
- **Checks:** [`test_c6_run_value`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py)
- **Taught on:** [Value of a run stopped at an event](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#stop-early)

**Current time**: `t` is the simulation clock, read in a condition or an expression, so a route or a balk can depend on the current time.

API details

- **SimPy:** `core.Environment.now`
- **YAML:** `t` in a condition, such as `t < 110`
- **Builder:** `context.env.now`
- **Low-level:** `env.now`
- **Checks:** [`test_c6_current_time`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_run_control.py)
- **Taught on:** [Current time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#read-the-current-time)

**Schedule an event with a priority**: `env.schedule(event, priority, delay)` puts an event on the queue, and a `fire` action with `after` fires a one-shot signal that many seconds from now, as SimPy's `schedule` does.

API details

- **SimPy:** `core.Environment.schedule`
- **YAML:** `fire: {signal: s, after: 5 s}` on a signal with `once: true`
- **Builder:** `{"fire": {"signal": "s", "after": 5}}` in `on=`, or `context.env.schedule(event, priority, delay)`
- **Low-level:** `env.schedule(event, priority, delay)`
- **Checks:** [`test_c6_schedule_with_a_priority`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_environment.py), [`test_c6_fire_with_after_is_not_counted_by_a_drain`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_environment.py), [`test_c6_fire_twice_with_after_is_refused_as_a_one_shot_signal`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_environment.py)
- **Taught on:** [Schedule an event with a priority](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens)

**Urgent and normal event priorities**: An urgent event is processed before the normal events at its time, as SimPy's `URGENT` and `NORMAL` order them; `urgent: true` on a `fire` action resumes the waiters first.

API details

- **SimPy:** `events.URGENT`, `events.NORMAL`
- **YAML:** `fire: {signal: s, urgent: true}` on a signal with `once: true`
- **Builder:** `{"fire": {"signal": "s", "urgent": True}}` in `on=`, or `simpy.events.URGENT` in `context.env.schedule`
- **Low-level:** `env.schedule(event, URGENT)`
- **Checks:** [`test_c6_fire_with_urgent_and_after`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_environment.py), [`test_c6_fire_urgent_and_after_need_a_one_shot_signal`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_environment.py)
- **Taught on:** [Urgent and normal event priorities](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens)

**Run in segments**: `advance(until)` runs a started run to a time or an event and returns with the run open, and `finish()` runs on to the end; the segments give the records of one run. `resume_pacing()` paces again from `now` after a pause.

API details

- **SimPy:** `core.Environment.run`, called again
- **YAML:** `app = SimulationContext.from_yaml(file)`, then `app.start()`, `app.advance(until)` and `app.finish()`
- **Builder:** `app.start()`, `app.advance(until)`, `app.finish()`
- **Low-level:** `env.run(until=)`, called again; `env.resume_pacing()`
- **Checks:** [`test_c6_stopped_and_resumed_run`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_run_in_segments`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_advance_refuses_what_a_segment_cannot_do`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_segments_with_stop_and_drain_give_the_records_of_one_run`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_step_after_a_segment_processes_its_event_as_simpy_does`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_advance_to_an_event_and_on`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_finish_raises_a_failed_stop_that_advance_reached`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_advance_to_events_adds_one_internal_event_to_the_queue`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_end_conditions.py), [`test_c6_resume_pacing`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_pacing.py)
- **Taught on:** [Run in segments](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#run-in-segments)

**Subclass the environment**: A subclass of `DynamicRealtimeEnvironment` runs the model, so an override of `step` or `schedule` sees its events, as a subclass of SimPy's `Environment` does.

API details

- **SimPy:** `core.Environment`
- **YAML:** `simulation: {environment: !python module.Env}`
- **Builder:** `SimulationContext(environment=Env)`
- **Low-level:** `class Env(DynamicRealtimeEnvironment)`
- **Checks:** [`test_c6_environment_subclass`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_environment.py), [`test_c6_environment_subclass_in_run`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_environment.py)
- **Taught on:** [Subclass the environment](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/blueprints/#run-on-your-own-environment)

**Real-time factor**: `factor` sets how many real seconds one simulation second takes, so a run paces to the wall clock, as SimPy's RealtimeEnvironment does.

API details

- **SimPy:** `rt.RealtimeEnvironment(factor)`
- **YAML:** `simulation.factor`
- **Builder:** `SimulationContext(factor=)`
- **Low-level:** `DynamicRealtimeEnvironment(factor=)`
- **Checks:** [`test_c6_real_time_factor`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_pacing.py)
- **Taught on:** [Real-time factor](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#factor)

**Strict pacing**: With `strict` on, a paced step more than `factor` seconds behind the wall clock raises `RuntimeError`, as SimPy's `RealtimeEnvironment(strict=True)` does.

API details

- **SimPy:** `rt.RealtimeEnvironment.strict`
- **YAML:** `simulation.strict`
- **Builder:** `SimulationContext(strict=True)`
- **Low-level:** `DynamicRealtimeEnvironment(strict=True)`
- **Checks:** [`test_c6_strict_raises_when_behind`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_pacing.py)
- **Taught on:** [Strict pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#strict-pacing)

**Strict by default**: The low-level environment is strict unless `strict=False` is passed, as SimPy's is; the builder and YAML default to off.

API details

- **SimPy:** `rt.RealtimeEnvironment(strict=True)`
- **YAML:** `simulation.strict`, default `false`
- **Builder:** `SimulationContext(strict=False)`, the default
- **Low-level:** `DynamicRealtimeEnvironment(strict=True)`, the default
- **Checks:** [`test_c6_strict_default`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_pacing.py)
- **Taught on:** [Strict by default](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#strict-pacing)

**Sync with the wall clock**: `sync()` resets only the wall-clock anchor (`real_start`), as SimPy's does, so strict stops counting the time before it. Going on after a pause with the pace kept needs `resume_pacing()`, which also moves the simulation anchor.

API details

- **SimPy:** `rt.RealtimeEnvironment.sync`
- **YAML:** not in YAML: `app.start()`, then `app.env.sync()`
- **Builder:** `app.env.sync()` after `start()`; the builder syncs once when it builds a strict run
- **Low-level:** `env.sync()`
- **Checks:** [`test_c6_sync_catches_up`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c6_pacing.py)
- **Taught on:** [Sync with the wall clock](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#strict-pacing)

## C7: Patience

Customers who give up after waiting too long (reneging) and customers who do not join a queue that is too long (balking).

**Reneging**: A customer leaves the queue after waiting longer than their patience.

API details

- **SimPy:** `simpy.events.AnyOf` of a request and a timeout
- **YAML:** `patience: 3 min` on a task
- **Builder:** `@app.task(..., patience=180)`
- **Low-level:** `yield req | env.timeout(patience)`, then `req.triggered`
- **Checks:** [`test_c7_reneging`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_reneging_tie_timeout_scheduled_after_the_grant`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_reneging_tie_timeout_scheduled_before_the_grant`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_reneged_request_leaves_the_queue`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_patience_pauses_during_service`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py)
- **Taught on:** [Reneging](https://jaehyeon.me/desflow/latest/learn/shop-logic/patience/#leave-after-a-wait)

**Balking**: A customer does not join the queue at all when a condition holds, such as the queue being too long.

API details

- **SimPy:** a check before `Resource.request`
- **YAML:** `balk: <condition>` on a task
- **Builder:** `@app.task(..., balk="...")`
- **Low-level:** an `if` before the request
- **Checks:** [`test_c7_balking`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py), [`test_c7_a_balk_condition_gives_back_the_unit_the_task_kept`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_c7_patience.py)
- **Taught on:** [Balking](https://jaehyeon.me/desflow/latest/learn/shop-logic/patience/#refuse-to-join)

## S1: Breakdowns

A resource that fails and is repaired, with the work it interrupts.

**Breakdown and repair**: A resource fails from time to time and is repaired, interrupting the work it was doing.

API details

- **SimPy:** a repair process holding the machine on a `PreemptiveResource`
- **YAML:** `resources.<name>.breakdown: {time_to_failure, repair, work}`
- **Builder:** `add_breakdown(resource, time_to_failure={...}, repair={...}, work=...)`
- **Low-level:** `run_breakdowns(env, resource, sampler, work=...)`
- **Checks:** [`test_s1_failure_during_service`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_s1_breakdowns.py), [`test_s1_failure_during_service_work_finishes`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_s1_breakdowns.py), [`test_s1_planned_change_during_repair`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_s1_breakdowns.py)
- **Difference:** The resource is down on a calendar clock, from the failure to the repair, even while it waits for the work in service to finish with `work: finish`.
- **Taught on:** [Breakdown and repair](https://jaehyeon.me/desflow/latest/how-to/modelling/breakdowns/#steps)

## S2: Schedules

A schedule of times at which an arrival rate or a capacity changes during a run.

**Finite schedules**: Arrival rates or capacities change at set times during the run.

API details

- **SimPy:** a process that changes values at set times, started before the others
- **YAML:** `scenario` steps with `at`, `path` and `value`
- **Builder:** `add_scenario(steps)` or `add_step(at, path=..., value=...)`
- **Low-level:** `run_scenario(env, steps, until=...)`
- **Checks:** [`test_s2_step_at_an_arrival_time`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_s2_schedules.py)
- **Difference:** A step applies before every other event at its time, so the arrival gap drawn then uses the new value.
- **Taught on:** [Finite schedules](https://jaehyeon.me/desflow/latest/how-to/modelling/time-varying-arrivals/#boundaries)

## S3: Replay

Arrivals read from a local CSV or Parquet file instead of drawn at random.

**Local replay**: Arrivals come from rows in a local CSV or Parquet file instead of a random distribution.

API details

- **SimPy:** a loop over recorded rows, each a timeout to its time
- **YAML:** `arrivals.<name>.replay: {path, time, unit, fields, on_invalid}`
- **Builder:** `add_arrival_replay(name, source, ...)`
- **Low-level:** `read_trace(source, ...)` and a SimPy loop over its rows
- **Checks:** [`test_s3_replay_equal_times_invalid_rows_and_the_horizon`](https://github.com/jaehyeon-kim/desflow/blob/v0.27.0/tests/conformance/test_s3_replay.py)
- **Taught on:** [Local replay](https://jaehyeon.me/desflow/latest/learn/inputs/local-replay/#replay-the-rows)

## Differences Kept by Design

- **A run with no until** (SimPy: `core.Environment.run()` returns when no event is left): A run with a sink or an ingress connector and no `until` runs for ever, because a live run has no end: the connectors keep events coming. Give it an `until`, with a `drain` when it should finish its work, or a `stop` that is sure to happen, or stop it with Ctrl+C.
- **Strict pacing at factor 0** (SimPy: `rt.RealtimeEnvironment(factor=0, strict=True)` raises once the wall clock has moved): An unpaced run has no wall clock to fall behind, so SimPy's check at factor 0 would raise on the first step of every unpaced script. `strict` is never checked at factor 0, also before `go_live_at`. **Taught on:** [Strict pacing at factor 0](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#strict-pacing)
- **Capacity 0** (SimPy: `resources.resource.Resource(capacity=0)` raises `ValueError`, and so do `Store` and `Container`): A resource, store or container may have a capacity of 0, at the start or later, because its capacity changes during a run: a shift with no packers, a shelf that is closed. At capacity 0 a resource grants no request, and a store or container accepts no put, until the capacity rises; a get still takes what a store or container holds.
