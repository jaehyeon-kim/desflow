# SimPy Compatibility

This page lists every modelling feature of SimPy 4.1.2, 98 in all, and how to write each one in desflow. The features are grouped by the part of SimPy they belong to.

Each feature starts with what it does in SimPy, in bold, and a link to the page that explains it. Below that are its form in SimPy, in YAML, in the Builder API and in the low-level API, how desflow differs in a detail where it does, and the tests that check it.

- **Native: 96.** A conformance test runs a plain SimPy 4.1.2 model and the same model in desflow, and compares the event times, outcomes and balances.
- **Gap: 1.** desflow gives other times, order or values than SimPy, or has no key for the feature. Each gap has an id and says how to get SimPy's behaviour.
- **Difference kept by design: 1.** desflow behaves differently on purpose, for the reason given.

[Where desflow Differs](#where-desflow-differs), at the end of the page, explains each gap and difference.

## Environment

**Starts the simulation clock at a given time instead of 0.** Explained in [Initial time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#initial-time).

- SimPy: `Environment(initial_time)`
- YAML: `simulation.initial_time`
- Builder: `SimulationContext(..., initial_time=100)`
- Low-level: `DynamicRealtimeEnvironment(initial_time=100)`

Checked by 1 test in [`c6_run_control`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_run_control.py).

**Gives the current simulation time.** Explained in [Current time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/#read-the-current-time).

- SimPy: `Environment.now`
- YAML: `t` in a condition, such as `t < 110`
- Builder: `context.env.now`
- Low-level: `env.now`

Checked by 1 test in [`c6_run_control`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_run_control.py).

**Gives the process whose code is running at this moment.** Explained in [The running process](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/#how-it-works).

- SimPy: `Environment.active_process`
- YAML: `process.name`, `process.instance`, `process.target` in a condition
- Builder: `context.env.active_process`; `.name` and `.target` on a process
- Low-level: `env.active_process.name`, `.target`

Checked by 7 tests in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (5), [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Starts a process that runs alongside the one that started it, which carries on without waiting.** Explained in [Start a flow without waiting for it](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#start-a-flow-without-waiting).

- SimPy: `env.process(...)` without `yield`
- YAML: `spawn: {task: courier}`
- Builder: `@app.task(spawn=...)`, or `context.spawn(generator)`
- Low-level: `env.process(generator)`

Checked by 23 tests in [`c1_processes`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_processes.py) (2), [`builder_additions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_builder_additions.py) (6) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (15).

**Puts an event on the schedule with a priority and a delay.** Explained in [Schedule an event with a priority](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens).

- SimPy: `Environment.schedule(event, priority, delay)`
- YAML: `fire: {signal: s, after: 5 s}` on a signal with `once: true`
- Builder: `{"fire": {"signal": "s", "after": 5}}` in `on=`, or `context.env.schedule(event, priority, delay)`
- Low-level: `env.schedule(event, priority, delay)`

Checked by 5 tests in [`c6_environment`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_environment.py) (3), [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (1) and [`c1_events`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_events.py) (1).

**Orders events due at the same time: an urgent event is processed before a normal one.** Explained in [Urgent and normal event priorities](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens).

- SimPy: `URGENT` and `NORMAL` event priorities
- YAML: `fire: {signal: s, urgent: true}` or `fire: {signal: s, priority: 2}` on a signal with `once: true`
- Builder: `{"fire": {"signal": "s", "urgent": True}}` in `on=` (`"priority": 2` for another priority), or `simpy.events.URGENT` in `context.env.schedule`
- Low-level: `env.schedule(event, URGENT)`

Checked by 3 tests in [`c6_environment`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_environment.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Processes events due at the same time and with the same priority in the order they were scheduled.** Explained in [Wait for one event](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-the-first).

- SimPy: Events at one time run in the order they were scheduled
- YAML: `wait: {any: {stock: {signal: restocked}, limit: {timeout: 3}}}`
- Builder: `wait={"any": {...}}`
- Low-level: `yield a | b`
- Where desflow differs: Losing signal, timeout and condition branches are dropped when the wait ends; losing child flows keep running, as SimPy leaves processes running. A request branch not yet granted when the wait ends is cancelled; a SimPy `with` block held over the rest of the task leaves it queued.

Checked by 8 tests in [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (6).

**Gives the time of the next scheduled event, or infinity when none is left.** Explained in [Inspect next event time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#step-and-peek).

- SimPy: `Environment.peek()`
- YAML: `run: {steps: true}` publishes `peek` after each step, or `SimulationContext.from_yaml(path).start()`, then `.peek()`
- Builder: `app.peek()`
- Low-level: `env.peek()`

Checked by 1 test in [`c6_end_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_end_conditions.py).

**Processes the next event, and raises `EmptySchedule` when none is left.** Explained in [Step one event](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#step-and-peek).

- SimPy: `Environment.step()`
- YAML: `run: {steps: true}`, or `SimulationContext.from_yaml(path).start()`, then `.step()`
- Builder: `app.start()`, `app.step()`, `app.close()`
- Low-level: `env.step()`
- Where desflow differs: `run.steps` publishes the step count and the clock after each step of a run; stepping a model by hand is for a script, which steps the context from `from_yaml` as it steps a builder context.

Checked by 4 tests in [`c6_end_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_end_conditions.py) (1), [`c6_run_control`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_run_control.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (2).

**Steps through the events of the model one at a time from a script.** Explained in [A step processes only the model's events](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#step-and-peek).

- SimPy: A step processes only the model's events
- YAML: `SimulationContext.from_yaml(path).start()`, then `.step()` and `.peek()`; `processes:` entries are model events
- Builder: `app.start()`, `app.step()`, `app.peek()`; `add_process` functions are model events
- Low-level: `env.step()` and `env.peek()`
- Where desflow differs: With only desflow's own events left, `step()` raises `EmptySchedule` and `peek()` gives infinity, but the queue is not empty: `run()` and `finish()` with an `until` still process those events, and a run with no `until` ends when only they are left.

Checked by 7 tests in [`c6_end_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_end_conditions.py).

**Runs until no event is left.** Explained in [A run with no until](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#run-length_1).

- SimPy: `run()` with no `until`
- YAML: `run:` with no `until`
- Builder: `app.run()` with no `until`
- Low-level: `env.run()` with no `until`

Checked by 5 tests in [`c6_end_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_end_conditions.py).

**Runs until the clock reaches a given time.** Explained in [Stop at a time](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#run-length_1).

- SimPy: `run(until=time)`
- YAML: `run.until`, or `run.segments` for several stops with a `look` at each
- Builder: `app.run(until=...)`
- Low-level: `env.run(until=...)`

Checked by 3 tests in [`c6_run_control`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_run_control.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (2).

**Runs until a given event is processed.** Explained in [Stop at an event](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#stop-early).

- SimPy: `run(until=event)`
- YAML: `run.stop: {signal: closing}` beside `run.until`
- Builder: `app.run(until=..., stop={"signal": "closing"})`, a signal's name, or a function returning an event
- Low-level: `env.run(until=event)`
- Where desflow differs: When the stop and `until` fall at the same time, `until` ends the run first, as SimPy's own `until` would.

Checked by 2 tests in [`c6_end_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_end_conditions.py).

**Returns the value of the event a run stops at, or raises its exception when it failed.** Explained in [Value of a run stopped at an event](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#stop-early).

- SimPy: The value or exception of `run(until=event)`
- YAML: `SimulationContext.from_yaml(path).run()` returns the value; `desflow run` exits with an error when the stop failed
- Builder: `app.run(until=..., stop=...)` and `app.finish()` return the value
- Low-level: `env.run(until=event)`

Checked by 1 test in [`c6_end_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_end_conditions.py).

**Continues a stopped run up to a later time.** Explained in [Run in segments](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#run-in-segments).

- SimPy: `run(until=t)` again with a later `t`
- YAML: `app = SimulationContext.from_yaml(file)`, then `app.start()`, `app.advance(until)` and `app.finish()`
- Builder: `app.start()`, `app.advance(until)`, `app.finish()`
- Low-level: `env.run(until=)`, called again; `env.resume_pacing()`

Checked by 9 tests in [`c6_end_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_end_conditions.py) (8) and [`c6_pacing`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_pacing.py) (1).

**Runs the callbacks of the event a run stopped at when the run continues.** Explained in [Run in segments](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#run-in-segments).

- SimPy: Callbacks of the stopping event run when the run resumes
- YAML: `app = SimulationContext.from_yaml(file)`, then `app.start()`, `app.advance(until)` and `app.finish()`
- Builder: `app.start()`, `app.advance(until)`, `app.finish()`
- Low-level: `env.run(until=)`, called again; `env.resume_pacing()`

Checked by 9 tests in [`c6_end_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_end_conditions.py) (8) and [`c6_pacing`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_pacing.py) (1).

**Changes how the environment schedules or steps events, in a class of your own.** Explained in [Subclass the environment](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/blueprints/#run-on-your-own-environment).

- SimPy: A subclass of `Environment`
- YAML: `simulation: {environment: !python module.Env}`
- Builder: `SimulationContext(environment=Env)`
- Low-level: `class Env(DynamicRealtimeEnvironment)`

Checked by 2 tests in [`c6_environment`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_environment.py).

## Events and Timeout

**Marks an event as successful with a value, and wakes every process that waits for it.** Explained in [Signal event success and value](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: `Event.succeed(value)` waking every waiter
- YAML: `signals: [restocked]`, `signal: restocked` on the firing task and `wait: {signal: restocked}`; `wait: {suspend: true}` and `resume: {task, where, value, limit}` wake instances that no signal names
- Builder: `add_signal`, `@app.task(signal=...)`, `wait={"signal": ...}`, or `context.signal(name).fire(value)`
- Low-level: `Signal(env, name)`: `yield signal.wait()` and `signal.fire(value)`

Checked by 2 tests in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Marks an event as failed with an exception, which is raised in every process that waits for it.** Explained in [Signal event failure](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: `Event.fail(exception)`
- YAML: a failing task with `signal`; its waiters fail with its reason
- Builder: the same, or `context.signal(name).fail(reason)`
- Low-level: `signal.fail(reason)`; waiters receive `SignalFailed`

Checked by 1 test in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py).

**Lets an event succeed or fail only once.** Explained in [One-shot event](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#one-shot-signals).

- SimPy: An event that triggers once
- YAML: `signals: {closing: {}}`, or `once: false` to re-arm
- Builder: `add_signal(name)`, or `once=False` to re-arm
- Low-level: `Signal(env, name)`, or `once=False` to re-arm

Checked by 2 tests in [`c1_events`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_events.py).

**Gives an event the outcome and value of another event, so one event can set off the next.** Explained in [Actions when an event happens](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens).

- SimPy: `Event.trigger(event)`
- YAML: `on: [{fire: {signal, from}}, {spawn: {...}}]` on a task or a signal, `fire` with `fail`, `defused` and `priority`, `when` on an entry, and `listen` on a task for a callback added during the run
- Builder: `on=[...]` on `@app.task` and `add_signal`
- Low-level: `event.callbacks.append(fn)`, `ev.trigger(other)`, `ev.defused = True`

Checked by 32 tests in [`c1_events`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_events.py) (26), [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (4) and [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (2).

**Holds the functions that run when an event is processed.** Explained in [Actions when an event happens](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens). Gap M2: see [Where desflow Differs](#where-desflow-differs).

- SimPy: `Event.callbacks`
- YAML: `on: [{fire: {signal, from}}, {spawn: {...}}]` on a task or a signal, `fire` with `fail`, `defused` and `priority`, `when` on an entry, and `listen` on a task for a callback added during the run
- Builder: `on=[...]` on `@app.task` and `add_signal`
- Low-level: `event.callbacks.append(fn)`, `ev.trigger(other)`, `ev.defused = True`

Checked by 32 tests in [`c1_events`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_events.py) (26), [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (4) and [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (2).

**Marks a failed event as handled, so its exception does not stop the run.** Explained in [Actions when an event happens](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens).

- SimPy: `Event.defused`
- YAML: `on: [{fire: {signal, from}}, {spawn: {...}}]` on a task or a signal, `fire` with `fail`, `defused` and `priority`, `when` on an entry, and `listen` on a task for a callback added during the run
- Builder: `on=[...]` on `@app.task` and `add_signal`
- Low-level: `event.callbacks.append(fn)`, `ev.trigger(other)`, `ev.defused = True`

Checked by 32 tests in [`c1_events`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_events.py) (26), [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (4) and [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (2).

**Stops the run with the exception of a failed event that no process or callback handles.** Explained in [Failure nobody handles](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/failures/#failures).

- SimPy: A failed event nobody handles stops the run
- YAML: `run: {on_unhandled_failure: continue}` to go on, `stop` is the default
- Builder: `run(on_unhandled_failure=...)` or `start(on_unhandled_failure=...)`
- Low-level: `DynamicRealtimeEnvironment(on_unhandled_failure=...)`

Checked by 3 tests in [`c1_events`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_events.py).

**Tell whether an event has been triggered, has been processed and succeeded, and give its value.** Explained in [Event inspection](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: `Event.triggered`, `processed`, `ok` and `value`
- YAML: `signal.<name>.count`, `.ok` and `.value` in a condition
- Builder: `context.signal(name).wait()` and its SimPy attributes, or the signal's `count`, `ok` and `value`
- Low-level: the same attributes of SimPy events and `Signal`

Checked by 1 test in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py).

**Combine events into one that waits for all of them (`&`) or for the first of them (`|`).** Explained in [Wait for one event](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-the-first).

- SimPy: `a & b` and `a | b`
- YAML: `wait: {any: {stock: {signal: restocked}, limit: {timeout: 3}}}`
- Builder: `wait={"any": {...}}`
- Low-level: `yield a | b`
- Where desflow differs: Losing signal, timeout and condition branches are dropped when the wait ends; losing child flows keep running, as SimPy leaves processes running. A request branch not yet granted when the wait ends is cancelled; a SimPy `with` block held over the rest of the task leaves it queued.

Checked by 8 tests in [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (6).

**An event that happens after a delay.** Explained in [Timed wait](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/#a-timed-step).

- SimPy: `Timeout(delay)`
- YAML: a task with `service` or `service_time` and no `resource`
- Builder: `@app.task(service_id=...)` with no resource
- Low-level: `yield env.timeout(delay)`

Checked by 12 tests in [`c1_timed_wait`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_timed_wait.py) (5), [`builder_additions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_builder_additions.py) (2), [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (4) and [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (1).

**An event that happens after a delay and carries a value.** Explained in [Timeout value](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: `Timeout(delay, value)`
- YAML: `wait: {timeout: {after: 40 s, value: ...}}`
- Builder: the same mapping in `wait=`
- Low-level: `env.timeout(delay, value)`

Checked by 2 tests in [`c1_events`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_events.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

## Processes

**Starts a process and suspends the one that started it until the new process ends.** Explained in [Start and wait for a process](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: Start a process and wait for it
- YAML: `wait: {task: inspect}`
- Builder: `wait={"task": "inspect"}`, or `yield context.call("inspect")`
- Low-level: `yield env.process(child())`

Checked by 13 tests in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (5), [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (7) and [`generator_streams`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_generator_streams.py) (1).

**Gives the waiting process the value a process returns when it ends.** Explained in [Process return value](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: A process's return value
- YAML: `wait.inspect.value.<field>`, the last payload of the child flow
- Builder: the same, or the value of `context.call(...)`
- Low-level: `value = yield env.process(child())`

Checked by 1 test in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py).

**Raises an exception from a process in the process that waits for it.** Explained in [Process failure](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: An exception in a process reaches the process that waits for it
- YAML: `fail`, `fail_reason` and `on_fail` on a task, and `on_fail` alone for a failed wait or step; a failure no `on_fail` handles fails the waiting task
- Builder: the same, and a function that raises fails its task
- Low-level: `try: yield env.process(child())` and `except`
- Where desflow differs: By default (`on_unhandled_failure` is `stop`) a failure no task waits for stops the run, matching SimPy and the low-level API, where an exception nobody handles stops the run; under `on_unhandled_failure` of `continue` it ends the entity's flow in YAML and the builder, publishes `failed` and is counted, and the run goes on.

Checked by 4 tests in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (2), [`builder_additions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_builder_additions.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Tells whether a process is still running.** Explained in [Process inspection](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: `Process.is_alive`
- YAML: `wait.<branch>.done`, `.ok` and `.value` after the wait
- Builder: the process `context.spawn` and `context.call` return
- Low-level: the process `env.process` returns

Checked by 2 tests in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py).

**Gives the event a process waits for at this moment.** Explained in [The running process](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/#how-it-works).

- SimPy: `Process.target`
- YAML: `process.name`, `process.instance`, `process.target` in a condition
- Builder: `context.env.active_process`; `.name` and `.target` on a process
- Low-level: `env.active_process.name`, `.target`

Checked by 7 tests in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (5), [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Gives the name of the function that started a process.** Explained in [The running process](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/processes/#how-it-works).

- SimPy: `Process.name`
- YAML: `process.name`, `process.instance`, `process.target` in a condition
- Builder: `context.env.active_process`; `.name` and `.target` on a process
- Low-level: `env.active_process.name`, `.target`

Checked by 7 tests in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (5), [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Suspends a process until another process, started earlier, ends.** Explained in [Wait for a process already running](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: Wait for a process that is already running
- YAML: `wait: {running: {task: charge, where: ...}}`
- Builder: the same in `wait=`, or `yield` a process from `context.spawn` or `call`
- Low-level: `yield process`

Checked by 8 tests in [`c1_processes`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_processes.py) (7) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Starts a process after a delay instead of now.** Explained in [Start a process after a delay](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#start-a-flow-without-waiting).

- SimPy: `start_delayed(env, generator, delay)`
- YAML: `spawn: {task: courier, after: 3 s}`
- Builder: `spawn={"task", "after"}`, or `context.spawn(generator, delay=3)`
- Low-level: `simpy.util.start_delayed(env, generator, 3)`

Checked by 3 tests in [`c1_processes`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_processes.py).

## Conditions

**An event that happens when the first of several events happens.** Explained in [Wait for one event](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-the-first).

- SimPy: `AnyOf` and `env.any_of`
- YAML: `wait: {any: {stock: {signal: restocked}, limit: {timeout: 3}}}`
- Builder: `wait={"any": {...}}`
- Low-level: `yield a | b`
- Where desflow differs: Losing signal, timeout and condition branches are dropped when the wait ends; losing child flows keep running, as SimPy leaves processes running. A request branch not yet granted when the wait ends is cancelled; a SimPy `with` block held over the rest of the task leaves it queued.

Checked by 8 tests in [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (6).

**An event that happens when all of several events have happened.** Explained in [Wait for all events](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-all).

- SimPy: `AllOf` and `env.all_of`
- YAML: `wait: {all: {...}}`
- Builder: `wait={"all": {...}}`
- Low-level: `yield a & b`

Checked by 9 tests in [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (7).

**Gives the events of a condition that have happened, with their values, in the order of the condition.** Explained in [Wait for one event](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-the-first).

- SimPy: `ConditionValue`
- YAML: `wait: {any: {stock: {signal: restocked}, limit: {timeout: 3}}}`
- Builder: `wait={"any": {...}}`
- Low-level: `yield a | b`
- Where desflow differs: Losing signal, timeout and condition branches are dropped when the wait ends; losing child flows keep running, as SimPy leaves processes running. A request branch not yet granted when the wait ends is cancelled; a SimPy `with` block held over the rest of the task leaves it queued.

Checked by 8 tests in [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (6).

**Fails a condition at once when one of its events fails.** Explained in [Wait for all events](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-all).

- SimPy: A member that fails fails the condition
- YAML: `wait: {all: {...}}`
- Builder: `wait={"all": {...}}`
- Low-level: `yield a & b`

Checked by 9 tests in [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (7).

**Builds a condition from other conditions, such as `(a & b) | c`.** Explained in [Nested and mixed conditions](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#nest-any-and-all).

- SimPy: Nested and mixed conditions
- YAML: `wait: {all: {look: {task: inspect}, either: {any: {...}}}}`
- Builder: `wait={"all": {...}}` with nested groups
- Low-level: `yield (a | b) & c`

Checked by 3 tests in [`c2_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_conditions.py).

**Builds a condition with a rule of your own that decides when it happens.** Explained in [A condition with a custom rule](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-some-of-them).

- SimPy: `Condition(env, evaluate, events)` with a rule of your own
- YAML: `wait: {at_least: 2, of: {...}}`
- Builder: `wait={"at_least": 2, "of": {...}}`
- Low-level: `Condition(env, lambda evs, c: c >= 2, [...])`

Checked by 6 tests in [`c2_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_conditions.py) (4) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (2).

**Builds a condition from an empty list of events, which happens at once.** Explained in [A condition over no events](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-nothing).

- SimPy: A condition over no events
- YAML: `wait: {any: {}}` or `wait: {all: {}}`
- Builder: `wait={"any": {}}`
- Low-level: `env.any_of([])`

Checked by 1 test in [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py).

**Uses a request, get or put as one of the events of a condition.** Explained in [Request, put or get as a wait branch](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-a-unit-an-item-or-room).

- SimPy: A request, get or put as a member of a condition
- YAML: `wait: {request: packers}`, `wait: {get: {store: parcels}}`, `wait: {put: {store: ...}}`
- Builder: `wait={"request": "packers"}` or `wait={"get": {...}}`
- Low-level: `yield req | env.timeout(3)`

Checked by 11 tests in [`c2_conditions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_conditions.py) (10) and [`c2_wait_for_one_or_all`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c2_wait_for_one_or_all.py) (1).

## Interrupts

**Raises an `Interrupt` in a process that is waiting, with a cause.** Explained in [Interrupt while waiting](https://jaehyeon.me/desflow/latest/learn/shop-logic/interruptions/#interrupt-a-task).

- SimPy: `Process.interrupt(cause)` while the process waits
- YAML: `interrupt: {task: waiter}` on a scenario step or a task; `on_interrupt` on the task
- Builder: `context.interrupt("waiter")`; `@app.task(on_interrupt=...)`
- Low-level: `process.interrupt(cause)`, caught as `simpy.Interrupt`
- Where desflow differs: A task's own `spawn:` and `interrupt:` keys run in a fixed order, interrupt first, so one task cannot interrupt the instance it spawns with them. List a `spawn` and then an `interrupt` in `on:`, which run in the order written, or use `context.spawn` and `context.interrupt`.

Checked by 5 tests in [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (3) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (2).

**Interrupts a process during a delay, which ends the delay early.** Explained in [Interrupt during service](https://jaehyeon.me/desflow/latest/learn/shop-logic/interruptions/#resume-restart-or-abandon).

- SimPy: An interrupt during a timeout
- YAML: `on_interrupt: resume`, `restart` or `abandon`, and `on_interrupt_run` for a task that runs first
- Builder: `@app.task(on_interrupt=...)`
- Low-level: `except simpy.Interrupt`, then request again

Checked by 27 tests in [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (14), [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (12).

**The exception an interrupted process receives, and the cause given with it.** Explained in [Interrupt during service](https://jaehyeon.me/desflow/latest/learn/shop-logic/interruptions/#resume-restart-or-abandon).

- SimPy: `Interrupt` and `Interrupt.cause`
- YAML: `on_interrupt: resume`, `restart` or `abandon`, and `on_interrupt_run` for a task that runs first
- Builder: `@app.task(on_interrupt=...)`
- Low-level: `except simpy.Interrupt`, then request again

Checked by 27 tests in [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (14), [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (12).

**Delivers several interrupts sent at the same time one after another, in the order they were sent.** Explained in [Interrupt during service](https://jaehyeon.me/desflow/latest/learn/shop-logic/interruptions/#resume-restart-or-abandon).

- SimPy: Several interrupts at one time arrive in order
- YAML: `on_interrupt: resume`, `restart` or `abandon`, and `on_interrupt_run` for a task that runs first
- Builder: `@app.task(on_interrupt=...)`
- Low-level: `except simpy.Interrupt`, then request again

Checked by 27 tests in [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (14), [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (12).

**Lets an interrupted process wait again for the event it waited for before.** Explained in [Interrupt while waiting](https://jaehyeon.me/desflow/latest/learn/shop-logic/interruptions/#interrupt-a-task).

- SimPy: A process waits again for the same event after an interrupt
- YAML: `interrupt: {task: waiter}` on a scenario step or a task; `on_interrupt` on the task
- Builder: `context.interrupt("waiter")`; `@app.task(on_interrupt=...)`
- Low-level: `process.interrupt(cause)`, caught as `simpy.Interrupt`
- Where desflow differs: A task's own `spawn:` and `interrupt:` keys run in a fixed order, interrupt first, so one task cannot interrupt the instance it spawns with them. List a `spawn` and then an `interrupt` in `on:`, which run in the order written, or use `context.spawn` and `context.interrupt`.

Checked by 5 tests in [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (3) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (2).

**Interrupts the calling process when a given event happens, most often the end of another process.** Explained in [Be told when an event happens](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#be-told-when-a-flow-ends).

- SimPy: `subscribe_at(event)`
- YAML: `subscribe: {task: charger}`
- Builder: `@app.task(subscribe=...)`, or `context.subscribe(event)`
- Low-level: `simpy.util.subscribe_at(event)`

Checked by 3 tests in [`c1_processes`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_processes.py) (2) and [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (1).

## Resources

**Gives the maximum capacity of a resource, container or store.** Explained in [Initial resources](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#initial-time).

- SimPy: `capacity`
- YAML: `resources.<name>.current_cap`
- Builder: `add_resource(name, current_cap=2, ...)`
- Low-level: `CapacityConfig(current_cap=2, ...)`

Checked by 7 tests in [`c6_initial_state`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_initial_state.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (6).

**Asks for a unit of a resource and waits for it, then gives it back.** Explained in [Ordinary request and release](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#declare-a-resource).

- SimPy: `Resource.request()` and `release(request)`
- YAML: `resource: packers` on a task
- Builder: `@app.task(resource_id="packers")`
- Low-level: `with packers.request() as req: yield req`

Checked by 10 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (1), [`low_level_surface`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_low_level_surface.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (8).

**Gives a resource one unit when no capacity is set.** Explained in [Resource capacity defaults to 1](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#declare-a-resource).

- SimPy: Capacity defaults to 1
- YAML: `resources: {packers: {}}`; `current_cap: .inf` for no limit
- Builder: `add_resource("packers")`; `current_cap=float("inf")` for no limit
- Low-level: `DynamicResource(env, sim_id, name)` with no registry entry; `CapacityConfig(current_cap=float("inf"))` for no limit

Checked by 2 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py).

**Refuses a capacity of 0 or below for a resource, container or store.** Difference kept by design: see [Where desflow Differs](#where-desflow-differs).

- SimPy: Capacity must be above 0
- YAML: `current_cap: 0` on a resource
- Builder: `add_resource(name, current_cap=0)`
- Low-level: `DynamicResource(env, ..., current_cap=0)`

**Gives the number of units of a resource in use.** Explained in [Resource count, queue and users](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#read-the-queue-and-users).

- SimPy: `Resource.count`
- YAML: `users.<resource>` and `queue.<resource>` in a condition, such as `balk: queue.machine >= 2`
- Builder: `get_resource(name).count`, `.users` and `.queue`
- Low-level: `resource.count`, `.users` and `.queue`, the list of waiting requests in service order
- Where desflow differs: A `lifo` queue serves the newest request first, which SimPy has no counterpart of.

Checked by 6 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (3) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (3).

**Gives the requests that wait for a unit of a resource.** Explained in [Resource count, queue and users](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#read-the-queue-and-users).

- SimPy: `Resource.queue`
- YAML: `users.<resource>` and `queue.<resource>` in a condition, such as `balk: queue.machine >= 2`
- Builder: `get_resource(name).count`, `.users` and `.queue`
- Low-level: `resource.count`, `.users` and `.queue`, the list of waiting requests in service order
- Where desflow differs: A `lifo` queue serves the newest request first, which SimPy has no counterpart of.

Checked by 6 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (3) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (3).

**Gives the requests that hold a unit of a resource.** Explained in [Resource count, queue and users](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#read-the-queue-and-users).

- SimPy: `Resource.users`
- YAML: `users.<resource>` and `queue.<resource>` in a condition, such as `balk: queue.machine >= 2`
- Builder: `get_resource(name).count`, `.users` and `.queue`
- Low-level: `resource.count`, `.users` and `.queue`, the list of waiting requests in service order
- Where desflow differs: A `lifo` queue serves the newest request first, which SimPy has no counterpart of.

Checked by 6 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (3) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (3).

**Gives back a unit of a resource when a `with` block ends, or cancels the request if the block ends before the grant.** Explained in [Cancel before a grant](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#cancel-and-release).

- SimPy: `Request` as a context manager
- YAML: `patience`, or an interrupt with `on_interrupt: abandon`
- Builder: the same
- Low-level: `request.cancel()`, or leaving the `with` block

Checked by 6 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (4).

**Withdraws a request that still waits for a unit.** Explained in [Cancel before a grant](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#cancel-and-release).

- SimPy: `Request.cancel()` before the grant
- YAML: `patience`, or an interrupt with `on_interrupt: abandon`
- Builder: the same
- Low-level: `request.cancel()`, or leaving the `with` block

Checked by 6 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (4).

**Gives back a unit that a request already holds.** Explained in [Cancel after a grant](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#cancel-and-release).

- SimPy: `cancel()` after the grant
- YAML: an interrupt with `on_interrupt: abandon` gives the unit back, as `release(request)` does; for a request made with `open`, `cancel` after the grant releases nothing, and `release: {handle: <name>}` does
- Builder: the same
- Low-level: `request.cancel()` releases nothing; `resource.release(request)`

Checked by 2 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**The event that gives back a unit, and the request it gives back.** Explained in [Release event and its request](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#cancel-and-release).

- SimPy: `Release` and `Release.request`
- YAML: implicit: a task releases its units when it ends
- Builder: `get_resource(name).release(request)`
- Low-level: `resource.release(request)` returns a `DynamicRelease` whose `request` is the request

Checked by 1 test in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py).

**Gives the time a request was granted its unit.** Explained in [Usage since](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#read-the-queue-and-users).

- SimPy: `Request.usage_since`
- YAML: `usage_since` in the `preempted` record
- Builder: `get_resource(name).users[i].usage_since`, and the `preempted` record
- Low-level: `request.usage_since`

Checked by 1 test in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py).

**Holds units of more than one resource at the same time.** Explained in [Hold several resources](https://jaehyeon.me/desflow/latest/user-guide/resources/several-resources/#several-resources).

- SimPy: Hold several resources at once
- YAML: `resource: [picker, bench]`, acquired in that order; `keep` hands units to the next task; `seize` keeps a unit across later tasks until `release`
- Builder: `@app.task(resource_id=["picker", "bench"], keep=[...])`, or `seize=` and `release=`
- Low-level: nested `with` blocks
- Where desflow differs: A model whose tasks acquire two resources in opposite orders gets a warning when it is built, because those tasks could each wait for ever; it runs as SimPy runs it. Two requests under one `all` wait are made together, so another task that acquires the two one after the other, in either order, gets the warning too. A task that keeps a unit for a task that requests it in its wait is refused, because the wait would queue behind the unit it keeps.

Checked by 9 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (4), [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (4).

**Leaves the queue when a unit is not granted within a time limit.** Explained in [Reneging](https://jaehyeon.me/desflow/latest/learn/shop-logic/patience/#leave-after-a-wait).

- SimPy: Reneging: `req | env.timeout(patience)`
- YAML: `patience: 3 min` on a task, and `on_renege` for the task that follows
- Builder: `@app.task(..., patience=180)`
- Low-level: `yield req | env.timeout(patience)`, then `req not in result`

Checked by 15 tests in [`c7_patience`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c7_patience.py) (10), [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (3) and [`builder_additions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_builder_additions.py) (2).

**Does not join the queue when a condition holds, such as the queue being too long.** Explained in [Balking](https://jaehyeon.me/desflow/latest/learn/shop-logic/patience/#refuse-to-join).

- SimPy: Balking: a check before `request()`
- YAML: `balk: <condition>` on a task
- Builder: `@app.task(..., balk="...")`
- Low-level: an `if` before the request

Checked by 2 tests in [`c7_patience`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c7_patience.py).

**Asks for a unit with a priority, and a lower number is served first.** Explained in [Priority request](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#task-priority).

- SimPy: `PriorityResource.request(priority)`
- YAML: `queue: priority` on the resource and `priority` on a task
- Builder: `add_resource(..., queue="priority")` and `@app.task(priority=...)`
- Low-level: `DynamicResource(..., queue="priority")` and `request(priority=...)`
- Where desflow differs: Equal priorities are served first in, first out, as in SimPy; desflow also offers `fifo` and `lifo` queues, under which a request's priority is ignored.

Checked by 7 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (5).

**Gives a request priority 0 when none is set.** Explained in [Default request priority 0](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#task-priority).

- SimPy: Default request priority 0
- YAML: a task with no `priority` on a `queue: priority` resource
- Builder: `@app.task(resource_id=...)` with no `priority`
- Low-level: `request()` with no priority

Checked by 1 test in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py).

**Orders waiting requests by priority, then by the time they were made, then preempting before not preempting.** Explained in [Priority request](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#task-priority).

- SimPy: Queue key `(priority, time, not preempt)`
- YAML: `queue: priority` on the resource and `priority` on a task
- Builder: `add_resource(..., queue="priority")` and `@app.task(priority=...)`
- Low-level: `DynamicResource(..., queue="priority")` and `request(priority=...)`
- Where desflow differs: Equal priorities are served first in, first out, as in SimPy; desflow also offers `fifo` and `lifo` queues, under which a request's priority is ignored.

Checked by 7 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (5).

**Lets a request with a higher priority take a unit from its user, unless it asks not to preempt.** Explained in [Priority preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#preemption).

- SimPy: `PreemptiveResource` and `request(preempt=False)`
- YAML: `resources.<name>.preempt: true` with `queue: priority`; `preempt: false` on a task opts out, and `preempt` on a task, a `seize` or a `request` mapping can be an expression read for each request
- Builder: `add_resource(..., queue="priority", preempt=True)`
- Low-level: `DynamicResource(..., queue="priority", preempt=True)` and `request(priority, preempt=True)`

Checked by 9 tests in [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (5) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (4).

**Picks as the user to preempt the one whose request has the largest key: the lowest priority, then the latest.** Explained in [Priority preemption](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#preemption).

- SimPy: The user to preempt is picked with `max`
- YAML: `resources.<name>.preempt: true` with `queue: priority`; `preempt: false` on a task opts out, and `preempt` on a task, a `seize` or a `request` mapping can be an expression read for each request
- Builder: `add_resource(..., queue="priority", preempt=True)`
- Low-level: `DynamicResource(..., queue="priority", preempt=True)` and `request(priority, preempt=True)`

Checked by 9 tests in [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py) (5) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (4).

**The cause a preempted process receives: the process that took the unit, when the unit was granted, and the resource.** Explained in [Preemption cause](https://jaehyeon.me/desflow/latest/user-guide/resources/priority-and-preemption/#preemption).

- SimPy: `Preempted` cause: `by`, `usage_since`, `resource`
- YAML: `preempted_by` and `usage_since` in the `preempted` record
- Builder: the same record
- Low-level: `simpy.Interrupt.cause` is a `Preempted` with `by`, `usage_since` and `resource`

Checked by 3 tests in [`c3_interrupts`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c3_interrupts.py).

**A waiting line that refuses a new request when a given number of requests already wait.** Explained in [A waiting line with a limit](https://jaehyeon.me/desflow/latest/learn/shop-logic/patience/#refuse-to-join).

- SimPy: `SortedQueue(maxlen)`, a waiting line with a limit
- YAML: `queue_limit: 5` on the resource; a scenario step, an `on:` entry or an ingress message can write the registry path `<sim_id>.resources.<name>.queue_limit` during the run, with a whole number or `null` for no limit
- Builder: `add_resource(..., queue_limit=5)`
- Low-level: `DynamicResource(..., queue_limit=5)`: `request()` raises `RuntimeError`
- Where desflow differs: A task whose first resource refuses it publishes `balked` with the resource before it publishes `queued`, which SimPy has no counterpart of.

Checked by 10 tests in [`c7_patience`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c7_patience.py) (9) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Changes how a resource grants requests or keeps its waiting line, in a class of your own.** Explained in [Subclass a resource to change its rules](https://jaehyeon.me/desflow/latest/user-guide/resources/custom-resource-types/#custom-resource-types).

- SimPy: A subclass of `PreemptiveResource` or `Resource`
- YAML: `type: !python module.Class` on the resource; for a queue order and a preemption rule, `queue: {rank, ties}` and `preempt: {when, victim}`
- Builder: `add_resource(..., type=Class)`, or `queue={...}` and `preempt={...}`
- Low-level: a subclass of `ResourceBase`, built as `Class(env, sim_id, name)`

Checked by 7 tests in [`c4_resources`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c4_resources.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (6).

## Containers

**Adds an amount to a container, and waits while there is not enough space.** Explained in [Bounded quantity put](https://jaehyeon.me/desflow/latest/learn/shop-logic/stock/#take-and-restock).

- SimPy: `Container.put(amount)`
- YAML: `put: {container: stock, amount: 2}` on a task
- Builder: `@app.task(put={"container": "stock", "amount": 2})`
- Low-level: `yield stock.put(2)`

Checked by 1 test in [`c5_bounded`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_bounded.py).

**Takes an amount from a container, and waits while there is not enough in it.** Explained in [Bounded quantity get](https://jaehyeon.me/desflow/latest/learn/shop-logic/stock/#take-and-restock).

- SimPy: `Container.get(amount)`
- YAML: `get: {container: stock, amount: 1}` on a task
- Builder: `@app.task(get={"container": "stock", "amount": 1})`
- Low-level: `yield stock.get(1)`

Checked by 1 test in [`c5_bounded`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_bounded.py).

**Starts a container with an amount already in it.** Explained in [Initial inventory](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#start-with-items-and-serve-by-priority).

- SimPy: `Container(init=)`
- YAML: `containers.<name>.level` and `stores.<name>.items`
- Builder: `add_container(..., level=6)` and `add_store(..., items=[...])`
- Low-level: `ContainerConfig(level=6)` and `DynamicStore(..., items=[...])`

Checked by 4 tests in [`c6_initial_state`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_initial_state.py) (1), [`low_level_surface`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_low_level_surface.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Gives the amount a container holds.** Explained in [Wait for a shared condition](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows).

- SimPy: `Container.level`
- YAML: `wait: {condition: level.stock >= 5}`
- Builder: `wait={"condition": "level.stock >= 5"}`
- Low-level: `env.watch_changes(check)`, or a SimPy loop

Checked by 1 test in [`c1_process_interaction`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c1_process_interaction.py).

**Gives a container no upper limit when no capacity is set.** Explained in [Container with no capacity limit](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#declare-them).

- SimPy: Capacity defaults to infinity
- YAML: `containers.<name>` with no `current_cap`, or `current_cap: .inf`
- Builder: `add_container("tank")`
- Low-level: `DynamicContainer(env, "Dock", "tank")`

Checked by 1 test in [`c5_unbounded`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_unbounded.py).

## Stores

**Adds an item to a store, and waits while the store is full.** Explained in [Bounded store put](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#take-and-add).

- SimPy: `Store.put(item)`
- YAML: `put: {store: parcels}` on a task
- Builder: `@app.task(put={"store": "parcels"})`
- Low-level: `yield parcels.put(item)`

Checked by 3 tests in [`c5_bounded`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_bounded.py) (1), [`c5_inventory`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_inventory.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Takes the first item from a store, and waits while the store is empty.** Explained in [Bounded store get](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#take-and-add).

- SimPy: `Store.get()`
- YAML: `get: {store: parcels}` on a task
- Builder: `@app.task(get={"store": "parcels"})`
- Low-level: `item = yield parcels.get()`

Checked by 1 test in [`c5_bounded`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_bounded.py).

**Grants one waiting get for each put made at the same time.** Explained in [Equal-time puts and gets](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#take-and-add).

- SimPy: One waiting get is granted per put at one time
- YAML: `get: {store: parcels}` and `put: {store: parcels}` on tasks
- Builder: `@app.task(get={"store": "parcels"})`
- Low-level: `yield parcels.put(item)`, `item = yield parcels.get()`
- Where desflow differs: While a get with a filter waits, the store grants as SimPy's `FilterStore` does, looking at every waiting get each time. Once no filtered get waits, it grants one get per trigger, as `Store` does, where `FilterStore` goes on to the next. With filtered and unfiltered gets in one queue, gets at the same instant can therefore resume in a different order than in `FilterStore`; each get receives the same item.

Checked by 2 tests in [`c5_store_order`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_store_order.py).

**Starts a store with items already in it.** Explained in [Initial inventory](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#start-with-items-and-serve-by-priority).

- SimPy: A store that starts with items
- YAML: `containers.<name>.level` and `stores.<name>.items`
- Builder: `add_container(..., level=6)` and `add_store(..., items=[...])`
- Low-level: `ContainerConfig(level=6)` and `DynamicStore(..., items=[...])`

Checked by 4 tests in [`c6_initial_state`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_initial_state.py) (1), [`low_level_surface`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_low_level_surface.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (1).

**Gives the items a store holds.** Explained in [Read a store's items](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#registry-paths-readings-and-conditions).

- SimPy: `Store.items`
- YAML: `count(parcels) > 3`, `payload.sku in items(shelf, item.sku)`
- Builder: `wait={"condition": "count(parcels) > 3"}`
- Low-level: `len(parcels.items)`, `sku in [i["sku"] for i in shelf.items]`

Checked by 4 tests in [`c5_store_items`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_store_items.py).

**Gives a store no upper limit when no capacity is set.** Explained in [Store with no capacity limit](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#declare-them).

- SimPy: Capacity defaults to infinity
- YAML: `stores.<name>` with no `current_cap`, or `current_cap: .inf`
- Builder: `add_store("parcels")`
- Low-level: `DynamicStore(env, "Shop", "parcels")`

Checked by 1 test in [`c5_unbounded`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_unbounded.py).

**A store that gives out its items smallest first, and the wrapper that pairs an item with a priority.** Explained in [Priority item storage](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#start-with-items-and-serve-by-priority).

- SimPy: `PriorityStore` and `PriorityItem`
- YAML: `stores.<name>.priority: true` and a `put` with `priority`; `get_queue` and `put_queue` of a store or container rank the waiting gets and puts by an expression
- Builder: `add_store(..., priority=True)` and `put={"store": ..., "priority": ...}`
- Low-level: `DynamicStore(..., priority=True)` with `simpy.PriorityItem`

Checked by 5 tests in [`c5_priority_store`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_priority_store.py) (2) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (3).

**Ranks the items of a priority store by any value that can be ordered, such as a number, a string or a tuple.** Explained in [Priorities of any orderable type](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#start-with-items-and-serve-by-priority).

- SimPy: Priorities of any orderable type
- YAML: `put: {store: queue, priority: [1, urgent]}`
- Builder: `put={"store": "queue", "priority": (1, "urgent")}`
- Low-level: `simpy.PriorityItem((1, "urgent"), item)`

Checked by 3 tests in [`c5_priority_store`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_priority_store.py).

**Takes the first item of a store that a given function accepts.** Explained in [Filtered retrieval](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#pick-an-item-or-give-up-waiting).

- SimPy: `FilterStore.get(filter)`
- YAML: `get: {store: parcels, filter: item.area == attr.area}`
- Builder: `get={"store": "parcels", "filter": ...}`, a condition or a function of the item
- Low-level: `parcels.get(filter=lambda item: ...)`
- Where desflow differs: A priority store takes no filter, because SimPy has no store that both ranks and filters.

Checked by 3 tests in [`c5_inventory`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_inventory.py) (1) and [`builder_additions`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_builder_additions.py) (2).

**Give the puts and gets that wait at a store or container.** Explained in [Put and get queues of a store or container](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#registry-paths-readings-and-conditions).

- SimPy: `put_queue` and `get_queue`
- YAML: `wait: {condition: putters.dock > 2}`, `getters.dock == 0`
- Builder: `wait={"condition": "putters.dock > 2"}`
- Low-level: `dock.put_queue`, `dock.get_queue`

Checked by 5 tests in [`c5_queues`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_queues.py) (3) and [`low_level_surface`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_low_level_surface.py) (2).

**Cancel a waiting put or get, by hand or when a `with` block ends before the grant.** Explained in [Cancel store get](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#pick-an-item-or-give-up-waiting).

- SimPy: `Put` and `Get` as context managers, and `cancel()`
- YAML: `patience` on a task with a store `get`
- Builder: `@app.task(get={"store": ...}, patience=...)`
- Low-level: `get.cancel()`

Checked by 1 test in [`c5_inventory`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_inventory.py).

**Tries waiting gets and puts in the order they joined their queue.** Explained in [Equal-time puts and gets](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#take-and-add).

- SimPy: Grant rules: waiting gets and puts are tried in queue order
- YAML: `get: {store: parcels}` and `put: {store: parcels}` on tasks
- Builder: `@app.task(get={"store": "parcels"})`
- Low-level: `yield parcels.put(item)`, `item = yield parcels.get()`
- Where desflow differs: While a get with a filter waits, the store grants as SimPy's `FilterStore` does, looking at every waiting get each time. Once no filtered get waits, it grants one get per trigger, as `Store` does, where `FilterStore` goes on to the next. With filtered and unfiltered gets in one queue, gets at the same instant can therefore resume in a different order than in `FilterStore`; each get receives the same item.

Checked by 2 tests in [`c5_store_order`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_store_order.py).

**Changes what a put or get does to a store or container, in a class of your own.** Explained in [Custom store and container types](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#custom-store-and-container-types).

- SimPy: A custom store or container with `_do_put` and `_do_get`
- YAML: `stores.<name>.type: !python pkg.module.MyStore`
- Builder: `add_store(..., type=MyStore)`
- Low-level: `class MyStore(StoreBase)`

Checked by 3 tests in [`c5_custom_types`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_custom_types.py).

**Changes how the puts and gets that wait are kept, in a class of your own.** Explained in [Custom put and get queue types](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#custom-store-and-container-types).

- SimPy: Custom `PutQueue` and `GetQueue` types
- YAML: `stores.<name>.type: !python pkg.module.MyStore`, with `PutQueue` and `GetQueue` set on the class
- Builder: `add_store(..., type=MyStore)`
- Low-level: `class MyStore(StoreBase): PutQueue = MyQueue`

Checked by 2 tests in [`c5_custom_types`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_custom_types.py).

**Changes the put and get events of a store or container, in a class of your own.** Explained in [Custom put and get event classes](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#custom-store-and-container-types).

- SimPy: Custom `Put` and `Get` event classes
- YAML: `stores.<name>.type: !python pkg.module.MyStore`, with `put` and `get` set on the class
- Builder: `add_store(..., type=MyStore)`
- Low-level: `class MyStore(StoreBase): put = BoundClass(MyPut)`

Checked by 1 test in [`c5_custom_types`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_custom_types.py).

**Makes a put or get and carries on at once, without waiting for it to be granted.** Explained in [A put or get the process does not wait for](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#take-and-add).

- SimPy: A put or get the process does not wait for
- YAML: `put: {store: dock, wait: false}` on a task
- Builder: `put={"store": "dock", "wait": False}`
- Low-level: `dock.put(item)` without `yield`

Checked by 4 tests in [`c5_no_wait`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c5_no_wait.py).

## Real-Time

**Runs the simulation in step with the wall clock, with each time unit taking `factor` seconds.** Explained in [Real-time factor](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#factor).

- SimPy: `RealtimeEnvironment(factor)`
- YAML: `simulation.factor`
- Builder: `SimulationContext(factor=)`
- Low-level: `DynamicRealtimeEnvironment(factor=)`

Checked by 1 test in [`c6_pacing`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_pacing.py).

**Raises `RuntimeError` when processing falls behind the wall clock by more than `factor` seconds.** Explained in [Strict pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#strict-pacing).

- SimPy: `strict`: a step that falls behind raises `RuntimeError`
- YAML: `simulation.strict`
- Builder: `SimulationContext(strict=True)`
- Low-level: `DynamicRealtimeEnvironment(strict=True)`

Checked by 1 test in [`c6_pacing`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_pacing.py).

**Turns strict mode on by default, so falling behind raises `RuntimeError` unless `strict=False` is set.** Explained in [Strict by default](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#strict-pacing).

- SimPy: `strict` defaults to true
- YAML: `simulation.strict`, default `true` when `factor` is above 0 or `go_live_at` is set
- Builder: `SimulationContext(strict=None)`, the default, resolves the same way
- Low-level: `DynamicRealtimeEnvironment(strict=None)`, the default, resolves the same way

Checked by 4 tests in [`c6_pacing`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_pacing.py).

**Sets the real-time clock to the wall clock now, so a delay before the run does not count as falling behind.** Explained in [Sync with the wall clock](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#strict-pacing).

- SimPy: `sync()`
- YAML: `run: {sync: true}` syncs as the run starts, and `sync: true` on a segment as that segment starts, or `app.start()`, then `app.env.sync()`
- Builder: `app.env.sync()` after `start()`; the builder syncs once when it builds a strict run
- Low-level: `env.sync()`

Checked by 3 tests in [`c6_pacing`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_c6_pacing.py) (1) and [`yaml_only`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc4/tests/conformance/test_yaml_only.py) (2).

## Where desflow Differs

**Holds the functions that run when an event is processed.** In [Events and Timeout](#events-and-timeout).

- SimPy: `Event.callbacks`
- **Gap M2:** A Builder `on=` refuses a callable. A generator started with `add_process` can add a function to an event's callbacks.

**Refuses a capacity of 0 or below for a resource, container or store.** In [Resources](#resources).

- SimPy: Capacity must be above 0
- **Difference kept by design:** A resource, store or container may have a capacity of 0, at the start or later, because its capacity changes during a run: a shift with no packers, a shelf that is closed. At capacity 0 a resource grants no request, and a store or container accepts no put, until the capacity rises; a get still takes what a store or container holds.
