# Upgrading to 1.0

This page is for a model, a script or a pipeline that runs on desflow 0.28.0. Each item says what 0.28.0 did, what 1.0 does and what to change. 1.0 follows SimPy's rules for time, ties and failures, so a seeded model can publish other records than it did on 0.28.0, and some YAML forms now mean something else. From 1.0 the [Compatibility Policy](https://jaehyeon.me/desflow/latest/about/compatibility/index.md) applies, and a 1.x release no longer changes a result without listing it.

To move a model to 1.0:

1. Install desflow 1.0 and uninstall `dynamic-des` if it is still installed, as [Removed Names](#removed-names) says.
1. Run `desflow check` on every blueprint. It names each key or value that 1.0 refuses, with its line.
1. Run the model's tests, such as `desflow test`, and compare its stored results. Where a test fails, find the change below that explains it. `desflow test --fix` writes the new values once you accept them.

## Removed Names

- **`dynamic_des` and `ddes`.** 0.28.0 kept the import name `dynamic_des` and the `ddes` command, each with a warning. 1.0 removes both: `import dynamic_des` raises `ModuleNotFoundError`, and no `ddes` command is installed. Import `desflow` and run `desflow`. Uninstall `dynamic-des`; its last release installs a desflow below 1.0.
- **`**registry_kwargs` of `ConfluentAvroSerializer` and `ConfluentAvroDeserializer`.** 0.26.0 deprecated the confluent-kafka keys. 1.0 removes them, and a call that passes one raises `TypeError`. Pass the named argument instead: `basic.auth.user.info` (`user:password`) becomes `auth=("user", "password")`, `ssl.ca.location` becomes `verify=`, `ssl.certificate.location`, `ssl.key.location` and `ssl.key.password` become `cert=`, and `max.retries` becomes `max_retries=`. The `bearer.auth.*` keys with the source `STATIC_TOKEN` become `headers={"Authorization": "Bearer <token>", "target-sr-cluster": "<logical cluster>"}`, with `"Confluent-Identity-Pool-Id": "<pool>"` when `bearer.auth.identity.pool.id` was set. Delete `cache.capacity` and `cache.latest.ttl.sec`, which had no effect. [Connect to a Secured Schema Registry](https://jaehyeon.me/desflow/latest/how-to/records-and-kafka/kafka-security/#connect-to-a-secured-schema-registry) shows the arguments.
- **`desflow.core.distributions`.** The module is gone. `DISTRIBUTIONS` is in `desflow.models.distributions` in 1.0, a module path that 1.x does not promise to keep. `desflow dist` lists the families from the command line.

## Run and Time

- **`strict` defaults to SimPy's rule.** In 0.28.0, `strict` was false in YAML and the Builder unless it was set, and it was never checked at `factor: 0`. In 1.0 it is true when it is not set and `factor` is above 0 or `go_live_at` is set, so a paced run that falls behind the wall clock raises `RuntimeError`. An explicit `strict: true` at `factor: 0` raises once the wall clock has moved, as SimPy does. `--sample`, replications, `desflow experiment` and `desflow explain --verify` run at `factor: 0` and ignore `strict`. Set `simulation.strict: false` (`SimulationContext(strict=False)`) to keep the old behaviour. See [Strict Pacing](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/clocks-and-pacing/#strict-pacing).
- **`on_unhandled_failure` defaults to `stop`.** In 0.28.0, YAML and the Builder defaulted to `continue`, so a failure that nobody handled ended one entity's flow and the run went on. In 1.0 the default is `stop`, as in SimPy and the Low-level API, and the run ends with the error. A model that relies on the old rule adds `run: {on_unhandled_failure: continue}` (`run(on_unhandled_failure="continue")`). See [Handle a Failure Nobody Handles](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/failures/#handle-a-failure-nobody-handles).
- **Signals fire once.** In 0.28.0 a signal re-armed after it fired. In 1.0 a signal fires once by default, as SimPy's `Event` does: a wait after the firing resumes at once, and a second firing is refused. Give a signal that fires more than once `once: false` (`add_signal(name, once=False)`, `Signal(env, name, once=False)`). A `once: true` signal that fired again dropped the second firing with a warning; it now raises under `on_unhandled_failure: stop`. See [One-Shot Signals](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/signals-and-waiting/#one-shot-signals).
- **A run with no `until` ends.** In 0.28.0 such a run never ended while statistics were on, or with a sink at a `factor` above 0, or with an ingress connector. In 1.0 it ends when only desflow's own events are left, and a `stop` that can no longer happen raises SimPy's `RuntimeError`. Give such a run an `until`. A live model meant to run on gives its connector an `until`. See [Stop Early](https://jaehyeon.me/desflow/latest/user-guide/time-and-pacing/run-length/#stop-early).
- **A Low-level run to an event ends.** In 0.28.0, `DynamicRealtimeEnvironment.run(until=<event>)` ran for ever on desflow's own timers when the event never came. In 1.0, with no ingress set up, it ends with SimPy's `RuntimeError` when no model event is left; with an ingress it goes on. Name a time as well: `env.run(until=event | env.timeout(t))`.
- **No smallest delay.** In 0.28.0 a service time or a drawn delay was at least 0.00001 seconds. In 1.0 the floor is 0: a constant 0 takes no time, and a normal draw below 0 is raised to 0. A constant gap of 0 starts entities at one instant without end, so the run never ends and `until` does not stop it, as in SimPy. Give such an arrival a gap above 0.
- **`sim_ts` is not rounded.** 0.28.0 rounded `sim_ts` to three decimals. 1.0 writes the clock as it is, so `1.357` may read `1.3565...`. Round `sim_ts` before you compare it. See [Field Values](https://jaehyeon.me/desflow/latest/reference/records-and-telemetry/#field-values).
- **A number keeps the type it is written in.** 0.28.0 converted times, amounts, container capacities and priorities to floats. In 1.0 a number keeps its written type: `env.now`, `peek()` and the readings of a step or segment print `5` for a time written `5`, where they printed `5.0`. A live update keeps the number as written, so `3` written over `2.5` is `3`. The record fields `capacity`, `level`, `wait_max` and `usage_since` are integers when their inputs are, such as `38` where 0.28.0 wrote `38.0`. `sim_ts` stays a float. Iceberg and Parquet sinks still write a statistic's `value` and `usage_since` as doubles. Text written over a parameter is still read as a float.

## Order at One Instant

These changes make a task go on in the same step as the SimPy process it stands for, so records at one instant can change order, and a unit or an item can go to another entity at a tie. A model with no events at the same time publishes the same records.

- **A task with one `wait` branch** now goes on in the step its event is processed, as `yield event` does in SimPy. To keep the old order, write the branch under `any`, as `wait: {any: {only: {timeout: 2}}}`. A `wait: {timeout: 5}` with `patience: 5 s` now starts and finishes at 5, where it reneged; the `any` form gives the old result.
- **`next`, `on_fail` and `on_interrupt_next`** now run the following task in the same process and the same step, so it asks for a unit before a child spawned at that instant, and a process you wait for ends when its whole flow ends. To let the child go first, give the following task `wait: {timeout: 0}`, which matches SimPy. A `subscribe` interrupt or a preempted kept unit now reaches the following task, which `on_interrupt: abandon` cuts short.
- **A task's `on:` actions** now run in the step the task ends, before its next task starts. A flag they set is read by the next task, but not by a `next` condition on the same task, which is chosen before they run. To run them one step later, fire a signal in the task's `on:` and put the actions in that signal's `on:`.
- **Tied replay rows** now start in one step, in file order, before any of them takes its first step. 0.28.0 started each row in a separate step.
- **An `interrupt` reaches an instance made in the same step,** from `spawn:` in `on:`, `context.spawn`, `context.call` or a `next` chain. In 0.28.0 it found no instance. The task keys `spawn:` and `interrupt:` still run interrupt first, so one task's own pair does not reach the new instance; two `on:` entries in the order spawn, interrupt do.
- **Patience at a tie.** When a patience ends at the instant a unit is granted and the patience's event comes first, the task now reneges. 0.28.0 served it. The same holds for a `get`, a `wait` branch and a request asked again after a preemption.
- **A grant at the end of a wait.** A request, get or put branch counts as won only when it was processed before the wait's condition, as `req in result` is in SimPy. An item a `get` was granted after that is lost. A request granted by then keeps its unit until the task ends. Order the events so the grant comes first when the model needs the unit or item.
- **Equal priorities in a priority store** now leave in the order SimPy's heap gives, which is not put order from four items on. To keep put order, give each item a priority that rises, such as `priority: "1 + process.instance / 1000000"`; in the Builder and the Low-level API a pair `(priority, number)` is a valid priority. See [Start With Items and Serve by Priority](https://jaehyeon.me/desflow/latest/user-guide/model-parts/stores-and-containers/#start-with-items-and-serve-by-priority).

## Resources, Waits and Interrupts

- **A unit is kept through a plain interrupt.** With `on_interrupt: resume` or `restart`, a plain interrupt no longer gives back a unit the task holds, so the task goes on with it, and other entities wait until the task ends. A task with several resources asks again only for the unit it lost. A unit kept with `keep` now goes back at once when the next task reneges, fails or has no next task. To give the unit back and queue again, use `on_interrupt: abandon` with `on_interrupt_next` naming a task that asks for it. See [Interrupts](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/#interrupts).
- **A granted `request` branch under `restart`.** In 0.28.0, an interrupt of a task with `on_interrupt: restart` gave back every granted `request` branch of its wait and asked again at the back of the line. In 1.0 the task keeps them and starts the other branches again. To queue again as before, write the task with `on_interrupt: abandon` and `on_interrupt_next` naming a copy of it, and name both tasks in every `interrupt` that names the first.
- **Interrupt or preemption at the end of the work.** A job interrupted or preempted at the instant its work ends now ends at that instant, publishes no `resumed` and asks for no unit. In 0.28.0 it waited for a further 0 seconds.
- **`keep` and a child flow.** A child flow started by `wait: {task: ...}` no longer gives back the unit its parent kept when it starts, balks or is abandoned. The unit stays until the parent's next task starts. A model that counted on the early release ends the parent's task with no `keep`.
- **Opposite-order acquisition loads.** 0.28.0 refused a model whose tasks take shared resources in opposite orders. 1.0 loads it and logs a WARNING from the logger `desflow.builder.context`, and the run can stop if two tasks wait for each other. A test that expected the `ValueError` reads the warning instead.
- **Preempting a finished process.** A unit held by a process that has ended, such as a Low-level `request` never released, now fails the flow that preempts it with SimPy's `RuntimeError`. 0.28.0 took the unit with no error. Release a unit before its process ends, with `resource.release(request)` or a `with` block.
- **Builder `preempt` as text.** `task(preempt="f")` was kept as text and counted as true. In 1.0, `"f"`, `"no"`, `"off"` and `"0"` are false, as in YAML. Write `preempt=True` or `preempt=False`.
- **`queue_limit` is a registry path.** A resource now has the path `<sim>.resources.<r>.queue_limit`, and `DynamicResource.queue_limit` is a read-only property that follows it, so `resource.queue_limit = 5` raises `AttributeError`. Change it with `env.registry.update("<sim>.resources.<r>.queue_limit", 5)`. `compile_parameters()` gives a resource as a subclass of `CapacityConfig` that also holds the limit, so compare its `current_cap` and `max_cap` rather than the whole object. See [Queue Limit](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#queue-limit).
- **A `batch` with a `timeout` whose members all leave** before it closes is dropped, so the next member starts a new batch and its `timeout` and `batch.age` count from that member. Compare the stored results of a model whose batch members can renege, balk or be interrupted while they wait. See [Know What Happens to a Member](https://jaehyeon.me/desflow/latest/how-to/modelling/serve-entities-together/#know-what-happens-to-a-member).

## Failures

- **A task that ends in a run failure runs none of its `on:` actions,** as no statement runs after an uncaught exception in SimPy. That covers a `next` condition that cannot be evaluated, a wait that fails because its child did, and a function that raises, when no `on_fail` handles it. A task's own `fail` condition, and a failure that `on_fail` handles, keep their actions.
- **A failed evaluation in a task with `on_fail`.** A generator or a step that raises in a task with `on_fail` now goes to `on_fail`, where 0.28.0 stopped the run. Remove `on_fail` from the task when the run must stop.
- **A `fail` expression that cannot be evaluated,** such as `payload.a == 2` on a payload without `a`, now fails the task: it publishes `failed` with the error as its reason, and the run stops with `TaskFailed` under `stop` and goes on under `continue`. In 0.28.0 the error left the run with no `failed` record, even under `continue`.
- **A child flow that fails after its waiter was abandoned** no longer raises `TaskFailed` under `continue`; the run goes on. Under `stop` it is unchanged.
- **An empty or zero failure reason is kept.** A child flow's or a wait's failure with `fail_reason: ""` or 0 replaced the reason with the error's text, such as `signal 's' failed:`. In 1.0, `upstream.reason` reads the reason as given. Give the failing task a reason that is not empty, or read the empty reason in the handler.
- **`on_fail` without `fail`** is accepted in YAML and the Builder, and catches a task function that raises. Only `fail_reason` without `fail` is refused, as "fail_reason needs a fail condition".

## Values and Expressions

- **A mapping with a `gen` key is a generator.** In 0.28.0 such a mapping was data in `attributes`, store `items`, an interrupt `cause`, a `fire`, `on:` or timeout `value`, a `payload`, `set` and a scenario `value` and `record`. In 1.0 it is a generator, or a load error with the line. A key named `gen` directly under `set`, `attributes` or `record` is still a field name. To keep such a mapping as data, write `{gen: literal, value: {gen: 5}}`, which publishes `{"gen": 5}`. See [Generated Values](https://jaehyeon.me/desflow/latest/user-guide/writing-a-model/blueprints/#generated-values).
- **`item.<field>` where no `get` reaches** is refused at load, with the task named. In 0.28.0 it read nothing and failed when the run reached it.
- **A name that does not exist in `resume` or `open`.** A `lookup`, `aggregate` or `registry[...]` read that names a missing task, row or path in a `resume` or `open` value is refused at load. Correct the name.
- **Builder times.** `True` or `False` as a time, in `SimulationContext(initial_time=)`, `warmup`, `lag_interval`, a `drain` limit or a time-valued distribution parameter, was taken as 1 or 0 seconds. In 1.0 it raises `ValueError`, as a blueprint does. Write the number of seconds.
- **Builder draws in a `wait` or `open`.** A generator in a Builder `wait` or `open`, such as an `each` count or a timeout's `after`, now draws from the stream of its place in the blueprint, so the Builder gives the numbers of the same blueprint. At the same seed it gives other draws than 0.28.0. Compare the stored results of such a model again.
- **A sim id with a dot,** such as `Shop.v2`, draws the same random times as the same model with a plain sim id, so its results for a seed differ from 0.28.0. Record its results again.
- **An element named like a kind.** A live update of an element is stored by its kind alone. A resource named `service`, `containers` or `variables` has its capacity cast to a whole number, a container named `stores` keeps a fraction, and a whole-number variable named `max_cap` or `current_cap` refuses `inf`. The same holds for a sim id whose second part is a kind, such as `Shop.service`.

## Sinks and Connectors

- **A status record** (`queued`, `started` and each other status of a task) carries `sim_id` and `service` beside `path_id`. 0.28.0 wrote `path_id` alone. A consumer that checks the fields of a status value, or a table whose columns 0.28.0 records made, gets the two new fields. See [Records and Telemetry](https://jaehyeon.me/desflow/latest/reference/records-and-telemetry/index.md).
- **Routing keys in a payload.** A payload key `__topic__` (Kafka), `__table__` (Iceberg) or `__path__` (Parquet, JSON Lines) now chooses the destination, and the sink removes it. 0.28.0 dropped it with a warning. Rename such a key if it was only data. A `__table__` that PostgreSQL and Iceberg share on one record needs the form `namespace.table` for Iceberg, or a `table_router`. See [Reserved Payload Keys](https://jaehyeon.me/desflow/latest/reference/records-and-telemetry/#reserved-payload-keys).
- **A sink with `upsert_keys`** now writes the other records of a batch and skips only those that have none of the key columns, with one warning per flush that gives the counts. 0.28.0 lost the whole batch. Check the warning count after a run.
- **An Iceberg column that is an integer in the first batch** is no longer truncated when a later batch holds a fraction: that batch is logged at ERROR and lost. Name the column as a double in `schemas`, such as `schemas: {shop.events: {key: string, v: double}}`. See [Iceberg](https://jaehyeon.me/desflow/latest/user-guide/live-runs-and-connectors/egress/#iceberg).
- **A statistic and a reading need separate destinations.** A statistic's `value` is a number and a reading's is a mapping, so a `table_router` or `path_router` that sends telemetry to Iceberg or Parquet gives readings a table or path of their own.
- **`KafkaIngress` creates its topic** at the start of a run when it is missing. Give `topic_policy: manual` to keep it from creating one.
- **A Kafka record its serializer refuses** is skipped with a warning that names its key and the reason, and the rest of the batch is sent. 0.28.0 stopped the batch and retried the connection.
- **A PostgreSQL sink with `envelope` on** warns when it skips a record whose value is not a mapping. The reason in that warning, for every sink, now reads "which this sink cannot write as a row".
- **Parquet, JSON Lines and Iceberg sinks** load their libraries before the run starts. In a strict paced run without `logical_start_time`, the records are stamped behind the wall clock by the load time. A run with `strict: false` keeps its 0.28.0 timestamps.
- **A Parquet, JSON Lines or Iceberg sink without its extra** is refused when it is built, with an `ImportError` that names the extra. 0.28.0 built it, and the run wrote nothing.
- **`ConsoleEgress()` with no logging at INFO,** such as `python FILE` with no `logging.basicConfig`, prints one `UserWarning`. The records are the same.
- **`KafkaAdminConnector.get_state()`** keys a sim id and a service id that hold a dot by their whole names, such as `Shop.v2`, where it took one part of each.

## Builder and Low-level API

- **A second `run()` of one context** starts from the model's starting values, as a new context does, not from the values the first run changed. Pass other starting values to `start()` as `overrides`, or read the values a run ended with from `context.env.registry` before the next run.
- **`compile_parameters()`** gives the config objects as the model declares them, so a capacity or a stream name left out is `None`. Read the starting values a run registers from `registry_paths()`.
- **A task function that replaces `context.sampler`, `context.values` or `context.sim_id`** during a run no longer changes the arrivals, breakdowns and family members of that run. Set them before `start()`.
- **After a `start()` that fails before it makes the run's holders,** `get_resource`, `get_store` and `get_container` raise `KeyError`, where they gave the previous run's object.
- **`egress_*` lists of `DynamicRealtimeEnvironment`** are read-only: assigning one raises `AttributeError`, and changing a list it returns does not reach the egress. Pass the sinks to `setup_egress`.
- **`fields` of `add_arrival_replay` and `read_trace`** takes `"all"` or a list of columns, as a blueprint's `replay.fields` does. Write `fields=["id"]` for one column. See [Replay](https://jaehyeon.me/desflow/latest/user-guide/model-parts/arrivals-and-services/#replay).
- **A Builder `on=` entry,** in `task(on=)`, `task(listen=)` and `add_signal(on=)`, refuses a key other than the actions, `value` and `when`, where 0.28.0 ignored it. Remove the key, or move it into the action's own mapping.
- **An index on a number,** such as `users.machine[0]`, is refused when the Builder context starts (`run()` or `start()`), not at `task()`. A Low-level `Expression("users.machine[0]")` now parses.
- **`load_context(folder, "file.py", preview)`** applies the preview to the Builder file, where 0.28.0 raised `ValueError`.

## Command Line and Installs

- **desflow needs pydantic 2.6.1 or later,** where it accepted 2.6.0.
- **`desflow check FILE.py`** imports a Builder file and prints `ok FILE (sim_id ID)`, or the error. 0.28.0 exited 1. A Low-level file is refused.
- **`desflow check`** exits 1 for a blueprint whose `tests` entry has no seed in a file with no `simulation.random_seed`, and, in a run with no end, warns about each arrival with no `max_events`. The exit code of that warning is still 0. See [desflow check](https://jaehyeon.me/desflow/latest/cli/check/index.md).
- **`desflow run FILE.py --sample`** prints the Builder file's first events. Without `--sample`, or with `--stdout`, `--var` or `--watch`, it exits 2 as before.
- **`desflow fit`** writes its settings file in the blueprint layout: the comment wrapped at 88 characters, a blank line between keys and long lists wrapped. The file loads to the same settings.
- **`desflow fit` and `fit_column`** skip `poisson` and `geometric` named in `candidates`, as "counts whole numbers".
- **`desflow report` and `run_experiment`** build the report without reading `results.csv`, so a missing `results.csv` no longer stops the report.
- **An experiment's counts** are the task and flow counts that are 0 when no entity reached them; another whole-number statistic is no longer listed among them.

## Error Messages

A test that matches the text of an error matches the new text:

- The priority, patience and balk checks name `seize` and `open`: "needs a resource, a seize or a request in its wait or open".
- `on_balk` "needs a balk, a resource, a seize or an open", and the messages on `cause.<name>` and `upstream.<field>` also name `on_interrupt_run`, `on_renege`, `on_balk` and `on_fail`.
- An index on a number reads "'machine' is not a family; add family: true to its entry".
- `stats` with no `level`: "stats needs a level, unless it sets only trace, when the level is none".
- A `telemetry` entry with no `function`: "a telemetry entry takes exactly one of function and readings".
- An arrival: "an arrival takes exactly one of dist, replay or gap", and "an arrival with replay takes no distribution parameters".
- A task with a resource "needs a service or a service_time".
- A wait timeout: "a wait timeout is seconds, a duration such as '5 min' or an expression".
- The line of an error inside a wait is the line of the key, not of the `wait`.
- The warning of a sink that drops a reserved payload key names each routing key and its sink.
- A `Template` whose generator does not parse is refused when it is made, with the parse error.
