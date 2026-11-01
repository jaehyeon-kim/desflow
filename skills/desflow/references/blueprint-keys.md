# Blueprint keys

Every key a desflow blueprint takes, from the JSON Schema in [../assets/blueprint.schema.json](../assets/blueprint.schema.json). Where a value is a number, a whole number, `true` or `false`, or one of a fixed set of names, a `${VAR}` or `${VAR:-default}` reference is also accepted, and a Duration is a number and a unit such as `5 min`.

## simulation

Type: mapping. Required.

The `simulation` section: the arguments of `SimulationContext`.

`logical_start_time` and `go_live_at` take `now`, a signed duration from now such as `-1d` or `-10m`, an ISO datetime, or a `!python` datetime. Both read the same `now`, the moment the file is validated, so `-10m` and `now` are exactly ten minutes apart. `now` and a time without an offset are read in `timezone`, an IANA name, UTC by default.

`strict` makes a step raise `RuntimeError` when the run is more than `factor` seconds behind the wall clock, as SimPy's real-time environment does. Left unset it is true for a paced run or one with `go_live_at`, and false at `factor: 0` with no `go_live_at`. At `factor: 0`, `strict: true` raises once the wall clock has moved, as SimPy's does. `environment` is a subclass of `DynamicRealtimeEnvironment` the run is built on.

| Key | Type | Default |
|---|---|---|
| `sim_id` | text | required |
| `factor` | number | `1.0` |
| `random_seed` | integer | none |
| `timezone` | text | none |
| `logical_start_time` | Time | none |
| `go_live_at` | Time | none |
| `warmup` | integer or number or Duration | none |
| `initial_time` | integer or number or Duration | `0.0` |
| `lag_interval` | number or Duration | none |
| `strict` | boolean | none |
| `environment` | !python reference | none |

## resources

Type: mapping of names to [Resource](#resources). Optional.

A `resources` entry: its capacity, the order its queue is served in, whether a request of a lower priority value preempts a user, how many requests may wait, the class it is built from, and how it breaks down.

`current_cap` defaults to 1 and `max_cap` to `current_cap`, so `{}` is a resource of one unit. `.inf` makes a resource with no limit. `type` is a `!python` subclass of `ResourceBase`.

| Key | Type | Default |
|---|---|---|
| `current_cap` | integer or number | none |
| `max_cap` | integer or number | none |
| `queue` | text | `"fifo"` |
| `preempt` | boolean | `false` |
| `queue_limit` | integer | none |
| `type` | !python reference | none |
| `breakdown` | [Breakdown](#breakdown) | none |

### Breakdown

A resource's `breakdown`: the time to each failure, the repair time, and whether the tasks being served are interrupted or finish.

| Key | Type | Default |
|---|---|---|
| `time_to_failure` | [Distribution](#services) | required |
| `repair` | [Distribution](#services) | required |
| `work` | `interrupt` or `finish` | `"interrupt"` |

## containers

Type: mapping of names to [Container](#containers). Optional.

A `containers` entry: its capacity, the largest level, its starting level and the class it is built from.

`current_cap` defaults to `max_cap`, and with neither the container has no limit; `.inf` is no limit too. `max_cap` defaults to `current_cap`. `type` is a `!python` subclass of `ContainerBase`.

| Key | Type | Default |
|---|---|---|
| `current_cap` | integer or number | none |
| `max_cap` | integer or number | none |
| `level` | integer or number | `0` |
| `type` | !python reference | none |

## stores

Type: mapping of names to [Store](#stores). Optional.

A `stores` entry: how many items it holds, whether it serves by priority, the items it holds when the run starts, and the class it is built from.

`current_cap` defaults to `max_cap`, and with neither the store has no limit; `.inf` is no limit too. `max_cap` defaults to `current_cap`. A capacity is a whole number or `.inf`. `items` are returned in the order given; a priority store's items are `[priority, item]` pairs. `type` is a `!python` subclass of `StoreBase`, which sets its own order, so it cannot be given with `priority`.

| Key | Type | Default |
|---|---|---|
| `current_cap` | integer or number | none |
| `max_cap` | integer or number | none |
| `priority` | boolean | `false` |
| `items` | list of any value |  |
| `type` | !python reference | none |

## services

Type: mapping of names to [Distribution](#services). Optional.

A `services` entry: a distribution family and its parameters.

Time-valued parameters (`mean`, `std`, `low`, `mode`, `high`, `scale`, `value`, and an `empirical`'s `values` and `edges`) take seconds or a duration such as `5 min`; `rate` is a number per second.

| Key | Type | Default |
|---|---|---|
| `dist` | text | required |
| `rate` | integer or number | none |
| `mean` | integer or number or Duration | none |
| `std` | integer or number or Duration | none |
| `low` | integer or number or Duration | none |
| `value` | integer or number or Duration | none |
| `high` | integer or number or Duration | none |
| `mode` | integer or number or Duration | none |
| `shape` | integer or number | none |
| `scale` | integer or number or Duration | none |
| `values` | list of integer or number or Duration | none |
| `weights` | list of number | none |
| `edges` | list of number or Duration | none |
| `counts` | list of number | none |

## arrivals

Type: mapping of names to [Arrival](#arrivals). Optional.

An `arrivals` entry: a distribution, or a `replay` of recorded rows, the task it spawns, and the attributes each entity it starts carries.

File order is creation order: arrivals are made in the order they are written, then the `processes`. Events due at one time run in the order they were scheduled, so this order decides only ties between first due times.

| Key | Type | Default |
|---|---|---|
| `dist` | text | none |
| `rate` | integer or number | none |
| `mean` | integer or number or Duration | none |
| `std` | integer or number or Duration | none |
| `low` | integer or number or Duration | none |
| `value` | integer or number or Duration | none |
| `high` | integer or number or Duration | none |
| `mode` | integer or number or Duration | none |
| `shape` | integer or number | none |
| `scale` | integer or number or Duration | none |
| `values` | list of integer or number or Duration | none |
| `weights` | list of number | none |
| `edges` | list of number or Duration | none |
| `counts` | list of number | none |
| `replay` | [Replay](#replay) | none |
| `spawn` | text | none |
| `attributes` | mapping |  |

### Replay

An arrival's `replay`: the rows of a local file, each one arrival at its time.

`path` is a CSV or Parquet file, relative to the blueprint; `time` its column of times; `unit` `ms`, `s`, `min` or `h` from the start of the run, or `timestamp` for a date and time read from `simulation.logical_start_time`; `fields` the columns each entity carries, or `all`; `on_invalid` `error` or `skip` for a row with no valid time.

| Key | Type | Default |
|---|---|---|
| `path` | text | required |
| `time` | text | `"time"` |
| `unit` | `ms` or `s` or `min` or `h` or `timestamp` | `"s"` |
| `fields` | text or list of text | `"all"` |
| `on_invalid` | `error` or `skip` | `"error"` |

## variables

Type: mapping. Optional.

## signals

Type: mapping of names to [SignalSpec](#signals) or list of names. Optional.

A `signals` entry in the mapping form: `{closing: {once: true, on: [...]}}`.

A signal fires once, as SimPy's `Event` does, unless it says `once: false`, which re-arms it after each firing. `on` lists actions run when the signal fires, as a task's `on` lists actions run when it ends.

| Key | Type | Default |
|---|---|---|
| `once` | boolean | `true` |
| `on` | list of [OnEntry](#onentry) |  |

### OnEntry

One entry of an `on:` list: exactly one of a fire, spawn or interrupt action, or a `path` with a `value` as a scenario step sets a registry value.

| Key | Type | Default |
|---|---|---|
| `fire` | [FireAction](#fireaction) | none |
| `spawn` | [SpawnAction](#spawnaction) | none |
| `interrupt` | [InterruptAction](#interruptaction) | none |
| `path` | text | none |
| `value` | None | none |

### FireAction

An `on:` fire action: fire `signal` with a literal `value`, or with the outcome `from` copies from a signal or a task. `urgent` and `after` fire a one-shot signal ahead of the other events at its time, or later, as SimPy's `env.schedule(event, priority, delay)` does.

| Key | Type | Default |
|---|---|---|
| `signal` | text | required |
| `value` | None | none |
| `from` | text | none |
| `urgent` | boolean | `false` |
| `after` | integer or number or Duration | none |

### SpawnAction

A `spawn:` key or an `on:` spawn action: start `task` as a new entity, after an optional delay.

| Key | Type | Default |
|---|---|---|
| `task` | text | required |
| `after` | integer or number or Duration | none |

### InterruptAction

An `interrupt` on a task or a scenario step: interrupt every running instance of `task` whose `where` condition holds, with `cause`.

| Key | Type | Default |
|---|---|---|
| `task` | text | required |
| `where` | text | none |
| `cause` | None | none |

## tasks

Type: mapping of names to [Task](#tasks). Optional.

A `tasks` entry, built with `SimulationContext.task`.

Text in a value place stays text. A time place reads a number as seconds, text `time_to_seconds` accepts as a duration, and other text as an expression.

`payload` is the value of the `finished` event: a mapping, or a `!python` function called as `payload(task_id, context)`. `id_field` adds the task id to a mapping payload under that key, `key_field` names the payload field whose value becomes the key of the finished event, and so the Kafka message key (the `queued` and `started` records keep the entity's key; a payload without the field fails the run), `item_field` adds the item its store `get` took, and `attributes` adds the named entity attributes. A task with a `service` and no `resource` is a timed step; one with neither, and no `get`, emits its payload as soon as it is spawned.

`priority` is a number or an expression such as `attr.priority`, for a resource whose `queue` is `priority`. `patience` is seconds, a duration such as `30 s` or an expression; `balk` is a condition checked before the task joins the queue. `resource` is one resource or a list, acquired in the order listed; `keep` hands units to the next task. `on_interrupt` is `resume`, `restart` or `abandon`, and `interrupt` interrupts the running instances of a task when this one finishes. `wait` is what the task waits for first: a signal, a child task, a timeout or a condition, or any or all of several. `signal` names a signal the task fires when it finishes. `fail` is a condition checked after the service: when it holds the task publishes `failed` with `fail_reason`, and goes to `on_fail`. `get` and `put` take from and add to a store or container. `next` is a task name, `when` rules or `weights`. `set` gives the entity attribute values when the task finishes.

An expression uses Python's arithmetic: `/` gives a float, and `//` rounds down and gives an integer for two integers. `and` and `or` stop at the first operand that decides the result and do not absorb an error: an operand that cannot be evaluated fails the expression.

| Key | Type | Default |
|---|---|---|
| `service` | text | none |
| `resource` | text or list of text | none |
| `payload` | mapping or !python reference | required |
| `id_field` | text | none |
| `key_field` | text | none |
| `item_field` | text | none |
| `attributes` | list of text |  |
| `priority` | integer or number or text | none |
| `patience` | integer or number or text or Duration or expression | none |
| `balk` | text | none |
| `get` | [TransferStep](#transferstep) | none |
| `put` | [TransferStep](#transferstep) | none |
| `next` | text or [Next](#next) | none |
| `set` | mapping |  |
| `wait` | [SignalWait](#signalwait) or [TaskWait](#taskwait) or [TimeoutWait](#timeoutwait) or [ConditionWait](#conditionwait) or [AnyWait](#anywait) or [AllWait](#allwait) or [AtLeastWait](#atleastwait) or [RequestWait](#requestwait) or [GetWait](#getwait) or [PutWait](#putwait) or [RunningWait](#runningwait) | none |
| `signal` | text | none |
| `fail` | text | none |
| `fail_reason` | text | none |
| `on_fail` | text | none |
| `keep` | list of text |  |
| `preempt` | boolean | `true` |
| `on_interrupt` | `resume` or `restart` or `abandon` | `"resume"` |
| `on_interrupt_next` | text | none |
| `interrupt` | [InterruptAction](#interruptaction) | none |
| `spawn` | [SpawnAction](#spawnaction) | none |
| `subscribe` | mapping | none |
| `on` | list of [OnEntry](#onentry) |  |

### TransferStep

A task's `get` or `put`: one `container` with an `amount`, or one `store`.

`amount` is a number or an expression such as `payload.quantity`, and 1 when it is left out. `priority` is for a put into a priority store, lowest served first, and 0 when it is left out. Text is an expression such as `attr.priority`; any other value is used as written: a number, a list (which orders element by element, as a tuple does), a mapping, null, a date or a bool. `filter` is a condition for a store get, such as `item.size == attr.size`, where `item.<field>` reads the item offered: the get takes the oldest item that matches. `wait: false` makes the get or put and does not wait for it: the task goes on at once.

| Key | Type | Default |
|---|---|---|
| `container` | text | none |
| `store` | text | none |
| `amount` | integer or number or text | `1.0` |
| `priority` | None | `0.0` |
| `filter` | text | none |
| `wait` | boolean | `true` |

### Next

A task's `next`: `when` rules with an optional `default`, or `weights`.

| Key | Type | Default |
|---|---|---|
| `when` | list of [Rule](#rule) |  |
| `default` | text | none |
| `weights` | mapping of names to number | none |

### Rule

A `next.when` entry: a condition and the task it leads to.

| Key | Type | Default |
|---|---|---|
| `condition` | text | required |
| `task` | text | required |

### SignalWait

A `wait` branch for a signal's next firing.

| Key | Type | Default |
|---|---|---|
| `signal` | text | required |

### TaskWait

A `wait` branch for a child flow: it starts `task` for the same entity.

| Key | Type | Default |
|---|---|---|
| `task` | text | required |

### TimeoutWait

A `wait` branch of a number of seconds, a duration such as `30 s`, or `{after, value}` with a value the waiter receives.

| Key | Type | Default |
|---|---|---|
| `timeout` | integer or number or text or [TimeoutAfter](#timeoutafter) | required |

### TimeoutAfter

A `wait` timeout with a value: `{after: 40 s, value: ...}`, as SimPy's `env.timeout(delay, value)` carries a value the waiter receives.

| Key | Type | Default |
|---|---|---|
| `after` | integer or number or text | required |
| `value` | None | none |

### ConditionWait

A `wait` branch for a condition, checked again after each change.

| Key | Type | Default |
|---|---|---|
| `condition` | text | required |

### AnyWait

A `wait` done when the first of its named branches is.

| Key | Type | Default |
|---|---|---|
| `any` | mapping of names to [SignalWait](#signalwait) or [TaskWait](#taskwait) or [TimeoutWait](#timeoutwait) or [ConditionWait](#conditionwait) or [AnyWait](#anywait) or [AllWait](#allwait) or [AtLeastWait](#atleastwait) or [RequestWait](#requestwait) or [GetWait](#getwait) or [PutWait](#putwait) or [RunningWait](#runningwait) | required |

### AllWait

A `wait` done when every one of its named branches is.

| Key | Type | Default |
|---|---|---|
| `all` | mapping of names to [SignalWait](#signalwait) or [TaskWait](#taskwait) or [TimeoutWait](#timeoutwait) or [ConditionWait](#conditionwait) or [AnyWait](#anywait) or [AllWait](#allwait) or [AtLeastWait](#atleastwait) or [RequestWait](#requestwait) or [GetWait](#getwait) or [PutWait](#putwait) or [RunningWait](#runningwait) | required |

### AtLeastWait

A `wait` done when at least `at_least` of its named branches are.

| Key | Type | Default |
|---|---|---|
| `at_least` | integer | required |
| `of` | mapping of names to [SignalWait](#signalwait) or [TaskWait](#taskwait) or [TimeoutWait](#timeoutwait) or [ConditionWait](#conditionwait) or [AnyWait](#anywait) or [AllWait](#allwait) or [AtLeastWait](#atleastwait) or [RequestWait](#requestwait) or [GetWait](#getwait) or [PutWait](#putwait) or [RunningWait](#runningwait) | required |

### RequestWait

A `wait` branch for a resource unit: the task holds it if it wins.

| Key | Type | Default |
|---|---|---|
| `request` | text | required |

### GetWait

A `wait` branch for a store or container get.

| Key | Type | Default |
|---|---|---|
| `get` | [TransferStep](#transferstep) | required |

### PutWait

A `wait` branch for a store or container put.

| Key | Type | Default |
|---|---|---|
| `put` | [TransferStep](#transferstep) | required |

### RunningWait

A `wait` branch for the running instances of a task: done when they all end.

| Key | Type | Default |
|---|---|---|
| `running` | [RunningTarget](#runningtarget) | required |

### RunningTarget

A `running` wait branch target: the task whose running instances to wait for, narrowed by an optional `where` condition read in each instance's scope.

| Key | Type | Default |
|---|---|---|
| `task` | text | required |
| `where` | text | none |

## telemetry

Type: list of [Telemetry](#telemetry). Optional.

A `telemetry` entry: custom readings published every `interval` seconds.

`function` is a `!python` function called as `function(context)`, which publishes with `context.publish`. Resource, store and container readings come from `stats` instead.

| Key | Type | Default |
|---|---|---|
| `interval` | number | required |
| `function` | !python reference | required |

## processes

Type: list of [Process](#processes) or !python reference. Optional.

A `processes` entry: a generator function and extra keyword arguments.

The function is called as `function(context, **kwargs)` when the run starts. The processes are made after the arrivals, in the order they are written.

| Key | Type | Default |
|---|---|---|
| `function` | !python reference | required |
| `kwargs` | mapping |  |

## scenario

Type: list of [ScenarioStep](#scenario). Optional.

A `scenario` entry: at simulation time `at`, set `path` to `value`, publish a ground-truth record of `record`, or both.

`at` is seconds from the start of the run, or a duration such as `"10 min"`. A step with a `record` publishes a ground-truth record, `stream_type: label`, with the path, the value before and after, and the `record` mapping, to the sinks with `when: labels` only. A step with an `interrupt` interrupts the running instances of a task, as a task's `interrupt` does.

| Key | Type | Default |
|---|---|---|
| `at` | integer or number or Duration | required |
| `path` | text | none |
| `value` | None | none |
| `record` | mapping | none |
| `interrupt` | [InterruptAction](#interruptaction) | none |

## ingress

Type: list of [Connector](#ingress). Optional.

An `ingress` entry: a connector type and its constructor arguments.

`type` is a short name such as `Kafka`, or a `!python` class for a connector from another package.

| Key | Type | Default |
|---|---|---|
| `type` | short name or !python reference | required |
| `config` | mapping |  |

## egress

Type: list of [EgressConnector](#egress). Optional.

An `egress` entry, with the per-provider options of `add_egress`.

`when` is `history`, `live`, `events`, `telemetry`, `labels`, a mapping `{expr: ...}` or a `!python` function taking one record. `history` sends the records stamped before `simulation.go_live_at`, and `live` sends the records stamped at or after it. `events` sends the event records and `telemetry` the telemetry records. `labels` sends the ground-truth records of scenario steps with a `record`, and nothing else.

`envelope: false` makes the entry plain output: the sink writes each event's value without the record around it (`stream_type`, `sim_ts`, `timestamp`, `key`, `task` and `instance`), and it receives no telemetry record and no `queued` or `started` record of a task. A Kafka or Redis sink sends a value that is not a mapping as JSON. A storage or PostgreSQL sink skips it, with one warning. The message key of Kafka stays the record's key, and a `topic_router` still receives the whole record. Ground-truth records of a `when: labels` entry are not affected.

| Key | Type | Default |
|---|---|---|
| `type` | short name or !python reference | required |
| `config` | mapping |  |
| `when` | `history` or `live` or `events` or `telemetry` or `labels` or [WhenExpr](#whenexpr) or !python reference | none |
| `envelope` | boolean | `true` |
| `batch_size` | integer | none |
| `flush_interval` | number | none |

### WhenExpr

An egress `when` written as an expression: `when: {expr: <expression>}`.

The expression reads the record the sink is offered: `record.stream_type` (`event` or `telemetry`), `record.key`, `record.task` and `record.value.<field>`. A name the record lacks reads as None, so `record.value.event_type == 'order_packed'` is false for a telemetry record and for an event without that field. The sink receives the record when the expression is true. An expression that fails on a record, such as `record.value.amount > 100` on a record whose value has no `amount`, fails the run.

| Key | Type | Default |
|---|---|---|
| `expr` | text | required |

## batching

Type: [Batching](#batching). Optional.

The `batching` section: the arguments of `with_batching`.

| Key | Type | Default |
|---|---|---|
| `batch_size` | integer | required |
| `flush_interval` | number | required |
| `max_queued_batches` | integer | none |
| `drain_stall_seconds` | number | none |

## stats

Type: [Stats](#stats) or level name. Optional.

The `stats` section: the built-in statistics level and how often to publish.

Written as a level alone (`stats: basic`) or as a mapping (`stats: {level: all, interval: 60}`). The interval is in seconds or a duration such as `5 min`.

| Key | Type | Default |
|---|---|---|
| `level` | `none` or `minimal` or `basic` or `typical` or `all` | required |
| `interval` | integer or number or Duration | `10.0` |

## run

Type: mapping. Optional.

The `run` section.

`before` lists `!python` functions called with no arguments before the run starts, for setup such as creating topics or tables. `stop` ends the run before `until` at the next firing of a signal, `{signal: name}`, or when a condition first holds, `{condition: ...}`. `drain`, `true` or `{limit: 1 h}`, stops the arrivals at `until` and runs on until no entity is left in the model.

`segments` runs the model in parts and publishes a reading after each, as a SimPy program that calls `env.run(until=...)` in a loop does: a list of `RunSegment`, or `{every: <time>}` for equal parts up to `until`. `steps: true` publishes `system.simulation.step`, `{step, now, peek}`, after each model event, as a loop of `env.step()` and `env.peek()` reads them. `sync: true` calls `env.sync()` as the run starts. A run with no `until` ends when no model event is left.

| Key | Type | Default |
|---|---|---|
| `until` | integer or number or Duration | none |
| `before` | list of !python reference |  |
| `stop` | mapping of names to text | none |
| `drain` | boolean or [Drain](#drain) | `false` |
| `on_unhandled_failure` | `stop` or `continue` | `"stop"` |
| `segments` | list of [RunSegment](#runsegment) or [SegmentEvery](#segmentevery) | none |
| `steps` | boolean | `false` |
| `sync` | boolean | `false` |

### Drain

A `run.drain` mapping: the longest a drain may run after `until`.

| Key | Type | Default |
|---|---|---|
| `limit` | number or Duration | none |

### RunSegment

A `run.segments` entry: the run goes on to `until` or to `stop`, then waits for the next entry.

An entry has at most one of `until` and `stop`; `{}` runs to the end. After the segment stops, `look` names a task whose new entity reads the model before any event at the stop time runs, and `sync` re-anchors the wall clock at the start of the segment. A record at `system.simulation.segment` reports `index`, `now` and `peek` (null when no model event is left) as the segment ends.

| Key | Type | Default |
|---|---|---|
| `until` | integer or number or text or Duration | none |
| `stop` | mapping of names to text | none |
| `look` | text | none |
| `sync` | boolean | `false` |

### SegmentEvery

`run.segments: {every: 10 s}`: a segment every `every` seconds up to `run.until`.

| Key | Type | Default |
|---|---|---|
| `every` | integer or number or text or Duration | required |

## tests

Type: list of [BlueprintTest](#tests). Optional.

A `tests` entry: the statistics a model must give at one seed, checked by `desflow test`.

`expect` maps a statistic name, as `measures` in an experiment file names it (`tasks.pack_order.finished`), to the number it must have, or to `{value, within}` for a tolerance. A number is compared exactly. `seed` defaults to the file's `simulation.random_seed` and `until` to `run.until`. The entry runs the model once, unpaced, with no ingress and no egress, so a test contacts no service.

A test states the measures at one seed in one version of desflow. A new release may draw its random numbers differently, so a test does not promise that exact seeded records or values stay the same across versions; run `desflow test --fix` to write the new values and read them before you keep them.

| Key | Type | Default |
|---|---|---|
| `name` | text | required |
| `seed` | integer | none |
| `until` | integer or number or text or Duration | none |
| `expect` | mapping of names to [ExpectedWithin](#expectedwithin) or integer or number |  |

### ExpectedWithin

An `expect` value with a tolerance: the measured statistic passes when it is within `within` of `value`, in absolute terms. Use it for a value whose last digits change with the platform's floating point.

| Key | Type | Default |
|---|---|---|
| `value` | integer or number | required |
| `within` | integer or number | required |
