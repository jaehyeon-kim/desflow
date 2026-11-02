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

`current_cap` defaults to `max_cap`, and with neither the store has no limit; `.inf` is no limit too. `max_cap` defaults to `current_cap`. A capacity is a whole number or `.inf`. `items` are returned in the order given; a priority store's items are `[priority, item]` pairs. `type` is a `!python` subclass of `StoreBase`, which sets its own order, so it cannot be given with `priority`. An item is a constant or a `{gen: ...}` generator, drawn once when the run starts.

| Key | Type | Default |
|---|---|---|
| `current_cap` | integer or number | none |
| `max_cap` | integer or number | none |
| `priority` | boolean | `false` |
| `items` | list of Value |  |
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
| `p` | integer or number | none |

## arrivals

Type: mapping of names to [Arrival](#arrivals). Optional.

An `arrivals` entry: a distribution, or a `replay` of recorded rows, the task it spawns, and the attributes each entity it starts carries.

File order is creation order: arrivals are made in the order they are written, then the `processes`. Events due at one time run in the order they were scheduled, so this order decides only ties between first due times.

An `attributes` value is a constant or a `{gen: ...}` generator, drawn once for each entity when it starts, in the order the entities are made. A mapping with a `gen` key is a generator; `{gen: literal, value: ...}` keeps a mapping as data.

The gaps come from exactly one of `dist`, `replay` or `gap`. `gap` is a number of seconds, a duration such as `5 min`, an expression or a generator, read at the start of each gap, so a gap may read `registry[...]`, `level.<name>` and `t`. `first_at` is the simulation time of the first arrival, which waits for it in place of a gap; the gaps after it follow. A time already past arrives at once. `max_events` is how many times the arrival starts its task, in all: the arrival stops after the last, and its last start draws no further gap. A start whose key belongs to an entity of the arrival that is still running is skipped with a warning and counts as one.

`group` is how many entities one arrival starts, a count place read once at each arrival, after the gap: 0 or less starts nobody and the next gap still runs, and a dropped or empty arrival uses no key number. `initial` creates that many entities when the run is built, after the arrival's source starts and in file order, with the arrival's keys, and an arrival with `initial` alone needs no `dist`, `replay` or `gap`. `active: false` makes a source that starts off, and a task or an `on:` entry turns it on or off with `activate` and `deactivate`. With `off_mode: drop`, the default, the clock keeps running and an arrival while off starts nobody. With `restart` the pending arrival is dropped when the source goes off and `activate` starts a full new gap. An action on a source already in that state does nothing, and an arrival that falls at the instant of an action whose event comes first is dropped.

`count` makes that many entities, each with a loop of its own: it starts its task after each gap, `first_at` is the time of its first start, and `max_events` is how many times each entity starts. `keys` is a list of keys instead, one entity for each, and gives the number. `stagger` is the time between the starts of two entities, read after each one. `key` is a value place read once for each entity, for its key; `vars` are named values read once for each entity before its attributes and key, which read them as bare names. Every one of these places reads the run only: `attr` is refused in them, because the entity has none yet. `count` and `max_events` are keys of a blueprint, with no argument of `add_arrival`. `count`, `keys`, `first_at` and `stagger` do not apply to a replay. `count`, `keys`, `max_events` and `stagger` set how the arrival starts its task, so each needs `spawn`; `stagger` also needs `count` or `keys`. `key`, `vars`, `gap` and `first_at` are read by `wait_for_arrival` too, so an arrival without `spawn` may carry them.

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
| `p` | integer or number | none |
| `replay` | [Replay](#replay) | none |
| `gap` | Generator or integer or number or text or Duration or expression | none |
| `first_at` | integer or number or Duration | none |
| `count` | Generator or integer or text | none |
| `keys` | list of integer or text | none |
| `key` | Value | none |
| `vars` | mapping of names to Value |  |
| `stagger` | Generator or integer or number or text or Duration or expression | `0` |
| `max_events` | Generator or integer or text | none |
| `spawn` | text | none |
| `attributes` | mapping of names to Value |  |
| `group` | Generator or integer or text | `1` |
| `active` | boolean | `true` |
| `off_mode` | `drop` or `restart` | `"drop"` |
| `initial` | integer | `0` |

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

One entry of an `on:` list: exactly one of a fire, spawn or interrupt action, or a `path` with a `value` as a scenario step sets a registry value. A `value` is a constant or a `{gen: ...}` generator evaluated when the entry runs; a mapping with no `gen` key is data, and a generator nested in it is stored as written.

| Key | Type | Default |
|---|---|---|
| `fire` | [FireAction](#fireaction) | none |
| `spawn` | [SpawnAction](#spawnaction) | none |
| `interrupt` | [InterruptAction](#interruptaction) | none |
| `activate` | text or list of text | none |
| `deactivate` | text or list of text | none |
| `path` | text | none |
| `value` | Value | none |

### FireAction

An `on:` fire action: fire `signal` with a `value` (a constant, or a `{gen: ...}` generator evaluated when the action runs; a mapping with no `gen` key is data, and a generator nested in it is stored as written), or with the outcome `from` copies from a signal or a task. `urgent` and `after` fire a one-shot signal ahead of the other events at its time, or later, as SimPy's `env.schedule(event, priority, delay)` does.

| Key | Type | Default |
|---|---|---|
| `signal` | text | required |
| `value` | Value | none |
| `from` | text | none |
| `urgent` | boolean | `false` |
| `after` | integer or number or Duration | none |

### SpawnAction

A `spawn:` key or an `on:` spawn action: start `task` as a new entity, after an optional delay. `count` is how many entities one spawn starts: a whole number, an expression or a generator, read once in the parent's step (0 or less starts none, as `range` gives none). `copy_attributes: true` gives each child a copy of the parent's attributes, one level deep, so a nested mapping stays shared with the parent; `parent` is the parent's key as always. `args` gives each child its local names, read there as `arg.<name>`; each value is read in the parent's step, once for each child. `after` applies to each child.

| Key | Type | Default |
|---|---|---|
| `task` | text | required |
| `after` | integer or number or Duration | none |
| `count` | Generator or integer or text | `1` |
| `copy_attributes` | boolean | `false` |
| `args` | mapping of names to Value | none |

### InterruptAction

An `interrupt` on a task or a scenario step: interrupt every running instance of `task` whose `where` condition holds, with `cause`, a constant or a `{gen: ...}` generator evaluated at the moment of the interrupt.

| Key | Type | Default |
|---|---|---|
| `task` | text | required |
| `where` | text | none |
| `cause` | Value | none |

## tasks

Type: mapping of names to [Task](#tasks). Optional.

A `tasks` entry, built with `SimulationContext.task`.

Text in a value place stays text. A time place reads a number as seconds, text `time_to_seconds` accepts as a duration, and other text as an expression.

`payload` is the value of the `finished` event: a mapping, or a `!python` function called as `payload(task_id, context)`. `payload: null` publishes no `finished` record and the task runs its other steps; a store `put` on such a task is refused, because the put carries the payload. `id_field` adds the task id to a mapping payload under that key, `key_field` names the payload field whose value becomes the key of the finished event, and so the Kafka message key (the `queued` and `started` records keep the entity's key; a payload without the field fails the run), `item_field` adds the item its store `get` took, and `attributes` adds the named entity attributes. `key` is a value place for the key of the task's records, drawn once for each instance at its first record and written as text; it replaces `key_field`, and the two are not given together. A task with a `service` and no `resource` is a timed step; one with neither, and no `get`, emits its payload as soon as it is spawned.

`service_time` is the task's own time, in place of a `service`, and the two are not given together: seconds, a duration such as `30 s`, an expression, or a `{gen: ...}` generator, read after the task has its resource and, for a `get`, its item. It may read `vars_once` and `vars` as bare names, and `item.<field>` after a store get. A result below 0 fails the run. A task with a `resource` needs a `service` or a `service_time`.

`priority` is a number or an expression such as `attr.priority`, for a resource whose `queue` is `priority`. `patience` is seconds, a duration such as `30 s`, an expression or a generator; `balk` is a condition checked before the task joins the queue. `resource` is one resource or a list, acquired in the order listed; `keep` hands units to the next task. `on_interrupt` is `resume`, `restart` or `abandon`, and `interrupt` interrupts the running instances of a task when this one finishes. `wait` is what the task waits for first: a signal, a child task, a timeout or a condition, or any or all of several. `signal` names a signal the task fires when it finishes. `fail` is a condition checked after the service: when it holds the task publishes `failed` with `fail_reason`, and goes to `on_fail`. `get` and `put` take from and add to a store or container. `next` is a task name, `when` rules or `weights`, whose values are numbers or expressions read when the task finishes, with one draw for the choice. `set` gives the entity attribute values when the task finishes. `set_args` writes this process's own local names, read as `arg.<name>` by this task and by the tasks that follow in the same flow (not by a child flow, which has its own). It is read after `set`, so a payload reads the value from before the write. A parent passes names to a child with `args` on a `task` branch of its `wait`, or on a `spawn`; a read of a name that was neither passed nor written is a run failure. `activate` and `deactivate` turn the sources of the named arrivals on and off when the task finishes, after `set_args`. `cause.<name>` reads why the task was interrupted, in a task that an `on_interrupt_next` edge reaches: `by`, `usage_since` and `resource` after a preemption, `value` after an `interrupt`, and `left`, the service time that was left. `upstream.<field>` is the payload of the task that routed here, `item` is the whole item a store get took, and it lasts through the `next` chain.

The cause of an interrupt, the holders of a resource and the entries of a queue are lists and mappings an expression reads: `held(m)` and `waiting(m)` for a resource, `putting(s)` and `getting(s)` for a store or container, `running(task)` and `running(task, condition)`, `peek()`, `process.key` and `process.flow_name`, `signal.<name>.processed`, and in the `when` of a wait group `done_count`.

A value place takes a constant, a list, a mapping or a `{gen: ...}` generator: a mapping with a `gen` key is a generator, and `{gen: literal, value: ...}` keeps a mapping as data. The kinds of `gen` are `uniform`, `integer`, `normal`, `sample`, `one_of`, `weighted`, `pick`, `switch` (key `on`), `when`, `boolean`, `uuid`, `sequence`, `now`, `format_time`, `format`, `lower`, `upper`, `fake`, `add`, `subtract`, `multiply`, `divide`, `min`, `max`, `expr`, `var`, `attr`, `entity`, `param`, `repeat`, `literal`, `object`, `call`, `row`, `lookup`, `previous`, `upstream` and `aggregate`; `null_rate`, `unique`, `omit_rate` and `omit_when` go on any of them. `payload` may be such a mapping, or one generator that gives a mapping; `set` values and the `cause` of an interrupt are evaluated when the task finishes or interrupts. `vars_once` names are drawn once for each entity and task, at the first use, and may be read by a `wait` timeout `value`. `vars` are drawn in order, once for each instance of the task: when it starts if `on_start` is given, otherwise after the service, just before the payload; a `service_time` that reads a var draws it first. `{gen: entity}` gives the entity's key, in a task only. A var reads the vars above it with `{gen: var, name: ...}` or as a bare name in an `expr`. `on_start` values are published as one event record when the task starts. `finish_vars` are drawn after the service and may read the vars. A bare var name also reads in `put`, `fail` and `next.when`, but not in `next.weights`, which read the entity and the run. A var name is an identifier, not a name an expression already uses, and is in one of the three groups only. Every generator node draws from its own stream of the run's seed, so adding a node does not change the draws of the others.

A task keeps its last `history` finished payloads (default 1,000,000) when a `lookup` or `aggregate` in the file names it, or when `history` is written. A task that looks up another does not start before the other has a row: an arrival's start is dropped, and one INFO line says how long the task waited in simulation time. A task reached by `next` that has no row to look up ends its flow in the same way. `previous` reads the last finished payload of the same task on the same stream: the arrival, or the entity for an arrival with `count` or `keys`. `upstream` reads the payload of the task that routed here, in a task reached by `next`. `aggregate` is `mean`, `sum`, `min`, `max` or `count` over the last rows of a task that `where`, an expression over the row's fields, keeps.

An expression uses Python's arithmetic: `/` gives a float, and `//` rounds down and gives an integer for two integers. `and` and `or` stop at the first operand that decides the result and do not absorb an error: an operand that cannot be evaluated fails the expression. `**` raises to a power (an integer power above 10000 is refused), `abs`, `min`, `max`, `round`, `floor`, `ceil`, `sqrt`, `exp`, `log`, `log10`, `sin`, `cos` and `tan` are calls on numbers, and `has(attr.x)` is true when the entity has the attribute `x`.

An expression also reads a process's local names `arg.<name>`, the interrupt that reached the task as `cause.<name>`, the payload of the task that routed here as `upstream.<field>` (at any depth), `process.key` and `process.flow_name`, the bare `item` and `item_priority` after a store get, and `signal.<name>.processed`. `held(<resource>)`, `waiting(<resource>)`, `putting(<store or container>)` and `getting(<store or container>)` give lists of mappings (with `entity`, and `rank`, `time`, `preempt`, `key` and `since` for a resource, `item` for a store put and `amount` for a container); `running(<task>[, <condition>])` counts the instances of a task, and `peek()` is the time of the next event. Lists `[1, 2]`, mappings `{'a': 1}`, one-clause comprehensions `[x * 2 for x in list if x > 1]`, subscripts and slices, `is None`, `in`, and dotted reads at any depth work on them. The functions `len`, `sum`, `sorted`, `range`, `any`, `all`, `str`, `int`, `float`, `bool`, `get_key(mapping, key, default)`, `set_key(mapping, key, value)` and `drop_key(mapping, key)` work on lists and mappings; `min` and `max` take a list. `set_key` and `drop_key` give a new mapping. A reading makes at most 1,000,000 items, and `%` on text is refused.

| Key | Type | Default |
|---|---|---|
| `service` | text | none |
| `service_time` | Generator or integer or number or text or Duration or expression | none |
| `resource` | text or list of text | none |
| `payload` | Payload or !python reference | required |
| `id_field` | text | none |
| `key` | Value | none |
| `key_field` | text | none |
| `item_field` | text | none |
| `attributes` | list of text |  |
| `priority` | integer or number or text | none |
| `patience` | Generator or integer or number or text or Duration or expression | none |
| `balk` | text | none |
| `get` | [TransferStep](#transferstep) | none |
| `put` | [TransferStep](#transferstep) | none |
| `next` | text or [Next](#next) | none |
| `set` | mapping of names to Value |  |
| `set_args` | mapping of names to Value |  |
| `activate` | text or list of text | none |
| `deactivate` | text or list of text | none |
| `vars` | mapping of names to Value |  |
| `vars_once` | mapping of names to Value |  |
| `on_start` | mapping of names to Value |  |
| `finish_vars` | mapping of names to Value |  |
| `history` | integer | `1000000` |
| `wait` | [SignalWait](#signalwait) or [TaskWait](#taskwait) or [TimeoutWait](#timeoutwait) or [ConditionWait](#conditionwait) or [AnyWait](#anywait) or [AllWait](#allwait) or [AtLeastWait](#atleastwait) or [WhenWait](#whenwait) or [RequestWait](#requestwait) or [GetWait](#getwait) or [PutWait](#putwait) or [RunningWait](#runningwait) | none |
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

`amount` is a number or an expression such as `payload.quantity`, and 1 when it is left out. `priority` is for a put into a priority store, lowest served first, and 0 when it is left out. Text is an expression such as `attr.priority`; any other value is used as written: a number, a list (which orders element by element, as a tuple does), a mapping, null, a date or a bool. `filter` is a condition for a store get, such as `item.size == attr.size`, where `item.<field>` reads the item offered: the get takes the oldest item that matches. `wait: false` makes the get or put and does not wait for it: the task goes on at once. `item` is for a store `put`: the value to put, a constant, text, a list, a mapping or a `{gen: ...}` generator read as the put is made; left out, the put adds the task's payload. The bare name `item` in `{gen: expr, expr: item}` is the whole item a get took, so `item: {gen: expr, expr: item}` puts it back.

| Key | Type | Default |
|---|---|---|
| `container` | text | none |
| `store` | text | none |
| `amount` | integer or number or text | `1.0` |
| `priority` | None | `0.0` |
| `filter` | text | none |
| `wait` | boolean | `true` |
| `item` | Value | none |

### Next

A task's `next`: `when` rules with an optional `default`, or `weights`.

| Key | Type | Default |
|---|---|---|
| `when` | list of [Rule](#rule) |  |
| `default` | text | none |
| `weights` | mapping of names to number or text | none |

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

A `wait` branch for a child flow: it starts `task` for the same entity. `args` are value places read in the parent's scope when the child starts, and the child flow reads them as `arg.<name>`; each child has its own, as a SimPy process has its own arguments.

| Key | Type | Default |
|---|---|---|
| `task` | text | required |
| `args` | mapping of names to Value | none |

### TimeoutWait

A `wait` branch of a time place: a number of seconds, a duration such as `30 s`, an expression, a `{gen: ...}` generator, or `{after, value}` with a value the waiter receives. An expression or a generator is read when the wait starts.

| Key | Type | Default |
|---|---|---|
| `timeout` | Generator or integer or number or text or [TimeoutAfter](#timeoutafter) | required |

### TimeoutAfter

A `wait` timeout with a value: `{after: 40 s, value: ...}`, as SimPy's `env.timeout(delay, value)` carries a value the waiter receives. The value is a constant or a `{gen: ...}` generator evaluated when the wait starts, so it may read `vars_once` names but no `vars`. `after` is a time place: seconds, a duration, an expression or a generator, read when the wait starts.

| Key | Type | Default |
|---|---|---|
| `after` | Generator or integer or number or text | required |
| `value` | Value | none |

### ConditionWait

A `wait` branch for a condition, checked again after each change.

| Key | Type | Default |
|---|---|---|
| `condition` | text | required |

### AnyWait

A `wait` done when the first of its named branches is.

| Key | Type | Default |
|---|---|---|
| `any` | mapping of names to [SignalWait](#signalwait) or [TaskWait](#taskwait) or [TimeoutWait](#timeoutwait) or [ConditionWait](#conditionwait) or [AnyWait](#anywait) or [AllWait](#allwait) or [AtLeastWait](#atleastwait) or [WhenWait](#whenwait) or [RequestWait](#requestwait) or [GetWait](#getwait) or [PutWait](#putwait) or [RunningWait](#runningwait) | required |

### AllWait

A `wait` done when every one of its named branches is.

| Key | Type | Default |
|---|---|---|
| `all` | mapping of names to [SignalWait](#signalwait) or [TaskWait](#taskwait) or [TimeoutWait](#timeoutwait) or [ConditionWait](#conditionwait) or [AnyWait](#anywait) or [AllWait](#allwait) or [AtLeastWait](#atleastwait) or [WhenWait](#whenwait) or [RequestWait](#requestwait) or [GetWait](#getwait) or [PutWait](#putwait) or [RunningWait](#runningwait) | required |

### AtLeastWait

A `wait` done when at least `at_least` of its named branches are.

| Key | Type | Default |
|---|---|---|
| `at_least` | integer | required |
| `of` | mapping of names to [SignalWait](#signalwait) or [TaskWait](#taskwait) or [TimeoutWait](#timeoutwait) or [ConditionWait](#conditionwait) or [AnyWait](#anywait) or [AllWait](#allwait) or [AtLeastWait](#atleastwait) or [WhenWait](#whenwait) or [RequestWait](#requestwait) or [GetWait](#getwait) or [PutWait](#putwait) or [RunningWait](#runningwait) | required |

### WhenWait

A `wait` done when the expression `when` holds. It is read each time a branch of `of` is processed, and once at the start for a branch already processed, as SimPy's `Condition(env, evaluate, events)` does. It reads `wait.<branch>.done`, `.ok` and `.value` of the branches and `done_count`. A branch that fails fails the wait, and an empty `of` succeeds at once.

| Key | Type | Default |
|---|---|---|
| `when` | text | required |
| `of` | mapping of names to [SignalWait](#signalwait) or [TaskWait](#taskwait) or [TimeoutWait](#timeoutwait) or [ConditionWait](#conditionwait) or [AnyWait](#anywait) or [AllWait](#allwait) or [AtLeastWait](#atleastwait) or [WhenWait](#whenwait) or [RequestWait](#requestwait) or [GetWait](#getwait) or [PutWait](#putwait) or [RunningWait](#runningwait) | required |

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

`at` is seconds from the start of the run, or a duration such as `"10 min"`. A step with a `record` publishes a ground-truth record, `stream_type: label`, with the path, the value before and after, and the `record` mapping, to the sinks with `when: labels` only. A step with an `interrupt` interrupts the running instances of a task, as a task's `interrupt` does. A `value` and each `record` value are constants or `{gen: ...}` generators: `value` is evaluated just before the change and `record` just after it.

| Key | Type | Default |
|---|---|---|
| `at` | integer or number or Duration | required |
| `path` | text | none |
| `value` | Value | none |
| `record` | mapping of names to Value | none |
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

## Generators

A value place takes a constant or a generator: a mapping with a `gen` key that names its kind. The keys of each kind follow. `null_rate`, `unique`, `omit_rate` and `omit_when` go on any kind.

### Add

`add`: the sum of `args`. When the first is a time (the text `{gen: now}` gives), the rest are seconds and the result is a time.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `args` | list of Value | required |
| `decimals` | integer | none |
| `gen` | text | required |

### Aggregate

`aggregate`: `mean`, `sum`, `min`, `max` or `count` over the last `last` rows of the task `task` that `where` keeps. `where` is an expression over the row's own fields, such as `lead_days == 3 and precipitation > 0`; without it every row counts. `count` counts rows and takes no `path`; the others read the number at `path`. A reading looks back through the rows the task keeps (its `history`) until `last` rows are kept, and takes the rows there are when `where` keeps fewer.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `task` | text | required |
| `path` | text | none |
| `last` | integer | required |
| `function` | `mean` or `sum` or `min` or `max` or `count` | required |
| `where` | text | none |

### Attr

`attr`: the entity's attribute `name`, or the part of it at `path`.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `name` | text | required |
| `path` | text | none |

### Boolean

`boolean`: true with probability `p` (0.5 when not given).

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `p` | Value | `0.5` |

### Call

`call`: what a `!python` function returns, called as `function(context, **kwargs)` with the kwargs evaluated first.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `function` | !python reference | required |
| `kwargs` | mapping of names to Value |  |

### Divide

`divide`: the first of `args` divided by each of the others in turn (`/`).

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `args` | list of Value | required |
| `decimals` | integer | none |
| `gen` | text | required |

### Entity

`entity`: the key of the entity the task runs for, such as `sensor-1`. It is given only inside a task, so an arrival's `attributes`, `vars` and `key` refuse it.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |

### Expr

`expr`: the value of an expression, with the names of the language, the vars of the task and the entries of `names` (evaluated first) as bare names, and `t` for the simulation time. It can give any type; a number is rounded to `decimals`.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `expr` | text | required |
| `names` | mapping of names to Value |  |
| `decimals` | integer | none |

### Fake

`fake`: `getattr(faker, method)(*args, **kwargs)` for a Faker of `locale` (`en_US` by default). It needs the `fake` extra. Each `fake` generator has a Faker of its own, seeded from the run's seed, so adding one changes no other. A date, a time or a decimal result is written as text or a float.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `method` | text | required |
| `args` | list of Value |  |
| `kwargs` | mapping of names to Value |  |
| `locale` | text | `"en_US"` |

### Format

`format`: `template.format_map(...)` over `names`, then the vars of the task. A name in the template is a var, or an entry of `names`, which wins.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `template` | text | required |
| `names` | mapping of names to Value |  |

### FormatTime

`format_time`: a time as text or a number. `time` is the text `{gen: now}` gives, or seconds of simulation time (default: now). `format` is a `strftime` pattern, `iso`, `iso_utc` (milliseconds and a `Z`), `epoch_s` or `epoch_ms` (integers). `tz` is an IANA name and defaults to the run's time zone.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `time` | Value | none |
| `format` | text | `"iso"` |
| `tz` | text | none |

### Integer

`integer`: a whole number between `bounds[0]` and `bounds[1]`, both included.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `bounds` | list of Value | required |

### Literal_

`literal`: `value` as written, not evaluated, so a mapping that has a `gen` key can be a payload field.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `value` | None | none |

### Lookup

`lookup`: a row of the finished payloads of the task `task`, or the part of it at `path` (dotted; an integer part indexes a list).

A row is readable from the instant the task's finished record is published. The task keeps its last `history` rows (default 1,000,000).

`strategy` is `random` (the default), `first` (the oldest row kept) or `last` (the newest). `histogram` splits the rows kept, in the order they were written, into bins of the stated shares; a draw picks a bin by its frequency and then a row of the bin at random, so it is for `random` only. A bin that holds no row yet is left out of the draw, and while no bin with a frequency above 0 holds a row (a few rows and a last bin of frequency 0), the draw is over all the rows. `latest_by` keeps only the newest row for each value of that field of the row, so a lookup sees a customer's latest address; `strategy` and `histogram` then choose among those rows, oldest first by the time each was written. `distinct` draws without replacement within one `repeat`, so the items of one order differ.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `task` | text | required |
| `path` | text | none |
| `strategy` | `random` or `first` or `last` | `"random"` |
| `histogram` | list of [HistogramBin](#histogrambin) | none |
| `latest_by` | text | none |
| `distinct` | boolean | `false` |

### HistogramBin

One bin of a `lookup` histogram: `bin` is the share of the rows it covers, in the order they were written, and `frequency` is the chance that a draw falls in it. Both are scaled to add up to 1, so `{bin: 2, frequency: 8}, {bin: 8, frequency: 2}` means the same as 0.2 with 0.8, and 0.8 with 0.2.

| Key | Type | Default |
|---|---|---|
| `bin` | number | required |
| `frequency` | number | required |

### Lower

`lower`: the text of `value` in lower case.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `value` | Value | required |

### Max

`max`: the largest of `args`.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `args` | list of Value | required |
| `decimals` | integer | none |
| `gen` | text | required |

### Min

`min`: the smallest of `args`.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `args` | list of Value | required |
| `decimals` | integer | none |
| `gen` | text | required |

### Multiply

`multiply`: the product of `args`.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `args` | list of Value | required |
| `decimals` | integer | none |
| `gen` | text | required |

### Normal

`normal`: a float from a normal distribution with `mean` and `std`.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `mean` | Value | required |
| `std` | Value | required |
| `decimals` | integer | none |

### Now

`now`: the logical time plus `offset` (seconds or a duration, signed), as the ISO text a record's `timestamp` uses. It reads the simulation clock, never the wall clock.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `offset` | Value | `0` |

### Object

`object`: the mapping `fields`, each evaluated as a plain mapping is. It lets a modifier such as `null_rate` or `omit_rate` apply to the whole mapping.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `fields` | mapping of names to Value | required |

### OneOf

`one_of`: one of `choices`, each equally likely. Only the choice picked is evaluated.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `choices` | list of Value | required |

### Param

`param`: the live registry value at `path` (a full registry path, such as `Shop.variables.error_rate`) at the moment of the call, or the `field` key of a mapping value. A scenario step or an ingress update shows in the next call.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `path` | text | required |
| `field` | Value | none |

### Pick

`pick`: a list of `count` different `choices`, drawn without replacement, in the order drawn. Only the choices picked are evaluated.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `choices` | list of Value | required |
| `count` | Value | `1` |

### Previous

`previous`: the last finished payload of the same task for the same stream, or the part of it at `path`. The stream is the arrival, so all the entities it starts share one, except for an arrival with `count` or `keys`, where each entity has its own. `default` is evaluated only when the stream has no finished payload yet, and without one the first reading is null.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `path` | text | none |
| `default` | Value | none |

### Repeat

`repeat`: a list of `times` values, each evaluated on its own.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `times` | Value | required |
| `value` | Value | required |

### Row

`row`: one row of a CSV or Parquet file as a mapping, read once at load. A cell of digits is read as a number, so a postal code with leading zeros loses them; write them with `{gen: format, template: "{n:05d}"}`. `where` is an expression over the row's columns that keeps some rows; `weight` names a column of weights; `order` is `random` (the default), `sequential`, `shuffle` (every row once, in a random order) or `circular` (in file order, again from the first); `on_end` says what a `sequential` or `shuffle` order does when the rows run out: `error` (the default), `loop` or `stop`, which ends the run.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `file` | text | required |
| `where` | text | none |
| `weight` | text | none |
| `order` | `random` or `sequential` or `shuffle` or `circular` | `"random"` |
| `on_end` | `stop` or `loop` or `error` | `"error"` |

### Sample

`sample`: one draw from a distribution family with its parameters, or from the live distribution of a service.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `dist` | text | none |
| `service` | text | none |
| `rate` | Value | none |
| `mean` | Value | none |
| `std` | Value | none |
| `low` | Value | none |
| `value` | Value | none |
| `high` | Value | none |
| `mode` | Value | none |
| `shape` | Value | none |
| `scale` | Value | none |
| `p` | Value | none |
| `values` | list of integer or number or text | none |
| `weights` | list of number | none |
| `edges` | list of integer or number or text | none |
| `counts` | list of number | none |
| `decimals` | integer | none |

### Sequence

`sequence`: the next integer of a counter, `start` plus `step` times the count of calls so far, restarting after `loop_after` calls. With `format`, the text `format.format(n)`, such as `"sensor-{}"`. There is one counter for the run, shared by every entity, so ids stay unique.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `start` | integer | `0` |
| `step` | integer | `1` |
| `loop_after` | integer | none |
| `format` | text | none |

### Subtract

`subtract`: the first of `args` less the others. When the first is a time, the rest are seconds and the result is a time.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `args` | list of Value | required |
| `decimals` | integer | none |
| `gen` | text | required |

### Switch

`switch`: the value of the first case that matches `on`, evaluated only when picked. `cases` is a mapping from a value to its result, or a list of ranges; `default` is used when none matches (null when it is left out).

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `on` | Value | required |
| `cases` | mapping of names to Value or list of [SwitchRange](#switchrange) | required |
| `default` | Value | none |

### SwitchRange

One range case of `switch`: the first case whose `min` is at or below `on` and whose `max`, when given, is above it.

| Key | Type | Default |
|---|---|---|
| `min` | integer or number | none |
| `max` | integer or number | none |
| `value` | Value | required |

### Uniform

`uniform`: a float in [low, high), from `low + (high - low) * random()`; `decimals` rounds it, and 0 gives an integer.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `low` | Value | required |
| `high` | Value | required |
| `decimals` | integer | none |

### Upper

`upper`: the text of `value` in upper case.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `value` | Value | required |

### Upstream

`upstream`: the finished payload of the task that routed to this one with `next`, or the part of it at `path`. A start by an arrival has no upstream, and a task that `on_fail` or `on_interrupt_next` reaches is handed a failure or an interrupted record, so a blueprint refuses `upstream` there.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `path` | text | none |

### Uuid

`uuid`: a version 4 UUID from the generator's random sequence, so it repeats with the seed, or a version 7 UUID whose 48-bit millisecond field comes from the logical time.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `version` | `4` or `7` | `4` |

### Var

`var`: a var of the task, or the part of it at `path` (dotted; an integer part indexes a list). Every reference to a var in one event gives the same value.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `name` | text | required |
| `path` | text | none |

### Weighted

`weighted`: one choice, by weight. `options` maps each value to its weight, or `choices` lists `{value, weight}` for values that are mappings or generators. Each weight is 0 or more and at least one is above 0; weights are divided by their sum. Only the value picked is evaluated.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `options` | mapping of names to Value | none |
| `choices` | list of [WeightedChoice](#weightedchoice) | none |

### WeightedChoice

One entry of `weighted` `choices`: a value and its weight.

| Key | Type | Default |
|---|---|---|
| `value` | Value | required |
| `weight` | Value | required |

### When

`when`: `then` when the `condition` (an expression) holds, else `else`. Only the branch taken is evaluated.

| Key | Type | Default |
|---|---|---|
| `null_rate` | number | none |
| `unique` | boolean | `false` |
| `omit_rate` | number | none |
| `omit_when` | text | none |
| `gen` | text | required |
| `condition` | text | required |
| `then` | Value | required |
| `else` | Value | none |
