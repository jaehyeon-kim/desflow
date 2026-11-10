# Signals and Waiting

A [task](https://jaehyeon.me/desflow/latest/user-guide/tasks-and-processes/tasks/index.md) can wait for signals, conditions and child flows, start other flows and run actions when an event happens. This page shows each.

The tutorial page [Process Interaction](https://jaehyeon.me/desflow/latest/learn/shop-logic/process-interaction/index.md) teaches these features step by step.

______________________________________________________________________

## What It Is

In a shop, work often depends on other work. An order waits until its parts are picked, a courier leaves once an order is packed, and a guard is told when an alarm round ends. desflow models this with four tools: a wait that a task makes before it asks for its resource, signals that tasks fire, flows that a task starts without waiting for them, and actions that run when an event happens.

______________________________________________________________________

## How to Use It

### Wait, Signals and Child Flows

A task can wait for something before it queues for its resource: its `wait`. A wait has one branch, or a group of several named branches: `any`, `all` or `at_least`. A branch can itself be a group, so waits nest. An empty `any` or `all` succeeds at once. In SimPy, `AnyOf` waits for the first event and `AllOf` for every event, and a `Condition` with a count rule does what `at_least` does. [Wait for One or All](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/index.md) shows the groups. A leaf branch is one of:

| Branch                                                          | Waits for                                                                                                                                                                                                                                                                                                                                                                                                                                 | Its value                                                                                                 |
| --------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `{signal: name}`                                                | the next firing of a signal declared under `signals` (builder `add_signal`)                                                                                                                                                                                                                                                                                                                                                               | the firing's value, the payload of the task that fired it                                                 |
| `{task: name}`                                                  | a **child flow**: that task, started for the same entity with its own instance id, and the tasks its `next` leads to, until the flow ends                                                                                                                                                                                                                                                                                                 | the last task's finished payload                                                                          |
| `{timeout: 40 s}`, or `{timeout: {after: 40 s, value: ...}}`    | a time; `value` is carried to the waiter, as SimPy's `env.timeout(delay, value)`                                                                                                                                                                                                                                                                                                                                                          | `None`, or the `value` given                                                                              |
| `{condition: level.stock >= 5}`                                 | a condition to hold, checked when the wait starts and again after every store or container change, change in a queue that the condition reads, registry update, signal firing and finished task                                                                                                                                                                                                                                           | `True`                                                                                                    |
| `{request: name}`                                               | a unit of a resource; a won unit is held through the service and given back when the task ends                                                                                                                                                                                                                                                                                                                                            | the request                                                                                               |
| `{get: {store: name}}` or `{get: {container: name, amount: 2}}` | an item or an amount, as a task `get` does                                                                                                                                                                                                                                                                                                                                                                                                | the item taken                                                                                            |
| `{put: {store: name}}` or `{put: {container: name, amount: 2}}` | room to add the payload of the task before it, or an amount                                                                                                                                                                                                                                                                                                                                                                               | none                                                                                                      |
| `{running: {task: name, where: ...}}`                           | every running instance of a task, on the waiter's entity and on the entities it spawned, to end, which is the end of that instance and not of its flow's process; `where` narrows them, read in each instance's scope; with `whole_flow: true` it is the end of the whole flow the instance belongs to, as `yield process` waits for a SimPy process, and the value is the flow's last payload; when none match, the wait is done at once | the one instance's finished payload, or a mapping from instance id to payload for several, in start order |

A `running` branch waits for tasks already running. Here an order waits for the delivery it started, unless the delivery has already left with the van:

```yaml
# Wait until every flow of the deliver task has ended, unless it has left with the
# van
wait:
  running: {task: deliver, where: process.target != 'service', whole_flow: true}
```

`@app.task(wait=...)` takes the same mapping as a dict: `wait={"running": {"task": "deliver", "where": "process.target != 'service'"}}`.

A low-level process waits for another by yielding it, as SimPy's `yield process` does.

A single branch is named after its signal, task or resource, or `timeout`, `condition`, `get` or `put`. A `put` branch draws its `item` as a task's `put` does, and `payload: null` together with a store `put` in a `wait` or an `open` is refused when the blueprint loads, because there is no payload for the put to carry. After the wait, each branch has three values:

- `wait.<branch>.done` says whether the branch happened before the wait ended.
- `wait.<branch>.ok` says whether it succeeded.
- `wait.<branch>.value` (or `.value.<field>`) is its value.

`next`, `fail` and amounts read these values, and a builder function that declares `wait` receives them as a mapping.

A group can also end on an expression: `wait: {when: <expression>, of: {...}}` is done when the expression holds. It reads `wait.<branch>.done`, `.ok` and `.value` of each branch, and `done_count`, the number of branches done. Like `any`, a `when` group cancels the requests that did not win. [Wait for One or All](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/index.md) shows the groups with their ties and failures.

A task fires a signal with `signal: name` when it finishes, with its payload as the value. Any condition reads the signal as `signal.<name>.count`, `.ok` and `.value`. The `process-interaction` example waits for a child flow, then for the first of a courier's signal and a time limit:

```yaml
check:
  # Starts the child flow inspect and waits for it to end.
  wait: {task: inspect}
  payload: {event_type: order_checked}
  next: dispatch
inspect:
  service: inspecting
  payload: {event_type: inspected}
  # A gift fails inspection; on_fail sends it to wrap, and the flow goes on.
  fail: attr.gift == 1
  fail_reason: gift needs wrapping
  on_fail: wrap
wrap:
  service: wrapping
  payload: {event_type: wrapped}
```

```yaml
dispatch:
  # The courier's departure, or 40 seconds, whichever comes first.
  wait:
    any:
      courier: {signal: courier_left}
      limit: {timeout: 40 s}
  payload: {event_type: dispatched}
  next:
    when:
      - {condition: wait.courier.done, task: by_courier}
    default: by_post
```

```yaml
courier:
  payload: {event_type: courier_left}
  # Fires courier_left with this payload; every order waiting for it resumes.
  signal: courier_left
```

A child flow can take values from its parent. `args` are read in the parent's scope when the child starts, and the child reads each as `arg.<name>`:

```yaml
# Start a child flow of label_box for this entity, with the box number as arg.box
wait:
  any:
    child:
      task: label_box
      args: {box: 7}
    limit: {timeout: 30 s}
```

```python
# Starts the child flow inspect and waits for it to end
@app.task(wait={"task": "inspect"}, next="dispatch")
def check(task_id, context):
    return {"event_type": "order_checked"}


# A gift fails inspection; on_fail sends it to wrap, and the flow goes on
@app.task(
    service_id="inspecting",
    fail="attr.gift == 1",
    fail_reason="gift needs wrapping",
    on_fail="wrap",
)
def inspect(task_id, context):
    return {"event_type": "inspected"}


@app.task(service_id="wrapping")
def wrap(task_id, context):
    return {"event_type": "wrapped"}
```

```python
# The courier's departure, or 40 seconds, whichever comes first
@app.task(
    wait={"any": {"courier": {"signal": "courier_left"}, "limit": {"timeout": 40}}},
    next={
        "when": [{"condition": "wait.courier.done", "task": "by_courier"}],
        "default": "by_post",
    },
)
def dispatch(task_id, context):
    return {"event_type": "dispatched"}
```

```python
# Fires courier_left with this payload; every order waiting for it resumes
@app.task(signal="courier_left")
def courier(task_id, context):
    return {"event_type": "courier_left"}
```

`context.signal(name)` returns the `Signal`, which a process can fire, fail or wait for. `context.call(task, entity=...)` starts a child flow from a process and returns its SimPy process, which a process can yield. `context.spawn` returns the process it starts too, so `is_alive`, `triggered`, `ok` and `value` inspect it, as in SimPy.

```python
def check(shop, key, arrival):
    """Starts the child process inspect and waits for it to end."""
    env, stats, task_id = shop.env, shop.env.stats, shop.next_id("check")
    shop.publish(key, "check", task_id, {"status": "queued"})
    stats.task_queued("check", task_id)
    yield env.process(inspect(shop, key, arrival))
    shop.publish(key, "check", task_id, {"status": "started"})
    stats.task_started("check", task_id)
    shop.publish(key, "check", task_id, {"event_type": "order_checked"})
    stats.task_finished("check", task_id)
    yield from dispatch(shop, key, arrival)


def inspect(shop, key, arrival):
    """A gift fails inspection and is wrapped instead; returns the last payload."""
    env, stats, task_id = shop.env, shop.env.stats, shop.next_id("inspect")
    path_id = "Shop.service.inspecting"
    shop.publish(
        key,
        "inspect",
        task_id,
        {
            "path_id": path_id,
            "sim_id": "Shop",
            "service": "inspecting",
            "status": "started",
        },
    )
    stats.task_started("inspect", task_id)
    yield env.timeout(shop.service("inspecting")[1])
    if GIFT[arrival] == 1:
        failed = {
            "path_id": path_id,
            "sim_id": "Shop",
            "service": "inspecting",
            "status": "failed",
            "reason": "gift needs wrapping",
        }
        shop.publish(key, "inspect", task_id, failed)
        stats.task_failed("inspect", task_id)
        return (yield env.process(wrap(shop, key)))
    payload = {"event_type": "inspected"}
    shop.publish(key, "inspect", task_id, payload)
    stats.task_finished("inspect", task_id)
    return payload


def wrap(shop, key):
    env, stats, task_id = shop.env, shop.env.stats, shop.next_id("wrap")
    path_id = "Shop.service.wrapping"
    shop.publish(
        key,
        "wrap",
        task_id,
        {
            "path_id": path_id,
            "sim_id": "Shop",
            "service": "wrapping",
            "status": "started",
        },
    )
    stats.task_started("wrap", task_id)
    yield env.timeout(shop.service("wrapping")[1])
    payload = {"event_type": "wrapped"}
    shop.publish(key, "wrap", task_id, payload)
    stats.task_finished("wrap", task_id)
    return payload
```

```python
def dispatch(shop, key, arrival):
    """The courier's departure, or 40 seconds, whichever comes first."""
    env, stats, task_id = shop.env, shop.env.stats, shop.next_id("dispatch")
    shop.publish(key, "dispatch", task_id, {"status": "queued"})
    stats.task_queued("dispatch", task_id)
    courier = shop.courier_left.wait()
    done = yield env.any_of([courier, env.timeout(40)])
    shop.publish(key, "dispatch", task_id, {"status": "started"})
    stats.task_started("dispatch", task_id)
    shop.publish(key, "dispatch", task_id, {"event_type": "dispatched"})
    stats.task_finished("dispatch", task_id)
    task = "by_courier" if courier in done else "by_post"
    yield from send(shop, key, arrival, task)
```

```python
def courier(shop, key):
    """Fires courier_left; every order waiting for it resumes."""
    task_id = shop.next_id("courier")
    shop.env.stats.flow_started("couriers")
    payload = {"event_type": "courier_left"}
    shop.publish(key, "courier", task_id, payload)
    shop.env.stats.task_finished("courier", task_id)
    shop.courier_left.fire(payload)
    shop.env.stats.flow_finished("couriers")
    yield shop.env.timeout(0)
```

The low-level API is plain SimPy here: a child is `env.process(...)`, its value is what it returns, and `env.any_of` and `env.all_of` wait for the first or for every event. `Signal(env, name)` is the named event the builder and YAML use. `env.watch_changes(check)` calls `check` after every change a condition can read. `env.watch_changes(check, queues=True)` also calls it after a change in the number of puts or gets waiting at a store or container.

### One-Shot Signals

A signal is one-shot by default, as SimPy's `Event` is. It fires once, so a task that waits for it after it has fired resumes at once, and a second firing is refused. A closing-time signal is a one-shot: `signals: {closing: {}}`. `once` is `true` or `false`, and the builder refuses anything else. A signal that fires more than once is written `once: false` (builder `add_signal(name, once=False)`, low-level `Signal(env, name, once=False)`). It re-arms after each firing, so a waiter that starts later waits for the next. The courier signal of the example above is one that re-arms.

### Park a Task Until Another Resumes It

A task that waits with `suspend: true` parks until another task wakes it. The waking task writes `resume: {task, where, value, limit}`: it wakes the parked instances of `task` that `where` selects, oldest first, at most `limit` of them (`limit: 0` wakes nobody). `value` is read once for each `resume`, and a woken instance reads it as `wait.suspend.value`. `suspend` stands alone or in a group, and not in `open`. A `limit` below 0 is refused when the blueprint loads. An interrupt of a parked instance follows its `on_interrupt`. Three orders park, and a truck wakes the two oldest:

```yaml
simulation: {sim_id: Shop, factor: 0}

arrivals:
  orders: {dist: constant, value: 1, max_events: 3, spawn: hold}
  trucks: {dist: constant, value: 5, first_at: 5, max_events: 1, spawn: unload}

tasks:
  # Parks until a resume wakes it; wait.suspend.value is the value of that resume
  hold:
    wait: {suspend: true}
    payload:
      event_type: released
      by: {gen: expr, expr: wait.suspend.value}
  # Wakes the two oldest parked orders; the third stays parked
  unload:
    resume: {task: hold, limit: 2, value: truck-1}
    payload: {event_type: unloaded}

run: {until: 10}
```

`@app.task(wait={"suspend": True})` parks a task and `@app.task(resume={"task": "hold", "limit": 2, "value": "truck-1"})` wakes it. [Wait for One or All](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#park-a-task-until-another-resumes-it) runs both.

A parked process yields an event of its own, and the waking process calls `succeed(value)` on it, as SimPy code does.

### Wait for a Computed Number of Branches

`wait: {each: ..., group: ..., branch: ...}` makes the same `branch` once for each element of `each`, when the wait starts. `each` is a whole number, a list, an expression written as text, or any generator, and is read once. Inside `branch`, `each.index` counts from 0, and `each.value` is the element, or the index when `each` is a number. `group` is `all`, the default, `any`, `{at_least: <expression>}` or `{when: <expression>}`. The results are `wait.<name>.values` and `wait.<name>.flags`, lists in the order of the elements.

Here an order packs three boxes of 2, 5 and 9 seconds, and goes on when two are packed:

```yaml
simulation: {sim_id: Shop, factor: 0}

arrivals:
  orders: {initial: 1, spawn: order}

tasks:
  order:
    wait:
      all:
        boxes:
          each: [2, 5, 9]  # one branch for each element
          group: {at_least: 2}  # done when two branches are
          branch:  # each.value is the element
            timeout: {after: each.value}
    payload:
      event_type: packed
      flags: {gen: expr, expr: wait.boxes.flags}  # which branches were done

run: {until: 30}
```

The group of an `each` can also be a `when` expression, which reads only `done_count`:

```yaml
# Pack three boxes and go on when two are done; the group reads only done_count
wait:
  all:
    boxes:
      each: [2, 5, 9]
      group: {when: done_count >= 2}
      branch:
        timeout: {after: each.value}
```

`@app.task(wait={"each": [2, 5, 9], ...})` takes the same mapping. [Wait for One or All](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/#wait-for-a-computed-number-of-branches) shows it in the three forms.

A Low-level process makes the timeouts itself and waits on a `Condition` with the count it wants, as that page shows.

These rules hold for `each`:

- The `branch` is one leaf: a signal, task, timeout, request, get, put or handle. A group as the branch is refused.
- A literal `each: 2.0` is refused, and an `each` that computes 2.0 fails the run.
- An empty `each` succeeds at once under every group.
- The `when` of an `each` group reads only `done_count`. A reference to `wait.<name>`, its `values` or its `flags` is refused when the file loads. Read the flags in the payload or the task function after the wait.
- `each.index` and `each.value` are read only in the `branch`. A read in a `when` group, an `at_least` count, an `on` entry or a `listen` is refused when the file loads.
- A request of an `each` that is still queued is cancelled when the task ends.
- `subscribe` refuses an `each` branch.
- After a `resume` the wait keeps every leaf's event and reads no reference or index again. After a `restart` it reads `each` again. A granted request is kept for the leaf of the same name that asks for the same resource, and any other kept grant is given back and its leaf asks again.

### Name an Event and Cancel It

`open` starts wait branches at once without waiting for them, and names each, so a later step can wait for it (`wait: {handle: name}`), cancel it (`cancel: name`), release its unit (`release: {handle: name}`) or interrupt it. A handle's state is read as `handle.<name>.triggered`, `.processed`, `.ok`, `.value` and `.alive`. This order asks for three packers at once, packs with the first it gets, and cancels the other two requests:

```yaml
simulation: {sim_id: Shop, factor: 0}

resources:
  packers: {current_cap: 1}

arrivals:
  orders: {initial: 1, spawn: order}
  late: {dist: constant, value: 1, first_at: 1, max_events: 1, spawn: late_order}

tasks:
  order:
    open:
      # three requests, made at once
      pool:
        each: 3
        branch: {request: packers}
    wait: {handle: pool, index: 0}  # the first of them
    service_time: 2
    cancel: pool  # the requests not yet granted: the second and the third
    release: {handle: pool, index: 0}  # the unit of the first, when the task ends
    payload:
      event_type: packed
      granted: {gen: expr, expr: handle.pool.flags}

  late_order:
    resource: packers
    service_time: 1
    payload: {event_type: late_packed}

run: {until: 30}
```

A handle names one element of an `each` as `{handle: name, index: <expression>}`. `listen`, `interrupt`, `cancel` and `release` take that form. Here a task opens three child flows and two requests for a van. It hears the second child flow end, stops the third, gives up the second request and gives back the first:

```yaml
# Open three child flows and two requests for a van, without waiting for them
open:
  boxes:
    each: 3
    branch:
      task: label_box
      args:
        box: {gen: expr, expr: each.index}
  seats:
    each: 2
    branch: {request: vans}
# Hear the end of the second child flow
listen:
  handle: boxes
  index: 1
  on:
    - fire: {signal: second_done}
service_time: 1 s
# Stop the third child flow, give up the second request, and give back the first
interrupt: {handle: boxes, index: 2, cause: stop}
cancel: {handle: seats, index: 1}
release: {handle: seats, index: 0}
```

`@app.task(open={...}, wait={"handle": "pool", "index": 0}, cancel="pool")` takes the same mappings. [Wait for One or All](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/index.md) shows the pool in the three forms.

The requests are SimPy requests held in a list: `requests = [packers.request() for _ in range(3)]`, then `yield requests[0]` and `request.cancel()` on the others.

### Pick a Member of a Family

A resource, store, container or signal with `family: true` is a family: each member is an ordinary one named after the family and its key in brackets, such as `desk[2]`. `members` makes that many when the run starts, and a member not made yet is made at its first use. A name place takes `{of: <family>, key: <expression>}` for the member with that key, or `{of: <family>, lowest: <expression>}` for the member whose expression is lowest, read with `member.key`, `member.users`, `member.queue`, `member.level`, `member.count`, `member.putters` and `member.getters`. This works in `resource`, `seize`, `release`, a `request` branch, `get`, `put`, `signal`, `subscribe.signal`, `fire.signal` and `run.stop.signal`. A reference is not allowed in `keep`. The desks of a shop are a family, and each customer has a reply signal:

```yaml
serve:
  # The member of the family with the fewest users and waiting requests
  resource: {of: desk, lowest: "member.users + member.queue"}
  service: serving
  payload: {event_type: served}
  next: thank
```

```yaml
thank:
  # Fires this customer's own signal, and no other customer's
  on:
    - fire:
        signal: {of: reply, key: attr.number}
        value: thanks
  payload: {event_type: thanked}
```

The same mappings go to `resource=`, `wait=`, `on=` and the other arguments. `add_resource("desk", family=True, members=3)` and `add_signal("reply", family=True, once=True)` declare the families.

A family is a dictionary of ordinary objects that your code keeps, such as `desks[number % 3]`.

A member is read in a condition as `users.desk[k]`, `queue.desk[k]`, `level.c[k]`, `putters.c[k]`, `getters.c[k]`, `signal.reply[k].count`, `held(desk[k])` and `waiting(desk[k])`. `members(desk)` lists the members made so far. A read of a member not made yet gives what a new member gives, and makes nothing. [Units and Queues](https://jaehyeon.me/desflow/latest/user-guide/resources/units-and-queues/#make-a-family-of-resources) describes the capacity of a member.

### Start a Flow Without Waiting

A task's `spawn` starts another task as a **new entity**, keyed `<task>-n`, and goes on at once, as SimPy's `env.process` starts a process without yielding it. A courier that leaves once an order is packed is a spawn: `spawn: {task: courier}` on the pack task, or `@app.task(spawn={"task": "courier"})`. A process spawns a generator with `context.spawn(generator)`. The spawned entity is an entity like any other:

- It enters and leaves the model as any entity does, so a drain waits for it.
- It counts as a flow of its own, under the spawned task's name.
- Its records carry its own key, and the spawning entity's key is read as `attr.parent`.

A spawned task cannot share its name with an arrival, because their flow statistics and entity numbers would merge.

`spawn: {task: courier, after: 3 s}` starts the new entity 3 seconds after the task finishes, as SimPy's `start_delayed` does. `context.spawn(generator, delay=3)` does the same for a process. A drain that ends during the delay ends the run before the spawned entity starts, as it does for an arrival not yet due. An `after` of 0 or less is refused.

`count` starts that many entities at once: a whole number, an expression or a generator, read once in the parent's step. A `count` of 0 or less starts none. Each new entity has its own key, and `copy_attributes: true` gives each a copy of the parent's attributes:

```yaml
# Two deliveries start for each packed order
spawn: {task: deliver, count: 2}
```

`@app.task(spawn={"task": "deliver", "count": 2})` takes the same mapping.

A process calls `context.spawn(generator)` once for each entity it starts.

A task's `interrupt` runs when the task finishes, as a `spawn` does. Here the task that closes the shop interrupts each order still being checked, and starts a sweep 2 seconds later:

```yaml
# When the shop closes: stop each order still being checked, and start a sweep
# 2 seconds later
interrupt: {task: check, where: "process.target == 'service'", cause: closing}
spawn: {task: sweep, after: 2 s}
```

`@app.task(interrupt=..., spawn=...)` takes the same two mappings as dicts.

A low-level process calls `process.interrupt(cause)` on each process it stops, and starts the sweep with `env.process` after a timeout of 2 seconds.

### Be Told When a Flow Ends

A task's `subscribe` asks to be interrupted when something happens, rather than pausing for it. `subscribe: {task: charger}` starts the `charger` child flow, and interrupts the task when that flow ends, with the cause `{"event": "charger", "value": <charger's payload>}`. The task then follows its `on_interrupt`, as it does for any interrupt. The subscription belongs to the flow's process, so after a hand-on the interrupt reaches the next task. `context.subscribe(event)` subscribes a process to any SimPy event, a process included, as SimPy's `subscribe_at` does. It refuses an event that has already happened.

### Actions When an Event Happens

An `on:` list runs actions when a signal fires or a task ends. The list belongs to a task or to a signal. On a signal, the actions run before any waiter, as a SimPy callback runs when its event fires. On a task, they run in the step the task ends, before its next task starts, as a statement at the end of a SimPy process's step does. A `next` condition on the same task still reads the old values, because the next task is chosen before the actions run, and the next task reads the new ones. They run when the task ends by finishing, balking, reneging, abandoning, or failing in a way `on_fail` or `fail` handles. A task that ends in a run failure runs no `on:` action, because nothing runs after an uncaught exception in SimPy. A run failure is an error that stops the run: a `next` condition that cannot be evaluated, a wait that fails because its child did, or a task function that raises, when no `on_fail` handles it. Each entry is one of:

- `fire: {signal: name}` fires a signal. `value` fires it with a literal value. `from: name` copies an outcome instead. For a signal, it copies the signal's latest outcome. For a task, it copies the finished payload, or the failure reason, of the last instance of that task to end on the entity. SimPy's `trigger` copies another event's outcome in the same way. With nothing to copy yet it fires nothing and logs a warning. `value` and `from` together are refused. On a one-shot signal, `urgent: true` makes the waiters resume before the other events at their time, as SimPy's `URGENT` priority orders them. `after: 5 s` fires it 5 seconds from now, as `env.schedule(event, priority, delay)` does. The signal counts as fired at once, so a second `fire` is refused. A drain does not count the entity that fired it as waiting for the later firing. `priority` is a whole number that orders the events at one time, as `env.schedule(event, priority)` does: 1 is the default and 0 is what `urgent: true` gives. It is refused with `urgent`, works with `after`, and is refused on a signal that re-arms. On a signal that re-arms (`once: false`), `urgent` and `after` are refused at build too. `fail: true` fails the signal with `value` as the reason, and a waiter receives the failure, as with SimPy's `Event.fail`; `fail` with `from` is refused. A one-shot signal's failure that no task waits for stops the run under `on_unhandled_failure: stop`, as a failed SimPy event does, unless `defused: true` stands beside `fail`, which does what `event.defused = True` does. `signals.<name>.defused: true` defuses every failure of that signal, and a failure of a signal that re-arms is always defused. An entry may also carry `when: <expression>`, which skips the entry when it is false.
- `spawn: {task, after}` starts a new entity, as the `spawn` key does.
- `interrupt: {task, where, cause}` interrupts running instances, as a task's `interrupt` does.
- `path` with `value` sets a registry value, as a scenario step does.
- `resume: {task, where, value, limit}` wakes parked instances, as the `resume` key does.

The same closing task with an `on:` list: it stops the packing by hand, makes the orders come every 6 seconds, and fires a one-shot `reminder` with a value 5 seconds later:

```yaml
# However the task ends: stop the packing by hand, make the orders come every
# 6 seconds, and remind the couriers 5 seconds later
on:
  - interrupt: {task: pack_by_hand, cause: closing}
  - {path: Shop.arrival.orders.value, value: 6}
  - fire: {signal: reminder, value: last parcels, after: 5 s}
```

`@app.task(on=[...])` takes the same list of mappings.

A low-level model adds a callback to the event, as SimPy's `event.callbacks` runs one when the event fires.

A one-shot `closing` signal with an `on:` action that fires `last_call` before the other events at closing time:

```yaml
closing:
  once: true
  on:
    # Tell the couriers before anything else that happens at closing time
    - fire: {signal: last_call, urgent: true}
last_call: {once: true}
```

`app.add_signal("closing", once=True, on=[{"fire": {"signal": "last_call", "urgent": True}}])` declares the same signal, and `app.add_signal("last_call", once=True)` the other.

A low-level model schedules the event with SimPy's `URGENT` priority, `env.schedule(event, URGENT)`.

A task's `listen` adds callbacks to the next firing of a signal, of a family member (`signal: {of: reply, key: ...}`) or of an opened event (`handle: name`, with `index` for an element of an `each`), when the task starts and before its wait. It is `event.callbacks.append(f)`: the callbacks run in the order they were added, before the waiters added later, and each entry may carry a `when`. A signal whose firing was processed already takes no callback, and the task fails, as `event.callbacks.append` raises. An entry cannot read `item.`, because `listen` attaches before the task's `get`. A flow that wants to keep listening listens again. This task logs each closing, whatever else it does:

```yaml
watch:
  # Adds a callback to the next firing of closing, before the task waits
  listen:
    signal: closing
    on:
      - fire: {signal: logged, value: closing seen}
  wait: {signal: closing}
  payload: {event_type: watched}
```

A task with an `on:` list that fails a signal, and runs one entry only when a condition holds:

```yaml
on:
  # Fail the till signal with a reason; waiters receive the failure, and a failure
  # that nobody waits for is defused, so it does not stop the run
  - fire: {signal: till, fail: true, defused: true, value: the till did not balance}
  # Run this entry only when the condition holds
  - when: t >= 10
    fire: {signal: reminder, value: last parcels, priority: 2, after: 3 s}
```

The signals of that model are declared in the mapping form. `defused: true` on a signal does what `defused: true` on a `fire` does for every failure of that signal, and `family` with `members` makes three signals, `lane[0]` to `lane[2]`, when the run starts:

```yaml
signals:
  closing: {once: true}
  till: {once: true}
  reminder: {once: true}
  # Every failure of this signal is defused, so none of them stops the run
  logged: {once: true, defused: true}
  # A family of signals: one for each lane, numbered 0 to 2
  lane: {family: true, members: 3}
```

An `on:` list also turns an arrival on (`activate`) or off (`deactivate`), and wakes parked orders (`resume`, with a `task` and a `where` that keeps the instances it holds for):

```yaml
reopen:
  on:
    # Turn the rush source on, turn the walk-in source off, and wake the parked
    # orders of vip customers
    - {activate: rush}
    - {deactivate: walkins}
    - resume: {task: parked, where: attr.vip}
  payload: {event_type: reopened}
```

`@app.task(listen={"signal": "closing", "on": [...]})` and `@app.task(on=[...])` take the same mappings, and `app.add_signal("logged", once=True, defused=True)` and `app.add_signal("lane", family=True, members=3)` declare the signals.

`event.callbacks.append(fn)` adds the callback, as SimPy code does.

______________________________________________________________________

## How It Works

The task publishes `queued` when its wait starts, so the wait counts in its wait statistics. Its `patience` covers the wait as well as its resource and its `get`. When an `any` wait ends, its losing signal, timeout and condition branches are dropped. Its losing child flows keep running, as SimPy leaves processes running. [Wait for One or All](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/index.md) shows ties and failures. A wait for signals, tasks and times holds no resource. Waiting for all of several such things reserves nothing: the task asks for its resource only after the wait.

A drain waits for a task's `on:` actions and for the `on:` actions of signals its tasks fire, so a final task that fires a signal with `on:` actions leaves only after they have run.

______________________________________________________________________

## Example: Process Events

The `process-events` example combines the event features of this page:

- A guard starts an alarm round and is told when it ends (`subscribe`).
- A packed order fires the `ready` signal from its own payload, for a waiting shipper (an `on:` action with `fire ... from`).
- A one-shot `closing` signal has an `on:` action that spawns a sweep one second later (`spawn` with `after`).

The arrival gaps are set so that each of these happens once. It needs no Docker service and ends on its own after 12 simulation seconds.

```yaml
signals:
  # ready re-arms; the one-shot closing fires once and spawns a sweep
  ready: {once: false}
  closing:
    once: true
    on:
      - spawn: {task: sweep, after: 1 s}

arrivals:
  orders: {dist: constant, value: 6, spawn: pack}
  shippers: {dist: constant, value: 7, spawn: ship}
  closers: {dist: constant, value: 9, spawn: close}
  guards: {dist: constant, value: 8, spawn: guard}

tasks:
  # A packed order fires ready from its own finished payload; a shipper reads it
  pack:
    service: packing
    payload: {event_type: order_packed}
    on:
      - fire: {signal: ready, from: pack}
  ship:
    wait: {signal: ready}
    payload: {event_type: order_shipped}
  # Fires the one-shot closing when the shop closes
  close:
    signal: closing
    payload: {event_type: shop_closed}
  sweep:
    service: sweeping
    payload: {event_type: floor_swept}
  # The guard starts an alarm round and is interrupted when it ends
  guard:
    service: guarding
    subscribe: {task: alarm}
    on_interrupt: abandon
    payload: {event_type: shift_done}
  alarm:
    service: alarming
    payload: {event_type: alarm_rang}
```

```python
app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_service("packing", dist="constant", value=2)
    .add_service("alarming", dist="constant", value=2)
    .add_service("sweeping", dist="constant", value=1)
    .add_service("guarding", dist="constant", value=100)
    # A plain signal re-arms; the one-shot closing fires once and spawns a sweep
    .add_signal("ready", once=False)
    .add_signal("closing", once=True, on=[{"spawn": {"task": "sweep", "after": 1}}])
    .add_arrival("orders", dist="constant", value=6)
    .add_arrival("shippers", dist="constant", value=7)
    .add_arrival("closers", dist="constant", value=9)
    .add_arrival("guards", dist="constant", value=8)
)


# A packed order fires ready from its own finished payload; a shipper reads it
@app.task(service_id="packing", on=[{"fire": {"signal": "ready", "from": "pack"}}])
def pack(task_id):
    return {"event_type": "order_packed"}


@app.task(wait={"signal": "ready"})
def ship(task_id):
    return {"event_type": "order_shipped"}


# Fires the one-shot closing when the shop closes
@app.task(signal="closing")
def close(task_id):
    return {"event_type": "shop_closed"}


@app.task(service_id="sweeping")
def sweep(task_id):
    return {"event_type": "floor_swept"}


# The guard starts an alarm round and is interrupted when it ends
@app.task(service_id="guarding", subscribe={"task": "alarm"}, on_interrupt="abandon")
def guard(task_id):
    return {"event_type": "shift_done"}


@app.task(service_id="alarming")
def alarm(task_id):
    return {"event_type": "alarm_rang"}
```

The low-level version uses plain SimPy for each of these: a `Signal`, an interrupt handled by `env.any_of`, and `simpy.util.start_delayed`. The whole file is in the Low-level tab of the Quick Start below.

Run the example in the form you use:

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get process-events --api yaml
cd process-events/yaml

# 3. Run it
desflow run process_events.yaml
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get process-events --api yaml
cd process-events/yaml

# 3. Run it
desflow run process_events.yaml
```

The whole file is [`examples/yaml/process_events.yaml`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc1/examples/yaml/process_events.yaml).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get process-events --api builder
cd process-events/builder

# 3. Run it
uv run --no-project --with desflow process_events_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get process-events --api builder
cd process-events/builder

# 3. Run it
python process_events_example.py
```

The whole file is [`examples/builder/process_events_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc1/examples/builder/process_events_example.py).

**Quick Start**

`desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

```bash
# 1. Install
uv tool install desflow

# 2. Download the example
desflow examples get process-events --api low-level
cd process-events/low-level

# 3. Run it
uv run --no-project --with desflow process_events_example.py
```

```bash
# 1. Install
pip install desflow

# 2. Download the example
desflow examples get process-events --api low-level
cd process-events/low-level

# 3. Run it
python process_events_example.py
```

The whole file is [`examples/low-level/process_events_example.py`](https://github.com/jaehyeon-kim/desflow/blob/v1.0.0rc1/examples/low-level/process_events_example.py).
