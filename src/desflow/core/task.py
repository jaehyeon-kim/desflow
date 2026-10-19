"""The life of one task instance: what `@app.task` and a YAML task run.

`TaskSpec` holds what a task is, checked once when it is declared. `TaskRun` runs one
instance of it as a SimPy process, in this order: the balk check; `queued`; the
`wait`; the resource; the `get`; `started`; the user function; the service time;
the `put`; the `fail` check; the finished payload, or `failed`; the `signal`; the
`interrupt` action; `set`; then `next`, or `on_fail`, or the end of the flow. The
context keeps the decorator and the checks made before the clock starts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from simpy import Interrupt
from simpy.resources.resource import Preempted

from desflow.core.expressions import Expression, Scope
from desflow.core.flow import Router, Transfer, Wait, make_scope
from desflow.models.flow import Entity, Upstream
from desflow.utils import suggestion

if TYPE_CHECKING:
    from desflow.core.context import SimulationContext


class TaskFailed(Exception):
    """A task instance failed, and no `on_fail` handled it.

    A child flow, started by a `wait` branch or `context.call`, fails with it, so
    the task waiting for the flow fails too.

    Attributes:
        task (str): The task that failed.
        reason (Any): Why: its `fail` condition, its `fail_reason`, or the error its
            function raised.
    """

    def __init__(self, task: str, reason: Any):
        # SimPy copies a failure as type(error)(*error.args), so args are the
        # constructor's arguments
        super().__init__(task, reason)
        self.task = task
        self.reason = reason

    def __str__(self) -> str:
        return f"task '{self.task}' failed: {self.reason}"


# What a wait or a get returns when the task's patience ran out first
_RENEGED = object()


class TaskSpec:
    """What a task is: its keys, checked when it is declared, and what `run()`
    checks before the clock starts."""

    def __init__(
        self,
        name: str,
        service_id: Optional[str],
        resource_id: Optional[str],
        priority: Any,
        get: Optional[Transfer],
        put: Optional[Transfer],
        router: Router,
        patience: Any = None,
        balk: Optional[str] = None,
        wait: Optional[Wait] = None,
        signal: Optional[str] = None,
        fail: Any = None,
        fail_reason: Optional[str] = None,
        on_fail: Optional[str] = None,
        attribute_changes: Optional[Dict[str, Any]] = None,
        resources: Optional[List[str]] = None,
        keep: Optional[List[str]] = None,
        preempt: bool = True,
        on_interrupt: str = "resume",
        on_interrupt_next: Optional[str] = None,
        interrupt: Optional[Dict[str, Any]] = None,
    ):
        self.name = name
        self.service_id = service_id
        # Every resource it holds, acquired in this order; resource_id is the first
        self.resources: List[str] = list(
            resources or ([resource_id] if resource_id else [])
        )
        self.resource_id = self.resources[0] if self.resources else None
        self.keep: List[str] = list(keep or [])
        self.preempt = preempt
        self.on_interrupt = on_interrupt
        self.on_interrupt_next = on_interrupt_next
        self.interrupt = interrupt
        self.interrupt_where = (
            Expression(interrupt["where"])
            if interrupt is not None and interrupt.get("where") is not None
            else None
        )
        self.priority = priority
        self.get = get
        self.put = put
        self.router = router
        self.patience = patience
        self.balk = balk
        self.wait = wait
        self.signal = signal
        self.fail = fail
        self.fail_reason = fail_reason
        self.on_fail = on_fail
        self.attribute_changes = dict(attribute_changes or {})
        self.rank_of = Expression(priority) if isinstance(priority, str) else None
        self.patience_of = Expression(patience) if isinstance(patience, str) else None
        self.balk_of = Expression(balk) if balk is not None else None
        self.fail_of = Expression(fail) if isinstance(fail, str) else None
        # Waits for a unit or an item, so it queues
        self.queues = bool(self.resources) or get is not None or wait is not None
        # Publishes started before its work
        self.timed = service_id is not None or self.queues
        # Set when the function is decorated
        self.user_func: Callable[..., Any] = lambda *args, **kwargs: None
        self.declared: Any = {}

    @property
    def expressions(self) -> List[Tuple[str, Expression]]:
        """Every expression the task holds, with the key it is written under."""
        found: List[Tuple[str, Expression]] = []
        for key, expression in (
            ("priority", self.rank_of),
            ("patience", self.patience_of),
            ("balk", self.balk_of),
            ("fail", self.fail_of),
        ):
            if expression is not None:
                found.append((key, expression))
        for key, transfer in (("get", self.get), ("put", self.put)):
            if transfer is not None:
                found.extend((key, e) for e in transfer.expressions)
        found.extend(("next", e) for e in self.router.conditions)
        if self.interrupt_where is not None:
            found.append(("interrupt", self.interrupt_where))
        if self.wait is not None:
            found.extend(("wait", e) for e in self.wait.conditions)
        return found


class TaskRun:
    """One instance of a task, run as a SimPy process by `run()`."""

    def __init__(
        self,
        context: SimulationContext,
        spec: TaskSpec,
        task_id: Any,
        entity: Entity,
        upstream: Optional[Upstream],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        child: bool = False,
    ):
        self.context = context
        self.spec = spec
        self.task_id = task_id
        self.entity = entity
        self.upstream = upstream
        self.args = args
        self.kwargs = kwargs
        # A child flow returns its last payload, or fails, to the task waiting for it
        self.child = child
        self.env = context.env
        self.item: Any = None
        self.waits: Dict[str, Dict[str, Any]] = {}
        self.process: Any = None
        # The put or get the task is waiting for, cancelled by an interrupt
        self._pending: Any = None
        # The child flows a `{task}` wait started, by branch, kept for a resume
        self._children: Dict[str, Any] = {}
        # The patience still to use, and when the current wait began
        self._patience_left: Optional[float] = None
        self._patience_since: Optional[float] = None

    # ------------------------------------------------------------------ records

    def publish(self, value: Any) -> None:
        """Publishes an event of this instance, keyed by its entity.

        Args:
            value: The event's value: a status such as `status("queued")`, or the
                task's payload.
        """
        self.env.publish_event(self.entity.key, value, self.spec.name, self.task_id)

    def status(self, state: str, **extra: Any) -> Dict[str, Any]:
        """Returns the value of a status event of this task.

        Args:
            state: The status, such as `queued`, `started` or `failed`.
            **extra: Fields added after `status`, such as `reason` or `cause`.

        Returns:
            Dict[str, Any]: `path_id` when the task has a service, then `status` and
                the extra fields.
        """
        value: Dict[str, Any] = {}
        if self.spec.service_id is not None:
            value["path_id"] = f"{self.context.sim_id}.service.{self.spec.service_id}"
        value["status"] = state
        value.update(extra)
        return value

    def scope(self, payload_upstream: Optional[Upstream] = None) -> Scope:
        """Returns what the task's expressions read now.

        Args:
            payload_upstream: The finished payload, for a `next` condition read after
                the task ends; None before it.

        Returns:
            Scope: The names an expression reads: the entity's attributes, the levels,
                the waits, the signals and the item.
        """
        return make_scope(
            payload_upstream if payload_upstream is not None else self.upstream,
            self.entity.attributes,
            self.env,
            self.context._levels(),
            item=self.item,
            waits=self.waits,
            signals=self.context._signals,
        )

    # --------------------------------------------------------------------- run

    def run(self):
        """The task's process: everything from the balk check to the next task.

        While it runs, the instance is listed with its task, so `interrupt` can
        reach it.
        """
        self.process = self.env.active_process
        running = self.context._running.setdefault(self.spec.name, [])
        running.append(self)
        if not self.child and not self.entity.entered:
            self.entity.entered = True
            self.context._entity_entered(self.entity)
        try:
            return (yield from self._run())
        finally:
            self._stop_running()

    def _stop_running(self) -> None:
        running = self.context._running.get(self.spec.name, [])
        if self in running:
            running.remove(self)

    def _run(self):
        spec, env, stats = self.spec, self.env, self.env.stats
        if spec.balk_of is not None and spec.balk_of.evaluate(self.scope()):
            self.publish(self.status("balked"))
            stats.task_balked(spec.name, self.task_id)
            self._end_flow("ended")
            return None
        # The patience counts waiting time only: it runs while the task waits for
        # its wait, its resources or its get, and pauses while it is served, so a
        # preempted task that queues again goes on from the time it had used, as
        # SimPy's reneging pattern `req | env.timeout(patience_left)` does.
        if spec.queues:
            self.publish(self.status("queued"))
            stats.task_queued(spec.name, self.task_id)
            if spec.patience is not None:
                self._patience_left = float(self._patience())

        failure: Any = None
        while spec.wait is not None:
            try:
                outcome = yield from self._wait(self._deadline())
                self._waited()
            except Interrupt as interrupt:
                self._waited()
                if self._interrupted(interrupt):
                    if spec.on_interrupt == "restart":
                        self._children = {}  # a restart starts its child flows again
                    continue  # resume or restart: wait again
                return (yield from self._abandon(interrupt))
            if outcome is _RENEGED:
                self._renege()
                return None
            failure = outcome
            if failure is not None:
                self.publish(self.status("failed", reason=failure))
                stats.task_failed(spec.name, self.task_id)
            break

        payload: Any = None
        rank = self._rank() if self.spec.resources else None
        remaining: Optional[float] = None  # service still owed, when interrupted
        started = got_item = put_done = False
        while failure is None:
            held: List[Any] = []
            service_from: Optional[float] = None
            try:
                for resource in spec.resources:
                    request = self.context._resources_map[resource].request(
                        rank, preempt=spec.preempt
                    )
                    held.append(request)
                    deadline = self._deadline()
                    if deadline is None:
                        yield request
                    else:
                        yield request | deadline
                        self._waited()
                        # The patience ran out before a unit was free: leave the
                        # queue, give back the units held so far, and renege.
                        if not request.triggered:
                            self._release(held)
                            self._renege()
                            return None
                if spec.get is not None and not got_item:
                    got = yield from self._transfer(
                        spec.get, "get", None, self._deadline()
                    )
                    self._waited()
                    if got is _RENEGED:
                        self._release(held)
                        self._renege()
                        return None
                    self.item, got_item = got, True
                if not started:
                    started = True
                    if spec.timed:
                        self.publish(self.status("started"))
                        stats.task_started(spec.name, self.task_id)
                    # Units the task before kept for this one go back once it starts
                    self._release_kept()
                    try:
                        payload = spec.user_func(
                            self.task_id, *self.args, **self._extra()
                        )
                    except Exception as error:  # noqa: BLE001: a task's failure
                        failure = f"{type(error).__name__}: {error}"
                else:
                    self.publish(self.status("resumed"))
                    stats.task_resumed(spec.name, self.task_id)
                if failure is None:
                    if spec.service_id is not None and remaining != 0:
                        if remaining is None:
                            config = env.registry.get_config(
                                f"{self.context.sim_id}.service.{spec.service_id}"
                            )
                            remaining = float(self.context.sampler.sample(config))
                        service_from = env.now
                        yield env.timeout(remaining)
                        remaining, service_from = 0.0, None
                    if spec.put is not None and not put_done:
                        yield from self._transfer(spec.put, "put", payload, None)
                        put_done = True
                    failure = self._failure(payload)
                if failure is None:
                    self.publish(payload)
                    stats.task_finished(spec.name, self.task_id)
                    # A stop condition, such as `t >= 300`, is checked after every
                    # finished task as well as after every change of state
                    env.notify_change()
                else:
                    self.publish(self.status("failed", reason=failure))
                    stats.task_failed(spec.name, self.task_id)
            except Interrupt as interrupt:
                self._waited()
                self._release(held)
                if service_from is not None and remaining is not None:
                    # resume serves what is left; restart draws a new service time
                    if spec.on_interrupt == "resume":
                        remaining -= env.now - service_from
                    else:
                        remaining = None
                if self._interrupted(interrupt):
                    continue
                return (yield from self._abandon(interrupt))
            except Exception:
                # Any other error gives the units back before it goes on, as a
                # `with` block does, so a child flow whose failure is handled
                # elsewhere does not keep them for the rest of the run
                self._release(held)
                raise
            self._release(held)
            break

        self._stop_running()
        if failure is not None:
            if spec.signal is not None:
                self.context._signals[spec.signal].fail(failure)
            failed = Upstream(
                self.task_id,
                self.entity.key,
                self.entity.attributes,
                self.status("failed", reason=failure),
            )
            if spec.on_fail is not None:
                return (yield from self._follow(spec.on_fail, failed))
            if self.child:
                raise TaskFailed(spec.name, failure)
            self._end_flow("failed")
            return None

        if spec.signal is not None:
            self.context._signals[spec.signal].fire(payload)
        if spec.interrupt is not None:
            self.context.interrupt(
                spec.interrupt["task"],
                where=spec.interrupt_where,
                cause=spec.interrupt.get("cause"),
            )
        self.entity.attributes.update(spec.attribute_changes)
        handed = Upstream(
            self.task_id, self.entity.key, self.entity.attributes, payload
        )
        following = spec.router.choose(
            handed,
            env,
            self.context._levels(),
            item=self.item,
            scope=self.scope(handed) if spec.router.conditions else None,
        )
        if following is None:
            if self.child:
                return payload
            self._end_flow("finished")
            return None
        return (yield from self._follow(following, handed))

    def _deadline(self) -> Any:
        """A timer for the patience left, started now, or None without patience."""
        if self._patience_left is None:
            return None
        self._patience_since = self.env.now
        return self.env.timeout(self._patience_left)

    def _waited(self) -> None:
        """Takes the time waited since `_deadline` from the patience left."""
        if self._patience_since is not None and self._patience_left is not None:
            self._patience_left -= self.env.now - self._patience_since
            self._patience_since = None

    def _release(self, held: List[Any]) -> None:
        """Cancels requests still waiting and gives back granted units, last first,
        as nested `with` blocks do; units the task keeps for the next one stay."""
        for request in reversed(held):
            request.cancel()
            name = request.resource.obj_id
            if (
                name in self.spec.keep
                and request.triggered
                and request in request.resource.users
            ):
                self.entity.held[name] = request
                continue
            request.resource.release(request)
        held.clear()
        if self._pending is not None:
            self._pending.cancel()
            self._pending = None

    def _release_kept(self) -> None:
        """Gives back the units the task before kept for this one."""
        for name in list(self.entity.held):
            request = self.entity.held.pop(name)
            request.resource.release(request)

    def _interrupted(self, interrupt: Interrupt) -> bool:
        """Publishes an interrupt or a preemption; True when the task goes on.

        The task has already given back its units and cancelled its waits.
        """
        spec, stats = self.spec, self.env.stats
        self._release_kept()
        cause = interrupt.cause
        preempted = isinstance(cause, Preempted)
        if preempted:
            value = self.status("preempted", resource=cause.resource.obj_id)
        else:
            value = self.status("interrupted", cause=cause)
        self.publish(value)
        stats.task_interrupted(spec.name, self.task_id, preempted=preempted)
        return spec.on_interrupt != "abandon"

    def _abandon(self, interrupt: Interrupt):
        """Ends the task after an interrupt with `on_interrupt: abandon`."""
        spec, stats = self.spec, self.env.stats
        stats.task_dropped(spec.name, self.task_id)
        self._stop_running()
        if spec.on_interrupt_next is not None:
            handed = Upstream(
                self.task_id,
                self.entity.key,
                self.entity.attributes,
                self.status("interrupted", cause=_cause(interrupt)),
            )
            return (yield from self._follow(spec.on_interrupt_next, handed))
        if self.child:
            raise TaskFailed(spec.name, f"interrupted: {_cause(interrupt)}")
        self._end_flow("ended")
        return None

    # ----------------------------------------------------------------- helpers

    def _extra(self) -> Dict[str, Any]:
        """The keyword arguments the user function declares."""
        extra = dict(self.kwargs)
        declared = self.spec.declared
        if "entity" in declared:
            extra["entity"] = self.entity
        if "upstream" in declared:
            extra["upstream"] = self.upstream
        if "item" in declared and self.spec.get is not None:
            extra["item"] = self.item
        if "wait" in declared and self.spec.wait is not None:
            extra["wait"] = self.waits
        return extra

    def _patience(self) -> float:
        spec = self.spec
        if spec.patience_of is not None:
            return spec.patience_of.evaluate(self.scope())
        if callable(spec.patience):
            return spec.patience(self.task_id, self.entity.attributes)
        return spec.patience

    def _rank(self) -> Any:
        spec = self.spec
        if spec.rank_of is not None:
            return spec.rank_of.evaluate(self.scope())
        if callable(spec.priority):
            return spec.priority(self.task_id, self.entity.attributes)
        return 1 if spec.priority is None else spec.priority

    def _renege(self) -> None:
        self.publish(self.status("reneged"))
        self.env.stats.task_reneged(self.spec.name, self.task_id)
        self._end_flow("ended")

    def _end_flow(self, how: str) -> None:
        """Records the end of the entity's flow: `finished`, `failed`, or `ended`
        for a balk, a renege or an abandon. A child flow's end is its parent's."""
        if self.child:
            return
        stats, arrival = self.env.stats, self.entity.arrival
        if how == "finished":
            stats.flow_finished(arrival)
        elif how == "failed":
            stats.flow_failed(arrival)
        else:
            stats.flow_ended(arrival)
        self.context._entity_left(self.entity)

    def _failure(self, payload: Any) -> Any:
        """The reason the task fails after its service, or None when it does not."""
        spec = self.spec
        if spec.fail is None:
            return None
        if spec.fail_of is not None:
            handed = Upstream(
                self.task_id, self.entity.key, self.entity.attributes, payload
            )
            failed = spec.fail_of.evaluate(self.scope(handed))
            text = spec.fail_of.text
        else:
            failed = spec.fail(self.task_id, payload)
            text = getattr(spec.fail, "__name__", "fail")
        if not failed:
            return None
        return spec.fail_reason if spec.fail_reason is not None else text

    def _follow(self, task: str, handed: Upstream):
        """Starts the next task; a child flow waits for it and returns its result."""
        context = self.context
        if task not in context._tasks:
            raise ValueError(
                f"task '{self.spec.name}' routes to '{task}', which is not a task"
                + suggestion(task, context._tasks)
            )
        if self.child:
            result = yield context._start(
                task,
                self.entity,
                handed,
                child=True,
                args=self.args,
                kwargs=self.kwargs,
            )
            return result
        self.env.process(
            context._tasks[task](
                context.next_instance(task),
                *self.args,
                entity=self.entity,
                upstream=handed,
                **self.kwargs,
            )
        )
        return None

    def _transfer(
        self, transfer: Transfer, direction: str, payload: Any, deadline: Any
    ):
        """Gets from or puts into a store or container, waiting as long as it takes.

        A get with a `deadline` waits for whichever comes first; when the deadline
        does, the get is cancelled, so it takes nothing later, and `_RENEGED` is
        returned.
        """
        context = self.context
        scope = self.scope()
        if transfer.store is not None:
            store = context._stores_map[transfer.store]
            if direction == "get":
                event = store.get(transfer.filter_for(scope))
            elif context._store_priority.get(transfer.store):
                rank = transfer.priority_for(self.task_id, self.upstream, scope)
                event = store.put(store.wrap(payload, rank))
            else:
                event = store.put(payload)
        else:
            container = context._containers_map[transfer.target]
            amount = transfer.amount_for(self.task_id, self.upstream, scope)
            event = (
                container.get(amount) if direction == "get" else container.put(amount)
            )
        self._pending = event
        if deadline is None:
            yield event
        else:
            yield event | deadline
            if not event.triggered:
                event.cancel()
                self._pending = None
                return _RENEGED
        self._pending = None
        if direction == "get" and transfer.store is not None:
            # A priority store holds PriorityItem wrappers; the task reads the payload.
            if context._store_priority.get(transfer.store):
                return event.value.item
            return event.value
        return None

    def _wait(self, deadline: Any):
        """Waits for the task's `wait`, then records each branch for `wait.<branch>`.

        Returns None when the wait succeeded, `_RENEGED` when the patience ran out
        first, or the failure reason when a branch failed.
        """
        env, wait = self.env, self.spec.wait
        assert wait is not None
        events: Dict[str, Any] = {}
        stop_watching: List[Callable[[], None]] = []
        for name, (kind, target) in wait.branches.items():
            if kind == "signal":
                events[name] = self.context._signals[target].wait()
            elif kind == "task":
                # A wait resumed after an interrupt waits again for the child flow
                # it started, as a SimPy process yields the same process again
                child = self._children.get(name)
                if child is None:
                    child = self.context._start(
                        target,
                        self.entity,
                        self.upstream,
                        child=True,
                        args=self.args,
                        kwargs=self.kwargs,
                    )
                    self._children[name] = child
                events[name] = child
            elif kind == "timeout":
                events[name] = env.timeout(target)
            else:
                event, stop = self._condition(target)
                events[name] = event
                stop_watching.append(stop)
        members = list(events.values())
        joined = env.any_of(members) if wait.mode == "any" else env.all_of(members)
        failure: Any = None
        try:
            yield joined if deadline is None else joined | deadline
        except Interrupt:
            raise
        except Exception as error:  # noqa: BLE001: a branch failed
            failure = getattr(error, "reason", None) or str(error)
        finally:
            # Losing condition branches stop checking; losing child flows keep
            # running, as SimPy leaves processes running.
            for stop in stop_watching:
                stop()
        # The patience timer was scheduled first, so at a tie it wins: the wait's
        # condition is then triggered but not processed, and has no value yet
        if failure is None and not joined.processed:
            return _RENEGED
        for name, event in events.items():
            done = (event in joined.value) if joined.ok else event.processed
            self.waits[name] = {
                "done": done,
                "ok": event.ok if done else None,
                "value": _branch_value(event) if done else None,
            }
        return failure

    def _condition(self, condition: Expression):
        """An event that succeeds once the condition holds, and how to stop it."""
        env = self.env
        event = env.event()

        def check() -> None:
            if not event.triggered and condition.evaluate(self.scope()):
                event.succeed(True)
                stop()

        def stop() -> None:
            env.unwatch_changes(check)

        check()
        if not event.triggered:
            env.watch_changes(check)
        return event, stop


def _cause(interrupt: Interrupt) -> Any:
    """An interrupt's cause as a record holds it: a preemption by its resource."""
    cause = interrupt.cause
    if isinstance(cause, Preempted):
        return f"preempted on {cause.resource.obj_id}"
    return cause


def _branch_value(event: Any) -> Any:
    """A finished branch's value; a failed one's is its reason."""
    if event.ok:
        return event.value
    return getattr(event.value, "reason", str(event.value))


def check_acquisition_order(
    resources: Dict[str, List[str]],
    handoffs: Dict[str, Tuple[List[str], Any]],
) -> None:
    """
    Refuses tasks that acquire shared resources in opposite orders.

    Two tasks that each hold one resource while they wait for the other can each
    wait for ever. A task that lists `[picker, bench]` acquires the picker first; a
    task that keeps the picker for its next task acquires it before every resource
    that task lists. If one resource comes before another in one place and after it
    in another, the model is refused, so every task acquires in one order.

    Args:
        resources: Each task's resources, in the order it acquires them.
        handoffs: Each task's `keep` list and the tasks its `next` can lead to.

    Raises:
        ValueError: If two resources are acquired in opposite orders, naming them.
    """
    before: Dict[Tuple[str, str], str] = {}

    def note(first: str, second: str, where: str) -> None:
        if (second, first) in before:
            raise ValueError(
                f"{before[(second, first)]} acquires '{second}' before '{first}', "
                f"but {where} acquires '{first}' before '{second}'; acquire shared "
                f"resources in one order everywhere, so no two tasks can wait for "
                f"each other"
            )
        before.setdefault((first, second), where)

    for task, listed in resources.items():
        for i, first in enumerate(listed):
            for second in listed[i + 1 :]:
                note(first, second, f"task '{task}'")
    for task, (keep, following) in handoffs.items():
        for kept in keep:
            for after in following:
                for second in resources.get(after, []):
                    if second != kept:
                        note(kept, second, f"task '{task}' keeping it for '{after}'")
