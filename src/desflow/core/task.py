"""The life of one task instance: what `@app.task` and a YAML task run.

`TaskSpec` holds what a task is, checked once when it is declared. `TaskRun` runs one
instance of it as a SimPy process, in this order: the balk check; `queued`; the
`wait`; the resource; the `get`; `started`; the user function; the service time;
the `put`; the `fail` check; the finished payload, or `failed`; the `signal`; the
`interrupt` action; `set`; then `next`, or `on_fail`, or the end of the flow. The
context keeps the decorator and the checks made before the clock starts.
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from simpy import Interrupt
from simpy.events import Condition
from simpy.resources.resource import Preempted

from desflow.core.expressions import Expression, Scope
from desflow.core.flow import Router, Transfer, Wait, make_scope
from desflow.resources.resource import QueueFull
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
        spawn: Optional[Dict[str, Any]] = None,
        subscribe: Optional[Wait] = None,
        on: Optional[List[Dict[str, Any]]] = None,
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
        # Start a new entity on success (`spawn:` key); subscribe to an event; run
        # `on:` actions on every end. `on` is a list of normalized action mappings.
        self.spawn = spawn
        self.subscribe = subscribe
        self.on: List[Dict[str, Any]] = list(on or [])
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
        # The exception a task function raised, to chain the TaskFailed it causes
        self._fail_error: Optional[BaseException] = None
        # Units won by a `request` wait branch, held through the service and given
        # back when the task ends, as a `resource:` unit is
        self._wait_held: List[Any] = []
        # The live `request`, `get` and `put` branch events of the current wait, kept
        # so a resume reuses them and a renege or an abandon cancels them
        self._wait_events: Dict[str, Any] = {}
        # The patience still to use, and when the current wait began
        self._patience_left: Optional[float] = None
        self._patience_since: Optional[float] = None
        # What the instance waits on now, read as `process.target`; None once ended
        self._target: Optional[str] = None
        # The instance's end event (for `on:`, `running`, `subscribe`), and the
        # Upstream it hands on for that end; both None when nothing reads the end
        self.ended: Any = None
        self.outcome: Optional[Upstream] = None
        # The running instances a `running` branch selected, by branch name, kept
        # so the branch's value reads their end events after the wait
        self._running_runs: Dict[str, List["TaskRun"]] = {}
        # An instance whose task needs an end event (for `running`, `subscribe`, a
        # `fire from`, or its own `on:` actions) gets one, defused so its failure
        # stops the run only through the task process.
        if spec.name in context._ended:
            make_event = getattr(self.env, "internal_event", self.env.event)
            self.ended = make_event()  # type: ignore[misc]
            self.ended.defused = True
            for action in spec.on:
                self.ended.callbacks.append(
                    functools.partial(self._run_end_action, action)
                )
            if not child:
                # Count one pending end against the entity, taken back when the end
                # event is processed, so a drain waits for the `on:` actions.
                entity.ends_pending += 1
                self.ended.callbacks.append(
                    functools.partial(context._end_processed, entity)
                )
        if spec.name in context._tracked:
            self._list_instance()

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
            process={
                "name": self.spec.name,
                "instance": self.task_id,
                "target": self._target,
            },
            resources=self.context._resources_map,
        )

    # ----------------------------------------------------------- end event

    def _list_instance(self) -> None:
        """Lists this instance under its task on its entity, and on the entity that
        spawned it while that entity's flow has not ended, for a `running` wait."""
        if self.entity.left:
            return
        if self.entity.instances is None:
            self.entity.instances = {}
        self.entity.instances.setdefault(self.spec.name, []).append(self)
        parent = self.entity.spawned_by
        if parent is not None and not parent.ending:
            if parent.instances is None:
                parent.instances = {}
            parent.instances.setdefault(self.spec.name, []).append(self)

    def _set_ended(self, outcome: Optional[Upstream], ok: bool, result: Any) -> None:
        """Sets `outcome` and triggers the end event, if this instance has one and it
        has not been triggered. A success carries the finished payload or None; a
        failure carries the exception a waiter receives."""
        if self.ended is None:
            return
        self.outcome = outcome
        if self.ended.triggered:
            return
        self._record_last_ended()
        if ok:
            self.ended.succeed(result)
        else:
            self.ended.fail(result)

    def _record_last_ended(self) -> None:
        """Records this instance as the last-ended of its task on its entity, for a
        `fire from` that copies it, unless the entity has left."""
        if self.spec.name in self.context._copied and not self.entity.left:
            if self.entity.last_ended is None:
                self.entity.last_ended = {}
            self.entity.last_ended[self.spec.name] = self.ended

    def _end_default(self, ok: bool, error: Any = None) -> None:
        """Triggers the end event for a balk, a renege or an error, when no other end
        triggered it: a success with None, or a failure with the error."""
        if self.ended is None or self.ended.triggered:
            return
        self._record_last_ended()
        if ok:
            self.ended.succeed(None)
        else:
            self.ended.fail(error)

    def _run_end_action(self, action: Dict[str, Any], _event: Any = None) -> None:
        """Runs one of the task's `on:` actions when its end event is processed."""
        self.context._run_on_action(
            action, f"task '{self.spec.name}'", self.entity, self.outcome
        )

    def _running_event(self, node: Any) -> Any:
        """The `AllOf` of the end events of a `running` branch's selected instances:
        the task's instances on this entity and the entities it spawned, ended ones
        included, skipping this instance, each narrowed by `where` in its scope."""
        target = node.target
        where = target["where"]
        selected = [
            run
            for run in (self.entity.instances or {}).get(target["task"], [])
            if run is not self
            and run.ended is not None
            and (where is None or where.evaluate(run.scope()))
        ]
        self._running_runs[node.name] = selected
        return self.env.all_of([run.ended for run in selected])

    def _running_value(self, node: Any, done: bool) -> Any:
        """A `running` branch's value: the one instance's payload, a mapping of id to
        payload for several in start order, or an empty mapping when none matched."""
        if not done:
            return None
        selected = self._running_runs.get(node.name, [])
        if not selected:
            return {}
        if len(selected) == 1:
            return selected[0].ended.value
        return {run.task_id: run.ended.value for run in selected}

    def _subscribe_event(self, wait: Wait) -> Any:
        """The event a `subscribe:` key subscribes the instance to: a child flow for
        a task branch, a signal's firing, or the running instances of a task."""
        node = wait.root
        if node.kind == "task":
            return self.context._start(
                node.target,
                self.entity,
                self.upstream,
                child=True,
                args=self.args,
                kwargs=self.kwargs,
            )
        if node.kind == "signal":
            return self.context._signals[node.target].wait()
        if node.kind == "running":
            return self._running_event(node)
        raise ValueError(f"subscribe does not take a {node.kind} branch")

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
            # A spawn without `after` has already counted the entity in; the first
            # instance of any other entity counts it in here.
            self.context._entity_entered(self.entity)
        try:
            result = yield from self._run()
        except BaseException as error:
            # An error ends the instance: the end event fails with it, so a waiter
            # receives it. The run still raises, as before.
            self._end_default(ok=False, error=error)
            raise
        finally:
            self._target = None
            self._stop_running()
        # A balk or a renege returns with the end event not yet triggered: it
        # succeeds with None, so a `running` waiter resumes with no value.
        self._end_default(ok=True)
        return result

    @staticmethod
    def _target_label(node: Any) -> Optional[str]:
        """What `process.target` reads for a wait node: None for a group."""
        if node.is_group:
            return None
        if node.kind in ("signal", "task"):
            return f"{node.kind}.{node.target}"
        if node.kind == "request":
            return f"resource.{node.target}"
        if node.kind in ("get", "put"):
            return f"{node.kind}.{node.target.target}"
        if node.kind == "running":
            return f"task.{node.target['task']}"
        return node.kind  # timeout or condition

    def _stop_running(self) -> None:
        running = self.context._running.get(self.spec.name, [])
        if self in running:
            running.remove(self)

    def _run(self):
        spec, env, stats = self.spec, self.env, self.env.stats
        if spec.balk_of is not None and spec.balk_of.evaluate(self.scope()):
            self._release_kept()
            self.publish(self.status("balked"))
            stats.task_balked(spec.name, self.task_id)
            self.outcome = Upstream(
                self.task_id,
                self.entity.key,
                self.entity.attributes,
                self.status("balked"),
            )
            self._end_flow("ended")
            return None
        if spec.subscribe is not None:
            # Be told when the event happens: subscribe this instance, then go on.
            self.context.subscribe(self._subscribe_event(spec.subscribe))
        if spec.resources and spec.wait is None:
            # Nothing yields between here and the first request, so a full line is
            # known now and the task balks before it queues, as in SimPy it would
            # when the request raised.
            first = self.context._resources_map[spec.resources[0]]
            limit = getattr(first.queue, "maxlen", None)
            if limit is not None and len(first.queue) >= limit:
                self._release_kept()
                self._balk_full(first)
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
                        self._cancel_wait_events()  # and re-makes its wait branches
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
                # A unit a request wait branch won and a preemption then took is
                # requested again on resume, as a `resource:` unit is (C8).
                for index in range(len(self._wait_held)):
                    won = self._wait_held[index]
                    if won not in won.resource.users:
                        self._target = f"resource.{won.resource.obj_id}"
                        try:
                            fresh = won.resource.request(
                                self._rank(), preempt=spec.preempt
                            )
                        except QueueFull:
                            self._release(held)
                            self._release_kept()
                            self._balk_full(won.resource)
                            return None
                        self._wait_held[index] = fresh
                        deadline = self._deadline()
                        if deadline is None:
                            yield fresh
                        else:
                            yield fresh | deadline
                            self._waited()
                            # The patience ran out before the unit was free, as
                            # for a `resource:` unit.
                            if not fresh.triggered:
                                fresh.cancel()
                                self._release(held)
                                self._renege()
                                return None
                for resource in spec.resources:
                    self._target = f"resource.{resource}"
                    try:
                        request = self.context._resources_map[resource].request(
                            rank, preempt=spec.preempt
                        )
                    except QueueFull:
                        self._release(held)
                        self._release_kept()
                        self._balk_full(self.context._resources_map[resource])
                        return None
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
                    self._target = f"get.{spec.get.target}"
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
                        self._fail_error = error
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
                        self._target = "service"
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
                # A unit asked for again and not yet granted leaves the line, so
                # the resume asks once, not twice
                for request in self._wait_held:
                    if not request.triggered:
                        request.cancel()
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
                self._end_wait()
                raise
            self._release(held)
            break

        self._stop_running()
        self._end_wait()
        if failure is not None:
            if spec.signal is not None:
                self.context._fire_signal(spec.signal, False, failure, self.entity)
            failed = Upstream(
                self.task_id,
                self.entity.key,
                self.entity.attributes,
                self.status("failed", reason=failure),
            )
            # The end event fails whether or not on_fail handles the task, so a
            # waiter and the `on:` actions see the failure.
            self._set_ended(failed, ok=False, result=TaskFailed(spec.name, failure))
            if spec.on_fail is not None:
                return (yield from self._follow(spec.on_fail, failed))
            if self.child:
                raise TaskFailed(spec.name, failure) from self._fail_error
            # Under `continue` a root task's failure ends the flow and the run goes
            # on; under `stop` it raises, so the exception reaches the caller of
            # `run()`, as SimPy's unhandled failure does.
            if getattr(self.env, "on_unhandled_failure", "stop") == "stop":
                raise TaskFailed(spec.name, failure) from self._fail_error
            self._end_flow("failed")
            return None

        if spec.signal is not None:
            self.context._fire_signal(spec.signal, True, payload, self.entity)
        if spec.interrupt is not None:
            self.context.interrupt(
                spec.interrupt["task"],
                where=spec.interrupt_where,
                cause=spec.interrupt.get("cause"),
            )
        if spec.spawn is not None:
            # Start a new entity on success, after the signal and interrupt actions
            # and before `set`, so its upstream shares the spawning entity's
            # attributes dictionary as later `set` calls change it.
            self.context._spawn_entity(
                spec.spawn["task"],
                self.entity,
                Upstream(
                    self.task_id, self.entity.key, self.entity.attributes, payload
                ),
                spec.spawn["after"],
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
        # The end event succeeds once the next task is chosen, before the hand-on.
        self._set_ended(handed, ok=True, result=payload)
        if following is None:
            if self.child:
                return payload
            self._end_flow("finished")
            return payload
        result = yield from self._follow(following, handed)
        # A root task's process ends with its own finished payload, also when it
        # hands on; a child flow's process ends with the flow's last payload.
        return result if self.child else payload

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

    def _end_wait(self) -> None:
        """At the task's true end, not a resume, gives back the units a `request` wait
        branch won and cancels any wait-branch event still live, so a renege, a
        failing wait or an abandon leaves nothing reserved."""
        for request in self._wait_held:
            if request in request.resource.users:
                request.resource.release(request)
        self._wait_held = []
        self._cancel_wait_events()

    def _cancel_wait_events(self) -> None:
        """Cancels every live request, get or put wait-branch event, giving back a
        unit a request already won. A store or container get or put has no `users`,
        so it is only cancelled, as SimPy leaves a won get."""
        branches = self.spec.wait.branches if self.spec.wait is not None else {}
        for name, event in self._wait_events.items():
            event.cancel()
            node = branches.get(name)
            if (
                node is not None
                and node.kind == "request"
                and event.triggered
                and event in event.resource.users
            ):
                event.resource.release(event)
        self._wait_events = {}

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
            value = self.status(
                "preempted",
                resource=cause.resource.obj_id,
                preempted_by=self.context._entity_key_of(cause.by),
                usage_since=cause.usage_since,
            )
        else:
            value = self.status("interrupted", cause=cause)
        self.publish(value)
        stats.task_interrupted(spec.name, self.task_id, preempted=preempted)
        return spec.on_interrupt != "abandon"

    def _abandon(self, interrupt: Interrupt):
        """Ends the task after an interrupt with `on_interrupt: abandon`."""
        spec, stats = self.spec, self.env.stats
        self._end_wait()
        stats.task_dropped(spec.name, self.task_id)
        self._stop_running()
        handed = Upstream(
            self.task_id,
            self.entity.key,
            self.entity.attributes,
            self.status("interrupted", cause=_cause(interrupt)),
        )
        if spec.on_interrupt_next is not None:
            self._set_ended(handed, ok=True, result=None)
            return (yield from self._follow(spec.on_interrupt_next, handed))
        if self.child:
            # In a child flow with no on_interrupt_next, the end event fails with
            # the TaskFailed the abandon raises, so a waiter fails with it.
            self._set_ended(
                handed,
                ok=False,
                result=TaskFailed(spec.name, f"interrupted: {_cause(interrupt)}"),
            )
            raise TaskFailed(spec.name, f"interrupted: {_cause(interrupt)}")
        self._set_ended(handed, ok=True, result=None)
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
        return 0 if spec.priority is None else spec.priority

    def _balk_full(self, resource: Any) -> None:
        """Ends the flow of a task whose resource's waiting line is full: it balks."""
        self._end_wait()
        record = self.status("balked", resource=resource.obj_id)
        self.publish(record)
        self.env.stats.task_balked(self.spec.name, self.task_id)
        self.outcome = Upstream(
            self.task_id, self.entity.key, self.entity.attributes, dict(record)
        )
        self._end_flow("ended")

    def _renege(self) -> None:
        self._end_wait()
        self.publish(self.status("reneged"))
        self.env.stats.task_reneged(self.spec.name, self.task_id)
        self.outcome = Upstream(
            self.task_id,
            self.entity.key,
            self.entity.attributes,
            self.status("reneged"),
        )
        self._end_flow("ended")

    def _end_flow(self, how: str) -> None:
        """Records the end of the entity's flow: `finished`, `failed`, or `ended`
        for a balk, a renege or an abandon. A child flow's end is its parent's.

        The entity leaves once its own flow has ended and no end event or counted
        signal firing is still pending, so a drain waits for the `on:` actions.
        """
        if self.child:
            return
        stats, arrival = self.env.stats, self.entity.arrival
        if how == "finished":
            stats.flow_finished(arrival)
        elif how == "failed":
            stats.flow_failed(arrival)
        else:
            stats.flow_ended(arrival)
        self.entity.ending = True
        if self.entity.ends_pending == 0:
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
        event = self._transfer_event(transfer, direction, payload)
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
        return self._transfer_value(transfer, direction, event)

    def _transfer_event(self, transfer: Transfer, direction: str, payload: Any) -> Any:
        """Makes the SimPy get or put event, without waiting for it. A wait branch
        makes it this way to put it in the wait's condition; `_transfer` then waits."""
        context, scope = self.context, self.scope()
        if transfer.store is not None:
            store = context._stores_map[transfer.store]
            if direction == "get":
                return store.get(transfer.filter_for(scope))
            if context._store_priority.get(transfer.store):
                rank = transfer.priority_for(self.task_id, self.upstream, scope)
                return store.put(store.wrap(payload, rank))
            return store.put(payload)
        container = context._containers_map[transfer.target]
        amount = transfer.amount_for(self.task_id, self.upstream, scope)
        return container.get(amount) if direction == "get" else container.put(amount)

    def _transfer_value(self, transfer: Transfer, direction: str, event: Any) -> Any:
        """The value a finished get or put event gives the task: the item a get took,
        unwrapped from a priority store's wrapper, or None for a put."""
        if direction == "get" and transfer.store is not None:
            if self.context._store_priority.get(transfer.store):
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
        self._target = self._target_label(wait.root)
        events: Dict[str, Any] = {}
        stop_watching: List[Callable[[], None]] = []

        def build(node: Any) -> Any:
            """Builds the SimPy event for a node, nesting groups as SimPy conditions."""
            if node.is_group:
                members = [build(child) for child in node.children]
                if node.kind == "any":
                    event: Any = env.any_of(members)
                elif node.kind == "all":
                    event = env.all_of(members)
                else:
                    count = node.count
                    event = Condition(env, lambda evs, c, n=count: c >= n, members)
            elif node.kind == "signal":
                event = self.context._signals[node.target].wait()
            elif node.kind == "task":
                # A wait resumed after an interrupt waits again for the child flow it
                # started, as a SimPy process yields the same process again
                child = self._children.get(node.name)
                if child is None:
                    child = self.context._start(
                        node.target,
                        self.entity,
                        self.upstream,
                        child=True,
                        args=self.args,
                        kwargs=self.kwargs,
                    )
                    self._children[node.name] = child
                event = child
            elif node.kind == "timeout":
                seconds, timeout_value = node.target
                event = env.timeout(seconds, timeout_value)
            elif node.kind == "request":
                # Kept across a resume, as a `task` branch's child flow is, so the
                # task does not queue a second request each time it waits again.
                event = self._wait_events.get(node.name)
                if event is None:
                    resource = self.context._resources_map[node.target]
                    event = resource.request(self._rank(), preempt=self.spec.preempt)
                    self._wait_events[node.name] = event
            elif node.kind in ("get", "put"):
                event = self._wait_events.get(node.name)
                if event is None:
                    payload = (
                        self.upstream.payload
                        if node.kind == "put" and self.upstream is not None
                        else None
                    )
                    event = self._transfer_event(node.target, node.kind, payload)
                    self._wait_events[node.name] = event
            elif node.kind == "running":
                event = self._running_event(node)
            else:
                event, stop = self._condition(node.target)
                stop_watching.append(stop)
            if node.name is not None:
                events[node.name] = event
            return event

        try:
            root_event = build(wait.root)
        except QueueFull as error:
            # A request branch met a full line: the branches already built are
            # cancelled, and the wait fails with SimPy's message.
            for stop in stop_watching:
                stop()
            self._cancel_wait_events()
            return str(error)
        # A single-branch wait is joined as `any_of([branch])`, so a leaf reads
        # `in joined.value` the same way at the top as inside a group.
        joined = root_event if wait.root.is_group else env.any_of([root_event])
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
        # Named nodes are recorded children before parents (the parse registers a
        # group after its children), so a group reads its children's recorded values.
        for name, node in wait.branches.items():
            event = events[name]
            if node.is_group:
                done = event.processed
                value = (
                    {
                        child.name: self.waits[child.name]["value"]
                        for child in node.children
                        if child.name is not None and self.waits[child.name]["done"]
                    }
                    if done
                    else None
                )
            elif node.kind in ("request", "get", "put"):
                done = (event in joined.value) if joined.ok else event.processed
                value = self._resolve_branch(node, event, done)
            elif node.kind == "running":
                # The branch event is an `AllOf` condition, so the wait's value holds
                # its leaves, not the condition itself; `processed` is the done flag,
                # as for a group node.
                done = event.processed
                value = self._running_value(node, done)
            else:
                done = (event in joined.value) if joined.ok else event.processed
                value = _branch_value(event) if done else None
            self.waits[name] = {
                "done": done,
                "ok": event.ok if done else None,
                "value": value,
            }
        # The wait resolved: a won request is now tracked in `_wait_held`, and losing
        # branches are cancelled, so the per-branch events need no more tracking.
        self._wait_events = {}
        return failure

    def _resolve_branch(self, node: Any, event: Any, done: bool) -> Any:
        """Settles a won or losing request, get or put wait branch, and returns the
        value `wait.<branch>` reads: a won request keeps its unit for the task to
        release at its end; a won get gives the item; a losing branch is cancelled,
        and a losing granted request gives its unit back."""
        if done:
            if node.kind == "request":
                self._wait_held.append(event)
                return event
            if node.kind == "get":
                return self._transfer_value(node.target, "get", event)
            return None
        # A losing branch leaves the queue or reservation behind nothing.
        event.cancel()
        if node.kind == "request" and event.triggered and event in event.resource.users:
            event.resource.release(event)
        return None

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
