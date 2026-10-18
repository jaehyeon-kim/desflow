"""The life of one task instance: what `@app.task` and a YAML task run.

`TaskSpec` holds what a task is, checked once when it is declared. `TaskRun` runs one
instance of it as a SimPy process, in this order: the balk check; `queued`; the
`wait`; the resource; the `get`; `started`; the user function; the service time;
the `put`; the `fail` check; the finished payload, or `failed`; the `signal`; `set`;
then `next`, or `on_fail`, or the end of the flow. The context keeps the decorator
and the checks made before the clock starts.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from desflow.core.expressions import Expression, Scope
from desflow.core.flow import Router, Transfer, Wait, make_scope
from desflow.models.flow import Entity, Upstream

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
    ):
        self.name = name
        self.service_id = service_id
        self.resource_id = resource_id
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
        self.queues = resource_id is not None or get is not None or wait is not None
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

    # ------------------------------------------------------------------ records

    def publish(self, value: Any) -> None:
        self.env.publish_event(self.entity.key, value, self.spec.name, self.task_id)

    def status(self, state: str, **extra: Any) -> Dict[str, Any]:
        value: Dict[str, Any] = {}
        if self.spec.service_id is not None:
            value["path_id"] = f"{self.context.sim_id}.service.{self.spec.service_id}"
        value["status"] = state
        value.update(extra)
        return value

    def scope(self, payload_upstream: Optional[Upstream] = None) -> Scope:
        """What the task's expressions read now."""
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
        """The task's process: everything from the balk check to the next task."""
        spec, env, stats = self.spec, self.env, self.env.stats
        if spec.balk_of is not None and spec.balk_of.evaluate(self.scope()):
            self.publish(self.status("balked"))
            stats.task_balked(spec.name, self.task_id)
            return None
        # The patience timer starts when the task joins its first queue and covers
        # every wait after it: the wait, the resource, then the get.
        deadline = None
        if spec.queues:
            self.publish(self.status("queued"))
            stats.task_queued(spec.name, self.task_id)
            if spec.patience is not None:
                deadline = env.timeout(float(self._patience()))

        failure: Any = None
        if spec.wait is not None:
            outcome = yield from self._wait(deadline)
            if outcome is _RENEGED:
                self._renege()
                return None
            failure = outcome

        payload: Any = None
        if failure is None:
            unit: Any = contextlib.nullcontext()
            if spec.resource_id is not None:
                holder = self.context._resources_map[spec.resource_id]
                unit = holder.request(self._rank())
            with unit as request:
                if request is not None:
                    if deadline is None:
                        yield request
                    else:
                        yield request | deadline
                        # The patience ran out before a unit was free: leave the
                        # queue (the request is cancelled on exit) and renege.
                        if not request.triggered:
                            self._renege()
                            return None
                if spec.get is not None:
                    got = yield from self._transfer(spec.get, "get", None, deadline)
                    if got is _RENEGED:
                        self._renege()
                        return None
                    self.item = got
                if spec.timed:
                    self.publish(self.status("started"))
                    stats.task_started(spec.name, self.task_id)
                try:
                    payload = spec.user_func(self.task_id, *self.args, **self._extra())
                except Exception as error:  # noqa: BLE001: a task's failure, not a crash
                    failure = f"{type(error).__name__}: {error}"
                if failure is None:
                    if spec.service_id is not None:
                        config = env.registry.get_config(
                            f"{self.context.sim_id}.service.{spec.service_id}"
                        )
                        yield env.timeout(self.context.sampler.sample(config))
                    if spec.put is not None:
                        yield from self._transfer(spec.put, "put", payload, None)
                    failure = self._failure(payload)
                if failure is None:
                    self.publish(payload)
                    stats.task_finished(spec.name, self.task_id)
                else:
                    self.publish(self.status("failed", reason=failure))
                    stats.task_failed(spec.name, self.task_id)
        else:
            self.publish(self.status("failed", reason=failure))
            stats.task_failed(spec.name, self.task_id)

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
            stats.flow_failed(self.entity.arrival)
            return None

        if spec.signal is not None:
            self.context._signals[spec.signal].fire(payload)
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
            stats.flow_finished(self.entity.arrival)
            return None
        return (yield from self._follow(following, handed))

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
        if deadline is None:
            yield event
        else:
            yield event | deadline
            if not event.triggered:
                event.cancel()
                return _RENEGED
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
                events[name] = self.context._start(
                    target,
                    self.entity,
                    self.upstream,
                    child=True,
                    args=self.args,
                    kwargs=self.kwargs,
                )
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
        except Exception as error:  # noqa: BLE001: a branch failed
            failure = getattr(error, "reason", None) or str(error)
        # Losing condition branches stop checking; losing child flows keep running,
        # as SimPy leaves processes running.
        for stop in stop_watching:
            stop()
        if failure is None and not joined.triggered:
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


def _branch_value(event: Any) -> Any:
    """A finished branch's value; a failed one's is its reason."""
    if event.ok:
        return event.value
    return getattr(event.value, "reason", str(event.value))
