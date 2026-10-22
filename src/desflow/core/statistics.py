"""Built-in statistics: what a run measures about itself.

Every `DynamicRealtimeEnvironment` has one `Statistics` object, `env.stats`, which
collects nothing until `env.setup_stats` sets a level above `none`. Resources, stores
and containers add themselves when they are built and report every change, so the
time-weighted averages are integrated over simulation time at each change rather
than sampled. Tasks report when they are queued, started and finished: the builder
and YAML tasks do it for you, and low-level code calls the same three methods.

| Level | Adds |
|---|---|
| `minimal` | resource `capacity`, `in_use`, `queue_length`, `utilization`, and `down` for one that breaks down; store and container `capacity`, `level`, `getters_waiting`, `putters_waiting`; task `in_progress` |
| `basic` | task and flow `finished`; flow `started` and `in_system`; resource `downtime` and `failures` for one that breaks down; task `balked`, `reneged`, `failed`, `interrupted`, `preempted` and `resumed`, and flow `failed`, once one happens; resource `utilization_avg`, `queue_length_avg`; store and container `level_avg` |
| `typical` | task `wait_mean`, `wait_max`, `service_mean`, `service_max` |
| `all` | task `wait_p50`, `wait_p95`, `wait_p99`, `service_p50`, `service_p95`, `service_p99` |

Each is published as telemetry named `<sim_id>.stats.<category>.<name>.<statistic>`,
where the category is `resources`, `stores`, `containers`, `tasks` or `flows`.
"""

from typing import Any, Dict, Hashable, List, Optional, Set, Tuple

import numpy as np

from desflow.config import DEFAULT_STATS_INTERVAL, STATS_LEVELS

_MINIMAL, _BASIC, _TYPICAL, _ALL = (
    STATS_LEVELS.index(level) for level in STATS_LEVELS[1:]
)
_PERCENTILES = (50, 95, 99)


class _TimeAverage:
    """A value integrated over simulation time, so its average holds at any moment."""

    def __init__(self, now: float, value: float):
        self.start = self.last = now
        self.value = value
        self.area = 0.0

    def update(self, now: float, value: float) -> None:
        self.area += self.value * (now - self.last)
        self.last = now
        self.value = value

    def average(self, now: float) -> float:
        span = now - self.start
        if span <= 0:
            return float(self.value)
        return (self.area + self.value * (now - self.last)) / span

    def reset(self, now: float) -> None:
        self.start = self.last = now
        self.area = 0.0


class _Observations:
    """Durations of one kind for one task: a count, a total, a maximum, and with
    `keep` every value, for exact percentiles."""

    def __init__(self, keep: bool):
        self.count = 0
        self.total = 0.0
        self.max = 0.0
        self.values: Optional[List[float]] = [] if keep else None

    def add(self, value: float) -> int:
        """Adds one observation and returns its position, for `extend`."""
        self.count += 1
        self.total += value
        self.max = max(self.max, value)
        if self.values is not None:
            self.values.append(value)
            return len(self.values) - 1
        return self.count - 1

    def extend(self, position: int, before: float, more: float) -> None:
        """Adds `more` to an observation that was `before`, without counting a new
        one, as an interrupted task's second wait adds to its first."""
        self.total += more
        self.max = max(self.max, before + more)
        if self.values is not None:
            self.values[position] = before + more


class _InFlight:
    """One task instance between `queued` and its end: when it last queued and
    started, its wait so far and the time it has been served."""

    def __init__(self, queued: float):
        self.queued: Optional[float] = queued
        self.started: Optional[float] = None
        self.waited = 0.0
        self.served = 0.0
        # The task statistics and position of its wait observation
        self.observation: Optional[Tuple[Any, int]] = None


class _Task:
    def __init__(self, keep: bool):
        self.finished = 0
        self.balked = 0
        self.reneged = 0
        self.failed = 0
        self.interrupted = 0
        self.preempted = 0
        self.resumed = 0
        self.wait = _Observations(keep)
        self.service = _Observations(keep)


def _resource_readings(resource: Any) -> Dict[str, Any]:
    capacity = resource.capacity
    in_use = resource.in_use
    readings = {
        "capacity": capacity,
        "in_use": in_use,
        "queue_length": len(resource.queue),
        # Busy units, so a capacity lowered below them shows over 100 until they finish
        "utilization": in_use / capacity * 100 if capacity > 0 else 0.0,
    }
    if getattr(resource, "has_breakdowns", False):
        readings["down"] = 1 if resource.down else 0
    return readings


def _holder_readings(holder: Any) -> Dict[str, Any]:
    return {
        "capacity": holder.capacity,
        "level": holder.level,
        "getters_waiting": holder.getters_waiting,
        "putters_waiting": holder.putters_waiting,
    }


def _averaged(category: str, obj: Any) -> Dict[str, float]:
    """The readings that get a time average, by the name of the average."""
    if category == "resources":
        readings = _resource_readings(obj)
        return {
            "utilization_avg": readings["utilization"],
            "queue_length_avg": readings["queue_length"],
        }
    return {"level_avg": obj.level}


class Statistics:
    """
    Counters and time averages a run keeps about its resources, tasks and flows.

    Attributes:
        env: The environment whose clock the statistics read.
        sim_id (Optional[str]): The first part of every published name.
        level (str): `none`, `minimal`, `basic`, `typical` or `all`.
        interval (float): Simulation seconds between two publications.
    """

    def __init__(self, env: Any):
        """
        Creates statistics that collect nothing until `configure` sets a level.

        Args:
            env: The environment; its `now` is the clock, and `publish` sends through
                its `publish_telemetry`.
        """
        self.env = env
        self.sim_id: Optional[str] = None
        self.level = "none"
        self.interval = DEFAULT_STATS_INTERVAL
        self._rank = 0
        # The time of the last publication and the values published at it, so a
        # second publication at the same time publishes only what changed
        self._published_at: Optional[float] = None
        self._published: Dict[str, Any] = {}
        self._objects: Dict[Tuple[str, str], Any] = {}
        self._averages: Dict[Tuple[str, str], Dict[str, _TimeAverage]] = {}
        self._tasks: Dict[str, _Task] = {}
        self._flows: Dict[str, int] = {}
        self._failed_flows: Dict[str, int] = {}
        # Flows started since the last reset, and entities in the model now
        self._started_flows: Dict[str, int] = {}
        self._in_system: Dict[str, int] = {}
        # Task instances queued or started and not yet ended, and how many per task
        self._active: Set[Tuple[str, Hashable]] = set()
        self._in_progress: Dict[str, int] = {}
        # (task, instance) to the times it was queued and started
        self._in_flight: Dict[Tuple[str, Hashable], _InFlight] = {}

    def configure(
        self, sim_id: str, level: str, interval: float = DEFAULT_STATS_INTERVAL
    ) -> None:
        """
        Sets the names and the level. `env.setup_stats` calls it.

        Args:
            sim_id: The first part of every published name.
            level: `none`, `minimal`, `basic`, `typical` or `all`.
            interval: Simulation seconds between two publications.

        Raises:
            ValueError: If the level is unknown or the interval is not positive.
        """
        if level not in STATS_LEVELS:
            raise ValueError(
                f"stats level must be one of {', '.join(STATS_LEVELS)}, got '{level}'"
            )
        if not interval > 0:
            raise ValueError(f"stats interval must be greater than 0, got {interval}")
        self.sim_id = sim_id
        self.level = level
        self.interval = float(interval)
        self._rank = STATS_LEVELS.index(level)
        for key, obj in self._objects.items():
            self._start_averages(key, obj)

    @property
    def enabled(self) -> bool:
        """True when the level is above `none`."""
        return self._rank >= _MINIMAL

    def add(self, category: str, name: str, obj: Any) -> None:
        """
        Adds a resource, store or container. Each one calls this when it is built.

        Args:
            category: `resources`, `stores` or `containers`.
            name: Its name.
            obj: The object, read for its readings.
        """
        self._objects[(category, name)] = obj
        self._start_averages((category, name), obj)

    def changed(self, category: str, name: str) -> None:
        """
        Integrates an object's averages up to now, then takes its new readings.

        Args:
            category: `resources`, `stores` or `containers`.
            name: Its name.
        """
        averages = self._averages.get((category, name))
        if averages is None:
            return
        now = self.env.now
        for stat, value in _averaged(category, self._objects[(category, name)]).items():
            averages[stat].update(now, value)

    def task_queued(self, task: str, instance: Hashable) -> None:
        """
        Records that a task instance started waiting for what it needs.

        Args:
            task: The task's name.
            instance: Its instance id.
        """
        self._enter(task, instance)
        if self._rank >= _TYPICAL:
            self._in_flight[(task, instance)] = _InFlight(self.env.now)

    def _enter(self, task: str, instance: Hashable) -> None:
        """Counts an instance as in progress, once, at `minimal` and above."""
        if self._rank >= _MINIMAL and (task, instance) not in self._active:
            self._active.add((task, instance))
            self._in_progress[task] = self._in_progress.get(task, 0) + 1

    def _leave(self, task: str, instance: Hashable) -> None:
        """Stops counting an instance as in progress, at `minimal` and above."""
        # Below minimal nothing was counted, and the id may not be hashable
        if self._rank < _MINIMAL:
            return
        if (task, instance) in self._active:
            self._active.discard((task, instance))
            self._in_progress[task] -= 1

    def task_started(self, task: str, instance: Hashable) -> None:
        """
        Records that a task instance started work. One never queued waited 0.

        Args:
            task: The task's name.
            instance: Its instance id.
        """
        self._enter(task, instance)
        if self._rank < _TYPICAL:
            return
        now = self.env.now
        flight = self._in_flight.setdefault((task, instance), _InFlight(now))
        queued_at = flight.queued if flight.queued is not None else now
        flight.waited = now - queued_at
        flight.started, flight.queued = now, None
        stats = self._task(task)
        flight.observation = (stats, stats.wait.add(flight.waited))

    def task_interrupted(
        self, task: str, instance: Hashable, preempted: bool = False
    ) -> None:
        """
        Records that a task instance was interrupted or preempted, and waits again.

        The time it was served so far is kept, and its wait starts again from now.
        It is counted at `basic`, as `interrupted` or as `preempted`.

        Args:
            task: The task's name.
            instance: Its instance id.
            preempted: True when a higher-priority request took its unit.
        """
        if self._rank >= _BASIC:
            stats = self._task(task)
            if preempted:
                stats.preempted += 1
            else:
                stats.interrupted += 1
        flight = (
            self._in_flight.get((task, instance)) if self._rank >= _TYPICAL else None
        )
        if flight is None:
            return
        now = self.env.now
        if flight.started is not None:
            flight.served += now - flight.started
            flight.started = None
        if flight.queued is None:
            flight.queued = now

    def task_dropped(self, task: str, instance: Hashable) -> None:
        """Clears the waiting record of an instance that an interrupt ended.

        `task_interrupted` has counted it already; this adds no wait or service time.

        Args:
            task: The task's name.
            instance: The instance's id.
        """
        if self._rank >= _TYPICAL:
            self._in_flight.pop((task, instance), None)
        self._leave(task, instance)

    def task_resumed(self, task: str, instance: Hashable) -> None:
        """
        Records that an interrupted task instance has its units again and is served.

        Its wait since the interruption is added to its first wait, so the wait of
        the instance sums every wait, and it is counted at `basic` as `resumed`.

        Args:
            task: The task's name.
            instance: Its instance id.
        """
        if self._rank >= _BASIC:
            self._task(task).resumed += 1
        flight = (
            self._in_flight.get((task, instance)) if self._rank >= _TYPICAL else None
        )
        if flight is None:
            return
        now = self.env.now
        more = now - flight.queued if flight.queued is not None else 0.0
        if flight.observation is not None:
            stats, position = flight.observation
            # A reset since the first wait started new statistics; leave them
            if stats is self._tasks.get(task):
                stats.wait.extend(position, flight.waited, more)
        flight.waited += more
        flight.started, flight.queued = now, None

    def task_finished(self, task: str, instance: Hashable) -> None:
        """
        Records that a task instance finished, with the time it was served.

        Args:
            task: The task's name.
            instance: Its instance id.
        """
        self._leave(task, instance)
        if self._rank < _BASIC:
            return
        stats = self._task(task)
        stats.finished += 1
        flight = (
            self._in_flight.pop((task, instance), None)
            if self._rank >= _TYPICAL
            else None
        )
        if flight is not None and (flight.started is not None or flight.served):
            served = flight.served
            if flight.started is not None:
                served += self.env.now - flight.started
            stats.service.add(served)

    def task_balked(self, task: str, instance: Hashable) -> None:
        """
        Records that a task instance refused to join its queue.

        It adds no wait; one that had been counted as queued is cleared, as a
        renege clears it. It is counted at `basic`.

        Args:
            task: The task's name.
            instance: Its instance id.
        """
        if self._rank >= _TYPICAL:
            self._in_flight.pop((task, instance), None)
        self._leave(task, instance)
        if self._rank >= _BASIC:
            self._task(task).balked += 1

    def task_reneged(self, task: str, instance: Hashable) -> None:
        """
        Records that a task instance left its queue before it was served.

        Its waiting record is cleared, so a long run that reneges does not keep it;
        it never started, so it adds no wait or service time. It is counted at
        `basic`.

        Args:
            task: The task's name.
            instance: Its instance id.
        """
        if self._rank >= _TYPICAL:
            self._in_flight.pop((task, instance), None)
        self._leave(task, instance)
        if self._rank >= _BASIC:
            self._task(task).reneged += 1

    def task_failed(self, task: str, instance: Hashable) -> None:
        """
        Records that a task instance failed: its `fail` held, its function raised,
        or its wait failed.

        Its waiting record is cleared, and it adds no service time. It is counted at
        `basic`.

        Args:
            task: The task's name.
            instance: Its instance id.
        """
        if self._rank >= _TYPICAL:
            self._in_flight.pop((task, instance), None)
        self._leave(task, instance)
        if self._rank >= _BASIC:
            self._task(task).failed += 1

    def flow_failed(self, flow: str) -> None:
        """
        Records that an entity's flow ended in a failure no `on_fail` handled.

        Args:
            flow: The arrival that started the entity.
        """
        if self._rank >= _BASIC:
            self._failed_flows[flow] = self._failed_flows.get(flow, 0) + 1
            if flow in self._in_system:
                self._in_system[flow] -= 1

    def flow_started(self, flow: str) -> None:
        """
        Records that an entity entered the model: its first task instance began.

        Args:
            flow: The arrival that started the entity.
        """
        if self._rank >= _BASIC:
            self._started_flows[flow] = self._started_flows.get(flow, 0) + 1
            self._in_system[flow] = self._in_system.get(flow, 0) + 1

    def in_system(self) -> int:
        """
        Returns how many entities of every flow are in the model now.

        It counts only at `basic` or above, where `flow_started` records them.

        Returns:
            int: The entities started and not yet finished, failed or ended.
        """
        return sum(self._in_system.values())

    def flow_ended(self, flow: str) -> None:
        """
        Records that an entity left the model without finishing or failing: it
        balked, reneged or was abandoned after an interrupt.

        Args:
            flow: The arrival that started the entity.
        """
        if self._rank >= _BASIC and flow in self._in_system:
            self._in_system[flow] -= 1

    def flow_finished(self, flow: str) -> None:
        """
        Records that an entity left the model: its last task had no next task.

        Args:
            flow: The arrival that started the entity.
        """
        if self._rank >= _BASIC:
            self._flows[flow] = self._flows.get(flow, 0) + 1
            if flow in self._in_system:
                self._in_system[flow] -= 1

    def reset(self) -> None:
        """
        Starts every statistic again from now, as a warm-up does at its end.

        Instances already queued or started keep their times, so one that started
        before the reset and finishes after it is measured in full.
        """
        now = self.env.now
        for averages in self._averages.values():
            for average in averages.values():
                average.reset(now)
        keep = self._rank >= _ALL
        self._tasks = {name: _Task(keep) for name in self._tasks}
        self._flows = {name: 0 for name in self._flows}
        self._failed_flows = {name: 0 for name in self._failed_flows}
        self._started_flows = {name: 0 for name in self._started_flows}

    def readings(self) -> Dict[str, Any]:
        """
        Returns every statistic the level publishes, by its published name.

        Returns:
            Dict[str, Any]: Name to value, in a fixed order: resources, stores and
                containers in the order they were built, then tasks and flows in the
                order they were first seen. A task statistic appears once it has an
                observation.
        """
        values: Dict[str, Any] = {}
        if not self.enabled:
            return values
        now = self.env.now
        prefix = f"{self.sim_id}.stats"
        for (category, name), obj in self._objects.items():
            base = f"{prefix}.{category}.{name}"
            current = (
                _resource_readings(obj)
                if category == "resources"
                else _holder_readings(obj)
            )
            for stat, value in current.items():
                values[f"{base}.{stat}"] = value
            for stat, average in self._averages.get((category, name), {}).items():
                values[f"{base}.{stat}"] = average.average(now)
            if self._rank >= _BASIC and getattr(obj, "has_breakdowns", False):
                values[f"{base}.downtime"] = obj.downtime
                values[f"{base}.failures"] = obj.failures
        for name, count in self._in_progress.items():
            values[f"{prefix}.tasks.{name}.in_progress"] = count
        for name, task in self._tasks.items():
            base = f"{prefix}.tasks.{name}"
            values[f"{base}.finished"] = task.finished
            # Published once one happens, so a task without patience or a balk
            # condition publishes neither
            for outcome in (
                "balked",
                "reneged",
                "failed",
                "interrupted",
                "preempted",
                "resumed",
            ):
                if getattr(task, outcome):
                    values[f"{base}.{outcome}"] = getattr(task, outcome)
            for kind, observed in (("wait", task.wait), ("service", task.service)):
                if self._rank < _TYPICAL or not observed.count:
                    continue
                values[f"{base}.{kind}_mean"] = observed.total / observed.count
                values[f"{base}.{kind}_max"] = observed.max
                if observed.values is not None:
                    points = np.percentile(observed.values, _PERCENTILES)
                    for percent, point in zip(_PERCENTILES, points):
                        values[f"{base}.{kind}_p{percent}"] = float(point)
        for flow, finished in self._flows.items():
            values[f"{prefix}.flows.{flow}.finished"] = finished
        for flow, started in self._started_flows.items():
            values[f"{prefix}.flows.{flow}.started"] = started
            values[f"{prefix}.flows.{flow}.in_system"] = self._in_system[flow]
        for flow, failed in self._failed_flows.items():
            if failed:
                values[f"{prefix}.flows.{flow}.failed"] = failed
        return values

    def publish(self) -> None:
        """
        Publishes every statistic as telemetry at the current simulation time.

        When the last publication was at this same time, publishes only the
        statistics whose value changed since, so a run that ends at an interval time
        repeats no value and its last values are the state at the end.
        """
        readings = self.readings()
        if self._published_at != self.env.now:
            self._published_at = self.env.now
            self._published = {}
        for name, value in readings.items():
            if name in self._published and self._published[name] == value:
                continue
            self._published[name] = value
            self.env.publish_telemetry(name, value)

    def _task(self, task: str) -> _Task:
        stats = self._tasks.get(task)
        if stats is None:
            stats = self._tasks[task] = _Task(self._rank >= _ALL)
        return stats

    def _start_averages(self, key: Tuple[str, str], obj: Any) -> None:
        if self._rank < _BASIC:
            self._averages.pop(key, None)
            return
        now = self.env.now
        self._averages[key] = {
            stat: _TimeAverage(now, value)
            for stat, value in _averaged(key[0], obj).items()
        }
