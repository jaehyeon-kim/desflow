"""Built-in statistics: what a run measures about itself.

Every `DynamicRealtimeEnvironment` has one `Statistics` object, `env.stats`, which
collects nothing until `env.setup_stats` sets a level above `none`. Resources, stores
and containers add themselves when they are built and report every change, so the
time-weighted averages are integrated over simulation time at each change rather
than sampled. Tasks report when they are queued, started and finished: the builder
and YAML tasks do it for you, and low-level code calls the same three methods.

| Level | Adds |
|---|---|
| `minimal` | resource `capacity`, `in_use`, `queue_length`, `utilization`; store and container `capacity`, `level`, `getters_waiting`, `putters_waiting` |
| `basic` | task and flow `finished`; resource `utilization_avg`, `queue_length_avg`; store and container `level_avg` |
| `typical` | task `wait_mean`, `wait_max`, `service_mean`, `service_max` |
| `all` | task `wait_p50`, `wait_p95`, `wait_p99`, `service_p50`, `service_p95`, `service_p99` |

Each is published as telemetry named `<sim_id>.stats.<category>.<name>.<statistic>`,
where the category is `resources`, `stores`, `containers`, `tasks` or `flows`.
"""

from typing import Any, Dict, Hashable, List, Optional, Tuple

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

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.max = max(self.max, value)
        if self.values is not None:
            self.values.append(value)


class _Task:
    def __init__(self, keep: bool):
        self.finished = 0
        self.wait = _Observations(keep)
        self.service = _Observations(keep)


def _resource_readings(resource: Any) -> Dict[str, Any]:
    capacity = resource.capacity
    in_use = resource.in_use
    return {
        "capacity": capacity,
        "in_use": in_use,
        "queue_length": len(resource.queue.items),
        # Busy units, so a capacity lowered below them shows over 100 until they finish
        "utilization": in_use / capacity * 100 if capacity > 0 else 0.0,
    }


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
        self._objects: Dict[Tuple[str, str], Any] = {}
        self._averages: Dict[Tuple[str, str], Dict[str, _TimeAverage]] = {}
        self._tasks: Dict[str, _Task] = {}
        self._flows: Dict[str, int] = {}
        # (task, instance) to the times it was queued and started
        self._in_flight: Dict[Tuple[str, Hashable], List[Optional[float]]] = {}

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
        if self._rank >= _TYPICAL:
            self._in_flight[(task, instance)] = [self.env.now, None]

    def task_started(self, task: str, instance: Hashable) -> None:
        """
        Records that a task instance started work. One never queued waited 0.

        Args:
            task: The task's name.
            instance: Its instance id.
        """
        if self._rank < _TYPICAL:
            return
        now = self.env.now
        times = self._in_flight.setdefault((task, instance), [now, None])
        queued_at = times[0] if times[0] is not None else now
        times[1] = now
        self._task(task).wait.add(now - queued_at)

    def task_finished(self, task: str, instance: Hashable) -> None:
        """
        Records that a task instance finished, with its service time if it started.

        Args:
            task: The task's name.
            instance: Its instance id.
        """
        if self._rank < _BASIC:
            return
        stats = self._task(task)
        stats.finished += 1
        times = self._in_flight.pop((task, instance), None)
        if times is not None and times[1] is not None:
            stats.service.add(self.env.now - times[1])

    def flow_finished(self, flow: str) -> None:
        """
        Records that an entity left the model: its last task had no next task.

        Args:
            flow: The arrival that started the entity.
        """
        if self._rank >= _BASIC:
            self._flows[flow] = self._flows.get(flow, 0) + 1

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
        for name, task in self._tasks.items():
            base = f"{prefix}.tasks.{name}"
            values[f"{base}.finished"] = task.finished
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
        return values

    def publish(self) -> None:
        """Publishes every statistic as telemetry at the current simulation time."""
        for name, value in self.readings().items():
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
