import functools
import inspect
import itertools
import logging
import math
import secrets
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
)

import simpy
from simpy.core import StopSimulation

from desflow.config import (
    DEFAULT_QUEUE_ORDER,
    INTERRUPT_POLICIES,
    DEFAULT_STATS_INTERVAL,
    DEFAULT_STATS_LEVEL,
    LABELS,
    QUEUE_ORDERS,
    STATS_LEVELS,
)
from desflow.core.environment import DynamicRealtimeEnvironment, resolve_zone
from desflow.core.expressions import Expression
from desflow.core.flow import NextSpec, Router, Transfer, Wait, make_scope
from desflow.core.signals import Signal
from desflow.core.task import TaskRun, TaskSpec, check_acquisition_order
from desflow.core.registry import SimulationRegistry
from desflow.core.sampler import Sampler
from desflow.models.flow import Entity, Upstream
from desflow.core.breakdowns import run_breakdowns
from desflow.core.replay import UNITS, Row, read_trace
from desflow.core.scenario import apply_step, check_step, run_scenario
from desflow.models.params import (
    BreakdownConfig,
    CapacityConfig,
    ContainerConfig,
    DistributionConfig,
    SimParameter,
)
from desflow.resources.container import (
    ContainerBase,
    DynamicContainer,
    check_container_type,
)
from desflow.resources.resource import (
    DynamicResource,
    ResourceBase,
    check_resource_type,
)
from desflow.resources.store import DynamicStore, StoreBase, check_store_type
from desflow.utils import suggestion, time_to_seconds

if TYPE_CHECKING:
    # build.py imports this module, so the import is for type checkers only
    from desflow.blueprint.build import Preview
    from desflow.explain.models import Explanation

logger = logging.getLogger(__name__)


def _within_max(
    kind: str,
    name: str,
    current_cap: Optional[Union[int, float]],
    max_cap: Optional[Union[int, float]],
) -> Optional[Union[int, float]]:
    """A `current_cap` clamped to `[0, max_cap]`, with a warning when it changes, as
    the registry clamps a live update; the run and `analyze()` then use the same
    capacity."""
    if current_cap is None or max_cap is None:
        return current_cap
    clamped = type(current_cap)(min(max(current_cap, 0), max_cap))
    if clamped != current_cap:
        logger.warning(
            f"current_cap {current_cap} of {kind} '{name}' is outside "
            f"[0, max_cap {max_cap}]. Using {clamped}."
        )
    return clamped


class SimulationContext:
    """
    Builder and Orchestrator for Event-Driven Digital Twins.

    The `SimulationContext` implements the Builder Pattern to construct the
    foundational architecture of a Digital Twin. It acts as the central control
    plane, aggregating discrete machine capacities, continuous physical states
    (wear and tear), and statistical distributions into a unified `SimParameter`
    object.

    Architectural Guarantees:
    1.  **Immutability During Configuration**: The underlying `DynamicRealtimeEnvironment`
        and its background asynchronous I/O threads are completely deferred until
        `.run()` is explicitly invoked. This prevents partial state leaks or
        unintended clock starts.
    2.  **One registry**: All registered configurations are flattened
        and managed by the internal `SimulationRegistry`, allowing external
        Kafka Admin commands to mutate these states safely during runtime.
    3.  **Deterministic Stochasticity**: The `Sampler` gives each distribution a
        random sequence of its own, made from `random_seed`, so a seeded run repeats
        exactly, and adding a distribution leaves the others' draws unchanged. A run
        without `random_seed` draws a seed and logs it, so it can be repeated.

    Attributes:
        sim_id (str): The unique namespace for this simulation instance (e.g., 'HotRolling').
        factor (float): The real-time synchronization multiplier. A factor of 1.0
            syncs strictly with wall-clock time, while 0.0 executes instantly in a
            fast-forward loop (ideal for historical data processing).
        go_live_at (Optional[datetime]): Logical instant at which `factor` is replaced
            by real-time pacing, so one run can backfill history and then tail live.
        warmup (Optional[float]): The end of the warm-up, in simulation seconds.
        random_seed (Optional[int]): The run's seed. When None, `run()` draws one,
            logs it and uses it; read it afterwards from `sampler.seed`.
        sampler (Optional[Sampler]): The centralized Random Number Generator engine.
            Accessible to user-defined raw generators only after `.run()` is called.
    """

    def __init__(
        self,
        sim_id: str,
        factor: float = 1.0,
        random_seed: Optional[int] = None,
        logical_start_time: Optional[datetime] = None,
        go_live_at: Optional[datetime] = None,
        warmup: Union[float, str, None] = None,
        initial_time: Union[float, str] = 0.0,
        lag_interval: Union[float, str, None] = None,
        timezone: Optional[str] = None,
        strict: bool = False,
        environment: Optional[Type[DynamicRealtimeEnvironment]] = None,
    ):
        """
        Initializes the context builder with a designated namespace and temporal factor.

        Args:
            sim_id: The unique prefix for this group of parameters. All generated
                telemetry and event paths will be prefixed with this ID.
            factor: The real-time synchronization factor. Defaults to 1.0.
            random_seed: Optional seed for deterministic execution.
            logical_start_time: Optional override for the environment's base clock,
                forwarded to `DynamicRealtimeEnvironment`. Crucial for historical
                backfilling (e.g., generating data from last week).
            go_live_at: Optional logical instant at which the run stops using `factor`
                and switches to real time, forwarded to `DynamicRealtimeEnvironment`.
                With `factor=0.0` and a backdated `logical_start_time`, the history up
                to this instant is generated as fast as the machine allows and the rest
                arrives in real time, so a backfill and a live tail take one run.
            warmup: The end of the warm-up, in simulation seconds or as a duration
                such as `"10 min"`. No sink receives a record stamped before it, and
                the built-in statistics are reset at that time, so they measure the
                steady state only.
            initial_time: The simulation clock's starting value, in seconds or as a
                duration such as `"8 h"`, so event times, `sim_ts` and `until` are
                readings of a clock that starts there. Defaults to 0.
            lag_interval: Simulation seconds, or a duration such as `"10 s"`, between
                `system.simulation.lag_seconds` records, which measure how far the
                run is behind the wall clock. By default it is off at `factor` 0,
                where lag means nothing, and every simulation second otherwise; a
                run that goes live starts it then. 0 turns it off.
            timezone: An IANA time zone name, such as `"Australia/Melbourne"`, that
                record timestamps are written in, with their offset. By default the
                zone of `logical_start_time`, or UTC. A `logical_start_time` or
                `go_live_at` without an offset is read in it.
            strict: When True a paced step raises `RuntimeError` if the run is more
                than `factor` real seconds behind the wall clock, as SimPy's
                `RealtimeEnvironment` does. It is never checked at `factor` 0. False
                by default, unlike the low-level environment, because a live run
                whose sink blocks falls behind and should go on.
            environment: A subclass of `DynamicRealtimeEnvironment` that the run is
                built on, for a model that overrides `step`, `schedule` or `__init__`.
                A subclass that overrides `__init__` takes `**kwargs` and passes
                them on. One that overrides `step` or `schedule` calls `super()`.
                By default `DynamicRealtimeEnvironment`.

        Raises:
            ValueError: If `warmup` or `lag_interval` is negative or not a duration,
                `timezone` is not an IANA time zone name, or `environment` is not a
                subclass of `DynamicRealtimeEnvironment`.
        """
        if environment is not None and not (
            isinstance(environment, type)
            and issubclass(environment, DynamicRealtimeEnvironment)
        ):
            raise ValueError(
                "environment must be a subclass of DynamicRealtimeEnvironment, "
                f"got {environment!r}"
            )
        self.sim_id = sim_id
        self.factor = factor
        self.strict = strict
        self.environment: Type[DynamicRealtimeEnvironment] = (
            environment or DynamicRealtimeEnvironment
        )
        self.random_seed = random_seed
        self.logical_start_time = logical_start_time
        self.go_live_at = go_live_at
        self.initial_time = _seconds(initial_time)
        self.warmup = None if warmup is None else _seconds(warmup)
        if self.warmup is not None and self.warmup < 0:
            raise ValueError(f"warmup must be 0 or more, got {warmup}")
        resolve_zone(timezone)  # an unknown name is refused here, not at run()
        self.timezone = timezone
        self.lag_interval = None if lag_interval is None else _seconds(lag_interval)
        if self.lag_interval is not None and self.lag_interval < 0:
            raise ValueError(f"lag_interval must be 0 or more, got {lag_interval}")

        # Builder State (Pre-Compilation)
        self._ingress_providers: List[Any] = []
        self._egress_providers: List[Any] = []
        # Aligned with _egress_providers by position. None means the provider
        # receives every record.
        self._egress_predicates: List[Optional[Callable[[dict], bool]]] = []
        # Aligned with _egress_providers by position. None falls back to the
        # context default set by with_batching.
        self._egress_batch_sizes: List[Optional[int]] = []
        self._egress_flush_intervals: List[Optional[float]] = []
        self._batch_size = 500
        self._flush_interval = 1.0
        self._max_queued_batches = 2000
        self._drain_stall_seconds = 30.0

        # Aligned with _egress_providers by position.
        self._egress_labels: List[bool] = []
        self._stats_level = DEFAULT_STATS_LEVEL
        self._stats_interval = DEFAULT_STATS_INTERVAL

        self._resources_config: Dict[str, CapacityConfig] = {}
        self._resource_queues: Dict[str, str] = {}
        self._resource_preempt: Dict[str, bool] = {}
        self._resource_queue_limits: Dict[str, Optional[int]] = {}
        self._resource_types: Dict[str, Optional[Type[ResourceBase]]] = {}
        self._breakdowns_config: Dict[str, BreakdownConfig] = {}
        self._scenario_steps: List[Dict[str, Any]] = []
        # Replayed arrivals: how to read each, and the rows left once read
        self._replays: Dict[str, Dict[str, Any]] = {}
        self._replay_rows: Dict[str, List[Row]] = {}
        self._replays_done: set = set()
        self._containers_config: Dict[str, CapacityConfig] = {}
        self._stores_config: Dict[str, CapacityConfig] = {}
        self._store_priority: Dict[str, bool] = {}
        self._store_types: Dict[str, Optional[Type[StoreBase]]] = {}
        self._container_types: Dict[str, Optional[Type[ContainerBase]]] = {}
        self._store_items: Dict[str, List[Any]] = {}
        self._services_config: Dict[str, DistributionConfig] = {}
        self._arrivals_config: Dict[str, DistributionConfig] = {}
        # Per arrival: the attributes each entity starts with, and the key function
        self._arrival_entities: Dict[
            str, Tuple[Dict[str, Any], Optional[Callable[[int], str]]]
        ] = {}
        # Per arrival given `spawn`: the task each of its entities starts
        self._arrival_spawns: Dict[str, str] = {}
        self._variables_config: Dict[str, Any] = {}
        # Tasks by name, with what run() checks before the clock starts
        self._tasks: Dict[str, Callable] = {}
        self._task_specs: Dict[str, TaskSpec] = {}
        self._signal_names: List[str] = []
        self._signal_once: Dict[str, bool] = {}
        # Each signal's `on:` actions, in normalized form, run when it fires
        self._signal_on: Dict[str, List[Dict[str, Any]]] = {}
        self._signals: Dict[str, Signal] = {}
        # Tasks whose instances need an end event, filled by _check_tasks: those a
        # `running` branch or `subscribe` names (_tracked), those a `fire from`
        # copies (_copied), and every task with `on:` actions (_ended, the union)
        self._tracked: set = set()
        self._copied: set = set()
        self._ended: set = set()
        # Task instances running now, by task, which interrupt() reaches
        self._running: Dict[str, List[TaskRun]] = {}
        # The positional arguments a YAML task's function takes after its id
        self._task_args: Tuple[Any, ...] = ()

        # Runtime State (Post-Compilation)
        self._env: Optional[DynamicRealtimeEnvironment] = None
        # True once start() has built and started everything, so a caller can tell
        # a value refused before the run from an error inside it
        self.started = False
        self.sampler: Optional[Sampler] = None
        self._resources_map: Dict[str, ResourceBase] = {}
        self._stores_map: Dict[str, DynamicStore] = {}
        self._containers_map: Dict[str, DynamicContainer] = {}
        self._entity_counters: Dict[str, "itertools.count[int]"] = {}
        self._instance_counters: Dict[str, "itertools.count[int]"] = {}
        self._startup_loops: List[Tuple[Callable, Optional[str]]] = []
        # The functions add_process started, which explain shows as opaque nodes
        self._processes: List[Callable] = []
        # What builds this model again, a blueprint path or a function, set by
        # from_yaml and load_context, so analyze(verify=True) can run a copy
        self._source: Any = None

        # Set by from_yaml from the blueprint's `run` section.
        self._default_until: Any = None
        self._default_stop: Any = None
        self._default_drain: Any = None
        self._default_unhandled: str = "continue"
        self._on_unhandled_failure: str = "continue"
        # The run's end, entities in the model, and the drain state, set by start()
        self._until: Any = None
        self._end: Optional[simpy.Event] = None
        self._end_done = False
        # The internal event at `until` that advance() stops at, made on first use
        self._limit_event: Optional[simpy.Event] = None
        self._closed = True
        self._in_system = 0
        self._draining = False
        self._finish_run: Optional[Callable[..., None]] = None
        self._arrival_processes: List[simpy.Process] = []
        self._before_run: List[Callable[[], Any]] = []
        # How the log tells a run without a seed to repeat itself, with `{seed}`
        self._repeat_hint: Optional[str] = None
        # Set by from_yaml for `desflow run --sample`: the events a preview prints
        # before it ends the run, and how many it has printed
        self._sample_limit: Optional[int] = None
        self._sample_seen = 0

    @classmethod
    def from_yaml(
        cls, path: Union[str, Path], preview: Optional["Preview"] = None
    ) -> "SimulationContext":
        """
        Builds a context from a YAML blueprint file.

        The blueprint is validated before anything is built, and every error names
        the file and line. The context is built through the same builder methods a
        Python script calls, so the result is the same simulation.

        Args:
            path: The blueprint file.
            preview: The preview `desflow run --sample` and `--stdout` make. Every
                egress entry is replaced by a JSON console on stdout and
                `run.before` is not called, because it prepares sinks the preview
                does not use. A sample also leaves out the ingress, and runs
                unpaced from `logical_start_time`, with `go_live_at` cleared, until
                it has printed that many events.

        Returns:
            SimulationContext: The built context. Its `run()` calls the blueprint's
                `run.before` functions first, and uses `run.until`, `run.stop` and
                `run.drain` for whichever of `until`, `stop` and `drain` it is not
                passed.

        Raises:
            BlueprintError: If the blueprint is invalid.

        Example:
            ```python
            app = SimulationContext.from_yaml("examples/yaml/local.yaml")
            app.run()
            ```
        """
        from desflow.blueprint.build import build

        context, run = build(path, preview)
        context._default_until = run.until
        context._default_stop = run.stop
        context._default_drain = run.drain_setting()
        context._default_unhandled = run.on_unhandled_failure
        if preview is None:
            context._before_run = list(run.before)
        elif run.before:
            logger.warning("run.before skipped: preview runs use no real sinks")
        if preview is not None and preview.sample is not None:
            context.factor = 0.0
            context.go_live_at = None
            context._sample_limit = preview.sample
        context._repeat_hint = f"Repeat it with: desflow run {path} --seed {{seed}}"
        context._source = Path(path)
        return context

    # ==========================================
    # BUILDER METHODS (Fluent API)
    # ==========================================

    def add_ingress(self, provider: Any) -> "SimulationContext":
        """
        Registers an asynchronous ingress provider to listen for external mutations.

        Args:
            provider: An initialized connector subclassing `BaseIngress`
                (e.g., `KafkaIngress`).

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._ingress_providers.append(provider)
        return self

    def add_egress(
        self,
        provider: Any,
        when: Union[Callable[[dict], bool], str, None] = None,
        batch_size: Optional[int] = None,
        flush_interval: Optional[float] = None,
    ) -> "SimulationContext":
        """
        Registers an asynchronous egress provider for data exfiltration.

        Every registered provider receives every record, so attaching a stream sink and
        a lake sink writes the same dataset to both in one pass. Pass `when` to send
        only some records to a provider, which reads the same way as the `path_router`
        that `ParquetStorageEgress` already accepts.

        Args:
            provider: An initialized connector subclassing `BaseEgress`
                (e.g., `KafkaEgress` or `ParquetStorageEgress`).
            when: Optional predicate taking one record and returning True to send it to
                this provider. None sends every record, which is the usual case.
                `"labels"` makes it a labels sink: it receives the ground-truth
                records (`env.publish_label`) and nothing else, and no other sink
                receives them.
            batch_size: Records this provider buffers before flushing. None uses the
                context default from `with_batching`. A stream sink wants a small
                value to keep latency down; a file or lake sink wants a large one,
                because the batch is the file and, for Iceberg, the commit.
            flush_interval: Simulation seconds this provider waits before forcing a
                flush. None uses the context default. Ignored when `factor` is 0,
                because simulation time is then detached from the wall clock.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `when` is a string other than `"labels"`.

        Each provider has its own buffer and its own queue of batches. A record that
        several providers take is stored once, and stays in memory until the last
        provider that holds it has written it.

        Example:
            ```python
            # Same dataset to both sinks, in one run
            app.add_egress(kafka).add_egress(parquet)

            # Or split it: hot tail to Kafka, cold history to Parquet
            app.add_egress(kafka, when=lambda r: r["timestamp"] >= hot_from)
            app.add_egress(parquet, when=lambda r: r["timestamp"] < hot_from)

            # Different cadences: Kafka for latency, Parquet for file size
            app.add_egress(kafka, batch_size=500, flush_interval=1.0)
            app.add_egress(parquet, batch_size=200_000, flush_interval=300)
            ```
        """
        if isinstance(when, str) and when != LABELS:
            raise ValueError(f"when takes a function or '{LABELS}', got '{when}'")
        self._egress_providers.append(provider)
        self._egress_predicates.append(None if isinstance(when, str) else when)
        self._egress_labels.append(when == LABELS)
        self._egress_batch_sizes.append(batch_size)
        self._egress_flush_intervals.append(flush_interval)
        return self

    def with_stats(
        self, level: str = "basic", interval: float = DEFAULT_STATS_INTERVAL
    ) -> "SimulationContext":
        """
        Switches on the built-in statistics, published as telemetry.

        Each level publishes what the levels before it publish, and more: `minimal`
        the readings of every resource, store and container right now; `basic`
        finished counts and time-weighted averages; `typical` the mean and maximum
        wait and service time of each task; `all` their 50th, 95th and 99th
        percentiles. Names look like `<sim_id>.stats.tasks.<task>.wait_mean`.

        Args:
            level: `none`, `minimal`, `basic`, `typical` or `all`.
            interval: Simulation seconds between publications; they are also
                published once at the end of the run, except the values unchanged
                since a reading at that same time. Defaults to 10.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If the level is unknown or the interval is not positive.
        """
        if level not in STATS_LEVELS:
            raise ValueError(
                f"stats level must be one of {', '.join(STATS_LEVELS)}, got '{level}'"
            )
        if not interval > 0:
            raise ValueError(f"stats interval must be greater than 0, got {interval}")
        self._stats_level = level
        self._stats_interval = float(interval)
        return self

    def with_batching(
        self,
        batch_size: int,
        flush_interval: float,
        max_queued_batches: int = 2000,
        drain_stall_seconds: float = 30.0,
    ) -> "SimulationContext":
        """
        Configures the internal egress buffering strategy to mitigate I/O lock contention.

        `batch_size` and `flush_interval` are defaults. A provider that names either
        on `add_egress` uses its own instead, which is how a stream sink and a lake
        sink share one run.

        Args:
            batch_size: The maximum number of standard events to hold in memory
                before forcing an asynchronous flush to the egress thread.
            flush_interval: The maximum simulation time (in seconds) to wait
                before forcefully flushing the buffer, regardless of capacity.
            max_queued_batches: Upper bound on batches waiting for the egress
                threads. A full queue blocks the producer, so a sink that cannot keep
                up slows the simulation instead of building a backlog that teardown
                would then have to discard.
            drain_stall_seconds: How long teardown keeps waiting once the queue has
                stopped shrinking. While it keeps draining the wait is unbounded, so a
                slow sink finishes rather than losing its tail.

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._max_queued_batches = max_queued_batches
        self._drain_stall_seconds = drain_stall_seconds
        return self

    def add_resource(
        self,
        name: str,
        current_cap: Optional[Union[int, float]] = None,
        max_cap: Optional[Union[int, float]] = None,
        queue: str = DEFAULT_QUEUE_ORDER,
        preempt: bool = False,
        queue_limit: Optional[int] = None,
        type: Optional[Type[ResourceBase]] = None,
    ) -> "SimulationContext":
        """
        Registers a discrete logical resource capable of processing tasks.

        Resources act as the primary gating mechanism in the simulation, blocking
        concurrent processes to prevent physical collisions (e.g., restricting
        the number of slabs inside a mill stand).

        Args:
            name: The internal identifier (e.g., 'mill_structural').
            current_cap: The active physical token capacity at simulation start, a
                whole number or `float("inf")` for a resource with no limit. Defaults
                to 1.
            max_cap: The absolute physical maximum bound of tokens allowed. Defaults
                to `current_cap`.
            queue: The order waiting tasks are served in: `fifo` (the default),
                `lifo` or `priority`, which serves the lowest task priority first and
                the oldest first among equal priorities.
            preempt: When True, a task of a lower priority value takes a unit from
                a task of a higher one when every unit is busy, as SimPy's
                `PreemptiveResource` does; the task that loses it follows its
                `on_interrupt`. Needs `queue="priority"`.
            queue_limit: The most requests that may wait, or None for no limit. A
                task that finds that many waiting balks; a request made in the
                low-level API raises `RuntimeError`, as SimPy's `SortedQueue` does.
                0 refuses every request. Not allowed with a `type` whose `PutQueue`
                is not the default.
            type: A subclass of `ResourceBase` to build the resource from, such as
                one that overrides `_do_put`. Defaults to `DynamicResource`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `queue` is not one of the three orders, `preempt` is set
                without `queue="priority"`, `current_cap` or `max_cap` is not a
                whole number or infinity, only `max_cap` is given and is below 1,
                `type` is not a subclass of `ResourceBase`, or `queue_limit` is
                negative or is set on a `type` with its own `PutQueue`.
        """
        if queue not in QUEUE_ORDERS:
            raise ValueError(
                f"queue must be one of {', '.join(QUEUE_ORDERS)}, got '{queue}'"
            )
        if preempt and queue != "priority":
            raise ValueError(
                "preempt ranks users by priority, so it needs queue='priority'"
            )
        check_resource_type(type, queue_limit)
        for field, value in (("current_cap", current_cap), ("max_cap", max_cap)):
            if value is not None and not (
                math.isinf(value) or float(value).is_integer()
            ):
                raise ValueError(
                    f"resource '{name}' needs a whole number for {field}, got {value}"
                )
        if current_cap is None and max_cap is not None and max_cap < 1:
            raise ValueError(
                f"resource '{name}' has max_cap {max_cap} and no current_cap, whose "
                f"default is 1; set current_cap to {max_cap} or less"
            )
        self._resource_preempt[name] = preempt
        self._resources_config[name] = CapacityConfig(
            current_cap=_within_max("resource", name, current_cap, max_cap),
            max_cap=max_cap,
        )
        self._resource_queues[name] = queue
        self._resource_queue_limits[name] = queue_limit
        self._resource_types[name] = type
        return self

    def add_container(
        self,
        name: str,
        current_cap: Optional[float] = None,
        max_cap: Optional[float] = None,
        level: float = 0.0,
        type: Optional[Type[ContainerBase]] = None,
    ) -> "SimulationContext":
        """
        Registers a container: an amount that tasks take from and add to, such as stock.

        `current_cap` is the capacity, the largest level, as for a resource. The
        level starts at `level` and has a live registry path of its own,
        `<sim_id>.containers.<name>.level`, so a scenario or an ingress message can
        set it during the run, such as a restock. Models that used `current_cap` as a
        level, such as wear, use `level` instead.

        A container with no `current_cap` has no limit, and `max_cap` defaults to
        `current_cap`. Use `math.inf` or `float("inf")` for no limit.

        Args:
            name: The internal identifier (e.g., 'stock').
            current_cap: The capacity at simulation start. None, the default, is no
                limit, or `max_cap` when that is given.
            max_cap: The largest capacity a live update can set. None, the default,
                is `current_cap`.
            level: The amount held at simulation start. Defaults to 0.
            type: A subclass of `ContainerBase` to build the container from, such as
                one that overrides `_do_get`. Defaults to `ContainerBase`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If a store already has this name, or `type` is not a
                subclass of `ContainerBase`.
        """
        if name in self._stores_config:
            raise ValueError(f"'{name}' is already a store")
        check_container_type(type)
        self._container_types[name] = type
        self._containers_config[name] = ContainerConfig(
            current_cap=_within_max("container", name, current_cap, max_cap),
            max_cap=max_cap,
            level=level,
        )
        return self

    def add_store(
        self,
        name: str,
        current_cap: Optional[float] = None,
        max_cap: Optional[float] = None,
        priority: bool = False,
        items: Optional[List[Any]] = None,
        type: Optional[Type[StoreBase]] = None,
    ) -> "SimulationContext":
        """
        Registers a store: a place that holds items, which tasks put and get.

        A task puts its finished payload as an item and gets the next item,
        waiting when the store is full or empty. A `fifo` store, the default,
        returns the oldest item; a priority store returns the item whose task
        gave the lowest `put` priority first, and the oldest among equal
        priorities.

        Args:
            name: The internal identifier (e.g., 'parcels').
            current_cap: The number of items it can hold at simulation start, a
                whole number or infinity. None, the default, is no limit, or
                `max_cap` when that is given.
            max_cap: The largest capacity a live update can set. None, the default,
                is `current_cap`.
            priority: When True, items are returned by the `priority` each task's
                `put` gives, lowest first, instead of first in, first out.
            items: The items it holds when the run starts, in the order a get
                returns them. A priority store takes `(priority, item)` pairs.
            type: A subclass of `StoreBase` to build the store from, such as one
                that overrides `_do_get`. Defaults to a `simpy.FilterStore`. It
                cannot be combined with `priority`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If a container already has this name, a capacity is not a
                whole number or infinity, there are more items than `current_cap`,
                a priority store's item is not a pair, or `type` is not a subclass
                of `StoreBase` or is given with `priority`.
        """
        if name in self._containers_config:
            raise ValueError(f"'{name}' is already a container")
        check_store_type(type, priority)
        for field, value in (("current_cap", current_cap), ("max_cap", max_cap)):
            if value is not None and not _whole_or_unlimited(value):
                raise ValueError(
                    f"store '{name}' needs a whole number for {field}, got {value}"
                )
        start = list(items or [])
        room = [cap for cap in (current_cap, max_cap) if cap is not None]
        if room and len(start) > min(room):
            raise ValueError(
                f"store '{name}' holds {min(room)} items, so it "
                f"cannot start with {len(start)}; raise current_cap and max_cap, or "
                f"give fewer items"
            )
        if priority and not all(
            isinstance(item, (tuple, list)) and len(item) == 2 for item in start
        ):
            raise ValueError(
                f"priority store '{name}' starts with (priority, item) pairs; give "
                f"each item as a pair, such as (1, item)"
            )
        self._store_items[name] = start
        self._stores_config[name] = CapacityConfig(
            current_cap=_within_max("store", name, current_cap, max_cap),
            max_cap=max_cap,
        )
        self._store_priority[name] = priority
        self._store_types[name] = type
        return self

    def add_scenario(self, steps: List[Mapping[str, Any]]) -> "SimulationContext":
        """
        Adds scenario steps: changes and records at set simulation times.

        Each step is a mapping: `at`, seconds or a duration from the start of the
        run, and a `path` and `value` to set in the registry, a `record` to publish
        as a ground-truth label, an `interrupt` (`{"task": ..., "where": ...,
        "cause": ...}`), or more than one. A step applies before every other event
        at its time, so an arrival gap drawn at that moment uses a new rate; a step
        at or after `until` never applies. Steps at equal times apply in the order
        given.

        Args:
            steps: The steps.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If a step is malformed. A path the model does not have is
                refused when the run starts.
        """
        checked = [check_step(step) for step in steps]
        if not self._scenario_steps:
            self.add_process(self._run_scenario)
        self._scenario_steps.extend(checked)
        return self

    def add_step(
        self,
        at: Union[float, str],
        path: Optional[str] = None,
        value: Any = None,
        record: Optional[Mapping[str, Any]] = None,
        interrupt: Optional[Mapping[str, Any]] = None,
    ) -> "SimulationContext":
        """
        Adds one scenario step; `add_scenario` describes the steps.

        Args:
            at: Seconds, or a duration such as `"10 min"`, from the start of the run.
            path: A registry path to set, with `value`.
            value: The value to set.
            record: A ground-truth record to publish.
            interrupt: `{"task": ..., "where": ..., "cause": ...}` to interrupt.

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        step: Dict[str, Any] = {"at": at}
        if path is not None:
            step.update(path=path, value=value)
        if record is not None:
            step["record"] = record
        if interrupt is not None:
            step["interrupt"] = interrupt
        return self.add_scenario([step])

    def _run_scenario(self, context: "SimulationContext"):
        """The scenario process: `run_scenario` with this run's until."""
        return run_scenario(
            self.env,
            self._scenario_steps,
            until=self._until,
            interrupt=self.interrupt,
        )

    def add_breakdown(
        self,
        resource: str,
        time_to_failure: Mapping[str, Any],
        repair: Mapping[str, Any],
        work: str = "interrupt",
    ) -> "SimulationContext":
        """
        Makes a resource break down at random and be repaired.

        The failure clock runs on calendar time: after each repair, or the start,
        the time to the next failure is drawn, busy or idle. While the resource is
        down its capacity is 0, its `current_cap` keeps the planned value, and a
        change to it during the repair sets the capacity the resource returns to.
        Each failure and repair publishes an event keyed by the resource's name,
        `{"status": "down", "reason": "failure"}` and `{"status": "repaired", ...}`.
        The two distributions are registry paths,
        `<sim_id>.breakdown.<resource>.time_to_failure` and `.repair`.

        Args:
            resource: The resource's name.
            time_to_failure: A distribution, such as
                `{"dist": "weibull", "shape": 1.5, "scale": 3600}`.
            repair: A distribution, such as `{"dist": "exponential", "rate": 0.01}`.
            work: `interrupt` (the default) interrupts the tasks being served, which
                follow their `on_interrupt`; `finish` lets them finish.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If a distribution or `work` is invalid.
        """
        self._breakdowns_config[resource] = BreakdownConfig(
            time_to_failure=_distribution_of(time_to_failure),
            repair=_distribution_of(repair),
            work=work,
        )
        return self

    def add_variable(self, name: str, value: Any) -> "SimulationContext":
        """
        Registers a dynamic, untyped variable accessible via the central Registry.

        Variables act as arbitrary state payloads that can be targeted by the
        Control Plane. For example, storing a JSON dictionary defining the
        velocity and frequency of a gradual wear injection loop.

        Args:
            name: The internal identifier (e.g., 'velocity_structural').
            value: The initial payload (supports primitives, lists, and dicts).

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        self._variables_config[name] = value
        return self

    def add_service(
        self,
        name: str,
        dist: str,
        *,
        rate: Optional[float] = None,
        mean: Optional[float] = None,
        std: Optional[float] = None,
        low: Optional[float] = None,
        value: Optional[float] = None,
        high: Optional[float] = None,
        mode: Optional[float] = None,
        shape: Optional[float] = None,
        scale: Optional[float] = None,
        values: Optional[List[float]] = None,
        weights: Optional[List[float]] = None,
        edges: Optional[List[float]] = None,
        counts: Optional[List[float]] = None,
    ) -> "SimulationContext":
        """
        Registers a statistical distribution defining a process service duration.

        The parameters are keyword-only, so their order never changes the meaning.
        Give only those `dist` takes; `desflow.core.distributions.DISTRIBUTIONS`
        lists each family's parameters and rules.

        Args:
            name: The internal identifier (e.g., 'packing').
            dist: The family: exponential, normal, lognormal, truncated_normal,
                constant, uniform, triangular, gamma, weibull or empirical.
            rate: Events per second, for exponential.
            mean: The mean, for normal, lognormal and truncated_normal.
            std: The standard deviation, for the same three (0 when not given).
            low: The lowest value, for truncated_normal (0 when not given), uniform
                and triangular.
            value: The time a constant distribution always gives.
            high: The highest value, for uniform and triangular.
            mode: The most likely value, for triangular.
            shape: The shape, for gamma and weibull.
            scale: The scale, for gamma and weibull.
            values: The values an empirical draws from, taken at load.
            weights: One weight per empirical value.
            edges: The bin edges of an empirical histogram.
            counts: One count per empirical histogram bin.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If the family is unknown, or a parameter is missing, out of
                range or not one the family takes.
        """
        self._services_config[name] = _distribution(dist, locals())
        return self

    def add_arrival(
        self,
        name: str,
        dist: str,
        *,
        rate: Optional[float] = None,
        mean: Optional[float] = None,
        std: Optional[float] = None,
        low: Optional[float] = None,
        value: Optional[float] = None,
        high: Optional[float] = None,
        mode: Optional[float] = None,
        shape: Optional[float] = None,
        scale: Optional[float] = None,
        values: Optional[List[float]] = None,
        weights: Optional[List[float]] = None,
        edges: Optional[List[float]] = None,
        counts: Optional[List[float]] = None,
        attributes: Optional[Mapping[str, Any]] = None,
        key: Optional[Callable[[int], str]] = None,
        spawn: Optional[str] = None,
    ) -> "SimulationContext":
        """
        Registers a statistical distribution defining the inter-arrival times of entities.

        Each arrival starts a new entity, which `wait_for_arrival` returns. Its key,
        the record key of every event it causes, is the arrival name and a counter
        (`orders-1`, `orders-2`) unless `key` gives another.

        Args:
            name: The internal identifier (e.g., 'orders').
            dist: The family, as for `add_service`.
            rate: Events per second, for exponential.
            mean: The mean, for normal, lognormal and truncated_normal.
            std: The standard deviation, for the same three (0 when not given).
            low: The lowest value, for truncated_normal, uniform and triangular.
            value: The time a constant distribution always gives.
            high: The highest value, for uniform and triangular.
            mode: The most likely value, for triangular.
            shape: The shape, for gamma and weibull.
            scale: The scale, for gamma and weibull.
            values: The values an empirical draws from, taken at load.
            weights: One weight per empirical value.
            edges: The bin edges of an empirical histogram.
            counts: One count per empirical histogram bin.
            attributes: The attributes each entity starts with, read in conditions
                as `attr.<name>`. Each entity gets its own copy.
            key: A function of the entity's counter, from 1, returning its key.
            spawn: The task each arrival starts, with the arrival's entity and the
                task's next instance id, as YAML's `spawn` key does. None leaves
                the arrival to an `arrival_loop` written by hand.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If the family is unknown, or a parameter is missing, out of
                range or not one the family takes. A `spawn` naming no task is
                refused when the run starts.
        """
        params = {
            k: v for k, v in locals().items() if k not in ("attributes", "key", "spawn")
        }
        self._arrivals_config[name] = _distribution(dist, params)
        self._arrival_entities[name] = (dict(attributes or {}), key)
        self._add_spawn(name, spawn)
        return self

    def _add_spawn(self, arrival: str, task: Optional[str]) -> None:
        """Starts `task` for each arrival, from a loop added now, so the arrival
        loops start in the order the arrivals were added."""
        if task is None:
            return
        self._arrival_spawns[arrival] = task
        self._startup_loops.append((_spawn_loop(arrival, task), arrival))

    # ==========================================
    # DECORATORS (Execution Logic)
    # ==========================================

    def task(
        self,
        service_id: Optional[str] = None,
        resource_id: Union[str, List[str], None] = None,
        *,
        name: Optional[str] = None,
        priority: Union[
            float, str, Callable[[Any, Dict[str, Any]], float], None
        ] = None,
        get: Union[Transfer, Mapping[str, Any], None] = None,
        put: Union[Transfer, Mapping[str, Any], None] = None,
        next: NextSpec = None,
        set: Optional[Mapping[str, Any]] = None,
        patience: Union[
            float, str, Callable[[Any, Dict[str, Any]], float], None
        ] = None,
        balk: Optional[str] = None,
        wait: Optional[Mapping[str, Any]] = None,
        signal: Optional[str] = None,
        fail: Union[str, Callable[[Any, Any], bool], None] = None,
        fail_reason: Optional[str] = None,
        on_fail: Optional[str] = None,
        keep: Optional[List[str]] = None,
        preempt: bool = True,
        on_interrupt: str = "resume",
        on_interrupt_next: Optional[str] = None,
        interrupt: Optional[Mapping[str, Any]] = None,
        spawn: Optional[Mapping[str, Any]] = None,
        subscribe: Optional[Mapping[str, Any]] = None,
        on: Optional[List[Mapping[str, Any]]] = None,
    ) -> Callable:
        """
        Transforms a standard Python function into an event-driven SimPy task.

        This decorator completely abstracts the boilerplate of resource acquisition,
        clock synchronization, and standardized telemetry lineage.

        Lifecycle Execution:
        1. Checks `balk`; when it holds, emits `balked` and stops.
        2. Emits a `queued` event, when the task waits for anything: its `wait`, a
           resource or a `get`.
        3. Waits for its `wait`, then for a unit of each resource in `resource_id`,
           in the order listed and each in its queue order, then for its `get`;
           `patience` counts the time spent waiting for all three, and pauses
           while the task is served.
        4. Emits a `started` event.
        5. Executes the wrapped user function to extract custom payload data.
        6. Samples the live `service_id` distribution and yields the timeout.
        7. Adds its `put`, waiting for room, and checks `fail`.
        8. Emits its finished event with the user's payload, or `failed`.
        9. Fires its `signal` (or fails it), sends its `interrupt` action, applies
           `set` to the entity's attributes and starts the `next` task, or
           `on_fail`.

        Every event carries the entity's key as its record key, and the task's name
        and instance id as the record's `task` and `instance` fields.

        A task with a service and no resource is a timed step: no `queued` event, so
        its wait is 0. A task with no service, resource or get takes no time: when
        spawned it emits the payload at once (after its `put`, if it has one), with
        no `queued` or `started` event.

        Call the task as `task(task_id, ...)`. Pass `entity=` with the entity
        `wait_for_arrival` returned; without one the task starts an entity of its
        own, keyed by its name and a counter. The function receives `entity`,
        `upstream` (what the previous task handed on, or None) and `item` (what a
        store `get` took) by keyword, each only when it declares that parameter, so
        existing functions keep working. A task started by `next` is called with its
        own instance id and the same other arguments as the task before it. A
        function that declares `wait` receives the wait's branches, each a mapping
        of `done`, `ok` and `value`.

        Args:
            service_id: The ID of the configured distribution dictating the execution time.
            resource_id: The resource the task holds while it works, or a list of
                them, acquired in the order listed while the earlier ones are held.
            name: The task's name in records, statistics and `next` rules. Defaults
                to the function's name.
            priority: For a resource whose queue order is `priority`, lowest first: a
                number, an expression such as `"attr.priority"`, or a function of the
                task id and the entity's attributes. It is read when the task joins
                the queue, before its payload exists. A task with no priority ranks 0, as a
                SimPy `PriorityRequest` does.
            get: What the task takes before it starts: `{"container": name,
                "amount": ...}` or `{"store": name}`. The amount is a number, an
                expression such as `"payload.quantity"`, or a function of the task id
                and the upstream payload. Defaults to an amount of 1.
            put: What the task adds when it finishes, in the same form; a store
                receives the finished payload.
            next: The task that follows: a task name, `{"when": [{"condition": ...,
                "task": ...}], "default": ...}`, `{"weights": {name: weight}}`, or a
                function of the `Upstream` returning a name or None.
            set: Attribute values applied to the entity when the task finishes.
            patience: How long the task may wait, for its `wait`, its resources and
                its `get` together, before it leaves the queue and publishes
                `reneged`. It pauses while the task is served, so a preempted task
                that queues again goes on with the time it has left. Seconds, a
                duration such as `"30 s"`,
                an expression such as `"attr.patience"`, or a function of the task id
                and the entity's attributes.
            balk: A condition, such as `"t >= 3300"`, checked before the task joins
                the queue; when it holds, the task publishes `balked` and never
                requests the resource.
            wait: What the task waits for before it queues for its resource: one
                branch, `{"signal": name}`, `{"task": name}`, `{"timeout": seconds}`
                or `{"condition": "..."}`, or `{"any": {...}}` or `{"all": {...}}`
                of named branches. After it, `wait.<branch>.done`, `.ok` and
                `.value` are read in `next`, `fail` and amounts.
            signal: A signal, added with `add_signal`, that the task fires with its
                finished payload, or fails with its reason when it fails.
            fail: A condition checked after the service, such as
                `"payload.defect == 1"`, where `payload` is the task's own payload,
                or a function of the task id and the payload. When it holds, the
                task publishes `failed` instead of its payload.
            fail_reason: The `reason` a failure publishes. Defaults to the `fail`
                condition's text.
            on_fail: The task a failed instance goes to. Without it, a failure ends
                the entity's flow. A function that raises fails the task the same
                way, with the error as the reason.
            keep: Resources in `resource_id` whose units the task hands to the next
                task of the flow instead of releasing them; that task releases them
                once it publishes `started`.
            preempt: On a resource added with `preempt=True`, whether the task's
                requests may take a unit from a task of a higher priority value.
            on_interrupt: What an interrupt or a preemption does: `resume` (the
                default) gives back every unit, queues again and serves the
                remaining time; `restart` does the same with a new service time;
                `abandon` ends the flow, or goes to `on_interrupt_next`.
            on_interrupt_next: The task an abandoned instance goes to.
            interrupt: An action applied when the task finishes:
                `{"task": name, "where": condition, "cause": value}` interrupts every
                running instance of that task whose condition holds.

        Raises:
            ValueError: If a resource is given without a service, a priority without
                a resource, the name is taken, or `get`, `put` or `next` is
                malformed.

        Returns:
            Callable: A decorator that wraps the target function into a SimPy generator.

        Example:
            ```python
            @app.task(service_id="packing", resource_id="packers", next="ship")
            def pack_order(task_id: int, context):
                return {"status": "packed"}
            ```
        """
        resources = (
            []
            if resource_id is None
            else [resource_id]
            if isinstance(resource_id, str)
            else list(resource_id)
        )
        # `set` is a parameter here, so the sets are written as literals
        if len({*resources}) != len(resources):
            raise ValueError("a task lists each resource once")
        if not {*(keep or [])} <= {*resources}:
            raise ValueError(
                "keep names resources the task holds; list only resources in its "
                "resource_id"
            )
        if on_interrupt not in INTERRUPT_POLICIES:
            raise ValueError(
                f"on_interrupt must be one of {', '.join(INTERRUPT_POLICIES)}, "
                f"got '{on_interrupt}'"
            )
        if on_interrupt_next is not None and on_interrupt != "abandon":
            raise ValueError("on_interrupt_next needs on_interrupt='abandon'")
        if interrupt is not None:
            interrupt = dict(interrupt)
            unknown = {*interrupt} - {"task", "where", "cause"}
            if "task" not in interrupt or unknown:
                raise ValueError(
                    "interrupt takes a task, and an optional where and cause"
                )
            if interrupt.get("where") is not None:
                Expression(interrupt["where"])
        resource_id = resources[0] if resources else None
        if resource_id is not None and service_id is None:
            raise ValueError(
                "a task with a resource needs a service; give service_id too"
            )
        if (
            priority is not None
            and resource_id is None
            and not (wait is not None and Wait(wait).requests())
        ):
            raise ValueError(
                "priority orders a resource's queue, so it needs a resource or a "
                "request in its wait"
            )
        if balk is not None and resource_id is None:
            raise ValueError("balking refuses a resource queue, so it needs a resource")
        if isinstance(patience, str):
            try:
                patience = time_to_seconds(patience)
            except ValueError:
                pass  # not a duration, so an expression such as "attr.patience"
        take = None if get is None else Transfer.of(get)
        if take is not None and take.priority != 0.0:
            raise ValueError(
                "a get takes no priority; priority orders a put into a priority store"
            )
        give = None if put is None else Transfer.of(put)
        if give is not None and give.filter is not None:
            raise ValueError(
                "a put takes no filter; a filter picks the item a get takes"
            )
        if (
            patience is not None
            and resource_id is None
            and (take is None or not take.wait)
            and wait is None
        ):
            raise ValueError(
                "patience is how long a task waits, so it needs a wait, a resource "
                "or a get"
            )
        if fail is None and (fail_reason is not None or on_fail is not None):
            raise ValueError("fail_reason and on_fail need a fail condition")
        spec_kwargs = dict(
            patience=patience,
            balk=balk,
            wait=None if wait is None else Wait(wait),
            signal=signal,
            fail=fail,
            fail_reason=fail_reason,
            on_fail=on_fail,
            attribute_changes=dict(set or {}),
            resources=resources,
            keep=keep,
            preempt=preempt,
            on_interrupt=on_interrupt,
            on_interrupt_next=on_interrupt_next,
            interrupt=interrupt,
            spawn=None if spawn is None else _parse_spawn(spawn),
            subscribe=None if subscribe is None else Wait(subscribe),
            on=[_parse_on_action(entry) for entry in on or []],
        )
        router = Router(next)

        def decorator(user_func):
            task_name = name or user_func.__name__
            if task_name in self._tasks:
                raise ValueError(f"a task called '{task_name}' already exists")
            spec = TaskSpec(
                task_name,
                service_id,
                resource_id,
                priority,
                take,
                give,
                router,
                **spec_kwargs,
            )
            spec.user_func = user_func
            spec.declared = inspect.signature(user_func).parameters
            if "item" in spec.declared and take is not None and not take.wait:
                raise ValueError(
                    f"task '{task_name}' declares item, but its get has wait=False, "
                    "which takes the item later and discards it"
                )

            def wrapper(
                task_id: Any,
                *args: Any,
                entity: Optional[Entity] = None,
                upstream: Optional[Upstream] = None,
                **kwargs: Any,
            ):
                if not self._env or not self.sampler:
                    raise RuntimeError("Simulation has not been built yet.")
                if entity is None:
                    entity = self._new_entity(task_name, None)
                generator = TaskRun(
                    self, spec, task_id, entity, upstream, args, kwargs
                ).run()
                # A process is named after its generator; name it after the task (P6)
                generator.__name__ = task_name  # type: ignore[attr-defined]
                return generator

            self._tasks[task_name] = wrapper
            self._task_specs[task_name] = spec
            return wrapper

        return decorator

    def arrival_loop(self, arrival_id: str) -> Callable:
        """
        Registers an infinite generator loop to automatically start on boot.

        The wrapped function will be injected with the `SimulationContext`
        instance at runtime, granting it access to `context.wait_for_arrival()`.

        Args:
            arrival_id: The ID of the configured arrival rate to dynamically sample.
        """

        def decorator(user_func):
            self._startup_loops.append((user_func, arrival_id))
            return user_func

        return decorator

    def telemetry_loop(self, interval: float) -> Callable:
        """
        Registers an infinite background daemon loop for periodic data publishing.

        This is primarily used for observing and extracting continuous state
        (like wear containers) at strict intervals.

        Args:
            interval: The simulation time (in seconds) to wait between iterations.
        """

        def decorator(user_func):
            def wrapper():
                if not self._env:
                    raise RuntimeError("Simulation has not been built yet.")
                while True:
                    user_func(self)
                    yield self._env.timeout(interval)

            self._startup_loops.append((wrapper, None))
            return user_func

        return decorator

    def add_process(self, func: Callable, **kwargs: Any) -> "SimulationContext":
        """
        Registers a generator function to start when the run starts.

        This is the builder form of `spawn`, for a process that is not an arrival or
        telemetry loop, such as a drift engine or a scripted scenario. It is how a
        YAML blueprint starts the processes it references with `!python`.

        Args:
            func: A generator function. It is called as `func(context, **kwargs)`.
            **kwargs: Extra keyword arguments passed to `func`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Example:
            ```python
            def drift(context, step):
                while True:
                    yield context.env.timeout(1.0)
                    ...

            app.add_process(drift, step=0.5)
            ```
        """
        self._startup_loops.append((functools.partial(func, self, **kwargs), None))
        self._processes.append(func)
        return self

    # ==========================================
    # RUNTIME HELPERS (For Raw Generators)
    # ==========================================

    @property
    def env(self) -> DynamicRealtimeEnvironment:
        """
        Read-only handle to the active `DynamicRealtimeEnvironment`.

        Grants raw generators access to environment primitives such as
        `publish_event`, `timeout`, `now`, and `start_datetime` without
        reaching into private builder state.

        Raises:
            RuntimeError: If accessed during the Builder Phase (before `.run()`).
        """
        if self._env is None:
            raise RuntimeError("Environment is not attached until context is run.")
        return self._env

    def wait_for_arrival(self, arrival_id: str) -> simpy.Event:
        """
        Returns the wait for the next arrival: a timeout from the live distribution,
        or for a replayed arrival the wait for its next row.

        The timeout's value is the new `Entity`, so an arrival loop writes
        `entity = yield context.wait_for_arrival("orders")` and passes
        `entity=entity` to the task it spawns.

        Args:
            arrival_id: The arrival's name.

        Returns:
            simpy.Event: A timeout for the gap to the next arrival, or the wait for
                the next replayed row, which succeeds with the new `Entity`; after a
                replay's last row the wait never ends.

        Raises:
            RuntimeError: If called during the Builder Phase (before `.run()`).
        """
        if not self._env or not self.sampler:
            raise RuntimeError("Cannot sample arrival before context is run.")

        if arrival_id in self._replays:
            return self._next_replayed(arrival_id)
        cfg = self._env.registry.get_config(f"{self.sim_id}.arrival.{arrival_id}")
        entity = self._new_entity(arrival_id, self._arrival_entities.get(arrival_id))
        return self._env.timeout(self.sampler.sample(cfg), value=entity)

    def _next_replayed(self, arrival_id: str) -> simpy.Event:
        """The wait for the next row of a replayed arrival; it never ends after the
        last row, so the arrival stops."""
        rows = self._replay_rows[arrival_id]
        if not rows:
            if arrival_id not in self._replays_done:
                self._replays_done.add(arrival_id)
                logger.info(
                    f"Arrival '{arrival_id}' replayed its last row at "
                    f"{self.env.now:g} s; it stops."
                )
            return self.env.event()
        row = rows.pop(0)
        attributes = {
            **self._arrival_entities.get(arrival_id, ({}, None))[0],
            **row.fields,
        }
        entity = self._new_entity(
            arrival_id, (attributes, self._arrival_entities[arrival_id][1])
        )
        return self.env.timeout(max(0.0, row.time - self.env.now), value=entity)

    def add_arrival_replay(
        self,
        name: str,
        source: Union[str, Path, Iterable[Mapping[str, Any]]],
        *,
        time: str = "time",
        unit: str = "s",
        fields: Union[str, List[str]] = "all",
        on_invalid: str = "error",
        attributes: Optional[Mapping[str, Any]] = None,
        key: Optional[Callable[[int], str]] = None,
        spawn: Optional[str] = None,
    ) -> "SimulationContext":
        """
        Registers an arrival taken from recorded rows instead of a distribution.

        Each row becomes one arrival at its recorded time, in the order given, and
        its fields become the entity's attributes. `wait_for_arrival(name)` returns
        the wait for the next row, so an arrival loop is written as for any other
        arrival. Rows at equal times arrive at that time in their order; rows at or
        after `until` never arrive; after the last row the arrival stops, with one
        INFO line. The rows are read and checked when the run starts.

        Args:
            name: The arrival's name.
            source: A CSV or Parquet file (Parquet needs the `parquet` extra), or
                an iterable of mappings.
            time: The column of each row's time.
            unit: `ms`, `s`, `min` or `h` from the start of the run, or `timestamp`
                for a date and time, which arrives when the run's records carry
                that time: `logical_start_time` is simulation time 0, whatever
                the `initial_time`.
            fields: The columns each entity carries, or `all`.
            on_invalid: `error` refuses a row with no valid time; `skip` leaves it
                out with a warning.
            attributes: Attributes every entity starts with; a row's fields of the
                same name replace them.
            key: A function of the entity's counter, from 1, returning its key.
            spawn: The task each row starts, as for `add_arrival`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `unit` or `on_invalid` is unknown. A `spawn` naming no
                task is refused when the run starts.
        """
        if unit not in UNITS and unit != "timestamp":
            raise ValueError(
                f"unit must be one of {', '.join([*UNITS, 'timestamp'])}, got '{unit}'"
            )
        if on_invalid not in ("error", "skip"):
            raise ValueError(f"on_invalid must be error or skip, got '{on_invalid}'")
        self._replays[name] = dict(
            source=source, time=time, unit=unit, fields=fields, on_invalid=on_invalid
        )
        self._arrival_entities[name] = (dict(attributes or {}), key)
        self._add_spawn(name, spawn)
        return self

    def next_instance(self, task: str) -> int:
        """
        Returns the next instance id of a task, counting from 0.

        Each task has one counter, which `next` uses when it starts the task. An
        arrival loop that spawns a task by hand takes the id from here too, so a
        task started both by an arrival and by `next` never repeats an id.

        Args:
            task: The task's name.

        Returns:
            int: The task's next instance id.
        """
        counter = self._instance_counters.setdefault(task, itertools.count())
        return next(counter)

    def _new_entity(
        self,
        arrival: str,
        options: Optional[Tuple[Dict[str, Any], Optional[Callable[[int], str]]]],
    ) -> Entity:
        """Starts an entity: the next counter of `arrival`, its key and attributes."""
        counter = self._entity_counters.setdefault(arrival, itertools.count(1))
        number = next(counter)
        attributes, key = options if options is not None else ({}, None)
        return Entity(
            key=f"{arrival}-{number}" if key is None else key(number),
            arrival=arrival,
            attributes=dict(attributes),
        )

    def _entity_key_of(self, process: Any) -> Optional[str]:
        """The key of the entity whose task runs in `process`, as text; None for a
        process that is not a task, or None itself."""
        if process is not None:
            for runs in self._running.values():
                for run in runs:
                    if run.process is process:
                        return str(run.entity.key)
        return None

    def _levels(self) -> Dict[str, Any]:
        """Stores and containers by name, which `level.<name>` reads."""
        return {**self._stores_map, **self._containers_map}

    def get_store(self, store_id: str) -> DynamicStore:
        """
        Retrieves an active `DynamicStore`.

        Args:
            store_id: The store's name.

        Returns:
            DynamicStore: The store.

        Raises:
            RuntimeError: If called during the Builder Phase.
        """
        if not self._env:
            raise RuntimeError("Stores are not instantiated until context is run.")
        return self._stores_map[store_id]

    def get_container(self, container_id: str) -> DynamicContainer:
        """
        Retrieves an active `DynamicContainer`.

        Args:
            container_id: The container's name.

        Returns:
            DynamicContainer: The container.

        Raises:
            RuntimeError: If called during the Builder Phase.
        """
        if not self._env:
            raise RuntimeError("Containers are not instantiated until context is run.")
        return self._containers_map[container_id]

    def get_resource(self, resource_id: str) -> ResourceBase:
        """Returns the resource a running model built: a `DynamicResource`, or the
        `ResourceBase` subclass its `type` names.

        Args:
            resource_id: The resource's name, as given to `add_resource`.

        Returns:
            ResourceBase: The resource, for raw requests and readings.

        Raises:
            RuntimeError: If called before the run has started.
        """
        if not self._env:
            raise RuntimeError("Resources are not instantiated until context is run.")
        return self._resources_map[resource_id]

    def publish(self, metric_name: str, value: Any) -> None:
        """Publishes a telemetry value, named `<sim_id>.<metric_name>`, to the sinks.

        Args:
            metric_name: The value's name, after the sim id, such as `packers_free`.
            value: The value, which must be a JSON value.

        Raises:
            RuntimeError: If called before the run has started.
        """
        if not self._env:
            raise RuntimeError("Cannot publish telemetry before context is run.")
        self._env.publish_telemetry(f"{self.sim_id}.{metric_name}", value)

    def spawn(self, process: Any, delay: Optional[float] = None) -> simpy.Process:
        """
        Starts a generator as a SimPy process, such as a task instance.

        This acts as the "escape hatch" for complex logic (like physical slab
        tracking) that outgrows the strict abstraction of the `@task` decorator.

        Args:
            process: A generator, such as `pack_order(task_id, entity=entity)`.
            delay: Seconds to wait before starting it, as SimPy's `start_delayed`.
                None starts it now; a value of 0 or less is refused.

        Returns:
            simpy.Process: The process. Yield it to wait until it ends; its value is
                what the generator returns, and `is_alive`, `triggered`, `ok` and
                `value` inspect it, as in SimPy. With `delay` the returned process is
                the helper whose value is the started process.

        Raises:
            ValueError: If `delay` is 0 or less, as SimPy's `start_delayed` does.
        """
        if not self._env:
            raise RuntimeError("Cannot spawn processes before context is run.")
        if delay is None:
            return self._env.process(process)
        import simpy.util

        return simpy.util.start_delayed(self._env, process, delay)

    def subscribe(self, event: Any) -> None:
        """
        Asks to be interrupted when `event` happens, as SimPy's `subscribe_at`.

        The calling process keeps running; when `event` fires it is interrupted one
        urgent step later with the cause `{"event": name, "value": value}`, where
        `name` is the process's name for a process and None for any other event.

        Args:
            event: Any SimPy event, a process included.

        Raises:
            RuntimeError: If called outside a process, or `event` has already
                happened, as SimPy's `subscribe_at` refuses a processed event.
        """
        if not self._env:
            raise RuntimeError("Cannot subscribe before context is run.")
        subscriber = self._env.active_process
        if subscriber is None:
            raise RuntimeError("subscribe must be called from inside a process")
        if event.processed:
            raise RuntimeError("subscribe cannot be used for a processed event")
        name = getattr(event, "name", None)

        def signaller(signal: Any, receiver: Any):
            try:
                result = yield signal
            except Exception as error:  # noqa: BLE001: the event failed
                # Under `continue` the subscriber is told; under `stop` the failure
                # is left undefused, so it stops the run as SimPy's does.
                if getattr(self.env, "on_unhandled_failure", "stop") == "stop":
                    raise
                result = getattr(error, "reason", str(error))
            if receiver.is_alive:
                receiver.interrupt({"event": name, "value": result})

        self._env.process(signaller(event, subscriber))

    def call(
        self,
        task: str,
        *args: Any,
        entity: Optional[Entity] = None,
        upstream: Optional[Upstream] = None,
        **kwargs: Any,
    ) -> simpy.Process:
        """
        Starts a task as a child flow and returns its process.

        The task runs with its own instance id, and so do the tasks its `next`
        leads to. The process ends when that flow ends: its value is the last
        task's finished payload, and it fails with `TaskFailed` when a task of the
        flow fails and no `on_fail` handles it. A flow started this way is not
        counted as the entity's flow finishing.

        Args:
            task: The task's name.
            *args: Positional arguments its function takes after the task id.
            entity: The entity, such as the caller's own; a new one when None.
            upstream: What the task receives as `upstream`.
            **kwargs: Keyword arguments its function takes.

        Returns:
            simpy.Process: The child flow. A failure nobody waits for does not stop
                the run.

        Raises:
            RuntimeError: If called before the run starts.
            ValueError: If no task has that name.
        """
        if not self._env:
            raise RuntimeError(
                "Cannot call tasks before context is run; call them from a task or "
                "a process"
            )
        if task not in self._task_specs:
            raise ValueError(
                f"no task is called '{task}'"
                + (suggestion(task, self._task_specs) or "; add it with @app.task()")
            )
        if entity is None:
            entity = self._new_entity(task, None)
        return self._start(
            task, entity, upstream, child=True, args=args or None, kwargs=kwargs
        )

    def _start(
        self,
        task: str,
        entity: Entity,
        upstream: Optional[Upstream],
        child: bool = False,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> simpy.Process:
        """Starts one instance of a task as a process, with its next instance id.

        `args` are the positional arguments after the task id; None gives the ones a
        YAML task takes, the context, and none for a builder task.
        """
        spec = self._task_specs[task]
        run = TaskRun(
            self,
            spec,
            self.next_instance(task),
            entity,
            upstream,
            self._task_args if args is None else args,
            kwargs or {},
            child=child,
        )
        generator = run.run()
        # SimPy names a process after its generator's __name__; name it after the
        # task, not `run`, so `active_process.name` and records read the task (P6).
        generator.__name__ = spec.name  # type: ignore[attr-defined]
        process = self.env.process(generator)
        if child and self._on_unhandled_failure == "continue":
            # Under `continue`, a child flow that fails with no task waiting for it
            # ends that flow, not the run; under `stop` the failure stops the run.
            process.callbacks.append(_defuse)
        return process

    def _spawn_entity(
        self,
        task: str,
        parent: Optional[Entity],
        upstream: Optional[Upstream],
        after: Optional[float],
    ) -> simpy.Process:
        """Starts `task` as the first task of a new entity, keyed `<task>-n`, with
        `parent` as its `spawned_by` and `attr.parent` the parent's key.

        Without `after` the entity is made and counted in now; with `after` a wrapper
        named after the task makes and counts it in when the task starts, built with
        SimPy's `start_delayed` so the events order as SimPy's.
        """
        parent_key = parent.key if parent is not None else None

        def make() -> Any:
            entity = self._new_entity(task, ({"parent": parent_key}, None))
            entity.spawned_by = parent
            entity.entered = True
            self._entity_entered(entity)
            run = TaskRun(
                self,
                self._task_specs[task],
                self.next_instance(task),
                entity,
                upstream,
                self._task_args,
                {},
                child=False,
            )
            gen = run.run()
            gen.__name__ = task  # type: ignore[attr-defined]
            return gen

        if after is None:
            return self.env.process(make())

        def wrapper() -> Any:
            return (yield from make())

        delayed = wrapper()
        delayed.__name__ = task  # type: ignore[attr-defined]
        import simpy.util

        return simpy.util.start_delayed(self.env, delayed, after)

    def _fire_signal(
        self,
        name: str,
        ok: bool,
        value: Any,
        entity: Optional[Entity],
        urgent: bool = False,
        after: Optional[float] = None,
    ) -> None:
        """Fires or fails a signal through one path, counting its `on:` actions
        against `entity` so a drain waits for them.

        When the firing is new, the signal has `on:` actions, and `entity` has not
        left, 1 is added to `entity.ends_pending` and the entity is stored on the
        pending event, so the signal's last callback counts it back out.

        A firing with `urgent` or `after` is for a one-shot signal and is scheduled
        with that priority and delay. With `after` it is not counted against
        `entity`, because the entity may leave before the signal fires, so a drain
        can end before the signal's `on:` actions run.
        """
        signal = self._signals[name]
        pending = signal.wait()
        if (
            after is None
            and not pending.triggered
            and self._signal_on.get(name)
            and entity is not None
            and not entity.left
        ):
            entity.ends_pending += 1
            pending._desflow_entity = entity  # type: ignore[attr-defined]
        if ok:
            signal.fire(value, urgent=urgent, after=after or 0.0)
        else:
            signal.fail(value, urgent=urgent, after=after or 0.0)

    def _signal_fired(self, name: str, event: Any) -> None:
        """The signal's last `on:` callback: runs its actions, then counts the firing
        task's entity out, as a pending `ended` is counted out."""
        entity = getattr(event, "_desflow_entity", None)
        for action in self._signal_on.get(name, []):
            self._run_on_action(action, f"signal '{name}'", entity, None)
        if entity is not None:
            self._end_processed(entity)

    def _end_processed(self, entity: Entity, _event: Any = None) -> None:
        """Takes 1 from `entity.ends_pending`; when it reaches 0 and the flow has
        ended, counts the entity out."""
        entity.ends_pending -= 1
        if entity.ends_pending == 0 and entity.ending:
            self._entity_left(entity)

    def _run_on_action(
        self,
        action: Dict[str, Any],
        owner: str,
        entity: Optional[Entity],
        upstream: Optional[Upstream],
    ) -> None:
        """Runs one `on:` action: fire a signal, spawn a new entity, interrupt a
        task, or set a registry value. `owner` labels the action for a warning,
        `entity` is the entity it runs for, and `upstream` is what a spawn hands on.
        """
        if "fire" in action:
            spec = action["fire"]
            copied = self._copy_outcome(spec, entity)
            if copied is None:
                logger.warning(
                    "the on: action of %s fires '%s' from '%s', which has nothing "
                    "to copy yet; it fires nothing",
                    owner,
                    spec["signal"],
                    spec["from"],
                )
                return
            ok, value = copied
            self._fire_signal(
                spec["signal"],
                ok,
                value,
                entity,
                urgent=spec.get("urgent", False),
                after=spec.get("after"),
            )
        elif "spawn" in action:
            spec = action["spawn"]
            self._spawn_entity(spec["task"], entity, upstream, spec["after"])
        elif "interrupt" in action:
            spec = action["interrupt"]
            where = spec.get("where")
            self.interrupt(
                spec["task"],
                where=Expression(where) if isinstance(where, str) else where,
                cause=spec.get("cause"),
            )
        else:
            apply_step(self.env, action["path"], action["value"])

    def _copy_outcome(
        self, fire: Dict[str, Any], entity: Optional[Entity]
    ) -> Optional[Tuple[bool, Any]]:
        """The outcome a `fire` action fires with: its literal `value`, or the
        outcome `from` copies from a signal or a task. None when there is nothing to
        copy (a signal not fired, a task with no ended instance, or no entity)."""
        source = fire["from"]
        if source is None:
            return True, fire["value"]
        if source in self._signals:
            signal = self._signals[source]
            if signal.ok is None:
                return None
            return signal.ok, signal.value
        # a task: copy its last-ended instance on this entity
        if (
            entity is None
            or entity.last_ended is None
            or source not in entity.last_ended
        ):
            return None
        event = entity.last_ended[source]
        if event.ok:
            return True, event.value
        return False, getattr(event.value, "reason", event.value)

    def add_signal(
        self,
        name: str,
        once: bool = False,
        on: Optional[List[Mapping[str, Any]]] = None,
    ) -> "SimulationContext":
        """
        Declares a signal that tasks fire and wait for.

        A task fires it with `signal=name` when it finishes, and fails it when the
        task fails; a task waits for it with `wait={"signal": name}`; a process
        reaches it with `context.signal(name)`.

        Args:
            name: The signal's name.
            once: When True the signal fires once, as SimPy's `Event` does: it does
                not re-arm, a wait after the firing resumes at once, and a second
                firing is refused.
            on: Actions run when the signal fires, each a mapping with one of `fire`,
                `spawn`, `interrupt` or `path`, as a task's `on` takes them.

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        if name not in self._signal_names:
            self._signal_names.append(name)
        self._signal_once[name] = once
        self._signal_on[name] = [_parse_on_action(entry) for entry in on or []]
        return self

    def signal(self, name: str) -> Signal:
        """
        Returns a declared signal, to fire, fail or wait for from a process.

        Args:
            name: A name `add_signal` declared.

        Returns:
            Signal: The signal: `fire(value)`, `fail(reason)`, `wait()`, and its
                `count`, `ok` and `value`.

        Raises:
            RuntimeError: If called before the run starts.
        """
        if not self._env:
            raise RuntimeError(
                "Signals are not created until context is run; read them from a "
                "task or a process"
            )
        return self._signals[name]

    def interrupt(
        self,
        task: str,
        where: Union[str, Expression, None] = None,
        cause: Any = None,
    ) -> int:
        """
        Interrupts every running instance of a task whose condition holds.

        Each instance receives SimPy's `Interrupt` with `cause`, publishes
        `interrupted` and follows its task's `on_interrupt`. The caller's own
        instance is never interrupted, and an instance that has finished is not
        running.

        Args:
            task: The task's name.
            where: A condition read for each instance, such as `"attr.order == 3"`;
                None interrupts every instance.
            cause: What the instances receive as the interrupt's cause, published
                as the record's `cause`.

        Returns:
            int: How many instances were interrupted.

        Raises:
            RuntimeError: If called before the run starts.
        """
        if not self._env:
            raise RuntimeError(
                "Cannot interrupt tasks before context is run; interrupt them from a "
                "task, a process or a scenario step"
            )
        condition = Expression(where) if isinstance(where, str) else where
        active = self._env.active_process
        count = 0
        for run in list(self._running.get(task, [])):
            process = run.process
            if process is None or process is active or not process.is_alive:
                continue
            if condition is not None and not condition.evaluate(run.scope()):
                continue
            process.interrupt(cause)
            count += 1
        return count

    # ==========================================
    # ORCHESTRATION (The Compilation Phase)
    # ==========================================

    def compile_parameters(self) -> SimParameter:
        """
        Compiles the builder state into the `SimParameter` that `run()` registers.

        Useful before the run, to see which registry paths a configuration creates
        or to compare two configurations.

        Returns:
            SimParameter: The parameters, sharing their config objects with the builder.
        """
        return SimParameter(
            sim_id=self.sim_id,
            resources=self._resources_config,
            containers=self._containers_config,
            stores=self._stores_config,
            breakdowns=self._breakdowns_config,
            service=self._services_config,
            arrival=self._arrivals_config,
            variables=self._variables_config,
        )

    def analyze(
        self,
        verify: bool = False,
        *,
        until: Union[float, str, None] = None,
        seed: Optional[int] = None,
        replications: Optional[int] = None,
        target: float = 0.85,
    ) -> "Explanation":
        """
        Explains the model: its graph, and the queueing estimates that apply to it.

        The graph and the analysis come from what the builder declared, before
        `start()`, and leave the context as it was: it is not started, and a run
        after it gives the same records as a run without it. `desflow explain`
        prints the same `Explanation`.

        Args:
            verify: Also run the model unpaced and compare its statistics with the
                analysis. The run uses a new copy of the model, built again from
                the blueprint or the Python file it came from.
            until: The end of the verifying run, in seconds or as a duration; the
                blueprint's `run.until` when not given.
            seed: The verifying run's seed; the model's `random_seed`, or one
                drawn, when not given.
            replications: Verify with this many runs, 2 or more, and an interval
                for each measure, instead of one run.
            target: The utilisation the tuning hints aim for, above 0 and at most
                10; at 1 or above the hints are for deliberate overload.

        Returns:
            Explanation: The graph, the estimates per arrival, task, resource, store
                and container, the bottleneck, the hints, and with `verify` the
                comparison.

        Raises:
            ValueError: If the model names a task or another part that is not
                added, as `run()` does; if `target` is out of range; or if
                `verify` is asked of a context built in code, which cannot be
                built again, or with no end time.
        """
        from desflow.explain.analysis import analyze
        from desflow.explain.verify import verify as run_verify
        from desflow.explain.graph import build_graph

        self._check_tasks()
        explanation = analyze(build_graph(self), target)
        if not verify:
            return explanation
        if self._source is None:
            raise ValueError(
                "verify needs a model it can build again; use desflow explain "
                "--verify FILE"
            )
        return run_verify(self._source, explanation, until, seed, replications)

    def run(
        self,
        until: Any = None,
        stop: Any = None,
        drain: Any = None,
        on_unhandled_failure: Optional[str] = None,
    ) -> Any:
        """
        Compiles the defined infrastructure architecture and triggers the simulation loop.

        `run()` is `start()`, then SimPy's `env.run` until the end, then `close()`,
        which runs even when the run stops with an error or Ctrl+C.

        Args:
            until: The absolute simulation termination time. Can be a numeric float
                (representing base seconds) or a human-readable string parsed by
                the utility module (e.g., "1 week", "8 hours"). If None, a context
                built by `from_yaml` uses the blueprint's `run.until`, and otherwise
                the simulation runs infinitely. An event due at exactly `until` does
                not run, as in SimPy.
            stop: Ends the run before `until` when it happens: `{"signal": name}`,
                or the signal's name, for the next firing of a signal;
                `{"condition": "..."}` for the first moment a condition holds; or
                a function of the context, called once the run is built, that
                returns a SimPy event, such as `lambda c: c.signal("closing").wait()`.
                When it happens at the same time as `until`, `until` ends the run.
            drain: At `until`, stop the arrivals and keep running until no entity
                is in the model: True, or `{"limit": duration}` to stop at the
                latest that long after `until`. Needs `until`.

        Returns:
            Any: The value of the event that stopped the run, as SimPy's `env.run`
                returns it: a signal's value for a `stop` on a signal, True for a
                condition, and a stop function's event value. None when `until` or
                a drain ends the run, or when `until` and `stop` happen together.

        Raises:
            ValueError: If a task's `next`, `priority`, `get` or `put` names something
                the context does not define, or `stop` or `drain` is malformed.
            SignalFailed: If the signal that `stop` names fails. Any exception of a
                failed stop event is raised the same way. The run closes first, and
                the sinks receive the records.
        """
        self.start(until, stop, drain, on_unhandled_failure=on_unhandled_failure)
        return self.finish()

    def finish(self) -> Any:
        """
        Runs a started run to its end, then closes it, as the rest of `run()` does.

        The end is `until`, the `stop` or the end of the drain that `start()` set
        up. After one or more `advance()` or `step()` calls it runs on from where
        they stopped. When `advance()` already reached the end, it returns at once
        with the end's value: `None` for `until` or a drain, the stop's value for a
        stop, and a failed stop's exception raised again. When `advance()` or `step()`
        has brought the clock to `until` or past it and there is no `stop` or `drain`,
        it returns `None` at once, as a run that ended at `until` does. When `step()`
        brought the clock to exactly `until`, the records hold one run's records
        (which end with the final statistics) plus those of the events at `until`
        that `step()` processed, as in SimPy; a statistics reading due at `until`
        is not published again, and the close adds only the values that changed
        after it. After steps past `until`, the run closes where the steps stopped,
        with the final statistics at that time.
        `close()` runs even when the run stops with an error or Ctrl+C.

        Returns:
            Any: The value of the event that stopped the run, as for `run()`.

        Raises:
            RuntimeError: If the run has not started.
            SignalFailed: If the signal that `stop` names fails, as for `run()`.
        """
        if self._env is None or not self.started:
            raise RuntimeError("finish() needs a run that start() has started")
        try:
            env = self._env
            if self._end is not None:
                if self._end.callbacks is None and not self._end._ok:
                    # `env.run(until=...)` returns the value of a processed event,
                    # so a failed stop that advance() reached is raised here
                    raise self._end._value
                return env.run(until=self._end)
            if self._run_ended():
                # advance() or step() already reached until, where a run ends
                return None
            return env.run(until=self._until)
        finally:
            self.close()

    def advance(self, until: Union[float, str, simpy.Event]) -> Any:
        """
        Runs a started run up to `until`, then returns with the run still open.

        Call it again to run the next segment, or `finish()` to run to the end.
        Segments give the same records as one run, because nothing is flushed at a
        segment boundary. Between segments the model can be read, changed with
        `context.env`, and stepped with `step()`. `advance()` never moves the
        wall-clock anchor, so a paced run that waits between segments is behind
        when it goes on; call `context.env.resume_pacing()` first to pace again from
        `now`.

        The run stops at the earlier of `until` and its own end: `start()`'s `until`,
        the `stop` or the end of the `drain`. An `until` that is an event, such as
        `context.signal("gate").wait()`, runs to that event and returns its value;
        one that has been processed already returns at once.

        Args:
            until: A time in simulation seconds or as a duration such as `"10 min"`,
                after `now`; or a SimPy event.

        Returns:
            Any: None when the run stops at a time, the event's value for an event,
                and the value `run()` returns when the run reaches its own end.

        Raises:
            RuntimeError: If the run has not started or is closed, or it has ended
                (`close()` it, or `start()` a new run).
            ValueError: If `until` is not after `now`, as SimPy refuses it.
            SignalFailed: If the signal that `stop` names fails, as for `run()`.
        """
        if self._env is None or not self.started:
            raise RuntimeError("advance() needs a run that start() has started")
        env = self._env
        if self._run_ended():
            raise RuntimeError(
                f"the run ended at {env.now:g}; close() it, or start() a new run"
            )
        if isinstance(until, str):
            until = time_to_seconds(until)
        if isinstance(until, simpy.Event):
            if until.callbacks is None:
                return until.value
            return self._advance_to(until)
        # SimPy's own text and number format, as `env.run(until=...)` raises it
        at = until if isinstance(until, int) else float(until)
        if at <= env.now:
            raise ValueError(
                f"until ({at}) must be greater than the current simulation time"
            )
        if self._end is None:
            return env.run(
                until=until if self._until is None else min(until, self._until)
            )
        segment = simpy.Event(env)
        segment._ok, segment._value = True, None
        env.schedule(segment, simpy.events.URGENT, until - env.now)
        return self._advance_to(segment)

    def _run_ended(self) -> bool:
        """Whether the run has reached its own end: the end event of a `stop` or
        `drain` has been processed, else `until` has been reached."""
        if self._end is not None:
            return self._end_done
        return (
            self._until is not None
            and self._env is not None
            and self._env.now >= self._until
        )

    def _advance_to(self, target: simpy.Event) -> Any:
        """Runs to `target`, or to the run's own end if it comes first, and returns
        the value of whichever stopped the run."""
        env = self._env
        assert env is not None  # advance() checked that the run has started
        limit = self._end
        if limit is None and self._until is not None:
            # No stop or drain: the run's end is `until`, as `env.run(until)` ends it.
            # One internal event per run, so later calls add nothing to the queue.
            if self._limit_event is None:
                self._limit_event = env.internal_event()
                self._limit_event._ok, self._limit_event._value = True, None
                env.schedule(
                    self._limit_event, simpy.events.URGENT, self._until - env.now
                )
            limit = self._limit_event
        stop = StopSimulation.callback
        if limit is not None:
            limit.callbacks.append(stop)
        try:
            return env.run(until=target)
        finally:
            # An event that did not stop the run must not stop a later one
            for event in (limit, target):
                if event is not None and event.callbacks and stop in event.callbacks:
                    event.callbacks.remove(stop)

    def start(
        self,
        until: Any = None,
        stop: Any = None,
        drain: Any = None,
        *,
        seed: Optional[int] = None,
        overrides: Optional[Mapping[str, Any]] = None,
        stats: Optional[str] = None,
        egress: bool = True,
        ingress: bool = True,
        run_settings: bool = True,
        on_unhandled_failure: Optional[str] = None,
    ) -> DynamicRealtimeEnvironment:
        """
        Builds and starts everything `run()` does before the clock moves.

        It checks the tasks, calls the blueprint's `run.before` functions, builds the
        environment, the sampler, the statistics, the parameters, the connectors,
        the resources, stores and containers, and starts every loop and process.
        Then `step()` processes one event at a time and `peek()` reads the time of
        the next, as SimPy's `env.step()` and `env.peek()` do; `close()` ends the
        run. A context built by `from_yaml` takes its `run` section's values.

        Replications and experiments start each run here too, with the arguments after
        `drain`, and finish it with `finish()`, so a replication builds and ends
        exactly as `run()` does, `run.stop` and `run.drain` included.

        Args:
            until: As for `run()`. With `step()` it is not enforced: `peek()` tells
                you when to stop.
            stop: As for `run()`.
            drain: As for `run()`.
            seed: The run's seed, in place of `random_seed`.
            overrides: Registry paths and the values they take before the clock
                starts, applied as a live update is, except that a value the
                registry would refuse or clamp raises.
            stats: The lowest statistics level the run keeps; a higher level set
                with `with_stats` stays.
            egress: False builds the run without its sinks and without the
                statistics timer, as a replication runs: nothing receives its
                records, and its statistics are read at the end.
            ingress: False builds the run without its ingress, so no connector
                listens for parameter changes.
            run_settings: False leaves out a blueprint's `run.until`, `run.stop`
                and `run.drain`, for a run whose clock never moves, such as the
                one that sends replications' summary to the sinks.

        Returns:
            DynamicRealtimeEnvironment: The environment, also `context.env`.

        Raises:
            ValueError: As for `run()`; or `stats` is not a level, or an override's
                path does not exist or its value is refused.
        """
        if until is None and run_settings:
            until = self._default_until
        if stop is None and run_settings:
            stop = self._default_stop
        if drain is None and run_settings:
            drain = self._default_drain
        if on_unhandled_failure is None:
            on_unhandled_failure = (
                self._default_unhandled if run_settings else "continue"
            )
        self._on_unhandled_failure = on_unhandled_failure
        # Read before any connector starts its thread, so a bad value stops the run
        # with nothing to tear down
        if isinstance(until, str):
            until = time_to_seconds(until)
        if until is not None and until <= self.initial_time:
            # SimPy's run(until=) refuses it; with stop or drain the end is an
            # event desflow schedules itself, which would be in the past
            raise ValueError(
                f"until ({until:g}) must be greater than the initial time "
                f"({self.initial_time:g}); set until later, or initial_time earlier"
            )
        if isinstance(drain, Mapping) and drain.get("limit") is not None:
            _seconds(drain["limit"])
        if stats is not None and stats not in STATS_LEVELS:
            raise ValueError(
                f"stats must be one of {', '.join(STATS_LEVELS)}, got {stats!r}"
            )
        _check_end(until, stop, drain, self._signal_names)
        self._check_tasks()
        for setup in self._before_run:
            setup()
        # A run started and not closed is closed first, so its connectors stop
        if self._env is not None and not self._closed:
            self.close()
        # A second run of the same context starts with no entity and no task, and
        # numbers its entities and instances from the start again, so it repeats
        self._in_system = 0
        self._draining = False
        self._running = {}
        self._entity_counters = {}
        self._instance_counters = {}
        self._sample_seen = 0
        self._env = None
        self.started = False

        logger.info(f"Building SimulationContext for '{self.sim_id}'...")
        try:
            return self._build(
                until, stop, drain, seed, overrides, stats, egress, ingress
            )
        except BaseException:
            # The connectors may have started their threads; stop them, as run()
            # would, so a failed start leaves no thread behind
            if self._env is not None:
                self._env.teardown()
            raise

    def _build(
        self,
        until: Any,
        stop: Any,
        drain: Any,
        seed: Optional[int],
        overrides: Optional[Mapping[str, Any]],
        stats: Optional[str],
        egress: bool,
        ingress: bool,
    ) -> DynamicRealtimeEnvironment:
        """The part of `start()` after the checks: builds and starts the run."""

        env_kwargs: Dict[str, Any] = {"factor": self.factor, "strict": self.strict}
        if self.initial_time:
            env_kwargs["initial_time"] = self.initial_time
        if self.logical_start_time is not None:
            env_kwargs["logical_start_time"] = self.logical_start_time
        if self.go_live_at is not None:
            env_kwargs["go_live_at"] = self.go_live_at
        if self.timezone is not None:
            env_kwargs["timezone"] = self.timezone
        env_kwargs["on_unhandled_failure"] = self._on_unhandled_failure
        self._env = self.environment(**env_kwargs)

        # One random sequence per distribution, all from the run's seed
        if seed is None:
            seed = self.random_seed
        if seed is None:
            seed = secrets.randbelow(2**32)
            logger.info(
                f"No random_seed was given, so this run uses {seed}. "
                + (
                    self._repeat_hint.format(seed=seed)
                    if self._repeat_hint
                    else f"Pass random_seed={seed} to repeat it."
                )
            )
        self.sampler = Sampler(seed=seed)
        for task_name, spec in self._task_specs.items():
            if spec.router.weighted:
                spec.router.rng = self.sampler.generator(f"route.{task_name}")

        level = self._stats_level
        if stats is not None and STATS_LEVELS.index(stats) > STATS_LEVELS.index(level):
            level = stats
        if level != "none" and egress:
            self._env.setup_stats(self.sim_id, level, self._stats_interval)
        elif level != "none":
            # Without sinks nothing receives a publication, so no timer runs and
            # the statistics are read at the end
            self._env.stats.configure(self.sim_id, level, self._stats_interval)
        self._signals = {
            name: Signal(self._env, name, once=self._signal_once.get(name, False))
            for name in self._signal_names
        }
        for name, actions in self._signal_on.items():
            if actions:
                self._signals[name].attach(
                    [functools.partial(self._signal_fired, name)]
                )
        self._replay_rows = {
            name: read_trace(
                **spec,
                start=self._env.start_datetime,
                initial_time=float(self.initial_time),
            )
            for name, spec in self._replays.items()
        }
        self._replays_done = set()

        # Compile Master Parameter Object
        self._env.registry.register_sim_parameter(self.compile_parameters())
        for path, value in (overrides or {}).items():
            self._env.registry.set_exactly(path, value)

        # Connect External Infrastructure
        if self._ingress_providers and ingress:
            self._env.setup_ingress(self._ingress_providers)

        if self._egress_providers and egress:
            self._env.setup_egress(
                self._egress_providers,
                batch_size=self._batch_size,
                flush_interval=self._flush_interval,
                max_queued_batches=self._max_queued_batches,
                drain_stall_seconds=self._drain_stall_seconds,
                predicates=self._egress_predicates,
                batch_sizes=self._egress_batch_sizes,
                flush_intervals=self._egress_flush_intervals,
                labels=self._egress_labels,
                warmup=self.warmup,
                lag_monitor_interval=(
                    "auto" if self.lag_interval is None else self.lag_interval
                ),
            )
        elif self.warmup is not None:
            self._env.start_internal(self._end_warmup(self.warmup))

        # Hydrate Physical SimPy Resources
        for res_id in self._resources_config.keys():
            resource_type = self._resource_types.get(res_id) or DynamicResource
            self._resources_map[res_id] = resource_type(
                self._env,
                self.sim_id,
                res_id,
                queue=self._resource_queues[res_id],
                preempt=self._resource_preempt.get(res_id, False),
                **(
                    {"queue_limit": self._resource_queue_limits[res_id]}
                    if self._resource_queue_limits.get(res_id) is not None
                    else {}
                ),
            )
        for store_id in self._stores_config:
            self._stores_map[store_id] = DynamicStore(
                self._env,
                self.sim_id,
                store_id,
                priority=self._store_priority.get(store_id, False),
                items=self._store_items.get(store_id),
                type=self._store_types.get(store_id),
            )
        for container_id in self._containers_config:
            self._containers_map[container_id] = DynamicContainer(
                self._env,
                self.sim_id,
                container_id,
                type=self._container_types.get(container_id),
            )
        for res_id, breakdown in self._breakdowns_config.items():
            self._env.process(
                run_breakdowns(
                    self._env,
                    self._resources_map[res_id],
                    self.sampler,
                    work=breakdown.work,
                )
            )

        # Boot Registered Daemon Generators
        self._arrival_processes = []
        for loop_func, meta in self._startup_loops:
            if meta:
                # Arrival loops require context injection
                process = self._env.process(loop_func(self))
                # Stopped by an interrupt when a drain starts, which ends the loop;
                # any other error in the loop still stops the run, as in SimPy
                process.callbacks.append(_defuse_arrivals_stopped)
                self._arrival_processes.append(process)
            else:
                # Telemetry loops run blindly
                self._env.process(loop_func())

        self._until = until
        self._end_done = False
        self._limit_event = None
        self._end = self._end_event(until, stop, drain)
        self._closed = False
        self.started = True
        if self.strict:
            # Building takes wall time, which a strict run must not count as lag
            self._env.sync()
        logger.info("Simulation engine started.")
        return self._env

    def step(self) -> None:
        """
        Processes the next model event of a started run, as SimPy's `env.step()` does.

        The events of desflow's own that are due before it, such as a statistics
        reading or a flush timer, run in the same call and are not counted.

        Raises:
            RuntimeError: If the run has not started.
            simpy.core.EmptySchedule: If no model event is left.
        """
        self.env.step()

    def peek(self) -> float:
        """
        Returns the time of the next model event of a started run, or infinity if none.

        The events of desflow's own, such as a statistics reading or a flush timer,
        are not counted.

        Raises:
            RuntimeError: If the run has not started.
        """
        return self.env.peek()

    def close(self) -> None:
        """
        Ends a started run: publishes the last statistics, flushes and stops the
        egress connectors, then stops the ingress connectors. Closing twice does
        nothing.
        """
        if self._env is not None and not self._closed:
            self._closed = True
            self.started = False
            self._env.teardown()

    def stop_arrivals(self) -> None:
        """
        Stops every arrival loop, so no new entity arrives; the entities already in
        the model carry on. A drain calls it at `until`.
        """
        for process in getattr(self, "_arrival_processes", []):
            if process.is_alive:
                process.interrupt(_ARRIVALS_STOPPED)

    def _end_event(self, until: Any, stop: Any, drain: Any) -> Optional[simpy.Event]:
        """The event that ends the run when `stop`, `drain` or a sample limit is
        given, else None.

        It is scheduled urgent, like SimPy's own `until`, so no other event at that
        time runs after it. A sample's console calls `_finish_run` at its last event.
        """
        if stop is None and not drain and self._sample_limit is None:
            return None
        env = self.env
        end = env.internal_event()

        def finish(event: Any = None) -> None:
            if not end.triggered:
                if event is None:
                    end._ok, end._value = True, None
                else:
                    # The run ends with what its stop event carried: a signal's
                    # value, True for a condition, or the exception of a failed
                    # stop, which `end` raises once. The stop event is defused so
                    # the run does not raise it a second time.
                    end._ok, end._value = event._ok, event._value
                    if not event._ok:
                        event.defused = True
                env.schedule(end, simpy.events.URGENT)

        self._finish_run = finish
        limit = None
        if drain:
            if until is None:
                raise ValueError("a drain starts at until, so it needs until")
            if isinstance(drain, Mapping):
                unknown = {*drain} - {"limit"}
                if unknown:
                    raise ValueError(
                        f"drain takes a limit, not {', '.join(sorted(unknown))}"
                        + suggestion(sorted(unknown)[0], ["limit"])
                    )
                if drain.get("limit") is not None:
                    limit = _seconds(drain["limit"])
        if until is not None:
            horizon = env.internal_event()
            horizon._ok, horizon._value = True, None
            env.schedule(horizon, simpy.events.URGENT, float(until) - env.now)
            if drain:

                def begin_drain(_event: Any) -> None:
                    self.stop_arrivals()
                    self._draining = True
                    if self._in_system == 0:
                        finish()
                    elif limit is not None:
                        timer = env.internal_event()
                        timer._ok, timer._value = True, None
                        timer.callbacks.append(finish)
                        env.schedule(timer, simpy.events.URGENT, limit)

                horizon.callbacks.append(begin_drain)
            else:
                horizon.callbacks.append(finish)
        if stop is not None:
            self._stop_event(stop).callbacks.append(finish)
        end.callbacks.append(self._mark_end)
        return end

    def _mark_end(self, _event: Any) -> None:
        """Records that the end event has been processed, for `advance()`."""
        self._end_done = True

    def _stop_event(self, stop: Any) -> simpy.Event:
        """The event a `stop` names: a signal's next firing, a condition, or an event."""
        if isinstance(stop, str):
            stop = {"signal": stop}
        elif callable(stop):
            # Built once the run exists, so it can read the run's own objects
            event = stop(self)
            if not isinstance(event, simpy.Event):
                raise ValueError(
                    f"a stop function returns a SimPy event, got {type(event).__name__}"
                )
            return event
        if not isinstance(stop, Mapping) or len(stop) != 1:
            raise ValueError(
                f"stop takes {{'signal': name}} or {{'condition': '...'}}, got {stop!r}"
            )
        ((kind, target),) = stop.items()
        if kind == "signal":
            if target not in self._signals:
                raise ValueError(
                    f"stop names signal '{target}', "
                    + _not_added(target, self._signals, f"add_signal('{target}')")
                )
            return self._signals[target].wait()
        if kind != "condition":
            raise ValueError(f"stop takes a signal or a condition, not '{kind}'")
        condition = Expression(target)
        env, event = self.env, self.env.event()

        def check() -> None:
            scope = make_scope(
                None,
                {},
                env,
                self._levels(),
                signals=self._signals,
                resources=self._resources_map,
            )
            if not event.triggered and condition.evaluate(scope):
                event.succeed(True)
                env.unwatch_changes(check)

        check()
        if not event.triggered:
            env.watch_changes(check, queues=condition.reads_queues)
        return event

    def _entity_entered(self, entity: Entity) -> None:
        """Counts an entity whose first task instance has begun as in the model."""
        self._in_system += 1
        self.env.stats.flow_started(entity.arrival)

    def _entity_left(self, entity: Entity) -> None:
        """Counts an entity out; a drain ends when the last one leaves.

        It drops the entity's instance lists, so a live run keeps lists only for
        entities still in the model and a `running` branch reads None as no instance.
        """
        self._in_system -= 1
        entity.instances = None
        entity.spawned_by = None
        entity.last_ended = None
        if self._draining and self._in_system == 0 and self._finish_run is not None:
            self._finish_run()

    def _end_warmup(self, warmup: float):
        """Resets the statistics at the end of the warm-up, for a run with no sink."""
        yield self.env.internal_timeout(warmup)
        self.env.stats.reset()

    def _build_end_sets(self) -> None:
        """Fills `_tracked`, `_copied` and `_ended` and checks the `spawn` and `on:`
        names, so a task knows at build time whether its instances need an end event.
        """
        self._tracked = set()
        self._copied = set()
        has_on = set()
        arrivals = set(self._arrival_entities)

        def fire_sources(actions: Iterable[Mapping[str, Any]]) -> Iterable[str]:
            for action in actions:
                source = (
                    action.get("fire", {}).get("from") if "fire" in action else None
                )
                if source is not None:
                    yield source

        def check_spawn(task: str, owner: str, spawned: str) -> None:
            if spawned not in self._task_specs:
                raise ValueError(
                    f"{owner} spawns task '{spawned}', "
                    + _not_added(spawned, self._task_specs, "@app.task()")
                )
            if spawned in arrivals:
                raise ValueError(
                    f"task '{spawned}', which {owner} spawns, has the name of arrival "
                    f"'{spawned}'; rename the task or the arrival, because their flow "
                    f"statistics and entity numbers would merge"
                )

        def check_action(name: str, action: Mapping[str, Any], owner: str) -> None:
            if "spawn" in action:
                check_spawn(name, owner, action["spawn"]["task"])
            if "interrupt" in action:
                target = action["interrupt"]["task"]
                if target not in self._task_specs:
                    raise ValueError(
                        f"the on: action of {owner} interrupts task '{target}', "
                        + _not_added(target, self._task_specs, "@app.task()")
                    )
            if "fire" in action:
                signal = action["fire"]["signal"]
                if signal not in self._signal_names:
                    raise ValueError(
                        f"the on: action of {owner} fires signal '{signal}', "
                        + _not_added(
                            signal, self._signal_names, f"add_signal('{signal}')"
                        )
                    )
                fire = action["fire"]
                if (fire.get("urgent") or fire.get("after") is not None) and not (
                    self._signal_once.get(signal, False)
                ):
                    raise ValueError(
                        "`urgent` and `after` need a one-shot signal (`once=True`); "
                        "a re-arming signal fires now"
                    )
                if fire["from"] is not None:
                    check_from(fire["from"], owner)

        def check_from(source: str, owner: str) -> None:
            is_signal = source in self._signal_names
            is_task = source in self._task_specs
            if is_signal and is_task:
                raise ValueError(
                    f"the on: action of {owner} fires from '{source}', which is both "
                    f"a signal and a task; rename one"
                )
            if not is_signal and not is_task:
                raise ValueError(
                    f"the on: action of {owner} fires from '{source}', which is "
                    f"neither a signal nor a task"
                )

        for name, spec in self._task_specs.items():
            owner = f"task '{name}'"
            if spec.wait is not None:
                self._tracked |= spec.wait.targets("running")
            if spec.subscribe is not None:
                self._tracked |= spec.subscribe.targets("running")
                for target in spec.subscribe.targets("task"):
                    if target not in self._task_specs:
                        raise ValueError(
                            f"task '{name}' subscribes to task '{target}', "
                            + _not_added(target, self._task_specs, "@app.task()")
                        )
                for target in spec.subscribe.targets("signal"):
                    if target not in self._signal_names:
                        raise ValueError(
                            f"task '{name}' subscribes to signal '{target}', "
                            + _not_added(
                                target, self._signal_names, f"add_signal('{target}')"
                            )
                        )
            if spec.on:
                has_on.add(name)
            if spec.spawn is not None:
                check_spawn(name, owner, spec.spawn["task"])
            for action in spec.on:
                check_action(name, action, owner)
            for source in fire_sources(spec.on):
                if source in self._task_specs:
                    self._copied.add(source)

        for name, actions in self._signal_on.items():
            owner = f"signal '{name}'"
            for action in actions:
                check_action(name, action, owner)
            for source in fire_sources(actions):
                if source in self._task_specs:
                    self._copied.add(source)

        self._ended = self._tracked | self._copied | has_on

    def _check_tasks(self) -> None:
        """Checks what each task names, before the clock or any connector starts."""
        self._build_end_sets()
        if self._scenario_steps:
            known = SimulationRegistry(simpy.Environment())
            known.register_sim_parameter(self.compile_parameters())
            for step in self._scenario_steps:
                if step.get("path") is not None:
                    try:
                        known.get(step["path"])
                    except KeyError:
                        raise ValueError(
                            f"scenario step at {step['at']:g} s sets '{step['path']}', "
                            f"which is not a registry path"
                            + suggestion(step["path"], known.paths())
                        ) from None
                action = step.get("interrupt")
                if action is not None and action["task"] not in self._tasks:
                    raise ValueError(
                        f"scenario step at {step['at']:g} s interrupts task "
                        f"'{action['task']}', "
                        + _not_added(action["task"], self._tasks, "@app.task()")
                    )
        for arrival, task in self._arrival_spawns.items():
            if task not in self._tasks:
                raise ValueError(
                    f"arrival '{arrival}' spawns task '{task}', "
                    + _not_added(task, self._tasks, "@app.task()")
                )
        for resource in self._breakdowns_config:
            if resource not in self._resources_config:
                raise ValueError(
                    f"a breakdown names resource '{resource}', "
                    + _not_added(resource, self._resources_config, "add_resource()")
                )
        for name, spec in self._task_specs.items():
            for resource in spec.wait.requests() if spec.wait else ():
                if resource in spec.resources:
                    raise ValueError(
                        f"task '{name}' holds each resource once, but resource "
                        f"'{resource}' is both requested in the wait and listed in "
                        f"resource; list it in only one"
                    )
        check_acquisition_order(
            {name: list(spec.resources) for name, spec in self._task_specs.items()},
            {
                name: (spec.keep, spec.router.tasks or set())
                for name, spec in self._task_specs.items()
            },
            {name: spec.wait for name, spec in self._task_specs.items()},
        )
        holders = {**self._stores_config, **self._containers_config}
        registry: Optional[SimulationRegistry] = None
        for name, spec in self._task_specs.items():
            for resource in spec.resources:
                if resource not in self._resources_config:
                    raise ValueError(
                        f"task '{name}' uses resource '{resource}', "
                        + _not_added(resource, self._resources_config, "add_resource()")
                    )
            if (
                spec.service_id is not None
                and spec.service_id not in self._services_config
            ):
                raise ValueError(
                    f"task '{name}' uses service '{spec.service_id}', "
                    + _not_added(
                        spec.service_id, self._services_config, "add_service()"
                    )
                )
            for resource in (
                (*(spec.wait.requests() if spec.wait else ()), *spec.resources)
                if spec.priority is not None
                else ()
            ):
                if (
                    resource in self._resources_config
                    and self._resource_queues.get(resource) != "priority"
                ):
                    raise ValueError(
                        f"task '{name}' has a priority, but resource '{resource}' "
                        f"does not queue by priority; add it with queue='priority'"
                    )
            for transfer in (spec.get, spec.put):
                if transfer is None:
                    continue
                kind = "store" if transfer.store is not None else "container"
                config = (
                    self._stores_config if kind == "store" else self._containers_config
                )
                if transfer.target not in config:
                    raise ValueError(
                        f"task '{name}' uses {kind} '{transfer.target}', which is not added"
                    )
                if transfer.filter is not None and self._store_priority.get(
                    transfer.target
                ):
                    raise ValueError(
                        f"task '{name}' filters priority store '{transfer.target}'; "
                        f"SimPy has no store that both ranks and filters its items"
                    )
                if (
                    transfer.filter is not None
                    and kind == "store"
                    and (store_type := self._store_types.get(transfer.target))
                    is not None
                    and not issubclass(store_type, simpy.FilterStore)
                ):
                    raise ValueError(
                        f"task '{name}' filters store '{transfer.target}', whose "
                        f"type is not a simpy.FilterStore; subclass FilterStore to "
                        f"filter its gets"
                    )
            for node in spec.wait.branches.values() if spec.wait else ():
                if node.kind == "request" and node.target not in self._resources_config:
                    raise ValueError(
                        f"task '{name}' requests resource '{node.target}' in a wait, "
                        "which is not added"
                    )
                if node.kind in ("get", "put"):
                    kind = "store" if node.target.store is not None else "container"
                    config = (
                        self._stores_config
                        if kind == "store"
                        else self._containers_config
                    )
                    if node.target.target not in config:
                        raise ValueError(
                            f"task '{name}' {node.kind}s from {kind} "
                            f"'{node.target.target}' in a wait, which is not added"
                        )
            for target in spec.router.tasks or ():
                if target not in self._tasks:
                    raise ValueError(
                        f"task '{name}' routes to '{target}', which is not a task"
                    )
            for target in (
                spec.on_fail,
                spec.on_interrupt_next,
                spec.interrupt["task"] if spec.interrupt else None,
                *(spec.wait.targets("task") if spec.wait else ()),
            ):
                if target is not None and target not in self._tasks:
                    raise ValueError(
                        f"task '{name}' names task '{target}', "
                        + _not_added(target, self._tasks, "@app.task()")
                    )
            signals = set(spec.wait.targets("signal")) if spec.wait else set()
            if spec.signal is not None:
                signals.add(spec.signal)
            expressions = [expression for _key, expression in spec.expressions]
            for expression in expressions:
                signals |= expression.references["signal"]
            for signal in signals:
                if signal not in self._signal_names:
                    raise ValueError(
                        f"task '{name}' uses signal '{signal}', which is not added; "
                        f"add it with add_signal('{signal}')"
                    )
            branches = set(spec.wait.branches) if spec.wait else set()
            for key, expression in spec.expressions:
                unknown = expression.references["wait"] - branches
                # balk and patience are read before the wait starts
                early = key in ("wait", "balk", "patience")
                if unknown or (expression.references["wait"] and early):
                    branch = sorted(unknown or expression.references["wait"])[0]
                    raise ValueError(
                        f"task '{name}' reads wait.{branch} in '{expression.text}', "
                        f"which is not a branch of its wait read after it"
                    )
                # item.<field> reads what the task's store get took, so it means
                # nothing before the get, and nothing without one
                takes_item = spec.get is not None and spec.get.store is not None
                # A get that is not waited for gives the task no item, so only its
                # own filter reads one
                after_get = key == "get" or (
                    takes_item
                    and spec.get is not None
                    and spec.get.wait
                    and key in ("put", "next", "fail")
                )
                if expression.references["item"] and not (takes_item and after_get):
                    raise ValueError(
                        f"task '{name}' reads item.<field> in '{expression.text}', "
                        f"but it has no store get before it"
                    )
            for expression in expressions:
                for level in expression.references["level"]:
                    if level not in holders:
                        raise ValueError(
                            f"task '{name}' reads level.{level}, but no store or "
                            f"container is called '{level}'"
                        )
                for kind in ("putters", "getters"):
                    for holder in expression.references[kind]:
                        if holder not in holders:
                            raise ValueError(
                                f"task '{name}' reads {kind}.{holder}, but no store "
                                f"or container is called '{holder}'"
                            )
                for kind in ("count", "items"):
                    for store in expression.references[kind]:
                        if store not in self._stores_config:
                            raise ValueError(
                                f"task '{name}' reads {kind}({store}), but no store "
                                f"is called '{store}'"
                            )
                for kind in ("users", "queue"):
                    for resource in expression.references[kind]:
                        if resource not in self._resources_config:
                            raise ValueError(
                                f"task '{name}' reads {kind}.{resource}, but no "
                                f"resource is called '{resource}'"
                                + (
                                    suggestion(resource, self._resources_config)
                                    or "; add it with add_resource()"
                                )
                            )
                for path in expression.references["registry"]:
                    if registry is None:
                        # The paths the run will register, built by the same code
                        registry = SimulationRegistry(simpy.Environment())
                        registry.register_sim_parameter(self.compile_parameters())
                    try:
                        registry.get(path)
                    except KeyError:
                        raise ValueError(
                            f"task '{name}' reads registry[\"{path}\"], which is "
                            f"not a registry path"
                        ) from None


def _whole_or_unlimited(value: Any) -> bool:
    """Whether a store capacity is a whole number or positive infinity."""
    if isinstance(value, (str, bytes)):
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return number == math.inf or (math.isfinite(number) and number.is_integer())


def _check_end(until: Any, stop: Any, drain: Any, signals: List[str]) -> None:
    """Checks `stop` and `drain` before anything is built."""
    if drain and until is None:
        raise ValueError("a drain starts at until, so it needs until")
    if isinstance(drain, Mapping) and {*drain} - {"limit"}:
        raise ValueError(
            f"drain takes a limit, not {', '.join(sorted({*drain} - {'limit'}))}"
            + suggestion(sorted({*drain} - {"limit"})[0], ["limit"])
        )
    if isinstance(stop, str):
        stop = {"signal": stop}
    if stop is None or (callable(stop) and not isinstance(stop, Mapping)):
        return
    if not isinstance(stop, Mapping) or len(stop) != 1:
        raise ValueError(
            f"stop takes {{'signal': name}} or {{'condition': '...'}}, got {stop!r}"
        )
    ((kind, target),) = stop.items()
    if kind == "signal":
        if target not in signals:
            raise ValueError(
                f"stop names signal '{target}', "
                + _not_added(target, signals, f"add_signal('{target}')")
            )
    elif kind == "condition":
        Expression(target)
    else:
        raise ValueError(f"stop takes a signal or a condition, not '{kind}'")


def _not_added(name: str, added: Iterable[str], how: str) -> str:
    """The end of a message for a name nothing added: the closest, or how to add it."""
    return "which is not added" + (suggestion(name, added) or f"; add it with {how}")


_ARRIVALS_STOPPED = "arrivals stopped"


def _spawn_loop(arrival: str, task: str) -> Callable:
    """The arrival loop of an arrival with `spawn`: each entity starts `task`.

    The instance id comes from the task's one counter on the context, which `next`
    and every other arrival that spawns the task share.
    """

    def loop(context: "SimulationContext"):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context._start(task, entity, None)

    return loop


def _defuse_arrivals_stopped(event: Any) -> None:
    """Marks an arrival loop ended by `stop_arrivals` as handled, and nothing else."""
    value = getattr(event, "_value", None)
    if (
        not event.ok
        and isinstance(value, simpy.Interrupt)
        and value.cause == _ARRIVALS_STOPPED
    ):
        event.defused = True


def _defuse(event: Any) -> None:
    """Marks a failed child flow as handled, so a failure nobody waits for ends that
    flow and not the run, as the builder and YAML treat every task failure."""
    if not event.ok:
        event.defused = True


def _parse_spawn(spec: Mapping[str, Any]) -> Dict[str, Any]:
    """Checks a `spawn:` key or spawn action and returns `{task, after}`."""
    if not isinstance(spec, Mapping):
        raise ValueError(f"a spawn is a mapping with a task, got {spec!r}")
    extra = set(spec) - {"task", "after"}
    if extra:
        raise ValueError(
            f"a spawn takes task and after, not {', '.join(sorted(extra))}"
        )
    task = spec.get("task")
    if not isinstance(task, str):
        raise ValueError(f"a spawn names a task, got {task!r}")
    after = spec.get("after")
    if isinstance(after, str):
        after = time_to_seconds(after)
    if isinstance(after, bool):
        raise ValueError(f"a spawn after is seconds or a duration, got {after!r}")
    if after is not None and not after > 0:
        raise ValueError(f"a spawn after is more than 0, got {after:g}")
    return {"task": task, "after": after}


def _parse_on_action(entry: Mapping[str, Any]) -> Dict[str, Any]:
    """Checks one `on:` action and returns it normalized: `{fire: {signal, value,
    from}}`, `{spawn: {task, after}}`, `{interrupt: {...}}` or `{path, value}`."""
    if not isinstance(entry, Mapping):
        raise ValueError(f"an on: action is a mapping, got {entry!r}")
    present = [
        k for k in ("fire", "spawn", "interrupt", "path") if entry.get(k) is not None
    ]
    if len(present) != 1:
        raise ValueError(
            "an on: action takes exactly one of fire, spawn, interrupt or path"
        )
    kind = present[0]
    if kind == "fire":
        fire = dict(entry["fire"])
        signal = fire.get("signal")
        if not isinstance(signal, str):
            raise ValueError(f"a fire action names a signal, got {signal!r}")
        value, source = fire.get("value"), fire.get("from")
        if value is not None and source is not None:
            raise ValueError("a fire action takes value or from, not both")
        urgent, after = fire.get("urgent", False), fire.get("after")
        if not isinstance(urgent, bool):
            raise ValueError(f"a fire urgent is true or false, got {urgent!r}")
        if isinstance(after, str):
            after = time_to_seconds(after)
        if isinstance(after, bool):
            raise ValueError(f"a fire after is seconds or a duration, got {after!r}")
        if after is not None and not after > 0:
            raise ValueError(f"a fire after is more than 0, got {after:g}")
        extra = set(fire) - {"signal", "value", "from", "urgent", "after"}
        if extra:
            raise ValueError(
                f"a fire action takes signal, value, from, urgent and after, "
                f"not {', '.join(sorted(extra))}"
            )
        return {
            "fire": {
                "signal": signal,
                "value": value,
                "from": source,
                "urgent": urgent,
                "after": after,
            }
        }
    if kind == "spawn":
        return {"spawn": _parse_spawn(entry["spawn"])}
    if kind == "interrupt":
        action = dict(entry["interrupt"])
        if "task" not in action or set(action) - {"task", "where", "cause"}:
            raise ValueError(
                "an interrupt action takes a task, and an optional where and cause"
            )
        if action.get("where") is not None:
            Expression(action["where"])
        return {"interrupt": action}
    return {"path": entry["path"], "value": entry.get("value")}


def _seconds(value: Union[float, str]) -> float:
    """Reads seconds, or a duration such as `"10 min"`."""
    return time_to_seconds(value) if isinstance(value, str) else float(value)


def _distribution_of(spec: Mapping[str, Any]) -> DistributionConfig:
    """Builds a config from a mapping such as `{"dist": "exponential", "rate": 1}`."""
    if not isinstance(spec, Mapping) or "dist" not in spec:
        raise ValueError(f"a distribution is a mapping with a dist, got {spec!r}")
    given = {key: value for key, value in spec.items() if key != "dist"}
    return DistributionConfig(dist=spec["dist"], **given)


def _distribution(dist: str, params: Mapping[str, Any]) -> DistributionConfig:
    """Builds a config from the parameters a builder method was given."""
    given = {
        key: value
        for key, value in params.items()
        if key not in ("self", "name", "dist") and value is not None
    }
    return DistributionConfig(dist=dist, **given)
