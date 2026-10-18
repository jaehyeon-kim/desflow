import functools
import inspect
import itertools
import logging
import secrets
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple, Union

import simpy

from desflow.config import (
    DEFAULT_QUEUE_ORDER,
    INTERRUPT_POLICIES,
    DEFAULT_STATS_INTERVAL,
    DEFAULT_STATS_LEVEL,
    LABELS,
    QUEUE_ORDERS,
    STATS_LEVELS,
)
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.expressions import Expression
from desflow.core.flow import NextSpec, Router, Transfer, Wait
from desflow.core.signals import Signal
from desflow.core.task import TaskRun, TaskSpec, check_acquisition_order
from desflow.core.registry import SimulationRegistry
from desflow.core.sampler import Sampler
from desflow.models.flow import Entity, Upstream
from desflow.models.params import (
    CapacityConfig,
    ContainerConfig,
    DistributionConfig,
    SimParameter,
)
from desflow.resources.container import DynamicContainer
from desflow.resources.resource import DynamicResource
from desflow.resources.store import DynamicStore
from desflow.utils import time_to_seconds

logger = logging.getLogger(__name__)


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

        Raises:
            ValueError: If `warmup` is negative or not a duration.
        """
        self.sim_id = sim_id
        self.factor = factor
        self.random_seed = random_seed
        self.logical_start_time = logical_start_time
        self.go_live_at = go_live_at
        self.initial_time = _seconds(initial_time)
        self.warmup = None if warmup is None else _seconds(warmup)
        if self.warmup is not None and self.warmup < 0:
            raise ValueError(f"warmup must be 0 or more, got {warmup}")

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
        self._containers_config: Dict[str, CapacityConfig] = {}
        self._stores_config: Dict[str, CapacityConfig] = {}
        self._store_priority: Dict[str, bool] = {}
        self._store_items: Dict[str, List[Any]] = {}
        self._services_config: Dict[str, DistributionConfig] = {}
        self._arrivals_config: Dict[str, DistributionConfig] = {}
        # Per arrival: the attributes each entity starts with, and the key function
        self._arrival_entities: Dict[
            str, Tuple[Dict[str, Any], Optional[Callable[[int], str]]]
        ] = {}
        self._variables_config: Dict[str, Any] = {}
        # Tasks by name, with what run() checks before the clock starts
        self._tasks: Dict[str, Callable] = {}
        self._task_specs: Dict[str, TaskSpec] = {}
        self._signal_names: List[str] = []
        self._signals: Dict[str, Signal] = {}
        # Task instances running now, by task, which interrupt() reaches
        self._running: Dict[str, List[TaskRun]] = {}
        # The positional arguments a YAML task's function takes after its id
        self._task_args: Tuple[Any, ...] = ()

        # Runtime State (Post-Compilation)
        self._env: Optional[DynamicRealtimeEnvironment] = None
        self.sampler: Optional[Sampler] = None
        self._resources_map: Dict[str, DynamicResource] = {}
        self._stores_map: Dict[str, DynamicStore] = {}
        self._containers_map: Dict[str, DynamicContainer] = {}
        self._entity_counters: Dict[str, "itertools.count[int]"] = {}
        self._instance_counters: Dict[str, "itertools.count[int]"] = {}
        self._startup_loops: List[Tuple[Callable, Optional[str]]] = []

        # Set by from_yaml from the blueprint's `run` section.
        self._default_until: Any = None
        self._before_run: List[Callable[[], Any]] = []
        # How the log tells a run without a seed to repeat itself, with `{seed}`
        self._repeat_hint: Optional[str] = None

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> "SimulationContext":
        """
        Builds a context from a YAML blueprint file.

        The blueprint is validated before anything is built, and every error names
        the file and line. The context is built through the same builder methods a
        Python script calls, so the result is the same simulation.

        Args:
            path: The blueprint file.

        Returns:
            SimulationContext: The built context. Its `run()` calls the blueprint's
                `run.before` functions first, and uses `run.until` when no `until`
                is passed.

        Raises:
            BlueprintError: If the blueprint is invalid.

        Example:
            ```python
            app = SimulationContext.from_yaml("examples/yaml/local.yaml")
            app.run()
            ```
        """
        from desflow.blueprint.build import build

        context, run = build(path)
        context._default_until = run.until
        context._before_run = list(run.before)
        context._repeat_hint = f"Repeat it with: desflow run {path} --seed {{seed}}"
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

        Each provider buffers separately, so memory is the sum of the buffers rather
        than one. A large lake buffer will dominate that sum.

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
                published once at the end of the run. Defaults to 10.

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
        current_cap: int,
        max_cap: int,
        queue: str = DEFAULT_QUEUE_ORDER,
        preempt: bool = False,
    ) -> "SimulationContext":
        """
        Registers a discrete logical resource capable of processing tasks.

        Resources act as the primary gating mechanism in the simulation, blocking
        concurrent processes to prevent physical collisions (e.g., restricting
        the number of slabs inside a mill stand).

        Args:
            name: The internal identifier (e.g., 'mill_structural').
            current_cap: The active physical token capacity at simulation start.
            max_cap: The absolute physical maximum bound of tokens allowed.
            queue: The order waiting tasks are served in: `fifo` (the default),
                `lifo` or `priority`, which serves the lowest task priority first and
                the oldest first among equal priorities.
            preempt: When True, a task of a lower priority value takes a unit from
                a task of a higher one when every unit is busy, as SimPy's
                `PreemptiveResource` does; the task that loses it follows its
                `on_interrupt`. Needs `queue="priority"`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `queue` is not one of the three orders, or `preempt` is
                set without `queue="priority"`.
        """
        if queue not in QUEUE_ORDERS:
            raise ValueError(
                f"queue must be one of {', '.join(QUEUE_ORDERS)}, got '{queue}'"
            )
        if preempt and queue != "priority":
            raise ValueError(
                "preempt ranks users by priority, so it needs queue='priority'"
            )
        self._resource_preempt[name] = preempt
        self._resources_config[name] = CapacityConfig(
            current_cap=current_cap, max_cap=max_cap
        )
        self._resource_queues[name] = queue
        return self

    def add_container(
        self, name: str, current_cap: float, max_cap: float, level: float = 0.0
    ) -> "SimulationContext":
        """
        Registers a container: an amount that tasks take from and add to, such as stock.

        `current_cap` is the capacity, the largest level, as for a resource. The
        level starts at `level` and has a live registry path of its own,
        `<sim_id>.containers.<name>.level`, so a scenario or an ingress message can
        set it during the run, such as a restock. Models that used `current_cap` as a
        level, such as wear, use `level` instead.

        Args:
            name: The internal identifier (e.g., 'stock').
            current_cap: The capacity at simulation start.
            max_cap: The largest capacity a live update can set.
            level: The amount held at simulation start. Defaults to 0.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If a store already has this name.
        """
        if name in self._stores_config:
            raise ValueError(f"'{name}' is already a store")
        self._containers_config[name] = ContainerConfig(
            current_cap=current_cap, max_cap=max_cap, level=float(level)
        )
        return self

    def add_store(
        self,
        name: str,
        current_cap: int,
        max_cap: int,
        priority: bool = False,
        items: Optional[List[Any]] = None,
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
            current_cap: The number of items it can hold at simulation start.
            max_cap: The largest capacity a live update can set.
            priority: When True, items are returned by the `priority` each task's
                `put` gives, lowest first, instead of first in, first out.
            items: The items it holds when the run starts, in the order a get
                returns them. A priority store takes `(priority, item)` pairs.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If a container already has this name, there are more items
                than `current_cap`, or a priority store's item is not a pair.
        """
        if name in self._containers_config:
            raise ValueError(f"'{name}' is already a container")
        start = list(items or [])
        if len(start) > min(current_cap, max_cap):
            raise ValueError(
                f"store '{name}' holds {min(current_cap, max_cap)} items, so it "
                f"cannot start with {len(start)}"
            )
        if priority and not all(
            isinstance(item, (tuple, list)) and len(item) == 2 for item in start
        ):
            raise ValueError(
                f"priority store '{name}' starts with (priority, item) pairs"
            )
        self._store_items[name] = start
        self._stores_config[name] = CapacityConfig(
            current_cap=current_cap, max_cap=max_cap
        )
        self._store_priority[name] = priority
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

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If the family is unknown, or a parameter is missing, out of
                range or not one the family takes.
        """
        params = {k: v for k, v in locals().items() if k not in ("attributes", "key")}
        self._arrivals_config[name] = _distribution(dist, params)
        self._arrival_entities[name] = (dict(attributes or {}), key)
        return self

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
           `patience` covers all three.
        4. Emits a `started` event.
        5. Executes the wrapped user function to extract custom payload data.
        6. Samples the live `service_id` distribution and yields the timeout.
        7. Adds its `put`, waiting for room, and checks `fail`.
        8. Emits its finished event with the user's payload, or `failed`.
        9. Fires its `signal` (or fails it), applies `set` to the entity's
           attributes and starts the `next` task, or `on_fail`.

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
                the queue, before its payload exists.
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
            patience: How long the task waits for its resource before it leaves the
                queue and publishes `reneged`: seconds, a duration such as `"30 s"`,
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
            raise ValueError("keep names resources the task holds")
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
        if priority is not None and resource_id is None:
            raise ValueError(
                "priority orders a resource's queue, so it needs a resource"
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
            and get is None
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
                return TaskRun(
                    self, spec, task_id, entity, upstream, args, kwargs
                ).run()

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

    def wait_for_arrival(self, arrival_id: str) -> Any:
        """
        Yields a dynamic SimPy timeout based on the live arrival distribution.

        The timeout's value is the new `Entity`, so an arrival loop writes
        `entity = yield context.wait_for_arrival("orders")` and passes
        `entity=entity` to the task it spawns.

        Args:
            arrival_id: The arrival's name.

        Returns:
            simpy.events.Timeout: The gap to the next arrival, valued with its entity.

        Raises:
            RuntimeError: If called during the Builder Phase (before `.run()`).
        """
        if not self._env or not self.sampler:
            raise RuntimeError("Cannot sample arrival before context is run.")

        cfg = self._env.registry.get_config(f"{self.sim_id}.arrival.{arrival_id}")
        entity = self._new_entity(arrival_id, self._arrival_entities.get(arrival_id))
        return self._env.timeout(self.sampler.sample(cfg), value=entity)

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

    def get_resource(self, resource_id: str) -> DynamicResource:
        """
        Retrieves an active SimPy `DynamicResource` instance for raw lock management.

        Raises:
            RuntimeError: If called during the Builder Phase.
        """
        if not self._env:
            raise RuntimeError("Resources are not instantiated until context is run.")
        return self._resources_map[resource_id]

    def publish(self, metric_name: str, value: Any) -> None:
        """
        Publishes a scalar telemetry metric to the non-blocking egress buffer.
        """
        if not self._env:
            raise RuntimeError("Cannot publish telemetry before context is run.")
        self._env.publish_telemetry(f"{self.sim_id}.{metric_name}", value)

    def spawn(self, process: Any) -> simpy.Process:
        """
        Starts a generator as a SimPy process, such as a task instance.

        This acts as the "escape hatch" for complex logic (like physical slab
        tracking) that outgrows the strict abstraction of the `@task` decorator.

        Args:
            process: A generator, such as `pack_order(task_id, entity=entity)`.

        Returns:
            simpy.Process: The process. Yield it to wait until it ends; its value is
                what the generator returns, and `is_alive`, `triggered`, `ok` and
                `value` inspect it, as in SimPy.
        """
        if not self._env:
            raise RuntimeError("Cannot spawn processes before context is run.")
        return self._env.process(process)

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
            raise RuntimeError("Cannot call tasks before context is run.")
        if task not in self._task_specs:
            raise ValueError(f"no task is called '{task}'")
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
        process = self.env.process(run.run())
        if child:
            # A child flow that fails with no task waiting for it ends that flow,
            # not the run
            process.callbacks.append(_defuse)
        return process

    def add_signal(self, name: str) -> "SimulationContext":
        """
        Declares a signal that tasks fire and wait for.

        A task fires it with `signal=name` when it finishes, and fails it when the
        task fails; a task waits for it with `wait={"signal": name}`; a process
        reaches it with `context.signal(name)`.

        Args:
            name: The signal's name.

        Returns:
            SimulationContext: The current instance for method chaining.
        """
        if name not in self._signal_names:
            self._signal_names.append(name)
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
            raise RuntimeError("Signals are not created until context is run.")
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
            raise RuntimeError("Cannot interrupt tasks before context is run.")
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
            service=self._services_config,
            arrival=self._arrivals_config,
            variables=self._variables_config,
        )

    def run(self, until: Any = None) -> None:
        """
        Compiles the defined infrastructure architecture and triggers the simulation loop.

        Before Phase 1, a context built by `from_yaml` calls the blueprint's
        `run.before` functions.

        Phase 1: Instantiates the foundational `DynamicRealtimeEnvironment` and `Sampler`.
        Phase 2: Compiles all builder dictionaries into a monolithic `SimParameter`
                 object and registers it with the registry.
        Phase 3: Connects and boots the asynchronous Kafka/Redis background threads.
        Phase 4: Hydrates physical SimPy limits based on the registry boundaries.
        Phase 5: Spawns all registered `@arrival_loop`, `@telemetry_loop` and
                 `add_process` generators.
        Phase 6: Relinquishes the main thread to the SimPy environment execution clock.

        Args:
            until: The absolute simulation termination time. Can be a numeric float
                (representing base seconds) or a human-readable string parsed by
                the utility module (e.g., "1 week", "8 hours"). If None, a context
                built by `from_yaml` uses the blueprint's `run.until`, and otherwise
                the simulation runs infinitely.

        Raises:
            ValueError: If a task's `next`, `priority`, `get` or `put` names something
                the context does not define.
        """
        if until is None:
            until = self._default_until
        self._check_tasks()
        for setup in self._before_run:
            setup()

        logger.info(f"Building SimulationContext for '{self.sim_id}'...")

        env_kwargs: Dict[str, Any] = {"factor": self.factor}
        if self.initial_time:
            env_kwargs["initial_time"] = self.initial_time
        if self.logical_start_time is not None:
            env_kwargs["logical_start_time"] = self.logical_start_time
        if self.go_live_at is not None:
            env_kwargs["go_live_at"] = self.go_live_at
        self._env = DynamicRealtimeEnvironment(**env_kwargs)

        # One random sequence per distribution, all from the run's seed
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

        if self._stats_level != "none":
            self._env.setup_stats(self.sim_id, self._stats_level, self._stats_interval)
        self._signals = {name: Signal(self._env, name) for name in self._signal_names}

        # Compile Master Parameter Object
        self._env.registry.register_sim_parameter(self.compile_parameters())

        # Connect External Infrastructure
        if self._ingress_providers:
            self._env.setup_ingress(self._ingress_providers)

        if self._egress_providers:
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
            )
        elif self.warmup is not None:
            self._env.process(self._end_warmup(self.warmup))

        # Hydrate Physical SimPy Resources
        for res_id in self._resources_config.keys():
            self._resources_map[res_id] = DynamicResource(
                self._env,
                self.sim_id,
                res_id,
                queue=self._resource_queues[res_id],
                preempt=self._resource_preempt.get(res_id, False),
            )
        for store_id in self._stores_config:
            self._stores_map[store_id] = DynamicStore(
                self._env,
                self.sim_id,
                store_id,
                priority=self._store_priority.get(store_id, False),
                items=self._store_items.get(store_id),
            )
        for container_id in self._containers_config:
            self._containers_map[container_id] = DynamicContainer(
                self._env, self.sim_id, container_id
            )

        # Boot Registered Daemon Generators
        for loop_func, meta in self._startup_loops:
            if meta:
                # Arrival loops require context injection
                self._env.process(loop_func(self))
            else:
                # Telemetry loops run blindly
                self._env.process(loop_func())

        try:
            logger.info("Simulation engine started.")
            if isinstance(until, str):
                until = time_to_seconds(until)

            self._env.run(until=until)
        finally:
            self._env.teardown()

    def _end_warmup(self, warmup: float):
        """Resets the statistics at the end of the warm-up, for a run with no sink."""
        yield self.env.timeout(warmup)
        self.env.stats.reset()

    def _check_tasks(self) -> None:
        """Checks what each task names, before the clock or any connector starts."""
        check_acquisition_order(
            {name: spec.resources for name, spec in self._task_specs.items()},
            {
                name: (spec.keep, spec.router.tasks or set())
                for name, spec in self._task_specs.items()
            },
        )
        holders = {**self._stores_config, **self._containers_config}
        registry: Optional[SimulationRegistry] = None
        for name, spec in self._task_specs.items():
            for resource in spec.resources:
                if resource not in self._resources_config:
                    raise ValueError(
                        f"task '{name}' uses resource '{resource}', which is not added"
                    )
            if (
                spec.service_id is not None
                and spec.service_id not in self._services_config
            ):
                raise ValueError(
                    f"task '{name}' uses service '{spec.service_id}', which is not added"
                )
            for resource in spec.resources if spec.priority is not None else ():
                if self._resource_queues.get(resource) != "priority":
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
                        f"task '{name}' names task '{target}', which is not a task"
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
                if unknown or (expression.references["wait"] and key == "wait"):
                    raise ValueError(
                        f"task '{name}' reads wait.{sorted(unknown or expression.references['wait'])[0]} "
                        f"in '{expression.text}', which is not a branch of its wait "
                        f"read after it"
                    )
                # item.<field> reads what the task's store get took, so it means
                # nothing before the get, and nothing without one
                takes_item = spec.get is not None and spec.get.store is not None
                after_get = key in ("get", "put", "next", "fail")
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


def _defuse(event: Any) -> None:
    """Marks a failed child flow as handled, so a failure nobody waits for ends that
    flow and not the run, as the builder and YAML treat every task failure."""
    if not event.ok:
        event.defused = True


def _seconds(value: Union[float, str]) -> float:
    """Reads seconds, or a duration such as `"10 min"`."""
    return time_to_seconds(value) if isinstance(value, str) else float(value)


def _distribution(dist: str, params: Mapping[str, Any]) -> DistributionConfig:
    """Builds a config from the parameters a builder method was given."""
    given = {
        key: value
        for key, value in params.items()
        if key not in ("self", "name", "dist") and value is not None
    }
    return DistributionConfig(dist=dist, **given)
