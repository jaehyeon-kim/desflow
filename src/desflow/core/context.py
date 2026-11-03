import copy
import functools
import inspect
import itertools
import logging
import math
import numbers
import operator
import secrets
import weakref
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
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
    TRACE,
    QUEUE_ORDERS,
    STATS_LEVELS,
)
from desflow.core.environment import DynamicRealtimeEnvironment, resolve_zone
from desflow.core.expressions import (
    ANY_NAME,
    Expression,
    Scope,
    boolean_word,
    rank_problem,
)
from desflow.core.arrivals import (
    ARRIVALS_STOPPED as _ARRIVALS_STOPPED,
    ArrivalPlan,
    defuse_stopped as _defuse_arrivals_stopped,
    source as _source,
    start_entities,
    whole,
)
from desflow.core.flow import (
    NextSpec,
    Listen,
    Open,
    Release,
    Router,
    Seize,
    Transfer,
    Wait,
    make_scope,
)
from desflow.core.families import (
    SECTIONS as _FAMILY_SECTIONS,
    Family,
    Reference,
    family_part,
    member_name,
    member_path,
    name_of,
    normal_key,
)
from desflow.core.signals import Signal, event_priority, timing_problem
from desflow.core.task import (
    Batch,
    TaskRun,
    TaskSpec,
    Holding,
    batch_mismatch,
    batch_mismatch_text,
    check_acquisition_order,
    flow_sets,
    family_problem,
    handle_problem,
    scope_problem,
    whole_flow_problems,
)
from desflow.core.registry import ParamChange, ParamRejected, SimulationRegistry
from desflow.core.sampler import Sampler
from desflow.models.flow import Entity, Upstream
from desflow.core.breakdowns import run_breakdowns
from desflow.core.replay import UNITS, Row, read_trace
from desflow.core.scenario import apply_step, check_step, run_scenario
from desflow.values.models import (
    Expr,
    When,
    _Gen,
    has_generator,
    values_adapter,
)
from desflow.values.runtime import DEFAULT_HISTORY, RunStopped, ValueRuntime
from desflow.values.check import (
    task_value_places,
    generators_in,
    load_rows,
    lookup_cycle,
    looked_up,
    task_value_problems,
)
from desflow.values.models import Row as RowGenerator
from desflow.values.template import bind as bind_values
from desflow.models.params import (
    BreakdownConfig,
    CapacityConfig,
    ContainerConfig,
    DistributionConfig,
    ResourceConfig,
    SimParameter,
)
from desflow.resources.base import check_ties
from desflow.resources.container import (
    ContainerBase,
    DynamicContainer,
    check_container_type,
)
from desflow.resources.resource import (
    DynamicResource,
    PreemptRule,
    ResourceBase,
    _default_put_queue,
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
        on_change (List[Callable[[ParamChange], None]]): Callbacks that receive a
            `ParamChange` for every accepted registry change. Each `start()` copies
            the list into the registry of the run it builds, so a callback added
            after `start()` reaches the next run; for the current run, add it to
            `env.registry.on_change`. A YAML model takes callbacks on the context
            `from_yaml` returns.
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
        strict: Optional[bool] = None,
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
            strict: When True a step raises `RuntimeError` if the run is more than
                `factor` real seconds behind the wall clock, as SimPy's
                `RealtimeEnvironment` does; at `factor` 0 that is any step once the
                wall clock has moved. By default (None) it is True for a paced run or
                one with `go_live_at`, and False at `factor` 0 with no `go_live_at`.
                Pass False for a live run whose sink blocks and should go on when it
                falls behind.
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
        self._egress_traces: List[bool] = []
        self._egress_envelopes: List[bool] = []
        self._stats_level = DEFAULT_STATS_LEVEL
        self._stats_interval = DEFAULT_STATS_INTERVAL
        self._stats_on_change = False
        self._stats_trace = False

        self._resources_config: Dict[str, CapacityConfig] = {}
        self._resource_queues: Dict[str, str] = {}
        self._resource_preempt: Dict[str, bool] = {}
        self._resource_queue_limits: Dict[str, Optional[int]] = {}
        # A resource's `queue` rank (the expression and `ties`), and its `preempt` rule
        # (the `when` and `victim` expressions)
        self._resource_rank: Dict[str, Tuple[Expression, str]] = {}
        self._resource_rule: Dict[
            str, Tuple[Optional[Expression], Optional[Expression]]
        ] = {}
        # A store's or container's `get_queue` and `put_queue` ranks, by
        # (name, "get" or "put")
        self._holder_ranks: Dict[Tuple[str, str], Tuple[Expression, str]] = {}
        self._resource_types: Dict[str, Optional[Type[ResourceBase]]] = {}
        self._breakdowns_config: Dict[str, BreakdownConfig] = {}
        # The readings of `add_readings`, one mapping of name to expression for each call
        self._readings: List[Dict[str, Expression]] = []
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
        # Per arrival: the keys of a blueprint that limit or repeat it, or take its
        # gaps from an expression; and per run, what it needs while it runs
        self._arrival_plans: Dict[str, ArrivalPlan] = {}
        self._first_due: Dict[str, Union[int, float]] = {}
        self._live_keys: Dict[str, Dict[str, Callable[[], bool]]] = {}
        self._key_skips: Dict[str, int] = {}
        self._variables_config: Dict[str, Any] = {}
        # Tasks by name, with what run() checks before the clock starts
        self._tasks: Dict[str, Callable] = {}
        self._task_specs: Dict[str, TaskSpec] = {}
        # What the values of the tasks read from other records: the tasks a task looks
        # up (it does not start before each has a row), the tasks that must keep rows,
        # and the tasks whose last payload `previous` reads
        self._gates: Dict[str, Set[str]] = {}
        self._row_targets: Set[str] = set()
        # Whether a `row` file is read when the generator is added (a builder script,
        # from the working directory) or later by the blueprint, from its own folder
        self._rows_deferred = False
        self._previous_users: Set[str] = set()
        self._signal_names: List[str] = []
        self._signal_once: Dict[str, bool] = {}
        # Whether each signal's failures are defused, as `event.defused = True` does
        self._signal_defused: Dict[str, bool] = {}
        # Each signal's `on:` actions, in normalized form, run when it fires
        self._signal_on: Dict[str, List[Dict[str, Any]]] = {}
        self._signals: Dict[str, Signal] = {}
        # The declared families, by name, whose entries in the sections above are the
        # template each member is made from
        self._families: Dict[str, Family] = {}
        # Tasks whose instances need an end event, filled by _check_tasks: those a
        # `running` branch or `subscribe` names (_tracked), those a `fire from`
        # copies (_copied), and every task with `on:` actions (_ended, the union)
        self._tracked: set = set()
        self._copied: set = set()
        self._ended: set = set()
        # Tasks a `running` branch with `whole_flow` names, and the instances of each,
        # ended ones too; the requests the tasks named by a `release: {resource, task}`
        # hold or wait for, by task, and the length at which that list is pruned
        self._order_warned: set = set()
        self._whole_flow: set = set()
        self._flow_runs: Dict[str, List[TaskRun]] = {}
        self._held_by_task: Dict[str, List[Any]] = {}
        self._held_prune = 64
        # Task instances running now, by task, which interrupt() reaches
        self._running: Dict[str, List[TaskRun]] = {}
        # The events of the instances suspended in a `wait: {suspend: true}`, by task,
        # in the order they suspended, which `resume` wakes
        self._suspended: Dict[str, List[Any]] = {}
        # The batch open under each batch name (a task's name unless it names one)
        self._open_batches: Dict[str, Any] = {}
        # The instance a task function's generator runs, until `spawn` starts it
        self._runs_of: "weakref.WeakKeyDictionary[Any, TaskRun]" = (
            weakref.WeakKeyDictionary()
        )
        # The positional arguments a YAML task's function takes after its id
        self._task_args: Tuple[Any, ...] = ()

        # Runtime State (Post-Compilation)
        self._env: Optional[DynamicRealtimeEnvironment] = None
        # Called with a `ParamChange` for every accepted registry change of a run;
        # start() copies them into that run's registry
        self.on_change: List[Callable[[ParamChange], None]] = []
        # True once start() has built and started everything, so a caller can tell
        # a value refused before the run from an error inside it
        self.started = False
        self.sampler: Optional[Sampler] = None
        # The generators' state for the run, made when the run is built; nothing
        # generates a value before that
        self.values: ValueRuntime = None  # type: ignore[assignment]
        self._resources_map: Dict[str, ResourceBase] = {}
        self._stores_map: Dict[str, DynamicStore] = {}
        self._containers_map: Dict[str, DynamicContainer] = {}
        self._entity_counters: Dict[str, _Counter] = {}
        # Whether each arrival's source is on, and the event a restarting source waits
        # on for a change
        self._arrival_on: Dict[str, bool] = {}
        self._arrival_changed: Dict[str, simpy.Event] = {}
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
        self._default_unhandled: str = "stop"
        self._on_unhandled_failure: str = "stop"
        # The blueprint's `run.segments` (a list of mappings, or `{every: seconds}`),
        # `run.steps` and `run.sync`; start() keeps them for the run that uses them
        self._default_segments: Any = None
        self._default_steps = False
        self._default_sync = False
        self._segments: Any = None
        self._steps = False
        self._sync = False
        # True once advance() or step() has moved the run, so finish() leaves the
        # segments and steps to the caller who is driving it
        self._moved = False
        # The run's end, entities in the model, and the drain state, set by start()
        self._until: Any = None
        self._end: Optional[simpy.Event] = None
        self._end_done = False
        self._stopped = False
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
        cls,
        path: Union[str, Path],
        preview: Optional["Preview"] = None,
        variables: Optional[Mapping[str, str]] = None,
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
            variables: Values for `${NAME}` in the file and the files it includes,
                used before the environment, as `desflow run --var NAME=VALUE` gives
                them.

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

        context, run = build(path, preview, variables)
        context._default_until = run.until
        context._default_stop = run.stop
        context._default_drain = run.drain_setting()
        context._default_unhandled = run.on_unhandled_failure
        context._default_segments = _segment_plan(run)
        context._default_steps = run.steps
        context._default_sync = run.sync
        if preview is None:
            context._before_run = list(run.before)
        elif run.before:
            logger.warning("run.before skipped: preview runs use no real sinks")
        if preview is not None and preview.sample is not None:
            context.factor = 0.0
            context.go_live_at = None
            context.strict = None
            context._sample_limit = preview.sample
        context._repeat_hint = f"Repeat it with: desflow run {path} --seed {{seed}}"
        # A copy that `analyze(verify=True)` builds reads the same variables
        context._source = (
            functools.partial(cls.from_yaml, path, variables=dict(variables))
            if variables
            else Path(path)
        )
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
        envelope: bool = True,
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
                receives them. `"trace"` makes it a trace sink: it receives the
                event trace that `with_stats(trace=True)` switches on, and nothing
                else, and no other sink receives it. A run with a trace sink and no
                `with_stats(trace=True)` is refused, as YAML refuses it at load.
            batch_size: Records this provider buffers before flushing. None uses the
                context default from `with_batching`. A stream sink wants a small
                value to keep latency down; a file or lake sink wants a large one,
                because the batch is the file and, for Iceberg, the commit.
            flush_interval: Simulation seconds this provider waits before forcing a
                flush. None uses the context default. Ignored when `factor` is 0,
                because simulation time is then detached from the wall clock.
            envelope: False makes this a plain-output provider. It receives no
                telemetry record and no `queued` or `started` record, and it writes
                each event's `value` without the record around it. Kafka and Redis
                send a value that is not a mapping as JSON, and the storage and
                PostgreSQL sinks skip it with one warning. Ground-truth records
                are not affected. Defaults to True.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `when` is a string other than `"labels"` or `"trace"`.

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
        if isinstance(when, str) and when not in (LABELS, TRACE):
            raise ValueError(
                f"when takes a function, '{LABELS}' or '{TRACE}', got '{when}'"
            )
        self._egress_providers.append(provider)
        self._egress_predicates.append(None if isinstance(when, str) else when)
        self._egress_labels.append(when == LABELS)
        self._egress_traces.append(when == TRACE)
        self._egress_batch_sizes.append(batch_size)
        self._egress_flush_intervals.append(flush_interval)
        self._egress_envelopes.append(envelope)
        return self

    def with_stats(
        self,
        level: str = "basic",
        interval: float = DEFAULT_STATS_INTERVAL,
        on_change: bool = False,
        trace: bool = False,
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
            on_change: True also publishes a resource's readings, the names it
                publishes at each interval, right after each `request` and each
                `release` call on it, as a patched `request` and `release` would
                record them. A grant that a release event makes later, and a cancel,
                publish none of their own. Needs a level above `none`.
            trace: True publishes one record of the kind `trace` for each model event
                the environment processes, before its callbacks run, with the value
                `{"priority", "event_id", "event"}`: the event's priority, its
                `event_id` (SimPy's sequence number of the event, which desflow's own
                events also advance) and its name, one of `Initialize`, `Process`,
                `Timeout`, `Condition`, `Interruption`, `Request`, `Release`, `Get`,
                `Put`, `Suspension` or `Event`. It reaches only the sinks added with
                `when="trace"`.
                Needs no level.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If the level is unknown, the interval is not positive, or
                `on_change` is set with the level `none`.
        """
        if level not in STATS_LEVELS:
            raise ValueError(
                f"stats level must be one of {', '.join(STATS_LEVELS)}, got '{level}'"
            )
        if not interval > 0:
            raise ValueError(f"stats interval must be greater than 0, got {interval}")
        if on_change and level == "none":
            raise ValueError(
                "on_change publishes the statistics at each request and release, so "
                "it needs a level above none"
            )
        self._stats_level = level
        self._stats_interval = interval
        self._stats_on_change = bool(on_change)
        self._stats_trace = bool(trace)
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
        queue: Union[str, Mapping[str, Any]] = DEFAULT_QUEUE_ORDER,
        preempt: Union[bool, Mapping[str, Any]] = False,
        queue_limit: Optional[int] = None,
        type: Optional[Type[ResourceBase]] = None,
        family: bool = False,
        members: Any = None,
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
                the oldest first among equal priorities. A mapping `{"rank":
                "<expression>", "ties": "fifo" or "lifo"}` serves the lowest value of
                the expression first, read once as each request is made, in the scope
                of the task that makes it. `ties` orders requests of one rank, the
                oldest first (the default) or the latest first, and does not change who
                preempts. A task, a `seize` or a `request` of such a resource gives no
                `priority`, because the rank is the request's priority.
            preempt: When True, a task of a lower priority value takes a unit from
                a task of a higher one when every unit is busy, as SimPy's
                `PreemptiveResource` does; the task that loses it follows its
                `on_interrupt`. Needs `queue="priority"` or a `queue` rank. A mapping
                `{"when": "<expression>", "victim": "<expression>"}` changes the
                rule: `victim` gives each holder a value and the highest is the one
                `when` is tested on (the first granted among equals), and `when` says
                whether the request takes its unit. Both read `request.rank`,
                `request.time`, `request.preempt`, `request.key`, `holder.rank`,
                `holder.time`, `holder.preempt`, `holder.key`, `holder.since` and
                `holder.attr.<name>`, and `t`, `users.`, `queue.`, `level.` and
                `registry[...]`.
            queue_limit: The most requests that may wait, or None for no limit. A
                task that finds that many waiting balks; a request made in the
                low-level API raises `RuntimeError`, as SimPy's `SortedQueue` does.
                0 refuses every request. Not allowed with a `type` whose `PutQueue`
                is not the default. The limit is also the registry path
                `<sim_id>.resources.<name>.queue_limit`: a change applies from the
                next request and removes nobody who waits.
            type: A subclass of `ResourceBase` to build the resource from, such as
                one that overrides `_do_put`. Defaults to `DynamicResource`.
            family: When True, the entry is a family: a set of resources, one for each
                key, made from this entry. A task names a member with `{"of": name,
                "key": "<expression>"}` where it names a resource, and a member is made
                at its first use. `current_cap` and `max_cap` may then be text, read
                once when the member is made.
            members: With `family`, how many members exist when the run starts, with
                the keys 0 to n - 1: a whole number, an expression text, or a
                generator, read once when the run is built. None starts the family
                empty.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `family` is not set and `members` is given, `queue` is not one of the three orders or a rank mapping,
                `preempt` is set without `queue="priority"` or a rank, a rank or
                a preempt rule is malformed or reads what it cannot, `current_cap` or
                `max_cap` is not a whole number or infinity, only `max_cap` is given
                and is below 1, `type` is not a subclass of `ResourceBase`, or
                `queue_limit` is negative or is set on a `type` with its own
                `PutQueue`.
        """
        (current_cap, max_cap), texts = _family_fields(
            "resource", name, family, members, current_cap=current_cap, max_cap=max_cap
        )
        rank = None
        if isinstance(queue, Mapping):
            rank = _queue_rule(queue, f"resource '{name}' queue")
            queue = "priority"
        rule = None
        if isinstance(preempt, Mapping):
            rule = _preempt_rule(preempt, f"resource '{name}' preempt")
            preempt = True
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
        if rank is None:
            self._resource_rank.pop(name, None)
        else:
            self._resource_rank[name] = rank
        if rule is None:
            self._resource_rule.pop(name, None)
        else:
            self._resource_rule[name] = rule
        self._resources_config[name] = ResourceConfig(
            current_cap=_within_max("resource", name, current_cap, max_cap),
            max_cap=max_cap,
            queue_limit=queue_limit,
            limit_path=_default_put_queue(type or ResourceBase),
        )
        self._resource_queues[name] = queue
        self._resource_queue_limits[name] = queue_limit
        self._resource_types[name] = type
        self._set_family("resource", name, family, members, texts)
        return self

    def add_container(
        self,
        name: str,
        current_cap: Union[float, str, None] = None,
        max_cap: Union[float, str, None] = None,
        level: Union[float, str] = 0.0,
        type: Optional[Type[ContainerBase]] = None,
        get_queue: Optional[Mapping[str, Any]] = None,
        put_queue: Optional[Mapping[str, Any]] = None,
        family: bool = False,
        members: Any = None,
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
            get_queue: A mapping `{"rank": "<expression>", "ties": "fifo" or "lifo"}`
                that ranks the waiting gets, lowest first, in place of first in, first
                out. The expression is read once as each get is made, in the scope of
                the task that makes it, and `ties` orders equal ranks. A get at the
                head that cannot be met holds back the gets behind it, as in SimPy.
                Not allowed with `type`.
            put_queue: As `get_queue`, for the waiting puts.
            family: When True, the entry is a family of containers, as for
                `add_resource`. `current_cap`, `max_cap` and `level` may then be text,
                read once when the member is made.
            members: With `family`, how many members exist when the run starts, as
                for `add_resource`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `family` is not set and `members` is given, a store already has this name, `type` is not a
                subclass of `ContainerBase`, or `get_queue` or `put_queue` is
                malformed, reads what it cannot or is given with `type`.
        """
        if name in self._stores_config:
            raise ValueError(f"'{name}' is already a store")
        (cap, top, start), texts = _family_fields(
            "container",
            name,
            family,
            members,
            current_cap=current_cap,
            max_cap=max_cap,
            level=level,
        )
        check_container_type(type)
        config = ContainerConfig(
            current_cap=_within_max("container", name, cap, top),
            max_cap=top,
            level=start,
        )
        self._set_holder_ranks("container", name, get_queue, put_queue, type)
        self._container_types[name] = type
        self._containers_config[name] = config
        self._set_family("container", name, family, members, texts, start=start)
        return self

    def add_store(
        self,
        name: str,
        current_cap: Optional[float] = None,
        max_cap: Optional[float] = None,
        priority: bool = False,
        items: Optional[List[Any]] = None,
        type: Optional[Type[StoreBase]] = None,
        get_queue: Optional[Mapping[str, Any]] = None,
        put_queue: Optional[Mapping[str, Any]] = None,
        family: bool = False,
        members: Any = None,
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
            get_queue: A mapping `{"rank": "<expression>", "ties": "fifo" or "lifo"}`
                that ranks the waiting gets, lowest first, in place of first in, first
                out. The expression is read once as each get is made, in the scope of
                the task that makes it, and `ties` orders equal ranks. It ranks
                waiters and not items: `priority` still ranks the items. Not allowed
                with `type`.
            put_queue: As `get_queue`, for the waiting puts.
            family: When True, the entry is a family of stores, as for
                `add_resource`, each member made with the `items` given. `current_cap`
                and `max_cap` may then be text, read once when the member is made.
            members: With `family`, how many members exist when the run starts, as
                for `add_resource`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `family` is not set and `members` is given, a container already has this name, a capacity is not a
                whole number or infinity, there are more items than `current_cap`,
                a priority store's item is not a pair, `type` is not a subclass
                of `StoreBase` or is given with `priority`, or `get_queue` or
                `put_queue` is malformed, reads what it cannot or is given with
                `type`.
        """
        if name in self._containers_config:
            raise ValueError(f"'{name}' is already a container")
        (current_cap, max_cap), texts = _family_fields(
            "store", name, family, members, current_cap=current_cap, max_cap=max_cap
        )
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
            (isinstance(item, (tuple, list)) and len(item) == 2)
            or isinstance(item, _Gen)
            for item in start
        ):
            raise ValueError(
                f"priority store '{name}' starts with (priority, item) pairs; give "
                f"each item as a pair, such as (1, item)"
            )
        self._bind_values(start, ("stores", name, "items"), overwrite=False)
        config = CapacityConfig(
            current_cap=_within_max("store", name, current_cap, max_cap),
            max_cap=max_cap,
        )
        self._set_holder_ranks("store", name, get_queue, put_queue, type)
        self._store_items[name] = start
        self._stores_config[name] = config
        self._store_priority[name] = priority
        self._store_types[name] = type
        self._set_family("store", name, family, members, texts)
        return self

    def _set_family(
        self,
        kind: str,
        name: str,
        family: bool,
        members: Any,
        texts: Mapping[str, str],
        start: Any = 0,
    ) -> None:
        """Keeps or drops the family of an entry that `add_resource`, `add_store`,
        `add_container` or `add_signal` registered, as it was written."""
        if not family:
            self._families.pop(name, None)
            return
        if isinstance(members, str):
            members = Expression(members)
        made = Family(name, kind, members, texts)
        made.starting_level = start
        self._families[name] = made

    def _set_holder_ranks(
        self,
        kind: str,
        name: str,
        get_queue: Optional[Mapping[str, Any]],
        put_queue: Optional[Mapping[str, Any]],
        holder_type: Optional[Any],
    ) -> None:
        """Checks and keeps the `get_queue` and `put_queue` ranks of a store or
        container. Nothing is kept when one is refused."""
        found = {}
        for direction, rule in (("get", get_queue), ("put", put_queue)):
            if rule is not None:
                found[direction] = _queue_rule(
                    rule, f"{kind} '{name}' {direction}_queue"
                )
        check_ties(
            found["get"][1] if "get" in found else None,
            found["put"][1] if "put" in found else None,
            holder_type,
        )
        for direction in ("get", "put"):
            if direction in found:
                self._holder_ranks[(name, direction)] = found[direction]
            else:
                self._holder_ranks.pop((name, direction), None)

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
        for index, step in enumerate(checked, len(self._scenario_steps)):
            self._bind_values(step, ("scenario", index), overwrite=False)
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
            evaluate=self._evaluate_free,
        )

    def _evaluate_free(self, value: Any) -> Any:
        """Evaluates a value outside a task, such as a scenario step's: it reads the
        registry and the clock, and no entity."""
        return self.values.evaluate(value, self.values.scope_of())

    def add_breakdown(
        self,
        resource: str,
        time_to_failure: Mapping[str, Any],
        repair: Mapping[str, Any],
        work: str = "interrupt",
        clock: str = "calendar",
    ) -> "SimulationContext":
        """
        Makes a resource break down at random and be repaired.

        After each repair, or the start, the time to the next failure is drawn. The
        `calendar` clock counts it from then, busy or idle. The `busy` clock counts
        it only while the resource holds a unit, so an idle resource does not wear out.
        While the resource is down its capacity is 0, its `current_cap` keeps the
        planned value, and a change to it during the repair sets the capacity the
        resource returns to.
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
            clock: `calendar` (the default) or `busy`, the time the time to failure
                counts.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If a distribution, `work` or `clock` is invalid.
        """
        self._breakdowns_config[resource] = BreakdownConfig(
            time_to_failure=_distribution_of(time_to_failure),
            repair=_distribution_of(repair),
            work=work,
            clock=clock,
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
        p: Optional[float] = None,
    ) -> "SimulationContext":
        """
        Registers a statistical distribution defining a process service duration.

        The parameters are keyword-only, so their order never changes the meaning.
        Give only those `dist` takes; `desflow.core.distributions.DISTRIBUTIONS`
        lists each family's parameters and rules.

        Args:
            name: The internal identifier (e.g., 'packing').
            dist: The family: exponential, normal, lognormal, truncated_normal,
                poisson, geometric, constant, uniform, triangular, gamma, weibull or
                empirical.
            rate: Events per second, for exponential.
            mean: The mean, for normal, lognormal, truncated_normal and poisson.
            std: The standard deviation, for the first three (0 when not given).
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
            p: The chance of success on each try, for geometric.

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
        dist: Optional[str] = None,
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
        p: Optional[float] = None,
        gap: Any = None,
        attributes: Optional[Mapping[str, Any]] = None,
        key: Optional[Callable[[int], str]] = None,
        spawn: Optional[str] = None,
        first_at: Optional[float] = None,
        group: Any = 1,
        active: bool = True,
        off_mode: str = "drop",
        initial: int = 0,
    ) -> "SimulationContext":
        """
        Registers a statistical distribution defining the inter-arrival times of entities.

        Each arrival starts a new entity, which `wait_for_arrival` returns. Its key,
        the record key of every event it causes, is the arrival name and a counter
        (`orders-1`, `orders-2`) unless `key` gives another.

        Args:
            name: The internal identifier (e.g., 'orders').
            dist: The family, as for `add_service`. Give `gap` instead of it for a gap
                that is not a distribution.
            rate: Events per second, for exponential.
            mean: The mean, for normal, lognormal, truncated_normal and poisson.
            std: The standard deviation, for the first three (0 when not given).
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
            p: The chance of success on each try, for geometric.
            gap: The time to the next arrival in place of `dist`, as a blueprint's
                `gap`: a number of seconds, a duration such as `"5 min"`, an
                expression such as `"1 + t / 4"` or a generator, read at the start
                of each gap.
            attributes: The attributes each entity starts with, read in conditions
                as `attr.<name>`. Each entity gets its own copy.
            key: A function of the entity's counter, from 1, returning its key.
            spawn: The task each arrival starts, with the arrival's entity and the
                task's next instance id, as YAML's `spawn` key does. None leaves
                the arrival to an `arrival_loop` written by hand.
            first_at: The simulation time of the first arrival, which the first
                `wait_for_arrival` waits for in place of a gap. The gaps after it
                come from the distribution. A time already past arrives at once.
            group: How many entities one arrival starts, with `spawn`: a whole number,
                an expression such as `"int(t / 5)"` or a generator, read at each
                arrival after the gap. 0 or less starts nobody and takes no key
                number.
            active: False starts the source off, with `spawn`. A task's `activate`
                turns it on.
            off_mode: With `spawn`, `"drop"` (the default): an arrival that falls
                while the source is off starts nobody. `"restart"`: turning the
                source off drops the pending arrival, and turning it on starts a
                whole new gap.
            initial: With `spawn`, how many entities exist, in creation order, when
                the run starts, before any gap passes.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If the family is unknown, or a parameter is missing, out of
                range or not one the family takes, or both or neither of `dist` and
                `gap` are given, or `gap` is a negative number or neither a number,
                an expression nor a generator, or `first_at` is below 0 or not a
                number, or `off_mode`, `active`, `group` or `initial` is malformed or
                given with no `spawn`. A `spawn` naming no task is refused when the
                run starts.
        """
        params = {
            k: v
            for k, v in locals().items()
            if k
            not in (
                "attributes",
                "key",
                "spawn",
                "first_at",
                "group",
                "active",
                "off_mode",
                "initial",
                "gap",
            )
        }
        if (dist is None) == (gap is None):
            raise ValueError(
                f"arrival '{name}' takes a dist or a gap, "
                + ("not both" if dist is not None else "and neither is given")
            )
        if gap is not None:
            given = sorted(
                k
                for k, v in params.items()
                if v is not None and k not in ("self", "name", "dist")
            )
            if given:
                raise ValueError(
                    f"arrival '{name}' has a gap, which takes no distribution "
                    f"parameter: {', '.join(given)}"
                )
            if isinstance(gap, str):
                from desflow.blueprint.models import _duration_text

                gap = _duration_text(gap)  # a duration or a plain number in text
                if isinstance(gap, str):  # not a number, so an expression
                    try:
                        gap = Expression(gap)
                    except ValueError as error:
                        raise ValueError(f"arrival '{name}': gap: {error}") from None
            if isinstance(gap, bool) or not isinstance(
                gap, (numbers.Real, _Gen, Expression)
            ):
                raise ValueError(
                    f"arrival '{name}': gap is a number of seconds, a duration, "
                    f"an expression or a generator, got {gap!r}"
                )
            if isinstance(gap, numbers.Real) and not float(gap) >= 0:
                raise ValueError(f"arrival '{name}': gap is 0 or more, got {gap!r}")
        else:
            assert dist is not None
            self._arrivals_config[name] = _distribution(dist, params)
        self._bind_values(
            attributes or {}, ("arrivals", name, "attributes"), overwrite=False
        )
        self._arrival_entities[name] = (dict(attributes or {}), key)
        if first_at is not None:
            if (
                isinstance(first_at, bool)
                or not isinstance(first_at, numbers.Real)
                or not first_at >= 0
            ):
                raise ValueError(f"first_at is a time of 0 or more, got {first_at!r}")
        plan = _source_plan(spawn, group, active, off_mode, initial)
        if plan is not None:
            self._bind_values(plan.group, ("arrivals", name, "group"), overwrite=False)
        if first_at is not None:
            plan = plan or ArrivalPlan()
            plan.first_at = first_at
        if gap is not None:
            plan = plan or ArrivalPlan()
            plan.gap = gap
            self._bind_values(gap, ("arrivals", name, "gap"), overwrite=False)
        if plan is not None:
            self._arrival_plans[name] = plan
        else:
            self._arrival_plans.pop(name, None)  # a second call replaces the first
        self._add_spawn(name, spawn)
        return self

    def add_initial_arrival(
        self,
        name: str,
        initial: int,
        *,
        spawn: str,
        attributes: Optional[Mapping[str, Any]] = None,
        key: Optional[Callable[[int], str]] = None,
        gap: Any = None,
    ) -> "SimulationContext":
        """
        Registers an arrival that has only `initial` entities and no source: `initial`
        entities exist when the run starts, in creation order, before any step runs,
        each keyed `<name>-n` unless `key` gives another, and each starts `spawn`. A
        blueprint's arrival with `initial` alone.

        Args:
            name: The arrival's name.
            initial: How many entities, 1 or more.
            spawn: The task each entity starts.
            attributes: The attributes each entity starts with.
            key: A function of the entity's counter, from 1, returning its key, as for
                `add_arrival`.
            gap: With a gap the arrival is not sourceless: it also starts an entity
                after each gap once the first `initial` exist, as
                `add_arrival(name, gap=gap, initial=initial, ...)` does.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `initial` is not a whole number above 0, or `gap` is
                refused as for `add_arrival`.
        """
        if isinstance(initial, bool) or not isinstance(initial, int) or initial < 1:
            raise ValueError(f"initial is a whole number above 0, got {initial!r}")
        if gap is not None:
            return self.add_arrival(
                name,
                gap=gap,
                attributes=attributes,
                key=key,
                spawn=spawn,
                initial=initial,
            )
        self._bind_values(
            attributes or {}, ("arrivals", name, "attributes"), overwrite=False
        )
        self._arrival_entities[name] = (dict(attributes or {}), key)
        self._arrival_plans[name] = ArrivalPlan(initial=initial, sourceless=True)
        self._add_spawn(name, spawn)
        return self

    def _arrival_value(self, place: Any, names: Optional[Dict[str, Any]] = None) -> Any:
        """Reads a place of an arrival: a number as it is, an expression or a
        generator over the run, with `names` as its bare names."""
        scope = self.values.scope_of(names=names)
        if isinstance(place, Expression):
            return place.evaluate(scope)
        return self.values.evaluate(place, scope)

    def _arrival_seconds(self, place: Any, what: str) -> Union[int, float]:
        """A time place of an arrival, read now, as seconds.

        Raises:
            TypeError: If the result is not a number.
            ValueError: If it is below 0.
        """
        value = self._arrival_value(place)
        if isinstance(value, bool) or not isinstance(value, numbers.Real):
            raise TypeError(f"{what} gave {value!r}, which is not a number of seconds")
        if value < 0:
            raise ValueError(f"{what} gave {value!r}, which is below 0")
        return value

    def _gap(self, arrival: str) -> Union[int, float]:
        """The time to the arrival's next entity: its `gap`, or a draw from its
        distribution."""
        plan = self._arrival_plans.get(arrival)
        if plan is not None and plan.gap is not None:
            return self._arrival_seconds(plan.gap, f"arrival '{arrival}' gap")
        sampler = self.sampler
        assert sampler is not None  # a run is under way
        return sampler.sample(
            self.env.registry.get_config(f"{self.sim_id}.arrival.{arrival}")
        )

    def _make_entity(
        self,
        entity: Entity,
        attributes: Optional[Mapping[str, Any]] = None,
        index: Optional[int] = None,
    ) -> None:
        """Gives an entity that starts now its vars, then its attributes, then its key.

        Args:
            entity: The entity.
            attributes: The attributes to evaluate, when they are not the arrival's.
            index: The entity's place among the entities of an arrival with `keys`.
        """
        plan = self._arrival_plans.get(entity.arrival)
        names: Optional[Dict[str, Any]] = None
        if plan is not None and plan.vars:
            names = self.values.evaluate_vars(plan.vars, self.values.scope_of())
        if attributes is None:
            attributes = self._arrival_entities.get(entity.arrival, ({}, None))[0]
        self._generate_attributes(entity, attributes, names)
        if plan is None:
            return
        if plan.keys is not None and index is not None:
            entity.key = str(plan.keys[index])
        elif plan.key is not None:
            made = self.values.evaluate(
                plan.key, self.values.scope_of(entity, names=names)
            )
            if isinstance(made, bool) or not isinstance(made, (int, str)):
                raise ValueError(
                    f"arrival '{entity.arrival}' key gave {made!r}, which is not an "
                    f"integer or text"
                )
            entity.key = str(made)

    def _admit(self, arrival: str, entity: Entity, in_use: Callable[[], bool]) -> bool:
        """Whether an entity may start: False, with a warning, when an entity of the
        arrival that is still running has its key. Only an arrival with `key` or
        `keys` checks.

        Args:
            arrival: The arrival.
            entity: The entity that would start.
            in_use: Tells whether the entity holds its key, from now on.
        """
        plan = self._arrival_plans.get(arrival)
        if plan is None or (plan.key is None and plan.keys is None):
            return True
        live = self._live_keys.setdefault(arrival, {})
        holder = live.get(entity.key)
        if holder is not None and holder():
            skipped = self._key_skips.get(arrival, 0) + 1
            self._key_skips[arrival] = skipped
            if skipped == 1 or skipped in (10, 100, 1000) or skipped % 10000 == 0:
                logger.warning(
                    f"Arrival '{arrival}' skipped a start because an entity that is "
                    f"still running has the key '{entity.key}'; {skipped} skipped so "
                    f"far."
                )
            return False
        live[entity.key] = in_use
        if len(live) > 1024 and len(live) % 1024 == 0:
            for key in [key for key, held in live.items() if not held()]:
                del live[key]
        return True

    def _visit(self, entity: Entity) -> Entity:
        """A run of an entity that repeats: the same key and attributes with the
        state of a new flow, so each run enters and leaves the model on its own."""
        if entity.loop is not None:
            entity.loop.active += 1
        return replace(
            entity,
            held={},
            entered=False,
            instances=None,
            spawned_by=None,
            ends_pending=0,
            ending=False,
            last_ended=None,
        )

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
        resource_id: Union[str, Mapping[str, Any], List[Any], None] = None,
        *,
        name: Optional[str] = None,
        priority: Union[
            float, str, Callable[[Any, Dict[str, Any]], float], None
        ] = None,
        get: Union[Transfer, Mapping[str, Any], None] = None,
        put: Union[Transfer, Mapping[str, Any], None] = None,
        next: NextSpec = None,
        set: Optional[Mapping[str, Any]] = None,
        patience: Union[float, str, Callable[[Any, Dict[str, Any]], float], Any] = None,
        balk: Optional[str] = None,
        wait: Optional[Mapping[str, Any]] = None,
        open: Optional[Mapping[str, Any]] = None,
        listen: Optional[Mapping[str, Any]] = None,
        cancel: Union[str, Mapping[str, Any], None] = None,
        seize: Union[str, Mapping[str, Any], None] = None,
        release: Union[str, Mapping[str, Any], None] = None,
        signal: Union[str, Mapping[str, Any], None] = None,
        fail: Union[str, Callable[[Any, Any], bool], None] = None,
        fail_reason: Any = None,
        on_fail: Optional[str] = None,
        on_renege: Optional[str] = None,
        on_balk: Optional[str] = None,
        service_time: Union[float, str, Callable[[Entity], float], Any] = None,
        key_field: Optional[str] = None,
        key: Any = None,
        keep: Optional[List[str]] = None,
        preempt: Union[bool, str] = True,
        on_interrupt: str = "resume",
        on_interrupt_next: Optional[str] = None,
        on_interrupt_run: Optional[str] = None,
        interrupt: Optional[Mapping[str, Any]] = None,
        resume: Optional[Mapping[str, Any]] = None,
        batch: Optional[Mapping[str, Any]] = None,
        spawn: Optional[Mapping[str, Any]] = None,
        subscribe: Optional[Mapping[str, Any]] = None,
        on: Optional[List[Mapping[str, Any]]] = None,
        vars: Optional[Mapping[str, Any]] = None,
        vars_once: Optional[Mapping[str, Any]] = None,
        on_start: Optional[Mapping[str, Any]] = None,
        finish_vars: Optional[Mapping[str, Any]] = None,
        set_args: Optional[Mapping[str, Any]] = None,
        activate: Union[str, List[str], None] = None,
        deactivate: Union[str, List[str], None] = None,
        history: Optional[int] = None,
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
        6. Samples the live `service_id` distribution, or reads `service_time`, and
           yields the timeout.
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
            service_time: The execution time of this task, in place of `service_id`:
                seconds, a duration such as `"30 s"`, an expression such as
                `"attr.fuel / 2"` read when the service starts, or a function of the
                entity, such as `lambda entity: entity.attributes["fuel"] / 2`, or a
                typed generator from `desflow.values.generator`, drawn when the
                service starts. A text or a generator may read the task's vars,
                which then get their value at that moment. A result below 0 fails
                the run. A task takes `service_id` or `service_time`, not both, and
                records no `path_id`. An interrupt under `restart` reads it again.
            resource_id: The resource the task holds while it works, or a list of
                them, acquired in the order listed while the earlier ones are held.
                A member of a family is `{"of": family, "key": expression}` or
                `{"of": family, "lowest": expression}`.
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
                an expression such as `"attr.patience"`, a function of the task id
                and the entity's attributes, or a typed generator from
                `desflow.values.generator`.
            balk: A condition, such as `"t >= 3300"`, checked before the task joins
                the queue; when it holds, the task publishes `balked` and never
                requests the resource.
            wait: What the task waits for before it queues for its resource: one
                branch, `{"signal": name}`, `{"task": name}`, `{"timeout": seconds}`
                or `{"condition": "..."}`, or `{"any": {...}}` or `{"all": {...}}`
                of named branches. After it, `wait.<branch>.done`, `.ok` and
                `.value` are read in `next`, `fail` and amounts. A branch
                `{"handle": name}` waits for an event `open` made.
            open: Events the task makes at its step, before its `wait`, and does not
                wait for: a mapping of names to one wait branch each (`timeout`,
                `request`, `get`, `put`, `task` or `signal`), or `any` or `all` of
                named `timeout`, `task`, `signal` and `handle` branches. A `task`
                branch may carry `args`. A name is read as `handle.<name>.triggered`,
                `.processed`, `.ok`, `.value` and `.alive`, and by `{"handle": name}`
                in a `wait`, `cancel`, `release` and `interrupt`, in this task, the
                tasks after it in the flow and the child flows it starts. A request
                keeps its place in the line and publishes `queued` when it is made and
                `started` when it is granted. Opening a name again replaces it.
            listen: Callbacks added to an event when the task starts, after its
                `open` and before its `wait`: `{"signal": name, "on": [...]}` (a
                member of a family is `{"of": family, "key": expression}`), or
                `{"handle": name, "on": [...]}` for an opened event, with `"index"` to
                pick one element of an opened `each` group. The `on` entries are those
                of a task's `on`. They run once, on the next firing of the event, after
                the callbacks and waiters added before. Listening to a signal whose
                firing was processed already, or to an event that was processed, fails
                the task, as `event.callbacks.append` raises.
            cancel: The name of an opened request, get or put, taken out of its line
                (`"slot"` or `{"handle": "slot"}`). A granted one is left as it is.
                A second cancel of one that left the line fails the run.
            seize: A unit of a resource the flow keeps across later tasks until a
                `release`: a resource name, or `{"resource": ..., "priority": ...,
                "preempt": ..., "patience": ...}`. The task asks at its step and goes
                on once the unit is granted. `priority` and `preempt` default to the
                task's; `priority` is a number, an expression or a function of the
                task id and the entity's attributes, and `preempt` is a bool or an
                expression. A preemption takes the unit, and the task asks again under
                the same name.
            release: Gives back units when the task finishes: a resource name (the
                oldest unit of it the flow holds, and nothing if it holds none),
                `{"handle": name}` (the unit of that opened request), or
                `{"resource": ..., "task": ..., "where": ...}` (the units of that
                resource that instances of that task hold, whichever flow they are in).
            signal: A signal, added with `add_signal`, that the task fires with its
                finished payload, or fails with its reason when it fails. A member of
                a family is `{"of": family, "key": expression}`.
            fail: A condition checked after the service, such as
                `"payload.defect == 1"`, where `payload` is the task's own payload,
                or a function of the task id and the payload. When it holds, the
                task publishes `failed` instead of its payload.
            fail_reason: The `reason` a failure publishes: text, or a generator
                from `desflow.values.generator` read in the step the failure happens
                (it may read `payload.`). Defaults to the `fail` condition's text.
                A reason that is None is the condition's text.
            on_fail: The task a failed instance goes to. Without it, a failure ends
                the entity's flow. A function that raises fails the task the same
                way, with the error as the reason, and goes to `on_fail`; so
                `on_fail` needs no `fail` condition.
            on_renege: The task an instance goes to after it reneges, in place of the
                end of its flow. It still publishes `reneged` and counts as
                reneged. Needs a `patience`.
            on_balk: The task an instance goes to after it balks, in place of the end
                of its flow, whether its `balk` condition holds or the waiting line
                of its resource is full. It still publishes `balked` and counts as
                balked. Needs a `balk` or a resource.
            key_field: A field of the task's payload whose value becomes the key of
                the finished event, and so the Kafka message key, in place of the
                entity's key. The `queued` and `started` records keep the entity's
                key. A payload without the field fails the run.
            key: One record key for every record of an instance, in place of the
                entity's key: a constant, or a typed generator from
                `desflow.values.generator`, drawn at the instance's first record. It
                gives an integer or text, and records carry its text. It cannot be
                given with `key_field`.
            keep: Resources in `resource_id` whose units the task hands to the next
                task of the flow instead of releasing them; that task releases them
                once it publishes `started`.
            preempt: On a resource added with `preempt=True`, whether the task's
                requests may take a unit from a task of a higher priority value. It is
                a bool, a boolean word of a blueprint (`"t"`, `"off"`) or an expression
                read for each request, such as `"attr.urgent"`.
            on_interrupt: What an interrupt or a preemption does: `resume` (the
                default) goes on with the units it still holds, asks again for the
                one it lost or waited for, and serves the remaining time; `restart`
                does the same with a new service time; `abandon` gives back every
                unit and ends the flow, or goes to `on_interrupt_next`. A plain
                interrupt leaves a granted unit with the task; a preemption or a
                breakdown takes it.
            on_interrupt_next: The task an abandoned instance goes to.
            on_interrupt_run: The task that runs as a child flow of the same entity
                after each interrupt or preemption, under `resume` or `restart`, before
                the task goes on with the time left (a new time under `restart`). The
                child reads `cause.<name>`. Not with `abandon`, a `resource` or a
                request in the `wait`, because it is for work that holds no unit of
                its own. The child runs in the task's process, so an interrupt that
                arrives while it runs reaches the child at its next wait and follows
                the child's own `on_interrupt`.
            resume: An action applied when the task finishes:
                `{"task": name, "where": condition, "value": value, "limit": n}` wakes
                the instances of that task suspended in `wait={"suspend": True}`, in the
                order they suspended, up to `limit` (all when it is None), whose
                `where` holds. Each reads `value` as `wait.suspend.value`.
            batch: Members that wait together: `{"size": n, "timeout": seconds,
                "close_when": condition, "name": text}`. The batch closes when `size`
                members have joined (a number, or an expression read when the first
                member joins), `timeout` after its first member, or when `close_when`
                holds as a member joins; it reads `batch.size` and `batch.age`. The
                `resource` and service run once for the batch, and each member then
                publishes its own payload and follows its own `next`. `name` defaults
                to the task's name, and tasks with the same name share one open batch,
                so they must have the same `size`, `timeout`, `close_when`, `resource`
                and service; a run with different ones is refused.
                It takes no `get`, `seize`, `open` or `cancel`.
            interrupt: An action applied when the task finishes:
                `{"task": name, "where": condition, "cause": value}` interrupts every
                running instance of that task whose condition holds, and
                `{"handle": name, "cause": value}` interrupts the child flow that
                name was opened as; that is a run failure if the flow has ended.
            vars: Named values, each a generator mapping such as
                `{"gen": "uniform", "low": 2, "high": 200}` or a constant, evaluated in
                order after the service, before the payload. A later var and the
                payload read an earlier one with `{"gen": "var", "name": ...}`, and an
                expression after the service (`put`, `fail`, `next`) reads it as a
                bare name. With `on_start` they are evaluated when the task starts.
            vars_once: Named values made once for each entity, at its first event,
                and the same in every later instance of this task for the entity. A wait
                timeout value can read them.
            on_start: A mapping of values published as a record when the task starts,
                after `started`.
            finish_vars: Named values evaluated after `vars` and before the payload,
                which can read the vars.
            activate: The arrival, or arrivals, whose source the task turns on when it
                finishes, after `set_args`.
            deactivate: The same, turning the source off. A source already in that
                state is left as it is.
            set_args: Names written in this process's own locals after `set`, read in
                expressions as `arg.<name>`. A child flow has its own names, and the
                `next` chain of a flow shares them.
            history: How many finished payloads the task keeps for `lookup` and
                `aggregate` (a whole number of 1 or more). A task
                keeps none unless this is given, or a generator in a `vars`,
                `vars_once`, `on_start`, `finish_vars`, `set` or `set_args` value of a
                task, or in a blueprint, names it. A `Template` payload is not
                searched, so a builder task that a `Template` looks up asks for its
                rows here.

        Raises:
            ValueError: If a resource is given without a service, a priority without
                a resource, the name is taken, `key` and `key_field` are both given,
                or `get`, `put` or `next` is malformed.

        Returns:
            Callable: A decorator that wraps the target function into a SimPy generator.

        Example:
            ```python
            @app.task(service_id="packing", resource_id="packers", next="ship")
            def pack_order(task_id: int, context):
                return {"status": "packed"}
            ```
        """
        resources = [
            name_of(found, "resource_id")
            for found in (
                []
                if resource_id is None
                else [resource_id]
                if isinstance(resource_id, (str, Mapping))
                else list(resource_id)
            )
        ]
        # `set` is a parameter here, so the sets are written as literals
        plain = [found for found in resources if not isinstance(found, Reference)]
        if len({*plain}) != len(plain):
            raise ValueError("a task lists each resource once")
        if any(isinstance(found, Reference) for found in resources) and (
            {*(keep or [])}
            & {*(found for found in resources if isinstance(found, Reference))}
        ):
            raise ValueError(
                "keep hands a unit to the next task, so it names a resource and not a "
                "computed member"
            )
        if not {*(keep or [])} <= {*resources}:
            raise ValueError(
                "keep names resources the task holds; list only resources in its "
                "resource_id"
            )
        if history is not None and (
            isinstance(history, bool)
            or not isinstance(history, numbers.Integral)
            or history < 1
        ):
            raise ValueError(
                f"history is how many finished payloads to keep, a whole number "
                f"of 1 or more, got {history!r}"
            )
        if on_interrupt not in INTERRUPT_POLICIES:
            raise ValueError(
                f"on_interrupt must be one of {', '.join(INTERRUPT_POLICIES)}, "
                f"got '{on_interrupt}'"
            )
        if on_interrupt_next is not None and on_interrupt != "abandon":
            raise ValueError("on_interrupt_next needs on_interrupt='abandon'")
        if on_interrupt_run is not None:
            if on_interrupt == "abandon":
                raise ValueError(
                    "on_interrupt_run runs a flow and then goes on with the task, so "
                    "it needs on_interrupt='resume' or 'restart'; with 'abandon', use "
                    "on_interrupt_next"
                )
            if resources or (wait is not None and Wait(wait).requests()):
                raise ValueError(
                    "on_interrupt_run is for work that holds no unit of its own, so "
                    "a task with resource or a request in its wait does not take it; "
                    "a repair task preempts the unit instead, and a unit an earlier "
                    "seize took is kept"
                )
        resumed = None if resume is None else _parse_resume(resume)
        batched = None if batch is None else _parse_batch(batch)
        if batched is not None:
            for given_key, given in (
                ("get", get),
                ("seize", seize),
                ("open", open),
                ("cancel", cancel),
            ):
                if given is not None:
                    raise ValueError(
                        f"a batch serves its members with one run of the resource and "
                        f"service, so it takes no {given_key}"
                    )
        if interrupt is not None:
            interrupt = dict(interrupt)
            unknown = {*interrupt} - {"task", "handle", "where", "cause", "index"}
            if interrupt.get("index") is not None:
                if interrupt.get("handle") is None:
                    raise ValueError("interrupt takes an index with a handle only")
                interrupt["index"] = Expression(str(interrupt["index"]))
            if (interrupt.get("task") is None) == (interrupt.get("handle") is None):
                raise ValueError(
                    "interrupt takes a task or a handle, and an optional where and "
                    "cause"
                )
            if unknown:
                raise ValueError(
                    "interrupt takes a task or a handle, and an optional where and "
                    "cause"
                )
            if interrupt.get("handle") is not None:
                if interrupt.get("where") is not None:
                    raise ValueError(
                        "interrupt takes no where with a handle, which names one "
                        "child flow"
                    )
                interrupt.setdefault("task", None)
            if interrupt.get("where") is not None:
                Expression(interrupt["where"])
        cancel_index = None
        if isinstance(cancel, Mapping):
            if "handle" not in cancel or not {*cancel} <= {"handle", "index"}:
                raise ValueError(
                    f"cancel is a name or {{handle: name, index: expression}}, got "
                    f"{cancel!r}"
                )
            if cancel.get("index") is not None:
                cancel_index = Expression(str(cancel["index"]))
            cancel = cancel["handle"]
        if cancel is not None and not isinstance(cancel, str):
            raise ValueError(f"cancel is a name or {{handle: name}}, got {cancel!r}")
        seized = None if seize is None else Seize(seize)
        if seized is not None and seized.patience is not None:
            if patience is not None:
                raise ValueError(
                    "patience is on the task and on its seize; give it in one place"
                )
            patience = seized.patience
        released = None if release is None else Release(release)
        opened = None if open is None else Open(open)
        resource_id = resources[0] if resources else None
        if service_id is not None and service_time is not None:
            raise ValueError("a task takes service_id or service_time, not both")
        if key is not None and key_field is not None:
            raise ValueError(
                "key and key_field both set the key of the finished record; use one"
            )
        if isinstance(service_time, str):
            try:
                service_time = time_to_seconds(service_time)
            except ValueError:
                pass  # not a duration, so an expression such as "attr.fuel / 2"
        if service_time is not None and not (
            isinstance(service_time, (str, _Gen))
            or callable(service_time)
            or (
                isinstance(service_time, numbers.Real)
                and not isinstance(service_time, bool)
            )
        ):
            raise ValueError(
                f"service_time must be seconds, a duration, an expression, a generator "
                f"or a function of the entity, got {service_time!r}"
            )
        if resource_id is not None and service_id is None and service_time is None:
            raise ValueError(
                "a task with a resource needs a service; give service_id or "
                "service_time too"
            )
        if (
            priority is not None
            and resource_id is None
            and seized is None
            and not (wait is not None and Wait(wait).requests())
            and not (opened is not None and opened.requests())
        ):
            raise ValueError(
                "priority orders a resource's queue, so it needs a resource, a seize "
                "or a request in its wait or open"
            )
        if (
            (priority is not None or boolean_word(preempt) is not True)
            and resource_id is None
            and seized is None
            and (
                (wait is not None and Wait(wait).requests())
                or (opened is not None and opened.requests())
            )
            and not (wait is not None and Wait(wait).plain_requests())
            and not (opened is not None and opened.plain_requests())
        ):
            raise ValueError(
                f"task '{name}': priority and preempt of a task apply to a resource, a "
                "seize or a request written as a name; every request of this task is "
                "a mapping, which takes its own priority and preempt, so put them there"
            )
        if balk is not None and resource_id is None and opened is None:
            raise ValueError(
                "balking refuses a resource queue, so it needs a resource or an open"
            )
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
            and seized is None
            and batched is None
        ):
            raise ValueError(
                "patience is how long a task waits, so it needs a wait, a seize, a "
                "resource or a get"
            )
        if fail is None and fail_reason is not None:
            raise ValueError("fail_reason needs a fail condition")
        if on_renege is not None and patience is None:
            raise ValueError(
                "on_renege is where a reneging task goes, so it needs patience"
            )
        if (
            on_balk is not None
            and balk is None
            and resource_id is None
            and seized is None
            and opened is None
        ):
            raise ValueError(
                "on_balk is where a balking task goes, so it needs a balk, a "
                "resource, a seize or an open"
            )
        spec_kwargs = dict(
            patience=patience,
            balk=balk,
            wait=None if wait is None else Wait(wait),
            seize=seized,
            release=released,
            open=opened,
            listen=None if listen is None else _parse_listen(listen),
            cancel=cancel,
            cancel_index=cancel_index,
            signal=signal,
            fail=fail,
            fail_reason=fail_reason,
            on_fail=on_fail,
            on_renege=on_renege,
            on_balk=on_balk,
            service_time=service_time,
            key_field=key_field,
            key=key,
            attribute_changes=dict(set or {}),
            resources=resources,
            keep=keep,
            preempt=preempt,
            on_interrupt=on_interrupt,
            on_interrupt_next=on_interrupt_next,
            on_interrupt_run=on_interrupt_run,
            interrupt=interrupt,
            resume=resumed,
            batch=batched,
            spawn=None if spawn is None else _parse_spawn(spawn),
            subscribe=None if subscribe is None else Wait(subscribe),
            on=[_parse_on_action(entry) for entry in on or []],
            activate=_arrival_list(activate),
            deactivate=_arrival_list(deactivate),
            history=None if history is None else int(history),
        )
        generated = {
            "vars": vars,
            "vars_once": vars_once,
            "on_start": on_start,
            "finish_vars": finish_vars,
            "set_args": set_args,
        }
        for key, mapping in generated.items():
            # The values are generator mappings such as {"gen": "uuid"}; a mistake in
            # one raises pydantic's ValidationError, with no file or line
            spec_kwargs[key] = (
                values_adapter().validate_python(dict(mapping)) if mapping else {}
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
            if not self._rows_deferred:
                self._bind_values(
                    spec.spawn, ("tasks", task_name, "spawn"), overwrite=False
                )
            self.note_values(
                task_name,
                *(getattr(spec, field) for field in generated),
                spec.attribute_changes,
                spec.on,
                spec.interrupt,
                spec.spawn,
                spec.service_time,
                spec.patience,
                spec.key,
            )
            for field in (*generated, "attribute_changes", "on", "interrupt"):
                self._bind_values(
                    getattr(spec, field),
                    (
                        "tasks",
                        task_name,
                        "set" if field == "attribute_changes" else field,
                    ),
                    overwrite=False,
                )
            for field in ("service_time", "patience", "key"):
                self._bind_values(
                    getattr(spec, field), ("tasks", task_name, field), overwrite=False
                )
            for key, parsed in (
                ("wait", spec.wait),
                ("open", None if spec.open is None else spec.open.wait),
            ):
                for node in parsed.branches.values() if parsed else ():
                    if node.kind == "task" and node.args:
                        self._bind_values(
                            node.args,
                            ("tasks", task_name, key, node.name, "args"),
                            overwrite=False,
                        )
                    if node.kind == "timeout":
                        seconds, value = node.target
                        self._bind_values(
                            seconds,
                            ("tasks", task_name, key, node.name, "timeout"),
                            overwrite=False,
                        )
                        self._bind_values(
                            value,
                            ("tasks", task_name, key, node.name),
                            overwrite=False,
                        )
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
                run = TaskRun(self, spec, task_id, entity, upstream, args, kwargs)
                generator = run.run()
                # A process is named after its generator; name it after the task (P6)
                generator.__name__ = task_name  # type: ignore[attr-defined]
                self._runs_of[generator] = run
                return generator

            self._tasks[task_name] = wrapper
            self._task_specs[task_name] = spec
            return wrapper

        return decorator

    def _bind_values(
        self, node: Any, path: Tuple[Any, ...], *, overwrite: bool = True
    ) -> None:
        """Gives the generators of a place their position, and reads the file of each
        `row` among them, from the working directory. A blueprint reads the files
        itself, from its own folder, after it has built the context.

        Raises:
            ValueError: If a `row` file is missing or cannot be used.
        """
        bind_values(node, path, overwrite=overwrite)
        if self._rows_deferred:
            return
        problems = []
        for _, row in generators_in(node, (RowGenerator,)):
            problem = load_rows(row, Path.cwd())
            if problem is not None:
                problems.append(f"{row._path}.file: {problem}")
        if problems:
            raise ValueError("; ".join(problems))

    def note_values(self, task: Optional[str], *places: Any) -> None:
        """Notes what the values of a place read from other records, so the run keeps
        what they need: the rows of the tasks a `lookup` or `aggregate` names, and the
        last payload of a task whose values hold a `previous`. A task with a `lookup`
        does not start before each task it names has a row.

        A blueprint calls this for the places that `task()` cannot see, such as a
        payload and an arrival's `attributes`. A builder script needs it only to
        search a place of its own.

        Args:
            task: The task whose values the places are, or None for a place that
                belongs to no task.
            places: Values: generators, mappings, lists or constants, or a `Template`
                (its `payload`, `vars` and `vars_once` are searched).

        Raises:
            TypeError: If a place is a set or an iterator, which are not searched.
        """
        gates, rows, previous = looked_up(*places)
        self._row_targets |= rows
        if task is not None:
            if gates:
                self._gates.setdefault(task, set()).update(gates)
            if previous:
                self._previous_users.add(task)

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

    def add_readings(
        self, interval: float, readings: Mapping[str, str]
    ) -> "SimulationContext":
        """
        Publishes expressions of the run as telemetry every `interval` seconds.

        It is the builder form of a YAML `telemetry` entry with `readings`, and
        `telemetry_loop` with a function that publishes values, written as
        expressions. Each reading is evaluated over the run (`level.`, `users.`,
        `queue.`, `signal.`, `registry[...]`, `t` and the functions of the expression
        language, and no name of an entity or a task), at the start and then every
        `interval`. Each interval publishes one telemetry record for each reading, in
        the order given, named `<sim_id>.<reading>` as `publish` names one. The
        readings of one interval are all evaluated before the first is published.

        Args:
            interval: Simulation seconds between two publications.
            readings: A reading name mapped to an expression, such as
                `{"waiting_per_packer": "queue.packers / 2"}`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `interval` is not positive, `readings` is empty or not a
                mapping of text to text, or an expression is invalid or reads an
                entity. A name that is not in the model, or an expression that cannot
                be evaluated, is refused when the run is built or stops the run.
        """
        if not interval > 0:
            raise ValueError(
                f"the interval of readings must be above 0, got {interval}"
            )
        if not isinstance(readings, Mapping) or not readings:
            raise ValueError("readings is a mapping of a reading name to an expression")
        parsed: Dict[str, Expression] = {}
        for name, text in readings.items():
            if not isinstance(name, str) or not name or not isinstance(text, str):
                raise ValueError(
                    f"a reading is a name and the text of an expression, got "
                    f"{name!r}: {text!r}"
                )
            try:
                expression = Expression(text, names=ANY_NAME)
            except ValueError as error:
                raise ValueError(f"reading '{name}': {error}") from None
            outside = expression.outside_run()
            if outside:
                raise ValueError(
                    f"reading '{name}' '{text}' reads {outside}, but a reading reads "
                    f"the run (level, users, queue, signal, registry and t), not an "
                    f"entity or a task"
                )
            parsed[name] = expression
        self._readings.append(parsed)

        def report(context: "SimulationContext") -> None:
            scope = context.values.scope_of()
            values: Dict[str, Any] = {}
            for name, expression in parsed.items():
                try:
                    values[name] = expression.evaluate(scope)
                except ValueError as error:
                    raise ValueError(f"telemetry reading '{name}': {error}") from None
            for name, value in values.items():
                context.publish(name, value)

        self.telemetry_loop(interval)(report)
        return self

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
        options = self._arrival_entities.get(arrival_id)
        entity = self._new_entity(arrival_id, options, generate=False)
        first = self._first_due.pop(arrival_id, None)
        if first is not None:
            delay: Union[int, float] = max(first - self._env.now, 0)
        else:
            delay = self._gap(arrival_id)
        event = self._env.timeout(delay, value=entity)
        self._generate_when_due(event, entity, options)
        return event

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
        options = (attributes, self._arrival_entities[arrival_id][1])
        entity = self._new_entity(arrival_id, options, generate=False)
        if row.time <= self.env.now:
            self._make_entity(entity, attributes)
            return self._next_replayed_tied(entity)
        event = self.env.timeout(row.time - self.env.now, value=entity)
        self._generate_when_due(event, entity, options)
        return event

    def _next_replayed_tied(self, entity: Entity) -> simpy.Event:
        """The wait for a row due at the step in progress: an event already processed,
        so the arrival loop goes on in the same step. The rows of one time then start
        their entities in one step, in file order, as a SimPy loop that starts them
        one after another does, before any of them takes its first step."""
        event = self.env.event()
        event._ok, event._value = True, entity
        event.callbacks = None  # type: ignore[assignment]
        return event

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
        active: bool = True,
        off_mode: str = "drop",
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
            active: False starts the source off, as for `add_arrival`.
            off_mode: `"drop"` or `"restart"`, as for `add_arrival`.

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
        self._bind_values(
            attributes or {}, ("arrivals", name, "attributes"), overwrite=False
        )
        self._arrival_entities[name] = (dict(attributes or {}), key)
        plan = _source_plan(spawn, 1, active, off_mode, 0)
        if plan is not None:
            self._arrival_plans[name] = plan
        else:
            self._arrival_plans.pop(name, None)
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
        generate: bool = True,
    ) -> Entity:
        """Starts an entity: the next counter of `arrival`, its key and attributes.

        An attribute written as a generator is evaluated once, here, or with
        `generate=False` by `_generate_attributes`, which the caller runs when the
        entity's arrival is due. Until then the attribute is None."""
        counter = self._entity_counters.setdefault(arrival, _Counter())
        number = counter.take()
        attributes, key = options if options is not None else ({}, None)
        entity = Entity(
            key=f"{arrival}-{number}" if key is None else key(number),
            arrival=arrival,
            attributes={
                name: None if has_generator(value) else value
                for name, value in attributes.items()
            },
        )
        if generate:
            self._generate_attributes(entity, attributes)
        return entity

    def _generate_attributes(
        self,
        entity: Entity,
        attributes: Mapping[str, Any],
        names: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Evaluates the generated attributes of an entity, in the order written, each
        seeing the attributes before it as `attr`, and `names` as bare names."""
        for name, value in attributes.items():
            if has_generator(value):
                entity.attributes[name] = self.values.evaluate(
                    value, self.values.scope_of(entity, names=names)
                )

    def _drop_arrival(
        self, arrival: str, entity: Entity, pending: Optional[simpy.Event] = None
    ) -> None:
        """Drops an arrival that starts nobody: its entity is forgotten and the key
        number it took is free for the next arrival, so a dropped arrival uses none.
        `pending` is its event when that has not been processed yet, so its attributes
        are not drawn when it is."""
        if pending is not None:
            pending.dropped = True  # type: ignore[attr-defined]
        counter = self._entity_counters.get(arrival)
        if counter is not None:
            counter.give_back()

    def _arrival_change(self, arrival: str) -> simpy.Event:
        """The event that succeeds when the arrival's source is next turned on or
        off, for a source with `off_mode: restart`."""
        event = self._arrival_changed.get(arrival)
        if event is None or event.triggered:
            event = self._arrival_changed[arrival] = self.env.event()
        return event

    def _set_arrivals(self, names: Iterable[str], on: bool) -> None:
        """Turns arrival sources on or off, as `activate` and `deactivate` do. A
        source already in that state is left as it is.

        Raises:
            ValueError: If a name is not an arrival that has a task to spawn.
        """
        for name in names:
            if name not in self._arrival_spawns:
                raise ValueError(
                    f"cannot {'activate' if on else 'deactivate'} '{name}', which "
                    f"is not an arrival with a spawn"
                    + suggestion(name, self._arrival_spawns)
                )
            if self._arrival_on.get(name, True) == on:
                continue
            self._arrival_on[name] = on
            event = self._arrival_changed.pop(name, None)
            if event is not None and not event.triggered:
                event.succeed()

    def _start_initial(self, arrival: str, plan: ArrivalPlan) -> None:
        """Creates the `initial` entities of an arrival and starts the arrival's task
        for each, in order, with the arrival's keys and attributes."""
        task = self._arrival_spawns[arrival]
        for _ in range(plan.initial):
            entity = self._new_entity(
                arrival, self._arrival_entities.get(arrival), generate=False
            )
            try:
                self._make_entity(entity)
            except RunStopped:
                return  # a `row` with on_end: stop ended the run before it began
            self._start(task, entity, None)

    def _generate_when_due(
        self,
        event: simpy.Event,
        entity: Entity,
        options: Optional[Tuple[Dict[str, Any], Optional[Callable[[int], str]]]],
    ) -> None:
        """Has the arrival's generated attributes evaluated when the event is
        processed, before the arrival loop resumes, so a generator reads the clock
        and the registry as they are when the entity arrives."""
        if options is None:
            return
        plan = self._arrival_plans.get(entity.arrival)
        if (plan is not None and plan.makes_entities) or has_generator(options[0]):
            attributes = options[0]
            arrival = entity.arrival

            def make(_event: Any) -> None:
                # An arrival that starts nobody draws nothing: the source is off, or
                # a restarting source dropped it
                if getattr(_event, "dropped", False):
                    return
                # A group reads its size first, and draws attributes for members only
                if plan is not None and plan.group != 1:
                    return
                if self._arrival_on.get(arrival, True):
                    self._make_entity(entity, attributes)

            event.callbacks.append(make)

    def _entity_key_of(self, process: Any) -> Optional[str]:
        """The key of the entity whose task runs in `process`, as text; None for a
        process that is not a task, or None itself."""
        if process is not None:
            for runs in self._running.values():
                for run in runs:
                    if run.process is process:
                        return str(run.entity.key)
        return None

    def _starting_items(self, store_id: str) -> Optional[List[Any]]:
        """The items a store starts with; a generator in `items` is evaluated now,
        once for each item."""
        items = self._store_items.get(store_id)
        if not items or not has_generator(items):
            return items
        made = self.values.evaluate(items, self.values.scope_of())
        if self._store_priority.get(store_id) and not all(
            isinstance(item, (tuple, list)) and len(item) == 2 for item in made
        ):
            raise ValueError(
                f"priority store '{store_id}' starts with (priority, item) pairs; "
                f"a generator in its items gave {made!r}"
            )
        return made

    def _limit_path(self, resource: str) -> str:
        """The registry path of a resource's `queue_limit`."""
        return f"{self.sim_id}.resources.{resource}.queue_limit"

    def _written_paths(self) -> Set[str]:
        """The registry paths the model writes: its scenario steps and the `path`
        actions of its tasks' and signals' `on:` lists."""
        paths = {step["path"] for step in self._scenario_steps if step.get("path")}
        actions = [
            action
            for spec in self._task_specs.values()
            for action in [*spec.on, *(spec.listen.on if spec.listen else [])]
        ] + [action for found in self._signal_on.values() for action in found]
        return paths | {a["path"] for a in actions if isinstance(a.get("path"), str)}

    def _rule_expressions(self) -> List[Tuple[str, Expression]]:
        """The `queue` ranks, `preempt` rules, store and container ranks, telemetry
        readings and the `when` of a signal's `on` entries, each with the place it is
        written for a message."""
        found: List[Tuple[str, Expression]] = []
        for name, (rank, _ties) in self._resource_rank.items():
            found.append((f"resource '{name}' queue rank", rank))
        for name, rule in self._resource_rule.items():
            for key, expression in zip(("when", "victim"), rule):
                if expression is not None:
                    found.append((f"resource '{name}' preempt {key}", expression))
        for (name, direction), (rank, _ties) in self._holder_ranks.items():
            found.append((f"'{name}' {direction}_queue rank", rank))
        for parsed in self._readings:
            for name, expression in parsed.items():
                found.append((f"telemetry reading '{name}'", expression))
        for name, actions in self._signal_on.items():
            for index, action in enumerate(actions):
                if action.get("when") is not None:
                    found.append(
                        (f"signal '{name}' on entry {index} when", action["when"])
                    )
                for _where, generator in generators_in(action, (Expr,)):
                    try:
                        read = Expression(generator.expr, names=ANY_NAME)
                    except ValueError:
                        continue
                    found.append((f"signal '{name}' on entry {index} value", read))
        return found

    def _check_rule_names(
        self, place: str, expression: Expression, holders: Mapping[str, Any]
    ) -> None:
        """Checks that the resources, stores, containers, signals and registry paths a
        queue rank or a preempt rule reads exist.

        Raises:
            ValueError: Naming the place and the name, for the first that does not.
        """
        for branch in sorted(expression.references["wait"]):
            raise ValueError(
                f"{place} reads wait.{branch}, where no wait is in scope; a signal's "
                f"on entries run with no wait in scope"
            )
        for kind in ("users", "queue", "held", "waiting"):
            for resource in expression.references[kind]:
                if resource not in self._resources_config:
                    raise ValueError(
                        f"{place} reads {kind}.{resource}, but no resource is called "
                        f"'{resource}'" + suggestion(resource, self._resources_config)
                    )
        for kind in ("level", "putters", "getters", "putting", "getting"):
            for holder in expression.references[kind]:
                if holder not in holders:
                    raise ValueError(
                        f"{place} reads {kind}.{holder}, but no store or container "
                        f"is called '{holder}'" + suggestion(holder, holders)
                    )
        for store in (*expression.references["count"], *expression.references["items"]):
            if store not in self._stores_config:
                raise ValueError(
                    f"{place} reads count or items of '{store}', but no store is "
                    f"called '{store}'" + suggestion(store, self._stores_config)
                )
        for signal in expression.references["signal"]:
            if signal not in self._signal_names:
                raise ValueError(
                    f"{place} reads signal.{signal}, but no signal is called '{signal}'"
                )
        for target in expression.references["running"]:
            if target not in self._tasks:
                raise ValueError(
                    f"{place} reads running({target}), but no task is called '{target}'"
                )
        for path in expression.references["registry"]:
            if member_path(path, self._families) is not None:
                continue
            try:
                self._scratch_registry().get(path)
            except KeyError:
                raise ValueError(
                    f'{place} reads registry["{path}"], which is not a registry path'
                ) from None

    def _holder_ties(self, name: str) -> Dict[str, Any]:
        """The `get_ties` and `put_ties` a store or container is built with."""
        return {
            f"{direction}_ties": self._holder_ranks[(name, direction)][1]
            for direction in ("get", "put")
            if (name, direction) in self._holder_ranks
        }

    def _rank_holder(self, holder: Any, source: Optional[str] = None) -> None:
        """Gives a built store or container the rank expressions its tasks read;
        `source` is the family a member is made from."""
        for direction in ("get", "put"):
            rule = self._holder_ranks.get((source or holder.obj_id, direction))
            if rule is not None:
                setattr(holder, f"{direction}_rank_of", rule[0])

    def _preemption(
        self,
        resource: str,
        when: Optional[Expression],
        victim: Optional[Expression],
    ) -> PreemptRule:
        """The `PreemptRule` of a resource's `when` and `victim` expressions, which
        read the run and the request and holder they are given."""

        def read(expression: Expression, request: Any, holder: Any) -> Any:
            scope = make_scope(
                None,
                env=self._env,
                levels=self._levels(),
                resources=self._resources_map,
                signals=self._signals,
                families=self._families,
                request=_rule_row(request),
                holder=_rule_row(holder),
            )
            try:
                return expression.evaluate(scope)
            except ValueError as error:
                raise ValueError(
                    f"the preempt rule of resource '{resource}': {error}"
                ) from None

        return PreemptRule(
            when=None if when is None else lambda r, h: bool(read(when, r, h)),
            victim=None if victim is None else lambda r, h: read(victim, r, h),
        )

    def _new_resource(
        self, res_id: str, source: Optional[str] = None, config: Any = None
    ) -> ResourceBase:
        """Builds a resource and registers it by name.

        Args:
            res_id: The name it is built and registered under.
            source: The entry whose settings it takes, when it is a member of a
                family; `res_id` otherwise.
            config: The capacity of a member, already registered; None for an entry.

        Returns:
            ResourceBase: The resource.
        """
        origin = source or res_id
        resource_type = self._resource_types.get(origin) or DynamicResource
        rank = self._resource_rank.get(origin)
        rule = self._resource_rule.get(origin)
        options: Dict[str, Any] = {}
        if self._resource_queue_limits.get(origin) is not None:
            options["queue_limit"] = self._resource_queue_limits[origin]
        if rank is not None and rank[1] != "fifo":
            options["ties"] = rank[1]
        resource = resource_type(
            self._env,
            self.sim_id,
            res_id,
            queue=self._resource_queues[origin],
            preempt=(
                self._preemption(res_id, *rule)
                if rule is not None
                else self._resource_preempt.get(origin, False)
            ),
            **options,
        )
        if rank is not None:
            setattr(resource, "rank_of", rank[0])
        self._resources_map[res_id] = resource
        return resource

    def _new_store(self, store_id: str, source: Optional[str] = None) -> DynamicStore:
        """Builds a store and registers it by name; `source` is the family it is a
        member of."""
        origin = source or store_id
        store = DynamicStore(
            self._env,
            self.sim_id,
            store_id,
            priority=self._store_priority.get(origin, False),
            items=self._starting_items(origin),
            type=self._store_types.get(origin),
            **self._holder_ties(origin),
        )
        self._stores_map[store_id] = store
        self._rank_holder(store, origin)
        return store

    def _new_container(
        self, container_id: str, source: Optional[str] = None
    ) -> DynamicContainer:
        """Builds a container and registers it by name; `source` is the family it is
        a member of."""
        origin = source or container_id
        container = DynamicContainer(
            self._env,
            self.sim_id,
            container_id,
            type=self._container_types.get(origin),
            **self._holder_ties(origin),
        )
        self._containers_map[container_id] = container
        self._rank_holder(container, origin)
        return container

    def _start_breakdown(self, res_id: str, breakdown: BreakdownConfig) -> None:
        """Starts the process that breaks the resource down and repairs it."""
        assert self._env is not None and self.sampler is not None
        self._env.process(
            run_breakdowns(
                self._env,
                self._resources_map[res_id],
                self.sampler,
                work=breakdown.work,
                clock=breakdown.clock,
            )
        )

    def _holders_of(self, family: Family) -> Dict[str, Any]:
        """The map a family's members are kept in."""
        holders: Dict[str, Dict[str, Any]] = {
            "resource": self._resources_map,  # type: ignore[dict-item]
            "store": self._stores_map,  # type: ignore[dict-item]
            "container": self._containers_map,  # type: ignore[dict-item]
            "signal": self._signals,  # type: ignore[dict-item]
        }
        return holders[family.kind]

    def _start_members(self) -> None:
        """Makes the members `members` asks for, in family order, keys 0 to n - 1, in
        run scope."""
        for family in self._families.values():
            if family.members is None:
                continue
            count = whole(
                self._arrival_value(family.members),
                f"{_FAMILY_SECTIONS[family.kind]}.{family.name}.members",
            )
            scope = self.values.scope_of()
            for key in range(max(count, 0)):
                self._open_member(family, key, scope)

    def _open_member(self, family: Family, key: Any, scope: Optional[Scope]) -> str:
        """Gives the name of a family's member, making the member when this is the
        first use of its key. The member lasts to the end of the run.

        Args:
            family: The family.
            key: The member's key, an integer or text; a boolean is 1 or 0.
            scope: What a capacity written as text reads. A family with `members`
                reads the run scope for every member.

        Returns:
            str: The member's name, such as `desk[3]`.

        Raises:
            TypeError: If `key` is not an integer or text.
            ValueError: If a capacity written as text gives an unusable value.
        """
        key = normal_key(key)
        name = member_name(family.name, key)
        holders = self._holders_of(family)
        if name in holders:
            return name
        base = family.name
        if family.kind == "signal":
            self._signal_once[name] = self._signal_once.get(base, True)
            self._signal_defused[name] = self._signal_defused.get(base, False)
            self._signal_on[name] = self._signal_on.get(base, [])
            signal = Signal(
                self._env,
                name,
                once=self._signal_once[name],
                defused=self._signal_defused[name],
            )
            if self._signal_on[name]:
                signal.attach([functools.partial(self._signal_fired, name)])
            self._signals[name] = signal
            family.keys.append(key)
            return name
        if family.members is not None or scope is None:
            scope = self.values.scope_of()
        config = copy.deepcopy(
            {
                "resource": self._resources_config,
                "store": self._stores_config,
                "container": self._containers_config,
            }[family.kind][base]
        )
        for field, expression in family.expressions.items():
            found = expression.evaluate(scope)
            where = f"{_FAMILY_SECTIONS[family.kind]}.{base}.{field} for {name}"
            if isinstance(found, bool) or not isinstance(found, numbers.Real):
                raise TypeError(f"{where} gave {found!r}, which is not a number")
            if (
                field != "level"
                and family.kind != "container"
                and not _whole_or_unlimited(found)
            ):
                raise ValueError(f"{where} gave {found!r}, not a whole number")
            setattr(config, field, found)
        if config.current_cap is not None and config.max_cap is not None:
            config.current_cap = _within_max(
                family.kind, name, config.current_cap, config.max_cap
            )
        assert self._env is not None
        registry = self._env.registry
        if family.kind == "resource":
            registry.register_resource(self.sim_id, name, config)
            self._new_resource(name, base, config)
            breakdown = self._breakdowns_config.get(base)
            if breakdown is not None:
                found_breakdown = copy.deepcopy(breakdown)
                registry.register_breakdown(self.sim_id, name, found_breakdown)
                self._start_breakdown(name, found_breakdown)
        elif family.kind == "store":
            registry.register_store(self.sim_id, name, config)
            self._new_store(name, base)
        else:
            registry.register_container(self.sim_id, name, config)
            self._new_container(name, base)
        family.keys.append(key)
        return name

    def _open_reference(self, reference: Reference, scope: Scope) -> str:
        """The name a reference gives now: the member of its `key`, made when it is
        new, or the member with the lowest `lowest`. A key and a lowest are read once.

        Args:
            reference: The reference.
            scope: What its expression reads: the step of the reference.

        Returns:
            str: The name of the resource, store, container or signal.

        Raises:
            KeyError: If `key` gives a name that `of` does not list.
            ValueError: If the family has no member to pick from.
            TypeError: If `key` is not an integer or text.
        """
        if reference.listed:
            chosen = reference.key_expression.evaluate(scope)  # type: ignore[union-attr]
            if chosen not in reference.names:
                raise KeyError(
                    f"the key of {reference.mapping()} gave {chosen!r}, which is not "
                    f"one of {list(reference.names)}"
                )
            return str(chosen)
        family = self._families[str(reference.of)]
        if reference.key_expression is not None:
            return self._open_member(
                family, reference.key_expression.evaluate(scope), scope
            )
        if not family.keys:
            raise ValueError(
                f"{reference.mapping()} has no member to pick from: the family is empty"
            )
        best: Any = None
        found: Any = None
        for key in list(family.keys):
            value = reference.lowest_expression.evaluate(  # type: ignore[union-attr]
                replace(scope, member=self._member_row(family, key))
            )
            if best is None or value < best:
                best, found = value, key
        return self._open_member(family, found, scope)

    def _origin(self, name: str) -> str:
        """The entry whose settings a store or container takes: the family of a member,
        or the name itself."""
        base = family_part(name)
        return base if "[" in name and base in self._families else name

    def _member_row(self, family: Family, key: Any) -> Dict[str, Any]:
        """What `member.<name>` reads for one member."""
        name = member_name(family.name, key)
        row: Dict[str, Any] = {"key": key}
        if family.kind == "resource":
            resource = self._resources_map[name]
            row.update(users=len(resource.users), queue=len(resource.queue))
        elif family.kind == "signal":
            signal = self._signals[name]
            row.update(count=signal.count)
        else:
            holder = self._levels()[name]
            row.update(
                level=getattr(holder, "level", None),
                count=len(getattr(holder, "items", [])),
                putters=len(holder.put_queue),
                getters=len(holder.get_queue),
            )
        return row

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
            started = self._env.process(process)
            run = self._runs_of.pop(process, None)
            if run is not None:
                # A task instance is reachable by `interrupt` from the step it starts in
                run.reach(started)
            return started
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
            except Exception as error:  # the event failed
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
        arg_values: Optional[Dict[str, Any]] = None,
        handles: Any = None,
    ) -> simpy.Process:
        """Starts one instance of a task as a process, with its next instance id.

        `args` are the positional arguments after the task id; None gives the ones a
        YAML task takes, the context, and none for a builder task. `arg_values` are
        the names the flow reads as `arg.<name>`; `handles` is the table of the flow that
        starts it, which it reads as `handle.<name>`.
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
            arg_values=arg_values,
            handles=handles,
        )
        generator = run.run()
        # SimPy names a process after its generator's __name__; name it after the
        # task, not `run`, so `active_process.name` and records read the task (P6).
        generator.__name__ = spec.name  # type: ignore[attr-defined]
        process = self.env.process(generator)
        run.reach(process)
        if child and self._on_unhandled_failure == "continue":
            # Under `continue`, a child flow that fails with no task waiting for it
            # ends that flow, not the run; under `stop` the failure stops the run.
            process.callbacks.append(_defuse)
        return process

    def _instance_scopes(self, task: str) -> List[Any]:
        """The scopes of the running instances of a task, for `running(task, ...)`."""
        return [run.scope() for run in self._running.get(task, [])]

    def _spawn_children(
        self,
        spec: Mapping[str, Any],
        parent: Optional[Entity],
        upstream: Optional[Upstream],
        scope: Optional[Scope] = None,
    ) -> None:
        """Starts the entities of a `spawn:` key or `on:` action: `count` of them, read
        once before the first starts, each by `_spawn_entity`. The count and the `args`
        read `scope`, the spawning task's names when it is given, and the run and the
        parent's attributes otherwise; each child reads its `args` once.

        A count of 0 or less starts none, as `range` gives none. A function of the
        entity gives the count, and a result that is not a whole number (a boolean
        counts as 1) raises `TypeError`, as `range(2.0)` does.
        """
        count = spec["count"]
        if scope is None:
            scope = self.values.scope_of(parent)
        if callable(count):
            count = count(parent)
        elif isinstance(count, Expression):
            count = count.evaluate(scope)
        elif isinstance(count, _Gen):
            count = self.values.evaluate(count, scope)
        if isinstance(count, bool):
            count = int(count)
        try:
            count = operator.index(count)
        except TypeError:
            raise TypeError(
                f"the spawn count of task '{spec['task']}' gave {count!r}, which is "
                f"not a whole number"
            ) from None
        args = spec.get("args")
        for _ in range(count):
            self._spawn_entity(
                spec["task"],
                parent,
                upstream,
                spec["after"],
                spec.get("copy_attributes", False),
                None if args is None else self.values.evaluate(dict(args), scope),
            )

    def _spawn_entity(
        self,
        task: str,
        parent: Optional[Entity],
        upstream: Optional[Upstream],
        after: Optional[float],
        copy_attributes: bool = False,
        arg_values: Optional[Dict[str, Any]] = None,
    ) -> simpy.Process:
        """Starts `task` as the first task of a new entity, keyed `<task>-n`, with
        `parent` as its `spawned_by` and `attr.parent` the parent's key.

        Without `after` the entity is made and counted in now; with `after` a wrapper
        named after the task makes and counts it in when the task starts, built with
        SimPy's `start_delayed` so the events order as SimPy's.
        """
        parent_key = parent.key if parent is not None else None

        def make() -> Any:
            # A copy is one level deep: a nested mapping stays shared with the parent
            copied = dict(parent.attributes) if copy_attributes and parent else {}
            entity = self._new_entity(task, ({**copied, "parent": parent_key}, None))
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
                arg_values=arg_values,
            )
            gen = run.run()
            gen.__name__ = task  # type: ignore[attr-defined]
            return run, gen

        if after is None:
            run, gen = make()
            process = self.env.process(gen)
            run.reach(process)
            return process

        def wrapper() -> Any:
            return (yield from make()[1])

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
        priority: Optional[int] = None,
        defused: bool = False,
    ) -> None:
        """Fires or fails a signal through one path, counting its `on:` actions
        against `entity` so a drain waits for them.

        When the firing is new, the signal has `on:` actions, and `entity` has not
        left, 1 is added to `entity.ends_pending` and the entity is stored on the
        pending event, so the signal's last callback counts it back out.

        A firing with `urgent`, `after` or a `priority` is for a one-shot signal and is
        scheduled with that priority and delay. `defused` is for a failure nobody waits
        for. With `after` it is not counted against
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
            signal.fire(value, urgent=urgent, after=after or 0.0, priority=priority)
        else:
            signal.fail(
                value,
                urgent=urgent,
                after=after or 0.0,
                priority=priority,
                defused=defused,
            )

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
        names: Optional[Mapping[str, Any]] = None,
        scope: Optional[Scope] = None,
    ) -> None:
        """Runs one `on:` action: fire a signal, spawn a new entity, interrupt a
        task, or set a registry value. `owner` labels the action for a warning,
        `entity` is the entity it runs for, and `upstream` is what a spawn hands on.
        An entry with `when` runs only when its expression is true in the entity's
        scope, which is read before any value is evaluated.
        A value that holds a generator is evaluated first, over the entity's
        attributes and, for a task's action, the task's `scope`: its vars, `arg`, `item`,
        `upstream`, `process` and the rest.
        """
        needs_scope = action.get("when") is not None or (
            "spawn" not in action
            and (
                has_generator(action)
                or isinstance(action.get("fire", {}).get("signal"), Reference)
            )
        )
        if scope is None and needs_scope:
            from desflow.core.flow import make_scope

            assert self._env is not None

            scope = make_scope(
                upstream,
                {} if entity is None else entity.attributes,
                self._env,
                self._levels(),
                signals=self._signals,
                resources=self._resources_map,
                families=self._families,
                names=names,
                entity=None if entity is None else entity.key,
                peek=self._env.peek,
                instances=self._instance_scopes,
            )
        when = action.get("when")
        if when is not None:
            assert scope is not None
            if not when.evaluate(scope):
                return
        # A spawn reads its count and its args itself, once for each child
        if needs_scope and scope is not None and "spawn" not in action:
            action = self.values.evaluate(action, scope)
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
            target = spec["signal"]
            if isinstance(target, Reference):
                assert scope is not None
                target = self._open_reference(target, scope)
            self._fire_signal(
                target,
                ok,
                value,
                entity,
                urgent=spec.get("urgent", False),
                after=spec.get("after"),
                priority=spec.get("priority"),
                defused=spec.get("defused", False),
            )
        elif "spawn" in action:
            self._spawn_children(action["spawn"], entity, upstream, scope)
        elif "activate" in action:
            self._set_arrivals(action["activate"], True)
        elif "deactivate" in action:
            self._set_arrivals(action["deactivate"], False)
        elif "resume" in action:
            spec = action["resume"]
            self._resume(
                spec["task"],
                spec.get("where"),
                spec.get("value"),
                spec.get("limit"),
                scope,
            )
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

    def _resume(
        self,
        task: str,
        where: Union[str, Expression, None],
        value: Any,
        limit: Any,
        scope: Optional[Scope] = None,
    ) -> int:
        """Wakes the instances of `task` suspended in a `wait: {suspend: true}`, in
        the order they suspended, as `event.succeed(value)` wakes a process.

        Args:
            task: The task whose suspended instances to wake.
            where: A condition read in each instance's scope, or None for all.
            value: What each woken instance reads as `wait.suspend.value`, already
                evaluated.
            limit: How many to wake at most: a whole number, a boolean counting as 1,
                an `Expression` read in `scope`, or None for all. 0 or less wakes none.
            scope: What a `limit` expression reads.

        Returns:
            int: How many instances were woken.

        Raises:
            TypeError: If `limit` is not a whole number.
        """
        if isinstance(limit, Expression):
            limit = limit.evaluate(
                scope if scope is not None else self.values.scope_of(None)
            )
        if limit is not None:
            limit = whole(limit, f"the resume limit of task '{task}'")
        condition = Expression(where) if isinstance(where, str) else where
        woken = 0
        for event in list(self._suspended.get(task, [])):
            if limit is not None and woken >= limit:
                break
            if condition is not None and not condition.evaluate(event.run.scope()):
                continue
            event.cancel()  # leaves the list
            event.succeed(value)
            woken += 1
        return woken

    def _copy_outcome(
        self, fire: Dict[str, Any], entity: Optional[Entity]
    ) -> Optional[Tuple[bool, Any]]:
        """The outcome a `fire` action fires with: its literal `value`, as a success or,
        with `fail`, as the reason of a failure, or the outcome `from` copies from a
        signal or a task. None when there is nothing to copy (a signal not fired, a task
        with no ended instance, or no entity)."""
        source = fire["from"]
        if source is None:
            return not fire.get("fail", False), fire["value"]
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
        once: bool = True,
        on: Optional[List[Mapping[str, Any]]] = None,
        family: bool = False,
        members: Any = None,
        defused: bool = False,
    ) -> "SimulationContext":
        """
        Declares a signal that tasks fire and wait for.

        A task fires it with `signal=name` when it finishes, and fails it when the
        task fails; a task waits for it with `wait={"signal": name}`; a process
        reaches it with `context.signal(name)`.

        Args:
            name: The signal's name.
            once: When True, the default, the signal fires once, as SimPy's `Event`
                does: it does not re-arm, a wait after the firing resumes at once,
                and a second firing is an error under `stop`. When False it re-arms
                after each firing, and a wait after a firing waits for the next.
            on: Actions run when the signal fires, each a mapping with one of `fire`,
                `spawn`, `interrupt` or `path`, as a task's `on` takes them.
            family: When True, the entry is a family of signals, one for each key,
                made from this entry. A task names a member with `{"of": name, "key":
                "<expression>"}` where it names a signal, and a member is made at its
                first use.
            members: With `family`, how many members exist when the run starts, with
                the keys 0 to n - 1: a whole number, an expression text, or a
                generator, read once when the run is built. None starts the family
                empty.
            defused: When True, a failure of the signal that no waiter handles does
                not stop the run, as a callback's `event.defused = True` does.
                Without it, a one-shot signal's failure with no waiter stops the run
                under `on_unhandled_failure: stop`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `members` is given without `family`, or `defused` is not
                True or False.
        """
        _family_fields("signal", name, family, members)
        if not isinstance(defused, bool):
            raise ValueError(
                f"signal '{name}': defused is true or false, got {defused!r}"
            )
        self._set_family("signal", name, family, members, {})
        if name not in self._signal_names:
            self._signal_names.append(name)
        self._signal_once[name] = once
        self._signal_defused[name] = defused
        self._signal_on[name] = [_parse_on_action(entry) for entry in on or []]
        self._bind_values(
            self._signal_on[name], ("signals", name, "on"), overwrite=False
        )
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
            resources=self._without_families(self._resources_config),
            containers=self._without_families(self._containers_config),
            stores=self._without_families(self._stores_config),
            breakdowns=self._without_families(self._breakdowns_config),
            service=self._services_config,
            arrival=self._arrivals_config,
            variables=self._variables_config,
        )

    def _without_families(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """The entries of a section that are not families: a family's entry is the
        template of its members and has no paths of its own."""
        if not self._families:
            return config
        return {k: v for k, v in config.items() if k not in self._families}

    def _scratch_registry(self, isolated: bool = False) -> SimulationRegistry:
        """Builds a registry with the paths a run registers, on a plain SimPy clock.

        The load checks and `registry_paths` call it, so they cannot build the
        registry two ways. Registration fills defaults on the shared config objects,
        as a run does.

        Args:
            isolated: Register a deep copy of the parameters instead. A check that
                applies values with `change` needs this, because a change is stored
                in the config object, and the model's own starting values must not
                move.
        """
        registry = SimulationRegistry(simpy.Environment())
        parameters = self.compile_parameters()
        registry.register_sim_parameter(
            copy.deepcopy(parameters) if isolated else parameters
        )
        return registry

    def registry_paths(self) -> Dict[str, Any]:
        """
        Returns the registry paths a run will have, with their starting values.

        It builds the registry the way a run does, runs no model code and starts no
        connector or thread. A server's inspector uses it to list the paths a live
        change can set.

        A capacity with no limit has the value `math.inf`, which JSON cannot hold,
        so a caller that writes JSON converts it.

        Returns:
            Dict[str, Any]: Each path, such as `Shop.resources.packers.current_cap`,
                with its starting value, in the order a run registers them.

        Raises:
            ValueError: If the registry refuses the model's parameters, as a run
                would at its start.
        """
        registry = self._scratch_registry()
        return {path: registry.get(path).value for path in registry.paths()}

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
        segments: Any = None,
        steps: Optional[bool] = None,
        sync: Optional[bool] = None,
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
            on_unhandled_failure: `"stop"` or `"continue"`, as `run.on_unhandled_failure`.
            segments: Runs the model in parts and publishes `system.simulation.segment`
                after each, as `run.segments` does: a list of mappings with the keys
                `until`, `stop`, `look` and `sync` of a blueprint's entry, or
                `{"every": seconds}` for equal parts up to `until`. A context built
                by `from_yaml` uses the blueprint's `run.segments` when this is None.
            steps: True publishes `system.simulation.step` after each model event,
                as `run.steps` does. Not with `segments`, `stop` or `drain`.
            sync: True calls `env.sync()` as the run starts, as `run.sync` does.

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
        self.start(
            until,
            stop,
            drain,
            on_unhandled_failure=on_unhandled_failure,
            segments=segments,
            steps=steps,
            sync=sync,
        )
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
            if not self._moved:
                self._run_plan()
            if self._stopped:
                return None
            if self._end is not None:
                if self._end.callbacks is None and not self._end._ok:
                    # `env.run(until=...)` returns the value of a processed event,
                    # so a failed stop that advance() reached is raised here
                    raise self._end._value
                return self._run_env(self._end)
            if self._run_ended():
                # advance() or step() already reached until, where a run ends
                return None
            return self._run_env(self._until)
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
        self._moved = True
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
            try:
                return env.run(
                    until=until if self._until is None else min(until, self._until)
                )
            except RunStopped:
                return None  # a `row` with on_end: stop ended the run
        segment = simpy.Event(env)
        segment._ok, segment._value = True, None
        env.schedule(segment, simpy.events.URGENT, until - env.now)
        return self._advance_to(segment)

    def _run_plan(self) -> None:
        """Runs the blueprint's `run.segments` or `run.steps` before `finish()` runs the
        rest of the run, publishing a reading after each segment or event."""
        if self._segments is not None:
            self._run_segments(self._segments)
        elif self._steps:
            self._run_steps()

    def _segment_entries(self, segments: Any) -> List[Mapping[str, Any]]:
        """The entries of `run.segments`: the list as given, or for `{every: t}` one
        entry ending at each multiple of `t` below `until`, and one at `until`."""
        if not isinstance(segments, Mapping):
            return list(segments)
        every, entries, count = segments["every"], [], 1
        while (
            self._until is not None and self.initial_time + count * every < self._until
        ):
            entries.append({"until": self.initial_time + count * every})
            count += 1
        return [*entries, {"until": self._until}]

    def _run_segments(self, segments: Any) -> None:
        """Runs the model one segment at a time, as a SimPy program that calls
        `env.run(until=...)` in a loop does, and publishes `system.simulation.segment`
        after each.

        A segment ends at its `until`, at its `stop`, or, for `{}`, at the end of the
        run. A segment after the run's own end does not run. A list with no `run.until`
        whose last entry has an `until` or a `stop` ends the run there, as a SimPy
        program that stops calling `env.run` does; `{}` as the last entry runs to the
        end. `look` names a task whose new entity runs its zero-time steps once the
        segment has stopped, ahead of the events at the stop time.
        """
        env = self.env
        entries = self._segment_entries(segments)
        for index, entry in enumerate(entries):
            if self._run_ended():
                break
            if entry.get("sync"):
                env.sync()
            if entry.get("until") is not None:
                self.advance(entry["until"])
            elif entry.get("stop") is not None:
                self.advance(self._stop_event(entry["stop"]))
            else:
                self._run_to_end()
            if entry.get("look") is not None:
                self._look(entry["look"])
            self._publish_reading("system.simulation.segment", {"index": index})
            if entry.get("until") is None and entry.get("stop") is None:
                break
        else:
            if entries and self._until is None and isinstance(segments, list):
                self._stopped = True

    def _run_to_end(self) -> None:
        """Runs a started run to its own end, which `finish()` then finds reached."""
        self._moved = True
        self._run_env(self._end if self._end is not None else self._until)

    def _look(self, task: str) -> None:
        """Runs a new entity of `task` to the end of its zero-time steps, before any
        other event at this instant, which SimPy's urgent start of a process ensures."""
        process = self._spawn_entity(task, None, None, None)
        try:
            while not process.triggered:
                self.env.step()
        except StopSimulation:
            pass  # _stop_run() ended the run

    def _run_steps(self) -> None:
        """Processes the model's events one at a time up to `until`, as a loop of
        `env.peek()` and `env.step()` does, publishing `system.simulation.step` after
        each. The run ends in place of SimPy's `EmptySchedule` when none is left."""
        count = 0
        while self.peek() < (math.inf if self._until is None else self._until):
            self.step()
            if self._stopped:
                break
            count += 1
            self._publish_reading("system.simulation.step", {"step": count})

    def _publish_reading(self, path: str, fields: Dict[str, Any]) -> None:
        """Publishes `fields`, then `now` and `peek` (null when no model event is
        left, because a JSON record cannot hold infinity), as telemetry."""
        env = self.env
        head = env.peek()
        env.publish_telemetry(
            path, {**fields, "now": env.now, "peek": None if math.isinf(head) else head}
        )

    def _run_env(self, until: Any) -> Any:
        """Runs the environment to `until`, ending the run when no model event is left
        if the context has no `until` of its own.

        A `stop` or a drain reaches `env.run` as an event, so the environment cannot
        tell from its argument that the run has no time limit.
        """
        env = self._env
        assert env is not None  # the callers checked that the run has started
        previous, env._until_none = env._until_none, self._until is None
        try:
            return env.run(until=until)
        except RunStopped:
            # A `row` with on_end: stop used its last row where no task ran: an
            # arrival's attributes, vars or key. _stop_run() ended the run.
            return None
        finally:
            env._until_none = previous

    def _run_ended(self) -> bool:
        """Whether the run has reached its own end: the end event of a `stop` or
        `drain` has been processed, else `until` has been reached."""
        if self._stopped:
            return True
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
            return self._run_env(target)
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
        source: Optional[str] = None,
        actor: Optional[str] = None,
        reason: Optional[str] = None,
        segments: Any = None,
        steps: Optional[bool] = None,
        sync: Optional[bool] = None,
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
            source: How the `ParamChange` of each override describes it: where it
                came from, such as `"user"`.
            actor: How the `ParamChange` of each override describes it: who made it.
            reason: How the `ParamChange` of each override describes it: why.
            segments: As for `run()`. The segments and the steps run in `finish()`,
                and only when `advance` and `step` have not moved the run.
            steps: As for `run()`.
            sync: As for `run()`.

        Returns:
            DynamicRealtimeEnvironment: The environment, also `context.env`.

        Raises:
            ValueError: As for `run()`; or `segments` is malformed, a segment's
                `look` names a task that does not exist, or `{"every": ...}` has no
                `until`; or `steps` is used with `segments`, or `steps` or `sync` is
                not a bool; or `stats` is not a level, or an override's path does not
                exist or its value is refused, or a trace sink has no
                `with_stats(trace=True)`.
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
        if segments is not None:
            self._segments = _segments_arg(segments)
        elif run_settings:
            self._segments = self._default_segments
        else:
            self._segments = None
        self._steps = _flag_arg(
            "steps", steps, self._default_steps if run_settings else False
        )
        self._sync = _flag_arg(
            "sync", sync, self._default_sync if run_settings else False
        )
        self._moved = False
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
        if (stop or drain) and self._segments is not None:
            raise ValueError(
                "run.segments cannot be used with run.stop or run.drain: a segment "
                "ends at its own until or stop"
            )
        if (stop or drain) and self._steps:
            raise ValueError(
                "run.steps cannot be used with run.stop or run.drain: steps already "
                "reads the run after each event"
            )
        if self._steps and self._segments is not None:
            raise ValueError(
                "run.steps cannot be used with run.segments: steps already reads "
                "the run after each event"
            )
        if isinstance(self._segments, Mapping) and until is None:
            raise ValueError("segments {'every': ...} needs an until to divide")
        for entry in self._segments if isinstance(self._segments, list) else ():
            look = entry.get("look")
            if look is not None and look not in self._tasks:
                raise ValueError(
                    f"a segment's look names task '{look}', "
                    + _not_added(look, self._tasks, f"@app.task(name='{look}')")
                )
        if isinstance(drain, Mapping) and drain.get("limit") is not None:
            _seconds(drain["limit"])
        if stats is not None and stats not in STATS_LEVELS:
            raise ValueError(
                f"stats must be one of {', '.join(STATS_LEVELS)}, got {stats!r}"
            )
        _check_end(until, stop, drain, self._signal_names)
        self._check_tasks(overrides)
        if any(self._egress_traces) and not self._stats_trace:
            raise ValueError(
                "when='trace' receives the records of stats.trace, so the context needs "
                "with_stats(trace=True)"
            )
        for setup in self._before_run:
            setup()
        # A run started and not closed is closed first, so its connectors stop
        if self._env is not None and not self._closed:
            self.close()
        # A second run of the same context starts with no entity and no task, and
        # numbers its entities and instances from the start again, so it repeats
        self._in_system = 0
        self._first_due = {
            name: plan.first_at
            for name, plan in self._arrival_plans.items()
            if plan.first_at is not None and not plan.has_entities
        }
        self._live_keys = {}
        self._key_skips = {}
        self._draining = False
        self._running = {}
        self._suspended = {}
        self._open_batches = {}
        self._entity_counters = {}
        self._arrival_on = {
            name: plan.active for name, plan in self._arrival_plans.items()
        }
        self._arrival_changed = {}
        self._instance_counters = {}
        self._sample_seen = 0
        self._env = None
        self.started = False
        self._stopped = False

        logger.info(f"Building SimulationContext for '{self.sim_id}'...")
        try:
            return self._build(
                until,
                stop,
                drain,
                seed,
                overrides,
                stats,
                egress,
                ingress,
                source,
                actor,
                reason,
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
        source: Optional[str],
        actor: Optional[str],
        reason: Optional[str],
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
        # The callbacks of this run, in place before the overrides are applied
        self._env.registry.on_change.extend(self.on_change)

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
        # Fresh counters, vars_once and unique history for every run; each generator
        # draws from a sequence of its own, made from the seed, never the sampler's
        self.values = ValueRuntime(seed, self._env, self.sampler, self)
        for task_name in sorted(
            self._row_targets
            | {n for n, spec in self._task_specs.items() if spec.history is not None}
        ):
            self.values.keep_rows(
                task_name, self._task_specs[task_name].history or DEFAULT_HISTORY
            )
        for task_name in self._previous_users:
            self.values.keep_previous(task_name)
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
        self._env.stats.on_change = self._stats_on_change and level != "none"
        self._signals = {
            name: Signal(
                self._env,
                name,
                once=self._signal_once.get(name, True),
                defused=self._signal_defused.get(name, False),
            )
            for name in self._signal_names
            if name not in self._families
        }
        for family in self._families.values():
            family.keys = []
        for name, actions in self._signal_on.items():
            if actions and name not in self._families:
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
            self._env.registry.set_exactly(
                path, value, source=source, actor=actor, reason=reason
            )

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
                traces=self._egress_traces,
                envelopes=self._egress_envelopes,
                warmup=self.warmup,
                lag_monitor_interval=(
                    "auto" if self.lag_interval is None else self.lag_interval
                ),
            )
        elif self.warmup is not None:
            self._env.start_internal(self._end_warmup(self.warmup))
        self._env.trace_events(
            self._stats_trace and egress and any(self._egress_traces)
        )

        # Hydrate Physical SimPy Resources
        for holders in (self._resources_map, self._stores_map, self._containers_map):
            for old in [
                n for n in holders if "[" in n and family_part(n) in self._families
            ]:
                del holders[old]
        for res_id in self._resources_config.keys():
            if res_id not in self._families:
                self._new_resource(res_id)
        for store_id in self._stores_config:
            if store_id not in self._families:
                self._new_store(store_id)
        for container_id in self._containers_config:
            if container_id not in self._families:
                self._new_container(container_id)
        for res_id, breakdown in self._breakdowns_config.items():
            if res_id not in self._families:
                self._start_breakdown(res_id, breakdown)
        self._start_members()

        # Boot Registered Daemon Generators
        self._arrival_processes = []
        for loop_func, meta in self._startup_loops:
            if meta:
                # Arrival loops require context injection
                process = self._env.process(loop_func(self))
                # An arrival's `initial` entities are created right after its source
                # starts, so file order is creation order
                plan = self._arrival_plans.get(meta)
                if plan is not None and plan.initial:
                    self._start_initial(meta, plan)
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
        if self._env.strict or self._sync:
            # Building takes wall time, which a strict run must not count as lag
            self._env.sync()
        logger.info("Simulation engine started.")
        return self._env

    def _stop_run(self) -> None:
        """Ends the run at the current time: the events already due run no further,
        `finish()` returns at once, and a later `advance()` or `step()` raises and
        `peek()` gives infinity.

        A `row` generator with `on_end: stop` calls it when its rows run out."""
        env = self._env
        if env is None or self._stopped:
            return
        self._stopped = True
        event = env.internal_event()
        event._ok, event._value = True, None
        event.callbacks.append(StopSimulation.callback)
        env.schedule(event, simpy.events.URGENT)

    def step(self) -> None:
        """
        Processes the next model event of a started run, as SimPy's `env.step()` does.

        The events of desflow's own that are due before it, such as a statistics
        reading or a flush timer, run in the same call and are not counted.

        Raises:
            RuntimeError: If the run has not started, or it was ended by a `row` with
                `on_end: stop` or by the last entry of `run.segments`, as `advance()`
                raises (`close()` it, or `start()` a new run).
            simpy.core.EmptySchedule: If no model event is left.
        """
        env = self.env
        if self._stopped:
            raise RuntimeError(
                f"the run ended at {env.now:g}; close() it, or start() a new run"
            )
        try:
            env.step()
        except (StopSimulation, RunStopped):
            pass  # _stop_run() ended the run
        self._moved = True

    def peek(self) -> float:
        """
        Returns the time of the next model event of a started run, or infinity if none.

        The events of desflow's own, such as a statistics reading or a flush timer,
        are not counted. It is infinity once a `row` with `on_end: stop` or the last
        entry of `run.segments` has ended the run, because `step()` runs no more.

        Raises:
            RuntimeError: If the run has not started.
        """
        env = self.env
        return math.inf if self._stopped else env.peek()

    def close(self) -> None:
        """
        Ends a started run: publishes the last statistics, flushes and stops the
        egress connectors, then stops the ingress connectors. Closing twice does
        nothing.
        """
        if self._env is not None and not self._closed:
            self._closed = True
            self.started = False
            if self.values is not None:
                self.values.report_refused()
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
            env.schedule(horizon, simpy.events.URGENT, until - env.now)
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
            target = name_of(target, "a stop signal")
            if isinstance(target, Reference):
                # Read once, in run scope, as the run starts
                target = self._open_reference(target, self.values.scope_of())
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
                families=self._families,
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
        if entity.loop is None:
            self.values.forget(entity.key)
        else:
            entity.loop.active -= 1
            entity.loop.release(self, entity.key)
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
            if "resume" in action:
                target = action["resume"]["task"]
                if target not in self._task_specs:
                    raise ValueError(
                        f"the on: action of {owner} resumes task '{target}', "
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
                problem = timing_problem(
                    fire.get("urgent", False),
                    fire.get("after"),
                    fire.get("priority"),
                    self._signal_once.get(signal, True),
                    "`once=True`",
                )
                if problem is not None:
                    raise ValueError(problem)
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
            listened = [] if spec.listen is None else spec.listen.on
            for action in [*spec.on, *listened]:
                check_action(name, action, owner)
            for source in fire_sources([*spec.on, *listened]):
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
        self._whole_flow = {
            node.target["task"]
            for spec in self._task_specs.values()
            for node in (spec.wait.branches.values() if spec.wait else ())
            if node.kind == "running" and node.target["whole_flow"]
        }
        targets = {
            spec.release.task
            for spec in self._task_specs.values()
            if spec.release is not None and spec.release.task is not None
        }
        self._held_by_task = {
            task: self._held_by_task.get(task, []) for task in sorted(targets)
        }

    def _check_lookups(self) -> None:
        """Checks that every task a `lookup` or `aggregate` names is a task, and that
        no tasks look each other up, because none of them could start. A task that
        `note_values` names must be a task too."""
        for target in sorted(self._row_targets):
            if target not in self._task_specs:
                raise ValueError(
                    f"a lookup or aggregate names task '{target}', "
                    + _not_added(target, self._task_specs, "@app.task()")
                )
        for task in sorted(self._gates):
            if task not in self._task_specs:
                raise ValueError(
                    f"note_values names task '{task}', "
                    + _not_added(task, self._task_specs, "@app.task()")
                )
        cycle = lookup_cycle(self._gates)
        if cycle:
            raise ValueError(
                "tasks look each other up, so none could start a flow before the "
                "other had a row: " + " -> ".join(cycle)
            )

    def _check_batches(self) -> None:
        """Refuses two tasks that share a batch name but differ in what the batch needs
        one answer for.

        Raises:
            ValueError: Naming both tasks and the key that differs.
        """
        found = batch_mismatch(self._task_specs)
        if found is not None:
            raise ValueError(batch_mismatch_text(*found))

    def _declared_names(self) -> Dict[str, Dict[str, Any]]:
        """The declared names of each kind, a family's entry included."""
        return {
            "resource": self._resources_config,
            "store": self._stores_config,
            "container": self._containers_config,
            "signal": {name: None for name in self._signal_names},
        }

    _FAMILY_READ_SOURCES = (
        "users",
        "queue",
        "held",
        "waiting",
        "level",
        "putters",
        "getters",
        "putting",
        "getting",
        "count",
        "items",
        "signal",
    )

    def family_read_problem(self, expression: Expression) -> Optional[str]:
        """What is wrong with the way an expression reads families, or None: a family
        read without an index, or an index on a name that is no family.

        Args:
            expression: The parsed expression.

        Returns:
            Optional[str]: The reason, naming the read.
        """
        for source, name in sorted(expression.plain):
            if name in self._families and source in self._FAMILY_READ_SOURCES:
                return (
                    f"reads {source}.{name} of a family; read one member with "
                    f"{source}.{name}[<key>], or list the keys with members({name})"
                )
        for source, name in sorted(expression.indexed):
            if name not in self._families:
                return (
                    f"reads {source}.{name}[...], but '{name}' is not a family; add "
                    f"family: true to its entry"
                )
        return None

    def _family_read_problem(self) -> Optional[Tuple[str, str, str]]:
        """The first expression of a task, a generator or a rule that reads a family
        wrongly.

        Returns:
            Optional[Tuple[str, str, str]]: The task, the key and the message, or None.
        """
        for task, spec in self._task_specs.items():
            for key, expression in spec.expressions:
                found = self.family_read_problem(expression)
                if found is not None:
                    return task, key, found
            for key, _where, _after, place in task_value_places(
                lambda key: _builder_value(spec, key)
            ):
                for _where, generator in generators_in(place, (_Gen,)):
                    for text in _generator_expressions(generator):
                        try:
                            read = Expression(text, names=ANY_NAME)
                        except ValueError:
                            continue
                        found = self.family_read_problem(read)
                        if found is not None:
                            return task, key, found
        for place, expression in self._rule_expressions():
            found = self.family_read_problem(expression)
            if found is not None:
                return place, "", found
        return None

    def _check_families(self) -> None:
        """Checks that every place naming a family uses a reference, a reference's `of`
        is a family of the right kind, a list names declared entries, and no entry is
        named as a member.

        Raises:
            ValueError: Naming the task and the key.
        """
        found = (
            family_problem(
                self._task_specs,
                self._families,
                self._declared_names(),
                self._signal_on,
            )
            or self._family_read_problem()
        )
        if found is not None:
            holder, key, message = found
            if not key:
                raise ValueError(f"{holder} {message}")
            label = holder if holder.startswith("signals.") else f"task '{holder}'"
            raise ValueError(f"{label}: {key} {message}")
        for kind, names in self._declared_names().items():
            for name in names:
                if name in self._families:
                    continue
                for family in self._families:
                    if name.startswith(f"{family}["):
                        raise ValueError(
                            f"{_FAMILY_SECTIONS[kind]}.{name} starts with the name of "
                            f"the family '{family}' and a bracket; the members of a "
                            f"family are named that way, so no declared entry may be"
                        )

    def _check_tasks(self, overrides: Optional[Mapping[str, Any]] = None) -> None:
        """Checks what each task names, before the clock or any connector starts.

        Args:
            overrides: The values `start()` applies before the clock starts. The
                scenario is checked against them, as the run will apply it.
        """
        self._build_end_sets()
        self._check_batches()
        self._check_lookups()
        self._check_families()
        if self._scenario_steps:
            known = self._scratch_registry(isolated=True)
            for path, value in (overrides or {}).items():
                try:
                    known.change(path, value)
                except ParamRejected:
                    # A refused override is reported by the run's own
                    # set_exactly, with its own message; change logs nothing here
                    pass
            # In the order run_scenario applies them, so a value that is valid only
            # after an earlier step loads as it runs
            for step in sorted(self._scenario_steps, key=lambda step: step["at"]):
                if (
                    step.get("path") is not None
                    and member_path(str(step["path"]), self._families) is None
                ):
                    try:
                        known.get(step["path"])
                    except KeyError:
                        raise ValueError(
                            f"scenario step at {step['at']:g} s sets '{step['path']}', "
                            f"which is not a registry path"
                            + suggestion(step["path"], known.paths())
                        ) from None
                    try:
                        if not has_generator(step["value"]):
                            # A generated value is known when the step is due
                            known.change(step["path"], step["value"])
                    except ParamRejected as exc:
                        raise ValueError(
                            f"scenario step at {step['at']:g} s sets "
                            f"'{step['path']}': {exc.reason}"
                        ) from None
                action = step.get("interrupt")
                if (
                    action is not None
                    and action["task"] is not None
                    and action["task"] not in self._tasks
                ):
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
        opens: Dict[str, List[Tuple[str, str]]] = {}
        for name, spec in self._task_specs.items():
            if spec.open is not None:
                for handle in spec.open.nodes:
                    opens.setdefault(handle, []).append(
                        (name, spec.open.opened_as(handle))
                    )
        uses: List[Tuple[str, str, str, str]] = []
        for name, spec in self._task_specs.items():
            if spec.wait is not None:
                uses += [
                    (name, "wait", h, "any")
                    for h in sorted(spec.wait.targets("handle"))
                ]
            if spec.open is not None:
                uses += [
                    (name, "open", h, "any")
                    for h in sorted(spec.open.targets("handle"))
                ]
            if spec.wait is not None:
                uses += [
                    (name, "wait", h, "element")
                    for h in sorted(spec.wait.indexed_handles())
                ]
            if spec.open is not None:
                uses += [
                    (name, "open", h, "element")
                    for h in sorted(spec.open.indexed_handles())
                ]
            if spec.cancel is not None:
                uses.append((name, "cancel", spec.cancel, "line"))
                if spec.cancel_index is not None:
                    uses.append((name, "cancel", spec.cancel, "element"))
            if spec.release is not None and spec.release.handle is not None:
                uses.append((name, "release", spec.release.handle, "unit"))
                if spec.release.index is not None:
                    uses.append((name, "release", spec.release.handle, "element"))
            if spec.listen is not None and spec.listen.handle is not None:
                uses.append((name, "listen", spec.listen.handle, "any"))
                if spec.listen.index is not None:
                    uses.append((name, "listen", spec.listen.handle, "element"))
            if spec.interrupt is not None and spec.interrupt.get("handle") is not None:
                uses.append((name, "interrupt", spec.interrupt["handle"], "child"))
                if spec.interrupt.get("index") is not None:
                    uses.append(
                        (name, "interrupt", spec.interrupt["handle"], "element")
                    )
            for key, expression in spec.expressions:
                uses += [
                    (name, key, h, "any")
                    for h in sorted(expression.references["handle"])
                ]
            # The expressions inside the generators of the task's value places
            for key, _where, _after, place in task_value_places(
                lambda key: _builder_value(spec, key)
            ):
                for _where, generator in generators_in(place, (_Gen,)):
                    for text in _generator_expressions(generator):
                        try:
                            read = Expression(text, names=ANY_NAME)
                        except ValueError:
                            continue  # reported where the value is checked
                        uses += [
                            (name, key, h, "any")
                            for h in sorted(read.references["handle"])
                        ]
        unmatched = handle_problem(opens, uses)
        if unmatched is not None:
            holder, key, message = unmatched
            raise ValueError(f"task '{holder}': {key} {message}")
        for message in check_acquisition_order(
            {name: list(spec.resources) for name, spec in self._task_specs.items()},
            {
                name: (spec.keep, spec.router.tasks or set())
                for name, spec in self._task_specs.items()
            },
            {name: spec.wait for name, spec in self._task_specs.items()},
            {name: _holding(spec) for name, spec in self._task_specs.items()},
        ):
            if message not in self._order_warned:
                self._order_warned.add(message)
                logger.warning(message)
        holders = {**self._stores_config, **self._containers_config}
        registry: Optional[SimulationRegistry] = None
        written = self._written_paths()
        for place, expression in self._rule_expressions():
            self._check_rule_names(place, expression, holders)
        routes = {
            name: {
                *(spec.router.tasks or ()),
                *(
                    target
                    for target in (
                        spec.on_fail,
                        spec.on_renege,
                        spec.on_balk,
                        spec.on_interrupt_next,
                    )
                    if target is not None
                ),
            }
            for name, spec in self._task_specs.items()
        }
        waited_handles = {
            handle
            for spec in self._task_specs.values()
            if spec.wait is not None
            for handle in spec.wait.targets("handle")
        }
        for message in whole_flow_problems(
            {
                # a child flow a task waits for ends before the task does, and so
                # does one it opens when a task waits for its handle
                name: {
                    *routes[name],
                    *([spec.on_interrupt_run] if spec.on_interrupt_run else ()),
                    *(spec.wait.targets("task") if spec.wait else ()),
                    *(
                        node.target
                        for handle, node in (
                            spec.open.nodes if spec.open else {}
                        ).items()
                        if node.kind == "task" and handle in waited_handles
                    ),
                }
                for name, spec in self._task_specs.items()
            },
            {
                name: [
                    node.target["task"]
                    for node in (spec.wait.branches.values() if spec.wait else ())
                    if node.kind == "running" and node.target.get("whole_flow")
                ]
                for name, spec in self._task_specs.items()
            },
        ):
            if message not in self._order_warned:
                self._order_warned.add(message)
                logger.warning(message)
        after_get_tasks, cause_targets = flow_sets(
            routes,
            [
                name
                for name, spec in self._task_specs.items()
                if spec.get is not None and spec.get.store is not None and spec.get.wait
            ],
            [
                target
                for spec in self._task_specs.values()
                for target in (spec.on_interrupt_next, spec.on_interrupt_run)
                if target
            ],
        )
        for name, spec in self._task_specs.items():
            holds = [
                *spec.resources,
                *([spec.seize.resource] if spec.seize is not None else []),
                *(spec.open.requests() if spec.open is not None else []),
                *(
                    [spec.release.resource]
                    if spec.release is not None and spec.release.resource is not None
                    else []
                ),
            ]
            for resource in holds:
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
                (
                    *(spec.wait.requests() if spec.wait else ()),
                    *spec.resources,
                    *([spec.seize.resource] if spec.seize is not None else []),
                    *(spec.open.requests() if spec.open is not None else []),
                )
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
                if resource in self._resource_rank:
                    raise ValueError(
                        f"task '{name}' has a priority, but resource '{resource}' "
                        f"ranks its queue by an expression, which is every "
                        f"request's priority; remove the priority"
                    )
            if (
                spec.seize is not None
                and spec.seize.priority is not None
                and spec.seize.resource in self._resources_config
                and self._resource_queues.get(spec.seize.resource) != "priority"
            ):
                raise ValueError(
                    f"task '{name}' has a priority on its seize, but resource "
                    f"'{spec.seize.resource}' does not queue by priority; add it "
                    f"with queue='priority'"
                )
            if (
                spec.seize is not None
                and spec.seize.priority is not None
                and spec.seize.resource in self._resource_rank
            ):
                raise ValueError(
                    f"task '{name}' has a priority on its seize, but resource "
                    f"'{spec.seize.resource}' ranks its queue by an expression, which "
                    f"is every request's priority; remove the priority"
                )
            for place, group in (
                ("wait", spec.wait),
                ("open", spec.open.wait if spec.open is not None else None),
            ):
                for node in group.branches.values() if group else ():
                    if (
                        node.options is None
                        or node.options.priority is None
                        or node.target not in self._resources_config
                    ):
                        continue
                    if self._resource_queues.get(node.target) != "priority":
                        raise ValueError(
                            f"task '{name}' has a priority on its request in its "
                            f"{place}, but resource '{node.target}' does not queue by "
                            f"priority; add it with queue='priority'"
                        )
                    if node.target in self._resource_rank:
                        raise ValueError(
                            f"task '{name}' has a priority on its request in its "
                            f"{place}, but resource '{node.target}' ranks its queue "
                            f"by an expression, which is every request's priority; "
                            f"remove the priority"
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
            for place, group in (
                ("wait", spec.wait),
                ("open", spec.open.wait if spec.open is not None else None),
            ):
                for node in group.branches.values() if group else ():
                    if (
                        node.kind == "request"
                        and node.target not in self._resources_config
                    ):
                        raise ValueError(
                            f"task '{name}' requests resource '{node.target}' in "
                            f"its {place}, which is not added"
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
                                f"'{node.target.target}' in its {place}, which is "
                                "not added"
                            )
            for target in spec.router.tasks or ():
                if target not in self._tasks:
                    raise ValueError(
                        f"task '{name}' routes to '{target}', which is not a task"
                    )
            for target in (
                spec.on_fail,
                spec.on_renege,
                spec.on_balk,
                spec.on_interrupt_next,
                spec.on_interrupt_run,
                spec.resume["task"] if spec.resume else None,
                spec.interrupt["task"] if spec.interrupt else None,
                *(spec.wait.targets("task") if spec.wait else ()),
                *(spec.open.targets("task") if spec.open is not None else ()),
                spec.release.task if spec.release is not None else None,
            ):
                if target is not None and target not in self._tasks:
                    raise ValueError(
                        f"task '{name}' names task '{target}', "
                        + _not_added(target, self._tasks, "@app.task()")
                    )
            signals = set(spec.wait.targets("signal")) if spec.wait else set()
            if spec.open is not None:
                signals |= spec.open.targets("signal")
            if spec.signal is not None:
                signals.add(spec.signal)
            if spec.listen is not None and spec.listen.signal is not None:
                signals.add(spec.listen.signal)
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
                problem = scope_problem(
                    expression,
                    own_item=bool(
                        spec.get is not None
                        and spec.get.store is not None
                        and (
                            key == "get"
                            or (
                                spec.get.wait
                                and key in ("put", "next", "fail", "service_time")
                            )
                        )
                    ),
                    later_item=name in after_get_tasks,
                    cause_ok=name in cause_targets,
                    in_when=key == "wait_when",
                    has_each=spec.has_each,
                    each_here=key in ("wait", "open", "subscribe"),
                )
                if problem:
                    raise ValueError(
                        f"task '{name}' reads '{expression.text}': {problem}"
                    )
                unknown = expression.references["wait"] - branches
                # balk and patience are read before the wait starts
                early = key in (
                    "wait",
                    "batch",
                    "balk",
                    "patience",
                    "open",
                    "seize",
                    "preempt",
                )
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
                    and key in ("put", "next", "fail", "service_time")
                )
                if (
                    expression.references["item"]
                    and not (takes_item and after_get)
                    and name not in after_get_tasks
                ):
                    raise ValueError(
                        f"task '{name}' reads item.<field> in '{expression.text}', "
                        f"but it has no store get before it"
                    )
            for key, expression in spec.expressions:
                # vars get a value at step 14, after the service: put, fail and
                # next read them, and nothing evaluated earlier can
                bare = expression.references["name"]
                allowed = (
                    set(spec.var_names)
                    if key in ("put", "fail", "next")
                    else {*spec.vars_once, *spec.vars}
                    if key == "service_time"
                    else set()
                )
                if bare - allowed:
                    var = sorted(bare - allowed)[0]
                    raise ValueError(
                        f"task '{name}' reads the name '{var}' in "
                        f"'{expression.text}', which is "
                        + (
                            "not one of its vars"
                            if key in ("put", "fail", "next")
                            else "not one of the vars a service time reads "
                            "(vars_once and vars)"
                            if key == "service_time"
                            else f"not available in {key}, which is evaluated before "
                            "the vars have a value"
                        )
                    )
            if registry is None:
                # The paths the run will register, built by the same code
                registry = self._scratch_registry()
            problems = task_value_problems(
                vars_once=spec.vars_once,
                vars=spec.vars,
                on_start=spec.on_start,
                finish_vars=spec.finish_vars,
                others={
                    "set": spec.attribute_changes,
                    "interrupt": spec.interrupt or {},
                    "on": spec.on,
                    "fail_reason": spec.fail_reason,
                    "listen": [] if spec.listen is None else spec.listen.on,
                },
                wait_values={
                    **{
                        where: found
                        for node in (spec.wait.branches.values() if spec.wait else ())
                        if node.kind == "timeout"
                        for where, found in (
                            (("wait", node.name, "timeout"), node.target[0]),
                            (("wait", node.name), node.target[1]),
                        )
                    },
                    ("patience",): spec.patience,
                    ("key",): spec.key,
                    ("batch", "timeout"): (
                        None if spec.batch is None else spec.batch.timeout
                    ),
                },
                service_time=spec.service_time,
                services=set(self._services_config),
                base_dir=Path.cwd(),
                registry=registry,
                early_end=(
                    spec.patience is not None
                    or spec.balk is not None
                    or spec.wait is not None
                    or spec.on_interrupt == "abandon"
                    or any(
                        self._resource_queue_limits.get(resource) is not None
                        or self._limit_path(resource) in written
                        for resource in (
                            *spec.resources,
                            *([spec.seize.resource] if spec.seize is not None else []),
                            *(spec.open.requests() if spec.open is not None else []),
                        )
                    )
                ),
            )
            if problems:
                where, message = problems[0]
                raise ValueError(
                    f"task '{name}' {'.'.join(str(p) for p in where)}: {message}"
                )
            for expression in expressions:
                for level in expression.references["level"]:
                    if level not in holders:
                        raise ValueError(
                            f"task '{name}' reads level.{level}, but no store or "
                            f"container is called '{level}'"
                        )
                for kind in ("putters", "getters", "putting", "getting"):
                    for holder in expression.references[kind]:
                        if holder not in holders:
                            raise ValueError(
                                f"task '{name}' reads {kind}({holder}), but no store "
                                f"or container is called '{holder}'"
                                if kind in ("putting", "getting")
                                else f"task '{name}' reads {kind}.{holder}, but no "
                                f"store or container is called '{holder}'"
                            )
                for target in expression.references["running"]:
                    if target not in self._tasks:
                        raise ValueError(
                            f"task '{name}' reads running({target}), but no task is "
                            f"called '{target}'"
                            + _not_added(target, self._tasks, "@app.task()")
                        )
                for kind in ("count", "items"):
                    for store in expression.references[kind]:
                        if store not in self._stores_config:
                            raise ValueError(
                                f"task '{name}' reads {kind}({store}), but no store "
                                f"is called '{store}'"
                            )
                for kind in ("users", "queue", "held", "waiting"):
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
                    if member_path(path, self._families) is not None:
                        continue
                    if registry is None:
                        # The paths the run will register, built by the same code
                        registry = self._scratch_registry()
                    try:
                        registry.get(path)
                    except KeyError:
                        raise ValueError(
                            f"task '{name}' reads registry[\"{path}\"], which is "
                            f"not a registry path"
                        ) from None


def _rule_row(request: Any) -> Dict[str, Any]:
    """What a preempt rule reads of a request or a holder: its fields."""
    return {
        "rank": request.priority,
        "time": request.time,
        "preempt": request.preempt,
        "key": request.key,
        "since": request.usage_since,
        "attr": request.attributes,
    }


def _queue_rule(rule: Mapping[str, Any], what: str) -> Tuple[Expression, str]:
    """Checks a `queue`, `get_queue` or `put_queue` rank: `{rank, ties}`.

    Args:
        rule: The mapping; `rank` is an expression and `ties` is `fifo` (the default)
            or `lifo`.
        what: The place, for the messages.

    Returns:
        Tuple[Expression, str]: The parsed rank and the ties order.

    Raises:
        ValueError: If the mapping is malformed, `ties` is not `fifo` or `lifo`, or the
            rank is not an expression or reads what a request does not have.
    """
    if (
        not isinstance(rule, Mapping)
        or set(rule) - {"rank", "ties"}
        or not isinstance(rule.get("rank"), str)
    ):
        raise ValueError(
            f"{what} is a mapping with a rank expression and, optionally, ties "
            f"('fifo' or 'lifo'), got {rule!r}"
        )
    ties = rule.get("ties", "fifo")
    if ties not in ("fifo", "lifo"):
        raise ValueError(f"{what} ties is 'fifo' or 'lifo', got {ties!r}")
    try:
        expression = Expression(rule["rank"])
    except ValueError as error:
        raise ValueError(f"{what} rank: {error}") from None
    problem = rank_problem(expression)
    if problem:
        raise ValueError(f"{what} rank: {problem}")
    return expression, ties


def _preempt_rule(
    rule: Mapping[str, Any], what: str
) -> Tuple[Optional[Expression], Optional[Expression]]:
    """Checks a `preempt` rule: `{when, victim}`.

    Args:
        rule: The mapping; each of `when` and `victim` is an expression.
        what: The place, for the messages.

    Returns:
        Tuple[Optional[Expression], Optional[Expression]]: The parsed `when` and
            `victim`, None where the rule leaves one out.

    Raises:
        ValueError: If the mapping is malformed, or an expression is not valid or
            reads the entity, a payload or any other name a rule has no value for.
    """
    if not isinstance(rule, Mapping) or set(rule) - {"when", "victim"}:
        raise ValueError(
            f"{what} is true, false, or a mapping with when and victim "
            f"expressions, got {rule!r}"
        )
    parsed: List[Optional[Expression]] = []
    for key in ("when", "victim"):
        text = rule.get(key)
        if text is None:
            parsed.append(None)
            continue
        if not isinstance(text, str):
            raise ValueError(f"{what} {key} is an expression, got {text!r}")
        try:
            expression = Expression(text, rule=True)
        except ValueError as error:
            raise ValueError(f"{what} {key}: {error}") from None
        outside = expression.outside_run()
        if outside:
            raise ValueError(
                f"{what} {key}: '{text}' reads {outside}, which a preempt rule "
                f"cannot: it reads request., holder., t and the run"
            )
        parsed.append(expression)
    return parsed[0], parsed[1]


def _builder_value(spec: Any, key: str) -> Any:
    """The value of one place of `TASK_VALUE_PLACES` in a task made by the Builder.
    A Builder task has no payload generator: its body is a function."""
    if key == "payload":
        return None
    if key == "set":
        return spec.attribute_changes
    if key == "put.item":
        return spec.put.item if spec.put is not None else None
    if key == "listen":
        return None if spec.listen is None else spec.listen.on
    if key.startswith("batch."):
        return None if spec.batch is None else getattr(spec.batch, key[6:])
    if key in ("wait", "open"):
        node = getattr(spec, key)
        if node is None:
            return None
        root = node.root if key == "wait" else node.wait.root
        return [leaf.args for leaf in _walk_nodes(root) if leaf.args]
    return getattr(spec, key)


def _walk_nodes(node: Any) -> Iterator[Any]:
    """A wait node and every node below it."""
    yield node
    for child in node.children:
        yield from _walk_nodes(child)


def _generator_expressions(generator: Any) -> List[str]:
    """The texts of the expressions one generator holds: its `omit_when`, the `expr` of
    an `expr` generator and the condition of a `when`."""
    texts: List[str] = []
    if generator.omit_when is not None:
        texts.append(generator.omit_when)
    if isinstance(generator, Expr):
        texts.append(generator.expr)
    elif isinstance(generator, When):
        texts.append(generator.condition)
    return texts


def _holding(spec: TaskSpec) -> Holding:
    """What a task does with the units a flow holds, for the order check: the resource
    it seizes, the resource a `release` names, and the tasks that can follow it."""
    release = spec.release
    return Holding(
        None if spec.seize is None else spec.seize.resource,
        release.resource if release is not None and release.task is None else None,
        {
            *(spec.router.tasks or ()),
            *(
                target
                for target in (
                    spec.on_fail,
                    spec.on_renege,
                    spec.on_balk,
                    spec.on_interrupt_next,
                )
                if target is not None
            ),
        },
    )


def _family_fields(
    kind: str,
    name: str,
    family: bool,
    members: Any,
    **fields: Any,
) -> Tuple[Tuple[Any, ...], Dict[str, str]]:
    """Checks the `family` and `members` of an entry and takes out the capacities
    written as text, which only a family accepts.

    Args:
        kind: `resource`, `store`, `container` or `signal`.
        name: The entry's name.
        family: The entry's `family`.
        members: The entry's `members`.
        **fields: The capacities `current_cap`, `max_cap` and `level` as given.

    Returns:
        Tuple[Tuple[Any, ...], Dict[str, str]]: The values in the order given, with a
            text replaced by None, and the texts by field.

    Raises:
        ValueError: If `members` is given without `family`, or is not a whole number,
            text or generator.
    """
    where = f"{_FAMILY_SECTIONS[kind]}.{name}"
    if not isinstance(family, bool):
        raise ValueError(f"{where}.family is true or false, got {family!r}")
    if members is not None:
        if not family:
            raise ValueError(f"{where}.members needs family: true")
        from desflow.values.models import _Gen

        if isinstance(members, bool) or not (
            isinstance(members, (numbers.Integral, str, _Gen)) or callable(members)
        ):
            raise ValueError(
                f"{where}.members is a whole number, an expression or a generator, "
                f"got {members!r}"
            )
        if isinstance(members, numbers.Integral) and members < 0:
            raise ValueError(f"{where}.members is 0 or more, got {members!r}")
    texts: Dict[str, str] = {}
    values: List[Any] = []
    for field, value in fields.items():
        if isinstance(value, str) and family:
            texts[field] = value
            value = None
        values.append(value)
    return tuple(values), texts


def _whole_or_unlimited(value: Any) -> bool:
    """Whether a store capacity is a whole number or positive infinity."""
    if isinstance(value, (str, bytes)):
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return number == math.inf or (math.isfinite(number) and number.is_integer())


def _segment_plan(run: Any) -> Any:
    """A blueprint's `run.segments` as plain data: a list of mappings with the keys
    that are set, or `{"every": seconds}`; None when it has none."""
    segments = run.segments
    if segments is None:
        return None
    if hasattr(segments, "every"):
        return {"every": segments.every}
    return [segment.model_dump(exclude_none=True) for segment in segments]


def _segments_arg(segments: Any) -> Any:
    """`segments` as `run()` takes it, as plain data like `_segment_plan` gives: a list
    of mappings with the keys that are set, or `{"every": seconds}`.

    Raises:
        ValueError: If it is not a list of segment mappings or an `every` mapping, or
            an entry takes a key a segment does not.
    """
    from desflow.blueprint.models import RunSegment, SegmentEvery

    if isinstance(segments, Mapping):
        try:
            return {"every": SegmentEvery.model_validate(dict(segments)).every}
        except ValueError as error:
            raise ValueError(f"segments: {error}") from None
    if isinstance(segments, (str, bytes)) or not isinstance(segments, (list, tuple)):
        raise ValueError(
            "segments is a list of segment mappings or {'every': seconds}, "
            f"got {segments!r}"
        )
    plan = []
    for index, entry in enumerate(segments):
        try:
            plan.append(RunSegment.model_validate(entry).model_dump(exclude_none=True))
        except ValueError as error:  # a pydantic error is a ValueError
            raise ValueError(f"segments[{index}]: {error}") from None
    return plan


def _flag_arg(name: str, value: Any, default: bool) -> bool:
    """A `run()` switch: `value` when it is given, else `default`."""
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ValueError(f"{name} is True or False, got {value!r}")
    return value


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
        target = name_of(target, "a stop signal")
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


class _Counter:
    """The next number of an arrival's entities, which a dropped arrival gives back."""

    __slots__ = ("last",)

    def __init__(self) -> None:
        self.last = 0

    def take(self) -> int:
        """The next number."""
        self.last += 1
        return self.last

    def give_back(self) -> None:
        """Frees the number taken last."""
        self.last -= 1


def _spawn_loop(arrival: str, task: str) -> Callable:
    """The arrival loop of an arrival with `spawn`: each entity starts `task`.

    The instance id comes from the task's one counter on the context, which `next`
    and every other arrival that spawns the task share.
    """

    def loop(context: "SimulationContext"):
        plan = context._arrival_plans.get(arrival)
        if plan is None:
            while True:
                entity = yield context.wait_for_arrival(arrival)
                if not context._arrival_on.get(arrival, True):
                    context._drop_arrival(arrival, entity)
                    continue
                context._start(task, entity, None)
        elif plan.sourceless:
            return
        elif plan.has_entities:
            yield from start_entities(context, arrival, task, plan)
        else:
            yield from _source(context, arrival, task, plan)

    return loop


def _defuse(event: Any) -> None:
    """Marks a failed child flow as handled, so a failure nobody waits for ends that
    flow and not the run, as the builder and YAML treat every task failure."""
    if not event.ok:
        event.defused = True


def _source_plan(
    spawn: Optional[str],
    group: Any = 1,
    active: bool = True,
    off_mode: str = "drop",
    initial: int = 0,
) -> Optional[ArrivalPlan]:
    """The plan a Builder arrival has for `group`, `active`, `off_mode` and `initial`,
    or None when it keeps all four defaults.

    Raises:
        ValueError: If one is malformed, or one is set with no `spawn`.
    """
    if isinstance(group, str):
        group = Expression(group)
    elif isinstance(group, bool) or not (
        isinstance(group, (int, _Gen)) or callable(group)
    ):
        raise ValueError(
            f"group is a whole number, an expression or a generator, got {group!r}"
        )
    if not isinstance(active, bool):
        raise ValueError(f"active is true or false, got {active!r}")
    if off_mode not in ("drop", "restart"):
        raise ValueError(f"off_mode is 'drop' or 'restart', got {off_mode!r}")
    if isinstance(initial, bool) or not isinstance(initial, int) or initial < 0:
        raise ValueError(f"initial is a whole number of 0 or more, got {initial!r}")
    if group == 1 and active and off_mode == "drop" and not initial:
        return None
    if spawn is None:
        raise ValueError("group, active, off_mode and initial need a spawn")
    return ArrivalPlan(group=group, active=active, off_mode=off_mode, initial=initial)


def _parse_spawn(spec: Mapping[str, Any]) -> Dict[str, Any]:
    """Checks a `spawn:` key or spawn action and returns `{task, after, count}`.

    `count` is how many entities one spawn starts: a whole number, or a function of
    the spawning entity. It defaults to 1.
    """
    if not isinstance(spec, Mapping):
        raise ValueError(f"a spawn is a mapping with a task, got {spec!r}")
    extra = set(spec) - {"task", "after", "count", "copy_attributes", "args"}
    if extra:
        raise ValueError(
            f"a spawn takes task, after, count, copy_attributes and args, not "
            f"{', '.join(sorted(extra))}"
        )
    args = spec.get("args")
    if args is not None and not isinstance(args, Mapping):
        raise ValueError(f"spawn args is a mapping of names to values, got {args!r}")
    count = spec.get("count", 1)
    if isinstance(count, str):
        count = Expression(count)
    elif not callable(count) and not isinstance(count, _Gen):
        if isinstance(count, bool) or not isinstance(count, int):
            raise ValueError(
                f"a spawn count is a whole number, an expression, a generator or a "
                f"function of the entity, got {count!r}"
            )
    copy_attributes = spec.get("copy_attributes", False)
    if not isinstance(copy_attributes, bool):
        raise ValueError(f"copy_attributes is true or false, got {copy_attributes!r}")
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
    return {
        "task": task,
        "after": after,
        "count": count,
        "copy_attributes": copy_attributes,
        "args": None if args is None else dict(args),
    }


def _arrival_list(value: Union[str, List[str], None]) -> List[str]:
    """The arrival names of an `activate` or `deactivate`, as a list."""
    if value is None:
        return []
    names = [value] if isinstance(value, str) else list(value)
    if not all(isinstance(name, str) for name in names):
        raise ValueError(f"activate and deactivate name arrivals, got {value!r}")
    return names


_ON_ACTIONS = (
    "fire",
    "spawn",
    "interrupt",
    "resume",
    "activate",
    "deactivate",
    "path",
)


def _parse_on_action(entry: Mapping[str, Any]) -> Dict[str, Any]:
    """Checks one `on:` action and returns it normalized: `{fire: {signal, value,
    from, urgent, after, priority, fail, defused}}`, `{spawn: {task, after}}`,
    `{interrupt: {...}}` or `{path, value}`, with `when`, the parsed expression, when
    the entry has one."""
    found = _parse_action(entry)
    when = entry.get("when")
    if when is not None:
        if not isinstance(when, str):
            raise ValueError(f"an on: entry's when is an expression, got {when!r}")
        found["when"] = Expression(when)
    return found


def _parse_action(entry: Mapping[str, Any]) -> Dict[str, Any]:
    """Checks the action of one `on:` entry and returns it normalized, without its
    `when`."""
    if not isinstance(entry, Mapping):
        raise ValueError(f"an on: action is a mapping, got {entry!r}")
    present = [k for k in _ON_ACTIONS if entry.get(k) is not None]
    if len(present) != 1:
        raise ValueError(
            "an on: action takes exactly one of fire, spawn, interrupt, resume, "
            "activate, deactivate or path"
        )
    unknown = set(entry) - {*_ON_ACTIONS, "value", "when"}
    if unknown:
        raise ValueError(
            f"an on: action takes {', '.join(_ON_ACTIONS)}, value and when, not "
            f"{', '.join(sorted(map(str, unknown)))}"
        )
    kind = present[0]
    if kind == "fire":
        fire = dict(entry["fire"])
        try:
            signal = name_of(fire.get("signal"), "a fire action's signal")
        except ValueError as error:
            raise ValueError(str(error)) from None
        value, source = fire.get("value"), fire.get("from")
        if value is not None and source is not None:
            raise ValueError("a fire action takes value or from, not both")
        urgent, after = fire.get("urgent", False), fire.get("after")
        if not isinstance(urgent, bool):
            raise ValueError(f"a fire urgent is true or false, got {urgent!r}")
        priority = event_priority(fire.get("priority"), urgent)
        fail, defused = fire.get("fail", False), fire.get("defused", False)
        for word, flag in (("fail", fail), ("defused", defused)):
            if not isinstance(flag, bool):
                raise ValueError(f"a fire {word} is true or false, got {flag!r}")
        if fail and source is not None:
            raise ValueError(
                "fail: true fails the signal with the value as the reason, and from "
                "copies an outcome, so a fire action takes one of them"
            )
        if defused and not fail:
            raise ValueError("defused is for a failure, so it needs fail: true")
        if isinstance(after, str):
            after = time_to_seconds(after)
        if isinstance(after, bool):
            raise ValueError(f"a fire after is seconds or a duration, got {after!r}")
        if after is not None and not after > 0:
            raise ValueError(f"a fire after is more than 0, got {after:g}")
        extra = set(fire) - {
            "signal",
            "value",
            "from",
            "urgent",
            "after",
            "priority",
            "fail",
            "defused",
        }
        if extra:
            raise ValueError(
                f"a fire action takes signal, value, from, urgent, after, priority, "
                f"fail and defused, not {', '.join(sorted(extra))}"
            )
        return {
            "fire": {
                "signal": signal,
                "value": value,
                "from": source,
                "urgent": urgent,
                "after": after,
                "priority": priority,
                "fail": fail,
                "defused": defused,
            }
        }
    if kind == "spawn":
        return {"spawn": _parse_spawn(entry["spawn"])}
    if kind in ("activate", "deactivate"):
        return {kind: _arrival_list(entry[kind])}
    if kind == "interrupt":
        action = dict(entry["interrupt"])
        if "task" not in action or set(action) - {"task", "where", "cause"}:
            raise ValueError(
                "an interrupt action takes a task, and an optional where and cause"
            )
        if action.get("where") is not None:
            Expression(action["where"])
        return {"interrupt": action}
    if kind == "resume":
        return {"resume": _parse_resume(entry["resume"])}
    return {"path": entry["path"], "value": entry.get("value")}


def _parse_listen(spec: Mapping[str, Any]) -> Listen:
    """Checks a `listen` key and returns it, with its `on` entries normalized."""
    if not isinstance(spec, Mapping):
        raise ValueError(
            f"listen is {{signal: ..., on: [...]}} or {{handle: ..., on: [...]}}, "
            f"got {spec!r}"
        )
    entries = spec.get("on") or []
    if not isinstance(entries, (list, tuple)):
        raise ValueError(f"listen on is a list of on: entries, got {entries!r}")
    return Listen(spec, [_parse_on_action(entry) for entry in entries])


def _parse_batch(spec: Mapping[str, Any]) -> Batch:
    """Checks a `batch` key and returns it.

    Args:
        spec: A mapping of `size` and optionally `name`, `timeout` and `close_when`.

    Returns:
        Batch: The batch.

    Raises:
        ValueError: If it is not such a mapping.
    """
    if not isinstance(spec, Mapping) or "size" not in spec:
        raise ValueError(
            f"a batch is a mapping with a size, and optionally a name, timeout and "
            f"close_when, got {spec!r}"
        )
    extra = set(spec) - {"name", "size", "timeout", "close_when"}
    if extra:
        raise ValueError(
            f"a batch takes name, size, timeout and close_when, not "
            f"{', '.join(sorted(map(str, extra)))}"
        )
    timeout = spec.get("timeout")
    if isinstance(timeout, str):
        try:
            timeout = time_to_seconds(timeout)
        except ValueError:
            pass  # not a duration, so an expression
    return Batch(spec["size"], spec.get("name"), timeout, spec.get("close_when"))


def _parse_resume(spec: Mapping[str, Any]) -> Dict[str, Any]:
    """Checks a `resume` action and returns it with `where` and a text `limit` parsed:
    `{task, where, value, limit}`."""
    if (
        not isinstance(spec, Mapping)
        or not isinstance(spec.get("task"), str)
        or set(spec) - {"task", "where", "value", "limit"}
    ):
        raise ValueError(
            "a resume action takes a task, and an optional where, value and limit, "
            f"got {spec!r}"
        )
    where, limit = spec.get("where"), spec.get("limit")
    if where is not None and not isinstance(where, (str, Expression)):
        raise ValueError(f"a resume where is an expression, got {where!r}")
    if isinstance(limit, str):
        limit = Expression(limit)
    elif isinstance(limit, bool) or not (
        limit is None or isinstance(limit, (numbers.Integral, _Gen, Expression))
    ):
        raise ValueError(
            f"a resume limit is a whole number, an expression or a generator, "
            f"got {limit!r}"
        )
    return {
        "task": spec["task"],
        "where": Expression(where) if isinstance(where, str) else where,
        "value": spec.get("value"),
        "limit": limit,
    }


def _seconds(value: Union[int, float, str]) -> Union[int, float]:
    """Reads seconds, or a duration such as `"10 min"`. A number is returned as given,
    so an integer stays an integer."""
    return time_to_seconds(value) if isinstance(value, str) else value


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
