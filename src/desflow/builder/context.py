import collections
import dataclasses
import functools
import inspect
import logging
import math
import numbers
import secrets
import shlex
import weakref
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

import simpy
from pydantic import ValidationError
from simpy.core import StopSimulation

from desflow.config import (
    DEFAULT_QUEUE_ORDER,
    INTERRUPT_POLICIES,
    DEFAULT_STATS_INTERVAL,
    LABELS,
    TRACE,
    STATS_LEVELS,
    UNHANDLED_FAILURE_POLICIES,
)
from desflow.core.environment import (
    DynamicRealtimeEnvironment,
    end_is_set,
    resolve_zone,
    schedule_urgent,
)
from desflow.core.expressions import (
    ANY_NAME,
    Expression,
    Scope,
    boolean_word,
    rank_problem,
)
from desflow.builder.arrivals import (
    Arrivals,
    defuse_stopped,
    distribution,
    distribution_of,
)
from desflow.builder.flow import (
    NextSpec,
    Listen,
    Open,
    Release,
    Router,
    Seize,
    Transfer,
    LiveScope,
    Wait,
    check_batch_size,
)
from desflow.core.families import (
    Families,
    SECTIONS as _FAMILY_SECTIONS,
    Reference,
    name_of,
)
from desflow.builder.signals import Signal, Signals, event_priority
from desflow.builder.task import Batch, TaskRun, TaskSpec
from desflow.builder.task_checks import (
    family_read_problem,
)
from desflow.core.registry import ParamChange, SimulationRegistry
from desflow.core.rules import check_capacity, checked_datetime
from desflow.builder.holders import (
    _run_copy,
    _within_max,
    Holders,
    check_level,
    check_queue_limit,
)
from desflow.core.sinks import Sink, Sinks
from desflow.builder.model_checks import ModelChecks, not_added
from desflow.builder.runs import EndSets, Parts, RunState
from desflow.builder.tasks import Tasks, task_generator
from desflow.core.sampler import Sampler
from desflow.models.flow import Entity, Upstream
from desflow.core.replay import read_trace
from desflow.builder.scenario import check_step, run_scenario
from desflow.values.models import (
    Gen,
    values_adapter,
)
from desflow.values.runtime import DEFAULT_HISTORY, RunStopped, ValueRuntime
from desflow.values.check import (
    generators_in,
    load_rows,
)
from desflow.values.models import Row as RowGenerator
from desflow.values.template import bind as bind_values, looked_up
from desflow.models.params import (
    BreakdownConfig,
    check_breakdown_advances,
    CapacityConfig,
    ContainerConfig,
    DistributionConfig,
    SimParameter,
)
from desflow.resources.base import check_ties
from desflow.resources.container import (
    ContainerBase,
    DynamicContainer,
    check_container_type,
)
from desflow.resources.resource import (
    ResourceBase,
    check_resource_queue,
    check_resource_type,
)
from desflow.resources.store import DynamicStore, StoreBase, check_store_type
from desflow.utils import (
    _seconds,
    checked_count,
    checked_factor,
    checked_interval,
    checked_priority,
    checked_seconds,
    checked_whole,
    suggestion,
    time_to_seconds,
)

if TYPE_CHECKING:
    # build.py imports this module, so the import is for type checkers only
    from desflow.blueprint.build import Preview
    from desflow.explain.models import Explanation

logger = logging.getLogger(__name__)


@dataclass
class RunDefaults:
    """What a model's runs take when `run()` or `start()` does not say.

    `from_yaml` sets it from the blueprint's `run` section. `load_context` sets
    `source`, and `desflow run` adds its options to `repeat_hint`.

    Attributes:
        until: The end of a run, in simulation seconds.
        stop: The stop condition of a run.
        drain: The drain setting of a run.
        unhandled: What a run does with a failure no task handles.
        segments: The blueprint's `run.segments`, a list of mappings or
            `{every: seconds}`, which `start()` keeps for the run that uses them.
        steps: The blueprint's `run.steps`.
        sync: The blueprint's `run.sync`.
        before_run: The functions each run calls before it starts.
        repeat_hint: How the log tells a run without a seed to repeat itself, with
            `{seed}`.
        sample_limit: The events a preview prints before it ends the run, for
            `desflow run --sample`.
        source: What builds the model again, a blueprint path or a function, so
            `analyze(verify=True)` can run a copy.
    """

    until: Any = None
    stop: Any = None
    drain: Any = None
    unhandled: str = "stop"
    segments: Any = None
    steps: bool = False
    sync: bool = False
    before_run: List[Callable[[], Any]] = dataclasses.field(default_factory=list)
    repeat_hint: Optional[str] = None
    sample_limit: Optional[int] = None
    source: Any = None


@dataclass(frozen=True)
class _RunSettings:
    """The settings of one run as `start()` resolved them, which `_build` reads.

    Attributes:
        until: The end of the run, in simulation seconds, or None.
        stop: The stop condition, or None.
        drain: The drain setting, or None.
        seed: The run's seed, in place of `random_seed`, or None.
        overrides: Registry paths and the values they take before the clock starts.
        stats: The lowest statistics level the run keeps, or None.
        egress: Whether the run has its sinks and the statistics timer.
        ingress: Whether the run has its ingress.
        source: Where each override came from, for its `ParamChange`.
        actor: Who made each override, for its `ParamChange`.
        reason: Why each override was made, for its `ParamChange`.
    """

    until: Any
    stop: Any
    drain: Any
    seed: Optional[int]
    overrides: Optional[Mapping[str, Any]]
    stats: Optional[str]
    egress: bool
    ingress: bool
    source: Optional[str]
    actor: Optional[str]
    reason: Optional[str]


class SimulationContext:
    """
    Builder and Orchestrator for Event-Driven Digital Twins.

    The `SimulationContext` implements the Builder Pattern to construct the
    foundational architecture of a Digital Twin. It acts as the central control
    plane, aggregating discrete machine capacities, continuous physical states
    (wear and tear), and statistical distributions into a unified `SimParameter`
    object.

    Its public methods fall into four groups:

    - **Declaring parts:** `from_yaml`, `add_resource`, `add_container`,
      `add_store`, `add_breakdown`, `add_variable`, `add_service`, `add_arrival`,
      `add_initial_arrival`, `add_arrival_replay`, `add_signal`, `add_scenario`,
      `add_step`, `add_readings`, `add_process`, `add_ingress`, `add_egress`,
      `with_stats`, `with_batching`, `task`, `note_values`, `arrival_loop` and
      `telemetry_loop`.
    - **Checks made before a run:** `family_read_problem`, `compile_parameters`,
      `registry_paths` and `analyze`, which read the declarations;
      `analyze(verify=True)` also builds the model again and runs it. `start()`
      and `analyze()` check the declarations before they build anything, and
      raise `ValueError` for the first that is wrong; `run()` checks them through
      `start()`.
    - **Running:** `run`, `start`, `step`, `peek`, `advance`, `finish`, `close` and
      the `env` property.
    - **Calls a task makes during a run:** `publish`, `spawn`, `subscribe`, `call`,
      `signal`, `interrupt`, `get_resource`, `get_store`, `get_container`,
      `next_instance`, `wait_for_arrival` and `stop_arrivals`.

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
            ValueError: If `sim_id` is not text or is empty, `warmup` or
                `lag_interval` is negative or not a duration, `timezone` is not an
                IANA time zone name, or `environment` is not a subclass of
                `DynamicRealtimeEnvironment`.
        """
        if environment is not None and not (
            isinstance(environment, type)
            and issubclass(environment, DynamicRealtimeEnvironment)
        ):
            raise ValueError(
                "environment must be a subclass of DynamicRealtimeEnvironment, "
                f"got {environment!r}"
            )
        if not isinstance(sim_id, str) or not sim_id:
            raise ValueError(f"sim_id is text that is not empty, got {sim_id!r}")
        self.sim_id = sim_id
        self.factor = checked_factor(factor)
        self.strict = strict
        self.environment: Type[DynamicRealtimeEnvironment] = (
            environment or DynamicRealtimeEnvironment
        )
        self.random_seed = (
            None
            if random_seed is None
            else checked_whole(random_seed, "random_seed", 0)
        )
        self.logical_start_time = checked_datetime(
            logical_start_time, "logical_start_time"
        )
        self.go_live_at = checked_datetime(go_live_at, "go_live_at")
        self.initial_time = checked_seconds(
            initial_time, "initial_time", ends=True, below_zero=True
        )
        self.warmup = None if warmup is None else checked_seconds(warmup, "warmup")
        resolve_zone(timezone)  # an unknown name is refused here, not at run()
        self.timezone = timezone
        self.lag_interval = (
            None
            if lag_interval is None
            else checked_seconds(lag_interval, "lag_interval")
        )

        # The sinks piece: the egress and ingress providers, batching and statistics
        self._sinks = Sinks()
        # The holders piece: the resources, stores, containers and breakdowns
        self._holders = Holders()
        # The readings of `add_readings`, one mapping of name to expression for each call
        self._readings: List[Dict[str, Expression]] = []
        # The scenario's steps, which `_check_tasks`, the build and `explain` read
        self._scenario_steps: List[Dict[str, Any]] = []
        # Whether the process that applies the steps was added, once for all calls
        self._scenario_added = False
        # The startup loop each arrival with a spawn has, so a second call replaces it
        self._arrival_loops: Dict[str, Tuple[Callable, Optional[str]]] = {}
        # The services' distributions: a declaration that no piece owns, read only by
        # `compile_parameters` and the checks
        self._services_config: Dict[str, DistributionConfig] = {}
        # The loops and processes the run starts, in the order they were added
        self._startup_loops: List[Tuple[Callable, Optional[str]]] = []
        # The arrivals piece
        self._arrivals = Arrivals()
        # The variables: a declaration that no piece owns, read only by
        # `compile_parameters` and the checks
        self._variables_config: Dict[str, Any] = {}
        # The tasks: their specs and what the values of the tasks read from other
        # records; and the function `task` returns for each, by name
        self._tasks = Tasks()
        self._task_functions: Dict[str, Callable] = {}
        # The signals piece
        self._signals = Signals()
        # The declared families, by name, whose entries in the sections above are the
        # template each member is made from
        self._families = Families()
        # The checks of the declarations above, made before each run
        self._checks = ModelChecks(
            self._tasks,
            self._families,
            self._holders,
            self._signals,
            self._arrivals,
            self._readings,
            self._scenario_steps,
            self._services_config,
        )
        # Which tasks' instances need an end event, set by _check_tasks
        self._ends = EndSets()
        # The acquisition-order warnings logged, so each is logged once
        self._order_warned: set = set()
        # The state of the current run, which each start() makes new; this one is
        # read before the first run
        self._run = RunState()
        # The instance a task function's generator runs, until `spawn` starts it.
        # It is kept across runs, so `spawn` still finds a generator that a task
        # function made before the current run started
        self._runs_of: "weakref.WeakKeyDictionary[Any, TaskRun]" = (
            weakref.WeakKeyDictionary()
        )
        # The environment of the current run, None before the first start()
        self._env: Optional[DynamicRealtimeEnvironment] = None
        # Called with a `ParamChange` for every accepted registry change of a run;
        # start() copies them into that run's registry
        self.on_change: List[Callable[[ParamChange], None]] = []
        # True once start() has built and started everything, so a caller can tell
        # a value refused before the run from an error inside it
        self.started = False
        # The random draws of the current run, made by each start()
        self.sampler: Optional[Sampler] = None
        # The generators' state for the run, made when the run is built; nothing
        # generates a value before that
        self.values: ValueRuntime = None  # type: ignore[assignment]
        # The functions add_process started, which explain shows as opaque nodes
        self._processes: List[Callable] = []
        # What a run takes when its caller does not say, and what builds the model
        # again; from_yaml and load_context set it
        self._defaults = RunDefaults()
        # What every scope of the run reads, and the parts the run's tasks are given,
        # made by each run's build
        self._scope: Optional[LiveScope] = None
        self._parts: Optional[Parts] = None
        # Whether the current run is closed. start() reads it before it replaces the
        # run state, to close an open run first
        self._closed = True

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
        from desflow.blueprint.build import build, run_as_sample

        context, run = build(path, preview, variables)
        context._defaults.until = run.until
        context._defaults.stop = run.stop
        context._defaults.drain = run.drain_setting()
        context._defaults.unhandled = run.on_unhandled_failure
        context._defaults.segments = _segment_plan(run)
        context._defaults.steps = run.steps
        context._defaults.sync = run.sync
        if preview is None:
            context._defaults.before_run = list(run.before)
        elif run.before:
            logger.warning("run.before skipped: preview runs use no real sinks")
        if preview is not None:
            run_as_sample(context, preview)
        # Quoted as the shell reads it; the seed is filled in later, so braces in
        # the path are doubled
        shown = shlex.quote(str(path)).replace("{", "{{").replace("}", "}}")
        context._defaults.repeat_hint = (
            f"Repeat it with: desflow run {shown} --seed {{seed}}"
        )
        # A copy that `analyze(verify=True)` builds reads the same variables
        context._defaults.source = (
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
        self._sinks.ingress.append(provider)
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
                each event's `value` without the record around it. A Kafka sink
                sends a null value as a tombstone, a message with the record's key
                and a null value, and sends any other value that is not a mapping
                as JSON. A Redis sink sends a value that is not a mapping as JSON,
                null included. A storage, Iceberg or PostgreSQL sink skips it, with
                one warning. Ground-truth records are not affected. Defaults to
                True. With the envelope on, a null value stays in the record, and a
                Kafka serializer whose schema has no null for `value` refuses it:
                the record is skipped with a warning.

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
        if batch_size is not None:
            batch_size = checked_count(batch_size, "egress batch_size")
        if flush_interval is not None:
            flush_interval = checked_interval(flush_interval, "egress flush_interval")
        self._sinks.egress.append(
            Sink(
                provider,
                predicate=None if isinstance(when, str) else when,
                batch_size=batch_size,
                flush_interval=flush_interval,
                label=when == LABELS,
                trace=when == TRACE,
                envelope=envelope,
            )
        )
        return self

    def with_stats(
        self,
        level: str = "basic",
        interval: Union[float, str] = DEFAULT_STATS_INTERVAL,
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
            interval: Simulation seconds, or a duration such as `"10 s"`, between
                publications; they are also
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
        seconds = checked_seconds(interval, "stats interval")
        if seconds <= 0:
            raise ValueError(f"stats interval must be greater than 0, got {interval}")
        if on_change and level == "none":
            raise ValueError(
                "on_change publishes the statistics at each request and release, so "
                "it needs a level above none"
            )
        self._sinks.stats_level = level
        self._sinks.stats_interval = seconds
        self._sinks.stats_on_change = bool(on_change)
        self._sinks.stats_trace = bool(trace)
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
        self._sinks.batch_size = checked_count(batch_size, "batching batch_size")
        self._sinks.flush_interval = checked_interval(
            flush_interval, "batching flush_interval"
        )
        self._sinks.max_queued_batches = checked_count(
            max_queued_batches, "batching max_queued_batches"
        )
        self._sinks.drain_stall_seconds = checked_interval(
            drain_stall_seconds, "batching drain_stall_seconds"
        )
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
        self._families.check_name("resource", name)
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
        check_resource_queue(queue, preempt)
        check_resource_type(type, queue_limit)
        for field, value in (("current_cap", current_cap), ("max_cap", max_cap)):
            check_capacity("resource", name, field, value)
        check_queue_limit("resource", name, queue_limit)
        if current_cap is None and max_cap is not None and 0 <= max_cap < 1:
            raise ValueError(
                f"resource '{name}' has max_cap {max_cap} and no current_cap, whose "
                f"default is 1; set current_cap to {max_cap} or less"
            )
        self._holders.declare_resource(
            name, current_cap, max_cap, queue, preempt, queue_limit, rank, rule, type
        )
        self._families.declare("resource", name, family, members, texts)
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
        self._families.check_name("container", name)
        if name in self._holders.stores_config:
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
        for field, value in (("current_cap", cap), ("max_cap", top)):
            check_capacity("container", name, field, value, whole=False)
        check_level(name, start, cap if cap is not None else top)
        config = ContainerConfig(
            current_cap=_within_max("container", name, cap, top),
            max_cap=top,
            level=start,
        )
        self._set_holder_ranks("container", name, get_queue, put_queue, type)
        self._holders.declare_container(name, config, type)
        self._families.declare("container", name, family, members, texts, start=start)
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
        self._families.check_name("store", name)
        if not isinstance(priority, bool):
            raise ValueError(
                f"store '{name}': priority is true or false, got {priority!r}"
            )
        if name in self._holders.containers_config:
            raise ValueError(f"'{name}' is already a container")
        (current_cap, max_cap), texts = _family_fields(
            "store", name, family, members, current_cap=current_cap, max_cap=max_cap
        )
        check_store_type(type, priority)
        for field, value in (("current_cap", current_cap), ("max_cap", max_cap)):
            check_capacity("store", name, field, value)
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
            or isinstance(item, Gen)
            for item in start
        ):
            raise ValueError(
                f"priority store '{name}' starts with (priority, item) pairs; give "
                f"each item as a pair, such as (1, item)"
            )
        if priority:
            for item in start:
                if not isinstance(item, Gen):
                    checked_priority(item[0], f"store '{name}' starting item priority")
        self._bind_values(start, ("stores", name, "items"), overwrite=False)
        config = CapacityConfig(
            current_cap=_within_max("store", name, current_cap, max_cap),
            max_cap=max_cap,
        )
        self._set_holder_ranks("store", name, get_queue, put_queue, type)
        self._holders.declare_store(name, config, start, priority, type)
        self._families.declare("store", name, family, members, texts)
        return self

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
                self._holders.holder_ranks[(name, direction)] = found[direction]
            else:
                self._holders.holder_ranks.pop((name, direction), None)

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
                refused when the run starts, a path on a member of a family too when
                the family can never have it: no task names the family with a `key`,
                and the key is not one of 0 to `members` - 1 (none, when the family
                has no `members`).
        """
        checked = [check_step(step) for step in steps]
        for index, step in enumerate(checked, len(self._scenario_steps)):
            self._bind_values(step, ("scenario", index), overwrite=False)
        if not self._scenario_added:
            self._scenario_added = True
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
            until=self._run.until,
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
                `{"dist": "weibull", "shape": 1.5, "scale": "1 hour"}`, whose
                time-valued parameters take seconds or a duration, as for
                `add_service`.
            repair: A distribution, such as `{"dist": "exponential", "rate": 0.01}`,
                with the same durations.
            work: `interrupt` (the default) interrupts the tasks being served, which
                follow their `on_interrupt`; `finish` lets them finish.
            clock: `calendar` (the default) or `busy`, the time the time to failure
                counts.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If a distribution, `work` or `clock` is invalid, or the time
                to failure and the repair both always give 0.
        """
        config = BreakdownConfig(
            time_to_failure=distribution_of(time_to_failure),
            repair=distribution_of(repair),
            work=work,
            clock=clock,
        )
        check_breakdown_advances(
            config.time_to_failure, config.repair, f"resource '{resource}' breakdown"
        )
        self._holders.breakdowns_config[resource] = config
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
        mean: Optional[Union[float, str]] = None,
        std: Optional[Union[float, str]] = None,
        low: Optional[Union[float, str]] = None,
        value: Optional[Union[float, str]] = None,
        high: Optional[Union[float, str]] = None,
        mode: Optional[Union[float, str]] = None,
        shape: Optional[float] = None,
        scale: Optional[Union[float, str]] = None,
        values: Optional[Sequence[Union[float, str]]] = None,
        weights: Optional[List[float]] = None,
        edges: Optional[Sequence[Union[float, str]]] = None,
        counts: Optional[List[float]] = None,
        p: Optional[float] = None,
    ) -> "SimulationContext":
        """
        Registers a statistical distribution defining a process service duration.

        The parameters are keyword-only, so their order never changes the meaning.
        Give only those `dist` takes; `desflow.models.distributions.DISTRIBUTIONS`
        lists each family's parameters and rules. A time-valued parameter (`mean`,
        `std`, `low`, `mode`, `high`, `scale`, `value`, and the `values` and `edges`
        of an empirical) takes seconds or a duration such as `"4 min"`, as a
        blueprint's does.

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
        self._services_config[name] = distribution(dist, locals())
        return self

    def add_arrival(
        self,
        name: str,
        dist: Optional[str] = None,
        *,
        rate: Optional[float] = None,
        mean: Optional[Union[float, str]] = None,
        std: Optional[Union[float, str]] = None,
        low: Optional[Union[float, str]] = None,
        value: Optional[Union[float, str]] = None,
        high: Optional[Union[float, str]] = None,
        mode: Optional[Union[float, str]] = None,
        shape: Optional[float] = None,
        scale: Optional[Union[float, str]] = None,
        values: Optional[Sequence[Union[float, str]]] = None,
        weights: Optional[List[float]] = None,
        edges: Optional[Sequence[Union[float, str]]] = None,
        counts: Optional[List[float]] = None,
        p: Optional[float] = None,
        gap: Any = None,
        attributes: Optional[Mapping[str, Any]] = None,
        key: Any = None,
        spawn: Optional[str] = None,
        first_at: Union[float, str, None] = None,
        group: Any = 1,
        active: bool = True,
        off_mode: str = "drop",
        initial: int = 0,
        max_events: Any = None,
        vars: Optional[Mapping[str, Any]] = None,
    ) -> "SimulationContext":
        """
        Registers a statistical distribution defining the inter-arrival times of entities.

        Each arrival starts a new entity, which `wait_for_arrival` returns. Its key,
        the record key of every event it causes, is the arrival name and a counter
        (`orders-1`, `orders-2`) unless `key` gives another.

        Args:
            name: The internal identifier (e.g., 'orders').
            dist: The family, as for `add_service`, with the same parameters and the
                same durations. Give `gap` instead of it for a gap that is not a
                distribution.
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
            key: The entity's key. A function of the entity's counter, from 1, returning
                its key; or a value as a blueprint's `key` reads it: text or a number
                is the key itself, and a generator is read once for each entity, after
                `vars` and the attributes. An expression key is
                `generator({"gen": "expr", "expr": "..."})`. `wait_for_arrival` reads
                it too, so it needs no `spawn`.
            spawn: The task each arrival starts, with the arrival's entity and the
                task's next instance id, as YAML's `spawn` key does. None leaves
                the arrival to an `arrival_loop` written by hand.
            first_at: The simulation time of the first arrival, in seconds or as a
                duration such as `"5 s"`, which the first
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
            max_events: With `spawn`, how many times the arrival starts its task, in
                all, as a blueprint's `max_events`: a whole number of 0 or more, an
                expression such as `"int(t / 5)"` or a generator, read when the
                source starts. The arrival stops after the last, and its last start
                draws no further gap.
            vars: Named values read once for each entity, before its attributes and
                key, as a blueprint's arrival `vars`. Each is a constant or a
                generator such as `generator({"gen": "boolean", "p": 0.1})`; a plain
                mapping with `gen` is refused. A name is one an expression can read
                and does not already use. An attribute and the `key` read them with
                `generator({"gen": "var", "name": ...})`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If the family is unknown, or a parameter is missing, out of
                range or not one the family takes, or both or neither of `dist` and
                `gap` are given, or `gap` is a negative number or neither a number,
                an expression nor a generator, or `first_at` is below 0 or is neither
                seconds nor a duration, or `off_mode`, `active`, `group`, `initial`, `max_events` or
                `vars` is malformed, or `group`, `active`, `off_mode`, `initial` or
                `max_events` is given with no `spawn`, or `gap`, `group` or
                `max_events` is an expression that reads the entity, or a `vars`
                name is not one an expression can read. A `spawn` naming no task is
                refused when the run starts.
        """
        self._arrivals.add_arrival(
            self._bind_values,
            name,
            dist,
            rate=rate,
            mean=mean,
            std=std,
            low=low,
            value=value,
            high=high,
            mode=mode,
            shape=shape,
            scale=scale,
            values=values,
            weights=weights,
            edges=edges,
            counts=counts,
            p=p,
            gap=gap,
            attributes=attributes,
            key=key,
            spawn=spawn,
            first_at=first_at,
            group=group,
            active=active,
            off_mode=off_mode,
            initial=initial,
            max_events=max_events,
            vars=vars,
        )
        self._add_arrival_loop(name, spawn)
        return self

    def add_initial_arrival(
        self,
        name: str,
        initial: int,
        *,
        spawn: str,
        attributes: Optional[Mapping[str, Any]] = None,
        key: Any = None,
        gap: Any = None,
        vars: Optional[Mapping[str, Any]] = None,
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
            key: The entity's key, in every form `add_arrival` takes: a function of
                the entity's counter, from 1, or a value as a blueprint's `key`
                reads it.
            gap: With a gap the arrival is not sourceless: it also starts an entity
                after each gap once the first `initial` exist, as
                `add_arrival(name, gap=gap, initial=initial, ...)` does.
            vars: Named values read once for each entity, before its attributes and
                key, as for `add_arrival`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `initial` is not a whole number above 0, or `gap` or
                `vars` is refused as for `add_arrival`.
        """
        self._arrivals.add_initial_arrival(
            self._bind_values,
            name,
            initial,
            spawn=spawn,
            attributes=attributes,
            key=key,
            gap=gap,
            vars=vars,
        )
        self._add_arrival_loop(name, spawn)
        return self

    def _add_arrival_loop(self, arrival: str, task: Optional[str]) -> None:
        """Adds the loop that starts `task` for each entity of an arrival with
        `spawn` to the startup loops, so the arrival loops start in the order the
        arrivals were added."""
        replaced = self._arrival_loops.pop(arrival, None)
        if replaced is not None:
            self._startup_loops.remove(replaced)
        if task is None:
            return

        def loop(context: "SimulationContext"):
            return context._run.arrivals.loop(arrival, task)

        self._arrival_loops[arrival] = (loop, arrival)
        self._startup_loops.append((loop, arrival))

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
        spec_kwargs, generated, router, take, give, resource_id = _task_fields(
            service_id,
            resource_id,
            name=name,
            priority=priority,
            get=get,
            put=put,
            next=next,
            set=set,
            patience=patience,
            balk=balk,
            wait=wait,
            open=open,
            listen=listen,
            cancel=cancel,
            seize=seize,
            release=release,
            signal=signal,
            fail=fail,
            fail_reason=fail_reason,
            on_fail=on_fail,
            on_renege=on_renege,
            on_balk=on_balk,
            service_time=service_time,
            key_field=key_field,
            key=key,
            keep=keep,
            preempt=preempt,
            on_interrupt=on_interrupt,
            on_interrupt_next=on_interrupt_next,
            on_interrupt_run=on_interrupt_run,
            interrupt=interrupt,
            resume=resume,
            batch=batch,
            spawn=spawn,
            subscribe=subscribe,
            on=on,
            vars=vars,
            vars_once=vars_once,
            on_start=on_start,
            finish_vars=finish_vars,
            set_args=set_args,
            activate=activate,
            deactivate=deactivate,
            history=history,
        )

        def decorator(user_func):
            task_name = name or user_func.__name__
            if task_name in self._tasks.specs:
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
            if not self._tasks.rows_deferred:
                self._bind_values(
                    spec.spawn, ("tasks", task_name, "spawn"), overwrite=False
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
            for key, given in (("wait", wait), ("open", open)):
                # Each generator of a wait or an open at the place a blueprint gives
                # it, such as `wait.all.<name>.each`, as the YAML binds them
                self._bind_values(given, ("tasks", task_name, key), overwrite=False)
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
                    entity = self._run.arrivals.new_entity(task_name, None)
                assert self._parts is not None  # the run is built
                run = self._tasks.new_run(
                    self,
                    self._parts,
                    task_name,
                    entity,
                    upstream,
                    args=args,
                    kwargs=kwargs,
                    task_id=task_id,
                )
                generator = task_generator(run)
                self._runs_of[generator] = run
                return generator

            spec.router.where = f"task '{task_name}' next.weights"
            # A failure when an expression is read names the task and the place; a
            # blueprint replaces the name with the file and the line
            for found in spec.expressions:
                if found.place is not None and not found.expression.where:
                    found.expression.where = f"task '{task_name}' " + ".".join(
                        map(str, found.place)
                    )
            # Noted after every check, so a refused task leaves no row target behind
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
            self._task_functions[task_name] = wrapper
            self._tasks.specs[task_name] = spec
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
        if self._tasks.rows_deferred:
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
        self._tasks.row_targets |= rows
        if task is not None:
            if gates:
                self._tasks.gates.setdefault(task, set()).update(gates)
            if previous:
                self._tasks.previous_users.add(task)

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

    def telemetry_loop(self, interval: Union[float, str]) -> Callable:
        """
        Registers an infinite background daemon loop for periodic data publishing.

        This is primarily used for observing and extracting continuous state
        (like wear containers) at strict intervals.

        Args:
            interval: Simulation seconds, or a duration such as `"10 s"`, to wait
                between iterations.

        Raises:
            ValueError: If `interval` is not above 0.
        """
        seconds = checked_seconds(interval, "telemetry_loop interval")
        if seconds <= 0:
            raise ValueError(f"telemetry_loop interval must be above 0, got {interval}")

        def decorator(user_func):
            def wrapper():
                if not self._env:
                    raise RuntimeError("Simulation has not been built yet.")
                while True:
                    user_func(self)
                    yield self._env.timeout(seconds)

            self._startup_loops.append((wrapper, None))
            return user_func

        return decorator

    def add_readings(
        self, interval: Union[float, str], readings: Mapping[str, str]
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
            interval: Simulation seconds, or a duration such as `"10 s"`, between
                two publications.
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
        seconds = checked_seconds(interval, "the interval of readings")
        if seconds <= 0:
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
            problem = reading_problem(name, self._readings)
            if problem is not None:
                raise ValueError(problem)
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

        self.telemetry_loop(seconds)(report)
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
        return self._run.arrivals.wait_for_arrival(arrival_id)

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
        key: Any = None,
        spawn: Optional[str] = None,
        active: bool = True,
        off_mode: str = "drop",
        vars: Optional[Mapping[str, Any]] = None,
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
            key: The entity's key, in every form `add_arrival` takes: a function of
                the entity's counter, from 1, or a value as a blueprint's `key`
                reads it.
            spawn: The task each row starts, as for `add_arrival`.
            active: False starts the source off, as for `add_arrival`.
            off_mode: `"drop"` or `"restart"`, as for `add_arrival`.
            vars: Named values read once for each entity, before its attributes and
                key, as for `add_arrival`.

        Returns:
            SimulationContext: The current instance for method chaining.

        Raises:
            ValueError: If `unit` or `on_invalid` is unknown, `fields` is a string
                other than `all`, or `vars` is refused as for `add_arrival`. A
                `spawn` naming no task is refused when the run starts.
        """
        self._arrivals.add_arrival_replay(
            self._bind_values,
            name,
            source,
            time=time,
            unit=unit,
            fields=fields,
            on_invalid=on_invalid,
            attributes=attributes,
            key=key,
            spawn=spawn,
            active=active,
            off_mode=off_mode,
            vars=vars,
        )
        self._add_arrival_loop(name, spawn)
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
        return self._run.next_instance(task)

    def get_store(self, store_id: str) -> DynamicStore:
        """
        Retrieves an active `DynamicStore`.

        Args:
            store_id: The store's name.

        Returns:
            DynamicStore: The store.

        Raises:
            RuntimeError: If called during the Builder Phase.
            KeyError: If the model has no such holder, or the last start failed
                before it made the run's holders.
        """
        if not self._env:
            raise RuntimeError("Stores are not instantiated until context is run.")
        holders = self._run.holder_run
        if holders is None:
            # The last start failed before it made the run's holders
            raise KeyError(store_id)
        return holders.stores[store_id]

    def get_container(self, container_id: str) -> DynamicContainer:
        """
        Retrieves an active `DynamicContainer`.

        Args:
            container_id: The container's name.

        Returns:
            DynamicContainer: The container.

        Raises:
            RuntimeError: If called during the Builder Phase.
            KeyError: If the model has no such holder, or the last start failed
                before it made the run's holders.
        """
        if not self._env:
            raise RuntimeError("Containers are not instantiated until context is run.")
        holders = self._run.holder_run
        if holders is None:
            # The last start failed before it made the run's holders
            raise KeyError(container_id)
        return holders.containers[container_id]

    def get_resource(self, resource_id: str) -> ResourceBase:
        """Returns the resource a running model built: a `DynamicResource`, or the
        `ResourceBase` subclass its `type` names.

        Args:
            resource_id: The resource's name, as given to `add_resource`.

        Returns:
            ResourceBase: The resource, for raw requests and readings.

        Raises:
            RuntimeError: If called before the run has started.
            KeyError: If the model has no such holder, or the last start failed
                before it made the run's holders.
        """
        if not self._env:
            raise RuntimeError("Resources are not instantiated until context is run.")
        holders = self._run.holder_run
        if holders is None:
            # The last start failed before it made the run's holders
            raise KeyError(resource_id)
        return holders.resources[resource_id]

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
        if task not in self._tasks.specs:
            raise ValueError(
                f"no task is called '{task}'"
                + (suggestion(task, self._tasks.specs) or "; add it with @app.task()")
            )
        if entity is None:
            entity = self._run.arrivals.new_entity(task, None)
        assert self._parts is not None  # the run is built
        return self._tasks.start(
            self,
            self._parts,
            task,
            entity,
            upstream,
            child=True,
            args=args or None,
            kwargs=kwargs,
        )

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
        self._families.check_name("signal", name)
        _family_fields("signal", name, family, members)
        for flag, value in (("once", once), ("defused", defused)):
            if not isinstance(value, bool):
                raise ValueError(
                    f"signal '{name}': {flag} is true or false, got {value!r}"
                )
        self._families.declare("signal", name, family, members, {})
        self._signals.declare(
            name, once, defused, (_parse_on_action(entry) for entry in on or [])
        )
        self._bind_values(
            self._signals.on[name], ("signals", name, "on"), overwrite=False
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
        return self._run.signals.made[name]

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
            where: A condition read for each instance, such as `"attr.order == 3"`,
                in that instance's scope; a branch of its wait reads as not done
                until the wait has ended. None interrupts every instance.
            cause: What the instances receive as the interrupt's cause, published
                as the record's `cause`.

        Returns:
            int: How many instances were interrupted.

        Raises:
            RuntimeError: If called before the run starts.
            ValueError: If no task has that name.
        """
        if not self._env:
            raise RuntimeError(
                "Cannot interrupt tasks before context is run; interrupt them from a "
                "task, a process or a scenario step"
            )
        if task not in self._tasks.specs:
            raise ValueError(
                f"no task is called '{task}'"
                + (suggestion(task, self._tasks.specs) or "; add it with @app.task()")
            )
        condition = Expression(where) if isinstance(where, str) else where
        active = self._env.active_process
        count = 0
        for run in list(self._run.running.get(task, [])):
            process = run.flow.process
            if process is None or process is active or not process.is_alive:
                continue
            if condition is not None and not condition.evaluate(_wait_so_far(run)):
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
            SimParameter: The parameters, sharing their config objects with the
                builder; `run()` registers a copy of them.
        """
        return SimParameter(
            sim_id=self.sim_id,
            resources=self._families.without(self._holders.resources_config),
            containers=self._families.without(self._holders.containers_config),
            stores=self._families.without(self._holders.stores_config),
            breakdowns=self._families.without(self._holders.breakdowns_config),
            service=self._services_config,
            arrival=self._arrivals.configs,
            variables=self._variables_config,
        )

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
        registry = scratch_registry(self)
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
        if self._defaults.source is None:
            raise ValueError(
                "verify needs a model it can build again; use desflow explain "
                "--verify FILE"
            )
        return run_verify(self._defaults.source, explanation, until, seed, replications)

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
            if not self._run.moved:
                self._run_plan()
            if self._run.stopped:
                return None
            if self._run.end is not None:
                if self._run.end.callbacks is None and not self._run.end.ok:
                    # `env.run(until=...)` returns the value of a processed event,
                    # so a failed stop that advance() reached is raised here
                    raise self._run.end.value
                return self._run_env(self._run.end)
            if self._run_ended():
                # advance() or step() already reached until, where a run ends
                return None
            return self._run_env(self._run.until)
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
        self._run.moved = True
        env = self._env
        if self._run_ended():
            raise RuntimeError(
                f"the run ended at {env.now:g}; close() it, or start() a new run"
            )
        if not isinstance(until, simpy.Event):
            until = checked_seconds(until, "until", below_zero=True)
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
        if self._run.end is None:
            try:
                return env.run(
                    until=until
                    if self._run.until is None
                    else min(until, self._run.until)
                )
            except RunStopped:
                return None  # a `row` with on_end: stop ended the run
        segment = schedule_urgent(env, simpy.Event(env), until - env.now)
        return self._advance_to(segment)

    def _run_plan(self) -> None:
        """Runs the blueprint's `run.segments` or `run.steps` before `finish()` runs the
        rest of the run, publishing a reading after each segment or event."""
        if self._run.segments is not None:
            self._run_segments(self._run.segments)
        elif self._run.steps:
            self._run_steps()

    def _segment_entries(self, segments: Any) -> List[Mapping[str, Any]]:
        """The entries of `run.segments`: the list as given, or for `{every: t}` one
        entry ending at each multiple of `t` below `until`, and one at `until`."""
        if not isinstance(segments, Mapping):
            return list(segments)
        every, entries, count = segments["every"], [], 1
        while (
            self._run.until is not None
            and self.initial_time + count * every < self._run.until
        ):
            entries.append({"until": self.initial_time + count * every})
            count += 1
        return [*entries, {"until": self._run.until}]

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
            if entries and self._run.until is None and isinstance(segments, list):
                self._run.stopped = True

    def _run_to_end(self) -> None:
        """Runs a started run to its own end, which `finish()` then finds reached."""
        self._run.moved = True
        self._run_env(self._run.end if self._run.end is not None else self._run.until)

    def _look(self, task: str) -> None:
        """Runs a new entity of `task` to the end of its zero-time steps, before any
        other event at this instant, which SimPy's urgent start of a process ensures."""
        assert self._parts is not None  # the run is built
        process = self._tasks.spawn_entity(self, self._parts, task, None, None, None)
        try:
            while not process.triggered:
                if self.env.peek() > self.env.now:
                    raise RuntimeError(
                        f"the segment's look task '{task}' did not end at time "
                        f"{self.env.now}: it takes time or waits for something that "
                        "has not happened, and a look task runs only zero-time steps"
                    )
                self.env.step()
        except StopSimulation:
            pass  # _stop_run() ended the run

    def _run_steps(self) -> None:
        """Processes the model's events one at a time up to `until`, as a loop of
        `env.peek()` and `env.step()` does, publishing `system.simulation.step` after
        each. The run ends in place of SimPy's `EmptySchedule` when none is left."""
        count = 0
        while self.peek() < (math.inf if self._run.until is None else self._run.until):
            self.step()
            if self._run.stopped:
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
        previous, env._until_none = env._until_none, self._run.until is None
        try:
            with end_is_set(env):
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
        if self._run.stopped:
            return True
        if self._run.end is not None:
            return self._run.end_done
        return (
            self._run.until is not None
            and self._env is not None
            and self._env.now >= self._run.until
        )

    def _advance_to(self, target: simpy.Event) -> Any:
        """Runs to `target`, or to the run's own end if it comes first, and returns
        the value of whichever stopped the run."""
        env = self._env
        assert env is not None  # advance() checked that the run has started
        limit = self._run.end
        if limit is None and self._run.until is not None:
            # No stop or drain: the run's end is `until`, as `env.run(until)` ends it.
            # One internal event per run, so later calls add nothing to the queue.
            if self._run.limit_event is None:
                self._run.limit_event = schedule_urgent(
                    env, env.internal_event(), self._run.until - env.now
                )
            limit = self._run.limit_event
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
            until = self._defaults.until
        if stop is None and run_settings:
            stop = self._defaults.stop
        if drain is None and run_settings:
            drain = self._defaults.drain
        if on_unhandled_failure is None:
            on_unhandled_failure = (
                self._defaults.unhandled if run_settings else "continue"
            )
        # Held in locals and written to the run state after every check passes, so a
        # refused start leaves the run that is open as it was
        if segments is not None:
            run_segments = _segments_arg(segments)
        elif run_settings:
            run_segments = self._defaults.segments
        else:
            run_segments = None
        run_steps = _flag_arg(
            "steps", steps, self._defaults.steps if run_settings else False
        )
        run_sync = _flag_arg(
            "sync", sync, self._defaults.sync if run_settings else False
        )
        # Read before any connector starts its thread, so a bad value stops the run
        # with nothing to tear down
        if until is not None:
            until = checked_seconds(until, "until", below_zero=True)
        if until is not None and until <= self.initial_time:
            # SimPy's run(until=) refuses it; with stop or drain the end is an
            # event desflow schedules itself, which would be in the past
            raise ValueError(
                f"until ({until:g}) must be greater than the initial time "
                f"({self.initial_time:g}); set until later, or initial_time earlier"
            )
        if (stop or drain) and run_segments is not None:
            raise ValueError(
                "run.segments cannot be used with run.stop or run.drain: a segment "
                "ends at its own until or stop"
            )
        if (stop or drain) and run_steps:
            raise ValueError(
                "run.steps cannot be used with run.stop or run.drain: steps already "
                "reads the run after each event"
            )
        if run_steps and run_segments is not None:
            raise ValueError(
                "run.steps cannot be used with run.segments: steps already reads "
                "the run after each event"
            )
        if isinstance(run_segments, Mapping) and until is None:
            raise ValueError("segments {'every': ...} needs an until to divide")
        if isinstance(run_segments, Mapping) and math.isinf(until):
            raise ValueError(
                "segments {'every': ...} needs a finite until to divide, "
                f"and until gave {until:g}"
            )
        previous = self.initial_time
        for entry in run_segments if isinstance(run_segments, list) else ():
            if entry.get("until") is not None:
                problem = segment_until_problem(entry["until"], previous)
                if problem is not None:
                    raise ValueError(problem)
                previous = entry["until"]
            if entry.get("stop") is not None:
                _check_end(
                    None,
                    entry["stop"],
                    None,
                    self._signals.entries,
                    self._families.by_name,
                )
            look = entry.get("look")
            if look is not None and look not in self._tasks.specs:
                raise ValueError(
                    f"a segment's look names task '{look}', "
                    + not_added(look, self._tasks.specs, f"@app.task(name='{look}')")
                )
        if isinstance(drain, Mapping) and drain.get("limit") is not None:
            if checked_seconds(drain["limit"], "drain limit") <= 0:
                raise ValueError(
                    f"drain limit gave {drain['limit']!r}, and it must be above 0"
                )
        if stats is not None and stats not in STATS_LEVELS:
            raise ValueError(
                f"stats must be one of {', '.join(STATS_LEVELS)}, got {stats!r}"
            )
        _check_end(until, stop, drain, self._signals.entries, self._families.by_name)
        self._check_tasks(overrides)
        if self._sinks.traces and not self._sinks.stats_trace:
            raise ValueError(
                "when='trace' receives the records of stats.trace, so the context needs "
                "with_stats(trace=True)"
            )
        # Checked here, with the other settings, and not by _build(), which comes
        # after the run that is open has been closed
        if on_unhandled_failure not in UNHANDLED_FAILURE_POLICIES:
            raise ValueError(
                "on_unhandled_failure must be one of "
                + ", ".join(UNHANDLED_FAILURE_POLICIES)
                + f", got {on_unhandled_failure!r}"
            )
        self._checked_seed(seed)
        for setup in self._defaults.before_run:
            setup()
        # A run started and not closed is closed first, so its connectors stop
        if self._env is not None and not self._closed:
            self.close()
        # A second run of the same context starts with no entity and no task, and
        # numbers its entities and instances from the start again, so it repeats.
        # The settings above were set on the state of the run before, as they are
        # read from the new run's state from here on
        self._run = RunState(
            held=self._ends.held,
            segments=run_segments,
            steps=run_steps,
            sync=run_sync,
            on_unhandled_failure=on_unhandled_failure,
        )
        self._env = None
        self.started = False

        logger.info(f"Building SimulationContext for '{self.sim_id}'...")
        try:
            return self._build(
                _RunSettings(
                    until=until,
                    stop=stop,
                    drain=drain,
                    seed=seed,
                    overrides=overrides,
                    stats=stats,
                    egress=egress,
                    ingress=ingress,
                    source=source,
                    actor=actor,
                    reason=reason,
                )
            )
        except BaseException:
            # The connectors may have started their threads; stop them, as run()
            # would, so a failed start leaves no thread behind
            if self._env is not None:
                self._env.teardown()
            raise

    def _build(self, settings: _RunSettings) -> DynamicRealtimeEnvironment:
        """The part of `start()` after the checks: builds and starts the run, step by
        step in this order."""
        self._env = self._new_environment()
        self._run.env = self._env
        # The callbacks of this run, in place before the overrides are applied
        self._env.registry.on_change.extend(self.on_change)
        # One random sequence per distribution, all from the run's seed
        seed = self._run_seed(settings.seed)
        self.sampler = Sampler(seed=seed)
        # Fresh counters, vars_once and unique history for every run; each generator
        # draws from a sequence of its own, made from the seed, never the sampler's
        self._scope = LiveScope(self._env, self._run, self._families.by_name)
        parts = self._parts = parts_of(self)
        self.values = ValueRuntime(
            seed,
            self._env,
            self.sampler,
            self,
            live_scope=self._scope,
            stop=self._stop_run,
        )
        self._run.values = self.values
        self._new_arrival_and_holder_runs(self._env, self.sampler, self._scope, parts)
        self._keep_value_history(self.sampler)
        self._set_up_statistics(self._env, settings.stats, settings.egress)
        self._new_signal_and_family_runs(self._env, parts)
        self._read_replay_rows(self._env)
        self._register_parameters(self._env, settings)
        self._connect(self._env, settings.egress, settings.ingress)
        self._make_holders(self._env)
        # Set before the loops start: the scenario process reads it as it starts
        self._run.until = settings.until
        self._start_loops(self._env)
        self._run.end = self._end_event(settings.until, settings.stop, settings.drain)
        self._closed = False
        self.started = True
        if self._env.strict or self._run.sync:
            # Building takes wall time, which a strict run must not count as lag
            self._env.sync()
        logger.info("Simulation engine started.")
        return self._env

    def _new_environment(self) -> DynamicRealtimeEnvironment:
        """The run's environment, with the context's clock and time zone settings."""
        env_kwargs: Dict[str, Any] = {"factor": self.factor, "strict": self.strict}
        if self.initial_time:
            env_kwargs["initial_time"] = self.initial_time
        if self.logical_start_time is not None:
            env_kwargs["logical_start_time"] = self.logical_start_time
        if self.go_live_at is not None:
            env_kwargs["go_live_at"] = self.go_live_at
        if self.timezone is not None:
            env_kwargs["timezone"] = self.timezone
        env_kwargs["on_unhandled_failure"] = self._run.on_unhandled_failure
        return self.environment(**env_kwargs)

    def _checked_seed(self, seed: Optional[int]) -> Optional[int]:
        """The seed `start()` was given, else `random_seed`, checked to be a whole
        number from 0, and None when neither is set.

        Raises:
            ValueError: If the seed is not a whole number from 0.
        """
        if seed is not None:
            checked_whole(seed, "seed", 0)
            return seed
        if self.random_seed is not None:
            checked_whole(self.random_seed, "random_seed", 0)
        return self.random_seed

    def _run_seed(self, seed: Optional[int]) -> int:
        """The run's seed: the one `start()` was given, else `random_seed`, else a
        new one, which the log names so the run can be repeated."""
        seed = self._checked_seed(seed)
        if seed is None:
            seed = secrets.randbelow(2**32)
            logger.info(
                f"No random_seed was given, so this run uses {seed}. "
                + (
                    self._defaults.repeat_hint.format(seed=seed)
                    if self._defaults.repeat_hint
                    else f"Pass random_seed={seed} to repeat it."
                )
            )
        return seed

    def _new_arrival_and_holder_runs(
        self,
        env: DynamicRealtimeEnvironment,
        sampler: Sampler,
        scope: LiveScope,
        parts: Parts,
    ) -> None:
        """Makes the run's part of the arrivals and of the holders."""
        self._run.arrival_run = self._arrivals.new_run(
            env,
            self.values,
            sampler,
            self.sim_id,
            functools.partial(self._tasks.start, self, parts),
        )
        self._run.holder_run = self._holders.new_run(
            env, self.values, sampler, self.sim_id, scope.scope
        )

    def _keep_value_history(self, sampler: Sampler) -> None:
        """Keeps the rows, the previous values and the route generators the run's
        tasks read."""
        for task_name in sorted(
            self._tasks.row_targets
            | {n for n, spec in self._tasks.specs.items() if spec.history is not None}
        ):
            self.values.keep_rows(
                task_name, self._tasks.specs[task_name].history or DEFAULT_HISTORY
            )
        for task_name in self._tasks.previous_users:
            self.values.keep_previous(task_name)
        for task_name, spec in self._tasks.specs.items():
            if spec.router.weighted:
                self._run.routes[task_name] = sampler.generator(f"route.{task_name}")

    def _set_up_statistics(
        self, env: DynamicRealtimeEnvironment, stats: Optional[str], egress: bool
    ) -> None:
        """Sets the run's statistics level, and its timer when the run has sinks."""
        level = self._sinks.stats_level
        if stats is not None and STATS_LEVELS.index(stats) > STATS_LEVELS.index(level):
            level = stats
        if level != "none" and egress:
            env.setup_stats(self.sim_id, level, self._sinks.stats_interval)
        elif level != "none":
            # Without sinks nothing receives a publication, so no timer runs and
            # the statistics are read at the end
            env.stats.configure(self.sim_id, level, self._sinks.stats_interval)
        env.stats.on_change = self._sinks.stats_on_change and level != "none"

    def _new_signal_and_family_runs(
        self, env: DynamicRealtimeEnvironment, parts: Parts
    ) -> None:
        """Makes the run's part of the signals and of the families."""
        self._run.signal_run = self._signals.new_run(
            env,
            self._families.by_name,
            functools.partial(self._tasks.run_on_action, self, parts),
            self._run.end_processed,
        )
        self._run.family_run = self._families.new_run(
            self.values, self._run.holders, self._run.signals
        )

    def _read_replay_rows(self, env: DynamicRealtimeEnvironment) -> None:
        """Reads the rows of each replayed arrival into the run's arrivals."""
        rows: Dict[str, Any] = {}
        for name, spec in self._arrivals.replays.items():
            try:
                rows[name] = collections.deque(
                    read_trace(
                        **spec,
                        start=env.start_datetime,
                        initial_time=float(self.initial_time),
                    )
                )
            except ValueError as error:
                raise ValueError(f"{self._replay_place(name)}: {error}") from None
        self._run.arrivals.replay_rows = rows

    def _replay_place(self, name: str) -> str:
        """What starts the refusal of a replayed arrival's rows: where a blueprint
        writes its `replay.path`, or the arrival and its file. Rows given in memory
        are not printed."""
        place = self._arrivals.replay_places.get(name)
        if place is not None:
            return place
        source = self._arrivals.replays[name]["source"]
        if isinstance(source, (str, Path)):
            return f"arrival '{name}' replays {source}"
        return f"arrival '{name}' replays its rows"

    def _register_parameters(
        self, env: DynamicRealtimeEnvironment, settings: _RunSettings
    ) -> None:
        """Registers the run's copy of the parameters, then applies the overrides."""
        # The run registers its own copy of the parameters, so a live change or an
        # override changes the copy and the declarations stay as written: a second
        # run starts from the values the first one started from
        env.registry.register_sim_parameter(_run_parameters(self.compile_parameters()))
        for path, value in (settings.overrides or {}).items():
            env.registry.set_exactly(
                path,
                value,
                source=settings.source,
                actor=settings.actor,
                reason=settings.reason,
            )

    def _connect(
        self, env: DynamicRealtimeEnvironment, egress: bool, ingress: bool
    ) -> None:
        """Starts the run's ingress and egress connectors, or the warmup's end when
        the run has no sinks, and the event trace."""
        if self._sinks.ingress and ingress:
            env.setup_ingress(self._sinks.ingress)

        if self._sinks.egress and egress:
            self._sinks.setup_egress(
                env,
                self.warmup,
                "auto" if self.lag_interval is None else self.lag_interval,
            )
        elif self.warmup is not None:
            env.start_internal(self._end_warmup(self.warmup))
        env.trace_events(self._sinks.stats_trace and egress and self._sinks.traces)

    def _make_holders(self, env: DynamicRealtimeEnvironment) -> None:
        """Makes the resources, stores and containers that are not members of a
        family, starts their breakdowns, then the families' members."""
        for res_id in self._holders.resources_config.keys():
            if res_id not in self._families.by_name:
                self._run.holders.new_resource(res_id)
        for store_id in self._holders.stores_config:
            if store_id not in self._families.by_name:
                self._run.holders.new_store(store_id)
        for container_id in self._holders.containers_config:
            if container_id not in self._families.by_name:
                self._run.holders.new_container(container_id)
        for res_id, breakdown in self._holders.breakdowns_config.items():
            if res_id not in self._families.by_name:
                self._run.holders.start_breakdown(res_id, breakdown)
        self._run.families.start_members()

    def _start_loops(self, env: DynamicRealtimeEnvironment) -> None:
        """Starts the arrival loops, with each arrival's `initial` entities, and the
        other startup loops, in the order they were added."""
        for loop_func, meta in self._startup_loops:
            if meta:
                # Arrival loops require context injection
                process = env.process(loop_func(self))
                # An arrival's `initial` entities are created right after its source
                # starts, so file order is creation order
                plan = self._arrivals.plans.get(meta)
                if plan is not None and plan.initial:
                    self._run.arrivals.start_initial(meta, plan)
                # Stopped by an interrupt when a drain starts, which ends the loop;
                # any other error in the loop still stops the run, as in SimPy
                process.callbacks.append(defuse_stopped)
                self._run.arrivals.processes.append(process)
            else:
                # Telemetry loops run blindly
                env.process(loop_func())

    def _stop_run(self) -> None:
        """Ends the run at the current time: the events already due run no further,
        `finish()` returns at once, and a later `advance()` or `step()` raises and
        `peek()` gives infinity.

        A `row` generator with `on_end: stop` calls it when its rows run out."""
        env = self._env
        if env is None or self._run.stopped:
            return
        self._run.stopped = True
        event = schedule_urgent(env, env.internal_event())
        event.callbacks.append(StopSimulation.callback)

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
        if self._run.stopped:
            raise RuntimeError(
                f"the run ended at {env.now:g}; close() it, or start() a new run"
            )
        try:
            env.step()
        except (StopSimulation, RunStopped):
            pass  # _stop_run() ended the run
        self._run.moved = True

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
        return math.inf if self._run.stopped else env.peek()

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
        if self._run.arrival_run is not None:
            self._run.arrival_run.stop()

    def _end_event(self, until: Any, stop: Any, drain: Any) -> Optional[simpy.Event]:
        """The event that ends the run when `stop`, `drain` or a sample limit is
        given, else None.

        It is scheduled urgent, like SimPy's own `until`, so no other event at that
        time runs after it. A sample's console calls `RunState.finish_run` at its
        last event.
        """
        if stop is None and not drain and self._defaults.sample_limit is None:
            return None
        env = self.env
        end = env.internal_event()

        def finish(event: Any = None) -> None:
            if not end.triggered:
                if event is None:
                    schedule_urgent(env, end)
                else:
                    # The run ends with what its stop event carried: a signal's
                    # value, True for a condition, or the exception of a failed
                    # stop, which `end` raises once. The stop event is defused so
                    # the run does not raise it a second time.
                    if not event.ok:
                        event.defused = True
                    schedule_urgent(env, end, ok=event.ok, value=event.value)

        self._run.finish_run = finish
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
            horizon = schedule_urgent(env, env.internal_event(), until - env.now)
            if drain:

                def begin_drain(_event: Any) -> None:
                    self.stop_arrivals()
                    self._run.draining = True
                    if self._run.in_system == 0:
                        finish()
                    elif limit is not None:
                        timer = schedule_urgent(env, env.internal_event(), limit)
                        timer.callbacks.append(finish)

                horizon.callbacks.append(begin_drain)
            else:
                horizon.callbacks.append(finish)
        if stop is not None:
            self._stop_event(stop).callbacks.append(finish)
        end.callbacks.append(self._mark_end)
        return end

    def _mark_end(self, _event: Any) -> None:
        """Records that the end event has been processed, for `advance()`."""
        self._run.end_done = True

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
                target = self._run.families.open_reference(
                    target, self.values.scope_of()
                )
            if target not in self._run.signals.made:
                raise ValueError(
                    f"stop names signal '{target}', "
                    + not_added(
                        target, self._run.signals.made, f"add_signal('{target}')"
                    )
                )
            return self._run.signals.made[target].wait()
        if kind != "condition":
            raise ValueError(f"stop takes a signal or a condition, not '{kind}'")
        condition = Expression(target)
        env, event = self.env, self.env.event()
        live = self._scope
        assert live is not None

        def check() -> None:
            scope = live.scope(None, {})
            if not event.triggered and condition.evaluate(scope):
                event.succeed(True)
                env.unwatch_changes(check)

        check()
        if not event.triggered:
            env.watch_changes(check, queues=condition.reads_queues)
        return event

    def _end_warmup(self, warmup: float):
        """Resets the statistics at the end of the warm-up, for a run with no sink."""
        yield self.env.internal_timeout(warmup)
        self.env.stats.reset()

    def family_read_problem(self, expression: Expression) -> Optional[str]:
        """What is wrong with the way an expression reads families, or None: a family
        read without an index, or an index on a name that is no family.

        Args:
            expression: The parsed expression.

        Returns:
            Optional[str]: The reason, naming the read.
        """
        return family_read_problem(expression, self._families.by_name)

    def _check_tasks(self, overrides: Optional[Mapping[str, Any]] = None) -> None:
        """Sets `_ends` from the model's declarations, then checks them, before the
        clock or any connector starts.

        Args:
            overrides: The values `start()` applies before the clock starts. The
                scenario is checked against them, as the run will apply it.
        """
        self._ends = self._checks.end_sets()
        self._checks.check(
            self.sim_id,
            overrides,
            lambda: scratch_registry(self),
            self._order_warned,
        )


def _wait_so_far(run: Any) -> Scope:
    """The scope of a task instance that an interrupt `where` is read in. An instance
    records its wait's branches only when the wait ends, so an instance that has not
    reached its wait, or is still in it, reads each branch as not done, as a branch
    that did not happen before the wait ended reads. An `each` branch has no elements
    then, so its `values` and `flags` are empty."""
    scope = run.scope()
    wait = run.spec.wait
    if wait is None or all(name in scope.waits for name in wait.branches):
        return scope
    waits: Dict[str, Dict[str, Any]] = {}
    for name, node in wait.branches.items():
        waits[name] = {"done": False, "ok": None, "value": None}
        if node.template is not None:
            waits[name].update(values=[], flags=[])
    return dataclasses.replace(scope, waits={**waits, **scope.waits})


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
        from desflow.values.models import Gen

        if isinstance(members, bool) or not (
            isinstance(members, (numbers.Integral, str, Gen)) or callable(members)
        ):
            raise ValueError(
                f"{where}.members is a whole number, an expression or a generator, "
                f"got {members!r}"
            )
        if isinstance(members, numbers.Integral) and members < 0:
            raise ValueError(f"{where}.members is 0 or more, got {members!r}")
        if isinstance(members, str):
            outside = Expression(members).outside_run()
            if outside:
                raise ValueError(
                    f"{where}.members '{members}' reads {outside}, but a family has "
                    f"no entity; members reads the run (level, users, queue, signal, "
                    f"registry and t)"
                )
    texts: Dict[str, str] = {}
    values: List[Any] = []
    for field, value in fields.items():
        if isinstance(value, str) and family:
            texts[field] = value
            value = None
        values.append(value)
    return tuple(values), texts


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
    from desflow.core.rules import RunSegment, SegmentEvery

    if isinstance(segments, Mapping):
        try:
            return {"every": SegmentEvery.model_validate(dict(segments)).every}
        except ValidationError as error:
            raise ValueError(f"segments: {_first_message(error)}") from None
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
        except ValidationError as error:
            raise ValueError(f"segments[{index}]: {_first_message(error)}") from None
        except ValueError as error:
            raise ValueError(f"segments[{index}]: {error}") from None
    return plan


def _first_message(error: Any) -> str:
    """The first message of a pydantic error, as a blueprint gives it: a refusal a
    validator wrote as it is, and any other with the key it is about in front."""
    first = error.errors()[0]
    text = first["msg"].removeprefix("Value error, ")
    if first["type"] != "value_error" and first["loc"]:
        return f"{'.'.join(str(part) for part in first['loc'])}: {text}"
    return text


def segment_until_problem(until: float, previous: float) -> Optional[str]:
    """The refusal for a segment that does not end after the one before it.

    Args:
        until: The segment's end.
        previous: The end of the segment before it, or of the run so far.
    """
    if until <= previous:
        return (
            f"segment until ({until:g}) must be after the previous end "
            f"({previous:g}); segment untils increase"
        )
    return None


def _flag_arg(name: str, value: Any, default: bool) -> bool:
    """A `run()` switch: `value` when it is given, else `default`."""
    if value is None:
        return default
    if not isinstance(value, bool):
        raise ValueError(f"{name} is True or False, got {value!r}")
    return value


def reading_problem(name: str, earlier: Iterable[Mapping[str, Any]]) -> Optional[str]:
    """The refusal for a telemetry reading named as a statistic the run publishes, or
    as a reading of an earlier entry, which would put two records with one `path_id`
    in the stream.

    Args:
        name: The reading's name.
        earlier: The readings already declared, one mapping for each entry.
    """
    if name == "stats" or name.startswith("stats."):
        return (
            f"reading '{name}' has the name of a statistic the run publishes (the "
            f"names under stats.); use another name"
        )
    if any(name in readings for readings in earlier):
        return f"reading '{name}' is already a reading of another entry"
    return None


def stop_family_problem(name: str) -> str:
    """The refusal for a stop that names a family of signals rather than a member.

    Args:
        name: The family's name.
    """
    return (
        f"stop names the family '{name}'; a stop names a member with "
        f"{{of: {name}, key: ...}}"
    )


def stop_condition(text: Any) -> Expression:
    """A stop condition, parsed, and refused when it reads the entity or a payload.

    One check for `run(stop=)`, a segment's stop and a blueprint's `run.stop`.

    Args:
        text: The stop condition as written.

    Raises:
        ValueError: If the text does not parse or reads what no task is running.
    """
    expression = Expression(text)
    entity = [
        f"{kind}.<...>"
        for kind in ("payload", "attr", "item", "wait")
        if expression.references.get(kind)
    ]
    if entity:
        raise ValueError(
            f"a stop condition is checked outside every task, so it cannot read "
            f"{', '.join(entity)}; read levels, registry values, signals or t"
        )
    return expression


def _check_end(
    until: Any,
    stop: Any,
    drain: Any,
    signals: Mapping[str, Any],
    families: Iterable[str] = (),
) -> None:
    """Checks `stop` and `drain` before anything is built."""
    if drain is not None and not isinstance(drain, (bool, Mapping)):
        raise ValueError(f"drain is True, False or {{'limit': seconds}}, got {drain!r}")
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
        written = target
        target = name_of(target, "a stop signal")
        if isinstance(written, str) and target in families:
            raise ValueError(stop_family_problem(target))
        if target not in signals:
            raise ValueError(
                f"stop names signal '{target}', "
                + not_added(target, signals, f"add_signal('{target}')")
            )
    elif kind == "condition":
        stop_condition(target)
    else:
        raise ValueError(f"stop takes a signal or a condition, not '{kind}'")


def _run_parameters(params: SimParameter) -> SimParameter:
    """The parameters one run registers, copied from a model's parameters.

    Each section is a new mapping with `_run_copy` of each config object in it, and
    the variables are a new mapping. A variable's value and a store's starting items
    are the user's own objects, shared by every run. The run and `scratch_registry`
    both register what this gives.

    Args:
        params: The model's parameters, from `SimulationContext.compile_parameters`.
    """

    def copied(section: Dict[str, Any]) -> Dict[str, Any]:
        return {name: _run_copy(config) for name, config in section.items()}

    return SimParameter(
        sim_id=params.sim_id,
        resources=copied(params.resources),
        containers=copied(params.containers),
        stores=copied(params.stores),
        breakdowns=copied(params.breakdowns),
        service=copied(params.service),
        arrival=copied(params.arrival),
        variables=dict(params.variables),
    )


def scratch_registry(context: SimulationContext) -> SimulationRegistry:
    """Builds a registry with the paths a run of `context` registers, on a plain
    SimPy clock.

    The load and start checks and `registry_paths` call it, so they cannot build the
    registry two ways. It registers `_run_parameters` of the model's parameters, the
    copy a run registers, so the defaults registration fills in and a check's
    `change` go into the copy, and the model's own starting values do not move. A
    value that cannot be copied, such as a lock, is the user's own object here too.

    Args:
        context: The context, read through its public `compile_parameters`.
    """
    registry = SimulationRegistry(simpy.Environment())
    registry.register_sim_parameter(_run_parameters(context.compile_parameters()))
    return registry


def parts_of(context: SimulationContext) -> Parts:
    """The parts of `context`, read from its attributes now.

    The package's own modules reach the pieces of a context through this, where a
    frozen signature gives them only the context.

    Args:
        context: The context.
    """
    return Parts(
        tasks=context._tasks,
        families=context._families,
        arrivals=context._arrivals,
        holders=context._holders,
        signals=context._signals,
        sinks=context._sinks,
        defaults=context._defaults,
        scenario_steps=context._scenario_steps,
        startup=context._startup_loops,
        ends=context._ends,
        run=context._run,
        scope=context._scope,
        services=context._services_config,
        processes=context._processes,
        run_scenario=context._run_scenario,
        readings=context._readings,
    )


def _task_fields(
    service_id: Optional[str] = None,
    resource_id: Union[str, Mapping[str, Any], List[Any], None] = None,
    *,
    name: Optional[str] = None,
    priority: Union[float, str, Callable[[Any, Dict[str, Any]], float], None] = None,
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
) -> "_TaskFields":
    """The first phase of `task()`: checks its arguments, in the order `task()`
    documents its refusals, and turns them into the fields of its `TaskSpec`."""
    # The name is None until the decorator reads it from the function
    place = f"task '{name}'" if name is not None else "a task's"
    prefix = f"task '{name}': " if name is not None else ""
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
    interrupt = _parse_interrupt(interrupt)
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
    if isinstance(service_time, numbers.Real):
        service_time = checked_seconds(service_time, f"{place} service_time")
    if service_time is not None and not (
        isinstance(service_time, (str, Gen))
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
            f"{prefix}priority and preempt of a task apply to a resource, a "
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
    if isinstance(patience, numbers.Real):
        patience = checked_seconds(patience, f"{place} patience")
    take = None if get is None else Transfer.of(get)
    if take is not None and take.priority != 0.0:
        raise ValueError(
            "a get takes no priority; priority orders a put into a priority store"
        )
    give = None if put is None else Transfer.of(put)
    if give is not None and give.filter is not None:
        raise ValueError("a put takes no filter; a filter picks the item a get takes")
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
    return _TaskFields(spec_kwargs, generated, router, take, give, resource_id)


def _parse_interrupt(
    interrupt: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    """A task's `interrupt` mapping as a dict, checked, with its `index` parsed."""
    if interrupt is None:
        return None
    interrupt = dict(interrupt)
    unknown = {*interrupt} - {"task", "handle", "where", "cause", "index"}
    if interrupt.get("index") is not None:
        if interrupt.get("handle") is None:
            raise ValueError("interrupt takes an index with a handle only")
        interrupt["index"] = Expression(str(interrupt["index"]))
    if (interrupt.get("task") is None) == (interrupt.get("handle") is None):
        raise ValueError(
            "interrupt takes a task or a handle, and an optional where and cause"
        )
    if unknown:
        raise ValueError(
            "interrupt takes a task or a handle, and an optional where and cause"
        )
    if interrupt.get("handle") is not None:
        if interrupt.get("where") is not None:
            raise ValueError(
                "interrupt takes no where with a handle, which names one child flow"
            )
        interrupt.setdefault("task", None)
    if interrupt.get("where") is not None:
        Expression(interrupt["where"])
    return interrupt


class _TaskFields(NamedTuple):
    """What `_task_fields` gives `task()`'s decorator."""

    spec_kwargs: Dict[str, Any]
    generated: Dict[str, Any]
    router: Router
    take: Optional[Transfer]
    give: Optional[Transfer]
    resource_id: Any


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
    elif not callable(count) and not isinstance(count, Gen):
        if isinstance(count, bool) or not isinstance(count, numbers.Integral):
            raise ValueError(
                f"a spawn count is a whole number, an expression, a generator or a "
                f"function of the entity, got {count!r}"
            )
        if count < 0:
            raise ValueError(f"a spawn count must be 0 or more, got {count!r}")
        count = int(count)
    copy_attributes = spec.get("copy_attributes", False)
    if not isinstance(copy_attributes, bool):
        raise ValueError(f"copy_attributes is true or false, got {copy_attributes!r}")
    task = spec.get("task")
    if not isinstance(task, str):
        raise ValueError(f"a spawn names a task, got {task!r}")
    after = spec.get("after")
    if after is not None:
        after = checked_seconds(after, "a spawn after", below_zero=True)
        if not after > 0:
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
        if after is not None:
            after = checked_seconds(after, "a fire after", below_zero=True)
            if not after > 0:
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
    if "value" not in entry:
        raise ValueError("a path needs a value")
    if not isinstance(entry["path"], str):
        raise ValueError(f"an on: action's path is text, not {entry['path']!r}")
    return {"path": entry["path"], "value": entry["value"]}


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
    if isinstance(timeout, numbers.Real):
        timeout = checked_seconds(timeout, "batch timeout")
    check_batch_size(spec["size"])
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
        limit is None or isinstance(limit, (numbers.Integral, Gen, Expression))
    ):
        raise ValueError(
            f"a resume limit is a whole number, an expression or a generator, "
            f"got {limit!r}"
        )
    if isinstance(limit, numbers.Integral) and limit < 0:
        raise ValueError(f"a resume limit must be 0 or more, got {limit!r}")
    return {
        "task": spec["task"],
        "where": Expression(where) if isinstance(where, str) else where,
        "value": spec.get("value"),
        "limit": limit,
    }
