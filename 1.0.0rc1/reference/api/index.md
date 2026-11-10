# API Reference

## Simulation Context

### `desflow.SimulationContext(sim_id, factor=1.0, random_seed=None, logical_start_time=None, go_live_at=None, warmup=None, initial_time=0.0, lag_interval=None, timezone=None, strict=None, environment=None)`

Builder and Orchestrator for Event-Driven Digital Twins.

The `SimulationContext` implements the Builder Pattern to construct the foundational architecture of a Digital Twin. It acts as the central control plane, aggregating discrete machine capacities, continuous physical states (wear and tear), and statistical distributions into a unified `SimParameter` object.

Architectural Guarantees:

1. **Immutability During Configuration**: The underlying `DynamicRealtimeEnvironment` and its background asynchronous I/O threads are completely deferred until `.run()` is explicitly invoked. This prevents partial state leaks or unintended clock starts.
1. **One registry**: All registered configurations are flattened and managed by the internal `SimulationRegistry`, allowing external Kafka Admin commands to mutate these states safely during runtime.
1. **Deterministic Stochasticity**: The `Sampler` gives each distribution a random sequence of its own, made from `random_seed`, so a seeded run repeats exactly, and adding a distribution leaves the others' draws unchanged. A run without `random_seed` draws a seed and logs it, so it can be repeated.

Attributes:

| Name          | Type                                  | Description                                                                                                                                                                                                                                                                                                                 |
| ------------- | ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sim_id`      | `str`                                 | The unique namespace for this simulation instance (e.g., 'HotRolling').                                                                                                                                                                                                                                                     |
| `factor`      | `float`                               | The real-time synchronization multiplier. A factor of 1.0 syncs strictly with wall-clock time, while 0.0 executes instantly in a fast-forward loop (ideal for historical data processing).                                                                                                                                  |
| `go_live_at`  | `Optional[datetime]`                  | Logical instant at which factor is replaced by real-time pacing, so one run can backfill history and then tail live.                                                                                                                                                                                                        |
| `warmup`      | `Optional[float]`                     | The end of the warm-up, in simulation seconds.                                                                                                                                                                                                                                                                              |
| `random_seed` | `Optional[int]`                       | The run's seed. When None, run() draws one, logs it and uses it; read it afterwards from sampler.seed.                                                                                                                                                                                                                      |
| `sampler`     | `Optional[Sampler]`                   | The centralized Random Number Generator engine. Accessible to user-defined raw generators only after .run() is called.                                                                                                                                                                                                      |
| `on_change`   | `List[Callable[[ParamChange], None]]` | Callbacks that receive a ParamChange for every accepted registry change. Each start() copies the list into the registry of the run it builds, so a callback added after start() reaches the next run; for the current run, add it to env.registry.on_change. A YAML model takes callbacks on the context from_yaml returns. |

Initializes the context builder with a designated namespace and temporal factor.

Parameters:

| Name                 | Type                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                | Default    |
| -------------------- | -------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `sim_id`             | `str`                                        | The unique prefix for this group of parameters. All generated telemetry and event paths will be prefixed with this ID.                                                                                                                                                                                                                                                                                     | *required* |
| `factor`             | `float`                                      | The real-time synchronization factor. Defaults to 1.0.                                                                                                                                                                                                                                                                                                                                                     | `1.0`      |
| `random_seed`        | `Optional[int]`                              | Optional seed for deterministic execution.                                                                                                                                                                                                                                                                                                                                                                 | `None`     |
| `logical_start_time` | `Optional[datetime]`                         | Optional override for the environment's base clock, forwarded to DynamicRealtimeEnvironment. Crucial for historical backfilling (e.g., generating data from last week).                                                                                                                                                                                                                                    | `None`     |
| `go_live_at`         | `Optional[datetime]`                         | Optional logical instant at which the run stops using factor and switches to real time, forwarded to DynamicRealtimeEnvironment. With factor=0.0 and a backdated logical_start_time, the history up to this instant is generated as fast as the machine allows and the rest arrives in real time, so a backfill and a live tail take one run.                                                              | `None`     |
| `warmup`             | `Union[float, str, None]`                    | The end of the warm-up, in simulation seconds or as a duration such as "10 min". No sink receives a record stamped before it, and the built-in statistics are reset at that time, so they measure the steady state only.                                                                                                                                                                                   | `None`     |
| `initial_time`       | `Union[float, str]`                          | The simulation clock's starting value, in seconds or as a duration such as "8 h", so event times, sim_ts and until are readings of a clock that starts there. Defaults to 0.                                                                                                                                                                                                                               | `0.0`      |
| `lag_interval`       | `Union[float, str, None]`                    | Simulation seconds, or a duration such as "10 s", between system.simulation.lag_seconds records, which measure how far the run is behind the wall clock. By default it is off at factor 0, where lag means nothing, and every simulation second otherwise; a run that goes live starts it then. 0 turns it off.                                                                                            | `None`     |
| `timezone`           | `Optional[str]`                              | An IANA time zone name, such as "Australia/Melbourne", that record timestamps are written in, with their offset. By default the zone of logical_start_time, or UTC. A logical_start_time or go_live_at without an offset is read in it.                                                                                                                                                                    | `None`     |
| `strict`             | `Optional[bool]`                             | When True a step raises RuntimeError if the run is more than factor real seconds behind the wall clock, as SimPy's RealtimeEnvironment does; at factor 0 that is any step once the wall clock has moved. By default (None) it is True for a paced run or one with go_live_at, and False at factor 0 with no go_live_at. Pass False for a live run whose sink blocks and should go on when it falls behind. | `None`     |
| `environment`        | `Optional[Type[DynamicRealtimeEnvironment]]` | A subclass of DynamicRealtimeEnvironment that the run is built on, for a model that overrides step, schedule or __init__. A subclass that overrides __init__ takes \*\*kwargs and passes them on. One that overrides step or schedule calls super(). By default DynamicRealtimeEnvironment.                                                                                                                | `None`     |

Raises:

| Type         | Description                                                                                                                                                      |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If warmup or lag_interval is negative or not a duration, timezone is not an IANA time zone name, or environment is not a subclass of DynamicRealtimeEnvironment. |

Source code in `src/desflow/core/context.py`

```python
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
    self.logical_start_time = logical_start_time
    self.go_live_at = go_live_at
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
```

#### Attributes

##### `env`

Read-only handle to the active `DynamicRealtimeEnvironment`.

Grants raw generators access to environment primitives such as `publish_event`, `timeout`, `now`, and `start_datetime` without reaching into private builder state.

Raises:

| Type           | Description                                           |
| -------------- | ----------------------------------------------------- |
| `RuntimeError` | If accessed during the Builder Phase (before .run()). |

#### Methods:

##### `add_arrival(name, dist=None, *, rate=None, mean=None, std=None, low=None, value=None, high=None, mode=None, shape=None, scale=None, values=None, weights=None, edges=None, counts=None, p=None, gap=None, attributes=None, key=None, spawn=None, first_at=None, group=1, active=True, off_mode='drop', initial=0, max_events=None, vars=None)`

Registers a statistical distribution defining the inter-arrival times of entities.

Each arrival starts a new entity, which `wait_for_arrival` returns. Its key, the record key of every event it causes, is the arrival name and a counter (`orders-1`, `orders-2`) unless `key` gives another.

Parameters:

| Name         | Type                                    | Description                                                                                                                                                                                                                                                                                                                                                                  | Default    |
| ------------ | --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `name`       | `str`                                   | The internal identifier (e.g., 'orders').                                                                                                                                                                                                                                                                                                                                    | *required* |
| `dist`       | `Optional[str]`                         | The family, as for add_service, with the same parameters and the same durations. Give gap instead of it for a gap that is not a distribution.                                                                                                                                                                                                                                | `None`     |
| `rate`       | `Optional[float]`                       | Events per second, for exponential.                                                                                                                                                                                                                                                                                                                                          | `None`     |
| `mean`       | `Optional[Union[float, str]]`           | The mean, for normal, lognormal, truncated_normal and poisson.                                                                                                                                                                                                                                                                                                               | `None`     |
| `std`        | `Optional[Union[float, str]]`           | The standard deviation, for the first three (0 when not given).                                                                                                                                                                                                                                                                                                              | `None`     |
| `low`        | `Optional[Union[float, str]]`           | The lowest value, for truncated_normal, uniform and triangular.                                                                                                                                                                                                                                                                                                              | `None`     |
| `value`      | `Optional[Union[float, str]]`           | The time a constant distribution always gives.                                                                                                                                                                                                                                                                                                                               | `None`     |
| `high`       | `Optional[Union[float, str]]`           | The highest value, for uniform and triangular.                                                                                                                                                                                                                                                                                                                               | `None`     |
| `mode`       | `Optional[Union[float, str]]`           | The most likely value, for triangular.                                                                                                                                                                                                                                                                                                                                       | `None`     |
| `shape`      | `Optional[float]`                       | The shape, for gamma and weibull.                                                                                                                                                                                                                                                                                                                                            | `None`     |
| `scale`      | `Optional[Union[float, str]]`           | The scale, for gamma and weibull.                                                                                                                                                                                                                                                                                                                                            | `None`     |
| `values`     | `Optional[Sequence[Union[float, str]]]` | The values an empirical draws from, taken at load.                                                                                                                                                                                                                                                                                                                           | `None`     |
| `weights`    | `Optional[List[float]]`                 | One weight per empirical value.                                                                                                                                                                                                                                                                                                                                              | `None`     |
| `edges`      | `Optional[Sequence[Union[float, str]]]` | The bin edges of an empirical histogram.                                                                                                                                                                                                                                                                                                                                     | `None`     |
| `counts`     | `Optional[List[float]]`                 | One count per empirical histogram bin.                                                                                                                                                                                                                                                                                                                                       | `None`     |
| `p`          | `Optional[float]`                       | The chance of success on each try, for geometric.                                                                                                                                                                                                                                                                                                                            | `None`     |
| `gap`        | `Any`                                   | The time to the next arrival in place of dist, as a blueprint's gap: a number of seconds, a duration such as "5 min", an expression such as "1 + t / 4" or a generator, read at the start of each gap.                                                                                                                                                                       | `None`     |
| `attributes` | `Optional[Mapping[str, Any]]`           | The attributes each entity starts with, read in conditions as attr.<name>. Each entity gets its own copy.                                                                                                                                                                                                                                                                    | `None`     |
| `key`        | `Any`                                   | The entity's key. A function of the entity's counter, from 1, returning its key; or a value as a blueprint's key reads it: text or a number is the key itself, and a generator is read once for each entity, after vars and the attributes. An expression key is generator({"gen": "expr", "expr": "..."}). wait_for_arrival reads it too, so it needs no spawn.             | `None`     |
| `spawn`      | `Optional[str]`                         | The task each arrival starts, with the arrival's entity and the task's next instance id, as YAML's spawn key does. None leaves the arrival to an arrival_loop written by hand.                                                                                                                                                                                               | `None`     |
| `first_at`   | `Optional[float]`                       | The simulation time of the first arrival, which the first wait_for_arrival waits for in place of a gap. The gaps after it come from the distribution. A time already past arrives at once.                                                                                                                                                                                   | `None`     |
| `group`      | `Any`                                   | How many entities one arrival starts, with spawn: a whole number, an expression such as "int(t / 5)" or a generator, read at each arrival after the gap. 0 or less starts nobody and takes no key number.                                                                                                                                                                    | `1`        |
| `active`     | `bool`                                  | False starts the source off, with spawn. A task's activate turns it on.                                                                                                                                                                                                                                                                                                      | `True`     |
| `off_mode`   | `str`                                   | With spawn, "drop" (the default): an arrival that falls while the source is off starts nobody. "restart": turning the source off drops the pending arrival, and turning it on starts a whole new gap.                                                                                                                                                                        | `'drop'`   |
| `initial`    | `int`                                   | With spawn, how many entities exist, in creation order, when the run starts, before any gap passes.                                                                                                                                                                                                                                                                          | `0`        |
| `max_events` | `Any`                                   | With spawn, how many times the arrival starts its task, in all, as a blueprint's max_events: a whole number of 0 or more, an expression such as "int(t / 5)" or a generator, read when the source starts. The arrival stops after the last, and its last start draws no further gap.                                                                                         | `None`     |
| `vars`       | `Optional[Mapping[str, Any]]`           | Named values read once for each entity, before its attributes and key, as a blueprint's arrival vars. Each is a constant or a generator such as generator({"gen": "boolean", "p": 0.1}); a plain mapping with gen is refused. A name is one an expression can read and does not already use. An attribute and the key read them with generator({"gen": "var", "name": ...}). | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If the family is unknown, or a parameter is missing, out of range or not one the family takes, or both or neither of dist and gap are given, or gap is a negative number or neither a number, an expression nor a generator, or first_at is below 0 or not a number, or off_mode, active, group, initial, max_events or vars is malformed, or group, active, off_mode, initial or max_events is given with no spawn, or gap, group or max_events is an expression that reads the entity, or a vars name is not one an expression can read. A spawn naming no task is refused when the run starts. |

Source code in `src/desflow/core/context.py`

```python
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
    first_at: Optional[float] = None,
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
            an expression nor a generator, or `first_at` is below 0 or not a
            number, or `off_mode`, `active`, `group`, `initial`, `max_events` or
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
```

##### `add_arrival_replay(name, source, *, time='time', unit='s', fields='all', on_invalid='error', attributes=None, key=None, spawn=None, active=True, off_mode='drop', vars=None)`

Registers an arrival taken from recorded rows instead of a distribution.

Each row becomes one arrival at its recorded time, in the order given, and its fields become the entity's attributes. `wait_for_arrival(name)` returns the wait for the next row, so an arrival loop is written as for any other arrival. Rows at equal times arrive at that time in their order; rows at or after `until` never arrive; after the last row the arrival stops, with one INFO line. The rows are read and checked when the run starts.

Parameters:

| Name         | Type                                            | Description                                                                                                                                                                                            | Default    |
| ------------ | ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| `name`       | `str`                                           | The arrival's name.                                                                                                                                                                                    | *required* |
| `source`     | `Union[str, Path, Iterable[Mapping[str, Any]]]` | A CSV or Parquet file (Parquet needs the parquet extra), or an iterable of mappings.                                                                                                                   | *required* |
| `time`       | `str`                                           | The column of each row's time.                                                                                                                                                                         | `'time'`   |
| `unit`       | `str`                                           | ms, s, min or h from the start of the run, or timestamp for a date and time, which arrives when the run's records carry that time: logical_start_time is simulation time 0, whatever the initial_time. | `'s'`      |
| `fields`     | `Union[str, List[str]]`                         | The columns each entity carries, or all.                                                                                                                                                               | `'all'`    |
| `on_invalid` | `str`                                           | error refuses a row with no valid time; skip leaves it out with a warning.                                                                                                                             | `'error'`  |
| `attributes` | `Optional[Mapping[str, Any]]`                   | Attributes every entity starts with; a row's fields of the same name replace them.                                                                                                                     | `None`     |
| `key`        | `Any`                                           | The entity's key, in every form add_arrival takes: a function of the entity's counter, from 1, or a value as a blueprint's key reads it.                                                               | `None`     |
| `spawn`      | `Optional[str]`                                 | The task each row starts, as for add_arrival.                                                                                                                                                          | `None`     |
| `active`     | `bool`                                          | False starts the source off, as for add_arrival.                                                                                                                                                       | `True`     |
| `off_mode`   | `str`                                           | "drop" or "restart", as for add_arrival.                                                                                                                                                               | `'drop'`   |
| `vars`       | `Optional[Mapping[str, Any]]`                   | Named values read once for each entity, before its attributes and key, as for add_arrival.                                                                                                             | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                                                                                                        |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ValueError` | If unit or on_invalid is unknown, fields is a string other than all, or vars is refused as for add_arrival. A spawn naming no task is refused when the run starts. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `add_breakdown(resource, time_to_failure, repair, work='interrupt', clock='calendar')`

Makes a resource break down at random and be repaired.

After each repair, or the start, the time to the next failure is drawn. The `calendar` clock counts it from then, busy or idle. The `busy` clock counts it only while the resource holds a unit, so an idle resource does not wear out. While the resource is down its capacity is 0, its `current_cap` keeps the planned value, and a change to it during the repair sets the capacity the resource returns to. Each failure and repair publishes an event keyed by the resource's name, `{"status": "down", "reason": "failure"}` and `{"status": "repaired", ...}`. The two distributions are registry paths, `<sim_id>.breakdown.<resource>.time_to_failure` and `.repair`.

Parameters:

| Name              | Type                | Description                                                                                                                                                | Default       |
| ----------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `resource`        | `str`               | The resource's name.                                                                                                                                       | *required*    |
| `time_to_failure` | `Mapping[str, Any]` | A distribution, such as {"dist": "weibull", "shape": 1.5, "scale": "1 hour"}, whose time-valued parameters take seconds or a duration, as for add_service. | *required*    |
| `repair`          | `Mapping[str, Any]` | A distribution, such as {"dist": "exponential", "rate": 0.01}, with the same durations.                                                                    | *required*    |
| `work`            | `str`               | interrupt (the default) interrupts the tasks being served, which follow their on_interrupt; finish lets them finish.                                       | `'interrupt'` |
| `clock`           | `str`               | calendar (the default) or busy, the time the time to failure counts.                                                                                       | `'calendar'`  |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                  |
| ------------ | -------------------------------------------- |
| `ValueError` | If a distribution, work or clock is invalid. |

Source code in `src/desflow/core/context.py`

```python
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
        ValueError: If a distribution, `work` or `clock` is invalid.
    """
    self._holders.breakdowns_config[resource] = BreakdownConfig(
        time_to_failure=distribution_of(time_to_failure),
        repair=distribution_of(repair),
        work=work,
        clock=clock,
    )
    return self
```

##### `add_container(name, current_cap=None, max_cap=None, level=0.0, type=None, get_queue=None, put_queue=None, family=False, members=None)`

Registers a container: an amount that tasks take from and add to, such as stock.

`current_cap` is the capacity, the largest level, as for a resource. The level starts at `level` and has a live registry path of its own, `<sim_id>.containers.<name>.level`, so a scenario or an ingress message can set it during the run, such as a restock. Models that used `current_cap` as a level, such as wear, use `level` instead.

A container with no `current_cap` has no limit, and `max_cap` defaults to `current_cap`. Use `math.inf` or `float("inf")` for no limit.

Parameters:

| Name          | Type                            | Description                                                                                                                                                                                                                                                                                                                                                            | Default    |
| ------------- | ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `name`        | `str`                           | The internal identifier (e.g., 'stock').                                                                                                                                                                                                                                                                                                                               | *required* |
| `current_cap` | `Union[float, str, None]`       | The capacity at simulation start. None, the default, is no limit, or max_cap when that is given.                                                                                                                                                                                                                                                                       | `None`     |
| `max_cap`     | `Union[float, str, None]`       | The largest capacity a live update can set. None, the default, is current_cap.                                                                                                                                                                                                                                                                                         | `None`     |
| `level`       | `Union[float, str]`             | The amount held at simulation start. Defaults to 0.                                                                                                                                                                                                                                                                                                                    | `0.0`      |
| `type`        | `Optional[Type[ContainerBase]]` | A subclass of ContainerBase to build the container from, such as one that overrides \_do_get. Defaults to ContainerBase.                                                                                                                                                                                                                                               | `None`     |
| `get_queue`   | `Optional[Mapping[str, Any]]`   | A mapping {"rank": "<expression>", "ties": "fifo" or "lifo"} that ranks the waiting gets, lowest first, in place of first in, first out. The expression is read once as each get is made, in the scope of the task that makes it, and ties orders equal ranks. A get at the head that cannot be met holds back the gets behind it, as in SimPy. Not allowed with type. | `None`     |
| `put_queue`   | `Optional[Mapping[str, Any]]`   | As get_queue, for the waiting puts.                                                                                                                                                                                                                                                                                                                                    | `None`     |
| `family`      | `bool`                          | When True, the entry is a family of containers, as for add_resource. current_cap, max_cap and level may then be text, read once when the member is made.                                                                                                                                                                                                               | `False`    |
| `members`     | `Any`                           | With family, how many members exist when the run starts, as for add_resource.                                                                                                                                                                                                                                                                                          | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                                                                                                                                            |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ValueError` | If family is not set and members is given, a store already has this name, type is not a subclass of ContainerBase, or get_queue or put_queue is malformed, reads what it cannot or is given with type. |

Source code in `src/desflow/core/context.py`

```python
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
    self._families.claim("container", name)
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
```

##### `add_egress(provider, when=None, batch_size=None, flush_interval=None, envelope=True)`

Registers an asynchronous egress provider for data exfiltration.

Every registered provider receives every record, so attaching a stream sink and a lake sink writes the same dataset to both in one pass. Pass `when` to send only some records to a provider, which reads the same way as the `path_router` that `ParquetStorageEgress` already accepts.

Parameters:

| Name             | Type                                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Default    |
| ---------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| `provider`       | `Any`                                      | An initialized connector subclassing BaseEgress (e.g., KafkaEgress or ParquetStorageEgress).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | *required* |
| `when`           | `Union[Callable[[dict], bool], str, None]` | Optional predicate taking one record and returning True to send it to this provider. None sends every record, which is the usual case. "labels" makes it a labels sink: it receives the ground-truth records (env.publish_label) and nothing else, and no other sink receives them. "trace" makes it a trace sink: it receives the event trace that with_stats(trace=True) switches on, and nothing else, and no other sink receives it. A run with a trace sink and no with_stats(trace=True) is refused, as YAML refuses it at load.                                                                                                                                                                       | `None`     |
| `batch_size`     | `Optional[int]`                            | Records this provider buffers before flushing. None uses the context default from with_batching. A stream sink wants a small value to keep latency down; a file or lake sink wants a large one, because the batch is the file and, for Iceberg, the commit.                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `None`     |
| `flush_interval` | `Optional[float]`                          | Simulation seconds this provider waits before forcing a flush. None uses the context default. Ignored when factor is 0, because simulation time is then detached from the wall clock.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `None`     |
| `envelope`       | `bool`                                     | False makes this a plain-output provider. It receives no telemetry record and no queued or started record, and it writes each event's value without the record around it. A Kafka sink sends a null value as a tombstone, a message with the record's key and a null value, and sends any other value that is not a mapping as JSON. A Redis sink sends a value that is not a mapping as JSON, null included. A storage, Iceberg or PostgreSQL sink skips it, with one warning. Ground-truth records are not affected. Defaults to True. With the envelope on, a null value stays in the record, and a Kafka serializer whose schema has no null for value refuses it: the record is skipped with a warning. | `True`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                         |
| ------------ | --------------------------------------------------- |
| `ValueError` | If when is a string other than "labels" or "trace". |

Each provider has its own buffer and its own queue of batches. A record that several providers take is stored once, and stays in memory until the last provider that holds it has written it.

Example

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

Source code in `src/desflow/core/context.py`

````python
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
````

##### `add_ingress(provider)`

Registers an asynchronous ingress provider to listen for external mutations.

Parameters:

| Name       | Type  | Description                                                            | Default    |
| ---------- | ----- | ---------------------------------------------------------------------- | ---------- |
| `provider` | `Any` | An initialized connector subclassing BaseIngress (e.g., KafkaIngress). | *required* |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `add_initial_arrival(name, initial, *, spawn, attributes=None, key=None, gap=None, vars=None)`

Registers an arrival that has only `initial` entities and no source: `initial` entities exist when the run starts, in creation order, before any step runs, each keyed `<name>-n` unless `key` gives another, and each starts `spawn`. A blueprint's arrival with `initial` alone.

Parameters:

| Name         | Type                          | Description                                                                                                                                                               | Default    |
| ------------ | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `name`       | `str`                         | The arrival's name.                                                                                                                                                       | *required* |
| `initial`    | `int`                         | How many entities, 1 or more.                                                                                                                                             | *required* |
| `spawn`      | `str`                         | The task each entity starts.                                                                                                                                              | *required* |
| `attributes` | `Optional[Mapping[str, Any]]` | The attributes each entity starts with.                                                                                                                                   | `None`     |
| `key`        | `Any`                         | The entity's key, in every form add_arrival takes: a function of the entity's counter, from 1, or a value as a blueprint's key reads it.                                  | `None`     |
| `gap`        | `Any`                         | With a gap the arrival is not sourceless: it also starts an entity after each gap once the first initial exist, as add_arrival(name, gap=gap, initial=initial, ...) does. | `None`     |
| `vars`       | `Optional[Mapping[str, Any]]` | Named values read once for each entity, before its attributes and key, as for add_arrival.                                                                                | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                             |
| ------------ | --------------------------------------------------------------------------------------- |
| `ValueError` | If initial is not a whole number above 0, or gap or vars is refused as for add_arrival. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `add_process(func, **kwargs)`

Registers a generator function to start when the run starts.

This is the builder form of `spawn`, for a process that is not an arrival or telemetry loop, such as a drift engine or a scripted scenario. It is how a YAML blueprint starts the processes it references with `!python`.

Parameters:

| Name       | Type       | Description                                                      | Default    |
| ---------- | ---------- | ---------------------------------------------------------------- | ---------- |
| `func`     | `Callable` | A generator function. It is called as func(context, \*\*kwargs). | *required* |
| `**kwargs` | `Any`      | Extra keyword arguments passed to func.                          | `{}`       |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Example

```python
def drift(context, step):
    while True:
        yield context.env.timeout(1.0)
        ...

app.add_process(drift, step=0.5)
```

Source code in `src/desflow/core/context.py`

````python
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
````

##### `add_readings(interval, readings)`

Publishes expressions of the run as telemetry every `interval` seconds.

It is the builder form of a YAML `telemetry` entry with `readings`, and `telemetry_loop` with a function that publishes values, written as expressions. Each reading is evaluated over the run (`level.`, `users.`, `queue.`, `signal.`, `registry[...]`, `t` and the functions of the expression language, and no name of an entity or a task), at the start and then every `interval`. Each interval publishes one telemetry record for each reading, in the order given, named `<sim_id>.<reading>` as `publish` names one. The readings of one interval are all evaluated before the first is published.

Parameters:

| Name       | Type                | Description                                                                                  | Default    |
| ---------- | ------------------- | -------------------------------------------------------------------------------------------- | ---------- |
| `interval` | `float`             | Simulation seconds between two publications.                                                 | *required* |
| `readings` | `Mapping[str, str]` | A reading name mapped to an expression, such as {"waiting_per_packer": "queue.packers / 2"}. | *required* |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                                                                                                                                                                                                     |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If interval is not positive, readings is empty or not a mapping of text to text, or an expression is invalid or reads an entity. A name that is not in the model, or an expression that cannot be evaluated, is refused when the run is built or stops the run. |

Source code in `src/desflow/core/context.py`

```python
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
    if checked_seconds(interval, "the interval of readings") <= 0:
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

    self.telemetry_loop(interval)(report)
    return self
```

##### `add_resource(name, current_cap=None, max_cap=None, queue=DEFAULT_QUEUE_ORDER, preempt=False, queue_limit=None, type=None, family=False, members=None)`

Registers a discrete logical resource capable of processing tasks.

Resources act as the primary gating mechanism in the simulation, blocking concurrent processes to prevent physical collisions (e.g., restricting the number of slabs inside a mill stand).

Parameters:

| Name          | Type                             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Default               |
| ------------- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------- |
| `name`        | `str`                            | The internal identifier (e.g., 'mill_structural').                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | *required*            |
| `current_cap` | `Optional[Union[int, float]]`    | The active physical token capacity at simulation start, a whole number or float("inf") for a resource with no limit. Defaults to 1.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `None`                |
| `max_cap`     | `Optional[Union[int, float]]`    | The absolute physical maximum bound of tokens allowed. Defaults to current_cap.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `None`                |
| `queue`       | `Union[str, Mapping[str, Any]]`  | The order waiting tasks are served in: fifo (the default), lifo or priority, which serves the lowest task priority first and the oldest first among equal priorities. A mapping {"rank": "<expression>", "ties": "fifo" or "lifo"} serves the lowest value of the expression first, read once as each request is made, in the scope of the task that makes it. ties orders requests of one rank, the oldest first (the default) or the latest first, and does not change who preempts. A task, a seize or a request of such a resource gives no priority, because the rank is the request's priority.                                                                                                       | `DEFAULT_QUEUE_ORDER` |
| `preempt`     | `Union[bool, Mapping[str, Any]]` | When True, a task of a lower priority value takes a unit from a task of a higher one when every unit is busy, as SimPy's PreemptiveResource does; the task that loses it follows its on_interrupt. Needs queue="priority" or a queue rank. A mapping {"when": "<expression>", "victim": "<expression>"} changes the rule: victim gives each holder a value and the highest is the one when is tested on (the first granted among equals), and when says whether the request takes its unit. Both read request.rank, request.time, request.preempt, request.key, holder.rank, holder.time, holder.preempt, holder.key, holder.since and holder.attr.<name>, and t, users., queue., level. and registry[...]. | `False`               |
| `queue_limit` | `Optional[int]`                  | The most requests that may wait, or None for no limit. A task that finds that many waiting balks; a request made in the low-level API raises RuntimeError, as SimPy's SortedQueue does. 0 refuses every request. Not allowed with a type whose PutQueue is not the default. The limit is also the registry path \<sim_id>.resources.<name>.queue_limit: a change applies from the next request and removes nobody who waits.                                                                                                                                                                                                                                                                                | `None`                |
| `type`        | `Optional[Type[ResourceBase]]`   | A subclass of ResourceBase to build the resource from, such as one that overrides \_do_put. Defaults to DynamicResource.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `None`                |
| `family`      | `bool`                           | When True, the entry is a family: a set of resources, one for each key, made from this entry. A task names a member with {"of": name, "key": "<expression>"} where it names a resource, and a member is made at its first use. current_cap and max_cap may then be text, read once when the member is made.                                                                                                                                                                                                                                                                                                                                                                                                 | `False`               |
| `members`     | `Any`                            | With family, how many members exist when the run starts, with the keys 0 to n - 1: a whole number, an expression text, or a generator, read once when the run is built. None starts the family empty.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `None`                |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If family is not set and members is given, queue is not one of the three orders or a rank mapping, preempt is set without queue="priority" or a rank, a rank or a preempt rule is malformed or reads what it cannot, current_cap or max_cap is not a whole number or infinity, only max_cap is given and is below 1, type is not a subclass of ResourceBase, or queue_limit is negative or is set on a type with its own PutQueue. |

Source code in `src/desflow/core/context.py`

```python
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
    self._families.claim("resource", name)
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
```

##### `add_scenario(steps)`

Adds scenario steps: changes and records at set simulation times.

Each step is a mapping: `at`, seconds or a duration from the start of the run, and a `path` and `value` to set in the registry, a `record` to publish as a ground-truth label, an `interrupt` (`{"task": ..., "where": ..., "cause": ...}`), or more than one. A step applies before every other event at its time, so an arrival gap drawn at that moment uses a new rate; a step at or after `until` never applies. Steps at equal times apply in the order given.

Parameters:

| Name    | Type                      | Description | Default    |
| ------- | ------------------------- | ----------- | ---------- |
| `steps` | `List[Mapping[str, Any]]` | The steps.  | *required* |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                            |
| ------------ | -------------------------------------------------------------------------------------- |
| `ValueError` | If a step is malformed. A path the model does not have is refused when the run starts. |

Source code in `src/desflow/core/context.py`

```python
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
    if not self._scenario_added:
        self._scenario_added = True
        self.add_process(self._run_scenario)
    self._scenario_steps.extend(checked)
    return self
```

##### `add_service(name, dist, *, rate=None, mean=None, std=None, low=None, value=None, high=None, mode=None, shape=None, scale=None, values=None, weights=None, edges=None, counts=None, p=None)`

Registers a statistical distribution defining a process service duration.

The parameters are keyword-only, so their order never changes the meaning. Give only those `dist` takes; `desflow.models.distributions.DISTRIBUTIONS` lists each family's parameters and rules. A time-valued parameter (`mean`, `std`, `low`, `mode`, `high`, `scale`, `value`, and the `values` and `edges` of an empirical) takes seconds or a duration such as `"4 min"`, as a blueprint's does.

Parameters:

| Name      | Type                                    | Description                                                                                                                                   | Default    |
| --------- | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `name`    | `str`                                   | The internal identifier (e.g., 'packing').                                                                                                    | *required* |
| `dist`    | `str`                                   | The family: exponential, normal, lognormal, truncated_normal, poisson, geometric, constant, uniform, triangular, gamma, weibull or empirical. | *required* |
| `rate`    | `Optional[float]`                       | Events per second, for exponential.                                                                                                           | `None`     |
| `mean`    | `Optional[Union[float, str]]`           | The mean, for normal, lognormal, truncated_normal and poisson.                                                                                | `None`     |
| `std`     | `Optional[Union[float, str]]`           | The standard deviation, for the first three (0 when not given).                                                                               | `None`     |
| `low`     | `Optional[Union[float, str]]`           | The lowest value, for truncated_normal (0 when not given), uniform and triangular.                                                            | `None`     |
| `value`   | `Optional[Union[float, str]]`           | The time a constant distribution always gives.                                                                                                | `None`     |
| `high`    | `Optional[Union[float, str]]`           | The highest value, for uniform and triangular.                                                                                                | `None`     |
| `mode`    | `Optional[Union[float, str]]`           | The most likely value, for triangular.                                                                                                        | `None`     |
| `shape`   | `Optional[float]`                       | The shape, for gamma and weibull.                                                                                                             | `None`     |
| `scale`   | `Optional[Union[float, str]]`           | The scale, for gamma and weibull.                                                                                                             | `None`     |
| `values`  | `Optional[Sequence[Union[float, str]]]` | The values an empirical draws from, taken at load.                                                                                            | `None`     |
| `weights` | `Optional[List[float]]`                 | One weight per empirical value.                                                                                                               | `None`     |
| `edges`   | `Optional[Sequence[Union[float, str]]]` | The bin edges of an empirical histogram.                                                                                                      | `None`     |
| `counts`  | `Optional[List[float]]`                 | One count per empirical histogram bin.                                                                                                        | `None`     |
| `p`       | `Optional[float]`                       | The chance of success on each try, for geometric.                                                                                             | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                                    |
| ------------ | ---------------------------------------------------------------------------------------------- |
| `ValueError` | If the family is unknown, or a parameter is missing, out of range or not one the family takes. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `add_signal(name, once=True, on=None, family=False, members=None, defused=False)`

Declares a signal that tasks fire and wait for.

A task fires it with `signal=name` when it finishes, and fails it when the task fails; a task waits for it with `wait={"signal": name}`; a process reaches it with `context.signal(name)`.

Parameters:

| Name      | Type                                | Description                                                                                                                                                                                                                                                           | Default    |
| --------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `name`    | `str`                               | The signal's name.                                                                                                                                                                                                                                                    | *required* |
| `once`    | `bool`                              | When True, the default, the signal fires once, as SimPy's Event does: it does not re-arm, a wait after the firing resumes at once, and a second firing is an error under stop. When False it re-arms after each firing, and a wait after a firing waits for the next. | `True`     |
| `on`      | `Optional[List[Mapping[str, Any]]]` | Actions run when the signal fires, each a mapping with one of fire, spawn, interrupt or path, as a task's on takes them.                                                                                                                                              | `None`     |
| `family`  | `bool`                              | When True, the entry is a family of signals, one for each key, made from this entry. A task names a member with {"of": name, "key": "<expression>"} where it names a signal, and a member is made at its first use.                                                   | `False`    |
| `members` | `Any`                               | With family, how many members exist when the run starts, with the keys 0 to n - 1: a whole number, an expression text, or a generator, read once when the run is built. None starts the family empty.                                                                 | `None`     |
| `defused` | `bool`                              | When True, a failure of the signal that no waiter handles does not stop the run, as a callback's event.defused = True does. Without it, a one-shot signal's failure with no waiter stops the run under on_unhandled_failure: stop.                                    | `False`    |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                          |
| ------------ | -------------------------------------------------------------------- |
| `ValueError` | If members is given without family, or defused is not True or False. |

Source code in `src/desflow/core/context.py`

```python
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
    self._families.claim("signal", name)
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
```

##### `add_step(at, path=None, value=None, record=None, interrupt=None)`

Adds one scenario step; `add_scenario` describes the steps.

Parameters:

| Name        | Type                          | Description                                                         | Default    |
| ----------- | ----------------------------- | ------------------------------------------------------------------- | ---------- |
| `at`        | `Union[float, str]`           | Seconds, or a duration such as "10 min", from the start of the run. | *required* |
| `path`      | `Optional[str]`               | A registry path to set, with value.                                 | `None`     |
| `value`     | `Any`                         | The value to set.                                                   | `None`     |
| `record`    | `Optional[Mapping[str, Any]]` | A ground-truth record to publish.                                   | `None`     |
| `interrupt` | `Optional[Mapping[str, Any]]` | {"task": ..., "where": ..., "cause": ...} to interrupt.             | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `add_store(name, current_cap=None, max_cap=None, priority=False, items=None, type=None, get_queue=None, put_queue=None, family=False, members=None)`

Registers a store: a place that holds items, which tasks put and get.

A task puts its finished payload as an item and gets the next item, waiting when the store is full or empty. A `fifo` store, the default, returns the oldest item; a priority store returns the item whose task gave the lowest `put` priority first, and the oldest among equal priorities.

Parameters:

| Name          | Type                          | Description                                                                                                                                                                                                                                                                                                                                           | Default    |
| ------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `name`        | `str`                         | The internal identifier (e.g., 'parcels').                                                                                                                                                                                                                                                                                                            | *required* |
| `current_cap` | `Optional[float]`             | The number of items it can hold at simulation start, a whole number or infinity. None, the default, is no limit, or max_cap when that is given.                                                                                                                                                                                                       | `None`     |
| `max_cap`     | `Optional[float]`             | The largest capacity a live update can set. None, the default, is current_cap.                                                                                                                                                                                                                                                                        | `None`     |
| `priority`    | `bool`                        | When True, items are returned by the priority each task's put gives, lowest first, instead of first in, first out.                                                                                                                                                                                                                                    | `False`    |
| `items`       | `Optional[List[Any]]`         | The items it holds when the run starts, in the order a get returns them. A priority store takes (priority, item) pairs.                                                                                                                                                                                                                               | `None`     |
| `type`        | `Optional[Type[StoreBase]]`   | A subclass of StoreBase to build the store from, such as one that overrides \_do_get. Defaults to a simpy.FilterStore. It cannot be combined with priority.                                                                                                                                                                                           | `None`     |
| `get_queue`   | `Optional[Mapping[str, Any]]` | A mapping {"rank": "<expression>", "ties": "fifo" or "lifo"} that ranks the waiting gets, lowest first, in place of first in, first out. The expression is read once as each get is made, in the scope of the task that makes it, and ties orders equal ranks. It ranks waiters and not items: priority still ranks the items. Not allowed with type. | `None`     |
| `put_queue`   | `Optional[Mapping[str, Any]]` | As get_queue, for the waiting puts.                                                                                                                                                                                                                                                                                                                   | `None`     |
| `family`      | `bool`                        | When True, the entry is a family of stores, as for add_resource, each member made with the items given. current_cap and max_cap may then be text, read once when the member is made.                                                                                                                                                                  | `False`    |
| `members`     | `Any`                         | With family, how many members exist when the run starts, as for add_resource.                                                                                                                                                                                                                                                                         | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                                                                                                                                                                                                                                                                                                  |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ValueError` | If family is not set and members is given, a container already has this name, a capacity is not a whole number or infinity, there are more items than current_cap, a priority store's item is not a pair, type is not a subclass of StoreBase or is given with priority, or get_queue or put_queue is malformed, reads what it cannot or is given with type. |

Source code in `src/desflow/core/context.py`

```python
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
    self._families.claim("store", name)
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
    self._holders.declare_store(name, config, start, priority, type)
    self._families.declare("store", name, family, members, texts)
    return self
```

##### `add_variable(name, value)`

Registers a dynamic, untyped variable accessible via the central Registry.

Variables act as arbitrary state payloads that can be targeted by the Control Plane. For example, storing a JSON dictionary defining the velocity and frequency of a gradual wear injection loop.

Parameters:

| Name    | Type  | Description                                                  | Default    |
| ------- | ----- | ------------------------------------------------------------ | ---------- |
| `name`  | `str` | The internal identifier (e.g., 'velocity_structural').       | *required* |
| `value` | `Any` | The initial payload (supports primitives, lists, and dicts). | *required* |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `advance(until)`

Runs a started run up to `until`, then returns with the run still open.

Call it again to run the next segment, or `finish()` to run to the end. Segments give the same records as one run, because nothing is flushed at a segment boundary. Between segments the model can be read, changed with `context.env`, and stepped with `step()`. `advance()` never moves the wall-clock anchor, so a paced run that waits between segments is behind when it goes on; call `context.env.resume_pacing()` first to pace again from `now`.

The run stops at the earlier of `until` and its own end: `start()`'s `until`, the `stop` or the end of the `drain`. An `until` that is an event, such as `context.signal("gate").wait()`, runs to that event and returns its value; one that has been processed already returns at once.

Parameters:

| Name    | Type                       | Description                                                                                  | Default    |
| ------- | -------------------------- | -------------------------------------------------------------------------------------------- | ---------- |
| `until` | `Union[float, str, Event]` | A time in simulation seconds or as a duration such as "10 min", after now; or a SimPy event. | *required* |

Returns:

| Name  | Type  | Description                                                                                                                      |
| ----- | ----- | -------------------------------------------------------------------------------------------------------------------------------- |
| `Any` | `Any` | None when the run stops at a time, the event's value for an event, and the value run() returns when the run reaches its own end. |

Raises:

| Type           | Description                                                                                  |
| -------------- | -------------------------------------------------------------------------------------------- |
| `RuntimeError` | If the run has not started or is closed, or it has ended (close() it, or start() a new run). |
| `ValueError`   | If until is not after now, as SimPy refuses it.                                              |
| `SignalFailed` | If the signal that stop names fails, as for run().                                           |

Source code in `src/desflow/core/context.py`

```python
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
    if isinstance(until, (str, bool, float, int)):
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
```

##### `analyze(verify=False, *, until=None, seed=None, replications=None, target=0.85)`

Explains the model: its graph, and the queueing estimates that apply to it.

The graph and the analysis come from what the builder declared, before `start()`, and leave the context as it was: it is not started, and a run after it gives the same records as a run without it. `desflow explain` prints the same `Explanation`.

Parameters:

| Name           | Type                      | Description                                                                                                                                                                    | Default |
| -------------- | ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| `verify`       | `bool`                    | Also run the model unpaced and compare its statistics with the analysis. The run uses a new copy of the model, built again from the blueprint or the Python file it came from. | `False` |
| `until`        | `Union[float, str, None]` | The end of the verifying run, in seconds or as a duration; the blueprint's run.until when not given.                                                                           | `None`  |
| `seed`         | `Optional[int]`           | The verifying run's seed; the model's random_seed, or one drawn, when not given.                                                                                               | `None`  |
| `replications` | `Optional[int]`           | Verify with this many runs, 2 or more, and an interval for each measure, instead of one run.                                                                                   | `None`  |
| `target`       | `float`                   | The utilisation the tuning hints aim for, above 0 and at most 10; at 1 or above the hints are for deliberate overload.                                                         | `0.85`  |

Returns:

| Name          | Type          | Description                                                                                                                           |
| ------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `Explanation` | `Explanation` | The graph, the estimates per arrival, task, resource, store and container, the bottleneck, the hints, and with verify the comparison. |

Raises:

| Type         | Description                                                                                                                                                                                                |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If the model names a task or another part that is not added, as run() does; if target is out of range; or if verify is asked of a context built in code, which cannot be built again, or with no end time. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `arrival_loop(arrival_id)`

Registers an infinite generator loop to automatically start on boot.

The wrapped function will be injected with the `SimulationContext` instance at runtime, granting it access to `context.wait_for_arrival()`.

Parameters:

| Name         | Type  | Description                                                  | Default    |
| ------------ | ----- | ------------------------------------------------------------ | ---------- |
| `arrival_id` | `str` | The ID of the configured arrival rate to dynamically sample. | *required* |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `call(task, *args, entity=None, upstream=None, **kwargs)`

Starts a task as a child flow and returns its process.

The task runs with its own instance id, and so do the tasks its `next` leads to. The process ends when that flow ends: its value is the last task's finished payload, and it fails with `TaskFailed` when a task of the flow fails and no `on_fail` handles it. A flow started this way is not counted as the entity's flow finishing.

Parameters:

| Name       | Type                 | Description                                                | Default    |
| ---------- | -------------------- | ---------------------------------------------------------- | ---------- |
| `task`     | `str`                | The task's name.                                           | *required* |
| `*args`    | `Any`                | Positional arguments its function takes after the task id. | `()`       |
| `entity`   | `Optional[Entity]`   | The entity, such as the caller's own; a new one when None. | `None`     |
| `upstream` | `Optional[Upstream]` | What the task receives as upstream.                        | `None`     |
| `**kwargs` | `Any`                | Keyword arguments its function takes.                      | `{}`       |

Returns:

| Type      | Description                                                                      |
| --------- | -------------------------------------------------------------------------------- |
| `Process` | simpy.Process: The child flow. A failure nobody waits for does not stop the run. |

Raises:

| Type           | Description                      |
| -------------- | -------------------------------- |
| `RuntimeError` | If called before the run starts. |
| `ValueError`   | If no task has that name.        |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `close()`

Ends a started run: publishes the last statistics, flushes and stops the egress connectors, then stops the ingress connectors. Closing twice does nothing.

Source code in `src/desflow/core/context.py`

```python
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
```

##### `compile_parameters()`

Compiles the builder state into the `SimParameter` that `run()` registers.

Useful before the run, to see which registry paths a configuration creates or to compare two configurations.

Returns:

| Name           | Type           | Description                                                                                    |
| -------------- | -------------- | ---------------------------------------------------------------------------------------------- |
| `SimParameter` | `SimParameter` | The parameters, sharing their config objects with the builder; run() registers a copy of them. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `family_read_problem(expression)`

What is wrong with the way an expression reads families, or None: a family read without an index, or an index on a name that is no family.

Parameters:

| Name         | Type         | Description            | Default    |
| ------------ | ------------ | ---------------------- | ---------- |
| `expression` | `Expression` | The parsed expression. | *required* |

Returns:

| Type            | Description                                   |
| --------------- | --------------------------------------------- |
| `Optional[str]` | Optional\[str\]: The reason, naming the read. |

Source code in `src/desflow/core/context.py`

```python
def family_read_problem(self, expression: Expression) -> Optional[str]:
    """What is wrong with the way an expression reads families, or None: a family
    read without an index, or an index on a name that is no family.

    Args:
        expression: The parsed expression.

    Returns:
        Optional[str]: The reason, naming the read.
    """
    return family_read_problem(expression, self._families.by_name)
```

##### `finish()`

Runs a started run to its end, then closes it, as the rest of `run()` does.

The end is `until`, the `stop` or the end of the drain that `start()` set up. After one or more `advance()` or `step()` calls it runs on from where they stopped. When `advance()` already reached the end, it returns at once with the end's value: `None` for `until` or a drain, the stop's value for a stop, and a failed stop's exception raised again. When `advance()` or `step()` has brought the clock to `until` or past it and there is no `stop` or `drain`, it returns `None` at once, as a run that ended at `until` does. When `step()` brought the clock to exactly `until`, the records hold one run's records (which end with the final statistics) plus those of the events at `until` that `step()` processed, as in SimPy; a statistics reading due at `until` is not published again, and the close adds only the values that changed after it. After steps past `until`, the run closes where the steps stopped, with the final statistics at that time. `close()` runs even when the run stops with an error or Ctrl+C.

Returns:

| Name  | Type  | Description                                                |
| ----- | ----- | ---------------------------------------------------------- |
| `Any` | `Any` | The value of the event that stopped the run, as for run(). |

Raises:

| Type           | Description                                        |
| -------------- | -------------------------------------------------- |
| `RuntimeError` | If the run has not started.                        |
| `SignalFailed` | If the signal that stop names fails, as for run(). |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `from_yaml(path, preview=None, variables=None)`

Builds a context from a YAML blueprint file.

The blueprint is validated before anything is built, and every error names the file and line. The context is built through the same builder methods a Python script calls, so the result is the same simulation.

Parameters:

| Name        | Type                          | Description                                                                                                                                                                                                                                                                                                                                      | Default    |
| ----------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| `path`      | `Union[str, Path]`            | The blueprint file.                                                                                                                                                                                                                                                                                                                              | *required* |
| `preview`   | `Optional[Preview]`           | The preview desflow run --sample and --stdout make. Every egress entry is replaced by a JSON console on stdout and run.before is not called, because it prepares sinks the preview does not use. A sample also leaves out the ingress, and runs unpaced from logical_start_time, with go_live_at cleared, until it has printed that many events. | `None`     |
| `variables` | `Optional[Mapping[str, str]]` | Values for ${NAME} in the file and the files it includes, used before the environment, as desflow run --var NAME=VALUE gives them.                                                                                                                                                                                                               | `None`     |

Returns:

| Name                | Type                | Description                                                                                                                                                                        |
| ------------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SimulationContext` | `SimulationContext` | The built context. Its run() calls the blueprint's run.before functions first, and uses run.until, run.stop and run.drain for whichever of until, stop and drain it is not passed. |

Raises:

| Type             | Description                  |
| ---------------- | ---------------------------- |
| `BlueprintError` | If the blueprint is invalid. |

Example

```python
app = SimulationContext.from_yaml("examples/yaml/local.yaml")
app.run()
```

Source code in `src/desflow/core/context.py`

````python
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
````

##### `get_container(container_id)`

Retrieves an active `DynamicContainer`.

Parameters:

| Name           | Type  | Description           | Default    |
| -------------- | ----- | --------------------- | ---------- |
| `container_id` | `str` | The container's name. | *required* |

Returns:

| Name               | Type               | Description    |
| ------------------ | ------------------ | -------------- |
| `DynamicContainer` | `DynamicContainer` | The container. |

Raises:

| Type           | Description                                                                                 |
| -------------- | ------------------------------------------------------------------------------------------- |
| `RuntimeError` | If called during the Builder Phase.                                                         |
| `KeyError`     | If the model has no such holder, or the last start failed before it made the run's holders. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `get_resource(resource_id)`

Returns the resource a running model built: a `DynamicResource`, or the `ResourceBase` subclass its `type` names.

Parameters:

| Name          | Type  | Description                                    | Default    |
| ------------- | ----- | ---------------------------------------------- | ---------- |
| `resource_id` | `str` | The resource's name, as given to add_resource. | *required* |

Returns:

| Name           | Type           | Description                                  |
| -------------- | -------------- | -------------------------------------------- |
| `ResourceBase` | `ResourceBase` | The resource, for raw requests and readings. |

Raises:

| Type           | Description                                                                                 |
| -------------- | ------------------------------------------------------------------------------------------- |
| `RuntimeError` | If called before the run has started.                                                       |
| `KeyError`     | If the model has no such holder, or the last start failed before it made the run's holders. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `get_store(store_id)`

Retrieves an active `DynamicStore`.

Parameters:

| Name       | Type  | Description       | Default    |
| ---------- | ----- | ----------------- | ---------- |
| `store_id` | `str` | The store's name. | *required* |

Returns:

| Name           | Type           | Description |
| -------------- | -------------- | ----------- |
| `DynamicStore` | `DynamicStore` | The store.  |

Raises:

| Type           | Description                                                                                 |
| -------------- | ------------------------------------------------------------------------------------------- |
| `RuntimeError` | If called during the Builder Phase.                                                         |
| `KeyError`     | If the model has no such holder, or the last start failed before it made the run's holders. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `interrupt(task, where=None, cause=None)`

Interrupts every running instance of a task whose condition holds.

Each instance receives SimPy's `Interrupt` with `cause`, publishes `interrupted` and follows its task's `on_interrupt`. The caller's own instance is never interrupted, and an instance that has finished is not running.

Parameters:

| Name    | Type                           | Description                                                                                    | Default    |
| ------- | ------------------------------ | ---------------------------------------------------------------------------------------------- | ---------- |
| `task`  | `str`                          | The task's name.                                                                               | *required* |
| `where` | `Union[str, Expression, None]` | A condition read for each instance, such as "attr.order == 3"; None interrupts every instance. | `None`     |
| `cause` | `Any`                          | What the instances receive as the interrupt's cause, published as the record's cause.          | `None`     |

Returns:

| Name  | Type  | Description                          |
| ----- | ----- | ------------------------------------ |
| `int` | `int` | How many instances were interrupted. |

Raises:

| Type           | Description                      |
| -------------- | -------------------------------- |
| `RuntimeError` | If called before the run starts. |

Source code in `src/desflow/core/context.py`

```python
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
    for run in list(self._run.running.get(task, [])):
        process = run.flow.process
        if process is None or process is active or not process.is_alive:
            continue
        if condition is not None and not condition.evaluate(run.scope()):
            continue
        process.interrupt(cause)
        count += 1
    return count
```

##### `next_instance(task)`

Returns the next instance id of a task, counting from 0.

Each task has one counter, which `next` uses when it starts the task. An arrival loop that spawns a task by hand takes the id from here too, so a task started both by an arrival and by `next` never repeats an id.

Parameters:

| Name   | Type  | Description      | Default    |
| ------ | ----- | ---------------- | ---------- |
| `task` | `str` | The task's name. | *required* |

Returns:

| Name  | Type  | Description                  |
| ----- | ----- | ---------------------------- |
| `int` | `int` | The task's next instance id. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `note_values(task, *places)`

Notes what the values of a place read from other records, so the run keeps what they need: the rows of the tasks a `lookup` or `aggregate` names, and the last payload of a task whose values hold a `previous`. A task with a `lookup` does not start before each task it names has a row.

A blueprint calls this for the places that `task()` cannot see, such as a payload and an arrival's `attributes`. A builder script needs it only to search a place of its own.

Parameters:

| Name     | Type            | Description                                                                                                     | Default    |
| -------- | --------------- | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `task`   | `Optional[str]` | The task whose values the places are, or None for a place that belongs to no task.                              | *required* |
| `places` | `Any`           | Values: generators, mappings, lists or constants, or a Template (its payload, vars and vars_once are searched). | `()`       |

Raises:

| Type        | Description                                                 |
| ----------- | ----------------------------------------------------------- |
| `TypeError` | If a place is a set or an iterator, which are not searched. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `peek()`

Returns the time of the next model event of a started run, or infinity if none.

The events of desflow's own, such as a statistics reading or a flush timer, are not counted. It is infinity once a `row` with `on_end: stop` or the last entry of `run.segments` has ended the run, because `step()` runs no more.

Raises:

| Type           | Description                 |
| -------------- | --------------------------- |
| `RuntimeError` | If the run has not started. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `publish(metric_name, value)`

Publishes a telemetry value, named `<sim_id>.<metric_name>`, to the sinks.

Parameters:

| Name          | Type  | Description                                               | Default    |
| ------------- | ----- | --------------------------------------------------------- | ---------- |
| `metric_name` | `str` | The value's name, after the sim id, such as packers_free. | *required* |
| `value`       | `Any` | The value, which must be a JSON value.                    | *required* |

Raises:

| Type           | Description                           |
| -------------- | ------------------------------------- |
| `RuntimeError` | If called before the run has started. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `registry_paths()`

Returns the registry paths a run will have, with their starting values.

It builds the registry the way a run does, runs no model code and starts no connector or thread. A server's inspector uses it to list the paths a live change can set.

A capacity with no limit has the value `math.inf`, which JSON cannot hold, so a caller that writes JSON converts it.

Returns:

| Type             | Description                                                                                                                          |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `Dict[str, Any]` | Dict\[str, Any\]: Each path, such as Shop.resources.packers.current_cap, with its starting value, in the order a run registers them. |

Raises:

| Type         | Description                                                                  |
| ------------ | ---------------------------------------------------------------------------- |
| `ValueError` | If the registry refuses the model's parameters, as a run would at its start. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `run(until=None, stop=None, drain=None, on_unhandled_failure=None, segments=None, steps=None, sync=None)`

Compiles the defined infrastructure architecture and triggers the simulation loop.

`run()` is `start()`, then SimPy's `env.run` until the end, then `close()`, which runs even when the run stops with an error or Ctrl+C.

Parameters:

| Name                   | Type             | Description                                                                                                                                                                                                                                                                                                                                                                           | Default |
| ---------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `until`                | `Any`            | The absolute simulation termination time. Can be a numeric float (representing base seconds) or a human-readable string parsed by the utility module (e.g., "1 week", "8 hours"). If None, a context built by from_yaml uses the blueprint's run.until, and otherwise the simulation runs infinitely. An event due at exactly until does not run, as in SimPy.                        | `None`  |
| `stop`                 | `Any`            | Ends the run before until when it happens: {"signal": name}, or the signal's name, for the next firing of a signal; {"condition": "..."} for the first moment a condition holds; or a function of the context, called once the run is built, that returns a SimPy event, such as lambda c: c.signal("closing").wait(). When it happens at the same time as until, until ends the run. | `None`  |
| `drain`                | `Any`            | At until, stop the arrivals and keep running until no entity is in the model: True, or {"limit": duration} to stop at the latest that long after until. Needs until.                                                                                                                                                                                                                  | `None`  |
| `on_unhandled_failure` | `Optional[str]`  | "stop" or "continue", as run.on_unhandled_failure.                                                                                                                                                                                                                                                                                                                                    | `None`  |
| `segments`             | `Any`            | Runs the model in parts and publishes system.simulation.segment after each, as run.segments does: a list of mappings with the keys until, stop, look and sync of a blueprint's entry, or {"every": seconds} for equal parts up to until. A context built by from_yaml uses the blueprint's run.segments when this is None.                                                            | `None`  |
| `steps`                | `Optional[bool]` | True publishes system.simulation.step after each model event, as run.steps does. Not with segments, stop or drain.                                                                                                                                                                                                                                                                    | `None`  |
| `sync`                 | `Optional[bool]` | True calls env.sync() as the run starts, as run.sync does.                                                                                                                                                                                                                                                                                                                            | `None`  |

Returns:

| Name  | Type  | Description                                                                                                                                                                                                                                                    |
| ----- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Any` | `Any` | The value of the event that stopped the run, as SimPy's env.run returns it: a signal's value for a stop on a signal, True for a condition, and a stop function's event value. None when until or a drain ends the run, or when until and stop happen together. |

Raises:

| Type           | Description                                                                                                                                                |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError`   | If a task's next, priority, get or put names something the context does not define, or stop or drain is malformed.                                         |
| `SignalFailed` | If the signal that stop names fails. Any exception of a failed stop event is raised the same way. The run closes first, and the sinks receive the records. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `signal(name)`

Returns a declared signal, to fire, fail or wait for from a process.

Parameters:

| Name   | Type  | Description                 | Default    |
| ------ | ----- | --------------------------- | ---------- |
| `name` | `str` | A name add_signal declared. | *required* |

Returns:

| Name     | Type     | Description                                                                 |
| -------- | -------- | --------------------------------------------------------------------------- |
| `Signal` | `Signal` | The signal: fire(value), fail(reason), wait(), and its count, ok and value. |

Raises:

| Type           | Description                      |
| -------------- | -------------------------------- |
| `RuntimeError` | If called before the run starts. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `spawn(process, delay=None)`

Starts a generator as a SimPy process, such as a task instance.

This acts as the "escape hatch" for complex logic (like physical slab tracking) that outgrows the strict abstraction of the `@task` decorator.

Parameters:

| Name      | Type              | Description                                                                                                        | Default    |
| --------- | ----------------- | ------------------------------------------------------------------------------------------------------------------ | ---------- |
| `process` | `Any`             | A generator, such as pack_order(task_id, entity=entity).                                                           | *required* |
| `delay`   | `Optional[float]` | Seconds to wait before starting it, as SimPy's start_delayed. None starts it now; a value of 0 or less is refused. | `None`     |

Returns:

| Type      | Description                                                                                                                                                                                                                                           |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Process` | simpy.Process: The process. Yield it to wait until it ends; its value is what the generator returns, and is_alive, triggered, ok and value inspect it, as in SimPy. With delay the returned process is the helper whose value is the started process. |

Raises:

| Type         | Description                                           |
| ------------ | ----------------------------------------------------- |
| `ValueError` | If delay is 0 or less, as SimPy's start_delayed does. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `start(until=None, stop=None, drain=None, *, seed=None, overrides=None, stats=None, egress=True, ingress=True, run_settings=True, on_unhandled_failure=None, source=None, actor=None, reason=None, segments=None, steps=None, sync=None)`

Builds and starts everything `run()` does before the clock moves.

It checks the tasks, calls the blueprint's `run.before` functions, builds the environment, the sampler, the statistics, the parameters, the connectors, the resources, stores and containers, and starts every loop and process. Then `step()` processes one event at a time and `peek()` reads the time of the next, as SimPy's `env.step()` and `env.peek()` do; `close()` ends the run. A context built by `from_yaml` takes its `run` section's values.

Replications and experiments start each run here too, with the arguments after `drain`, and finish it with `finish()`, so a replication builds and ends exactly as `run()` does, `run.stop` and `run.drain` included.

Parameters:

| Name           | Type                          | Description                                                                                                                                                           | Default |
| -------------- | ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `until`        | `Any`                         | As for run(). With step() it is not enforced: peek() tells you when to stop.                                                                                          | `None`  |
| `stop`         | `Any`                         | As for run().                                                                                                                                                         | `None`  |
| `drain`        | `Any`                         | As for run().                                                                                                                                                         | `None`  |
| `seed`         | `Optional[int]`               | The run's seed, in place of random_seed.                                                                                                                              | `None`  |
| `overrides`    | `Optional[Mapping[str, Any]]` | Registry paths and the values they take before the clock starts, applied as a live update is, except that a value the registry would refuse or clamp raises.          | `None`  |
| `stats`        | `Optional[str]`               | The lowest statistics level the run keeps; a higher level set with with_stats stays.                                                                                  | `None`  |
| `egress`       | `bool`                        | False builds the run without its sinks and without the statistics timer, as a replication runs: nothing receives its records, and its statistics are read at the end. | `True`  |
| `ingress`      | `bool`                        | False builds the run without its ingress, so no connector listens for parameter changes.                                                                              | `True`  |
| `run_settings` | `bool`                        | False leaves out a blueprint's run.until, run.stop and run.drain, for a run whose clock never moves, such as the one that sends replications' summary to the sinks.   | `True`  |
| `source`       | `Optional[str]`               | How the ParamChange of each override describes it: where it came from, such as "user".                                                                                | `None`  |
| `actor`        | `Optional[str]`               | How the ParamChange of each override describes it: who made it.                                                                                                       | `None`  |
| `reason`       | `Optional[str]`               | How the ParamChange of each override describes it: why.                                                                                                               | `None`  |
| `segments`     | `Any`                         | As for run(). The segments and the steps run in finish(), and only when advance and step have not moved the run.                                                      | `None`  |
| `steps`        | `Optional[bool]`              | As for run().                                                                                                                                                         | `None`  |
| `sync`         | `Optional[bool]`              | As for run().                                                                                                                                                         | `None`  |

Returns:

| Name                         | Type                         | Description                        |
| ---------------------------- | ---------------------------- | ---------------------------------- |
| `DynamicRealtimeEnvironment` | `DynamicRealtimeEnvironment` | The environment, also context.env. |

Raises:

| Type         | Description                                                                                                                                                                                                                                                                                                                      |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | As for run(); or segments is malformed, a segment's look names a task that does not exist, or {"every": ...} has no until; or steps is used with segments, or steps or sync is not a bool; or stats is not a level, or an override's path does not exist or its value is refused, or a trace sink has no with_stats(trace=True). |

Source code in `src/desflow/core/context.py`

```python
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
    if until is not None and not isinstance(until, simpy.Event):
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
                + _not_added(look, self._tasks.specs, f"@app.task(name='{look}')")
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
```

##### `step()`

Processes the next model event of a started run, as SimPy's `env.step()` does.

The events of desflow's own that are due before it, such as a statistics reading or a flush timer, run in the same call and are not counted.

Raises:

| Type            | Description                                                                                                                                                          |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RuntimeError`  | If the run has not started, or it was ended by a row with on_end: stop or by the last entry of run.segments, as advance() raises (close() it, or start() a new run). |
| `EmptySchedule` | If no model event is left.                                                                                                                                           |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `stop_arrivals()`

Stops every arrival loop, so no new entity arrives; the entities already in the model carry on. A drain calls it at `until`.

Source code in `src/desflow/core/context.py`

```python
def stop_arrivals(self) -> None:
    """
    Stops every arrival loop, so no new entity arrives; the entities already in
    the model carry on. A drain calls it at `until`.
    """
    if self._run.arrival_run is not None:
        self._run.arrival_run.stop()
```

##### `subscribe(event)`

Asks to be interrupted when `event` happens, as SimPy's `subscribe_at`.

The calling process keeps running; when `event` fires it is interrupted one urgent step later with the cause `{"event": name, "value": value}`, where `name` is the process's name for a process and None for any other event.

Parameters:

| Name    | Type  | Description                          | Default    |
| ------- | ----- | ------------------------------------ | ---------- |
| `event` | `Any` | Any SimPy event, a process included. | *required* |

Raises:

| Type           | Description                                                                                                    |
| -------------- | -------------------------------------------------------------------------------------------------------------- |
| `RuntimeError` | If called outside a process, or event has already happened, as SimPy's subscribe_at refuses a processed event. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `task(service_id=None, resource_id=None, *, name=None, priority=None, get=None, put=None, next=None, set=None, patience=None, balk=None, wait=None, open=None, listen=None, cancel=None, seize=None, release=None, signal=None, fail=None, fail_reason=None, on_fail=None, on_renege=None, on_balk=None, service_time=None, key_field=None, key=None, keep=None, preempt=True, on_interrupt='resume', on_interrupt_next=None, on_interrupt_run=None, interrupt=None, resume=None, batch=None, spawn=None, subscribe=None, on=None, vars=None, vars_once=None, on_start=None, finish_vars=None, set_args=None, activate=None, deactivate=None, history=None)`

Transforms a standard Python function into an event-driven SimPy task.

This decorator completely abstracts the boilerplate of resource acquisition, clock synchronization, and standardized telemetry lineage.

Lifecycle Execution:

1. Checks `balk`; when it holds, emits `balked` and stops.
1. Emits a `queued` event, when the task waits for anything: its `wait`, a resource or a `get`.
1. Waits for its `wait`, then for a unit of each resource in `resource_id`, in the order listed and each in its queue order, then for its `get`; `patience` counts the time spent waiting for all three, and pauses while the task is served.
1. Emits a `started` event.
1. Executes the wrapped user function to extract custom payload data.
1. Samples the live `service_id` distribution, or reads `service_time`, and yields the timeout.
1. Adds its `put`, waiting for room, and checks `fail`.
1. Emits its finished event with the user's payload, or `failed`.
1. Fires its `signal` (or fails it), sends its `interrupt` action, applies `set` to the entity's attributes and starts the `next` task, or `on_fail`.

Every event carries the entity's key as its record key, and the task's name and instance id as the record's `task` and `instance` fields.

A task with a service and no resource is a timed step: no `queued` event, so its wait is 0. A task with no service, resource or get takes no time: when spawned it emits the payload at once (after its `put`, if it has one), with no `queued` or `started` event.

Call the task as `task(task_id, ...)`. Pass `entity=` with the entity `wait_for_arrival` returned; without one the task starts an entity of its own, keyed by its name and a counter. The function receives `entity`, `upstream` (what the previous task handed on, or None) and `item` (what a store `get` took) by keyword, each only when it declares that parameter, so existing functions keep working. A task started by `next` is called with its own instance id and the same other arguments as the task before it. A function that declares `wait` receives the wait's branches, each a mapping of `done`, `ok` and `value`.

Parameters:

| Name                | Type                                                              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Default    |
| ------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `service_id`        | `Optional[str]`                                                   | The ID of the configured distribution dictating the execution time.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `None`     |
| `service_time`      | `Union[float, str, Callable[[Entity], float], Any]`               | The execution time of this task, in place of service_id: seconds, a duration such as "30 s", an expression such as "attr.fuel / 2" read when the service starts, or a function of the entity, such as lambda entity: entity.attributes["fuel"] / 2, or a typed generator from desflow.values.generator, drawn when the service starts. A text or a generator may read the task's vars, which then get their value at that moment. A result below 0 fails the run. A task takes service_id or service_time, not both, and records no path_id. An interrupt under restart reads it again.                                                                                                                   | `None`     |
| `resource_id`       | `Union[str, Mapping[str, Any], List[Any], None]`                  | The resource the task holds while it works, or a list of them, acquired in the order listed while the earlier ones are held. A member of a family is {"of": family, "key": expression} or {"of": family, "lowest": expression}.                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `None`     |
| `name`              | `Optional[str]`                                                   | The task's name in records, statistics and next rules. Defaults to the function's name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `None`     |
| `priority`          | `Union[float, str, Callable[[Any, Dict[str, Any]], float], None]` | For a resource whose queue order is priority, lowest first: a number, an expression such as "attr.priority", or a function of the task id and the entity's attributes. It is read when the task joins the queue, before its payload exists. A task with no priority ranks 0, as a SimPy PriorityRequest does.                                                                                                                                                                                                                                                                                                                                                                                             | `None`     |
| `get`               | `Union[Transfer, Mapping[str, Any], None]`                        | What the task takes before it starts: {"container": name, "amount": ...} or {"store": name}. The amount is a number, an expression such as "payload.quantity", or a function of the task id and the upstream payload. Defaults to an amount of 1.                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `None`     |
| `put`               | `Union[Transfer, Mapping[str, Any], None]`                        | What the task adds when it finishes, in the same form; a store receives the finished payload.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `None`     |
| `next`              | `NextSpec`                                                        | The task that follows: a task name, {"when": [{"condition": ..., "task": ...}], "default": ...}, {"weights": {name: weight}}, or a function of the Upstream returning a name or None.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `None`     |
| `set`               | `Optional[Mapping[str, Any]]`                                     | Attribute values applied to the entity when the task finishes.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `None`     |
| `patience`          | `Union[float, str, Callable[[Any, Dict[str, Any]], float], Any]`  | How long the task may wait, for its wait, its resources and its get together, before it leaves the queue and publishes reneged. It pauses while the task is served, so a preempted task that queues again goes on with the time it has left. Seconds, a duration such as "30 s", an expression such as "attr.patience", a function of the task id and the entity's attributes, or a typed generator from desflow.values.generator.                                                                                                                                                                                                                                                                        | `None`     |
| `balk`              | `Optional[str]`                                                   | A condition, such as "t >= 3300", checked before the task joins the queue; when it holds, the task publishes balked and never requests the resource.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `None`     |
| `wait`              | `Optional[Mapping[str, Any]]`                                     | What the task waits for before it queues for its resource: one branch, {"signal": name}, {"task": name}, {"timeout": seconds} or {"condition": "..."}, or {"any": {...}} or {"all": {...}} of named branches. After it, wait.<branch>.done, .ok and .value are read in next, fail and amounts. A branch {"handle": name} waits for an event open made.                                                                                                                                                                                                                                                                                                                                                    | `None`     |
| `open`              | `Optional[Mapping[str, Any]]`                                     | Events the task makes at its step, before its wait, and does not wait for: a mapping of names to one wait branch each (timeout, request, get, put, task or signal), or any or all of named timeout, task, signal and handle branches. A task branch may carry args. A name is read as handle.<name>.triggered, .processed, .ok, .value and .alive, and by {"handle": name} in a wait, cancel, release and interrupt, in this task, the tasks after it in the flow and the child flows it starts. A request keeps its place in the line and publishes queued when it is made and started when it is granted. Opening a name again replaces it.                                                             | `None`     |
| `listen`            | `Optional[Mapping[str, Any]]`                                     | Callbacks added to an event when the task starts, after its open and before its wait: {"signal": name, "on": [...]} (a member of a family is {"of": family, "key": expression}), or {"handle": name, "on": [...]} for an opened event, with "index" to pick one element of an opened each group. The on entries are those of a task's on. They run once, on the next firing of the event, after the callbacks and waiters added before. Listening to a signal whose firing was processed already, or to an event that was processed, fails the task, as event.callbacks.append raises.                                                                                                                    | `None`     |
| `cancel`            | `Union[str, Mapping[str, Any], None]`                             | The name of an opened request, get or put, taken out of its line ("slot" or {"handle": "slot"}). A granted one is left as it is. A second cancel of one that left the line fails the run.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `None`     |
| `seize`             | `Union[str, Mapping[str, Any], None]`                             | A unit of a resource the flow keeps across later tasks until a release: a resource name, or {"resource": ..., "priority": ..., "preempt": ..., "patience": ...}. The task asks at its step and goes on once the unit is granted. priority and preempt default to the task's; priority is a number, an expression or a function of the task id and the entity's attributes, and preempt is a bool or an expression. A preemption takes the unit, and the task asks again under the same name.                                                                                                                                                                                                              | `None`     |
| `release`           | `Union[str, Mapping[str, Any], None]`                             | Gives back units when the task finishes: a resource name (the oldest unit of it the flow holds, and nothing if it holds none), {"handle": name} (the unit of that opened request), or {"resource": ..., "task": ..., "where": ...} (the units of that resource that instances of that task hold, whichever flow they are in).                                                                                                                                                                                                                                                                                                                                                                             | `None`     |
| `signal`            | `Union[str, Mapping[str, Any], None]`                             | A signal, added with add_signal, that the task fires with its finished payload, or fails with its reason when it fails. A member of a family is {"of": family, "key": expression}.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `None`     |
| `fail`              | `Union[str, Callable[[Any, Any], bool], None]`                    | A condition checked after the service, such as "payload.defect == 1", where payload is the task's own payload, or a function of the task id and the payload. When it holds, the task publishes failed instead of its payload.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `None`     |
| `fail_reason`       | `Any`                                                             | The reason a failure publishes: text, or a generator from desflow.values.generator read in the step the failure happens (it may read payload.). Defaults to the fail condition's text. A reason that is None is the condition's text.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `None`     |
| `on_fail`           | `Optional[str]`                                                   | The task a failed instance goes to. Without it, a failure ends the entity's flow. A function that raises fails the task the same way, with the error as the reason, and goes to on_fail; so on_fail needs no fail condition.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | `None`     |
| `on_renege`         | `Optional[str]`                                                   | The task an instance goes to after it reneges, in place of the end of its flow. It still publishes reneged and counts as reneged. Needs a patience.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `None`     |
| `on_balk`           | `Optional[str]`                                                   | The task an instance goes to after it balks, in place of the end of its flow, whether its balk condition holds or the waiting line of its resource is full. It still publishes balked and counts as balked. Needs a balk or a resource.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `None`     |
| `key_field`         | `Optional[str]`                                                   | A field of the task's payload whose value becomes the key of the finished event, and so the Kafka message key, in place of the entity's key. The queued and started records keep the entity's key. A payload without the field fails the run.                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `None`     |
| `key`               | `Any`                                                             | One record key for every record of an instance, in place of the entity's key: a constant, or a typed generator from desflow.values.generator, drawn at the instance's first record. It gives an integer or text, and records carry its text. It cannot be given with key_field.                                                                                                                                                                                                                                                                                                                                                                                                                           | `None`     |
| `keep`              | `Optional[List[str]]`                                             | Resources in resource_id whose units the task hands to the next task of the flow instead of releasing them; that task releases them once it publishes started.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `None`     |
| `preempt`           | `Union[bool, str]`                                                | On a resource added with preempt=True, whether the task's requests may take a unit from a task of a higher priority value. It is a bool, a boolean word of a blueprint ("t", "off") or an expression read for each request, such as "attr.urgent".                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `True`     |
| `on_interrupt`      | `str`                                                             | What an interrupt or a preemption does: resume (the default) goes on with the units it still holds, asks again for the one it lost or waited for, and serves the remaining time; restart does the same with a new service time; abandon gives back every unit and ends the flow, or goes to on_interrupt_next. A plain interrupt leaves a granted unit with the task; a preemption or a breakdown takes it.                                                                                                                                                                                                                                                                                               | `'resume'` |
| `on_interrupt_next` | `Optional[str]`                                                   | The task an abandoned instance goes to.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `None`     |
| `on_interrupt_run`  | `Optional[str]`                                                   | The task that runs as a child flow of the same entity after each interrupt or preemption, under resume or restart, before the task goes on with the time left (a new time under restart). The child reads cause.<name>. Not with abandon, a resource or a request in the wait, because it is for work that holds no unit of its own. The child runs in the task's process, so an interrupt that arrives while it runs reaches the child at its next wait and follows the child's own on_interrupt.                                                                                                                                                                                                        | `None`     |
| `resume`            | `Optional[Mapping[str, Any]]`                                     | An action applied when the task finishes: {"task": name, "where": condition, "value": value, "limit": n} wakes the instances of that task suspended in wait={"suspend": True}, in the order they suspended, up to limit (all when it is None), whose where holds. Each reads value as wait.suspend.value.                                                                                                                                                                                                                                                                                                                                                                                                 | `None`     |
| `batch`             | `Optional[Mapping[str, Any]]`                                     | Members that wait together: {"size": n, "timeout": seconds, "close_when": condition, "name": text}. The batch closes when size members have joined (a number, or an expression read when the first member joins), timeout after its first member, or when close_when holds as a member joins; it reads batch.size and batch.age. The resource and service run once for the batch, and each member then publishes its own payload and follows its own next. name defaults to the task's name, and tasks with the same name share one open batch, so they must have the same size, timeout, close_when, resource and service; a run with different ones is refused. It takes no get, seize, open or cancel. | `None`     |
| `interrupt`         | `Optional[Mapping[str, Any]]`                                     | An action applied when the task finishes: {"task": name, "where": condition, "cause": value} interrupts every running instance of that task whose condition holds, and {"handle": name, "cause": value} interrupts the child flow that name was opened as; that is a run failure if the flow has ended.                                                                                                                                                                                                                                                                                                                                                                                                   | `None`     |
| `vars`              | `Optional[Mapping[str, Any]]`                                     | Named values, each a generator mapping such as {"gen": "uniform", "low": 2, "high": 200} or a constant, evaluated in order after the service, before the payload. A later var and the payload read an earlier one with {"gen": "var", "name": ...}, and an expression after the service (put, fail, next) reads it as a bare name. With on_start they are evaluated when the task starts.                                                                                                                                                                                                                                                                                                                 | `None`     |
| `vars_once`         | `Optional[Mapping[str, Any]]`                                     | Named values made once for each entity, at its first event, and the same in every later instance of this task for the entity. A wait timeout value can read them.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `None`     |
| `on_start`          | `Optional[Mapping[str, Any]]`                                     | A mapping of values published as a record when the task starts, after started.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `None`     |
| `finish_vars`       | `Optional[Mapping[str, Any]]`                                     | Named values evaluated after vars and before the payload, which can read the vars.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `None`     |
| `activate`          | `Union[str, List[str], None]`                                     | The arrival, or arrivals, whose source the task turns on when it finishes, after set_args.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `None`     |
| `deactivate`        | `Union[str, List[str], None]`                                     | The same, turning the source off. A source already in that state is left as it is.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `None`     |
| `set_args`          | `Optional[Mapping[str, Any]]`                                     | Names written in this process's own locals after set, read in expressions as arg.<name>. A child flow has its own names, and the next chain of a flow shares them.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `None`     |
| `history`           | `Optional[int]`                                                   | How many finished payloads the task keeps for lookup and aggregate (a whole number of 1 or more). A task keeps none unless this is given, or a generator in a vars, vars_once, on_start, finish_vars, set or set_args value of a task, or in a blueprint, names it. A Template payload is not searched, so a builder task that a Template looks up asks for its rows here.                                                                                                                                                                                                                                                                                                                                | `None`     |

Raises:

| Type         | Description                                                                                                                                                     |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If a resource is given without a service, a priority without a resource, the name is taken, key and key_field are both given, or get, put or next is malformed. |

Returns:

| Name       | Type       | Description                                                        |
| ---------- | ---------- | ------------------------------------------------------------------ |
| `Callable` | `Callable` | A decorator that wraps the target function into a SimPy generator. |

Example

```python
@app.task(service_id="packing", resource_id="packers", next="ship")
def pack_order(task_id: int, context):
    return {"status": "packed"}
```

Source code in `src/desflow/core/context.py`

````python
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
        self._task_functions[task_name] = wrapper
        self._tasks.specs[task_name] = spec
        return wrapper

    return decorator
````

##### `telemetry_loop(interval)`

Registers an infinite background daemon loop for periodic data publishing.

This is primarily used for observing and extracting continuous state (like wear containers) at strict intervals.

Parameters:

| Name       | Type    | Description                                                  | Default    |
| ---------- | ------- | ------------------------------------------------------------ | ---------- |
| `interval` | `float` | The simulation time (in seconds) to wait between iterations. | *required* |

Raises:

| Type         | Description                 |
| ------------ | --------------------------- |
| `ValueError` | If interval is not above 0. |

Source code in `src/desflow/core/context.py`

```python
def telemetry_loop(self, interval: float) -> Callable:
    """
    Registers an infinite background daemon loop for periodic data publishing.

    This is primarily used for observing and extracting continuous state
    (like wear containers) at strict intervals.

    Args:
        interval: The simulation time (in seconds) to wait between iterations.

    Raises:
        ValueError: If `interval` is not above 0.
    """
    if checked_seconds(interval, "telemetry_loop interval") <= 0:
        raise ValueError(f"telemetry_loop interval must be above 0, got {interval}")

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
```

##### `wait_for_arrival(arrival_id)`

Returns the wait for the next arrival: a timeout from the live distribution, or for a replayed arrival the wait for its next row.

The timeout's value is the new `Entity`, so an arrival loop writes `entity = yield context.wait_for_arrival("orders")` and passes `entity=entity` to the task it spawns.

Parameters:

| Name         | Type  | Description         | Default    |
| ------------ | ----- | ------------------- | ---------- |
| `arrival_id` | `str` | The arrival's name. | *required* |

Returns:

| Type    | Description                                                                                                                                                                       |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Event` | simpy.Event: A timeout for the gap to the next arrival, or the wait for the next replayed row, which succeeds with the new Entity; after a replay's last row the wait never ends. |

Raises:

| Type           | Description                                         |
| -------------- | --------------------------------------------------- |
| `RuntimeError` | If called during the Builder Phase (before .run()). |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `with_batching(batch_size, flush_interval, max_queued_batches=2000, drain_stall_seconds=30.0)`

Configures the internal egress buffering strategy to mitigate I/O lock contention.

`batch_size` and `flush_interval` are defaults. A provider that names either on `add_egress` uses its own instead, which is how a stream sink and a lake sink share one run.

Parameters:

| Name                  | Type    | Description                                                                                                                                                                                                         | Default    |
| --------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `batch_size`          | `int`   | The maximum number of standard events to hold in memory before forcing an asynchronous flush to the egress thread.                                                                                                  | *required* |
| `flush_interval`      | `float` | The maximum simulation time (in seconds) to wait before forcefully flushing the buffer, regardless of capacity.                                                                                                     | *required* |
| `max_queued_batches`  | `int`   | Upper bound on batches waiting for the egress threads. A full queue blocks the producer, so a sink that cannot keep up slows the simulation instead of building a backlog that teardown would then have to discard. | `2000`     |
| `drain_stall_seconds` | `float` | How long teardown keeps waiting once the queue has stopped shrinking. While it keeps draining the wait is unbounded, so a slow sink finishes rather than losing its tail.                                           | `30.0`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `with_stats(level='basic', interval=DEFAULT_STATS_INTERVAL, on_change=False, trace=False)`

Switches on the built-in statistics, published as telemetry.

Each level publishes what the levels before it publish, and more: `minimal` the readings of every resource, store and container right now; `basic` finished counts and time-weighted averages; `typical` the mean and maximum wait and service time of each task; `all` their 50th, 95th and 99th percentiles. Names look like `<sim_id>.stats.tasks.<task>.wait_mean`.

Parameters:

| Name        | Type    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Default                  |
| ----------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------ |
| `level`     | `str`   | none, minimal, basic, typical or all.                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `'basic'`                |
| `interval`  | `float` | Simulation seconds between publications; they are also published once at the end of the run, except the values unchanged since a reading at that same time. Defaults to 10.                                                                                                                                                                                                                                                                                                                | `DEFAULT_STATS_INTERVAL` |
| `on_change` | `bool`  | True also publishes a resource's readings, the names it publishes at each interval, right after each request and each release call on it, as a patched request and release would record them. A grant that a release event makes later, and a cancel, publish none of their own. Needs a level above none.                                                                                                                                                                                 | `False`                  |
| `trace`     | `bool`  | True publishes one record of the kind trace for each model event the environment processes, before its callbacks run, with the value {"priority", "event_id", "event"}: the event's priority, its event_id (SimPy's sequence number of the event, which desflow's own events also advance) and its name, one of Initialize, Process, Timeout, Condition, Interruption, Request, Release, Get, Put, Suspension or Event. It reaches only the sinks added with when="trace". Needs no level. | `False`                  |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                                     |
| ------------ | ----------------------------------------------------------------------------------------------- |
| `ValueError` | If the level is unknown, the interval is not positive, or on_change is set with the level none. |

Source code in `src/desflow/core/context.py`

```python
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
    if checked_seconds(interval, "stats interval") <= 0:
        raise ValueError(f"stats interval must be greater than 0, got {interval}")
    if on_change and level == "none":
        raise ValueError(
            "on_change publishes the statistics at each request and release, so "
            "it needs a level above none"
        )
    self._sinks.stats_level = level
    self._sinks.stats_interval = interval
    self._sinks.stats_on_change = bool(on_change)
    self._sinks.stats_trace = bool(trace)
    return self
```

## Task Flow

A task's flow is built from the classes below. The signals and failures here are shared between tasks.

### `desflow.Signal(env, name='signal', once=True, defused=False)`

A named event that fires once, or many times with `once=False`, each firing waking every waiter.

Attributes:

| Name      | Type             | Description                                                                                |
| --------- | ---------------- | ------------------------------------------------------------------------------------------ |
| `env`     |                  | The environment.                                                                           |
| `name`    | `str`            | The signal's name.                                                                         |
| `count`   | `int`            | How many times it has fired or failed.                                                     |
| `ok`      | `Optional[bool]` | Whether the last firing succeeded; None before the first.                                  |
| `value`   | `Any`            | The value of the last firing, or the failure reason.                                       |
| `defused` | `bool`           | Whether every failure of the signal is defused, as a callback's event.defused = True does. |

Creates a signal that has not fired yet.

Parameters:

| Name      | Type          | Description                                                                                                                                                                                                                                               | Default    |
| --------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `env`     | `Environment` | The environment.                                                                                                                                                                                                                                          | *required* |
| `name`    | `str`         | Its name, as a blueprint's signals and signal.<name> use it.                                                                                                                                                                                              | `'signal'` |
| `once`    | `bool`        | When True, the default, the signal fires once, as SimPy's Event does: it does not re-arm, so a wait after the firing resumes at once, and a second firing is refused. When False it is replaced by a fresh event after each firing, so it can fire again. | `True`     |
| `defused` | `bool`        | When True, a failure with no waiter does not stop the run, as a callback that sets event.defused = True does for each failure.                                                                                                                            | `False`    |

Source code in `src/desflow/core/signals.py`

```python
def __init__(
    self,
    env: Environment,
    name: str = "signal",
    once: bool = True,
    defused: bool = False,
):
    """
    Creates a signal that has not fired yet.

    Args:
        env: The environment.
        name: Its name, as a blueprint's `signals` and `signal.<name>` use it.
        once: When True, the default, the signal fires once, as SimPy's `Event`
            does: it does not re-arm, so a wait after the firing resumes at once,
            and a second firing is refused. When False it is replaced by a fresh
            event after each firing, so it can fire again.
        defused: When True, a failure with no waiter does not stop the run, as
            a callback that sets `event.defused = True` does for each failure.
    """
    self.env = env
    self.name = name
    self.once = once
    self.defused = defused
    self.count = 0
    self.ok: Optional[bool] = None
    self.value: Any = None
    # Callbacks run when the signal fires, copied onto each new pending event
    self.callbacks: List[Callable[[Any], None]] = []
    self._event = env.event()
    # The event of the latest firing, for `processed`
    self._fired: Optional[Event] = None
```

#### Attributes

##### `processed`

bool: Whether the latest firing's event has been processed, as SimPy's `event.processed`; False before any firing. It is `signal.<name>.processed` in an expression.

#### Methods:

##### `attach(callbacks)`

Keeps `callbacks` as the signal's `on:` callbacks and adds them to the current pending event, so they run when the signal next fires, before any waiter, as SimPy runs a callback added before the waiters.

Parameters:

| Name        | Type                          | Description                                                                                            | Default    |
| ----------- | ----------------------------- | ------------------------------------------------------------------------------------------------------ | ---------- |
| `callbacks` | `List[Callable[[Any], None]]` | Functions called with the fired event, copied onto each new pending event so they run on every firing. | *required* |

Source code in `src/desflow/core/signals.py`

```python
def attach(self, callbacks: List[Callable[[Any], None]]) -> None:
    """Keeps `callbacks` as the signal's `on:` callbacks and adds them to the
    current pending event, so they run when the signal next fires, before any
    waiter, as SimPy runs a callback added before the waiters.

    Args:
        callbacks: Functions called with the fired event, copied onto each new
            pending event so they run on every firing.
    """
    self.callbacks = list(callbacks)
    for callback in self.callbacks:
        self._event.callbacks.append(callback)
```

##### `fail(reason, urgent=False, after=0.0, priority=None, defused=False)`

Fails the signal: every waiter receives `SignalFailed` with the reason.

A failure of a signal that re-arms is always defused, and a failure that nobody waits for does not stop the run then. A one-shot signal's failure with no waiter is left to stop the run under `on_unhandled_failure: stop`, as SimPy's failed event does, unless `defused` is given or the signal is defused.

Parameters:

| Name       | Type            | Description                                                                                         | Default    |
| ---------- | --------------- | --------------------------------------------------------------------------------------------------- | ---------- |
| `reason`   | `Any`           | Why it failed, such as a task's failure reason.                                                     | *required* |
| `urgent`   | `bool`          | As for fire().                                                                                      | `False`    |
| `after`    | `float`         | As for fire().                                                                                      | `0.0`      |
| `priority` | `Optional[int]` | As for fire().                                                                                      | `None`     |
| `defused`  | `bool`          | When True, this failure does not stop the run if nobody waits for it, as event.defused = True does. | `False`    |

Raises:

| Type         | Description    |
| ------------ | -------------- |
| `ValueError` | As for fire(). |
| `TypeError`  | As for fire(). |

Source code in `src/desflow/core/signals.py`

```python
def fail(
    self,
    reason: Any,
    urgent: bool = False,
    after: float = 0.0,
    priority: Optional[int] = None,
    defused: bool = False,
) -> None:
    """
    Fails the signal: every waiter receives `SignalFailed` with the reason.

    A failure of a signal that re-arms is always defused, and a failure that nobody
    waits for does not stop the run then. A one-shot signal's failure with no waiter
    is left to stop the run under `on_unhandled_failure: stop`, as SimPy's failed
    event does, unless `defused` is given or the signal is defused.

    Args:
        reason: Why it failed, such as a task's failure reason.
        urgent: As for `fire()`.
        after: As for `fire()`.
        priority: As for `fire()`.
        defused: When True, this failure does not stop the run if nobody waits for
            it, as `event.defused = True` does.

    Raises:
        ValueError: As for `fire()`.
        TypeError: As for `fire()`.
    """
    priority = event_priority(priority, urgent)
    self._check_timing(urgent, after, priority)
    if self.once and self.count >= 1:
        if getattr(self.env, "on_unhandled_failure", "stop") == "stop":
            raise RuntimeError(f"signal '{self.name}' is one-shot and has fired")
        logger.warning(
            "signal '%s' is one-shot and has already fired; the second failure "
            "is dropped",
            self.name,
        )
        return
    self.count += 1
    self.ok, self.value = False, reason
    if self.once:
        event = self._event
    else:
        event, self._event = self._event, self.env.event()
        for callback in self.callbacks:
            self._event.callbacks.append(callback)
    self._fired = event
    # A re-arming signal's failure is always defused, so a firing nobody waits
    # for is not an error. A one-shot signal's failure is defused only under
    # `continue`; under `stop` it is left to stop the run, as SimPy's event does.
    if (
        not self.once
        or defused
        or self.defused
        or getattr(self.env, "on_unhandled_failure", "stop") == "continue"
    ):
        event.defused = True
    if urgent or after or priority not in (None, NORMAL):
        self._schedule(
            False, SignalFailed(self.name, reason), urgent, after, priority
        )
    else:
        event.fail(SignalFailed(self.name, reason))
    self._notify()
```

##### `fire(value=None, urgent=False, after=0.0, priority=None)`

Fires the signal: every waiter resumes with `value`.

Parameters:

| Name       | Type            | Description                                                                                                                                                                                                                                                                                             | Default |
| ---------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `value`    | `Any`           | What the waiters receive, such as a task's finished payload.                                                                                                                                                                                                                                            | `None`  |
| `urgent`   | `bool`          | When True the waiters resume before the other events at their time, as SimPy's URGENT priority orders them. Needs a one-shot signal.                                                                                                                                                                    | `False` |
| `after`    | `float`         | Simulation seconds from now at which the waiters resume. The signal counts as fired now, so a second firing is refused. Needs a one-shot signal.                                                                                                                                                        | `0.0`   |
| `priority` | `Optional[int]` | A whole number, 1 when left out, the priority the firing is scheduled with: an event at one time runs by priority, lowest first, and then in the order it was scheduled, as env.schedule(event, priority=...) orders it. 0 is what urgent gives. Needs a one-shot signal, and is not given with urgent. | `None`  |

Raises:

| Type         | Description                                                                                                                              |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If urgent, after or a priority other than 1 is given to a signal that re-arms, after is negative, or urgent and priority are both given. |
| `TypeError`  | If priority is not a whole number.                                                                                                       |

Source code in `src/desflow/core/signals.py`

```python
def fire(
    self,
    value: Any = None,
    urgent: bool = False,
    after: float = 0.0,
    priority: Optional[int] = None,
) -> None:
    """
    Fires the signal: every waiter resumes with `value`.

    Args:
        value: What the waiters receive, such as a task's finished payload.
        urgent: When True the waiters resume before the other events at their
            time, as SimPy's `URGENT` priority orders them. Needs a one-shot
            signal.
        after: Simulation seconds from now at which the waiters resume. The
            signal counts as fired now, so a second firing is refused. Needs a
            one-shot signal.
        priority: A whole number, 1 when left out, the priority the firing is
            scheduled with: an event at one time runs by priority, lowest first,
            and then in the order it was scheduled, as
            `env.schedule(event, priority=...)` orders it. 0 is what `urgent`
            gives. Needs a one-shot signal, and is not given with `urgent`.

    Raises:
        ValueError: If `urgent`, `after` or a `priority` other than 1 is given to a
            signal that re-arms, `after` is negative, or `urgent` and `priority` are
            both given.
        TypeError: If `priority` is not a whole number.
    """
    priority = event_priority(priority, urgent)
    self._check_timing(urgent, after, priority)
    if self.once and self.count >= 1:
        # A one-shot signal fires once. A second firing is SimPy's RuntimeError
        # under `stop`; under `continue` it is dropped with a log line.
        if getattr(self.env, "on_unhandled_failure", "stop") == "stop":
            raise RuntimeError(f"signal '{self.name}' is one-shot and has fired")
        logger.warning(
            "signal '%s' is one-shot and has already fired; the second firing "
            "is dropped",
            self.name,
        )
        return
    self.count += 1
    self.ok, self.value = True, value
    if self.once:
        self._fired = self._event
        # Keep the triggered event, so a later wait resumes at once.
        if urgent or after or priority not in (None, NORMAL):
            self._schedule(True, value, urgent, after, priority)
        else:
            self._event.succeed(value)
    else:
        event, self._event = self._event, self.env.event()
        self._fired = event
        for callback in self.callbacks:
            self._event.callbacks.append(callback)
        event.succeed(value)
    self._notify()
```

##### `wait()`

Returns the event of the next firing, for a process to yield.

Returns:

| Type    | Description                                                                                            |
| ------- | ------------------------------------------------------------------------------------------------------ |
| `Event` | simpy.Event: Succeeds with the firing's value, or fails with SignalFailed, when the signal next fires. |

Source code in `src/desflow/core/signals.py`

```python
def wait(self) -> Event:
    """
    Returns the event of the next firing, for a process to yield.

    Returns:
        simpy.Event: Succeeds with the firing's value, or fails with
            `SignalFailed`, when the signal next fires.
    """
    return self._event
```

### `desflow.SignalFailed(signal, reason)`

Bases: `Exception`

The exception a waiter of a failed signal receives.

Attributes:

| Name     | Type  | Description                                                  |
| -------- | ----- | ------------------------------------------------------------ |
| `signal` | `str` | The signal's name.                                           |
| `reason` | `Any` | What Signal.fail was given, such as a task's failure reason. |

Source code in `src/desflow/core/signals.py`

```python
def __init__(self, signal: str, reason: Any):
    # SimPy copies a failure as type(error)(*error.args), so args are the
    # constructor's arguments
    super().__init__(signal, reason)
    self.signal = signal
    self.reason = reason
```

### `desflow.TaskFailed(task, reason)`

Bases: `Exception`

A task instance failed, and no `on_fail` handled it.

A child flow, started by a `wait` branch or `context.call`, fails with it, so the task waiting for the flow fails too.

Attributes:

| Name     | Type  | Description                                                                 |
| -------- | ----- | --------------------------------------------------------------------------- |
| `task`   | `str` | The task that failed.                                                       |
| `reason` | `Any` | Why: its fail condition, its fail_reason, or the error its function raised. |

Source code in `src/desflow/core/task.py`

```python
def __init__(self, task: str, reason: Any):
    # SimPy copies a failure as type(error)(*error.args), so args are the
    # constructor's arguments
    super().__init__(task, reason)
    self.task = task
    self.reason = reason
```

### `desflow.NO_RECORD = _NoRecord()`

What a task function returns to finish with no record: the Builder form of a blueprint's `payload: null`.

Return `desflow.NO_RECORD` from the function a task wraps. The task still runs its service, `set`, signal and `next`, and publishes no finished event.

Example

```python
from desflow import NO_RECORD


@app.task(name="quiet", service_time=1)
def quiet(task_id):
    return NO_RECORD
```

### `desflow.Entity(key, arrival, attributes=dict(), held=dict(), entered=False, instances=None, spawned_by=None, ends_pending=0, ending=False, last_ended=None, loop=None)`

One thing moving through the model, such as an order, a slab or a session.

Each arrival starts a new entity. Its key is the record key of every event it causes, in every task it passes through.

Attributes:

| Name         | Type             | Description                                                                                                                                                                                                        |
| ------------ | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `key`        | `str`            | The record key: the arrival name and a counter, such as orders-1, unless the arrival has a key function.                                                                                                           |
| `arrival`    | `str`            | The arrival that started it, which names its flow in the statistics.                                                                                                                                               |
| `attributes` | `Dict[str, Any]` | State kept across tasks: set by the arrival's attributes and by each task's set, and read in conditions as attr.<name>. A payload includes them only when it asks for them.                                        |
| `held`       | `Dict[str, Any]` | Resource units a task kept for the next task with keep, by resource name; the next task gives them back once it starts, or the flow does when it ends without one, as after a renege or a failure with no on_fail. |

#### Attributes

##### `left`

True once `RunState.entity_left` has run: the flow has ended and nothing is pending.

### `desflow.Upstream(task_id, key, attributes, payload=None)`

What a finished task hands the task routed after it.

Attributes:

| Name         | Type             | Description                                                         |
| ------------ | ---------------- | ------------------------------------------------------------------- |
| `task_id`    | `Any`            | The instance id of the finished task.                               |
| `key`        | `str`            | The entity's key.                                                   |
| `attributes` | `Dict[str, Any]` | The entity's attributes, after the finished task's set.             |
| `payload`    | `Any`            | The finished task's payload, read in conditions as payload.<field>. |

#### Attributes

##### `fields`

The payload when it is a mapping, and an empty mapping otherwise.

### `desflow.Router(rules, rng=None)`

Chooses the task that follows a finished one.

The rules take one of these forms, in every API:

- a task name: always that task;
- `{"when": [{"condition": "...", "task": "..."}, ...], "default": "..."}`: the first rule whose condition holds wins, and `default`, if given, catches the rest; with no default and no rule holding, the flow ends;
- `{"weights": {"ship": 9, "rework": 1}}`: a task drawn with these weights from the routing task's own random sequence, so seeded runs repeat;
- a function taking the `Upstream` and returning a task name or None;
- None: the flow ends.

A route back to an earlier task (rework) is allowed.

Attributes:

| Name    | Type                  | Description                                                                                                                                         |
| ------- | --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tasks` | `Optional[Set[str]]`  | Every task the rules can name, so a loader can check they exist; None for a function.                                                               |
| `rng`   | `Optional[Generator]` | The generator choose draws weights from. A run of a SimulationContext draws from a generator of its own for each task and leaves this one as it is. |

Checks the rules and parses their conditions.

Parameters:

| Name    | Type                  | Description                                                                                          | Default    |
| ------- | --------------------- | ---------------------------------------------------------------------------------------------------- | ---------- |
| `rules` | `NextSpec`            | The rules, in one of the forms above.                                                                | *required* |
| `rng`   | `Optional[Generator]` | The generator weights draw from, such as sampler.generator("route.<task>"). Needed only for weights. | `None`     |

Raises:

| Type         | Description                                           |
| ------------ | ----------------------------------------------------- |
| `ValueError` | If the rules are malformed or a condition is refused. |

Source code in `src/desflow/core/flow.py`

```python
def __init__(self, rules: NextSpec, rng: Optional[np.random.Generator] = None):
    """
    Checks the rules and parses their conditions.

    Args:
        rules: The rules, in one of the forms above.
        rng: The generator weights draw from, such as
            `sampler.generator("route.<task>")`. Needed only for weights.

    Raises:
        ValueError: If the rules are malformed or a condition is refused.
    """
    self._function: Optional[Callable[[Upstream], Optional[str]]] = None
    self._when: List[Tuple[Expression, str]] = []
    self._default: Optional[str] = None
    self._names: List[str] = []
    # Where the rules are written, which a weight that fails when read names
    self.where = ""
    self._cumulative: Optional[np.ndarray] = None
    # Weights with a text in them: each weight is a number or an `Expression`,
    # read when a task finishes
    self._weights: Optional[List[Union[int, float, Expression]]] = None
    self.rng = rng
    self.tasks: Optional[Set[str]] = set()

    if rules is None:
        return
    if isinstance(rules, str):
        self._default = rules
        self.tasks = {rules}
        return
    if callable(rules):
        self._function = rules
        self.tasks = None
        return
    if not isinstance(rules, Mapping):
        raise ValueError(
            f"next must be a task name, rules or a function, got {rules!r}"
        )
    unknown = set(rules) - {"when", "default", "weights"}
    if unknown:
        raise ValueError(f"next does not take {', '.join(sorted(unknown))}")
    if "weights" in rules:
        if "when" in rules or "default" in rules:
            raise ValueError(
                "next takes weights, or when rules with a default, not both"
            )
        self._set_weights(rules["weights"])
        return
    for rule in rules.get("when") or []:
        if not isinstance(rule, Mapping) or set(rule) != {"condition", "task"}:
            raise ValueError(
                f"each when rule needs a condition and a task, got {rule!r}"
            )
        self._when.append(
            (Expression(rule["condition"], names=ANY_NAME), str(rule["task"]))
        )
    self._default = rules.get("default")
    if not self._when and self._default is None:
        raise ValueError("next needs when rules, a default or weights")
    self.tasks = {task for _, task in self._when} | (
        {self._default} if self._default is not None else set()
    )
```

#### Attributes

##### `computed`

True when a weight is an expression, so the shares are known only when a task finishes.

##### `conditions`

The parsed conditions, in rule order.

##### `probabilities`

The share of finished instances each next task receives, where the rules fix it: 1.0 for a task name, the normalised weights for weights, and an empty mapping when the flow ends. None for when rules or a function, whose split depends on the run.

##### `weighted`

True when the rules are weights, which draw from `rng`.

#### Methods:

##### `choose(upstream, env=None, levels=None, item=None, scope=None)`

Returns the next task's name, or None when the flow ends.

Parameters:

| Name       | Type                          | Description                                                                                                 | Default    |
| ---------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------- | ---------- |
| `upstream` | `Upstream`                    | What the finished task hands on.                                                                            | *required* |
| `env`      | `Any`                         | The environment, for registry[...] and t in conditions.                                                     | `None`     |
| `levels`   | `Optional[Mapping[str, Any]]` | Stores and containers by name, for level.<name>.                                                            | `None`     |
| `item`     | `Any`                         | The item the finished task's store get took, for item.<field>.                                              | `None`     |
| `scope`    | `Optional[Scope]`             | What the conditions read, when the caller has built it; it replaces the one built from the arguments above. | `None`     |

Returns:

| Type            | Description                              |
| --------------- | ---------------------------------------- |
| `Optional[str]` | Optional\[str\]: The next task, or None. |

Raises:

| Type         | Description                                  |
| ------------ | -------------------------------------------- |
| `ValueError` | If a condition reads a name that is missing. |

Source code in `src/desflow/core/flow.py`

```python
def choose(
    self,
    upstream: Upstream,
    env: Any = None,
    levels: Optional[Mapping[str, Any]] = None,
    item: Any = None,
    scope: Optional[Scope] = None,
) -> Optional[str]:
    """
    Returns the next task's name, or None when the flow ends.

    Args:
        upstream: What the finished task hands on.
        env: The environment, for `registry[...]` and `t` in conditions.
        levels: Stores and containers by name, for `level.<name>`.
        item: The item the finished task's store get took, for `item.<field>`.
        scope: What the conditions read, when the caller has built it; it
            replaces the one built from the arguments above.

    Returns:
        Optional[str]: The next task, or None.

    Raises:
        ValueError: If a condition reads a name that is missing.
    """
    return next_task(self, upstream, self.rng, env, levels, item, scope)
```

### `desflow.Transfer(container=None, store=None, amount=1.0, priority=0.0, filter=None, wait=True, item=None)`

What a task takes from, or adds to, a store or a container.

A container transfer moves an amount: a number, an expression such as `payload.quantity`, or in Python a function of the task id and the upstream payload. A store transfer moves one item: a `get` takes the next item, and a `put` adds the task's finished payload.

A put with `wait=False` is made and not waited for: the task goes on at once, as a SimPy process that calls `store.put(item)` without `yield`. The put stays in the queue, completes in first in, first out order whenever there is room, and is never cancelled by an interrupt, a patience or an abandon. It publishes no record, a drain does not wait for it, and one still queued when the run ends is dropped.

Attributes:

| Name        | Type            | Description                                                                 |
| ----------- | --------------- | --------------------------------------------------------------------------- |
| `container` | `Optional[str]` | The container's name, or a Reference to a member of a family of containers. |
| `store`     | `Optional[str]` | The store's name, or a Reference.                                           |
| `amount`    |                 | The amount, for a container.                                                |
| `wait`      | `bool`          | False for a put the task does not wait for.                                 |

Checks the transfer.

Parameters:

| Name        | Type                                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Default |
| ----------- | ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `container` | `Optional[str]`                           | The container's name, or a reference {"of": family, "key": expression}.                                                                                                                                                                                                                                                                                                                                                                                                                                        | `None`  |
| `store`     | `Optional[str]`                           | The store's name, instead of a container, or a reference.                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `None`  |
| `amount`    | `Amount`                                  | For a container, a number, an expression or a function.                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `1.0`   |
| `priority`  | `Any`                                     | For a put into a priority store, any value SimPy can order: a number, a string, a tuple, or an expression or function that returns one, lowest served first. A string is an expression. It is kept as given, so "10" from a function compares as text and priorities of types that cannot be compared raise TypeError at the put. Ignored by a fifo store.                                                                                                                                                     | `0.0`   |
| `filter`    | `Union[str, Callable[[Any], bool], None]` | For a store get, which item it takes: a condition such as "item.size == attr.size", where item.<field> reads the item offered, or a function of the item returning True. A function with more than one parameter, one of them after the first named entity, also receives the entity that makes the get, by that name: lambda item, entity: .... A function with one parameter is called with the item, whatever the parameter is named. The get takes the oldest item that matches and waits while none does. | `None`  |
| `wait`      | `bool`                                    | False for a put the task does not wait for (see above). Defaults to True.                                                                                                                                                                                                                                                                                                                                                                                                                                      | `True`  |
| `item`      | `Any`                                     | For a store put, the item to put: a constant, text, or a {gen: ...} generator read when the put is made. None puts the task's finished payload, as a SimPy put(x) of the task's own value does.                                                                                                                                                                                                                                                                                                                | `None`  |

Raises:

| Type         | Description                                                                                                                                                                                                               |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If not exactly one of container and store is given, an amount is given for a store, a priority is given for a container, the amount is malformed, the priority is an expression that is malformed, or wait is not a bool. |

Source code in `src/desflow/core/flow.py`

```python
def __init__(
    self,
    container: Optional[str] = None,
    store: Optional[str] = None,
    amount: Amount = 1.0,
    priority: Any = 0.0,
    filter: Union[str, Callable[[Any], bool], None] = None,
    wait: bool = True,
    item: Any = None,
):
    """
    Checks the transfer.

    Args:
        container: The container's name, or a reference `{"of": family, "key":
            expression}`.
        store: The store's name, instead of a container, or a reference.
        amount: For a container, a number, an expression or a function.
        priority: For a put into a priority store, any value SimPy can order: a
            number, a string, a tuple, or an expression or function that
            returns one, lowest served first. A string is an expression. It is
            kept as given, so `"10"` from a function compares as text and
            priorities of types that cannot be compared raise `TypeError` at
            the put. Ignored by a `fifo` store.
        filter: For a store get, which item it takes: a condition such as
            `"item.size == attr.size"`, where `item.<field>` reads the item
            offered, or a function of the item returning True. A function with
            more than one parameter, one of them after the first named `entity`,
            also receives the entity that makes the get, by that name: `lambda
            item, entity: ...`. A function with one parameter is called with the
            item, whatever the parameter is named. The get takes the oldest item that matches
            and waits while none does.
        wait: False for a put the task does not wait for (see above). Defaults
            to True.
        item: For a store put, the item to put: a constant, text, or a `{gen:
            ...}` generator read when the put is made. None puts the task's
            finished payload, as a SimPy `put(x)` of the task's own value does.

    Raises:
        ValueError: If not exactly one of container and store is given, an
            amount is given for a store, a priority is given for a
            container, the amount is malformed, the priority is an expression
            that is malformed, or `wait` is not a bool.
    """
    if not isinstance(wait, bool):
        raise ValueError(f"wait must be true or false, got {wait!r}")
    self.wait = wait
    if container is not None:
        container = name_of(container, "container")
    if store is not None:
        store = name_of(store, "store")
    if (container is None) == (store is None):
        raise ValueError("a get or put names one container or one store")
    if store is not None and amount != 1.0:
        raise ValueError("a store moves one item, so it takes no amount")
    if container is not None and priority != 0.0:
        raise ValueError("priority orders a store's items, so it needs a store")
    if container is not None and filter is not None:
        raise ValueError("a filter picks a store's item, so it needs a store")
    if item is not None and store is None:
        raise ValueError("item is the value a store put adds, so it needs a store")
    self.container = container
    self.store = store
    self.amount = amount
    self.priority = priority
    self.item = item
    self._expression: Optional[Expression] = None
    if isinstance(amount, str):
        self._expression = Expression(amount, names=ANY_NAME)
    elif not callable(amount) and (
        isinstance(amount, bool) or not isinstance(amount, (int, float))
    ):
        raise ValueError(
            f"amount must be a number or an expression, got {amount!r}"
        )
    if (
        container is not None
        and isinstance(amount, (int, float))
        and not isinstance(amount, bool)
    ):
        _checked_amount(amount, f"the amount of container '{container}'")
    self.filter = filter
    self._filter_expression: Optional[Expression] = None
    self._filter_takes_entity = False
    if isinstance(filter, str):
        self._filter_expression = Expression(filter)
    elif filter is not None and not callable(filter):
        raise ValueError(
            f"filter must be a condition or a function, got {filter!r}"
        )
    elif filter is not None:
        self._filter_takes_entity = _names_entity(filter)
    self._priority_expression: Optional[Expression] = None
    if isinstance(priority, str):
        self._priority_expression = Expression(priority, names=ANY_NAME)
```

#### Attributes

##### `expressions`

Every expression the transfer holds, so a loader can check its names.

##### `reference_expressions`

The parsed `key` or `lowest` of a reference the transfer names.

##### `target`

The name of the store or container.

#### Methods:

##### `amount_for(task_id, upstream, scope)`

Returns the amount for one task instance.

Parameters:

| Name       | Type                 | Description                       | Default    |
| ---------- | -------------------- | --------------------------------- | ---------- |
| `task_id`  | `Any`                | The instance id.                  | *required* |
| `upstream` | `Optional[Upstream]` | What the previous task handed on. | *required* |
| `scope`    | `Scope`              | What an expression's names read.  | *required* |

Returns:

| Type                | Description                                                      |
| ------------------- | ---------------------------------------------------------------- |
| `Union[int, float]` | Union\[int, float\]: The amount, as written when it is a number. |

Source code in `src/desflow/core/flow.py`

```python
def amount_for(
    self, task_id: Any, upstream: Optional[Upstream], scope: Scope
) -> Union[int, float]:
    """
    Returns the amount for one task instance.

    Args:
        task_id: The instance id.
        upstream: What the previous task handed on.
        scope: What an expression's names read.

    Returns:
        Union[int, float]: The amount, as written when it is a number.
    """
    if self._expression is not None:
        return self._read_amount(self._expression.evaluate(scope))
    if callable(self.amount):
        return self._read_amount(self.amount(task_id, payload_of(upstream)))
    return as_given(self.amount)
```

##### `filter_for(scope, entity=None)`

Returns the function a store get offers each item to, or None for any item.

Parameters:

| Name     | Type    | Description                                                                                          | Default    |
| -------- | ------- | ---------------------------------------------------------------------------------------------------- | ---------- |
| `scope`  | `Scope` | What the condition's other names read, taken when the get is made; item is set to each item offered. | *required* |
| `entity` | `Any`   | The entity that makes the get, passed by name to a function that names a parameter entity.           | `None`     |

Returns:

| Type                              | Description                                                             |
| --------------------------------- | ----------------------------------------------------------------------- |
| `Optional[Callable[[Any], bool]]` | Optional\[Callable\[[Any], bool\]\]: True for an item the get may take. |

Source code in `src/desflow/core/flow.py`

```python
def filter_for(
    self, scope: Scope, entity: Any = None
) -> Optional[Callable[[Any], bool]]:
    """
    Returns the function a store get offers each item to, or None for any item.

    Args:
        scope: What the condition's other names read, taken when the get is made;
            `item` is set to each item offered.
        entity: The entity that makes the get, passed by name to a function that
            names a parameter `entity`.

    Returns:
        Optional[Callable[[Any], bool]]: True for an item the get may take.
    """
    if self._filter_expression is not None:
        condition = self._filter_expression

        def matches(item: Any) -> bool:
            scope.item = item
            return bool(condition.evaluate(scope))

        return matches
    if self._filter_takes_entity:
        function = self.filter

        def matches_entity(item: Any) -> bool:
            return bool(function(item, entity=entity))  # type: ignore[misc,call-arg,operator]

        return matches_entity
    return None if isinstance(self.filter, str) else self.filter
```

##### `of(spec)`

Returns a transfer from a mapping such as `{"container": "stock", "amount": 2}`.

Parameters:

| Name   | Type                                 | Description                                                              | Default    |
| ------ | ------------------------------------ | ------------------------------------------------------------------------ | ---------- |
| `spec` | `Union[Transfer, Mapping[str, Any]]` | A mapping with container or store and an optional amount, or a Transfer. | *required* |

Returns:

| Name       | Type       | Description   |
| ---------- | ---------- | ------------- |
| `Transfer` | `Transfer` | The transfer. |

Raises:

| Type         | Description                                    |
| ------------ | ---------------------------------------------- |
| `ValueError` | If the mapping has other keys or is malformed. |

Source code in `src/desflow/core/flow.py`

```python
@classmethod
def of(cls, spec: Union["Transfer", Mapping[str, Any]]) -> "Transfer":
    """
    Returns a transfer from a mapping such as `{"container": "stock", "amount": 2}`.

    Args:
        spec: A mapping with `container` or `store` and an optional `amount`, or
            a `Transfer`.

    Returns:
        Transfer: The transfer.

    Raises:
        ValueError: If the mapping has other keys or is malformed.
    """
    if isinstance(spec, Transfer):
        return spec
    if not isinstance(spec, Mapping):
        raise ValueError(f"a get or put is a mapping, got {spec!r}")
    unknown = set(spec) - {
        "container",
        "store",
        "amount",
        "priority",
        "filter",
        "wait",
        "item",
    }
    if unknown:
        raise ValueError(f"a get or put does not take {', '.join(sorted(unknown))}")
    return cls(**spec)
```

##### `priority_for(task_id, upstream, scope)`

Returns the priority for one put into a priority store, lowest first.

Parameters:

| Name       | Type                 | Description                       | Default    |
| ---------- | -------------------- | --------------------------------- | ---------- |
| `task_id`  | `Any`                | The instance id.                  | *required* |
| `upstream` | `Optional[Upstream]` | What the previous task handed on. | *required* |
| `scope`    | `Scope`              | What an expression's names read.  | *required* |

Returns:

| Name  | Type  | Description                                                        |
| ----- | ----- | ------------------------------------------------------------------ |
| `Any` | `Any` | The priority as it was given or computed, not changed to a number. |

Source code in `src/desflow/core/flow.py`

```python
def priority_for(
    self, task_id: Any, upstream: Optional[Upstream], scope: Scope
) -> Any:
    """
    Returns the priority for one put into a priority store, lowest first.

    Args:
        task_id: The instance id.
        upstream: What the previous task handed on.
        scope: What an expression's names read.

    Returns:
        Any: The priority as it was given or computed, not changed to a number.
    """
    if self._priority_expression is not None:
        return self._priority_expression.evaluate(scope)
    if callable(self.priority):
        return self.priority(task_id, payload_of(upstream))
    return self.priority
```

### `desflow.Expression(text, records=False, names=None, rule=False, batch=False, member=False)`

A parsed and checked expression, ready to evaluate against a `Scope`.

Attributes:

| Name         | Type                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ------------ | --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `text`       | `str`                 | The expression as written.                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `references` | `Dict[str, Set[str]]` | The names it reads, by source: payload, attr, level, users, queue, putters, getters, item, wait, signal, handle, process, registry and name (the bare names), so a loader can check them in advance. count and items hold the store names the two functions read, and store_item the item.<field> names inside them, which read each item the store holds and not the item of a get. has holds the attribute names has(attr.<name>) tests, which need not exist. |
| `where`      | `str`                 | The place of the expression in a blueprint, such as model.yaml:12: tasks.t.fail, which an evaluation error names first; empty when it has none.                                                                                                                                                                                                                                                                                                                  |

Parses and checks an expression.

Parameters:

| Name      | Type                       | Description                                                                                                                                                             | Default    |
| --------- | -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `text`    | `str`                      | The expression, such as level.stock == 0 or payload.total > 100.                                                                                                        | *required* |
| `records` | `bool`                     | True for an egress when, whose expression reads the record (record.stream_type, record.key, record.task and record.value.<field>) and nothing else.                     | `False`    |
| `names`   | `Optional[Container[str]]` | The bare names it may read, such as a task's vars. None refuses every bare name but t; ANY_NAME accepts any, for a loader that checks them against the vars afterwards. | `None`     |
| `rule`    | `bool`                     | True for the when and victim of a preempt rule, which also read request.<field> and holder.<field> and none of the entity's names.                                      | `False`    |
| `batch`   | `bool`                     | True for a batch's close_when, which also reads batch.size (how many members the batch holds) and batch.age (the seconds since its first member joined).                | `False`    |
| `member`  | `bool`                     | True for the lowest of a reference, which also reads member.key, member.users, member.queue, member.level, member.count, member.putters and member.getters.             | `False`    |

Raises:

| Type         | Description                                                                                           |
| ------------ | ----------------------------------------------------------------------------------------------------- |
| `ValueError` | If it is not valid Python or uses anything outside the whitelist. The message names what was refused. |

Source code in `src/desflow/core/expressions.py`

```python
def __init__(
    self,
    text: str,
    records: bool = False,
    names: Optional[Container[str]] = None,
    rule: bool = False,
    batch: bool = False,
    member: bool = False,
):
    """
    Parses and checks an expression.

    Args:
        text: The expression, such as `level.stock == 0 or payload.total > 100`.
        records: True for an egress `when`, whose expression reads the record
            (`record.stream_type`, `record.key`, `record.task` and
            `record.value.<field>`) and nothing else.
        names: The bare names it may read, such as a task's vars. None refuses
            every bare name but `t`; `ANY_NAME` accepts any, for a loader that
            checks them against the vars afterwards.
        rule: True for the `when` and `victim` of a preempt rule, which also read
            `request.<field>` and `holder.<field>` and none of the entity's names.
        batch: True for a batch's `close_when`, which also reads `batch.size` (how
            many members the batch holds) and `batch.age` (the seconds since its
            first member joined).
        member: True for the `lowest` of a reference, which also reads
            `member.key`, `member.users`, `member.queue`, `member.level`,
            `member.count`, `member.putters` and `member.getters`.

    Raises:
        ValueError: If it is not valid Python or uses anything outside the
            whitelist. The message names what was refused.
    """
    if not isinstance(text, str):
        raise ValueError(f"an expression must be text, got {text!r}")
    self.text = text
    self.where = ""
    self._records = records
    self._names = names
    self._rule = rule
    self._batch = batch
    self._member = member
    # The reads of a family by index, such as `users.desk[attr.k]`, and the reads
    # with no index, as (source, name), so a loader can match them to the families
    self.indexed: Set[Tuple[str, str]] = set()
    self.plain: Set[Tuple[str, str]] = set()
    nested = f"'{text}' is nested too deeply"
    try:
        tree = ast.parse(text.strip(), mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"'{text}' is not a valid expression: {exc.msg}") from None
    except (RecursionError, MemoryError):
        raise ValueError(nested) from None
    if _depth_of(tree) > _LARGEST_DEPTH:
        raise ValueError(nested)
    tree = _attr_indexes_as_reads(tree)
    self.references: Dict[str, Set[str]] = {
        "payload": set(),
        "attr": set(),
        "level": set(),
        "users": set(),
        "queue": set(),
        "putters": set(),
        "getters": set(),
        "count": set(),
        "items": set(),
        "store_item": set(),
        "item": set(),
        "wait": set(),
        "signal": set(),
        "handle": set(),
        "process": set(),
        "registry": set(),
        "name": set(),
        "has": set(),
        "arg": set(),
        "cause": set(),
        "upstream": set(),
        "held": set(),
        "waiting": set(),
        "putting": set(),
        "getting": set(),
        "running": set(),
        "members": set(),
        "each": set(),
        "member": set(),
        "flag": set(),
        "loop": set(),
    }
    # The `request.` and `holder.` reads of a preempt rule, such as `holder.rank`
    self.rule_reads: Set[str] = set()
    # The names of the comprehensions being checked
    self._local: Set[str] = set()
    # The record names an egress `when` reads, such as `value.event_type`
    self.record_names: Set[str] = set()
    # True inside the arguments of count() and items()
    self._in_call = False
    self._check(tree.body, strings=False)
    self._tree = tree.body
```

#### Attributes

##### `reads_queues`

bool: Whether the expression reads a queue length or what a store holds, so a wait on it is checked again after a change of a queue length.

#### Methods:

##### `evaluate(scope)`

Evaluates the expression.

Parameters:

| Name    | Type    | Description          | Default    |
| ------- | ------- | -------------------- | ---------- |
| `scope` | `Scope` | What its names read. | *required* |

Returns:

| Name  | Type  | Description                                                          |
| ----- | ----- | -------------------------------------------------------------------- |
| `Any` | `Any` | The value, such as a bool for a condition or a number for an amount. |

Raises:

| Type         | Description                                                                                                                                            |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ValueError` | If a name it reads is missing, or the arithmetic fails, such as a division by zero. The message names the expression, after its place when it has one. |

Source code in `src/desflow/core/expressions.py`

```python
def evaluate(self, scope: Scope) -> Any:
    """
    Evaluates the expression.

    Args:
        scope: What its names read.

    Returns:
        Any: The value, such as a bool for a condition or a number for an amount.

    Raises:
        ValueError: If a name it reads is missing, or the arithmetic fails, such
            as a division by zero. The message names the expression, after its
            place when it has one.
    """
    if _WORK.depth == 0:
        _WORK.made = 0
    _WORK.depth += 1
    place = f"{self.where}: " if self.where else ""
    try:
        return self._value(self._tree, scope)
    except RecursionError:
        raise ValueError(f"{place}'{self.text}' is nested too deeply") from None
    except MemoryError:
        raise ValueError(
            f"{place}cannot evaluate '{self.text}': it needs more memory than "
            "there is"
        ) from None
    except (KeyError, IndexError, ArithmeticError, TypeError, ValueError) as exc:
        reason = exc.args[0] if isinstance(exc, KeyError) else exc
        raise ValueError(
            f"{place}cannot evaluate '{self.text}': {reason}"
        ) from None
    finally:
        _WORK.depth -= 1
```

##### `outside_run(names=True)`

Names the sources the expression reads that a place reading the run only cannot: the entity's `attr` and `has`, `payload`, `item`, a `wait` branch, `process` and, unless `names` is False, a bare name.

Parameters:

| Name    | Type   | Description                                                      | Default |
| ------- | ------ | ---------------------------------------------------------------- | ------- |
| `names` | `bool` | Whether a bare name counts, as it does where no var is readable. | `True`  |

Returns:

| Type            | Description                                                             |
| --------------- | ----------------------------------------------------------------------- |
| `Optional[str]` | Optional\[str\]: The sources, such as attr, payload; None when it reads |
| `Optional[str]` | none of them.                                                           |

Source code in `src/desflow/core/expressions.py`

```python
def outside_run(self, names: bool = True) -> Optional[str]:
    """Names the sources the expression reads that a place reading the run only
    cannot: the entity's `attr` and `has`, `payload`, `item`, a `wait` branch,
    `process` and, unless `names` is False, a bare name.

    Args:
        names: Whether a bare name counts, as it does where no var is readable.

    Returns:
        Optional[str]: The sources, such as `attr, payload`; None when it reads
        none of them.
    """
    found = [
        kind
        for kind in (
            "attr",
            "payload",
            "item",
            "wait",
            "handle",
            "process",
            "arg",
            "cause",
            "upstream",
            "each",
            "member",
        )
        if self.references[kind] or (kind == "attr" and self.references["has"])
    ]
    found.extend(sorted(self.references["flag"]))
    if names and self.references["name"]:
        found.append("the name " + ", ".join(sorted(self.references["name"])))
    return ", ".join(found) if found else None
```

### `desflow.Scope(payload=dict(), attributes=dict(), levels=dict(), resources=dict(), registry=None, now=0.0, item=None, waits=dict(), signals=dict(), process=dict(), record=None, names=dict(), entity=None, args=None, cause=None, upstream=None, item_priority=None, done_count=None, loops=dict(), peek=None, instances=None, has_item=False, stream=None, handles=None, request=None, holder=None, batch=None, each=None, member=None, families=dict(), family_keys=dict())`

What the names in an expression read.

Attributes:

| Name            | Type                                     | Description                                                                                                                                                            |
| --------------- | ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `payload`       | `Optional[Mapping[str, Any]]`            | The upstream payload, read as payload.<field>; None when it is null.                                                                                                   |
| `attributes`    | `Mapping[str, Any]`                      | The entity's attributes, read as attr.<name>.                                                                                                                          |
| `levels`        | `Mapping[str, Any]`                      | Stores and containers by name, read as level.<name>, putters.<name> and getters.<name> (the puts and gets waiting), and, for a store, count(<name>) and items(<name>). |
| `resources`     | `Mapping[str, Any]`                      | Resources by name, read as users.<name> (units held) and queue.<name> (requests waiting).                                                                              |
| `registry`      | `Optional[Any]`                          | The simulation registry, read as registry\["<path>"\].                                                                                                                 |
| `now`           | `float`                                  | The simulation time, read as t.                                                                                                                                        |
| `item`          | `Optional[Mapping[str, Any]]`            | The item a store get took, or a filter's candidate, read as item.<field>.                                                                                              |
| `waits`         | `Mapping[str, Mapping[str, Any]]`        | The task's wait branches by name, each a mapping of done, ok and value, read as wait.<branch>.<part>.                                                                  |
| `signals`       | `Mapping[str, Any]`                      | Signals by name, read as signal.<name>.<part>.                                                                                                                         |
| `process`       | `Mapping[str, Any]`                      | The running task's name, instance and target, read as process.<part>.                                                                                                  |
| `record`        | `Optional[Mapping[str, Any]]`            | The record an egress when is offered, read as record.stream_type, record.key, record.task and record.value.<field>.                                                    |
| `names`         | `Mapping[str, Any]`                      | The bare names: a task's vars, and the names of an expr generator.                                                                                                     |
| `entity`        | `Optional[str]`                          | The key of the entity the task runs for, which gen: entity gives; None where no task runs.                                                                             |
| `args`          | `Optional[Mapping[str, Any]]`            | The process's local names, read as arg.<name>: what the parent passed and what set_args wrote; None where no task runs.                                                |
| `cause`         | `Optional[Mapping[str, Any]]`            | The cause of the interrupt the task was reached by, read as cause.<name>; None where none reached it.                                                                  |
| `upstream`      | `Optional[Mapping[str, Any]]`            | The payload of the task that routed here, read as upstream.<field>.                                                                                                    |
| `item_priority` | `Any`                                    | The priority of the item a priority store gave, or None.                                                                                                               |
| `done_count`    | `Optional[int]`                          | How many branches of a when group are done, or None.                                                                                                                   |
| `loops`         | `Mapping[str, Any]`                      | The names of the comprehensions being evaluated.                                                                                                                       |
| `peek`          | `Optional[Callable[[], Any]]`            | Gives the time of the next scheduled event.                                                                                                                            |
| `instances`     | `Optional[Callable[[str], List[Scope]]]` | Gives the scopes of the running instances of a task.                                                                                                                   |
| `has_item`      | `bool`                                   | Whether a get has given the task an item (it may be None).                                                                                                             |
| `stream`        | `Any`                                    | The stream previous follows for the entity, or None where no task runs.                                                                                                |
| `handles`       | `Optional[Callable[[str], Any]]`         | Gives the event opened under a name, read as handle.<name>.<part>; None where no task runs.                                                                            |
| `request`       | `Optional[Mapping[str, Any]]`            | The request a preempt rule tests, read as request.rank, request.time, request.preempt and request.key; None elsewhere.                                                 |
| `holder`        | `Optional[Mapping[str, Any]]`            | The user whose unit it would take, read as holder.rank, .time, .preempt, .key, .since and holder.attr.<name>; None elsewhere.                                          |
| `batch`         | `Optional[Mapping[str, Any]]`            | The open batch a close_when tests; None elsewhere.                                                                                                                     |
| `each`          | `Optional[Mapping[str, Any]]`            | The element an each branch is made for, read as each.index and each.value; None elsewhere.                                                                             |
| `member`        | `Optional[Mapping[str, Any]]`            | The member a lowest is read for, read as member.<name>; None elsewhere.                                                                                                |
| `families`      | `Mapping[str, Any]`                      | The declared families by name, which members() and a read of a member that does not exist yet use.                                                                     |
| `family_keys`   | `Mapping[str, List[Any]]`                | The keys of the members the run has made, by family name, which members() reads.                                                                                       |

## Loading a Model

`SimulationContext.from_yaml` is the usual entry point for a blueprint. `from_yaml` calls `build`, which returns the context and the `run` section. `load_context` loads a blueprint or a builder Python file from a folder, local or in object storage, in the way `desflow run` loads a blueprint. `desflow run --sample` and `--stdout` pass a `Preview` to these functions. `preview_builder` changes a context that a builder file defines, so that it runs as such a preview. `check_model` checks a blueprint or a builder file as `desflow check` does and returns a `ModelCheck`.

### `desflow.load_context(folder, entry, preview=None, variables=None)`

Builds the simulation that `entry` in `folder` defines.

An entry ending in `.py` is a builder file: it is imported under a new module name each call, with `folder` first on `sys.path` during the import, so its imports resolve from its folder and its `if __name__ == "__main__":` block does not run. Its `DESFLOW_APP` names the context to use; without it, the file must define exactly one `SimulationContext` at module level. Any other entry is a YAML blueprint, built by `SimulationContext.from_yaml`.

Parameters:

| Name        | Type                          | Description                                                                                                                                                                                                                                                                                             | Default    |
| ----------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `folder`    | `Union[str, Path]`            | A local folder, or a URI that pyarrow.fs.FileSystem.from_uri reads, such as s3://bucket/sims/shop or file:///srv/shop. A URI folder is copied to a temporary local folder first, which is removed when the process ends, because !python references and Python imports resolve from the entry's folder. | *required* |
| `entry`     | `str`                         | The entry file's name inside folder.                                                                                                                                                                                                                                                                    | *required* |
| `preview`   | `Optional[Preview]`           | The preview desflow run --sample and --stdout make. A builder file is imported first, and its sinks are then replaced as a blueprint's are (preview_builder).                                                                                                                                           | `None`     |
| `variables` | `Optional[Mapping[str, str]]` | Values for ${NAME}, used before the environment, as desflow run --var NAME=VALUE gives them, for a blueprint only.                                                                                                                                                                                      | `None`     |

Returns:

| Name                | Type                | Description                     |
| ------------------- | ------------------- | ------------------------------- |
| `SimulationContext` | `SimulationContext` | The built context, not yet run. |

Raises:

| Type             | Description                                                                                                                                                                                    |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `NoContextError` | If a Python entry defines no context. It is a ValueError.                                                                                                                                      |
| `ValueError`     | If folder is a URI and entry is not a file in it; if a Python entry defines several contexts with no DESFLOW_APP, or DESFLOW_APP names no context; or variables are given with a Python entry. |
| `ImportError`    | If folder is a URI and pyarrow is not installed.                                                                                                                                               |
| `BlueprintError` | If a blueprint is invalid.                                                                                                                                                                     |

Source code in `src/desflow/loading.py`

```python
def load_context(
    folder: Union[str, Path],
    entry: str,
    preview: Optional[Preview] = None,
    variables: Optional[Mapping[str, str]] = None,
) -> SimulationContext:
    """Builds the simulation that `entry` in `folder` defines.

    An entry ending in `.py` is a builder file: it is imported under a new module
    name each call, with `folder` first on `sys.path` during the import, so its
    imports resolve from its folder and its `if __name__ == "__main__":` block does
    not run. Its `DESFLOW_APP` names the context to use; without it, the file must
    define exactly one `SimulationContext` at module level. Any other entry is a
    YAML blueprint, built by `SimulationContext.from_yaml`.

    Args:
        folder: A local folder, or a URI that `pyarrow.fs.FileSystem.from_uri`
            reads, such as `s3://bucket/sims/shop` or `file:///srv/shop`. A URI
            folder is copied to a temporary local folder first, which is removed
            when the process ends, because `!python` references and Python imports
            resolve from the entry's folder.
        entry: The entry file's name inside `folder`.
        preview: The preview `desflow run --sample` and `--stdout` make. A
            builder file is imported first, and its sinks are then replaced as a
            blueprint's are (`preview_builder`).
        variables: Values for `${NAME}`, used before the environment, as `desflow
            run --var NAME=VALUE` gives them, for a blueprint only.

    Returns:
        SimulationContext: The built context, not yet run.

    Raises:
        NoContextError: If a Python entry defines no context. It is a
            `ValueError`.
        ValueError: If `folder` is a URI and `entry` is not a file in it; if a Python
            entry defines several contexts with no `DESFLOW_APP`, or `DESFLOW_APP`
            names no context; or variables are given with a Python entry.
        ImportError: If `folder` is a URI and pyarrow is not installed.
        BlueprintError: If a blueprint is invalid.
    """
    local = _local_folder(folder)
    path = local / entry
    if isinstance(folder, str) and "://" in folder and not path.is_file():
        raise ValueError(f"no file {folder}/{entry}")
    if path.suffix != _PYTHON_SUFFIX:
        return SimulationContext.from_yaml(path, preview, variables)
    if variables:
        raise ValueError(
            f"variables fill ${{NAME}} in a YAML blueprint, and {entry} is a Python "
            f"file, which reads its own settings"
        )
    context = _python_context(path)
    if preview is not None:
        preview_builder(context, preview)
    # analyze(verify=True) builds a new copy from the local file
    parts_of(context).defaults.source = functools.partial(load_context, local, entry)
    return context
```

### desflow.blueprint.build

```python
build(path, preview=None, variables=None)
```

Reads, validates and builds a blueprint file.

The structure, keys and names of the whole file are validated with or without a preview. A preview builds none of the connectors it replaces or skips, so it checks only their type; their config is checked only by a run that builds them, such as `desflow run FILE` without `--sample` or `--stdout`.

Parameters:

| Name        | Type                          | Description                                                                                                             | Default    |
| ----------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------- |
| `path`      | `Union[str, Path]`            | The YAML file.                                                                                                          | *required* |
| `preview`   | `Optional[Preview]`           | Replace the file's egress with a JSON console, and for a sample leave out its ingress; None builds the file as written. | `None`     |
| `variables` | `Optional[Mapping[str, str]]` | Values for ${NAME} that are used before the environment, as desflow run --var NAME=VALUE gives them.                    | `None`     |

Returns:

| Type                            | Description                                                   |
| ------------------------------- | ------------------------------------------------------------- |
| `Tuple[SimulationContext, Run]` | The built context, and the run section for whoever starts it. |

Raises:

| Type             | Description                                                  |
| ---------------- | ------------------------------------------------------------ |
| `BlueprintError` | If the file is invalid. The message names the file and line. |

Source code in `src/desflow/blueprint/build.py`

```python
def build(
    path: Union[str, Path],
    preview: Optional[Preview] = None,
    variables: Optional[Mapping[str, str]] = None,
) -> Tuple[SimulationContext, Run]:
    """Reads, validates and builds a blueprint file.

    The structure, keys and names of the whole file are validated with or without
    a preview. A preview builds none of the connectors it replaces or skips, so it
    checks only their type; their config is checked only by a run that builds
    them, such as `desflow run FILE` without `--sample` or `--stdout`.

    Args:
        path: The YAML file.
        preview: Replace the file's egress with a JSON console, and for a sample
            leave out its ingress; None builds the file as written.
        variables: Values for `${NAME}` that are used before the environment, as
            `desflow run --var NAME=VALUE` gives them.

    Returns:
        The built context, and the `run` section for whoever starts it.

    Raises:
        BlueprintError: If the file is invalid. The message names the file and line.
    """
    built = load_blueprint(path, preview, variables)
    return built.context, built.run
```

### `desflow.blueprint.Preview(sample=None)`

A preview run of a blueprint, as `desflow run --sample` and `--stdout` make.

Every egress entry is replaced by a JSON console on stdout, with one more console for labels when the file has a `when: labels` entry and one for the trace when it has a `when: trace` entry, so no sink module the file names by a short `type` is imported, and no sink is written to.

Attributes:

| Name     | Type            | Description                                                                                                                                                                           |
| -------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sample` | `Optional[int]` | Print this many events, unpaced and with no ingress, then stop. None, for --stdout, prints every record of the run as it is paced, except the system.simulation.lag_seconds readings. |

### `desflow.blueprint.preview_builder(context, preview)`

Makes a context that a builder file defined run as a preview, as a blueprint built with the same preview runs.

Every egress sink the file added is replaced by a JSON console on stdout, with one more console for labels when the file added a labels sink and one for the trace when it added a trace sink. A sample also leaves out the ingress, and runs unpaced from `logical_start_time`, with `go_live_at` cleared, until it has printed that many events. The file's connectors are already built, because importing the file built them, but none of them is set up or written to.

Parameters:

| Name      | Type                | Description                                        | Default    |
| --------- | ------------------- | -------------------------------------------------- | ---------- |
| `context` | `SimulationContext` | The context the builder file defines, not yet run. | *required* |
| `preview` | `Preview`           | The preview desflow run --sample makes.            | *required* |

Source code in `src/desflow/blueprint/build.py`

```python
def preview_builder(context: SimulationContext, preview: Preview) -> None:
    """Makes a context that a builder file defined run as a preview, as a blueprint
    built with the same preview runs.

    Every egress sink the file added is replaced by a JSON console on stdout, with
    one more console for labels when the file added a labels sink and one for the
    trace when it added a trace sink. A sample also leaves out the ingress, and runs
    unpaced from `logical_start_time`, with `go_live_at` cleared, until it has
    printed that many events. The file's connectors are already built, because
    importing the file built them, but none of them is set up or written to.

    Args:
        context: The context the builder file defines, not yet run.
        preview: The preview `desflow run --sample` makes.
    """
    sinks = parts_of(context).sinks
    kinds = []
    if any(sink.label for sink in sinks.egress):
        kinds.append(LABELS)
    if any(sink.trace for sink in sinks.egress):
        kinds.append(TRACE)
    sinks.egress = []
    if preview.sample is not None:
        sinks.ingress = []
    _add_preview_consoles(context, preview, kinds)
    run_as_sample(context, preview)
```

### `desflow.blueprint.check_model(file, variables=None)`

Checks one blueprint or builder file as `desflow check` does, without running it.

For a YAML blueprint that is accepted, it also reads the `run` section and the start time, with the loader that `desflow run` uses.

Parameters:

| Name        | Type                          | Description                                                     | Default    |
| ----------- | ----------------------------- | --------------------------------------------------------------- | ---------- |
| `file`      | `Path`                        | A YAML blueprint, or a Python file with a module-level context. | *required* |
| `variables` | `Optional[Mapping[str, str]]` | Values for ${NAME} in a blueprint, as --var gives them.         | `None`     |

Returns:

| Name         | Type         | Description                                                                                                        |
| ------------ | ------------ | ------------------------------------------------------------------------------------------------------------------ |
| `ModelCheck` | `ModelCheck` | The built context, or why the file was refused, and for an accepted YAML blueprint its run section and start time. |

Raises:

| Type        | Description                                                                          |
| ----------- | ------------------------------------------------------------------------------------ |
| `Exception` | An error of a YAML file other than a BlueprintError, an ImportError or a ValueError. |

Source code in `src/desflow/blueprint/model_check.py`

```python
def check_model(
    file: Path, variables: Optional[Mapping[str, str]] = None
) -> ModelCheck:
    """Checks one blueprint or builder file as `desflow check` does, without running it.

    For a YAML blueprint that is accepted, it also reads the `run` section and the
    start time, with the loader that `desflow run` uses.

    Args:
        file: A YAML blueprint, or a Python file with a module-level context.
        variables: Values for `${NAME}` in a blueprint, as `--var` gives them.

    Returns:
        ModelCheck: The built context, or why the file was refused, and for an
            accepted YAML blueprint its `run` section and start time.

    Raises:
        Exception: An error of a YAML file other than a `BlueprintError`, an
            `ImportError` or a `ValueError`.
    """
    from desflow.blueprint.build import load_blueprint
    from desflow.blueprint.loader import read_document
    from desflow.cli.check import check_file

    checked = check_file(file, variables)
    if checked.context is None or file.suffix == ".py":
        return ModelCheck(checked.context, checked.problem, checked.no_context)
    run = load_blueprint(file, variables=variables).run
    data = read_document(file, variables=variables)[0]
    start = (data.get("simulation") or {}).get("logical_start_time")
    if start is not None and not isinstance(start, str):
        start = str(start)
    return ModelCheck(checked.context, None, False, run, start)
```

### `desflow.blueprint.ModelCheck`

Bases: `NamedTuple`

What `check_model` found in one file.

Attributes:

| Name         | Type                          | Description                                                                                                                                                              |
| ------------ | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `context`    | `Optional[SimulationContext]` | The built context, or None when the file was refused.                                                                                                                    |
| `problem`    | `Optional[str]`               | Why the file was refused, as one message, or None.                                                                                                                       |
| `no_context` | `bool`                        | True when the file is a Python file refused only because it defines no module-level SimulationContext, as a Low-level file does.                                         |
| `run`        | `Optional[Run]`               | The run section of a YAML blueprint that was accepted, else None.                                                                                                        |
| `start`      | `Optional[str]`               | The simulation.logical_start_time of an accepted YAML blueprint as the file gives it after ${NAME} and !include are read, as text even when YAML read a time, else None. |

### `desflow.blueprint.Run`

Bases: `_Model`

The `run` section.

`before` lists `!python` functions called with no arguments before the run starts, for setup such as creating topics or tables. `stop` ends the run before `until` at the next firing of a signal, `{signal: name}`, or when a condition first holds, `{condition: ...}`. `drain`, `true` or `{limit: 1 h}`, stops the arrivals at `until` and runs on until no entity is left in the model.

`segments` runs the model in parts and publishes a reading after each, as a SimPy program that calls `env.run(until=...)` in a loop does: a list of `RunSegment`, or `{every: <time>}` for equal parts up to `until`. `steps: true` publishes `system.simulation.step`, `{step, now, peek}`, after each model event, as a loop of `env.step()` and `env.peek()` reads them. `sync: true` calls `env.sync()` as the run starts. A run with no `until` ends when no model event is left, except that a list of segments with no `until` whose last entry has an `until` or a `stop` ends there, as a SimPy program that stops calling `env.run` does; a last entry `{}` runs to the end.

#### Methods:

##### `drain_setting()`

`drain` in the form `SimulationContext.run` takes.

Source code in `src/desflow/blueprint/models.py`

```python
def drain_setting(self) -> Any:
    """`drain` in the form `SimulationContext.run` takes."""
    if isinstance(self.drain, Drain):
        return {"limit": self.drain.limit}
    return self.drain or None
```

### `desflow.blueprint.RunSegment`

Bases: `_Model`

A `run.segments` entry: the run goes on to `until` or to `stop`, then waits for the next entry.

An entry has at most one of `until` and `stop`; `{}` runs to the end. After the segment stops, `look` names a task whose new entity reads the model before any event at the stop time runs, and `sync` re-anchors the wall clock at the start of the segment. A record at `system.simulation.segment` reports `index`, `now` and `peek` (null when no model event is left) as the segment ends.

### `desflow.blueprint.SegmentEvery`

Bases: `_Model`

`run.segments: {every: 10 s}`: a segment every `every` seconds up to `run.until`.

### `desflow.blueprint.Drain`

Bases: `_Model`

A `run.drain` mapping: the longest a drain may run after `until`.

### `desflow.blueprint.BlueprintError`

Bases: `ValueError`

A blueprint that cannot be built. The message names the file and line.

## Replications

### `desflow.replicate(model, n, *, seed, until=None, warmup=None, measures=None, level=0.95, workers=1, overrides=None, stop=None, drain=None)`

Runs a model `n` times with spawned seeds and summarises its statistics.

Each replication runs unpaced, with no ingress and no egress, and reads the built-in statistics at its end. When the model has sinks (a blueprint's `egress`, or a builder's `add_egress`), they receive the summary as telemetry once, at the end: each replication's value and the mean, half-width and count of each measure.

Parameters:

| Name        | Type                          | Description                                                                                                                                                                                                                                                               | Default    |
| ----------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `model`     | `Model`                       | A YAML blueprint path, a builder factory (a function with no arguments returning a new SimulationContext), or a LowLevelModel.                                                                                                                                            | *required* |
| `n`         | `int`                         | How many replications, 2 or more.                                                                                                                                                                                                                                         | *required* |
| `seed`      | `Optional[int]`               | The seed the replications' seeds are spawned from. None draws one and logs it.                                                                                                                                                                                            | *required* |
| `until`     | `Union[float, str, None]`     | Where each replication's arrivals stop, in seconds or as a duration such as "1 h"; the replication ends there unless stop ends it earlier or drain runs on. None uses a blueprint's run.until.                                                                            | `None`     |
| `warmup`    | `Union[float, str, None]`     | The end of each replication's warm-up; None keeps the model's own.                                                                                                                                                                                                        | `None`     |
| `measures`  | `Optional[Sequence[str]]`     | Statistic names without the sim id, such as tasks.pack.wait_mean. None summarises every statistic the run publishes, at least up to typical.                                                                                                                              | `None`     |
| `level`     | `float`                       | The confidence level of the half-widths. Defaults to 0.95.                                                                                                                                                                                                                | `0.95`     |
| `workers`   | `int`                         | Replications run at once, each in its own process. Above 1 the model must be a path or a function defined at the top of a module.                                                                                                                                         | `1`        |
| `overrides` | `Optional[Mapping[str, Any]]` | Registry paths and the values they take in every replication.                                                                                                                                                                                                             | `None`     |
| `stop`      | `Any`                         | Ends each replication before until when it happens, as for SimulationContext.run: {"signal": name}, a signal's name or {"condition": "..."}; for a LowLevelModel, a function of the environment returning the event that ends the run. None keeps a blueprint's run.stop. | `None`     |
| `drain`     | `Any`                         | At until, stop the arrivals and run each replication on until no entity is left: True, or {"limit": duration}. None keeps a blueprint's run.drain. A LowLevelModel is drained when its function returns the processes that start its arrivals.                            | `None`     |

Returns:

| Name           | Type           | Description                                 |
| -------------- | -------------- | ------------------------------------------- |
| `Replications` | `Replications` | Every value with its seed, and the summary. |

Raises:

| Type         | Description                                                                                                                                                                             |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If n is below 2, until is missing, the level is not between 0 and 1, a measure is not a statistic of the model, or the model cannot be sent to another process when workers is above 1. |

Source code in `src/desflow/analysis/runner.py`

```python
def replicate(
    model: Model,
    n: int,
    *,
    seed: Optional[int],
    until: Union[float, str, None] = None,
    warmup: Union[float, str, None] = None,
    measures: Optional[Sequence[str]] = None,
    level: float = 0.95,
    workers: int = 1,
    overrides: Optional[Mapping[str, Any]] = None,
    stop: Any = None,
    drain: Any = None,
) -> Replications:
    """
    Runs a model `n` times with spawned seeds and summarises its statistics.

    Each replication runs unpaced, with no ingress and no egress, and reads the
    built-in statistics at its end. When the model has sinks (a blueprint's
    `egress`, or a builder's `add_egress`), they receive the summary as telemetry
    once, at the end: each replication's value and the mean, half-width and count of
    each measure.

    Args:
        model: A YAML blueprint path, a builder factory (a function with no
            arguments returning a new `SimulationContext`), or a `LowLevelModel`.
        n: How many replications, 2 or more.
        seed: The seed the replications' seeds are spawned from. None draws one and
            logs it.
        until: Where each replication's arrivals stop, in seconds or as a duration
            such as `"1 h"`; the replication ends there unless `stop` ends it
            earlier or `drain` runs on. None uses a blueprint's `run.until`.
        warmup: The end of each replication's warm-up; None keeps the model's own.
        measures: Statistic names without the sim id, such as
            `tasks.pack.wait_mean`. None summarises every statistic the run
            publishes, at least up to `typical`.
        level: The confidence level of the half-widths. Defaults to 0.95.
        workers: Replications run at once, each in its own process. Above 1 the
            model must be a path or a function defined at the top of a module.
        overrides: Registry paths and the values they take in every replication.
        stop: Ends each replication before `until` when it happens, as for
            `SimulationContext.run`: `{"signal": name}`, a signal's name or
            `{"condition": "..."}`; for a `LowLevelModel`, a function of the
            environment returning the event that ends the run. None keeps a
            blueprint's `run.stop`.
        drain: At `until`, stop the arrivals and run each replication on until no
            entity is left: True, or `{"limit": duration}`. None keeps a
            blueprint's `run.drain`. A `LowLevelModel` is drained when its function
            returns the processes that start its arrivals.

    Returns:
        Replications: Every value with its seed, and the summary.

    Raises:
        ValueError: If `n` is below 2, `until` is missing, the level is not between
            0 and 1, a measure is not a statistic of the model, or the model cannot
            be sent to another process when `workers` is above 1.
    """
    if n < 2:
        raise ValueError(f"replications need n of 2 or more, got {n}")
    if not 0.0 < level < 1.0:
        raise ValueError(f"level must be between 0 and 1, got {level}")
    if seed is not None:
        checked_whole(seed, "seed", 0)
    else:
        seed = secrets.randbelow(2**32)
        logger.info(f"No seed was given, so the replications use {seed}.")
    seeds = spawn_seeds(seed, n)
    results = run_replications(
        model,
        seeds,
        until=until,
        warmup=warmup,
        stats=level_for(measures),
        workers=workers,
        overrides=overrides,
        stop=stop,
        drain=drain,
    )
    names = select_measures(results, measures)
    replications = Replications(
        sim_id=results[0].sim_id,
        seeds=seeds,
        measures=names,
        values={m: values_of(results, m) for m in names},
        level=level,
        streams=[r.streams for r in results],
    )
    _publish(model, replications, until)
    return replications
```

### `desflow.Replications(sim_id, seeds, measures, values, level=0.95, streams=list())`

The result of `replicate`: every value with its seed, and a summary per measure.

Attributes:

| Name       | Type                     | Description                                                                           |
| ---------- | ------------------------ | ------------------------------------------------------------------------------------- |
| `sim_id`   | `str`                    | The model's sim id.                                                                   |
| `seeds`    | `List[int]`              | The seed of each replication, in order. Replication i (from 1) ran with seeds[i - 1]. |
| `measures` | `List[str]`              | The statistics summarised, such as tasks.pack.wait_mean.                              |
| `values`   | `Dict[str, List[float]]` | Each measure's value in each replication.                                             |
| `level`    | `float`                  | The confidence level of the half-widths.                                              |
| `streams`  | `List[List[str]]`        | The random streams each replication drew from.                                        |

#### Attributes

##### `summary`

One row per measure: `measure`, `n`, `mean`, `half_width` and `level`.

##### `table`

One row per replication and measure: `replication` (from 1), `seed`, `measure` and `value`.

#### Methods:

##### `summary_text()`

The summary as a table: each measure's n, mean and half-width.

Source code in `src/desflow/analysis/runner.py`

```python
def summary_text(self) -> str:
    """The summary as a table: each measure's n, mean and half-width."""
    rows = self.summary
    width = max([len("measure"), *(len(row["measure"]) for row in rows)])
    heading = f"half-width ({self.level * 100:g}%)"
    lines = [f"{'measure':<{width}}  {'n':>4}  {'mean':>12}  {heading:>18}"]
    for row in rows:
        lines.append(
            f"{row['measure']:<{width}}  {row['n']:>4}  {row['mean']:>12.4g}  "
            f"{row['half_width']:>18.4g}"
        )
    return "\n".join(lines)
```

##### `telemetry()`

The telemetry records the sinks receive, as `(path_id, value)` pairs.

One per measure and replication, `<sim_id>.stats.<measure>.replication.<i>`, then the mean, half-width and count across replications, `<sim_id>.stats.<measure>.mean`, `.half_width` and `.n`.

Source code in `src/desflow/analysis/runner.py`

```python
def telemetry(self) -> List[tuple]:
    """
    The telemetry records the sinks receive, as `(path_id, value)` pairs.

    One per measure and replication, `<sim_id>.stats.<measure>.replication.<i>`,
    then the mean, half-width and count across replications,
    `<sim_id>.stats.<measure>.mean`, `.half_width` and `.n`.
    """
    records: List[tuple] = []
    prefix = f"{self.sim_id}.stats"
    for row in self.table:
        name = f"{prefix}.{row['measure']}.replication.{row['replication']}"
        records.append((name, row["value"]))
    for row in self.summary:
        for key in ("mean", "half_width", "n"):
            records.append((f"{prefix}.{row['measure']}.{key}", row[key]))
    return records
```

### `desflow.LowLevelModel(function, sim_id)`

A model written with the low-level API, for `replicate` and `run_experiment`.

Attributes:

| Name       | Type       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ---------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `function` | `Callable` | Called as function(env, sampler) once per run. It registers the parameters, builds the resources and starts the processes, and draws every random number from sampler. It does not set up the statistics or the sinks: the runner does. To be drained, it returns the processes that start its arrivals, which a drain interrupts at until, and reports its entities with env.stats.flow_started and flow_finished, so the drain knows when the model is empty. |
| `sim_id`   | `str`      | The sim id the parameters are registered under, which the statistics' names start with.                                                                                                                                                                                                                                                                                                                                                                         |

### `desflow.analysis.interval(values, level=0.95)`

Returns the mean of `values` and the Student t half-width of its interval.

The half-width is `t × s / √n`, where `s` is the sample standard deviation and `t` the quantile at `(1 + level) / 2` with `n - 1` degrees of freedom.

Parameters:

| Name     | Type              | Description                                                      | Default    |
| -------- | ----------------- | ---------------------------------------------------------------- | ---------- |
| `values` | `Sequence[float]` | Independent values, such as one statistic from each replication. | *required* |
| `level`  | `float`           | The confidence level, between 0 and 1. Defaults to 0.95.         | `0.95`     |

Returns:

| Name       | Type       | Description                                                                                                        |
| ---------- | ---------- | ------------------------------------------------------------------------------------------------------------------ |
| `Interval` | `Interval` | The mean and its half-width. If a value is infinite or NaN, the mean is infinite or NaN and the half-width is NaN. |

Raises:

| Type         | Description                                                           |
| ------------ | --------------------------------------------------------------------- |
| `ValueError` | If there are fewer than 2 values or the level is not between 0 and 1. |

Source code in `src/desflow/analysis/intervals.py`

```python
def interval(values: Sequence[float], level: float = 0.95) -> Interval:
    """
    Returns the mean of `values` and the Student t half-width of its interval.

    The half-width is `t × s / √n`, where `s` is the sample standard deviation and
    `t` the quantile at `(1 + level) / 2` with `n - 1` degrees of freedom.

    Args:
        values: Independent values, such as one statistic from each replication.
        level: The confidence level, between 0 and 1. Defaults to 0.95.

    Returns:
        Interval: The mean and its half-width. If a value is infinite or NaN, the mean
            is infinite or NaN and the half-width is NaN.

    Raises:
        ValueError: If there are fewer than 2 values or the level is not between 0
            and 1.
    """
    if not 0.0 < level < 1.0:
        raise ValueError(f"level must be between 0 and 1, got {level}")
    n = len(values)
    if n < 2:
        raise ValueError(f"an interval needs at least 2 values, got {n}")
    if not all(math.isfinite(value) for value in values):
        # An infinite measure, such as the capacity of an unbounded store, has no
        # spread: its mean is infinite (or NaN) and its half-width is NaN
        return Interval(
            n=n, mean=fmean(values), std=math.nan, half_width=math.nan, level=level
        )
    spread = stdev(values)
    quantile = t_quantile((1.0 + level) / 2.0, n - 1)
    return Interval(
        n=n,
        mean=fmean(values),
        std=spread,
        half_width=quantile * spread / math.sqrt(n),
        level=level,
    )
```

### `desflow.analysis.Interval(n, mean, std, half_width, level)`

A mean and the half-width of its confidence interval.

Attributes:

| Name         | Type    | Description                         |
| ------------ | ------- | ----------------------------------- |
| `n`          | `int`   | How many values the mean is of.     |
| `mean`       | `float` | Their mean.                         |
| `std`        | `float` | Their sample standard deviation.    |
| `half_width` | `float` | The Student t half-width at level.  |
| `level`      | `float` | The confidence level, such as 0.95. |

#### Attributes

##### `high`

The upper bound, `mean + half_width`.

##### `low`

The lower bound, `mean - half_width`.

### `desflow.analysis.t_quantile(p, df)`

Returns the value a Student t variable is at most with probability `p`.

Parameters:

| Name | Type    | Description                       | Default    |
| ---- | ------- | --------------------------------- | ---------- |
| `p`  | `float` | The probability, between 0 and 1. | *required* |
| `df` | `float` | The degrees of freedom, above 0.  | *required* |

Returns:

| Name    | Type    | Description                                       |
| ------- | ------- | ------------------------------------------------- |
| `float` | `float` | The quantile, such as 2.262 for p=0.975 and df=9. |

Raises:

| Type         | Description                                        |
| ------------ | -------------------------------------------------- |
| `ValueError` | If p is not between 0 and 1, or df is not above 0. |

Source code in `src/desflow/analysis/intervals.py`

```python
def t_quantile(p: float, df: float) -> float:
    """
    Returns the value a Student t variable is at most with probability `p`.

    Args:
        p: The probability, between 0 and 1.
        df: The degrees of freedom, above 0.

    Returns:
        float: The quantile, such as 2.262 for `p=0.975` and `df=9`.

    Raises:
        ValueError: If `p` is not between 0 and 1, or `df` is not above 0.
    """
    if not 0.0 < p < 1.0:
        raise ValueError(f"p must be between 0 and 1, got {p}")
    if not df > 0:
        raise ValueError(f"degrees of freedom must be above 0, got {df}")
    if p == 0.5:
        return 0.0
    if p < 0.5:
        return -t_quantile(1.0 - p, df)
    low, high = 0.0, 1.0
    while _t_cdf(high, df) < p:
        low, high = high, high * 2.0
    # Halving the bracket until it stops shrinking gives the closest float
    while True:
        middle = (low + high) / 2.0
        if middle in (low, high):
            return high
        if _t_cdf(middle, df) < p:
            low = middle
        else:
            high = middle
```

## Experiments

### `desflow.run_experiment(spec, *, raise_on_failure=True, output=None)`

Runs an experiment: its variants and sweep, replications, comparisons and checks.

Every artifact is written to the experiment's `output` folder before anything is raised: `experiment.json` (settings and evidence), `results.csv`, `summary.csv`, `comparisons.csv`, `checks.csv` and `report.md`.

Parameters:

| Name               | Type                                       | Description                                                                                                                                                                      | Default    |
| ------------------ | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `spec`             | `Union[str, Path, Mapping[str, Any], Any]` | An experiment file, a mapping of the same settings, or an ExperimentSpec. Relative paths in a file are read from the file's folder, and in a mapping from the working directory. | *required* |
| `raise_on_failure` | `bool`                                     | When True, a failed check raises CheckFailed, which carries the result.                                                                                                          | `True`     |
| `output`           | `Union[str, Path, None]`                   | The folder to write the artifacts to, in place of the experiment's output.                                                                                                       | `None`     |

Returns:

| Name               | Type  | Description                |
| ------------------ | ----- | -------------------------- |
| `ExperimentResult` | `Any` | What the experiment found. |

Raises:

| Type             | Description                                                                                                                                                                                           |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BlueprintError` | If the experiment file is invalid, or the model or observed file it names does not exist, naming the file and line.                                                                                   |
| `ValueError`     | If the settings are invalid, a variant sets a path the model does not have, or the observed values do not match the measures; for settings given as a mapping, also a missing model or observed file. |
| `CheckFailed`    | If a check failed and raise_on_failure is True.                                                                                                                                                       |

Source code in `src/desflow/analysis/runner.py`

```python
def run_experiment(
    spec: Union[str, Path, Mapping[str, Any], Any],
    *,
    raise_on_failure: bool = True,
    output: Union[str, Path, None] = None,
) -> Any:
    """
    Runs an experiment: its variants and sweep, replications, comparisons and checks.

    Every artifact is written to the experiment's `output` folder before anything is
    raised: `experiment.json` (settings and evidence), `results.csv`, `summary.csv`,
    `comparisons.csv`, `checks.csv` and `report.md`.

    Args:
        spec: An experiment file, a mapping of the same settings, or an
            `ExperimentSpec`.
            Relative paths in a file are read from the file's folder, and in a
            mapping from the working directory.
        raise_on_failure: When True, a failed check raises `CheckFailed`, which
            carries the result.
        output: The folder to write the artifacts to, in place of the experiment's
            `output`.

    Returns:
        ExperimentResult: What the experiment found.

    Raises:
        BlueprintError: If the experiment file is invalid, or the model or observed file
            it names does not exist, naming the file and line.
        ValueError: If the settings are invalid, a variant sets a path the model
            does not have, or the observed values do not match the measures; for
            settings given as a mapping, also a missing model or observed file.
        CheckFailed: If a check failed and `raise_on_failure` is True.
    """
    checked = _checked_settings(spec)
    settings, model, all_designs = checked.settings, checked.model, checked.designs
    stats = level_for(settings.measures)
    for design in all_designs:
        design.runs = run_replications(
            model,
            design.seeds,
            until=settings.until,
            warmup=settings.warmup,
            stats=stats,
            workers=settings.workers,
            overrides=design.overrides,
            stop=settings.stop,
            drain=settings.drain,
        )
    result = _write_artifacts(checked, output)
    if result.failed and raise_on_failure:
        raise found.CheckFailed(result)
    return result
```

### `desflow.ExperimentSpec`

Bases: `_Model`

A whole experiment file, or the same settings given in Python.

`model` is a blueprint path, a builder factory (a function with no arguments returning a new `SimulationContext`), a `LowLevelModel`, or in a file a mapping of a low-level `function` and its `sim_id`. Relative paths are read from the experiment file's folder. `until` is where each replication's arrivals stop; `stop` and `drain` end each replication as `SimulationContext.run` does, and when they are not set a blueprint's own `run.stop` and `run.drain` apply.

### `desflow.analysis.Check`

Bases: `_Model`

A `checks` entry: a bound on one measure of one variant, or of every variant.

`on: interval`, the default, checks the far end of the confidence interval (the upper end against `max`, the lower against `min`), so noise cannot pass a check; `on: mean` checks the mean only.

### `desflow.analysis.Compare`

Bases: `_Model`

A `compare` entry: two variants compared run by run with the same seeds.

### `desflow.analysis.Observed`

Bases: `_Model`

The `observed` section: a CSV of values measured on the real system.

Its columns are `measure`, `value`, `unit`, `population` and `horizon`. Each row is compared with the simulated measure of `variant`, the first variant when it is not given.

### `desflow.analysis.load_experiment(path)`

Reads and validates an experiment file.

Parameters:

| Name   | Type               | Description          | Default    |
| ------ | ------------------ | -------------------- | ---------- |
| `path` | `Union[str, Path]` | The experiment file. | *required* |

Returns:

| Type                               | Description                                          |
| ---------------------------------- | ---------------------------------------------------- |
| `tuple[ExperimentSpec, SourceMap]` | The settings, and the map from each key to its line. |

Raises:

| Type             | Description                                                  |
| ---------------- | ------------------------------------------------------------ |
| `BlueprintError` | If the file is invalid; the message names the file and line. |

Source code in `src/desflow/analysis/spec.py`

```python
def load_experiment(path: Union[str, Path]) -> "tuple[ExperimentSpec, SourceMap]":
    """
    Reads and validates an experiment file.

    Args:
        path: The experiment file.

    Returns:
        The settings, and the map from each key to its line.

    Raises:
        BlueprintError: If the file is invalid; the message names the file and line.
    """
    data, source = read_document(path)
    try:
        spec = ExperimentSpec.model_validate(data)
    except ValidationError as exc:
        raise _error(exc, source) from None
    return spec, source
```

### `desflow.analysis.SourceMap(path, lines)`

Maps a location in the document, such as `("tasks", "mill", "service")`, to a line.

Validation errors and cross-reference errors carry a location rather than a line, so this is what turns them into `file:line` messages a reader can act on.

Source code in `src/desflow/blueprint/loader.py`

```python
def __init__(self, path: Path, lines: Dict[Location, int]):
    self.path = path
    self._lines = lines
```

#### Methods:

##### `error(location, message)`

Builds a `BlueprintError` pointing at `location`.

Parameters:

| Name       | Type                        | Description                                           | Default    |
| ---------- | --------------------------- | ----------------------------------------------------- | ---------- |
| `location` | `Iterable[Union[str, int]]` | The keys and list positions from the document's root. | *required* |
| `message`  | `str`                       | What is wrong, and how to fix it.                     | *required* |

Returns:

| Name             | Type             | Description                                        |
| ---------------- | ---------------- | -------------------------------------------------- |
| `BlueprintError` | `BlueprintError` | An error whose text starts with the file and line. |

Source code in `src/desflow/blueprint/loader.py`

```python
def error(
    self, location: Iterable[Union[str, int]], message: str
) -> BlueprintError:
    """Builds a `BlueprintError` pointing at `location`.

    Args:
        location: The keys and list positions from the document's root.
        message: What is wrong, and how to fix it.

    Returns:
        BlueprintError: An error whose text starts with the file and line.
    """
    return BlueprintError(f"{self.path}:{self.line(location)}: {message}")
```

##### `has(location)`

Whether the document has a key or list item at exactly `location`.

Parameters:

| Name       | Type                        | Description                                        | Default    |
| ---------- | --------------------------- | -------------------------------------------------- | ---------- |
| `location` | `Iterable[Union[str, int]]` | The keys and list indexes from the document's top. | *required* |

Source code in `src/desflow/blueprint/loader.py`

```python
def has(self, location: Iterable[Union[str, int]]) -> bool:
    """Whether the document has a key or list item at exactly `location`.

    Args:
        location: The keys and list indexes from the document's top.
    """
    return tuple(location) in self._lines
```

##### `line(location)`

Returns the line of the deepest part of `location` the document has.

Parameters:

| Name       | Type                        | Description                                                                                 | Default    |
| ---------- | --------------------------- | ------------------------------------------------------------------------------------------- | ---------- |
| `location` | `Iterable[Union[str, int]]` | The keys and list positions from the document's root, such as ("tasks", "pack", "service"). | *required* |

Returns:

| Name  | Type  | Description                                                         |
| ----- | ----- | ------------------------------------------------------------------- |
| `int` | `int` | The line, counting from 1; 1 when no part of the location is found. |

Source code in `src/desflow/blueprint/loader.py`

```python
def line(self, location: Iterable[Union[str, int]]) -> int:
    """Returns the line of the deepest part of `location` the document has.

    Args:
        location: The keys and list positions from the document's root, such as
            `("tasks", "pack", "service")`.

    Returns:
        int: The line, counting from 1; 1 when no part of the location is found.
    """
    location = tuple(location)
    for end in range(len(location), -1, -1):
        if location[:end] in self._lines:
            return self._lines[location[:end]]
    return 1
```

### `desflow.ExperimentResult(spec, output, designs, results, summary, comparisons, checks, observed, report)`

What an experiment found, as `run_experiment` returns it and its artifacts hold it.

Attributes:

| Name          | Type             | Description                                        |
| ------------- | ---------------- | -------------------------------------------------- |
| `spec`        | `ExperimentSpec` | The experiment's settings.                         |
| `output`      | `Path`           | The folder the artifacts were written to.          |
| `designs`     | `List[Design]`   | Each variant at each sweep point, with its seeds.  |
| `results`     | `List[Dict]`     | One row per design, replication and statistic.     |
| `summary`     | `List[Dict]`     | One row per design and statistic.                  |
| `comparisons` | `List[Dict]`     | One row per compared pair, point and measure.      |
| `checks`      | `List[Dict]`     | One row per check, design and bound.               |
| `observed`    | `List[Dict]`     | Each observed value beside its simulated interval. |
| `report`      | `str`            | The Markdown report.                               |

#### Attributes

##### `failed`

The check rows that failed.

### `desflow.analysis.Design(name, variant, point, overrides, seeds, runs=list())`

One variant at one point of the sweep, with the seeds its replications use.

Attributes:

| Name        | Type             | Description                                                                                   |
| ----------- | ---------------- | --------------------------------------------------------------------------------------------- |
| `name`      | `str`            | The variant's name, followed by the swept values, such as three Shop.arrival.orders.rate=0.5. |
| `variant`   | `str`            | The variant's name.                                                                           |
| `point`     | `Dict[str, Any]` | The swept path and value, by path; empty without a sweep.                                     |
| `overrides` | `Dict[str, Any]` | Every registry path the design sets.                                                          |
| `seeds`     | `List[int]`      | The seed of each replication.                                                                 |

### `desflow.analysis.RunResult(readings, streams, sim_id, parameters=dict())`

What one unpaced run of a model measured.

Attributes:

| Name         | Type             | Description                                                                                |
| ------------ | ---------------- | ------------------------------------------------------------------------------------------ |
| `readings`   | `Dict[str, Any]` | Every statistic, by its published name.                                                    |
| `streams`    | `List[str]`      | The names of the random streams the run drew from.                                         |
| `sim_id`     | `str`            | The model's sim id.                                                                        |
| `parameters` | `Dict[str, Any]` | Every distribution and capacity by registry path, as the run started, after its overrides. |

### `desflow.CheckFailed(result)`

Bases: `Exception`

An experiment's check failed. The artifacts are written before it is raised.

Attributes:

| Name     | Type               | Description                               |
| -------- | ------------------ | ----------------------------------------- |
| `result` | `ExperimentResult` | The whole result, failed checks included. |

Source code in `src/desflow/analysis/results.py`

```python
def __init__(self, result: ExperimentResult):
    self.result = result
    names = "; ".join(
        f"{row['design']}: {row['measure']} {row['bound']} {row['limit']:g} "
        f"(checked {row['value']:.4g})"
        for row in result.failed
    )
    count = len(result.failed)
    super().__init__(
        f"{'a check' if count == 1 else f'{count} checks'} failed: {names}"
    )
```

## Input Fitting

### `desflow.fit_column(values, *, unit='s', candidates=None, lower=None, upper=None, bins=None, min_count=20, tolerance=0.25, drop_invalid=False)`

Fits a column of measured times to desflow's distribution families.

Parameters:

| Name           | Type                      | Description                                                                                                                                               | Default    |
| -------------- | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `values`       | `Sequence[Any]`           | The data, as numbers or text; an empty cell or None is missing.                                                                                           | *required* |
| `unit`         | `str`                     | The data's unit: ms, s, min or h. Settings are in seconds.                                                                                                | `'s'`      |
| `candidates`   | `Optional[Sequence[str]]` | The families to try; None tries every family of times (not poisson or geometric, which count). empirical is always offered unless the list leaves it out. | `None`     |
| `lower`        | `Optional[float]`         | The lowest plausible value, in the data's unit; a value below it is invalid, and a family that can draw below it is flagged.                              | `None`     |
| `upper`        | `Optional[float]`         | The highest plausible value, in the same way.                                                                                                             | `None`     |
| `bins`         | `Optional[int]`           | Offer the empirical distribution as a histogram with this many bins, rather than as the values themselves.                                                | `None`     |
| `min_count`    | `int`                     | The fewest values a family is fitted to. Defaults to 20.                                                                                                  | `20`       |
| `tolerance`    | `float`                   | How far a fitted family's mean and standard deviation may be from the data's, as a share of the data's. Defaults to 0.25.                                 | `0.25`     |
| `drop_invalid` | `bool`                    | Leave invalid values out rather than raising.                                                                                                             | `False`    |

Returns:

| Name        | Type        | Description                              |
| ----------- | ----------- | ---------------------------------------- |
| `FitResult` | `FitResult` | Every candidate's outcome, and the best. |

Raises:

| Type          | Description                                                                             |
| ------------- | --------------------------------------------------------------------------------------- |
| `ValueError`  | If the unit or a candidate is unknown, or a value is invalid and drop_invalid is False. |
| `ImportError` | If the fit extra is not installed.                                                      |

Source code in `src/desflow/analysis/fit.py`

```python
def fit_column(
    values: Sequence[Any],
    *,
    unit: str = "s",
    candidates: Optional[Sequence[str]] = None,
    lower: Optional[float] = None,
    upper: Optional[float] = None,
    bins: Optional[int] = None,
    min_count: int = 20,
    tolerance: float = 0.25,
    drop_invalid: bool = False,
) -> FitResult:
    """
    Fits a column of measured times to desflow's distribution families.

    Args:
        values: The data, as numbers or text; an empty cell or None is missing.
        unit: The data's unit: `ms`, `s`, `min` or `h`. Settings are in seconds.
        candidates: The families to try; None tries every family of times (not `poisson`
            or `geometric`, which count). `empirical` is
            always offered unless the list leaves it out.
        lower: The lowest plausible value, in the data's unit; a value below it is
            invalid, and a family that can draw below it is flagged.
        upper: The highest plausible value, in the same way.
        bins: Offer the empirical distribution as a histogram with this many bins,
            rather than as the values themselves.
        min_count: The fewest values a family is fitted to. Defaults to 20.
        tolerance: How far a fitted family's mean and standard deviation may be from
            the data's, as a share of the data's. Defaults to 0.25.
        drop_invalid: Leave invalid values out rather than raising.

    Returns:
        FitResult: Every candidate's outcome, and the best.

    Raises:
        ValueError: If the unit or a candidate is unknown, or a value is invalid
            and `drop_invalid` is False.
        ImportError: If the fit extra is not installed.
    """
    if unit not in _UNITS:
        raise ValueError(f"unit must be one of {', '.join(_UNITS)}, got {unit!r}")
    _check_settings(tolerance, lower, upper)
    names = (
        list(candidates)
        if candidates is not None
        else [n for n in DISTRIBUTIONS if n in _FITS or n == "empirical"]
    )
    unknown = [n for n in names if n not in DISTRIBUTIONS]
    if unknown:
        raise ValueError(
            f"no family called {', '.join(repr(n) for n in unknown)}; choose from "
            f"{', '.join(DISTRIBUTIONS)}"
        )
    scale = _UNITS[unit]
    data, missing, invalid = _clean(values, scale, lower, upper, drop_invalid)
    low = None if lower is None else lower * scale
    high = None if upper is None else upper * scale
    scipy = _scipy()
    x = np.asarray(data, dtype=float)
    found = []
    for name in names:
        if name == "empirical":
            continue
        reason = _skip(name, x, min_count, low)
        if reason:
            found.append(Candidate(name, "skipped", reason))
            continue
        try:
            parameters, k = _FITS[name](scipy, x, low)
        except Exception as exc:  # noqa: BLE001 - every failure is a table row
            found.append(Candidate(name, "did not fit", f"{type(exc).__name__}: {exc}"))
            continue
        found.append(_checked(scipy, name, parameters, k, x, tolerance, low, high))
    if "empirical" in names:
        found.append(_empirical(x, bins))
    return FitResult(
        candidates=found,
        values=list(data),
        unit=unit,
        rows=len(values),
        missing=missing,
        invalid=invalid,
        lower=low,
        upper=high,
    )
```

### `desflow.FitResult(candidates, values, unit='s', rows=0, missing=0, invalid=0, lower=None, upper=None)`

The outcome of fitting one column: every candidate, the best and the data.

Attributes:

| Name         | Type              | Description                                                                                 |
| ------------ | ----------------- | ------------------------------------------------------------------------------------------- |
| `candidates` | `List[Candidate]` | One per family tried, empirical last.                                                       |
| `values`     | `List[float]`     | The values fitted, in seconds.                                                              |
| `unit`       | `str`             | The unit the data was written in.                                                           |
| `rows`       | `int`             | Rows read.                                                                                  |
| `missing`    | `int`             | Empty cells, left out.                                                                      |
| `invalid`    | `int`             | Values that are not numbers, not finite, or outside the bounds; left out with drop_invalid. |
| `lower`      | `Optional[float]` | The lowest plausible value, in seconds, or None.                                            |
| `upper`      | `Optional[float]` | The highest plausible value, in seconds, or None.                                           |

#### Attributes

##### `best`

The fitted family with the lowest AIC that cannot draw outside the bounds; `constant` when every value is the same. When no family within the bounds fitted, the empirical distribution of the data, which stays within them; then the fitted family with the lowest AIC; None when nothing is usable.

##### `table`

One row per candidate, with its status, reason, parameters and diagnostics, and whether it is the best.

#### Methods:

##### `plot(path, kind=None)`

Draws the data against values generated from a candidate, as a PNG.

The left panel puts the histogram of the data over the histogram of generated values; the right is a Q-Q plot, each sorted data value against the generated value at the same quantile. The generated values come from desflow's `Sampler`, as a run draws them, with the same smallest delay.

Parameters:

| Name   | Type               | Description                      | Default    |
| ------ | ------------------ | -------------------------------- | ---------- |
| `path` | `Union[str, Path]` | The image to write.              | *required* |
| `kind` | `Optional[str]`    | The family; None takes the best. | `None`     |

Returns:

| Name   | Type   | Description        |
| ------ | ------ | ------------------ |
| `Path` | `Path` | The image written. |

Source code in `src/desflow/analysis/fit.py`

```python
def plot(self, path: Union[str, Path], kind: Optional[str] = None) -> Path:
    """
    Draws the data against values generated from a candidate, as a PNG.

    The left panel puts the histogram of the data over the histogram of
    generated values; the right is a Q-Q plot, each sorted data value against
    the generated value at the same quantile. The generated values come from
    desflow's `Sampler`, as a run draws them, with the same smallest delay.

    Args:
        path: The image to write.
        kind: The family; None takes the best.

    Returns:
        Path: The image written.
    """
    try:
        from matplotlib.backends.backend_agg import FigureCanvasAgg
        from matplotlib.figure import Figure
    except ImportError:
        raise ImportError(_FIT_EXTRA) from None
    settings = self.settings(kind)
    generated = _generate(settings, max(10_000, 10 * len(self.values)))
    observed = np.sort(np.asarray(self.values))
    probabilities = (np.arange(1, len(observed) + 1) - 0.5) / len(observed)
    expected = np.quantile(generated, probabilities)

    figure = Figure(figsize=(9, 3.6))
    FigureCanvasAgg(figure)
    histogram, qq = figure.subplots(1, 2)
    bins = np.histogram_bin_edges(np.concatenate([observed, generated]), bins=30)
    histogram.hist(generated, bins=bins, density=True, alpha=0.5, label="generated")
    histogram.hist(observed, bins=bins, density=True, alpha=0.7, label="observed")
    histogram.set_xlabel("seconds")
    histogram.set_ylabel("density")
    histogram.legend(frameon=False)
    histogram.set_title(f"Observed and generated: {settings['dist']}")
    qq.plot(expected, observed, "o", markersize=3)
    ends = [min(expected[0], observed[0]), max(expected[-1], observed[-1])]
    qq.plot(ends, ends, linestyle="--", linewidth=1)
    qq.set_xlabel("generated quantile (s)")
    qq.set_ylabel("observed value (s)")
    qq.set_title("Q-Q")
    figure.tight_layout()
    path = Path(path)
    figure.savefig(path)
    return path
```

##### `settings(kind=None)`

Returns a usable candidate's settings, for `add_service(name, **settings)`.

Parameters:

| Name   | Type            | Description                      | Default |
| ------ | --------------- | -------------------------------- | ------- |
| `kind` | `Optional[str]` | The family; None takes the best. | `None`  |

Returns:

| Type             | Description                                                                                                                                                                                    |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Dict[str, Any]` | Dict\[str, Any\]: dist and the family's parameters, in seconds, with six significant digits, the numbers yaml writes, so a script and a blueprint that load the same fit draw the same values. |

Raises:

| Type         | Description                                            |
| ------------ | ------------------------------------------------------ |
| `ValueError` | If no candidate is usable, or kind is not one that is. |

Source code in `src/desflow/analysis/fit.py`

```python
def settings(self, kind: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns a usable candidate's settings, for `add_service(name, **settings)`.

    Args:
        kind: The family; None takes the best.

    Returns:
        Dict[str, Any]: `dist` and the family's parameters, in seconds, with six
            significant digits, the numbers `yaml` writes, so a script and a
            blueprint that load the same fit draw the same values.

    Raises:
        ValueError: If no candidate is usable, or `kind` is not one that is.
    """
    name = kind or self.best
    for candidate in self.candidates:
        if candidate.family == name and candidate.usable:
            return {
                "dist": name,
                **{
                    key: [_rounded(v) for v in value]
                    if isinstance(value, list)
                    else _rounded(value)
                    for key, value in candidate.parameters.items()
                },
            }
    usable = [c.family for c in self.candidates if c.usable]
    if not usable:
        raise ValueError(
            "no candidate is usable; the table says why for each. empirical "
            "fits any values, so give the column values, or keep empirical in "
            "the candidates (--candidates)"
        )
    raise ValueError(f"{name} is not usable; choose one of {', '.join(usable)}")
```

##### `text()`

The table as text: one line per candidate, the best marked.

Source code in `src/desflow/analysis/fit.py`

```python
def text(self) -> str:
    """The table as text: one line per candidate, the best marked."""
    lines = [
        f"{self.rows} rows: {len(self.values)} used, {self.missing} missing, "
        f"{self.invalid} invalid",
        f"{'family':<17} {'status':<12} {'AIC':>10} "
        f"{'KS stat':>8} {'KS p (approx.)':>14}"
        "  parameters or reason",
    ]
    best = self.best
    for c in self.candidates:
        detail = (
            ", ".join(
                f"{k}: {len(v)} numbers"
                if isinstance(v, list)
                else f"{k}={_short(v)}"
                for k, v in c.parameters.items()
            )
            if c.usable
            else c.reason
        )
        if c.bounds:
            detail += f" ({c.bounds})"
        lines.append(
            f"{c.family + (' *' if c.family == best else ''):<17} {c.status:<12} "
            f"{_number(c.aic):>10} {_number(c.ks_statistic):>8} "
            f"{_number(c.ks_p_value):>14}  {detail}"
        )
    lines.append(f"* best: {best}" if best else "No candidate is usable.")
    return "\n".join(lines)
```

##### `yaml(kind=None, source='')`

The settings as a YAML mapping a blueprint loads with `!include`.

Parameters:

| Name     | Type            | Description                                           | Default |
| -------- | --------------- | ----------------------------------------------------- | ------- |
| `kind`   | `Optional[str]` | The family; None takes the best.                      | `None`  |
| `source` | `str`           | Where the data came from, for the comment at the top. | `''`    |

Returns:

| Name  | Type  | Description                                                                                                                                                                                                                                        |
| ----- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `str` | `str` | A comment, then dist and each parameter with the numbers settings returns, in the layout of the repository's blueprint files (CONTRIBUTING.md, Blueprint Layout): no line over 88 characters where its words allow, and a blank line between keys. |

Source code in `src/desflow/analysis/fit.py`

```python
def yaml(self, kind: Optional[str] = None, source: str = "") -> str:
    """
    The settings as a YAML mapping a blueprint loads with `!include`.

    Args:
        kind: The family; None takes the best.
        source: Where the data came from, for the comment at the top.

    Returns:
        str: A comment, then `dist` and each parameter with the numbers
            `settings` returns, in the layout of the repository's blueprint
            files (`CONTRIBUTING.md`, Blueprint Layout): no line over 88
            characters where its words allow, and a blank line between keys.
    """
    settings = self.settings(kind)
    header = (
        f"Fitted by desflow fit{' from ' + source if source else ''}: "
        f"{settings['dist']}, from {len(self.values)} values in {self.unit}, "
        "written in seconds."
    )
    lines = [
        f"# {part}"
        for part in textwrap.wrap(
            header,
            width=_WIDTH - 2,
            break_long_words=False,
            break_on_hyphens=False,
        )
    ]
    for index, (key, value) in enumerate(settings.items()):
        if index:
            lines.append("")
        if isinstance(value, list):
            lines.extend(_flow_list(key, [repr(v) for v in value]))
        else:
            lines.append(
                f"{key}: {repr(value) if isinstance(value, float) else value}"
            )
    return "\n".join(lines) + "\n"
```

### `desflow.analysis.Candidate(family, status, reason='', parameters=dict(), log_likelihood=None, aic=None, ks_statistic=None, ks_p_value=None, bounds='')`

One family's outcome: its parameters, diagnostics and status.

Attributes:

| Name             | Type              | Description                                                                                              |
| ---------------- | ----------------- | -------------------------------------------------------------------------------------------------------- |
| `family`         | `str`             | The family, as dist names it.                                                                            |
| `status`         | `str`             | fitted, rejected, did not fit or skipped.                                                                |
| `reason`         | `str`             | Why it was rejected, did not fit or was skipped; empty when fitted.                                      |
| `parameters`     | `Dict[str, Any]`  | desflow's parameters, in seconds.                                                                        |
| `log_likelihood` | `Optional[float]` | The log-likelihood of the data.                                                                          |
| `aic`            | `Optional[float]` | Akaike's information criterion, 2k minus twice the log-likelihood, where k counts the fitted parameters. |
| `ks_statistic`   | `Optional[float]` | The Kolmogorov-Smirnov distance between the data and the fitted family.                                  |
| `ks_p_value`     | `Optional[float]` | Its p-value. It is approximate, because the parameters were estimated from the same data.                |
| `bounds`         | `str`             | Where the family can draw outside lower or upper; empty when it cannot.                                  |

#### Attributes

##### `usable`

True when the family was fitted and its result passed every check.

### `desflow.analysis.read_column(path, column)`

Reads one column of a CSV file, as text, in row order.

Parameters:

| Name     | Type               | Description                      | Default    |
| -------- | ------------------ | -------------------------------- | ---------- |
| `path`   | `Union[str, Path]` | A CSV file with a header row.    | *required* |
| `column` | `str`              | The column's name in the header. | *required* |

Returns:

| Type        | Description                                                                  |
| ----------- | ---------------------------------------------------------------------------- |
| `List[str]` | List\[str\]: Each row's cell, empty where the cell is empty, for fit_column. |

Raises:

| Type         | Description                     |
| ------------ | ------------------------------- |
| `ValueError` | If the file has no such column. |

Source code in `src/desflow/analysis/fit.py`

```python
def read_column(path: Union[str, Path], column: str) -> List[str]:
    """
    Reads one column of a CSV file, as text, in row order.

    Args:
        path: A CSV file with a header row.
        column: The column's name in the header.

    Returns:
        List[str]: Each row's cell, empty where the cell is empty, for `fit_column`.

    Raises:
        ValueError: If the file has no such column.
    """
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if column not in (reader.fieldnames or []):
            raise ValueError(
                f"{path} has no column {column!r}; it has "
                f"{', '.join(reader.fieldnames or [])}"
            )
        return [row[column] for row in reader]
```

## Explaining a Model

`SimulationContext.analyze` is the usual entry point, and `desflow explain` prints the same `Explanation`. `build_graph`, `analyze` and `verify` are the steps it takes, and `render_terminal` and `to_json` print the result. The JSON form is experimental through 1.x.

### `desflow.explain.Explanation`

Bases: `_Model`

A model's graph and its analysis.

Attributes:

| Name                | Type                     | Description                                                   |
| ------------------- | ------------------------ | ------------------------------------------------------------- |
| `schema_version`    | `Literal['0']`           | The version of this shape, "0" while it is experimental.      |
| `sim_id`            | `str`                    | The model's sim id.                                           |
| `target`            | `float`                  | The utilisation the hints aim for.                            |
| `graph`             | `Graph`                  | The graph.                                                    |
| `arrivals`          | `List[ArrivalResult]`    | Each arrival.                                                 |
| `tasks`             | `List[TaskResult]`       | Each task.                                                    |
| `resources`         | `List[ResourceResult]`   | Each resource.                                                |
| `holders`           | `List[HolderResult]`     | Each store and container.                                     |
| `bottleneck`        | `Optional[str]`          | The resource with the highest utilisation the formulas cover. |
| `bottleneck_reason` | `Optional[str]`          | Why no bottleneck can be named, when none is.                 |
| `hints`             | `List[Hint]`             | Changes that bring each resource to target.                   |
| `verification`      | `Optional[Verification]` | What --verify found, when it ran.                             |

### `desflow.explain.build_graph(context)`

Builds the graph of a context from what it declares, without starting it.

Parameters:

| Name      | Type                | Description                                       | Default    |
| --------- | ------------------- | ------------------------------------------------- | ---------- |
| `context` | `SimulationContext` | A builder or YAML context, before or after a run. | *required* |

Returns:

| Name    | Type    | Description                                                                                                |
| ------- | ------- | ---------------------------------------------------------------------------------------------------------- |
| `Graph` | `Graph` | Its arrivals, tasks, resources, stores, containers, opaque code and the exit, with the edges between them. |

Source code in `src/desflow/explain/graph.py`

```python
def build_graph(context: "SimulationContext") -> Graph:
    """Builds the graph of a context from what it declares, without starting it.

    Args:
        context: A builder or YAML context, before or after a run.

    Returns:
        Graph: Its arrivals, tasks, resources, stores, containers, opaque code and
            the exit, with the edges between them.
    """
    parts = parts_of(context)
    nodes: List[Node] = []
    edges: List[Edge] = []
    for name, config in parts.arrivals.configs.items():
        nodes.append(
            Node(
                id=node_id("arrival", name),
                kind="arrival",
                label=name,
                distribution=_parameters(config),
                features=features.arrival_features(context, name),
            )
        )
    for name in parts.arrivals.replays:
        nodes.append(
            Node(
                id=node_id("arrival", name),
                kind="arrival",
                label=name,
                features=features.arrival_features(context, name),
            )
        )
    for name, plan in parts.arrivals.plans.items():
        # An arrival with `initial` alone draws no gaps, and still starts its task
        if plan.gap is not None or plan.sourceless:
            nodes.append(
                Node(
                    id=node_id("arrival", name),
                    kind="arrival",
                    label=name,
                    features=features.arrival_features(context, name),
                )
            )
    for arrival, task in parts.arrivals.spawns.items():
        edges.append(
            Edge(
                source=node_id("arrival", arrival),
                target=node_id("task", task),
                kind="spawn",
            )
        )
    _add_opaque(context, nodes, edges)
    ends = False
    for name, spec in parts.tasks.specs.items():
        service = parts.services.get(spec.service_id or "")
        nodes.append(
            Node(
                id=node_id("task", name),
                kind="task",
                label=name,
                distribution=_parameters(service),
                features=features.task_features(context, spec),
                priority=features.priority(spec),
            )
        )
        ends = task_edges(spec, nodes, edges) or ends
    edges += signal_edges(parts.tasks.specs, parts.signals.on)
    for name, capacity in parts.holders.resources_config.items():
        current = 1 if capacity.current_cap is None else capacity.current_cap
        most = current if capacity.max_cap is None else capacity.max_cap
        nodes.append(
            Node(
                id=node_id("resource", name),
                kind="resource",
                label=name,
                capacity=_units(current),
                max_capacity=_units(most),
                features=features.resource_features(context, name),
                queue=features.queue_order(context, name),
            )
        )
    for kind, configs in (
        ("store", parts.holders.stores_config),
        ("container", parts.holders.containers_config),
    ):
        for name in configs:
            nodes.append(Node(id=node_id(kind, name), kind=kind, label=name))
    if ends:
        nodes.append(Node(id=EXIT, kind="exit", label="exit"))
    return Graph(
        sim_id=context.sim_id,
        nodes=nodes,
        edges=edges,
        features=features.model_features(context),
    )
```

### `desflow.explain.Graph`

Bases: `_Model`

A model as nodes and edges, read from its declared linkage.

Attributes:

| Name       | Type         | Description                                                                                  |
| ---------- | ------------ | -------------------------------------------------------------------------------------------- |
| `sim_id`   | `str`        | The model's sim id.                                                                          |
| `nodes`    | `List[Node]` | Every node.                                                                                  |
| `edges`    | `List[Edge]` | Every edge.                                                                                  |
| `features` | `List[str]`  | What changes demand or capacity during the run, for the whole model, such as scenario steps. |

### `desflow.explain.Node`

Bases: `_Model`

One part of the model.

Attributes:

| Name           | Type                       | Description                                                                                                                                                     |
| -------------- | -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`           | `str`                      | Unique in the graph: the kind and the name, such as task:pack_order.                                                                                            |
| `kind`         | `NodeKind`                 | arrival, task, resource, store, container, opaque or exit.                                                                                                      |
| `label`        | `str`                      | The name in the model, such as pack_order.                                                                                                                      |
| `opaque`       | `bool`                     | True for code explain cannot read, such as a hand-written arrival_loop or an add_process function.                                                              |
| `distribution` | `Optional[Dict[str, Any]]` | An arrival's gaps or a task's service, as its parameters such as {"dist": "exponential", "rate": 1.8}; None for a replayed arrival or a task without a service. |
| `capacity`     | `Optional[int]`            | A resource's current_cap; None for no limit.                                                                                                                    |
| `max_capacity` | `Optional[int]`            | A resource's max_cap; None for no limit.                                                                                                                        |
| `features`     | `List[str]`                | What the queueing formulas leave out, such as patience.                                                                                                         |
| `queue`        | `Optional[str]`            | A resource's queue order: fifo, lifo or priority.                                                                                                               |
| `priority`     | `Optional[str]`            | A task's priority as it was given: a number, an expression's text, or function; None for none, which ranks 0.                                                   |

### `desflow.explain.Edge`

Bases: `_Model`

A link between two nodes.

Attributes:

| Name          | Type              | Description                                                                                                                                                                             |
| ------------- | ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `source`      | `str`             | The id of the node it leaves.                                                                                                                                                           |
| `target`      | `str`             | The id of the node it reaches.                                                                                                                                                          |
| `kind`        | `EdgeKind`        | spawn (an arrival or a task starts an entity at a task), uses (a task holds a resource), next (a task's route to the next task or to the exit), takes and gives (a task's get and put). |
| `probability` | `Optional[float]` | The share of the source's entities that take a next edge, where the rules fix it; None where the run decides it.                                                                        |

### `desflow.explain.ArrivalResult`

Bases: `_Model`

An arrival's rate and the variability of its gaps.

Attributes:

| Name     | Type              | Description                                       |
| -------- | ----------------- | ------------------------------------------------- |
| `name`   | `str`             | The arrival's name.                               |
| `rate`   | `Optional[float]` | λ, entities a second; None when unknown.          |
| `ca2`    | `Optional[float]` | The squared coefficient of variation of the gaps. |
| `reason` | `Optional[str]`   | Why the rate is unknown.                          |

### `desflow.explain.CapacityRow`

Bases: `_Model`

Utilisation at one number of units.

Attributes:

| Name          | Type     | Description                |
| ------------- | -------- | -------------------------- |
| `capacity`    | `int`    | The number of units.       |
| `utilization` | `float`  | ρ at that number.          |
| `status`      | `Status` | OK, CONGESTED or UNSTABLE. |

### `desflow.explain.Comparison`

Bases: `_Model`

One analytical value beside the value a run measured.

Attributes:

| Name         | Type              | Description                                                                                                                                                                                    |
| ------------ | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `node`       | `str`             | The resource, task or arrival.                                                                                                                                                                 |
| `measure`    | `str`             | What is compared: utilization, queue_length, wait or throughput.                                                                                                                               |
| `analytical` | `Optional[float]` | The value of the analysis; None where it has none.                                                                                                                                             |
| `measured`   | `Optional[float]` | The run's value, or the mean of the replications; None when a run has no reading for it.                                                                                                       |
| `half_width` | `Optional[float]` | The half-width of the replications' interval.                                                                                                                                                  |
| `differs`    | `Optional[bool]`  | True when they disagree: by more than 10 percent for one run; for replications, by more than the interval's half-width and more than 1 percent; None without an analytical value or a reading. |
| `reason`     | `Optional[str]`   | Why the measure is not compared: where analytical is None, or where a run has no reading.                                                                                                      |

### `desflow.explain.Hint`

Bases: `_Model`

A change that brings a resource to the target utilisation.

Attributes:

| Name       | Type                         | Description                                                                  |
| ---------- | ---------------------------- | ---------------------------------------------------------------------------- |
| `resource` | `str`                        | The resource's name.                                                         |
| `kind`     | `Literal['servers', 'rate']` | servers (a number of units) or rate (the rate into it at its current units). |
| `value`    | `float`                      | The number of units, or the rate.                                            |
| `text`     | `str`                        | The hint as a sentence.                                                      |

### `desflow.explain.HolderResult`

Bases: `_Model`

A store's or container's put and get rates.

Attributes:

| Name       | Type                            | Description                                                  |
| ---------- | ------------------------------- | ------------------------------------------------------------ |
| `name`     | `str`                           | Its name.                                                    |
| `kind`     | `Literal['store', 'container']` | store or container.                                          |
| `put_rate` | `Optional[float]`               | Puts a second from the tasks that add to it, where known.    |
| `get_rate` | `Optional[float]`               | Gets a second from the tasks that take from it, where known. |
| `reason`   | `str`                           | Why there is no queueing analysis.                           |

### `desflow.explain.ResourceResult`

Bases: `_Model`

A resource's load, utilisation, status and wait estimate.

Attributes:

| Name           | Type                | Description                                                                                                                                                                  |
| -------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`         | `str`               | The resource's name.                                                                                                                                                         |
| `rate`         | `Optional[float]`   | λ, requests a second, summed over its tasks.                                                                                                                                 |
| `service_mean` | `Optional[float]`   | τ, the mean service time weighted by each task's rate.                                                                                                                       |
| `service_rate` | `Optional[float]`   | μ = 1 / τ.                                                                                                                                                                   |
| `servers`      | `Optional[int]`     | c, its current_cap; None for no limit.                                                                                                                                       |
| `utilization`  | `Optional[float]`   | ρ = λ τ / c.                                                                                                                                                                 |
| `status`       | `Optional[Status]`  | OK below 0.85, CONGESTED from 0.85, UNSTABLE from 1.                                                                                                                         |
| `wait`         | `Optional[float]`   | Wq, the mean wait in the queue, where a formula applies.                                                                                                                     |
| `queue_length` | `Optional[float]`   | Lq = λ Wq.                                                                                                                                                                   |
| `in_system`    | `Optional[float]`   | L = λ (Wq + τ).                                                                                                                                                              |
| `method`       | `Optional[str]`     | The formula behind the wait: kingman (exact, for a Poisson input), kingman_approx (an approximation, for other gaps, which --verify does not compare), erlang_c or no_limit. |
| `reason`       | `Optional[str]`     | Why the utilisation is unknown.                                                                                                                                              |
| `wait_reason`  | `Optional[str]`     | Why the wait is not estimated.                                                                                                                                               |
| `capacities`   | `List[CapacityRow]` | ρ at each number of units the resource allows.                                                                                                                               |

### `desflow.explain.TaskResult`

Bases: `_Model`

A task's rate from flow balance, and its service.

Attributes:

| Name           | Type              | Description                                               |
| -------------- | ----------------- | --------------------------------------------------------- |
| `name`         | `str`             | The task's name.                                          |
| `rate`         | `Optional[float]` | λ, instances started a second; None when unknown.         |
| `service_mean` | `float`           | The effective mean service time; 0 without a service.     |
| `cs2`          | `float`           | The squared coefficient of variation of the service time. |
| `reason`       | `Optional[str]`   | Why the rate is unknown.                                  |

### `desflow.explain.Verification`

Bases: `_Model`

What `--verify` ran and found.

Attributes:

| Name           | Type               | Description                                              |
| -------------- | ------------------ | -------------------------------------------------------- |
| `until`        | `float`            | The end of each run.                                     |
| `warmup`       | `Optional[float]`  | The model's warm-up; the measures leave it out.          |
| `seed`         | `int`              | The seed, or the seed the replications' seeds come from. |
| `replications` | `Optional[int]`    | The number of runs, or None for one.                     |
| `comparisons`  | `List[Comparison]` | Every comparison.                                        |
| `note`         | `str`              | What the check does and does not show.                   |

### `desflow.explain.analyze(graph, target=0.85)`

Analyses a graph: rates by flow balance, then each resource's load and wait.

A value is given only where the formulas cover it (`support.py`); everywhere else it is None, with the reason.

Parameters:

| Name     | Type    | Description                                                | Default    |
| -------- | ------- | ---------------------------------------------------------- | ---------- |
| `graph`  | `Graph` | The graph build_graph made.                                | *required* |
| `target` | `float` | The utilisation the hints aim for, above 0 and at most 10. | `0.85`     |

Returns:

| Name          | Type          | Description                 |
| ------------- | ------------- | --------------------------- |
| `Explanation` | `Explanation` | The graph and its analysis. |

Raises:

| Type         | Description                |
| ------------ | -------------------------- |
| `ValueError` | If target is out of range. |

Source code in `src/desflow/explain/analysis.py`

```python
def analyze(graph: Graph, target: float = 0.85) -> Explanation:
    """Analyses a graph: rates by flow balance, then each resource's load and wait.

    A value is given only where the formulas cover it (`support.py`); everywhere
    else it is None, with the reason.

    Args:
        graph: The graph `build_graph` made.
        target: The utilisation the hints aim for, above 0 and at most 10.

    Returns:
        Explanation: The graph and its analysis.

    Raises:
        ValueError: If `target` is out of range.
    """
    if not 0 < target <= 10:
        raise ValueError(f"target must be above 0 and at most 10, got {target:g}")
    arrivals = arrival_results(graph)
    tasks = task_results(graph, arrivals)
    by_id = {f"task:{t.name}": t for t in tasks}
    users = loads.users(graph)
    functions = loads.processes(graph)
    nodes = {node.id: node for node in graph.nodes}
    marks = support.static_marks(graph)
    while True:
        _unmark({**by_id, **{f"arrival:{a.name}": a for a in arrivals}}, marks)
        resources = [
            loads.load(node, [by_id[t] for t in users[node.id]], functions)
            for node in loads.resources(graph)
        ]
        before = len(marks)
        for node, r in zip(loads.resources(graph), resources):
            if r.status == "UNSTABLE":
                support.mark(
                    graph,
                    marks,
                    support.downstream(graph, users[node.id]),
                    f"its work comes through unstable {r.name}",
                )
        if len(marks) == before:
            break
    passes = stations(graph, by_id, users, {f"resource:{r.name}": r for r in resources})
    poisson = poisson_tasks(graph, by_id, passes)
    for node, resource in zip(loads.resources(graph), resources):
        if loads.mixed_priorities(node, [nodes[t] for t in users[node.id]]):
            resource.wait_reason = resource.reason or _PRIORITY
            continue
        fill_wait(graph, node, resource, users[node.id], by_id, poisson, arrivals)
    bottleneck, why = loads.bottleneck(resources)
    found = []
    if not functions:
        for node, resource in zip(loads.resources(graph), resources):
            found += hints(resource, target, node.max_capacity)
    return Explanation(
        sim_id=graph.sim_id,
        target=target,
        graph=graph,
        arrivals=arrivals,
        tasks=tasks,
        resources=resources,
        holders=holder_results(graph, by_id),
        bottleneck=bottleneck,
        bottleneck_reason=why,
        hints=found,
    )
```

### desflow.explain.verify

```python
verify(
    model,
    explanation,
    until=None,
    seed=None,
    replications=None,
)
```

Runs the model unpaced and adds the comparison to the explanation.

Each run is a replication's run (`run_once`): no ingress, no egress, statistics at `typical`. It compares, per resource, `utilization_avg` / 100 with ρ and `queue_length_avg` with Lq; per task with one resource, `wait_mean` with Wq; per arrival, `flows.<arrival>.finished` over the time after the warm-up with λ.

Parameters:

| Name           | Type                      | Description                                                                                                      | Default    |
| -------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------- | ---------- |
| `model`        | `Any`                     | A blueprint path, or a function that builds the context again.                                                   | *required* |
| `explanation`  | `Explanation`             | The analysis to compare with.                                                                                    | *required* |
| `until`        | `Union[float, str, None]` | The end of each run; the blueprint's run.until when None.                                                        | `None`     |
| `seed`         | `Optional[int]`           | The run's seed, or the seed the replications' seeds come from; the model's random_seed, or one drawn, when None. | `None`     |
| `replications` | `Optional[int]`           | The number of runs, 2 or more; None for one run.                                                                 | `None`     |

Returns:

| Name          | Type          | Description                                  |
| ------------- | ------------- | -------------------------------------------- |
| `Explanation` | `Explanation` | A copy of explanation with its verification. |

Raises:

| Type         | Description                                                                                                                                  |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If the model is a LowLevelModel, there is no end time, the end is not after the warm-up, or replications is not a whole number of 2 or more. |

Source code in `src/desflow/explain/verify.py`

```python
def verify(
    model: Any,
    explanation: Explanation,
    until: Union[float, str, None] = None,
    seed: Optional[int] = None,
    replications: Optional[int] = None,
) -> Explanation:
    """Runs the model unpaced and adds the comparison to the explanation.

    Each run is a replication's run (`run_once`): no ingress, no egress, statistics
    at `typical`. It compares, per resource, `utilization_avg` / 100 with ρ and
    `queue_length_avg` with Lq; per task with one resource, `wait_mean` with Wq; per
    arrival, `flows.<arrival>.finished` over the time after the warm-up with λ.

    Args:
        model: A blueprint path, or a function that builds the context again.
        explanation: The analysis to compare with.
        until: The end of each run; the blueprint's `run.until` when None.
        seed: The run's seed, or the seed the replications' seeds come from; the
            model's `random_seed`, or one drawn, when None.
        replications: The number of runs, 2 or more; None for one run.

    Returns:
        Explanation: A copy of `explanation` with its `verification`.

    Raises:
        ValueError: If the model is a `LowLevelModel`, there is no end time, the end
            is not after the warm-up, or `replications` is not a whole number of 2 or
            more.
    """
    from desflow.analysis.intervals import interval
    from desflow.analysis.models import context_of, run_once, seconds, spawn_seeds
    from desflow.analysis.runner import run_replications
    from desflow.core.context import parts_of
    from desflow.core.statistics import published_value

    if replications is not None:
        checked_whole(replications, "replications", 2)
    if seed is not None:
        checked_whole(seed, "seed", 0)
    context = context_of(model)
    if context is None:
        raise ValueError(
            "verify needs a model it can build again; a LowLevelModel cannot be "
            "verified, so pass a blueprint path or a factory"
        )
    end = seconds(until)
    if end is None:
        end = seconds(parts_of(context).defaults.until)
    if end is None:
        raise ValueError(
            "verify needs an end time: pass until, or set run.until in the blueprint"
        )
    start = context.initial_time + (context.warmup or 0.0)
    if end <= start:
        raise ValueError(window_error(end, start, "verify"))
    if seed is None:
        seed = context.random_seed
    if seed is None:
        seed = secrets.randbelow(2**32)
    if replications is None:
        results = [run_once(model, seed=seed, until=end, stats="typical")]
    else:
        results = run_replications(
            model, spawn_seeds(seed, replications), until=end, stats="typical"
        )
    comparisons = []
    seen = {name for r in results for name in r.readings}
    for pair in pairs(explanation, end - start):
        found = [
            published_value(
                r.readings,
                f"{r.sim_id}.stats.{pair.statistic}",
                seen,
                prefix=f"{r.sim_id}.stats.",
            )
            for r in results
        ]
        numbers = [float(v) * pair.scale for v in found if v is not None]
        # A run in which nothing it measures happened has no reading
        complete = numbers if len(numbers) == len(found) else None
        comparisons.append(compare(pair, complete, interval))
    return explanation.model_copy(
        update={
            "verification": Verification(
                until=end,
                warmup=context.warmup,
                seed=seed,
                replications=replications,
                comparisons=comparisons,
                note=NOTE,
            )
        }
    )
```

### `desflow.explain.render_terminal(explanation, width=None)`

Returns the explanation as plain text: the graph as a tree from each arrival, the resources, the units each allows, the bottleneck, what is not estimated and why, the hints, and with `--verify` the comparison.

Parameters:

| Name          | Type            | Description                                                | Default    |
| ------------- | --------------- | ---------------------------------------------------------- | ---------- |
| `explanation` | `Explanation`   | What analyze or context.analyze() returned.                | *required* |
| `width`       | `Optional[int]` | The width in characters; the terminal's, or 80, when None. | `None`     |

Returns:

| Name  | Type  | Description                    |
| ----- | ----- | ------------------------------ |
| `str` | `str` | The text, ending in a newline. |

Source code in `src/desflow/explain/render.py`

```python
def render_terminal(explanation: Explanation, width: Optional[int] = None) -> str:
    """Returns the explanation as plain text: the graph as a tree from each arrival,
    the resources, the units each allows, the bottleneck, what is not estimated and
    why, the hints, and with `--verify` the comparison.

    Args:
        explanation: What `analyze` or `context.analyze()` returned.
        width: The width in characters; the terminal's, or 80, when None.

    Returns:
        str: The text, ending in a newline.
    """
    # An ASCII file makes Rich draw the tree and the tables with ASCII characters,
    # so the text reads the same in any terminal and on a docs page
    out = _AsciiText()
    console = Console(
        file=out,
        width=width,
        color_system=None,
        highlight=False,
        emoji=False,
        soft_wrap=False,
    )
    console.print(_tree(explanation))
    console.print(resources(explanation))
    for resource in explanation.resources:
        if len(resource.capacities) > 1:
            console.print(capacities(resource))
    if explanation.bottleneck is not None:
        console.print(f"Bottleneck: {explanation.bottleneck}", soft_wrap=True)
    elif explanation.bottleneck_reason is not None:
        why = _while_verifying(explanation, explanation.bottleneck_reason)
        console.print(
            f"Bottleneck: none can be named: {why}",
            soft_wrap=True,
        )
    notes = _not_estimated(explanation)
    if notes:
        console.print("Not estimated:", soft_wrap=True)
        for line in notes:
            console.print(f"  {line}", soft_wrap=True)
    if explanation.hints:
        console.print(f"Hints for utilisation {explanation.target:g}:", soft_wrap=True)
        for hint in explanation.hints:
            console.print(f"  {hint.text}", soft_wrap=True)
    if explanation.verification is not None:
        console.print(comparisons(explanation))
        skipped = [c for c in explanation.verification.comparisons if c.reason]
        if skipped:
            console.print("Not compared:", soft_wrap=True)
            for c in skipped:
                console.print(f"  {c.node} {c.measure}: {c.reason}", soft_wrap=True)
        console.print(explanation.verification.note, soft_wrap=True)
    # Rich pads each table line to its width; the padding carries nothing
    return "".join(line.rstrip() + "\n" for line in out.getvalue().splitlines())
```

### `desflow.explain.to_json(explanation)`

Returns the explanation as JSON, with `schema_version` "0".

The shape is experimental through 1.x; `explain.schema.json`, beside this module, is its schema.

Parameters:

| Name          | Type          | Description                                 | Default    |
| ------------- | ------------- | ------------------------------------------- | ---------- |
| `explanation` | `Explanation` | What analyze or context.analyze() returned. | *required* |

Returns:

| Name  | Type  | Description    |
| ----- | ----- | -------------- |
| `str` | `str` | The JSON text. |

Source code in `src/desflow/explain/render.py`

```python
def to_json(explanation: Explanation) -> str:
    """Returns the explanation as JSON, with `schema_version` "0".

    The shape is experimental through 1.x; `explain.schema.json`, beside this
    module, is its schema.

    Args:
        explanation: What `analyze` or `context.analyze()` returned.

    Returns:
        str: The JSON text.
    """
    return json.dumps(explanation.model_dump(mode="json"), indent=2)
```

## Environment

### `desflow.DynamicRealtimeEnvironment(initial_time=0, factor=1.0, strict=None, logical_start_time=None, go_live_at=None, timezone=None, on_unhandled_failure='stop')`

Bases: `RealtimeEnvironment`, `RegistryMixIn`, `IngressMixIn`, `EgressMixIn`, `StatisticsMixIn`

The core simulation engine for `desflow`.

This environment extends SimPy's `RealtimeEnvironment` by incorporating a centralized `SimulationRegistry` for dynamic state updates, and MixIns for managing high-throughput asynchronous I/O with external systems.

Attributes:

| Name             | Type       | Description                                                                |
| ---------------- | ---------- | -------------------------------------------------------------------------- |
| `start_datetime` | `datetime` | The real-world clock time when the simulation started, with its time zone. |
| `zone`           | `tzinfo`   | The time zone timestamps are written in.                                   |

Initializes the real-time simulation environment.

Parameters:

| Name                   | Type       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Default  |
| ---------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| `initial_time`         | `float`    | The initial simulation time. Defaults to 0.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | `0`      |
| `factor`               | `float`    | The real-time factor (e.g., 1.0 = 1 sim second per real second). Defaults to 1.0.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `1.0`    |
| `strict`               | `bool`     | If True, a step raises RuntimeError when the run is more than factor real seconds behind the wall clock, as SimPy's RealtimeEnvironment does. At factor 0 that is any step once the wall clock has moved, as in SimPy; before go_live_at it is never checked. Pass False for a live run whose sinks can block. sync() moves only the wall-clock anchor to now; resume_pacing() moves both anchors. By default (None) it is True for a paced run or one with go_live_at, and False at factor 0 with no go_live_at, where an unpaced run has no wall clock to fall behind. | `None`   |
| `logical_start_time`   | `datetime` | Overrides the environment's base clock. Crucial for historical backfilling (e.g., generating data from last week).                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `None`   |
| `go_live_at`           | `datetime` | Logical instant at which pacing switches to real time. Until then the run uses factor, which is usually 0.0 so a backdated history is produced as fast as the machine allows. From that instant one simulated second takes one real second, so a backfill and a live tail run in one process instead of two.                                                                                                                                                                                                                                                             | `None`   |
| `timezone`             | `str`      | An IANA time zone name, such as Australia/Melbourne, that timestamps are written in, with their offset. By default the zone of logical_start_time, or UTC. A logical_start_time or go_live_at without an offset is read in it, and the default start is now in it.                                                                                                                                                                                                                                                                                                       | `None`   |
| `on_unhandled_failure` | `str`      | stop or continue. Under stop a one-shot signal failure that nobody waits for raises SignalFailed and stops the run. Under continue it is defused and the run goes on. Defaults to stop.                                                                                                                                                                                                                                                                                                                                                                                  | `'stop'` |

The clock runs in UTC, so a daylight-saving change in the zone never adds or drops simulation time. An instant at or before the start of the run makes the whole run real time, which is the degenerate case of the same rule.

Raises:

| Type         | Description                                                                                 |
| ------------ | ------------------------------------------------------------------------------------------- |
| `ValueError` | If timezone is not an IANA time zone name, or on_unhandled_failure is not stop or continue. |

Source code in `src/desflow/core/environment.py`

```python
def __init__(
    self,
    initial_time=0,
    factor=1.0,
    strict: Optional[bool] = None,
    logical_start_time: Optional[datetime] = None,
    go_live_at: Optional[datetime] = None,
    timezone: Optional[str] = None,
    on_unhandled_failure: str = "stop",
):
    """
    Initializes the real-time simulation environment.

    Args:
        initial_time (float, optional): The initial simulation time. Defaults to 0.
        factor (float, optional): The real-time factor (e.g., 1.0 = 1 sim second per real second). Defaults to 1.0.
        strict (bool, optional): If True, a step raises `RuntimeError` when the
            run is more than `factor` real seconds behind the wall clock, as SimPy's
            `RealtimeEnvironment` does. At `factor` 0 that is any step once the
            wall clock has moved, as in SimPy; before `go_live_at` it is never
            checked. Pass False for a live run whose sinks can block. `sync()`
            moves only the wall-clock anchor to now; `resume_pacing()` moves both
            anchors. By default (None) it is True for a paced run or one with
            `go_live_at`, and False at `factor` 0 with no `go_live_at`, where an
            unpaced run has no wall clock to fall behind.
        logical_start_time (datetime, optional): Overrides the environment's base clock.
            Crucial for historical backfilling (e.g., generating data from last week).
        go_live_at (datetime, optional): Logical instant at which pacing switches to
            real time. Until then the run uses `factor`, which is usually 0.0 so a
            backdated history is produced as fast as the machine allows. From that
            instant one simulated second takes one real second, so a backfill and a
            live tail run in one process instead of two.
        timezone (str, optional): An IANA time zone name, such as
            `Australia/Melbourne`, that timestamps are written in, with their
            offset. By default the zone of `logical_start_time`, or UTC. A
            `logical_start_time` or `go_live_at` without an offset is read in it,
            and the default start is now in it.
        on_unhandled_failure (str, optional): `stop` or `continue`. Under `stop`
            a one-shot signal failure that nobody waits for raises `SignalFailed`
            and stops the run. Under `continue` it is defused and the run goes on.
            Defaults to `stop`.

    The clock runs in UTC, so a daylight-saving change in the zone never adds or
    drops simulation time. An instant at or before the start of the run makes the
    whole run real time, which is the degenerate case of the same rule.

    Raises:
        ValueError: If `timezone` is not an IANA time zone name, or
            `on_unhandled_failure` is not `stop` or `continue`.
    """
    if on_unhandled_failure not in UNHANDLED_FAILURE_POLICIES:
        raise ValueError(
            "on_unhandled_failure must be one of "
            + ", ".join(UNHANDLED_FAILURE_POLICIES)
        )
    self.on_unhandled_failure = on_unhandled_failure
    self.zone: tzinfo = resolve_zone(timezone, logical_start_time)
    # The custom time, or the moment the script runs, in the run's zone
    self.start_datetime = (
        in_zone(logical_start_time, self.zone)
        if logical_start_time is not None
        else datetime.now(self.zone)
    )

    super().__init__(
        initial_time=initial_time,
        factor=factor,
        strict=_resolve_strict(strict, factor, go_live_at),
    )

    # True when `trace_events` is on: each model event processed is published as a
    # `trace` record to the trace sinks
    self._trace_on = False
    # True while `run()` is stepping, when `step()` processes every event
    self._in_run = False
    # True while a run has no end of its own: `step()` then ends the run when
    # no model event is left
    self._until_none = False
    # True while a run has an end of its own that `run()` does not see: set by
    # `SimulationContext` while it runs, as it sets `_until_none` itself, and by
    # `_run_low_level` of `analysis.models` while a model with a `stop` runs to
    # its `until`
    self._end_is_set = False
    # The event `run()` is running to, so a step knows it is already triggered
    self._until_event: Optional[Event] = None
    self._go_live_sim_time: Optional[float] = None
    if go_live_at is not None:
        self._go_live_sim_time = self._resolve_go_live_time(
            in_zone(go_live_at, self.zone), float(initial_time)
        )

    self.setup_registry()
    self._create_statistics()
    # Functions called after every change a shared condition can read
    self._change_watchers: List[Callable[[], None]] = []
    # The ones among them that also read the length of a queue
    self._queue_watchers: List[Callable[[], None]] = []
```

#### Methods:

##### `setup_ingress(providers)`

Initializes the ingress queues and starts the background listener threads.

Parameters:

| Name        | Type                | Description                                        | Default    |
| ----------- | ------------------- | -------------------------------------------------- | ---------- |
| `providers` | `List[BaseIngress]` | A list of initialized ingress connector instances. | *required* |

Source code in `src/desflow/core/environment.py`

```python
def setup_ingress(self, providers: List):
    """
    Initializes the ingress queues and starts the background listener threads.

    Args:
        providers (List[BaseIngress]): A list of initialized ingress connector instances.
    """
    self.ingress_queue: queue.Queue[Any] = queue.Queue()
    self.ingress_providers = providers
    self._ingress_loop: Optional[asyncio.AbstractEventLoop] = (
        None  # Store loop reference
    )
    self._ingress_thread = threading.Thread(
        target=self._run_ingress_loop, daemon=True
    )
    self._ingress_thread.start()
    self.start_internal(self._ingress_monitor())  # type: ignore[attr-defined]
```

##### `setup_egress(providers, batch_size=500, flush_interval=1.0, lag_monitor_interval='auto', max_queued_batches=2000, drain_stall_seconds=30.0, predicates=None, batch_sizes=None, flush_intervals=None, labels=None, warmup=None, envelopes=None, traces=None)`

Initializes the egress buffers and starts the background publisher threads.

It first calls each provider's `_load_libraries()` in the calling thread. In a strict environment it then moves `real_start` forward by the time that takes, so the load is not counted as lag, and a paced run's records trail the wall clock by the load time. A run with `strict` False counts the load as lag and catches up, so its records stay on the wall clock.

Parameters:

| Name                   | Type               | Description                                                                                                                                                                                                                                                                               | Default    |
| ---------------------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `providers`            | `List[BaseEgress]` | A list of initialized egress connector instances.                                                                                                                                                                                                                                         | *required* |
| `batch_size`           | `int`              | The maximum number of events to buffer before flushing. Defaults to 500.                                                                                                                                                                                                                  | `500`      |
| `flush_interval`       | `float`            | Maximum simulation seconds to wait before forcing a flush. Defaults to 1.0. Ignored when factor is 0, because simulation time is then detached from the wall clock and the interval bounds nothing, so batch_size alone decides when a batch is sent.                                     | `1.0`      |
| `lag_monitor_interval` | `float`            | Simulation seconds between system.simulation.lag_seconds records. "auto", the default, is off at factor 0, where lag means nothing, and 1.0 otherwise; a run at factor 0 that goes live starts it at the go-live instant. None or 0 turns it off.                                         | `'auto'`   |
| `max_queued_batches`   | `int`              | Upper bound on batches waiting for the egress threads. A full queue blocks the producer, so a sink that cannot keep up slows the simulation rather than accumulating a backlog that teardown would have to discard. Defaults to 2000.                                                     | `2000`     |
| `drain_stall_seconds`  | `float`            | At teardown every queue is drained until empty. The wait is abandoned only if a queue stops shrinking for this long, so a slow sink finishes rather than losing its tail. Defaults to 30.0.                                                                                               | `30.0`     |
| `predicates`           | `List`             | One entry per provider, aligned by position. A callable takes a record and returns True to send it to that provider. None means that provider receives every record. Defaults to None, which is every provider receiving everything.                                                      | `None`     |
| `batch_sizes`          | `List`             | One entry per provider, aligned by position. None falls back to batch_size. A stream sink wants a small value and a lake sink a large one, because the batch is the file and, for Iceberg, the commit.                                                                                    | `None`     |
| `flush_intervals`      | `List`             | One entry per provider, aligned by position. None falls back to flush_interval.                                                                                                                                                                                                           | `None`     |
| `labels`               | `List`             | One entry per provider, aligned by position. True makes it a labels sink, which receives the ground-truth records from publish_label and nothing else; every other provider never receives them. Defaults to None, which is no labels sink.                                               | `None`     |
| `warmup`               | `float`            | The length of the warm-up, in simulation seconds from now, the start of the run when the egress is set up with the model. No provider receives a record stamped before it ends, and the built-in statistics are reset then. Defaults to None, no warm-up.                                 | `None`     |
| `envelopes`            | `List`             | One entry per provider, aligned by position. False makes it a plain-output provider: it receives no telemetry and no queued or started record, and the sink writes each event's value without the record around it. Defaults to None, which is every provider receiving the whole record. | `None`     |
| `traces`               | `List`             | One entry per provider, aligned by position. True makes it a trace sink, which receives the event trace (trace_events) and nothing else; every other provider never receives it. Defaults to None, which is no trace sink.                                                                | `None`     |

Raises:

| Type         | Description                                  |
| ------------ | -------------------------------------------- |
| `ValueError` | If a per-provider list has the wrong length. |

Each provider gets its own queue. They previously shared one, so several providers competed for batches instead of each receiving them, and a run with two sinks wrote part of the data to each with no error and exit code 0.

Source code in `src/desflow/core/environment.py`

```python
def setup_egress(
    self,
    providers: List,
    batch_size: int = 500,
    flush_interval: float = 1.0,
    lag_monitor_interval: Union[float, str, None] = "auto",
    max_queued_batches: int = 2000,
    drain_stall_seconds: float = 30.0,
    predicates: Optional[List[Optional[Callable[[Dict[str, Any]], bool]]]] = None,
    batch_sizes: Optional[List[Optional[int]]] = None,
    flush_intervals: Optional[List[Optional[float]]] = None,
    labels: Optional[List[bool]] = None,
    warmup: Optional[float] = None,
    envelopes: Optional[List[bool]] = None,
    traces: Optional[List[bool]] = None,
):
    """
    Initializes the egress buffers and starts the background publisher threads.

    It first calls each provider's `_load_libraries()` in the calling thread. In a
    strict environment it then moves `real_start` forward by the time that takes,
    so the load is not counted as lag, and a paced run's records trail the wall
    clock by the load time. A run with `strict` False counts the load as lag and
    catches up, so its records stay on the wall clock.

    Args:
        providers (List[BaseEgress]): A list of initialized egress connector instances.
        batch_size (int, optional): The maximum number of events to buffer before flushing. Defaults to 500.
        flush_interval (float, optional): Maximum simulation seconds to wait before
            forcing a flush. Defaults to 1.0. Ignored when `factor` is 0, because
            simulation time is then detached from the wall clock and the interval
            bounds nothing, so `batch_size` alone decides when a batch is sent.
        lag_monitor_interval (float, optional): Simulation seconds between
            `system.simulation.lag_seconds` records. `"auto"`, the default, is
            off at `factor` 0, where lag means nothing, and 1.0 otherwise; a
            run at `factor` 0 that goes live starts it at the go-live instant.
            None or 0 turns it off.
        max_queued_batches (int, optional): Upper bound on batches waiting for the
            egress threads. A full queue blocks the producer, so a sink that cannot
            keep up slows the simulation rather than accumulating a backlog that
            teardown would have to discard. Defaults to 2000.
        drain_stall_seconds (float, optional): At teardown every queue is drained
            until empty. The wait is abandoned only if a queue stops shrinking for
            this long, so a slow sink finishes rather than losing its tail.
            Defaults to 30.0.
        predicates (List, optional): One entry per provider, aligned by position.
            A callable takes a record and returns True to send it to that provider.
            None means that provider receives every record. Defaults to None, which
            is every provider receiving everything.
        batch_sizes (List, optional): One entry per provider, aligned by position.
            None falls back to `batch_size`. A stream sink wants a small value and
            a lake sink a large one, because the batch is the file and, for
            Iceberg, the commit.
        flush_intervals (List, optional): One entry per provider, aligned by
            position. None falls back to `flush_interval`.
        labels (List, optional): One entry per provider, aligned by position.
            True makes it a labels sink, which receives the ground-truth records
            from `publish_label` and nothing else; every other provider never
            receives them. Defaults to None, which is no labels sink.
        warmup (float, optional): The length of the warm-up, in simulation seconds
            from now, the start of the run when the egress is set up with the
            model. No provider receives a record stamped before it ends, and the
            built-in statistics are reset then. Defaults to None, no warm-up.
        envelopes (List, optional): One entry per provider, aligned by position.
            False makes it a plain-output provider: it receives no telemetry and
            no `queued` or `started` record, and the sink writes each event's
            value without the record around it. Defaults to None, which is every
            provider receiving the whole record.
        traces (List, optional): One entry per provider, aligned by position.
            True makes it a trace sink, which receives the event trace
            (`trace_events`) and nothing else; every other provider never
            receives it. Defaults to None, which is no trace sink.

    Raises:
        ValueError: If a per-provider list has the wrong length.

    Each provider gets its own queue. They previously shared one, so several
    providers competed for batches instead of each receiving them, and a run with
    two sinks wrote part of the data to each with no error and exit code 0.
    """
    # Each provider imports its libraries here, before the run, and not in the
    # egress thread: a first import holds the interpreter lock while it loads,
    # which in a paced run stops the simulation and fails a strict one
    self._load_provider_libraries(providers)
    given, sizes, intervals, label_flags, trace_flags, envelope_flags = (
        self._resolve_sink_settings(
            providers,
            batch_size,
            flush_interval,
            drain_stall_seconds,
            predicates,
            batch_sizes,
            flush_intervals,
            labels,
            envelopes,
            traces,
        )
    )
    given = self._set_warmup_and_lag(warmup, lag_monitor_interval, given)
    # One buffer and one queue per provider. The buffers were shared until
    # batching moved behind the fan-out, which meant a single cadence for every
    # sink and no way to give a stream sink low latency while a lake sink built
    # large files.
    self._egress_sinks: List[_SinkRun] = [
        _SinkRun(
            Sink(provider, predicate, size, interval, label, trace, envelope),
            queue.Queue(maxsize=max_queued_batches),
        )
        for provider, predicate, size, interval, label, trace, envelope in zip(
            providers,
            given,
            sizes,
            intervals,
            label_flags,
            trace_flags,
            envelope_flags,
        )
    ]
    self._egress_traced = any(trace_flags)
    self._egress_loop: Optional[asyncio.AbstractEventLoop] = (
        None  # Store loop reference
    )

    self._start_egress(len(providers))
```

##### `publish_telemetry(path_id, value)`

Publishes a low-volume telemetry metric (e.g., utilization, queue length).

Telemetry shares the main event buffer to ensure efficient batching for high-throughput file storage (like Parquet or S3). Nothing happens when no egress is set up.

Parameters:

| Name      | Type  | Description                                                    | Default    |
| --------- | ----- | -------------------------------------------------------------- | ---------- |
| `path_id` | `str` | The value's name, written as the record's path_id.             | *required* |
| `value`   | `Any` | The value, made a JSON value on the way out, as an event's is. | *required* |

Source code in `src/desflow/core/environment.py`

```python
def publish_telemetry(self, path_id: str, value: Any):
    """Publishes a low-volume telemetry metric (e.g., utilization, queue length).

    Telemetry shares the main event buffer to ensure efficient batching for
    high-throughput file storage (like Parquet or S3). Nothing happens when no
    egress is set up.

    Args:
        path_id: The value's name, written as the record's `path_id`.
        value: The value, made a JSON value on the way out, as an event's is.
    """
    if not hasattr(self, "_egress_sinks"):
        return  # Fail silently if no egress is configured

    # Built directly rather than through TelemetryPayload. See publish_event.
    self._append_record(
        {
            "stream_type": "telemetry",
            "sim_ts": float(self.now),  # type: ignore[attr-defined]
            "timestamp": self._get_iso_timestamp(self.start_datetime, self.now),  # type: ignore[attr-defined]
            "path_id": (
                path_id
                if type(path_id) is str
                else _KEY_ADAPTER.validate_python(path_id)
            ),
            "value": _jsonable(value),
        }
    )
```

##### `publish_event(event_key, value, task=None, instance=None, key=None, lifecycle=False)`

Buffers a high-volume discrete event (e.g., a task starting or finishing).

Events are buffered and flushed either when `batch_size` is reached or when `flush_interval` occurs to optimize external I/O throughput.

Parameters:

| Name        | Type   | Description                                                                                                                                                                                                                                                             | Default    |
| ----------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `event_key` | `str`  | The record key: the entity's key, such as orders-1.                                                                                                                                                                                                                     | *required* |
| `value`     | `Any`  | A dictionary or a Pydantic model containing the event payload, or None, which a Kafka sink with envelope off sends as a tombstone. It is made JSON-serializable on the way out, so a nested model is flattened and a datetime becomes an ISO string, exactly as before. | *required* |
| `task`      | `str`  | The name of the task the event belongs to, written as the record's task field, so the payload stays the user's own.                                                                                                                                                     | `None`     |
| `instance`  | `Any`  | The task's instance id, written as the record's instance field.                                                                                                                                                                                                         | `None`     |
| `key`       | `Any`  | The record's key, when it is not event_key: a task's key_field gives the message key this way.                                                                                                                                                                          | `None`     |
| `lifecycle` | `bool` | True for a task's queued or started record, which a provider with envelope off does not receive.                                                                                                                                                                        | `False`    |

Source code in `src/desflow/core/environment.py`

```python
def publish_event(
    self,
    event_key: str,
    value: Any,
    task: Optional[str] = None,
    instance: Any = None,
    key: Any = None,
    lifecycle: bool = False,
):
    """
    Buffers a high-volume discrete event (e.g., a task starting or finishing).

    Events are buffered and flushed either when `batch_size` is reached
    or when `flush_interval` occurs to optimize external I/O throughput.

    Args:
        event_key (str): The record key: the entity's key, such as `orders-1`.
        value (Any): A dictionary or a Pydantic model containing the event
            payload, or None, which a Kafka sink with `envelope` off sends as a
            tombstone. It is made JSON-serializable on the way out, so a nested
            model is flattened and a datetime becomes an ISO string, exactly as
            before.
        task (str, optional): The name of the task the event belongs to, written
            as the record's `task` field, so the payload stays the user's own.
        instance (Any, optional): The task's instance id, written as the record's
            `instance` field.
        key (Any, optional): The record's key, when it is not `event_key`: a
            task's `key_field` gives the message key this way.
        lifecycle (bool, optional): True for a task's `queued` or `started`
            record, which a provider with `envelope` off does not receive.
    """
    if not hasattr(self, "_egress_sinks"):
        return  # Fail silently if no egress is configured

    # Built directly rather than through EventPayload.model_dump(mode="json").
    # Constructing and dumping the model costs 1,790 ns per record against 980 ns
    # for this dict, on a four-field event. About 550 ns of each is the timestamp
    # formatting below, which both paths run.
    #
    # Every coercion the model performed is reproduced here, because each one
    # changed what the sinks received. `sim_ts` was declared `float`, so an integer
    # simulation clock was published as 0.0 rather than 0, and a numpy float was
    # published as a plain float that orjson can serialize. `key` was declared
    # `str`. `value` goes through `_jsonable`, which is `to_jsonable_python` with
    # the two defaults that disagree with the model dump set back. Keys and their
    # order match what the model produced. EventPayload remains the published
    # schema and is what the Avro and documentation paths describe.
    if key is not None:
        event_key = key
    self._append_record(
        {
            "stream_type": "event",
            "sim_ts": float(self.now),  # type: ignore[attr-defined]
            "timestamp": self._get_iso_timestamp(self.start_datetime, self.now),  # type: ignore[attr-defined]
            "key": (
                event_key
                if type(event_key) is str
                else _KEY_ADAPTER.validate_python(event_key)
            ),
            "task": task,
            "instance": instance,
            "value": _jsonable(value),
        },
        lifecycle,
    )
```

##### `watch_changes(callback, queues=False)`

Calls `callback` after every change a shared condition can read.

A change is a store or container level, a resource's capacity or use, a registry update or a signal firing. A wait on a condition, such as `{condition: "level.stock >= 5"}`, watches this way and checks its condition again after each one, as a SimPy loop over a fresh "changed" event would.

A change of the length of a store's or container's `put_queue` or `get_queue` is not one of these: it calls only the functions added with `queues=True`, so a condition that does not read a queue is not checked again for it.

Parameters:

| Name       | Type                 | Description                                                                                                                                                                                           | Default    |
| ---------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `callback` | `Callable[[], None]` | A function of no arguments.                                                                                                                                                                           | *required* |
| `queues`   | `bool`               | True to call it also after a change of the length of a store's or container's put or get queue. A condition that reads putters., getters., count() or items() is watched this way. Defaults to False. | `False`    |

Source code in `src/desflow/core/environment.py`

```python
def watch_changes(self, callback: Callable[[], None], queues: bool = False) -> None:
    """
    Calls `callback` after every change a shared condition can read.

    A change is a store or container level, a resource's capacity or use, a
    registry update or a signal firing. A wait on a condition, such as
    `{condition: "level.stock >= 5"}`, watches this way and checks its condition
    again after each one, as a SimPy loop over a fresh "changed" event would.

    A change of the length of a store's or container's `put_queue` or
    `get_queue` is not one of these: it calls only the functions added with
    `queues=True`, so a condition that does not read a queue is not checked
    again for it.

    Args:
        callback: A function of no arguments.
        queues: True to call it also after a change of the length of a
            store's or container's put or get queue. A condition that reads
            `putters.`, `getters.`, `count()` or `items()` is watched this way.
            Defaults to False.
    """
    self._change_watchers.append(callback)
    if queues:
        self._queue_watchers.append(callback)
```

##### `unwatch_changes(callback)`

Stops calling a function that `watch_changes` added; does nothing if it is gone.

Parameters:

| Name       | Type                 | Description                           | Default    |
| ---------- | -------------------- | ------------------------------------- | ---------- |
| `callback` | `Callable[[], None]` | The function watch_changes was given. | *required* |

Source code in `src/desflow/core/environment.py`

```python
def unwatch_changes(self, callback: Callable[[], None]) -> None:
    """Stops calling a function that `watch_changes` added; does nothing if it is gone.

    Args:
        callback: The function `watch_changes` was given.
    """
    if callback in self._change_watchers:
        self._change_watchers.remove(callback)
    if callback in self._queue_watchers:
        self._queue_watchers.remove(callback)
```

##### `notify_queue_change()`

Calls the functions `watch_changes` added with `queues=True`. A store or container calls it after the length of its `put_queue` or `get_queue` changes; it does not change the holder's statistics.

Source code in `src/desflow/core/environment.py`

```python
def notify_queue_change(self) -> None:
    """Calls the functions `watch_changes` added with `queues=True`. A store or
    container calls it after the length of its `put_queue` or `get_queue`
    changes; it does not change the holder's statistics."""
    if self._queue_watchers:
        for callback in list(self._queue_watchers):
            callback()
```

##### `resume_pacing()`

Restarts pacing from the current simulation time and the wall clock now.

After a pause, such as a debugger stop or a wait between `advance()` calls, the wall clock is ahead of the simulation. The next paced event would run at once, or raise under `strict`. Calling this makes the next event wait `factor` real seconds for each simulation second after `now`. SimPy's `sync()` moves only the wall-clock anchor, so it is not enough on its own once `now` has moved. At `factor` 0 the call changes nothing a run reads.

Source code in `src/desflow/core/environment.py`

```python
def resume_pacing(self) -> None:
    """Restarts pacing from the current simulation time and the wall clock now.

    After a pause, such as a debugger stop or a wait between `advance()` calls,
    the wall clock is ahead of the simulation. The next paced event would run at
    once, or raise under `strict`. Calling this makes the next event wait `factor`
    real seconds for each simulation second after `now`. SimPy's `sync()` moves
    only the wall-clock anchor, so it is not enough on its own once `now` has
    moved. At `factor` 0 the call changes nothing a run reads.
    """
    self.env_start = self.now
    self.real_start = simpy.rt.monotonic()
```

##### `teardown()`

Gracefully terminates the environment.

Ensures that any remaining data in event buffers is flushed to the egress connectors, and that background asyncio threads for both ingress and egress are cleanly stopped. Should be called in a `finally` block.

Source code in `src/desflow/core/environment.py`

```python
def teardown(self):
    """
    Gracefully terminates the environment.

    Ensures that any remaining data in event buffers is flushed to the
    egress connectors, and that background asyncio threads for both
    ingress and egress are cleanly stopped. Should be called in a `finally` block.
    """
    logger.info("Environment teardown initiated.")
    # The last publication of the built-in statistics, at the end of the run
    if self.stats.enabled:
        self.stats.publish()
    if hasattr(self, "teardown_egress"):
        self.teardown_egress()
    if hasattr(self, "teardown_ingress"):
        self.teardown_ingress()
    logger.info("Environment teardown complete.")
```

## Registry and Parameters

### `desflow.SimulationRegistry(env)`

A central registry that maps dot-notation paths to dynamic simulation parameters.

The Registry acts as the single source of truth for the simulation's state. It allows external streams (like Kafka or Redis) to update parameters while the simulation runs, and passes each change to the SimPy processes that use it.

Attributes:

| Name        | Type                                  | Description                                                                                   |
| ----------- | ------------------------------------- | --------------------------------------------------------------------------------------------- |
| `env`       | `Environment`                         | The active SimPy environment.                                                                 |
| `on_change` | `List[Callable[[ParamChange], None]]` | Callbacks that receive a ParamChange for every accepted change, in the order they were added. |

Source code in `src/desflow/core/registry.py`

```python
def __init__(self, env: Environment):
    self.env = env
    self._values: Dict[str, DynamicValue] = {}
    self._configs: Dict[str, Any] = {}
    # What each path is, recorded when it is registered, so no reader works it
    # out from the text of the path
    self._kinds: Dict[str, _PathKind] = {}
    # Called with a `ParamChange` for every accepted change, in the order added
    self.on_change: List[Callable[[ParamChange], None]] = []
```

#### Methods:

##### `change(path, value, *, source=None, actor=None, reason=None)`

Changes a value as `update` does, and reports what happened.

The checks are the ones `update` runs. Where `update` logs a refusal and goes on, `change` raises `ParamRejected`, for a caller that must know. A value that is cast or clamped is accepted, and `ParamChange.new` shows the value stored. Every `on_change` callback receives the change before `change` returns.

Parameters:

| Name     | Type            | Description                                    | Default    |
| -------- | --------------- | ---------------------------------------------- | ---------- |
| `path`   | `str`           | The dot-notation path to change.               | *required* |
| `value`  | `Any`           | The new value.                                 | *required* |
| `source` | `Optional[str]` | Where the change came from, such as "ingress". | `None`     |
| `actor`  | `Optional[str]` | Who made the change.                           | `None`     |
| `reason` | `Optional[str]` | Why the change was made.                       | `None`     |

Returns:

| Name          | Type          | Description                                                 |
| ------------- | ------------- | ----------------------------------------------------------- |
| `ParamChange` | `ParamChange` | The old value, the value stored and the time of the change. |

Raises:

| Type            | Description                                                                                                                    |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `ParamRejected` | If the path does not exist, the value cannot be converted to the stored type, or a distribution parameter is out of its range. |

Source code in `src/desflow/core/registry.py`

```python
def change(
    self,
    path: str,
    value: Any,
    *,
    source: Optional[str] = None,
    actor: Optional[str] = None,
    reason: Optional[str] = None,
) -> ParamChange:
    """
    Changes a value as `update` does, and reports what happened.

    The checks are the ones `update` runs. Where `update` logs a refusal and goes
    on, `change` raises `ParamRejected`, for a caller that must know. A value that
    is cast or clamped is accepted, and `ParamChange.new` shows the value stored.
    Every `on_change` callback receives the change before `change` returns.

    Args:
        path (str): The dot-notation path to change.
        value (Any): The new value.
        source (Optional[str]): Where the change came from, such as `"ingress"`.
        actor (Optional[str]): Who made the change.
        reason (Optional[str]): Why the change was made.

    Returns:
        ParamChange: The old value, the value stored and the time of the change.

    Raises:
        ParamRejected: If the path does not exist, the value cannot be converted
            to the stored type, or a distribution parameter is out of its range.
    """
    return self._apply(path, value, source, actor, reason)
```

##### `check_value(path, new_value)`

The value a path accepts for `new_value`, cast to the path's type.

The one check a change, a scenario step and an `on:` action share, so each refuses the same values with the same text. A capacity, `max_cap` or queue limit is a whole number of 0 or more (a container's capacity any number of 0 or more), read from text only when the text is such a number; a container level is a finite number; a true-or-false path takes `true`, `false` or the text "true" or "false"; a variable takes `true` as 1, and a distribution parameter refuses it.

Parameters:

| Name        | Type  | Description          | Default    |
| ----------- | ----- | -------------------- | ---------- |
| `path`      | `str` | The registered path. | *required* |
| `new_value` | `Any` | The value to write.  | *required* |

Raises:

| Type            | Description                                                                                          |
| --------------- | ---------------------------------------------------------------------------------------------------- |
| `ParamRejected` | For an unknown path, a value the path refuses, or a distribution parameter the distribution refuses. |

Source code in `src/desflow/core/registry.py`

```python
def check_value(self, path: str, new_value: Any) -> Any:
    """The value a path accepts for `new_value`, cast to the path's type.

    The one check a change, a scenario step and an `on:` action share, so each
    refuses the same values with the same text. A capacity, `max_cap` or queue
    limit is a whole number of 0 or more (a container's capacity any number of 0
    or more), read from text only when the text is such a number; a container
    level is a finite number; a true-or-false path takes `true`, `false` or the
    text "true" or "false"; a variable takes `true` as 1, and a
    distribution parameter refuses it.

    Args:
        path: The registered path.
        new_value: The value to write.

    Raises:
        ParamRejected: For an unknown path, a value the path refuses, or a
            distribution parameter the distribution refuses.
    """
    if path not in self._values:
        rejected = ParamRejected(
            path,
            new_value,
            f"'{path}' is not a registry path" + suggestion(path, self._values),
        )
        rejected._level = logging.WARNING
        rejected._log = f"Attempted to update non-existent path: {path}"
        raise rejected

    current_val = self._values[path].value
    validated_value = new_value
    registered = self._kinds[path]
    kind, parent_path, attr = registered.kind, registered.config, registered.attr

    # Type Validation & Casting
    if self.is_queue_limit(path):
        # A whole number or null, whether the limit is set now or not
        validated_value = queue_limit_value(path, new_value)
    elif _is_capacity(registered):
        validated_value = _capacity_value(path, registered, new_value)
    elif kind == "containers" and attr == "level":
        validated_value = _level_value(path, new_value)
    elif isinstance(current_val, bool):
        validated_value = _flag_value(path, new_value)
    elif current_val is not None and _infinite_capacity(registered, new_value):
        # No limit: inf, or the text "inf", on a capacity path of any type
        validated_value = math.inf
    elif current_val is not None and _stored_as_given(kind, current_val, new_value):
        # A number written over a number keeps its type, as SimPy's keeps it:
        # 0.5 over 2 is 0.5, 3 over 2.5 is 3, and true over a variable is 1
        # `true` over a distribution parameter stays a bool, which the
        # distribution's own check refuses; over any other number it is 1
        validated_value = (
            int(new_value)
            if isinstance(new_value, bool) and kind not in ("service", "arrival")
            else new_value
        )
    elif current_val is not None:
        expected_type = type(current_val)
        if _is_number(current_val) and kind in _AS_GIVEN_KINDS:
            # Text, such as "0.5" from a connector, is read as a float, as it was
            # when these values were always floats
            expected_type = float
        # If types don't match, attempt a safe cast (e.g., "5" -> 5)
        if not isinstance(new_value, expected_type):
            try:
                validated_value = expected_type(new_value)
                logger.debug(
                    f"Cast '{new_value}' to {expected_type.__name__} for '{path}'"
                )
            except (ValueError, TypeError, OverflowError):
                rejected = ParamRejected(
                    path,
                    new_value,
                    f"'{path}' holds a {expected_type.__name__}, and "
                    f"{new_value!r} cannot be converted to one",
                )
                rejected._log = (
                    f"Type mismatch for '{path}'. Expected {expected_type.__name__}, "
                    f"got {type(new_value).__name__} with value '{new_value}'. Update ignored."
                )
                raise rejected

    parent_obj = self._configs.get(parent_path)

    # Check a distribution parameter with the same rules as at load
    if isinstance(parent_obj, DistributionConfig):
        candidate = copy.copy(parent_obj)
        setattr(candidate, attr, validated_value)
        try:
            check_distribution(candidate)
        except ValueError as exc:
            rejected = ParamRejected(
                path, new_value, f"invalid value for '{path}': {exc}"
            )
            rejected._log = f"Invalid value for '{path}': {exc}. Update ignored."
            raise rejected from exc
    return validated_value
```

##### `configs()`

Returns every configuration object by its path, such as `Shop.service.packing`.

An experiment records them as the model's compiled parameters. The objects are the live ones, so read them rather than change them.

Returns:

| Type             | Description                                                                                                               |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `Dict[str, Any]` | Dict\[str, Any\]: Path to DistributionConfig, CapacityConfig or the variables mapping, in the order they were registered. |

Source code in `src/desflow/core/registry.py`

```python
def configs(self) -> Dict[str, Any]:
    """
    Returns every configuration object by its path, such as `Shop.service.packing`.

    An experiment records them as the model's compiled parameters. The objects
    are the live ones, so read them rather than change them.

    Returns:
        Dict[str, Any]: Path to `DistributionConfig`, `CapacityConfig` or the
            variables mapping, in the order they were registered.
    """
    return dict(self._configs)
```

##### `get(path)`

Retrieve the `DynamicValue` object at a specific path.

Parameters:

| Name   | Type  | Description                                               | Default    |
| ------ | ----- | --------------------------------------------------------- | ---------- |
| `path` | `str` | The dot-notation path (e.g., 'Shop.arrival.orders.rate'). | *required* |

Returns:

| Name           | Type           | Description                                     |
| -------------- | -------------- | ----------------------------------------------- |
| `DynamicValue` | `DynamicValue` | The wrapper object for the requested parameter. |

Raises:

| Type       | Description                                 |
| ---------- | ------------------------------------------- |
| `KeyError` | If the path does not exist in the registry. |

Source code in `src/desflow/core/registry.py`

```python
def get(self, path: str) -> DynamicValue:
    """
    Retrieve the `DynamicValue` object at a specific path.

    Args:
        path (str): The dot-notation path (e.g., 'Shop.arrival.orders.rate').

    Returns:
        DynamicValue: The wrapper object for the requested parameter.

    Raises:
        KeyError: If the path does not exist in the registry.
    """
    if path not in self._values:
        raise KeyError(f"Path '{path}' not found in Simulation Registry.")
    return self._values[path]
```

##### `get_config(path)`

Retrieve the live configuration object (e.g., `DistributionConfig`).

Parameters:

| Name   | Type  | Description                                           | Default    |
| ------ | ----- | ----------------------------------------------------- | ---------- |
| `path` | `str` | The dot-notation path (e.g., 'Shop.service.packing'). | *required* |

Returns:

| Name  | Type  | Description                            |
| ----- | ----- | -------------------------------------- |
| `Any` | `Any` | The synchronized configuration object. |

Raises:

| Type       | Description                                        |
| ---------- | -------------------------------------------------- |
| `KeyError` | If the config path does not exist in the registry. |

Source code in `src/desflow/core/registry.py`

```python
def get_config(self, path: str) -> Any:
    """
    Retrieve the live configuration object (e.g., `DistributionConfig`).

    Args:
        path (str): The dot-notation path (e.g., 'Shop.service.packing').

    Returns:
        Any: The synchronized configuration object.

    Raises:
        KeyError: If the config path does not exist in the registry.
    """
    if path not in self._configs:
        raise KeyError(f"Config path '{path}' not found in Simulation Registry.")
    return self._configs[path]
```

##### `is_queue_limit(path)`

Whether `path` is a resource's `queue_limit`, as registered.

Parameters:

| Name   | Type  | Description      | Default    |
| ------ | ----- | ---------------- | ---------- |
| `path` | `str` | A registry path. | *required* |

Returns:

| Name   | Type   | Description                             |
| ------ | ------ | --------------------------------------- |
| `bool` | `bool` | True for a registered queue_limit path. |

Source code in `src/desflow/core/registry.py`

```python
def is_queue_limit(self, path: str) -> bool:
    """
    Whether `path` is a resource's `queue_limit`, as registered.

    Args:
        path (str): A registry path.

    Returns:
        bool: True for a registered `queue_limit` path.
    """
    registered = self._kinds.get(path)
    return (
        registered is not None
        and registered.kind == "resources"
        and registered.attr == "queue_limit"
    )
```

##### `paths()`

Returns every value path, such as `Shop.resources.packers.current_cap`.

Returns:

| Type        | Description                                                                    |
| ----------- | ------------------------------------------------------------------------------ |
| `List[str]` | List\[str\]: The paths get and update take, in the order they were registered. |

Source code in `src/desflow/core/registry.py`

```python
def paths(self) -> List[str]:
    """
    Returns every value path, such as `Shop.resources.packers.current_cap`.

    Returns:
        List[str]: The paths `get` and `update` take, in the order they were
            registered.
    """
    return list(self._values)
```

##### `register_breakdown(sim_id, name, breakdown)`

Registers a resource's time to failure and repair time.

Parameters:

| Name        | Type  | Description                                                                                | Default    |
| ----------- | ----- | ------------------------------------------------------------------------------------------ | ---------- |
| `sim_id`    | `str` | The simulation ID.                                                                         | *required* |
| `name`      | `str` | The resource's name, which for a member of a family is its bracketed name such as desk[3]. | *required* |
| `breakdown` | `Any` | The breakdown, with time_to_failure and repair.                                            | *required* |

Source code in `src/desflow/core/registry.py`

```python
def register_breakdown(self, sim_id: str, name: str, breakdown: Any):
    """
    Registers a resource's time to failure and repair time.

    Args:
        sim_id (str): The simulation ID.
        name (str): The resource's name, which for a member of a family is its
            bracketed name such as `desk[3]`.
        breakdown (Any): The breakdown, with `time_to_failure` and `repair`.
    """
    for part in ("time_to_failure", "repair"):
        path = f"{sim_id}.breakdown.{name}.{part}"
        dist_config = getattr(breakdown, part)
        self._configs[path] = dist_config
        self._register_dist(path, dist_config, "breakdown", sim_id)
```

##### `register_container(sim_id, name, cap_config)`

Registers a container's capacity paths and its `.level` path.

A `current_cap` left as None is the `max_cap` if that is given and no limit (infinity) if not, and a `max_cap` left as None is the `current_cap`. The config is updated to the values in effect.

Parameters:

| Name         | Type             | Description                      | Default    |
| ------------ | ---------------- | -------------------------------- | ---------- |
| `sim_id`     | `str`            | The simulation ID.               | *required* |
| `name`       | `str`            | The container's name.            | *required* |
| `cap_config` | `CapacityConfig` | Its capacity and starting level. | *required* |

Source code in `src/desflow/core/registry.py`

```python
def register_container(self, sim_id: str, name: str, cap_config: CapacityConfig):
    """
    Registers a container's capacity paths and its `.level` path.

    A `current_cap` left as None is the `max_cap` if that is given and no limit
    (infinity) if not, and a `max_cap` left as None is the `current_cap`. The
    config is updated to the values in effect.

    Args:
        sim_id (str): The simulation ID.
        name (str): The container's name.
        cap_config (CapacityConfig): Its capacity and starting level.
    """
    self._fill_unbounded(cap_config)
    path = f"{sim_id}.containers.{name}"
    self._configs[path] = cap_config
    self._register_cap(path, cap_config, "containers")
    level = getattr(cap_config, "level", 0)
    self._values[f"{path}.level"] = DynamicValue(self.env, f"{path}.level", level)
    self._kinds[f"{path}.level"] = _PathKind("containers", path, "level")
```

##### `register_resource(sim_id, name, cap_config)`

Registers a resource's capacity paths, filling in what the config leaves out.

A `current_cap` left as None is 1, and a `max_cap` left as None is the `current_cap`. The config is updated to the values in effect.

Parameters:

| Name         | Type             | Description                                   | Default    |
| ------------ | ---------------- | --------------------------------------------- | ---------- |
| `sim_id`     | `str`            | The simulation ID.                            | *required* |
| `name`       | `str`            | The resource's name.                          | *required* |
| `cap_config` | `CapacityConfig` | Its capacity, which may leave values as None. | *required* |

Raises:

| Type         | Description                                                          |
| ------------ | -------------------------------------------------------------------- |
| `ValueError` | If only max_cap is given and it is below 1, the default current_cap. |

Source code in `src/desflow/core/registry.py`

```python
def register_resource(self, sim_id: str, name: str, cap_config: CapacityConfig):
    """
    Registers a resource's capacity paths, filling in what the config leaves out.

    A `current_cap` left as None is 1, and a `max_cap` left as None is the
    `current_cap`. The config is updated to the values in effect.

    Args:
        sim_id (str): The simulation ID.
        name (str): The resource's name.
        cap_config (CapacityConfig): Its capacity, which may leave values as None.

    Raises:
        ValueError: If only `max_cap` is given and it is below 1, the default
            `current_cap`.
    """
    if cap_config.current_cap is None:
        if cap_config.max_cap is not None and cap_config.max_cap < 1:
            raise ValueError(
                f"resource '{name}' has max_cap {cap_config.max_cap} and no "
                f"current_cap, whose default is 1; set current_cap to "
                f"{cap_config.max_cap} or less"
            )
        cap_config.current_cap = 1
    if cap_config.max_cap is None:
        cap_config.max_cap = cap_config.current_cap
    path = f"{sim_id}.resources.{name}"
    self._configs[path] = cap_config
    self._register_cap(path, cap_config, "resources")
    if getattr(cap_config, "limit_path", True):
        limit = getattr(cap_config, "queue_limit", None)
        self._values[f"{path}.queue_limit"] = DynamicValue(
            self.env, f"{path}.queue_limit", limit
        )
        self._kinds[f"{path}.queue_limit"] = _PathKind(
            "resources", path, "queue_limit"
        )
```

##### `register_sim_parameter(param)`

Takes a `SimParameter` instance and flattens it into the registry.

Parameters:

| Name    | Type           | Description                           | Default    |
| ------- | -------------- | ------------------------------------- | ---------- |
| `param` | `SimParameter` | The initial state schema to register. | *required* |

Source code in `src/desflow/core/registry.py`

```python
def register_sim_parameter(self, param: SimParameter):
    """
    Takes a `SimParameter` instance and flattens it into the registry.

    Args:
        param (SimParameter): The initial state schema to register.
    """
    prefix = param.sim_id

    # Register Arrivals
    for name, dist_config in param.arrival.items():
        path = f"{prefix}.arrival.{name}"
        self._configs[path] = dist_config
        self._register_dist(path, dist_config, "arrival", prefix)

    # Register Service Steps
    for name, dist_config in param.service.items():
        path = f"{prefix}.service.{name}"
        self._configs[path] = dist_config
        self._register_dist(path, dist_config, "service", prefix)

    # Register Resources, Containers and Stores
    for name, cap_config in param.resources.items():
        self.register_resource(prefix, name, cap_config)
    for name, cap_config in param.containers.items():
        self.register_container(prefix, name, cap_config)
    for name, cap_config in param.stores.items():
        self.register_store(prefix, name, cap_config)

    # Register Breakdowns: each resource's time to failure and repair time
    for name, breakdown in param.breakdowns.items():
        self.register_breakdown(prefix, name, breakdown)

    # Register Custom Variables
    self._configs[f"{prefix}.variables"] = param.variables
    for name, value in param.variables.items():
        path = f"{prefix}.variables.{name}"
        self._values[path] = DynamicValue(self.env, path, value)
        self._kinds[path] = _PathKind("variables", f"{prefix}.variables", name)
```

##### `register_store(sim_id, name, cap_config)`

Registers a store's capacity paths, filling in what the config leaves out.

A `current_cap` left as None is the `max_cap` if that is given and no limit (infinity) if not, and a `max_cap` left as None is the `current_cap`. The config is updated to the values in effect.

Parameters:

| Name         | Type             | Description                                   | Default    |
| ------------ | ---------------- | --------------------------------------------- | ---------- |
| `sim_id`     | `str`            | The simulation ID.                            | *required* |
| `name`       | `str`            | The store's name.                             | *required* |
| `cap_config` | `CapacityConfig` | Its capacity, which may leave values as None. | *required* |

Source code in `src/desflow/core/registry.py`

```python
def register_store(self, sim_id: str, name: str, cap_config: CapacityConfig):
    """
    Registers a store's capacity paths, filling in what the config leaves out.

    A `current_cap` left as None is the `max_cap` if that is given and no limit
    (infinity) if not, and a `max_cap` left as None is the `current_cap`. The
    config is updated to the values in effect.

    Args:
        sim_id (str): The simulation ID.
        name (str): The store's name.
        cap_config (CapacityConfig): Its capacity, which may leave values as None.
    """
    self._fill_unbounded(cap_config)
    path = f"{sim_id}.stores.{name}"
    self._configs[path] = cap_config
    self._register_cap(path, cap_config, "stores")
```

##### `set_exactly(path, value, *, source=None, actor=None, reason=None)`

Sets a path to exactly `value`, raising where `update` would log and go on.

Replications and experiments set their variants' values this way before the clock starts, so a misspelt path or a refused value stops the experiment rather than leaving the model's own value in place.

Parameters:

| Name     | Type            | Description                             | Default    |
| -------- | --------------- | --------------------------------------- | ---------- |
| `path`   | `str`           | The dot-notation path.                  | *required* |
| `value`  | `Any`           | The value it must hold.                 | *required* |
| `source` | `Optional[str]` | Where the change came from; see change. | `None`     |
| `actor`  | `Optional[str]` | Who made the change.                    | `None`     |
| `reason` | `Optional[str]` | Why the change was made.                | `None`     |

Raises:

| Type            | Description                                                                                                                                                                                      |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ParamRejected` | A ValueError that carries the refused path, if the path does not exist, or the registry refuses, casts to another value or clamps the value, or a max_cap would clamp the current_cap beside it. |

Source code in `src/desflow/core/registry.py`

```python
def set_exactly(
    self,
    path: str,
    value: Any,
    *,
    source: Optional[str] = None,
    actor: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    """
    Sets a path to exactly `value`, raising where `update` would log and go on.

    Replications and experiments set their variants' values this way before the
    clock starts, so a misspelt path or a refused value stops the experiment rather
    than leaving the model's own value in place.

    Args:
        path (str): The dot-notation path.
        value (Any): The value it must hold.
        source (Optional[str]): Where the change came from; see `change`.
        actor (Optional[str]): Who made the change.
        reason (Optional[str]): Why the change was made.

    Raises:
        ParamRejected: A `ValueError` that carries the refused `path`, if the path
            does not exist, or the registry refuses, casts to another value or
            clamps the value, or a `max_cap` would clamp the `current_cap` beside
            it.
    """
    if path not in self._values:
        raise ParamRejected(
            path,
            value,
            f"'{path}' is not a registry path" + suggestion(path, self._values),
        )
    registered = self._kinds[path]
    sibling = f"{registered.config}.current_cap"
    if _is_capacity(registered) and registered.attr == "max_cap":
        current = self._values[sibling].value
        if value is not None and current is not None and current > value:
            # update would lower current_cap to it without a word
            raise ParamRejected(
                path,
                value,
                f"'{path}' cannot be set to {value!r}: '{sibling}' is "
                f"{current!r}; set current_cap first, to {value!r} or less",
            )
    self.update(path, value, source=source, actor=actor, reason=reason)
    kept = self._values[path].value
    if kept != value:
        raise ParamRejected(
            path,
            value,
            f"'{path}' cannot be set to {value!r}; the registry kept {kept!r}",
        )
```

##### `update(path, new_value, *, source=None, actor=None, reason=None)`

Update a value safely and synchronize its parent configuration object.

Includes dynamic type casting to protect the simulation from crashing if external systems send improperly typed data (e.g., sending the string "5" instead of the integer 5). A number written over a number is stored as given on a distribution parameter, a container value and a variable, so 0.5 over 2 stays 0.5 and 9.5 over a variable of 10 stays 9.5.

A capacity path (`current_cap` or `max_cap`) takes `inf` and the text `"inf"` as no limit, whatever type it held.

A distribution parameter outside its range (for example a negative std) is refused with an error log line, as a value of the wrong type is, and the old value is kept. A `current_cap` outside `[0, max_cap]` is clamped to that range with a warning log line, also when a lower `max_cap` arrives, and so is a container `level` outside `[0, current_cap]`. A path the model does not have, such as a parameter the distribution's family does not take, is refused with a warning log line. `update` never raises for a refused value; `change` does, and returns what was applied.

Parameters:

| Name        | Type            | Description                                    | Default    |
| ----------- | --------------- | ---------------------------------------------- | ---------- |
| `path`      | `str`           | The dot-notation path to update.               | *required* |
| `new_value` | `Any`           | The new value to apply.                        | *required* |
| `source`    | `Optional[str]` | Where the change came from, such as "ingress". | `None`     |
| `actor`     | `Optional[str]` | Who made the change.                           | `None`     |
| `reason`    | `Optional[str]` | Why the change was made.                       | `None`     |

Source code in `src/desflow/core/registry.py`

```python
def update(
    self,
    path: str,
    new_value: Any,
    *,
    source: Optional[str] = None,
    actor: Optional[str] = None,
    reason: Optional[str] = None,
) -> None:
    """
    Update a value safely and synchronize its parent configuration object.

    Includes dynamic type casting to protect the simulation from crashing if
    external systems send improperly typed data (e.g., sending the string "5"
    instead of the integer 5). A number written over a number is stored as given
    on a distribution parameter, a container value and a variable, so 0.5 over 2
    stays 0.5 and 9.5 over a variable of 10 stays 9.5.

    A capacity path (`current_cap` or `max_cap`) takes `inf` and the text `"inf"`
    as no limit, whatever type it held.

    A distribution parameter outside its range (for example a negative std) is
    refused with an error log line, as a value of the wrong type is, and the old
    value is kept. A `current_cap` outside `[0, max_cap]` is clamped to that range
    with a warning log line, also when a lower `max_cap` arrives, and so is a
    container `level` outside `[0, current_cap]`. A path the model does not have,
    such as a parameter the distribution's family does not take, is refused with a
    warning log line. `update` never raises for a refused value; `change` does, and
    returns what was applied.

    Args:
        path (str): The dot-notation path to update.
        new_value (Any): The new value to apply.
        source (Optional[str]): Where the change came from, such as `"ingress"`.
        actor (Optional[str]): Who made the change.
        reason (Optional[str]): Why the change was made.
    """
    try:
        self._apply(path, new_value, source, actor, reason)
    except ParamRejected as exc:
        logger.log(exc._level, exc._log)
```

### `desflow.DynamicValue(env, name, initial_value)`

The atom of the registry. Holds a value and a SimPy event that triggers whenever the value changes.

This class is used internally by the `SimulationRegistry` to wrap configuration values so that SimPy processes can yield and wait for them to change.

Attributes:

| Name   | Type          | Description                                                                            |
| ------ | ------------- | -------------------------------------------------------------------------------------- |
| `env`  | `Environment` | The active SimPy environment.                                                          |
| `name` | `str`         | The dot-notation path/name of this value (e.g., 'Shop.resources.packers.current_cap'). |

Source code in `src/desflow/core/registry.py`

```python
def __init__(self, env: Environment, name: str, initial_value: Any):
    self.env = env
    self.name = name
    self._value = initial_value
    # Use a Store to signal changes (prevents missed events)
    self._signal = InternalStore(env, capacity=1)
```

#### Attributes

##### `value`

The current underlying value.

#### Methods:

##### `track(new_value)`

Records a value the model changed itself, without signalling a change.

A container's level changes whenever a task puts or takes an amount. The registry keeps that level so a reader sees it, but the container that made the change must not be told to apply it again.

Parameters:

| Name        | Type  | Description              | Default    |
| ----------- | ----- | ------------------------ | ---------- |
| `new_value` | `Any` | The value now in effect. | *required* |

Source code in `src/desflow/core/registry.py`

```python
def track(self, new_value: Any):
    """
    Records a value the model changed itself, without signalling a change.

    A container's level changes whenever a task puts or takes an amount. The
    registry keeps that level so a reader sees it, but the container that made
    the change must not be told to apply it again.

    Args:
        new_value (Any): The value now in effect.
    """
    self._value = new_value
```

##### `update(new_value)`

Updates the value and signals any waiting SimPy processes.

Parameters:

| Name        | Type  | Description             | Default    |
| ----------- | ----- | ----------------------- | ---------- |
| `new_value` | `Any` | The new value to store. | *required* |

Source code in `src/desflow/core/registry.py`

```python
def update(self, new_value: Any):
    """
    Updates the value and signals any waiting SimPy processes.

    Args:
        new_value (Any): The new value to store.
    """
    if self._value != new_value:
        self._value = new_value
        # Signal a change if not already signaled
        if len(self._signal.items) == 0:
            self._signal.put(True)
```

##### `wait_for_change()`

Returns a SimPy event that will be triggered when the value updates.

Returns:

| Type       | Description                                                   |
| ---------- | ------------------------------------------------------------- |
| `StoreGet` | simpy.events.StoreGet: A get-request for the internal signal. |

Source code in `src/desflow/core/registry.py`

```python
def wait_for_change(self) -> StoreGet:
    """
    Returns a SimPy event that will be triggered when the value updates.

    Returns:
        simpy.events.StoreGet: A get-request for the internal signal.
    """
    return self._signal.get()
```

### `desflow.ParamChange(*, path, old, new, requested, at, source=None, actor=None, reason=None)`

One accepted change to a registry path.

`SimulationRegistry.change` returns it, and each `on_change` callback receives it. It is frozen, so a callback cannot alter what the next callback receives.

Attributes:

| Name        | Type            | Description                                                                                            |
| ----------- | --------------- | ------------------------------------------------------------------------------------------------------ |
| `path`      | `str`           | The dot-notation path that changed, such as Shop.resources.packers.current_cap.                        |
| `old`       | `Any`           | The value before the change.                                                                           |
| `new`       | `Any`           | The value stored, after any cast or clamp.                                                             |
| `requested` | `Any`           | The value the caller asked for.                                                                        |
| `at`        | `float`         | The simulation time at which the change was applied.                                                   |
| `source`    | `Optional[str]` | Where the change came from, such as "ingress" or "scenario". None for a change the model makes itself. |
| `actor`     | `Optional[str]` | Who made the change, such as the ingress connector's class name.                                       |
| `reason`    | `Optional[str]` | Why the caller made the change.                                                                        |

#### Attributes

##### `changed`

Whether the stored value differs from the value before the change.

### `desflow.ParamRejected(path, value, reason)`

Bases: `ValueError`

Raised by `SimulationRegistry.change` when the registry refuses a value.

It is a `ValueError`, so a caller that catches `ValueError` also catches it. `str(exc)` is the reason.

Attributes:

| Name     | Type  | Description                          |
| -------- | ----- | ------------------------------------ |
| `path`   | `str` | The path the caller asked to change. |
| `value`  | `Any` | The value the registry refused.      |
| `reason` | `str` | Why the registry refused it.         |

Source code in `src/desflow/core/registry.py`

```python
def __init__(self, path: str, value: Any, reason: str):
    # `args` holds all three, because SimPy rebuilds an exception that ends a
    # process as `type(exc)(*exc.args)`, and unpickling rebuilds it the same way.
    super().__init__(path, value, reason)
    self.path = path
    self.value = value
    self.reason = reason
    # The log level and line `update` writes for this refusal
    self._level = logging.ERROR
    self._log = reason
```

### `desflow.SimParameter(sim_id, arrival=dict(), service=dict(), resources=dict(), containers=dict(), stores=dict(), breakdowns=dict(), variables=dict())`

The master schema representing the state of a specific simulation entity (like a production line).

This object is registered with the `SimulationRegistry`, which flattens the nested dictionaries into dot-notation paths (e.g., 'Shop.arrival.orders.rate').

Attributes:

| Name         | Type                            | Description                                                                                                                                                  |
| ------------ | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `sim_id`     | `str`                           | The unique prefix for this group of parameters (e.g., 'Shop').                                                                                               |
| `arrival`    | `Dict[str, DistributionConfig]` | Configurations for arrival generation rates.                                                                                                                 |
| `service`    | `Dict[str, DistributionConfig]` | Configurations for process task durations.                                                                                                                   |
| `resources`  | `Dict[str, CapacityConfig]`     | Configurations for standard SimPy Resources.                                                                                                                 |
| `containers` | `Dict[str, CapacityConfig]`     | Configurations for continuous SimPy Containers. A ContainerConfig also sets the level the container starts at; a plain CapacityConfig starts it empty.       |
| `stores`     | `Dict[str, CapacityConfig]`     | Configurations for discrete SimPy Stores.                                                                                                                    |
| `breakdowns` | `Dict[str, BreakdownConfig]`    | How each named resource breaks down, registered at \<sim_id>.breakdown.<resource>.time_to_failure and .repair.                                               |
| `variables`  | `Dict[str, Any]`                | Flexible user-defined state variables (e.g., int, float, bool, str) used to drive custom logic, track string-based states, or act as external control dials. |

### `desflow.CapacityConfig(current_cap=None, max_cap=None)`

Configuration for the capacity of a simulated resource, container, or store.

A value left as None is filled in when the holder registers. A resource's `current_cap` is 1, and `max_cap` is `current_cap`. A store's or container's `current_cap` is `max_cap` when that is given and no limit if not, and its `max_cap` is `current_cap`. `float("inf")` also means no limit.

Attributes:

| Name          | Type                          | Description                             |
| ------------- | ----------------------------- | --------------------------------------- |
| `current_cap` | `Optional[Union[int, float]]` | The currently active capacity.          |
| `max_cap`     | `Optional[Union[int, float]]` | The absolute physical maximum capacity. |

### `desflow.DistributionConfig(dist, rate=None, mean=None, std=None, low=None, value=None, high=None, mode=None, shape=None, scale=None, values=None, weights=None, edges=None, counts=None, p=None)`

Configuration for a statistical distribution used in the simulation.

This config is dynamically updatable. For example, you can change the `rate` of an 'exponential' distribution via Kafka, and the `Sampler` will instantly use the new rate for the next event. The family (`dist`) is fixed at load, and only its parameters change live.

Numbers are stored as given: an integer stays an integer, as in SimPy, and a live update to 0.5 is not cast to an integer. The family and its parameters are checked when the config is built, against the registry of distributions in `desflow.models.distributions`.

Attributes:

| Name      | Type                    | Description                                                                                                                                                                                 |
| --------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dist`    | `str`                   | The family: exponential, normal, lognormal, truncated_normal, poisson, geometric, constant, uniform, triangular, gamma, weibull or empirical.                                               |
| `rate`    | `Optional[float]`       | Events per second, for exponential. Greater than 0.                                                                                                                                         |
| `mean`    | `Optional[float]`       | The mean, for normal, lognormal, truncated_normal and poisson. Greater than 0 for lognormal; 0 or more for poisson.                                                                         |
| `p`       | `Optional[float]`       | The chance of success on each try, for geometric. Above 0 and at most 1.                                                                                                                    |
| `std`     | `Optional[float]`       | The standard deviation, 0 or more; 0 (or no value) gives the mean every time.                                                                                                               |
| `low`     | `Optional[float]`       | The lowest value: of a truncated normal (0 by default, and a draw below it is drawn again), a uniform or a triangular.                                                                      |
| `value`   | `Optional[float]`       | The time a constant distribution always gives. 0 or more.                                                                                                                                   |
| `high`    | `Optional[float]`       | The highest value of a uniform or a triangular.                                                                                                                                             |
| `mode`    | `Optional[float]`       | The most likely value of a triangular.                                                                                                                                                      |
| `shape`   | `Optional[float]`       | The shape of a gamma or a weibull. Greater than 0.                                                                                                                                          |
| `scale`   | `Optional[float]`       | The scale of a gamma or a weibull. Greater than 0.                                                                                                                                          |
| `values`  | `Optional[List[float]]` | The values an empirical draws from.                                                                                                                                                         |
| `weights` | `Optional[List[float]]` | One weight per empirical value.                                                                                                                                                             |
| `edges`   | `Optional[List[float]]` | The bin edges of an empirical histogram.                                                                                                                                                    |
| `counts`  | `Optional[List[float]]` | One count per empirical histogram bin.                                                                                                                                                      |
| `stream`  | `Optional[str]`         | The name of the random sequence it draws from: its registry path without the sim id, such as arrival.orders. Set by the registry when the config is registered; not a constructor argument. |

Raises:

| Type         | Description                                                                                                          |
| ------------ | -------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If the family is unknown, a parameter it does not take is given, or a parameter it takes is missing or out of range. |

### `desflow.ContainerConfig(current_cap=None, max_cap=None, level=0)`

Bases: `CapacityConfig`

Configuration for a container: its capacity and the level it starts at.

`current_cap` is the container's capacity, its largest level, as it is a resource's capacity. The level has a registry path of its own, `.level`, so a scenario or an ingress message can set it during a run, such as a restock.

Attributes:

| Name          | Type                | Description                                                                                              |
| ------------- | ------------------- | -------------------------------------------------------------------------------------------------------- |
| `current_cap` | `Union[int, float]` | The capacity: the largest level.                                                                         |
| `max_cap`     | `Union[int, float]` | The largest capacity a live update can set.                                                              |
| `level`       | `Union[int, float]` | The amount held when the run starts, kept as the type given, as simpy.Container keeps it. Defaults to 0. |

### `desflow.BreakdownConfig(time_to_failure, repair, work='interrupt', clock='calendar')`

How a resource breaks down: the time to each failure, the repair time, and what happens to the work in progress.

The next failure is drawn when a repair ends, or at the start. With the `calendar` clock it counts that time busy or idle, and with the `busy` clock only while the resource holds a unit. While down, the resource's capacity is 0; its `current_cap` keeps the planned value, which a change during the repair sets for its return.

Attributes:

| Name              | Type                 | Description                                                                                                                                                        |
| ----------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `time_to_failure` | `DistributionConfig` | The time from a repair, or the start, to the next failure.                                                                                                         |
| `repair`          | `DistributionConfig` | How long a repair takes.                                                                                                                                           |
| `work`            | `str`                | interrupt (the default) interrupts the tasks being served, which follow their on_interrupt; finish lets them finish, and no new task starts until the repair ends. |
| `clock`           | `str`                | calendar (the default) or busy, the time the time to failure counts.                                                                                               |

## Data Payloads

### `desflow.TelemetryPayload`

Bases: `BaseStreamPayload`

Schema for low-volume, single-metric telemetry updates.

This payload is used for publishing continuous system state variables, such as resource utilization, queue lengths, or system lag, typically used for real-time dashboards.

Attributes:

| Name          | Type                   | Description                                                             |
| ------------- | ---------------------- | ----------------------------------------------------------------------- |
| `stream_type` | `Literal['telemetry']` | Hardcoded to "telemetry" for downstream routing.                        |
| `path_id`     | `str`                  | The dot-notation path of the metric (e.g., 'Shop.packers.utilization'). |
| `value`       | `Any`                  | The scalar value of the metric at the given simulation time.            |

### `desflow.EventPayload`

Bases: `BaseStreamPayload`

Schema for high-volume discrete simulation events.

This payload tracks state transitions of specific entities (like tasks or orders) throughout the simulation lifecycle. The `key` attribute allows message brokers like Kafka to maintain strict chronological ordering for specific tasks.

Attributes:

| Name          | Type               | Description                                                                               |
| ------------- | ------------------ | ----------------------------------------------------------------------------------------- |
| `stream_type` | `Literal['event']` | Hardcoded to "event" for downstream routing.                                              |
| `key`         | `str`              | The entity key, the record and partition key (e.g., 'orders-1').                          |
| `task`        | `Optional[str]`    | The name of the task the event belongs to, or None for an event published outside a task. |
| `instance`    | `Optional[Any]`    | The task's instance id, or None.                                                          |
| `value`       | `Dict[str, Any]`   | A dictionary containing the event's detailed payload.                                     |

### `desflow.LabelPayload`

Bases: `BaseStreamPayload`

Schema for ground-truth records: what a scenario step changed, and why.

They are sent only to sinks with `when: labels`, which receive nothing else, so data and labels always reach separate sinks.

Attributes:

| Name          | Type               | Description                                                                |
| ------------- | ------------------ | -------------------------------------------------------------------------- |
| `stream_type` | `Literal['label']` | Hardcoded to "label" for downstream routing.                               |
| `path`        | `Optional[str]`    | The registry path the step changed, or None for a step with only a record. |
| `old`         | `Any`              | The value before the step, or None.                                        |
| `new`         | `Any`              | The value after the step, or None.                                         |
| `label`       | `Dict[str, Any]`   | The step's record mapping.                                                 |

### `desflow.models.BaseStreamPayload`

Bases: `BaseModel`

Base schema for all egress data emitted by the simulation engine.

This schema guarantees that all outgoing data streams share a common temporal context, allowing downstream systems to accurately synchronize simulation time with real-world time.

Attributes:

| Name          | Type                                     | Description                                                                  |
| ------------- | ---------------------------------------- | ---------------------------------------------------------------------------- |
| `stream_type` | `Literal['telemetry', 'event', 'label']` | An identifier indicating the nature of the payload.                          |
| `sim_ts`      | `float`                                  | The simulation clock time (in seconds) when the payload was generated.       |
| `timestamp`   | `str`                                    | The real-world ISO 8601 timestamp indicating when the payload was generated. |

## Utilities

### `desflow.Sampler(rng=None, *, seed=None)`

Generates random numbers based on live `DistributionConfig` objects.

The Sampler evaluates the configuration *at the exact moment* it is called. This allows the simulation to dynamically respond to external parameter changes.

Each distribution draws from a random sequence of its own, made from the run's seed and the distribution's registry path without the sim id (`arrival.orders`, `service.packing`): `SeedSequence(seed, spawn_key=(CRC32 of the path,))`. Adding an arrival therefore leaves every other distribution's draws unchanged, so two versions of a model run with one seed draw the same numbers for every distribution they share (common random numbers).

Attributes:

| Name   | Type                  | Description                                                                                                                                                                      |
| ------ | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rng`  | `Optional[Generator]` | The run's main generator, for draws in your own code. Distributions do not draw from it, so adding one does not change these draws either. None when nothing is drawn at random. |
| `seed` | `Optional[int]`       | The seed the sequences come from, or None.                                                                                                                                       |

Initializes the Sampler.

With neither `rng` nor `seed`, nothing is drawn at random: each sample is the distribution's mean (for `truncated_normal`, its mean or `low`, whichever is higher).

Parameters:

| Name   | Type                  | Description                                                                                                                                                                                                                                                 | Default |
| ------ | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `rng`  | `Optional[Generator]` | A seeded NumPy generator, such as np.random.default_rng(42). Its seed gives each distribution its sequence, so Sampler(rng= np.random.default_rng(42)) draws the same numbers as Sampler(seed=42), and the generator itself stays the run's main generator. | `None`  |
| `seed` | `Optional[int]`       | The run's seed, instead of rng.                                                                                                                                                                                                                             | `None`  |

Raises:

| Type         | Description                     |
| ------------ | ------------------------------- |
| `ValueError` | If both rng and seed are given. |

Source code in `src/desflow/core/sampler.py`

```python
def __init__(
    self, rng: Optional[np.random.Generator] = None, *, seed: Optional[int] = None
):
    """
    Initializes the Sampler.

    With neither `rng` nor `seed`, nothing is drawn at random: each sample is
    the distribution's mean (for `truncated_normal`, its mean or `low`, whichever
    is higher).

    Args:
        rng: A seeded NumPy generator, such as `np.random.default_rng(42)`. Its
            seed gives each distribution its sequence, so `Sampler(rng=
            np.random.default_rng(42))` draws the same numbers as
            `Sampler(seed=42)`, and the generator itself stays the run's main
            generator.
        seed: The run's seed, instead of `rng`.

    Raises:
        ValueError: If both `rng` and `seed` are given.
    """
    if rng is not None and seed is not None:
        raise ValueError("give rng or seed, not both")
    if seed is not None:
        rng = np.random.default_rng(seed)
    self.rng = rng
    self._seed_sequence: Optional[np.random.SeedSequence] = None
    if rng is not None:
        sequence = rng.bit_generator.seed_seq
        if isinstance(sequence, np.random.SeedSequence):
            self._seed_sequence = sequence
    self.seed: Optional[int] = (
        None if self._seed_sequence is None else self._seed_sequence.entropy  # type: ignore[assignment]
    )
    self._generators: Dict[str, np.random.Generator] = {}
```

#### Methods:

##### `sample(config, min_delay=MIN_DELAY)`

Draws a random sample using the current parameters in the provided config.

A config the registry has registered draws from its own sequence; one built by hand and never registered draws from `rng`.

Parameters:

| Name        | Type                 | Description                                                | Default     |
| ----------- | -------------------- | ---------------------------------------------------------- | ----------- |
| `config`    | `DistributionConfig` | The live configuration object retrieved from the Registry. | *required*  |
| `min_delay` | `float`              | The smallest value returned.                               | `MIN_DELAY` |

Returns:

| Type                | Description                                                                                                                                                                                                                                       |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Union[int, float]` | Union\[int, float\]: A sampled time duration (e.g., arrival gap or service time), an integer when the config holds integers and its draw is one. Returns min_delay if the sample is smaller than min_delay. A std of 0 gives the mean every time. |

Raises:

| Type         | Description                               |
| ------------ | ----------------------------------------- |
| `ValueError` | If the dist type string is not supported. |

Source code in `src/desflow/core/sampler.py`

```python
def sample(
    self, config: DistributionConfig, min_delay: float = MIN_DELAY
) -> Union[int, float]:
    """
    Draws a random sample using the current parameters in the provided config.

    A config the registry has registered draws from its own sequence; one built
    by hand and never registered draws from `rng`.

    Args:
        config (DistributionConfig): The live configuration object retrieved from the Registry.
        min_delay (float): The smallest value returned.

    Returns:
        Union[int, float]: A sampled time duration (e.g., arrival gap or service time),
               an integer when the config holds integers and its draw is one.
               Returns `min_delay` if the sample is smaller than `min_delay`.
               A std of 0 gives the mean every time.

    Raises:
        ValueError: If the `dist` type string is not supported.
    """
    # The parameters were checked by DistributionConfig and by the registry on
    # every live update, so the ones each family uses are set and in range.
    family = get_distribution(config.dist)
    if self.rng is None:
        return max(min_delay, family.fixed(config))
    if not family.random:
        return max(min_delay, family.draw(config, self.rng))
    rng = self.rng if config.stream is None else self.generator(config.stream)
    return max(min_delay, family.draw(config, rng))  # type: ignore[arg-type]
```

### `desflow.time_to_seconds(time_str)`

Converts a human-readable duration into seconds.

Examples:

"1 week" -> 604800.0 "2.5 hours" -> 9000.0 "30 mins" -> 1800.0

Parameters:

| Name       | Type  | Description                                             | Default    |
| ---------- | ----- | ------------------------------------------------------- | ---------- |
| `time_str` | `str` | A number and a unit, such as 10 min, 2.5 hours or 90 s. | *required* |

Returns:

| Name    | Type    | Description  |
| ------- | ------- | ------------ |
| `float` | `float` | The seconds. |

Raises:

| Type         | Description                                   |
| ------------ | --------------------------------------------- |
| `ValueError` | If the text is not a number and a known unit. |

Source code in `src/desflow/utils.py`

```python
def time_to_seconds(time_str: str) -> float:
    """Converts a human-readable duration into seconds.

    Examples:
        "1 week"     -> 604800.0
        "2.5 hours"  -> 9000.0
        "30 mins"    -> 1800.0

    Args:
        time_str: A number and a unit, such as `10 min`, `2.5 hours` or `90 s`.

    Returns:
        float: The seconds.

    Raises:
        ValueError: If the text is not a number and a known unit.
    """
    match = _PATTERN.fullmatch(time_str)
    if not match:
        raise ValueError(f"Invalid time format: {time_str!r}")

    value = float(match.group(1))
    unit = match.group(2).lower()

    try:
        return value * MULTIPLIERS[unit]
    except KeyError:
        raise ValueError(f"Unknown time unit: {unit!r}") from None
```

## Resources

### `desflow.ResourceBase(env, sim_id, resource_id, queue=DEFAULT_QUEUE_ORDER, preempt=False, queue_limit=None, ties='fifo')`

Bases: `BaseDynamicResource`

A resource of units that tasks request, with a capacity that follows the registry.

It is `DynamicResource` built on SimPy's hooks, so a subclass changes it as a subclass of SimPy's `Resource` does: set `PutQueue` or `GetQueue`, set `request` or `release` to a `BoundClass` of a request or release class, or override `_do_put(request)` and `_do_get(release)`. Use a subclass with `type=` in `add_resource` or `type: !python module.Class` in a blueprint.

The constructor is `DynamicResource`'s, because a resource reads its capacity from the registry. A subclass that defines `__init__` takes `*args, **kwargs` and passes them on.

Grants follow SimPy's `Resource`: a request is granted when it is made if a unit is free and nothing waits ahead of it, and a release grants the head of the queue one event later, when its release event is processed; a release that gives back nothing tries the head too. Equal-time grants therefore happen in the order SimPy gives them.

Waiting requests are served in the resource's queue order: `fifo` (the default) serves the oldest first, `lifo` the newest first, and `priority` by the request's key, `(priority, time, not preempt)`, as SimPy's `PriorityResource` does: the lowest `priority` value first, then the earliest, then one that may preempt. A sequence number breaks the remaining ties, so such requests are served first in, first out.

Attributes:

| Name          | Type                           | Description                                         |
| ------------- | ------------------------------ | --------------------------------------------------- |
| `env`         | `DynamicRealtimeEnvironment`   | The active simulation environment.                  |
| `sim_id`      | `str`                          | The prefix ID (e.g., 'Shop').                       |
| `resource_id` | `str`                          | The specific resource name (e.g., 'packers').       |
| `queue_order` | `str`                          | fifo, lifo or priority.                             |
| `PutQueue`    | `Any`                          | The type of put_queue, RankedQueue by default.      |
| `GetQueue`    | `Any`                          | The type of get_queue, list by default.             |
| `put_queue`   |                                | The waiting requests in service order.              |
| `queue`       |                                | An alias of put_queue; len(queue) is how many wait. |
| `get_queue`   |                                | The releases being served, empty between events.    |
| `users`       | `List[DynamicResourceRequest]` | The granted requests, oldest first.                 |
| `count`       | `int`                          | How many users there are; the same as in_use.       |
| `request`     |                                | Makes a DynamicResourceRequest; a BoundClass.       |
| `release`     |                                | Makes a DynamicRelease; a BoundClass.               |

Initializes the resource and binds it to the registry.

Parameters:

| Name          | Type            | Description                                                                                                                                                                                                                                                                                                              | Default               |
| ------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------- |
| `env`         | `Environment`   | The SimPy environment (must include the RegistryMixIn).                                                                                                                                                                                                                                                                  | *required*            |
| `sim_id`      | `str`           | The parent simulation ID.                                                                                                                                                                                                                                                                                                | *required*            |
| `resource_id` | `str`           | The name of the resource.                                                                                                                                                                                                                                                                                                | *required*            |
| `queue`       | `str`           | The queue order: fifo, lifo or priority. Defaults to fifo. A request's priority counts only under priority.                                                                                                                                                                                                              | `DEFAULT_QUEUE_ORDER` |
| `preempt`     | `bool`          | When True, a request that finds every unit busy takes one from the user SimPy's PreemptiveResource would choose: the one whose key (priority, request time, not preempt) sorts last, if that key sorts after the request's. That user's process receives simpy.Interrupt with a Preempted cause. Needs queue="priority". | `False`               |
| `queue_limit` | `Optional[int]` | The most requests that may wait, or None for no limit. A request made while that many wait raises QueueFull, a RuntimeError, as SimPy's SortedQueue(maxlen) does, before it is granted or preempts a user. So 0 refuses every request.                                                                                   | `None`                |

Raises:

| Type         | Description                                                                                                                                                          |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If queue is not one of the three orders, preempt is set without queue="priority", or queue_limit is negative or is set on a class whose PutQueue is not the default. |

Source code in `src/desflow/resources/resource.py`

```python
def __init__(
    self,
    env: Environment,
    sim_id: str,
    resource_id: str,
    queue: str = DEFAULT_QUEUE_ORDER,
    preempt: Union[bool, PreemptRule] = False,
    queue_limit: Optional[int] = None,
    ties: str = "fifo",
):
    """
    Initializes the resource and binds it to the registry.

    Args:
        env (Environment): The SimPy environment (must include the RegistryMixIn).
        sim_id (str): The parent simulation ID.
        resource_id (str): The name of the resource.
        queue (str): The queue order: `fifo`, `lifo` or `priority`. Defaults to
            `fifo`. A request's `priority` counts only under `priority`.
        preempt (bool): When True, a request that finds every unit busy takes
            one from the user SimPy's `PreemptiveResource` would choose: the one
            whose key `(priority, request time, not preempt)` sorts last, if that
            key sorts after the request's. That user's process receives
            `simpy.Interrupt` with a `Preempted` cause. Needs `queue="priority"`.
        queue_limit (Optional[int]): The most requests that may wait, or None for
            no limit. A request made while that many wait raises `QueueFull`, a
            `RuntimeError`, as SimPy's `SortedQueue(maxlen)` does, before it is
            granted or preempts a user. So 0 refuses every request.

    Raises:
        ValueError: If `queue` is not one of the three orders, `preempt` is set
            without `queue="priority"`, or `queue_limit` is negative or is set on a
            class whose `PutQueue` is not the default.
    """
    check_resource_queue(queue, preempt, ties)
    check_resource_type(type(self), queue_limit)
    self.preempt = bool(preempt)
    self.preempt_rule = preempt if isinstance(preempt, PreemptRule) else None
    self.queue_order = queue
    self.ties = ties
    # Counts requests, so equal ranks keep their arrival order
    self._sequence = itertools.count()
    self._env = env
    self.put_queue = (
        self.PutQueue(maxlen=queue_limit)
        if queue_limit is not None
        else self.PutQueue()
    )
    self.queue = self.put_queue
    self.get_queue = self.GetQueue()
    self.users: List[DynamicResourceRequest] = []
    # The down state of a breakdown: no unit is granted while it is down
    self.down = False
    self.failures = 0
    self.has_breakdowns = False
    self._down_since: Optional[float] = None
    self._downtime = 0.0
    # The events `when_busy` and `when_idle` made, woken when the resource changes
    # between holding no unit and holding one
    self._busy_waiters: List[Event] = []
    self._idle_waiters: List[Event] = []
    # The registry values `_target` last read and its result, so a grant that
    # finds both values unchanged skips the conversion
    self._target_read: Tuple[Any, Any] = (None, None)
    self._target_value: Union[int, float] = 0
    super().__init__(env, sim_id, resource_id, "resources")
    BoundClass.bind_early(self)
    # The live limit, a registry path; one given here is its value from the start.
    # A class with its own PutQueue sets its own maxlen, so it has no path.
    path = f"{sim_id}.resources.{resource_id}.queue_limit"
    self._limit_val: Optional[Any] = None
    if _default_put_queue(type(self)):
        try:
            self._limit_val = env.registry.get(path)
        except KeyError:
            pass
    else:
        env.registry._forget(path)
    if self._limit_val is not None and queue_limit is not None:
        self._limit_val.track(queue_limit)

    self._capacity = self._target()
    self._register()
```

#### Attributes

##### `request = BoundClass(DynamicResourceRequest)`

Requests one unit: `request(priority=0, preempt=True)` returns a `DynamicResourceRequest`, an event that succeeds when a unit is granted. Lower `priority` values are served first under `queue="priority"`; `preempt` lets the request take a unit from a user of a higher value on a resource built with `preempt=True`. Set it to `BoundClass(MyRequest)` to use a subclass.

##### `release = BoundClass(DynamicRelease)`

Gives a granted unit back: `release(request)` returns a `DynamicRelease`, triggered at once. Releasing a request twice, or one never granted, gives back nothing; the queue is still tried when the release is processed. With no request, the oldest unit held is given back. While units are busy above a lowered capacity, a release brings the busy count down and grants nothing until it is within the capacity. Set it to `BoundClass(MyRelease)` to use a subclass.

##### `in_use`

Units currently held by tasks, including units above a lowered capacity.

##### `count`

Number of users currently using the resource, as SimPy's `Resource.count`; the same as `in_use`.

##### `capacity`

The current active capacity of the resource.

This value reflects the live state from the Registry and may change during the simulation.

### `desflow.DynamicResource(env, sim_id, resource_id, queue=DEFAULT_QUEUE_ORDER, preempt=False, queue_limit=None, ties='fifo')`

Bases: `ResourceBase`

A SimPy Resource with a dynamically adjustable capacity.

Unlike a standard `simpy.Resource` where capacity is fixed at creation, the `DynamicResource` listens to a `SimulationRegistry` path (e.g., 'Shop.resources.packers.current_cap'). When the registry updates the capacity, the resource adjusts at once, granting waiting requests if the capacity rises. It is `ResourceBase` as built, and the class to subclass for a resource of your own is `ResourceBase`.

Source code in `src/desflow/resources/resource.py`

```python
def __init__(
    self,
    env: Environment,
    sim_id: str,
    resource_id: str,
    queue: str = DEFAULT_QUEUE_ORDER,
    preempt: Union[bool, PreemptRule] = False,
    queue_limit: Optional[int] = None,
    ties: str = "fifo",
):
    """
    Initializes the resource and binds it to the registry.

    Args:
        env (Environment): The SimPy environment (must include the RegistryMixIn).
        sim_id (str): The parent simulation ID.
        resource_id (str): The name of the resource.
        queue (str): The queue order: `fifo`, `lifo` or `priority`. Defaults to
            `fifo`. A request's `priority` counts only under `priority`.
        preempt (bool): When True, a request that finds every unit busy takes
            one from the user SimPy's `PreemptiveResource` would choose: the one
            whose key `(priority, request time, not preempt)` sorts last, if that
            key sorts after the request's. That user's process receives
            `simpy.Interrupt` with a `Preempted` cause. Needs `queue="priority"`.
        queue_limit (Optional[int]): The most requests that may wait, or None for
            no limit. A request made while that many wait raises `QueueFull`, a
            `RuntimeError`, as SimPy's `SortedQueue(maxlen)` does, before it is
            granted or preempts a user. So 0 refuses every request.

    Raises:
        ValueError: If `queue` is not one of the three orders, `preempt` is set
            without `queue="priority"`, or `queue_limit` is negative or is set on a
            class whose `PutQueue` is not the default.
    """
    check_resource_queue(queue, preempt, ties)
    check_resource_type(type(self), queue_limit)
    self.preempt = bool(preempt)
    self.preempt_rule = preempt if isinstance(preempt, PreemptRule) else None
    self.queue_order = queue
    self.ties = ties
    # Counts requests, so equal ranks keep their arrival order
    self._sequence = itertools.count()
    self._env = env
    self.put_queue = (
        self.PutQueue(maxlen=queue_limit)
        if queue_limit is not None
        else self.PutQueue()
    )
    self.queue = self.put_queue
    self.get_queue = self.GetQueue()
    self.users: List[DynamicResourceRequest] = []
    # The down state of a breakdown: no unit is granted while it is down
    self.down = False
    self.failures = 0
    self.has_breakdowns = False
    self._down_since: Optional[float] = None
    self._downtime = 0.0
    # The events `when_busy` and `when_idle` made, woken when the resource changes
    # between holding no unit and holding one
    self._busy_waiters: List[Event] = []
    self._idle_waiters: List[Event] = []
    # The registry values `_target` last read and its result, so a grant that
    # finds both values unchanged skips the conversion
    self._target_read: Tuple[Any, Any] = (None, None)
    self._target_value: Union[int, float] = 0
    super().__init__(env, sim_id, resource_id, "resources")
    BoundClass.bind_early(self)
    # The live limit, a registry path; one given here is its value from the start.
    # A class with its own PutQueue sets its own maxlen, so it has no path.
    path = f"{sim_id}.resources.{resource_id}.queue_limit"
    self._limit_val: Optional[Any] = None
    if _default_put_queue(type(self)):
        try:
            self._limit_val = env.registry.get(path)
        except KeyError:
            pass
    else:
        env.registry._forget(path)
    if self._limit_val is not None and queue_limit is not None:
        self._limit_val.track(queue_limit)

    self._capacity = self._target()
    self._register()
```

### `desflow.ContainerBase`

Bases: `_Notifier`, `Container`

A SimPy `Container` that tells `DynamicContainer` about every change, the class to subclass for a container of your own.

Subclass it as you subclass `simpy.Container`: set `PutQueue` or `GetQueue`, set `put` or `get` to a `BoundClass` of an event class, or override `_do_put(event)` and `_do_get(event)`. Use the subclass with `type=` in `add_container` or `type: !python module.Class` in a blueprint. The constructor is SimPy's, `(env, capacity=inf, init=0)`; a subclass that defines `__init__` passes its arguments on.

A change of the level made by `_do_put` or `_do_get` is reported once to the statistics and to the conditions that read the container, whether the override calls `super()` or not, and whatever it returns. A change of the length of `put_queue` or `get_queue` is reported to the conditions that read `putters.` or `getters.`; a queue class must support `len()` and `remove()`.

Attributes:

| Name        | Type | Description                                |
| ----------- | ---- | ------------------------------------------ |
| `level`     |      | The amount held.                           |
| `put_queue` |      | The waiting puts, SimPy's queue.           |
| `get_queue` |      | The waiting gets, SimPy's queue.           |
| `put`       |      | Makes a DynamicContainerPut; a BoundClass. |
| `get`       |      | Makes a DynamicContainerGet; a BoundClass. |

#### Attributes

##### `put = BoundClass(DynamicContainerPut)`

##### `get = BoundClass(DynamicContainerGet)`

### `desflow.DynamicContainer(env, sim_id, container_id, init=None, type=None, get_ties=None, put_ties=None)`

Bases: `_WrappedHolder`, `Container`

A wrapper for SimPy Container with dynamic capacity updates.

It is also a SimPy `Container`, so `isinstance` holds. It runs none of SimPy's constructor: `put_queue`, `get_queue`, `level` and `capacity` read the wrapped container, and `put` and `get` call it.

Useful for modeling continuous bulk materials (fluids, gases, battery charge) and stock. Unlike a standard `simpy.Container`, the capacity of this container listens to a `SimulationRegistry` path and can be mutated at runtime.

`current_cap` is the capacity, the largest level, as it is for a resource. The level has a registry path of its own, `<sim_id>.containers.<name>.level`: the registry follows every put and get, and a live update to the path sets the level, such as a restock, waking the tasks it lets through.

**Capacity Shrinkage Behavior:** If the capacity is dynamically shrunk below the current `level` of the container, the material is NOT destroyed. The container will temporarily exist in an "overflow" state. All pending and future `put` requests will be strictly blocked until downstream processes `get` enough material to drain the `level` back below the new, smaller capacity.

Attributes:

| Name        | Type          | Description                                           |
| ----------- | ------------- | ----------------------------------------------------- |
| `env`       | `Environment` | The active SimPy simulation environment.              |
| `sim_id`    | `str`         | The parent simulation ID prefix.                      |
| `obj_id`    | `str`         | The specific container name/identifier.               |
| `put_queue` | `list`        | The waiting puts, SimPy's queue.                      |
| `get_queue` | `list`        | The waiting gets, SimPy's queue.                      |
| `PutQueue`  | `list`        | The type of put_queue, that of the wrapped container. |
| `GetQueue`  | `list`        | The type of get_queue, that of the wrapped container. |

Initializes the DynamicContainer and binds its capacity to the registry.

Parameters:

| Name           | Type                  | Description                                                                                                                                                                                                                                                                | Default    |
| -------------- | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `env`          | `Environment`         | The SimPy environment (must include Registry interface).                                                                                                                                                                                                                   | *required* |
| `sim_id`       | `str`                 | The parent simulation ID.                                                                                                                                                                                                                                                  | *required* |
| `container_id` | `str`                 | The unique name of this container.                                                                                                                                                                                                                                         | *required* |
| `init`         | `float`               | The amount in the container at simulation start. None, the default, takes the registry's level, which is the level of a ContainerConfig and 0 for a plain CapacityConfig.                                                                                                  | `None`     |
| `type`         | `Type[ContainerBase]` | A subclass of ContainerBase to build the container from, in place of the default; it is built as type(env, init=..., capacity=...). Defaults to None.                                                                                                                      | `None`     |
| `get_ties`     | `str`                 | None, the default, keeps SimPy's queue of waiting gets, first in, first out. fifo or lifo ranks them by the rank each get(rank=...) is made with, lowest first, and serves equal ranks oldest first or latest first. A get with no rank has rank 0. Not allowed with type. | `None`     |
| `put_ties`     | `str`                 | As get_ties, for the waiting puts.                                                                                                                                                                                                                                         | `None`     |

Raises:

| Type         | Description                                                                                                                                |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `ValueError` | If init is above the capacity, type is not a subclass of ContainerBase, or get_ties or put_ties is not fifo or lifo or is given with type. |

Source code in `src/desflow/resources/container.py`

```python
def __init__(
    self,
    env: Environment,
    sim_id: str,
    container_id: str,
    init: Optional[float] = None,
    type: Optional[Type[ContainerBase]] = None,
    get_ties: Optional[str] = None,
    put_ties: Optional[str] = None,
):
    """
    Initializes the DynamicContainer and binds its capacity to the registry.

    Args:
        env (Environment): The SimPy environment (must include Registry interface).
        sim_id (str): The parent simulation ID.
        container_id (str): The unique name of this container.
        init (float, optional): The amount in the container at simulation start.
            None, the default, takes the registry's `level`, which is the
            `level` of a `ContainerConfig` and 0 for a plain `CapacityConfig`.
        type (Type[ContainerBase], optional): A subclass of `ContainerBase` to
            build the container from, in place of the default; it is built as
            `type(env, init=..., capacity=...)`. Defaults to None.
        get_ties (str, optional): None, the default, keeps SimPy's queue of
            waiting gets, first in, first out. `fifo` or `lifo` ranks them by the
            `rank` each `get(rank=...)` is made with, lowest first, and serves
            equal ranks oldest first or latest first. A get with no `rank` has
            rank 0. Not allowed with `type`.
        put_ties (str, optional): As `get_ties`, for the waiting puts.

    Raises:
        ValueError: If `init` is above the capacity, `type` is not a subclass
            of `ContainerBase`, or `get_ties` or `put_ties` is not `fifo` or
            `lifo` or is given with `type`.
    """
    check_container_type(type)
    check_ties(get_ties, put_ties, type)
    super().__init__(env, sim_id, container_id, "containers")

    self._level_val = env.registry.get(f"{sim_id}.containers.{container_id}.level")
    max_cap = self._max_cap_val.value
    # Prevent initialization out-of-bounds
    initial_capacity = max(0, min(self._current_cap_val.value, max_cap))
    if init is None:
        # The configured level keeps the type it was given
        config = env.registry.get_config(f"{sim_id}.containers.{container_id}")
        init = getattr(config, "level", self._level_val.value)
    start = init
    # A number keeps its type, so a whole level stays an int, as SimPy keeps it
    if isinstance(start, bool) or not isinstance(start, (int, float)):
        start = float(start)

    if start > initial_capacity:
        # SimPy's own check, which the capacity of 1 below would not make
        raise ValueError('"init" must be <= "capacity".')
    kind = type or ContainerBase
    self._build_holder(
        lambda capacity: kind(env, init=start, capacity=capacity),
        initial_capacity,
        smallest=1.0,
        on_change=self._level_changed,
        get_ties=get_ties,
        put_ties=put_ties,
    )
    self._level_val.track(self._holder.level)
    internal_process(self.env, self._watch_level())
    self._register()
```

#### Attributes

##### `level`

float: The current amount of bulk material stored inside the container.

##### `capacity`

float: The current active maximum capacity of the container. This value reflects the live state from the Registry.

##### `putters_waiting`

int: Puts waiting for room. A put granted at once is not counted, although SimPy removes it from `put_queue` only after the grant.

##### `getters_waiting`

int: Gets waiting for an amount. A get granted at once is not counted, although SimPy removes it from `get_queue` only after the grant.

#### Methods:

##### `put(*args, **kwargs)`

Request to put a specific amount of material into the container.

Parameters:

| Name       | Type  | Description                                                                                | Default |
| ---------- | ----- | ------------------------------------------------------------------------------------------ | ------- |
| `*args`    | `Any` | The amount of material to add. A custom type whose put takes more arguments gets them all. | `()`    |
| `**kwargs` | `Any` | Keyword arguments, passed to the container's put.                                          | `{}`    |

Returns:

| Type    | Description                                                                 |
| ------- | --------------------------------------------------------------------------- |
| `Event` | simpy.events.Event: A SimPy event that triggers when capacity is available. |

Source code in `src/desflow/resources/container.py`

```python
def put(self, *args: Any, **kwargs: Any) -> Event:
    """
    Request to put a specific amount of material into the container.

    Args:
        *args: The amount of material to add. A custom `type` whose `put` takes
            more arguments gets them all.
        **kwargs: Keyword arguments, passed to the container's `put`.

    Returns:
        simpy.events.Event: A SimPy event that triggers when capacity is available.
    """
    return self._holder.put(*args, **kwargs)
```

##### `get(*args, **kwargs)`

Request to get a specific amount of material from the container.

Parameters:

| Name       | Type  | Description                                                                                     | Default |
| ---------- | ----- | ----------------------------------------------------------------------------------------------- | ------- |
| `*args`    | `Any` | The amount of material to withdraw. A custom type whose get takes more arguments gets them all. | `()`    |
| `**kwargs` | `Any` | Keyword arguments, passed to the container's get.                                               | `{}`    |

Returns:

| Type           | Description                                                                                            |
| -------------- | ------------------------------------------------------------------------------------------------------ |
| `ContainerGet` | simpy.resources.container.ContainerGet: A SimPy event that triggers when enough material is available. |

Source code in `src/desflow/resources/container.py`

```python
def get(self, *args: Any, **kwargs: Any) -> ContainerGet:
    """
    Request to get a specific amount of material from the container.

    Args:
        *args: The amount of material to withdraw. A custom `type` whose `get`
            takes more arguments gets them all.
        **kwargs: Keyword arguments, passed to the container's `get`.

    Returns:
        simpy.resources.container.ContainerGet: A SimPy event that triggers
            when enough material is available.
    """
    return self._holder.get(*args, **kwargs)
```

### `desflow.StoreBase`

Bases: `_Notifier`, `Store`

A SimPy `Store` that tells `DynamicStore` about every change, the class to subclass for a store of your own.

Subclass it as you subclass `simpy.Store`: set `PutQueue` or `GetQueue`, set `put` or `get` to a `BoundClass` of an event class, or override `_do_put(event)` and `_do_get(event)`. Use the subclass with `type=` in `add_store` or `type: !python module.Class` in a blueprint. The constructor is SimPy's, `(env, capacity=inf)`; a subclass that defines `__init__` passes its arguments on.

A change of the number of items made by `_do_put` or `_do_get` is reported once to the statistics and to the conditions that read the store, whether the override calls `super()` or not. A change of the length of `put_queue` or `get_queue` is reported to the conditions that read `putters.` or `getters.` (and `count`, `items`); a queue class must support `len()` and `remove()`.

Attributes:

| Name        | Type | Description                                      |
| ----------- | ---- | ------------------------------------------------ |
| `items`     |      | The items held, in the order a get returns them. |
| `put_queue` |      | The waiting puts, SimPy's queue.                 |
| `get_queue` |      | The waiting gets, SimPy's queue.                 |
| `put`       |      | Makes a DynamicStorePut; a BoundClass.           |
| `get`       |      | Makes a DynamicStoreGet; a BoundClass.           |

#### Attributes

##### `put = BoundClass(DynamicStorePut)`

##### `get = BoundClass(DynamicStoreGet)`

### `desflow.DynamicStore(env, sim_id, store_id, priority=False, items=None, type=None, get_ties=None, put_ties=None)`

Bases: `_WrappedHolder`, `Store`

A wrapper for SimPy FilterStore and PriorityStore with dynamic capacity updates.

It is also a SimPy `Store`, so `isinstance` holds. It runs none of SimPy's constructor: `put_queue`, `get_queue`, `items` and `capacity` read the wrapped store, and `put` and `get` call it.

Useful for modeling buffers, queues, or staging areas that hold discrete, heterogeneous items. By setting `priority=True`, the store will automatically sort incoming items (which must be sortable or wrapped in `simpy.PriorityItem`) so high-priority items are retrieved first.

**Capacity Shrinkage Behavior:** If the capacity is dynamically shrunk below the current number of `items` in the store, the existing items are NOT destroyed. The store will temporarily exist in an "over-capacity" state. All pending and future `put` requests will be blocked until downstream processes `get` enough items to bring the total item count below the new capacity.

Attributes:

| Name        | Type          | Description                                       |
| ----------- | ------------- | ------------------------------------------------- |
| `env`       | `Environment` | The active SimPy simulation environment.          |
| `sim_id`    | `str`         | The parent simulation ID prefix.                  |
| `obj_id`    | `str`         | The specific store name/identifier.               |
| `put_queue` | `list`        | The waiting puts, SimPy's queue.                  |
| `get_queue` | `list`        | The waiting gets, SimPy's queue.                  |
| `PutQueue`  | `list`        | The type of put_queue, that of the wrapped store. |
| `GetQueue`  | `list`        | The type of get_queue, that of the wrapped store. |

Initializes the DynamicStore and binds its capacity to the registry.

Parameters:

| Name       | Type              | Description                                                                                                                                                                                                                                                                | Default    |
| ---------- | ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `env`      | `Environment`     | The SimPy environment (must include Registry interface).                                                                                                                                                                                                                   | *required* |
| `sim_id`   | `str`             | The parent simulation ID.                                                                                                                                                                                                                                                  | *required* |
| `store_id` | `str`             | The unique name of this store.                                                                                                                                                                                                                                             | *required* |
| `priority` | `bool`            | If True, uses simpy.PriorityStore instead of simpy.FilterStore, which serves first in, first out as simpy.Store does and also takes a filter. Defaults to False.                                                                                                           | `False`    |
| `items`    | `Iterable`        | The items it holds when the run starts, in the order a get returns them. A priority store takes (priority, item) pairs and holds each as a PriorityItem, lowest priority first. Equal priorities leave in the order SimPy's heap gives them.                               | `None`     |
| `type`     | `Type[StoreBase]` | A subclass of StoreBase to build the store from, in place of the default; it is built as type(env, capacity=...). Defaults to None, the default store.                                                                                                                     | `None`     |
| `get_ties` | `str`             | None, the default, keeps SimPy's queue of waiting gets, first in, first out. fifo or lifo ranks them by the rank each get(rank=...) is made with, lowest first, and serves equal ranks oldest first or latest first. A get with no rank has rank 0. Not allowed with type. | `None`     |
| `put_ties` | `str`             | As get_ties, for the waiting puts.                                                                                                                                                                                                                                         | `None`     |

Raises:

| Type         | Description                                                                                                                                                                                                                             |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If there are more items than the capacity, a priority store's item is not a (priority, item) pair, type is not a subclass of StoreBase, type is given with priority, or get_ties or put_ties is not fifo or lifo or is given with type. |

Source code in `src/desflow/resources/store.py`

```python
def __init__(
    self,
    env: Environment,
    sim_id: str,
    store_id: str,
    priority: bool = False,
    items: Optional[Iterable[Any]] = None,
    type: Optional[Type[StoreBase]] = None,
    get_ties: Optional[str] = None,
    put_ties: Optional[str] = None,
):
    """
    Initializes the DynamicStore and binds its capacity to the registry.

    Args:
        env (Environment): The SimPy environment (must include Registry interface).
        sim_id (str): The parent simulation ID.
        store_id (str): The unique name of this store.
        priority (bool): If True, uses `simpy.PriorityStore` instead of
            `simpy.FilterStore`, which serves first in, first out as
            `simpy.Store` does and also takes a filter. Defaults to False.
        items (Iterable, optional): The items it holds when the run starts, in
            the order a get returns them. A priority store takes `(priority,
            item)` pairs and holds each as a `PriorityItem`, lowest priority
            first. Equal priorities leave in the order SimPy's heap gives them.
        type (Type[StoreBase], optional): A subclass of `StoreBase` to build the
            store from, in place of the default; it is built as
            `type(env, capacity=...)`. Defaults to None, the default store.
        get_ties (str, optional): None, the default, keeps SimPy's queue of
            waiting gets, first in, first out. `fifo` or `lifo` ranks them by the
            `rank` each `get(rank=...)` is made with, lowest first, and serves
            equal ranks oldest first or latest first. A get with no `rank` has
            rank 0. Not allowed with `type`.
        put_ties (str, optional): As `get_ties`, for the waiting puts.

    Raises:
        ValueError: If there are more items than the capacity, a priority
            store's item is not a `(priority, item)` pair, `type` is not a
            subclass of `StoreBase`, `type` is given with `priority`, or `get_ties`
            or `put_ties` is not `fifo` or `lifo` or is given with `type`.
    """
    check_store_type(type, priority)
    check_ties(get_ties, put_ties, type)
    super().__init__(env, sim_id, store_id, "stores")

    max_cap = _whole(self._max_cap_val.value)
    # Prevent initialization out-of-bounds
    initial_capacity = max(0, min(_whole(self._current_cap_val.value), max_cap))

    # Dynamically select the underlying SimPy storage engine
    self.priority = priority
    kind = type or (_PriorityStore if priority else _Store)
    self._build_holder(
        lambda capacity: kind(env, capacity=capacity),
        initial_capacity,
        smallest=1,
        on_change=self._changed,
        get_ties=get_ties,
        put_ties=put_ties,
    )
    start = list(items or [])
    if len(start) > initial_capacity:
        raise ValueError(
            f"store '{store_id}' holds {initial_capacity} items, so it cannot "
            f"start with {len(start)}"
        )
    for item in start:
        if priority:
            if not (isinstance(item, (tuple, list)) and len(item) == 2):
                raise ValueError(
                    f"a priority store starts with (priority, item) pairs, got "
                    f"{item!r}"
                )
            heapq.heappush(self._holder.items, self.wrap(item[1], item[0]))
        else:
            self._holder.items.append(item)
    self._register()
```

#### Attributes

##### `items`

list: The actual list of distinct item objects currently residing in the store. Assigning a list replaces the items, which `FilterStore` code does with `store.items = [...]`. As in SimPy, a get that is waiting is served at the next put or get, not at the assignment, and the store keeps the list given. A priority store takes a list that is already a heap.

##### `capacity`

int: The current active maximum slot capacity of the store. This value reflects the live state from the Registry.

##### `putters_waiting`

int: Puts waiting for room. A put granted at once is not counted, although SimPy removes it from `put_queue` only after the grant.

##### `getters_waiting`

int: Gets waiting for an item. A get granted at once is not counted, although SimPy removes it from `get_queue` only after the grant.

#### Methods:

##### `put(*args, **kwargs)`

Request to put a specific distinct item into the store.

Parameters:

| Name       | Type  | Description                                                                                   | Default |
| ---------- | ----- | --------------------------------------------------------------------------------------------- | ------- |
| `*args`    | `Any` | The item to place into the store. A custom type whose put takes more arguments gets them all. | `()`    |
| `**kwargs` | `Any` | Keyword arguments, passed to the store's put.                                                 | `{}`    |

Returns:

| Type    | Description                                                               |
| ------- | ------------------------------------------------------------------------- |
| `Event` | simpy.events.Event: A SimPy event that triggers when a slot is available. |

Source code in `src/desflow/resources/store.py`

```python
def put(self, *args: Any, **kwargs: Any) -> Event:
    """
    Request to put a specific distinct item into the store.

    Args:
        *args: The item to place into the store. A custom `type` whose `put`
            takes more arguments gets them all.
        **kwargs: Keyword arguments, passed to the store's `put`.

    Returns:
        simpy.events.Event: A SimPy event that triggers when a slot is available.
    """
    return self._holder.put(*args, **kwargs)
```

##### `get(*args, **kwargs)`

Request to get the next available item from the store.

Parameters:

| Name       | Type  | Description                                                                                                                                                                                                                                                                                                                                                            | Default |
| ---------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `*args`    | `Any` | The filter, if any. A custom type whose get takes more arguments gets them all.                                                                                                                                                                                                                                                                                        | `()`    |
| `**kwargs` | `Any` | Keyword arguments, passed to the store's get. filter, for a store without priority, is a function of an item that returns True for an item this get may take, as SimPy's FilterStore.get takes. The get takes the oldest matching item and waits while none matches; a waiting get that matches nothing does not stop a later one from taking an item that matches it. | `{}`    |

Returns:

| Type       | Description                                                                                                                                            |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `StoreGet` | simpy.resources.store.StoreGet: A SimPy event that yields the requested item object when available. Its cancel() takes a waiting get out of the queue. |

Raises:

| Type         | Description                                                                            |
| ------------ | -------------------------------------------------------------------------------------- |
| `ValueError` | If a priority store is given a filter: SimPy has no store that both ranks and filters. |

Source code in `src/desflow/resources/store.py`

```python
def get(self, *args: Any, **kwargs: Any) -> StoreGet:
    """
    Request to get the next available item from the store.

    Args:
        *args: The filter, if any. A custom `type` whose `get` takes more
            arguments gets them all.
        **kwargs: Keyword arguments, passed to the store's `get`. `filter`, for a
            store without priority, is a function of an item that
            returns True for an item this get may take, as SimPy's
            `FilterStore.get` takes. The get takes the oldest matching item and
            waits while none matches; a waiting get that matches nothing does
            not stop a later one from taking an item that matches it.

    Returns:
        simpy.resources.store.StoreGet: A SimPy event that yields the
            requested item object when available. Its `cancel()` takes a
            waiting get out of the queue.

    Raises:
        ValueError: If a priority store is given a filter: SimPy has no store
            that both ranks and filters.
    """
    # A filter of None is no filter
    if args and args[0] is None:
        args = args[1:]
    if kwargs.get("filter", 0) is None:
        del kwargs["filter"]
    if self.priority and (args or "filter" in kwargs):
        raise ValueError(
            "a priority store takes no filter: SimPy has no store that both "
            "ranks and filters its items"
        )
    return self._holder.get(*args, **kwargs)
```

### `desflow.resources.DynamicResourceRequest(resource, priority=0, preempt=True)`

Bases: `PriorityRequest`

A request for one unit of a `DynamicResource`, and the event of its grant.

It succeeds when a unit is granted. Used in a `with` block, leaving the block cancels it while it waits and releases the unit once it is granted, as a SimPy `Request` does. It is a SimPy `PriorityRequest`, so `isinstance` holds, and it does not run SimPy's constructor: the resource ranks and queues the request.

Attributes:

| Name          | Type              | Description                                                                                                                              |
| ------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `resource`    | `ResourceBase`    | The resource it asks.                                                                                                                    |
| `priority`    | `float`           | Its rank under the priority queue order, lowest first.                                                                                   |
| `preempt`     | `bool`            | Whether it may take a unit from a user of a higher priority value, on a resource built with preempt=True.                                |
| `key`         | `tuple`           | (priority, time, not preempt), the order SimPy ranks users by.                                                                           |
| `time`        | `float`           | The simulation time the request was made.                                                                                                |
| `usage_since` | `Optional[float]` | The simulation time of the grant, or None.                                                                                               |
| `proc`        |                   | The process that made the request, the one an interrupt would reach.                                                                     |
| `preempted`   | `bool`            | Whether a request of a higher rank took the unit.                                                                                        |
| `freed`       | `bool`            | Whether a release: {resource, task} of another flow gave the unit back; its holder is not told and does not ask again.                   |
| `attributes`  | `Any`             | The attributes of the entity that asked, which a holder.attr read of a preemption rule reads; None for a request made in Low-level code. |

Source code in `src/desflow/resources/resource.py`

```python
def __init__(
    self, resource: ResourceBase, priority: float = 0, preempt: bool = True
):
    Event.__init__(self, resource.env)
    self.resource = resource
    self.priority = priority
    self.preempt = preempt
    self.time = resource.env.now
    # SimPy's PreemptiveResource ranks users by this key: a lower priority
    # value, then an earlier request, then one that may preempt
    self.key = (priority, self.time, not preempt)
    self.usage_since: Optional[float] = None
    self.proc = resource.env.active_process
    resource._enqueue(self)
    resource._called()
```

#### Methods:

##### `cancel()`

Leaves the queue at once while waiting; does nothing once granted.

A granted unit is given back with `DynamicResource.release(request)`, as in SimPy, where `cancel` never releases a unit either.

Source code in `src/desflow/resources/resource.py`

```python
def cancel(self) -> None:
    """Leaves the queue at once while waiting; does nothing once granted.

    A granted unit is given back with `DynamicResource.release(request)`, as in
    SimPy, where `cancel` never releases a unit either.
    """
    if not self.triggered:
        self.resource._remove(self)
```

### `desflow.resources.DynamicRelease(resource, request=None)`

Bases: `Event`

The release of a unit of a `ResourceBase`, as SimPy's `Release` event.

It is triggered when it is made, because a release is served at once. The resource then tries the head of its queue one event later, when this event is processed. A subclass of this class, set as `release = BoundClass(MyRelease)` on a `ResourceBase` subclass, replaces it.

Attributes:

| Name       | Type                               | Description                                                                                                      |
| ---------- | ---------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `resource` | `ResourceBase`                     | The resource it gives a unit back to.                                                                            |
| `request`  | `Optional[DynamicResourceRequest]` | The request it releases. A release made with no request holds the oldest user, or None when nobody holds a unit. |
| `proc`     |                                    | The process that made the release.                                                                               |

Source code in `src/desflow/resources/resource.py`

```python
def __init__(
    self,
    resource: ResourceBase,
    request: Optional[DynamicResourceRequest] = None,
):
    super().__init__(resource.env)
    self.resource = resource
    if request is None and resource.users:
        request = resource.users[0]
    self.request = request
    self.proc = resource.env.active_process

    resource.get_queue.append(self)
    self.callbacks.append(resource._trigger_put)
    resource._trigger_get(None)
    resource._called()
```

### `desflow.resources.PreemptRule(when=None, victim=None)`

How a resource chooses the user a request preempts, and when it may.

Each argument is a function of the new request and a holder, both `DynamicResourceRequest`s. The resource gives the functions the request that found no free unit, and the holders that use a unit.

Parameters:

| Name     | Type                                  | Description                                                                                                                                                                                         | Default |
| -------- | ------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `when`   | `Optional[Callable[[Any, Any], Any]]` | Returns whether request takes the unit of holder. None is SimPy's rule: the holder's key sorts after the request's.                                                                                 | `None`  |
| `victim` | `Optional[Callable[[Any, Any], Any]]` | Returns a value for each holder, and the holder with the highest value is the one when is asked about; among equal values the holder granted first. None ranks holders by their key, as SimPy does. | `None`  |

Source code in `src/desflow/resources/resource.py`

```python
def __init__(
    self,
    when: Optional[Callable[[Any, Any], Any]] = None,
    victim: Optional[Callable[[Any, Any], Any]] = None,
) -> None:
    self.when = when
    self.victim = victim
```

### `desflow.resources.QueueFull`

Bases: `RuntimeError`

A request was refused because its resource's waiting line is full.

It is a `RuntimeError` with SimPy's message, as `SortedQueue(maxlen)` raises.

### `desflow.resources.DynamicContainerPut(resource, *args, rank=None, **kwargs)`

Bases: `_Ranked`, `_CancelReports`, `ContainerPut`

The put event of `ContainerBase`: SimPy's `ContainerPut`, whose `cancel()` also reports the change of the queue length. Its `rank` keyword ranks it in a queue that `DynamicContainer` was built to rank. Subclass it and set `put = BoundClass(MyPut)`.

Source code in `src/desflow/resources/base.py`

```python
def __init__(self, resource: Any, *args: Any, rank: Any = None, **kwargs: Any):
    if rank is not None:
        self.rank = rank
    super().__init__(resource, *args, **kwargs)  # type: ignore[call-arg]
```

### `desflow.resources.DynamicContainerGet(resource, *args, rank=None, **kwargs)`

Bases: `_Ranked`, `_CancelReports`, `ContainerGet`

The get event of `ContainerBase`: SimPy's `ContainerGet`, whose `cancel()` also reports the change of the queue length. Its `rank` keyword ranks it in a queue that `DynamicContainer` was built to rank. Subclass it and set `get = BoundClass(MyGet)`.

Source code in `src/desflow/resources/base.py`

```python
def __init__(self, resource: Any, *args: Any, rank: Any = None, **kwargs: Any):
    if rank is not None:
        self.rank = rank
    super().__init__(resource, *args, **kwargs)  # type: ignore[call-arg]
```

### `desflow.resources.DynamicStorePut(resource, *args, rank=None, **kwargs)`

Bases: `_Ranked`, `_CancelReports`, `StorePut`

The put event of `StoreBase`: SimPy's `StorePut`, whose `cancel()` also reports the change of the queue length. Its `rank` keyword ranks it in a queue that `DynamicStore` was built to rank. Subclass it and set `put = BoundClass(MyPut)`.

Source code in `src/desflow/resources/base.py`

```python
def __init__(self, resource: Any, *args: Any, rank: Any = None, **kwargs: Any):
    if rank is not None:
        self.rank = rank
    super().__init__(resource, *args, **kwargs)  # type: ignore[call-arg]
```

### `desflow.resources.DynamicStoreGet(resource, *args, rank=None, **kwargs)`

Bases: `_Ranked`, `_CancelReports`, `StoreGet`

The get event of `StoreBase`: SimPy's `StoreGet`, whose `cancel()` also reports the change of the queue length. Its `rank` keyword ranks it in a queue that `DynamicStore` was built to rank. Subclass it and set `get = BoundClass(MyGet)`.

Source code in `src/desflow/resources/base.py`

```python
def __init__(self, resource: Any, *args: Any, rank: Any = None, **kwargs: Any):
    if rank is not None:
        self.rank = rank
    super().__init__(resource, *args, **kwargs)  # type: ignore[call-arg]
```

### `desflow.resources.DynamicFilterStoreGet(resource, *args, rank=None, **kwargs)`

Bases: `_Ranked`, `_CancelReports`, `FilterStoreGet`

The get event of a filtered store: SimPy's `FilterStoreGet`, whose `cancel()` also reports the change of the queue length. It takes a `rank` keyword, as `DynamicStoreGet` does.

Source code in `src/desflow/resources/base.py`

```python
def __init__(self, resource: Any, *args: Any, rank: Any = None, **kwargs: Any):
    if rank is not None:
        self.rank = rank
    super().__init__(resource, *args, **kwargs)  # type: ignore[call-arg]
```

## Breakdowns, Schedules and Replay

The builder and YAML start these SimPy processes and readers for you. Low-level code starts them itself.

### `desflow.run_breakdowns(env, resource, sampler, work='interrupt', clock='calendar')`

Breaks a resource down and repairs it, for as long as the run lasts.

The time to failure and the repair time are read from the registry, at `<sim_id>.breakdown.<resource>.time_to_failure` and `.repair`, which a `SimParameter`'s `breakdowns` registers, so they can change during the run. Each failure publishes an event with the resource's name as its key and `{"status": "down", "reason": "failure"}`, and each repair one with `{"status": "repaired", "reason": "failure"}`; neither has a task.

Parameters:

| Name       | Type           | Description                                                                                                                                                                                                                                                                                                                 | Default       |
| ---------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| `env`      | `Any`          | The environment.                                                                                                                                                                                                                                                                                                            | *required*    |
| `resource` | `ResourceBase` | The resource that breaks down.                                                                                                                                                                                                                                                                                              | *required*    |
| `sampler`  | `Sampler`      | The sampler the times are drawn with.                                                                                                                                                                                                                                                                                       | *required*    |
| `work`     | `str`          | interrupt interrupts the tasks being served, which follow their on_interrupt; finish lets them finish.                                                                                                                                                                                                                      | `'interrupt'` |
| `clock`    | `str`          | calendar counts the time to failure from the start or the last repair, busy or idle. busy counts it only while the resource holds at least one unit, so an idle resource does not wear out. The time to failure is drawn once for each cycle in both. On busy, a time to failure of 0 fails at the grant of the first unit. | `'calendar'`  |

Raises:

| Type         | Description                                                                     |
| ------------ | ------------------------------------------------------------------------------- |
| `ValueError` | If work is neither interrupt nor finish, or clock is neither calendar nor busy. |

Source code in `src/desflow/core/breakdowns.py`

```python
def run_breakdowns(
    env: Any,
    resource: ResourceBase,
    sampler: Sampler,
    work: str = "interrupt",
    clock: str = "calendar",
):
    """
    Breaks a resource down and repairs it, for as long as the run lasts.

    The time to failure and the repair time are read from the registry, at
    `<sim_id>.breakdown.<resource>.time_to_failure` and `.repair`, which a
    `SimParameter`'s `breakdowns` registers, so they can change during the run. Each
    failure publishes an event with the resource's name as its key and
    `{"status": "down", "reason": "failure"}`, and each repair one with
    `{"status": "repaired", "reason": "failure"}`; neither has a task.

    Args:
        env: The environment.
        resource: The resource that breaks down.
        sampler: The sampler the times are drawn with.
        work: `interrupt` interrupts the tasks being served, which follow their
            `on_interrupt`; `finish` lets them finish.
        clock: `calendar` counts the time to failure from the start or the last
            repair, busy or idle. `busy` counts it only while the resource holds at
            least one unit, so an idle resource does not wear out. The time to
            failure is drawn once for each cycle in both. On `busy`, a time to failure
            of 0 fails at the grant of the first unit.

    Raises:
        ValueError: If `work` is neither `interrupt` nor `finish`, or `clock` is
            neither `calendar` nor `busy`.
    """
    if work not in BREAKDOWN_WORK:
        raise ValueError(f"work must be interrupt or finish, got '{work}'")
    if clock not in BREAKDOWN_CLOCKS:
        raise ValueError(f"clock must be calendar or busy, got '{clock}'")
    path = f"{resource.sim_id}.breakdown.{resource.obj_id}"
    resource.has_breakdowns = True
    while True:
        failure = env.registry.get_config(f"{path}.time_to_failure")
        if clock == "busy":
            yield from _busy_time(env, resource, sampler.sample(failure))
        else:
            yield env.timeout(sampler.sample(failure))
        resource.fail(interrupt_work=work == "interrupt")
        _publish(env, resource, "down")
        repair = env.registry.get_config(f"{path}.repair")
        yield env.timeout(sampler.sample(repair))
        resource.repair()
        _publish(env, resource, "repaired")
```

### `desflow.run_scenario(env, steps, until=None, interrupt=None, evaluate=None)`

Applies each step at its time, in time order and, at equal times, in the order given. A step's `at` counts from when this process starts, the start of the run when it is started with the model, so it does not depend on `initial_time`.

A step with a `path` and `value` changes the registry, and each `on_change` callback receives the change with the source `"scenario"`; one with a `record` publishes a ground-truth record of the change (`env.publish_label`); one with an `interrupt`, `{task, where, cause}`, calls `interrupt(task, where=..., cause=...)`, which the builder and YAML pass as `context.interrupt`.

Parameters:

| Name        | Type                             | Description                                                                                                                                                                                                                | Default    |
| ----------- | -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `env`       | `Any`                            | The environment.                                                                                                                                                                                                           | *required* |
| `steps`     | `Iterable[Mapping[str, Any]]`    | The steps, each a mapping as check_step takes.                                                                                                                                                                             | *required* |
| `until`     | `Optional[float]`                | The run's end, a reading of the clock; steps due at or after it are dropped, as an event due at exactly until does not run.                                                                                                | `None`     |
| `interrupt` | `Optional[Callable[..., Any]]`   | What an interrupt step calls; such steps need it.                                                                                                                                                                          | `None`     |
| `evaluate`  | `Optional[Callable[[Any], Any]]` | Evaluates a value that holds a generator when its step is due; the builder and YAML pass the run's value runtime. A value is evaluated before the change, a record after it, so a param in the record reads the new value. | `None`     |

Raises:

| Type            | Description                                                                                                                                                                                                                                                                                                                                                                       |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ParamRejected` | When the run reaches a step whose value the registry refuses, such as an exponential rate of 0. It stops the run, as any exception in a process does. The builder and a blueprint check the values with the run's overrides before the clock starts, so their runs reach it only when an ingress message, an on: action or a process has changed a related value before the step. |

Source code in `src/desflow/core/scenario.py`

```python
def run_scenario(
    env: Any,
    steps: Iterable[Mapping[str, Any]],
    until: Optional[float] = None,
    interrupt: Optional[Callable[..., Any]] = None,
    evaluate: Optional[Callable[[Any], Any]] = None,
):
    """
    Applies each step at its time, in time order and, at equal times, in the order
    given. A step's `at` counts from when this process starts, the start of the run
    when it is started with the model, so it does not depend on `initial_time`.

    A step with a `path` and `value` changes the registry, and each `on_change`
    callback receives the change with the source `"scenario"`; one with a `record`
    publishes a ground-truth record of the change (`env.publish_label`); one with an
    `interrupt`, `{task, where, cause}`, calls `interrupt(task, where=..., cause=...)`,
    which the builder and YAML pass as `context.interrupt`.

    Args:
        env: The environment.
        steps: The steps, each a mapping as `check_step` takes.
        until: The run's end, a reading of the clock; steps due at or after it are
            dropped, as an event due at exactly `until` does not run.
        interrupt: What an `interrupt` step calls; such steps need it.
        evaluate: Evaluates a value that holds a generator when its step is due;
            the builder and YAML pass the run's value runtime. A `value` is
            evaluated before the change, a `record` after it, so a `param` in the
            record reads the new value.

    Raises:
        ParamRejected: When the run reaches a step whose value the registry refuses,
            such as an exponential rate of 0. It stops the run, as any exception in
            a process does. The builder and a blueprint check the values with the
            run's overrides before the clock starts, so their runs reach it only
            when an ingress message, an `on:` action or a process has changed a
            related value before the step.
    """
    ordered: List[dict] = sorted(
        (check_step(step) for step in steps), key=lambda step: step["at"]
    )
    # `at` counts from the start of the run, the moment this process starts, so a
    # run with an initial_time of 8 h applies a step at 30 min at 8.5 h
    origin = env.now
    for step in ordered:
        at = origin + step["at"]
        if until is not None and at >= until:
            return
        if at > env.now:
            yield _urgent_timeout(env, at - env.now)
        apply_step(
            env,
            step.get("path"),
            step.get("value"),
            record=step.get("record"),
            interrupt=interrupt,
            action=step.get("interrupt"),
            source="scenario",
            refuse=True,
            evaluate=evaluate,
        )
```

### `desflow.read_trace(source, time='time', unit='s', fields='all', on_invalid='error', start=None, initial_time=0.0)`

Reads and checks the rows of a trace, in their order.

Parameters:

| Name           | Type                                            | Description                                                                                                                                                                                                     | Default    |
| -------------- | ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `source`       | `Union[str, Path, Iterable[Mapping[str, Any]]]` | A CSV file, a Parquet file (which needs the parquet extra), or an iterable of mappings, such as a list of dicts.                                                                                                | *required* |
| `time`         | `str`                                           | The column that holds each row's time.                                                                                                                                                                          | `'time'`   |
| `unit`         | `str`                                           | ms, s, min or h for a number of that unit from the start of the run, or timestamp for a date and time, read against start so that a row arrives at the time its records are stamped with.                       | `'s'`      |
| `fields`       | `Union[str, Sequence[str]]`                     | The columns each arrival carries as attributes, or all.                                                                                                                                                         | `'all'`    |
| `on_invalid`   | `str`                                           | error refuses a row whose time is missing or not a time; skip leaves it out, with a warning naming it.                                                                                                          | `'error'`  |
| `start`        | `Optional[datetime]`                            | The date and time of simulation time 0, as for every record's timestamp, needed with unit="timestamp", such as env.start_datetime. When it has a time zone, a timestamp without an offset is read in that zone. | `None`     |
| `initial_time` | `float`                                         | The simulation clock's start.                                                                                                                                                                                   | `0.0`      |

Returns:

| Type        | Description                                           |
| ----------- | ----------------------------------------------------- |
| `List[Row]` | List\[Row\]: The rows, each with its simulation time. |

Raises:

| Type          | Description                                                                                                                                                                                                                          |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `ValueError`  | If the unit or on_invalid is unknown; fields is a string other than all; a row is out of time order, naming the first one; a row's time is before the start; a field is missing; or a row's time is invalid and on_invalid is error. |
| `ImportError` | If a Parquet file is read without pyarrow.                                                                                                                                                                                           |

Source code in `src/desflow/core/replay.py`

```python
def read_trace(
    source: Union[str, Path, Iterable[Mapping[str, Any]]],
    time: str = "time",
    unit: str = "s",
    fields: Union[str, Sequence[str]] = "all",
    on_invalid: str = "error",
    start: Optional[datetime] = None,
    initial_time: float = 0.0,
) -> List[Row]:
    """
    Reads and checks the rows of a trace, in their order.

    Args:
        source: A CSV file, a Parquet file (which needs the `parquet` extra), or an
            iterable of mappings, such as a list of dicts.
        time: The column that holds each row's time.
        unit: `ms`, `s`, `min` or `h` for a number of that unit from the start of
            the run, or `timestamp` for a date and time, read against `start` so
            that a row arrives at the time its records are stamped with.
        fields: The columns each arrival carries as attributes, or `all`.
        on_invalid: `error` refuses a row whose time is missing or not a time;
            `skip` leaves it out, with a warning naming it.
        start: The date and time of simulation time 0, as for every record's
            `timestamp`, needed with `unit="timestamp"`, such as
            `env.start_datetime`. When it has a time zone, a timestamp without an
            offset is read in that zone.
        initial_time: The simulation clock's start.

    Returns:
        List[Row]: The rows, each with its simulation time.

    Raises:
        ValueError: If the unit or `on_invalid` is unknown; `fields` is a string
            other than `all`; a row is out of time order, naming the first one; a
            row's time is before the start; a field is missing; or a row's time is
            invalid and `on_invalid` is `error`.
        ImportError: If a Parquet file is read without pyarrow.
    """
    if unit not in UNITS and unit != TIMESTAMP:
        raise ValueError(
            f"unit must be one of {', '.join([*UNITS, TIMESTAMP])}, got '{unit}'"
        )
    if on_invalid not in ("error", "skip"):
        raise ValueError(f"on_invalid must be error or skip, got '{on_invalid}'")
    if unit == TIMESTAMP and start is None:
        raise ValueError("a timestamp column needs the run's logical_start_time")
    check_fields(fields)
    rows: List[Row] = []
    last: Optional[float] = None
    for number, record in enumerate(read_records(source), start=1):
        when = _time(record.get(time), unit, start, initial_time)
        if when is None:
            message = f"row {number} has no valid {time}: {record.get(time)!r}"
            if on_invalid == "error":
                raise ValueError(message)
            logger.warning(
                f"Skipped row {number}, which has no valid {time}: {record.get(time)!r}"
            )
            continue
        if when < initial_time:
            raise ValueError(
                f"row {number} is at {when:g} s, before the start of the run at "
                f"{initial_time:g} s; remove the row, or start the run earlier"
            )
        if last is not None and when < last:
            raise ValueError(
                f"row {number} is at {when:g} s, before the row above it at {last:g} s; "
                f"the rows must be in time order"
            )
        last = when
        if fields == "all":
            kept = {key: plain_value(value) for key, value in record.items()}
        else:
            missing = [name for name in fields if name not in record]
            if missing:
                raise ValueError(
                    f"row {number} has no field {', '.join(missing)}; add it to the "
                    f"row, or leave it out of fields"
                )
            kept = {name: plain_value(record[name]) for name in fields}
        rows.append(Row(when, number, kept))
    return rows
```

### `desflow.Row(time, number, fields)`

One arrival of a trace, as `read_trace` gives it (the `row` generator is `desflow.values.Row`).

Attributes:

| Name     | Type             | Description                                                  |
| -------- | ---------------- | ------------------------------------------------------------ |
| `time`   | `float`          | Its simulation time.                                         |
| `number` | `int`            | Its row number in the source, counting the first row as 1.   |
| `fields` | `Dict[str, Any]` | The fields it carries, which become the entity's attributes. |

## Admin and Infrastructure

### `desflow.connectors.admin.kafka.KafkaAdminConnector(bootstrap_servers, max_tasks=100, **kwargs)`

Unified Kafka Admin and Monitoring Connector.

This connector acts as a management layer for the simulation's Kafka infrastructure. It creates topics, sends config and collects telemetry and events, all with `aiokafka`; `create_topics` is a synchronous method.

It maintains an internal state of simulation vitals and task lifecycles by consuming from the simulation's egress topics.

Attributes:

| Name                | Type   | Description                                                                                   |
| ------------------- | ------ | --------------------------------------------------------------------------------------------- |
| `bootstrap_servers` | `str`  | Kafka broker addresses.                                                                       |
| `max_tasks`         | `int`  | The maximum number of task records to keep per service in memory to prevent unbounded growth. |
| `kwargs`            | `dict` | Additional arguments passed to Kafka clients.                                                 |

Initialize the connector with broker settings and state limits.

Parameters:

| Name                | Type  | Description                                                                        | Default    |
| ------------------- | ----- | ---------------------------------------------------------------------------------- | ---------- |
| `bootstrap_servers` | `str` | Kafka broker addresses.                                                            | *required* |
| `max_tasks`         | `int` | The maximum number of task records to keep per service in memory (rolling window). | `100`      |
| `**kwargs`          | `Any` | Additional arguments passed to Kafka clients (e.g., security settings).            | `{}`       |

Raises:

| Type         | Description                                      |
| ------------ | ------------------------------------------------ |
| `ValueError` | If max_tasks is not a whole number of 1 or more. |

Source code in `src/desflow/connectors/admin/kafka.py`

```python
def __init__(self, bootstrap_servers: str, max_tasks: int = 100, **kwargs: Any):
    """
    Initialize the connector with broker settings and state limits.

    Args:
        bootstrap_servers: Kafka broker addresses.
        max_tasks: The maximum number of task records to keep per service
            in memory (rolling window).
        **kwargs: Additional arguments passed to Kafka clients (e.g., security settings).

    Raises:
        ValueError: If `max_tasks` is not a whole number of 1 or more.
    """
    self.bootstrap_servers = bootstrap_servers
    self.max_tasks = checked_whole(max_tasks, "max_tasks", 1)
    self.kwargs = kwargs

    # Event State: sim_id -> service -> task_id -> {status: timestamp}
    self._state: DefaultDict[str, DefaultDict[str, Dict[str, Any]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    # Where each task key was last seen: key -> (sim_id, service), the oldest
    # forgotten first, so a finish record can be filed with its task's records
    self._homes: Dict[str, Any] = {}
    # Telemetry State: path_id -> latest_value
    self._vitals: Dict[str, Any] = {}
```

#### Methods:

##### `create_topics(topics_config)`

Creates Kafka topics required for the simulation.

This is a synchronous call to ensure that all necessary infrastructure (config, telemetry, and event topics) is ready before the simulation environment starts. It runs the asynchronous admin client to completion. When the calling thread already runs an event loop, it does so in a worker thread. A topic that already exists is ignored. A topic error that says the broker is not the controller is retried once with a new client. Any other error that the broker reports for a topic is raised.

Parameters:

| Name            | Type                   | Description                                                                                                                                                                                             | Default    |
| --------------- | ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `topics_config` | `List[Dict[str, Any]]` | A list of topic configurations. Each dict should contain 'name', and optionally 'partitions', 'replication' and 'topic_configs', a mapping of topic setting to value such as {"retention.ms": "60000"}. | *required* |

Raises:

| Type         | Description                                                                                                                                                                        |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `TypeError`  | If a setting passed to the connector is not one of AIOKafkaAdminClient's, such as the kafka-python setting ssl_cafile. Pass ssl_context instead.                                   |
| `KafkaError` | If the broker reports an error for a topic, such as an invalid replication factor. The error is the aiokafka.errors class for the error code. A topic that exists is not an error. |

Source code in `src/desflow/connectors/admin/kafka.py`

```python
def create_topics(self, topics_config: List[Dict[str, Any]]):
    """
    Creates Kafka topics required for the simulation.

    This is a synchronous call to ensure that all necessary infrastructure
    (config, telemetry, and event topics) is ready before the simulation
    environment starts. It runs the asynchronous admin client to completion. When
    the calling thread already runs an event loop, it does so in a worker
    thread. A topic that already exists is ignored. A topic error that says the
    broker is not the controller is retried once with a new client. Any other
    error that the broker reports for a topic is raised.

    Args:
        topics_config: A list of topic configurations.
            Each dict should contain 'name', and optionally 'partitions',
            'replication' and 'topic_configs', a mapping of topic setting to
            value such as `{"retention.ms": "60000"}`.

    Raises:
        TypeError: If a setting passed to the connector is not one of
            `AIOKafkaAdminClient`'s, such as the kafka-python setting
            `ssl_cafile`. Pass `ssl_context` instead.
        KafkaError: If the broker reports an error for a topic, such as an
            invalid replication factor. The error is the `aiokafka.errors`
            class for the error code. A topic that exists is not an error.
    """
    new_topics = [
        NewTopic(
            name=cfg["name"],
            num_partitions=cfg.get("partitions", 1),
            replication_factor=cfg.get("replication", 1),
            topic_configs=cfg.get("topic_configs") or None,
        )
        for cfg in topics_config
    ]
    self._run_to_completion(self._create_topics(new_topics))
```

##### `send_config(topic, path, value)`

Sends a surgical parameter update to a simulation config topic.

This method acts as an external controller, allowing users to dynamically update registry paths (e.g., arrival rates or resource capacities) while the simulation is running. The message is the JSON object `{"path": path, "value": value}`, the format `KafkaIngress` reads.

Parameters:

| Name    | Type  | Description                                                              | Default    |
| ------- | ----- | ------------------------------------------------------------------------ | ---------- |
| `topic` | `str` | The Kafka topic the simulation is listening to for config.               | *required* |
| `path`  | `str` | The registry dot-notation path (e.g., 'Shop.resources.packers.max_cap'). | *required* |
| `value` | `Any` | The new value to be applied to the path.                                 | *required* |

Source code in `src/desflow/connectors/admin/kafka.py`

```python
async def send_config(self, topic: str, path: str, value: Any):
    """
    Sends a surgical parameter update to a simulation config topic.

    This method acts as an external controller, allowing users to
    dynamically update registry paths (e.g., arrival rates or resource
    capacities) while the simulation is running. The message is the JSON
    object `{"path": path, "value": value}`, the format `KafkaIngress` reads.

    Args:
        topic: The Kafka topic the simulation is listening to for config.
        path: The registry dot-notation path (e.g., 'Shop.resources.packers.max_cap').
        value: The new value to be applied to the path.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=self.bootstrap_servers, **self.kwargs
    )
    try:
        await producer.start()  # inside the try, so a failed start closes it
        payload = json.dumps({"path": path, "value": value}).encode("utf-8")
        await producer.send_and_wait(topic, payload)
    finally:
        await producer.stop()
```

##### `collect_data(topics, auto_offset_reset='latest')`

Async loop to consume from telemetry and event topics.

This loop continuously listens to the simulation's egress and updates the connector's internal `_vitals` and `_state` attributes. A message whose value is null, JSON `null` or not JSON changes neither and is passed over, because `KafkaEgress` writes a null value for compacted topics.

Parameters:

| Name                | Type        | Description                                            | Default    |
| ------------------- | ----------- | ------------------------------------------------------ | ---------- |
| `topics`            | `List[str]` | List of topics to consume from (telemetry and events). | *required* |
| `auto_offset_reset` | `str`       | Where to start consuming if no offset is committed.    | `'latest'` |

Source code in `src/desflow/connectors/admin/kafka.py`

```python
async def collect_data(self, topics: List[str], auto_offset_reset: str = "latest"):
    """
    Async loop to consume from telemetry and event topics.

    This loop continuously listens to the simulation's egress and updates
    the connector's internal `_vitals` and `_state` attributes. A message whose
    value is null, JSON `null` or not JSON changes neither and is passed over,
    because `KafkaEgress` writes a null value for compacted topics.

    Args:
        topics: List of topics to consume from (telemetry and events).
        auto_offset_reset: Where to start consuming if no offset is committed.
    """
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=self.bootstrap_servers,
        auto_offset_reset=auto_offset_reset,
        **self.kwargs,
    )
    try:
        await consumer.start()  # inside the try, so a failed start closes it
        async for msg in consumer:
            self._process_message(_body(msg.value))
    finally:
        await consumer.stop()
```

##### `get_state()`

Returns the aggregated event state for task lifecycles.

Returns:

| Type             | Description                                                               |
| ---------------- | ------------------------------------------------------------------------- |
| `Dict[str, Any]` | A nested dictionary: sim_id -> service -> task_id -> {status: timestamp}. |

Source code in `src/desflow/connectors/admin/kafka.py`

```python
def get_state(self) -> Dict[str, Any]:
    """
    Returns the aggregated event state for task lifecycles.

    Returns:
        A nested dictionary: sim_id -> service -> task_id -> {status: timestamp}.
    """
    return self._state
```

##### `get_vitals()`

Returns the latest system telemetry metrics.

Returns:

| Type             | Description                                                                |
| ---------------- | -------------------------------------------------------------------------- |
| `Dict[str, Any]` | A dictionary of path_id to latest value (e.g., utilization, queue length). |

Source code in `src/desflow/connectors/admin/kafka.py`

```python
def get_vitals(self) -> Dict[str, Any]:
    """
    Returns the latest system telemetry metrics.

    Returns:
        A dictionary of path_id to latest value (e.g., utilization, queue length).
    """
    return self._vitals
```

## Connectors (Ingress)

### `desflow.connectors.ingress.base.BaseIngress`

Base class for all ingress providers in the simulation.

Ingress providers act as asynchronous listeners that bridge external data sources (such as Kafka topics, Redis channels, or local schedules) with the simulation's internal state. They are responsible for fetching updates and placing them into a thread-safe queue for the registry to process.

#### Methods:

##### `run(ingress_queue)`

Listens to an external source and pushes updates to the ingress queue.

This method should be implemented as an asynchronous loop that waits for external signals or data and converts them into (path, value) tuples suitable for the SimulationRegistry.

Parameters:

| Name            | Type    | Description                                                                      | Default    |
| --------------- | ------- | -------------------------------------------------------------------------------- | ---------- |
| `ingress_queue` | `Queue` | A thread-safe queue used to transmit updates to the main simulation environment. | *required* |

Raises:

| Type                  | Description                                    |
| --------------------- | ---------------------------------------------- |
| `NotImplementedError` | If the subclass does not override this method. |

Source code in `src/desflow/connectors/ingress/base.py`

```python
async def run(self, ingress_queue: queue.Queue) -> None:
    """
    Listens to an external source and pushes updates to the ingress queue.

    This method should be implemented as an asynchronous loop that waits
    for external signals or data and converts them into (path, value)
    tuples suitable for the SimulationRegistry.

    Args:
        ingress_queue: A thread-safe queue used to transmit updates
            to the main simulation environment.

    Raises:
        NotImplementedError: If the subclass does not override this method.
    """
    raise NotImplementedError("Subclasses must implement the run method.")
```

### `desflow.connectors.ingress.file.FileIngress(path, poll=1.0)`

Bases: `BaseIngress`

Ingress provider that applies the entries of a local YAML file when you edit it.

The file is a mapping of registry paths to values, one `path: value` line each:

```text
Shop.resources.packers.current_cap: 3
Shop.arrival.orders.rate: 0.5
```

The connector needs no service and no extra. It checks the file's modification time every `poll` wall-clock seconds and reads the file only when the time has changed. The file as it stands when the run starts counts as a first read.

An entry is applied when it first appears or when its value differs from the last valid read. An entry that is removed changes nothing. A file that does not exist yet, or is empty, is waited for without a log line. A file that is not valid YAML, or is not a mapping, is logged as one WARNING line and skipped, and the next valid read is compared with the last valid one. An entry with an empty path or a null value is logged as one WARNING line and skipped, as the other ingress connectors do.

Attributes:

| Name   | Type    | Description                                                        |
| ------ | ------- | ------------------------------------------------------------------ |
| `path` | `Path`  | The file to watch.                                                 |
| `poll` | `float` | Wall-clock seconds between checks of the file's modification time. |

Initializes the FileIngress.

Parameters:

| Name   | Type               | Description                                                                                                                                                                              | Default    |
| ------ | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `path` | `Union[str, Path]` | The YAML file to watch. A relative path is relative to the working directory of the run.                                                                                                 | *required* |
| `poll` | `float`            | Wall-clock seconds between checks of the file's modification time. The default is 1.0 second, so a saved edit lands within about one second; pass a smaller value for a faster reaction. | `1.0`      |

Source code in `src/desflow/connectors/ingress/file.py`

```python
def __init__(self, path: Union[str, Path], poll: float = 1.0):
    """
    Initializes the FileIngress.

    Args:
        path: The YAML file to watch. A relative path is relative to the
            working directory of the run.
        poll: Wall-clock seconds between checks of the file's modification
            time. The default is 1.0 second, so a saved edit lands within
            about one second; pass a smaller value for a faster reaction.
    """
    self.path = Path(path)
    self.poll = poll
```

#### Methods:

##### `run(ingress_queue)`

Watches the file and puts each new or changed entry on the queue.

Parameters:

| Name            | Type    | Description                                                                               | Default    |
| --------------- | ------- | ----------------------------------------------------------------------------------------- | ---------- |
| `ingress_queue` | `Queue` | A thread-safe queue used to transmit the (path, value) updates to the SimulationRegistry. | *required* |

Source code in `src/desflow/connectors/ingress/file.py`

```python
async def run(self, ingress_queue: queue.Queue) -> None:
    """
    Watches the file and puts each new or changed entry on the queue.

    Args:
        ingress_queue: A thread-safe queue used to transmit the (path, value)
            updates to the SimulationRegistry.
    """
    modified: Optional[int] = None
    previous: Dict[Any, Any] = {}
    try:
        while True:
            try:
                current = self.path.stat().st_mtime_ns
            except FileNotFoundError:
                # Waited for without a log line; a file created later is read
                modified = None
            else:
                if current != modified:
                    modified = current
                    entries = self._read()
                    if entries is not None:
                        self._put_changes(entries, previous, ingress_queue)
                        previous = entries
            await asyncio.sleep(self.poll)
    except asyncio.CancelledError:
        pass
```

### `desflow.connectors.ingress.kafka.KafkaIngress(topic, bootstrap_servers, topic_deserializers=None, default_deserializer=None, topic_policy=None, ssl=None, sasl_oauth_token_provider=None, **kwargs)`

Bases: `BaseIngress`

Resilient Kafka consumer for dynamic simulation configuration updates.

This connector subscribes to a specified Kafka topic and decodes incoming messages into state updates for the SimulationRegistry. It reads JSON by default. `default_deserializer` or `topic_deserializers` sets another deserializer, such as Avro, JSON Schema or Protobuf.

The expected resulting dictionary format must contain a 'path' string and a non-null 'value' of any serializable type. A message that cannot be decoded or does not have both is logged as a warning and skipped.

Attributes:

| Name                   | Type                                       | Description                                                   |
| ---------------------- | ------------------------------------------ | ------------------------------------------------------------- |
| `topic`                | `str`                                      | The Kafka topic name used for configuration signals.          |
| `bootstrap_servers`    | `str`                                      | Comma-separated string of Kafka broker addresses.             |
| `topic_deserializers`  | `Optional[Dict[str, MessageDeserializer]]` | Mapping of topics to specific deserializers.                  |
| `default_deserializer` | `Optional[MessageDeserializer]`            | Fallback deserializer. Defaults to JsonDeserializer.          |
| `kwargs`               | `dict`                                     | Additional configuration parameters for the AIOKafkaConsumer. |

Initializes the KafkaIngress with connection and subscription details.

Parameters:

| Name                        | Type                                       | Description                                                                                                                                                                                                                                                                                                                                                                                             | Default    |
| --------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `topic`                     | `str`                                      | The Kafka topic to subscribe to.                                                                                                                                                                                                                                                                                                                                                                        | *required* |
| `bootstrap_servers`         | `str`                                      | Kafka broker address or list of addresses.                                                                                                                                                                                                                                                                                                                                                              | *required* |
| `topic_deserializers`       | `Optional[Dict[str, MessageDeserializer]]` | Mapping of topics to their specific deserializers.                                                                                                                                                                                                                                                                                                                                                      | `None`     |
| `default_deserializer`      | `Optional[MessageDeserializer]`            | Fallback deserializer if the topic isn't explicitly mapped.                                                                                                                                                                                                                                                                                                                                             | `None`     |
| `topic_policy`              | `Any`                                      | How the control topic is treated before the run: a mode word (create, drop_and_create or manual) or a mapping of mode, partitions, replication_factor and topic_configs. create, the default, creates the topic when it is missing, so a consumer started before any producer does not wait for a topic that is not there. drop_and_create deletes it and creates it again, and manual creates nothing. | `None`     |
| `ssl`                       | `Optional[Dict[str, Any]]`                 | The keys cafile, certfile, keyfile and password, passed to aiokafka.helpers.create_ssl_context. The result is the consumer's ssl_context; set security_protocol to SSL or SASL_SSL as well.                                                                                                                                                                                                             | `None`     |
| `sasl_oauth_token_provider` | `Any`                                      | {type: aws_msk_iam, region: <region>} builds the token provider for Amazon MSK IAM, which needs the package aws-msk-iam-sasl-signer-python; a token provider object is used as it is.                                                                                                                                                                                                                   | `None`     |
| `**kwargs`                  | `Any`                                      | Arbitrary keyword arguments passed to the underlying AIOKafkaConsumer.                                                                                                                                                                                                                                                                                                                                  | `{}`       |

Raises:

| Type          | Description                                                                                            |
| ------------- | ------------------------------------------------------------------------------------------------------ |
| `ValueError`  | If topic_policy, ssl or the token provider is malformed.                                               |
| `ImportError` | If sasl_oauth_token_provider asks for aws_msk_iam and aws-msk-iam-sasl-signer-python is not installed. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(
    self,
    topic: str,
    bootstrap_servers: str,
    topic_deserializers: Optional[Dict[str, MessageDeserializer]] = None,
    default_deserializer: Optional[MessageDeserializer] = None,
    topic_policy: Any = None,
    ssl: Optional[Dict[str, Any]] = None,
    sasl_oauth_token_provider: Any = None,
    **kwargs: Any,
):
    """
    Initializes the KafkaIngress with connection and subscription details.

    Args:
        topic: The Kafka topic to subscribe to.
        bootstrap_servers: Kafka broker address or list of addresses.
        topic_deserializers: Mapping of topics to their specific deserializers.
        default_deserializer: Fallback deserializer if the topic isn't explicitly mapped.
        topic_policy: How the control topic is treated before the run: a mode word
            (`create`, `drop_and_create` or `manual`) or a mapping of `mode`,
            `partitions`, `replication_factor` and `topic_configs`. `create`,
            the default, creates the topic when it is missing, so a consumer
            started before any producer does not wait for a topic that is not
            there. `drop_and_create` deletes it and creates it again, and
            `manual` creates nothing.
        ssl: The keys `cafile`, `certfile`, `keyfile` and `password`, passed to
            `aiokafka.helpers.create_ssl_context`. The result is the consumer's
            `ssl_context`; set `security_protocol` to `SSL` or `SASL_SSL` as well.
        sasl_oauth_token_provider: `{type: aws_msk_iam, region: <region>}` builds
            the token provider for Amazon MSK IAM, which needs the package
            `aws-msk-iam-sasl-signer-python`; a token provider object is used as
            it is.
        **kwargs: Arbitrary keyword arguments passed to the underlying AIOKafkaConsumer.

    Raises:
        ValueError: If `topic_policy`, `ssl` or the token provider is malformed.
        ImportError: If `sasl_oauth_token_provider` asks for `aws_msk_iam` and
            `aws-msk-iam-sasl-signer-python` is not installed.
    """
    self.topic = topic
    self.bootstrap_servers = bootstrap_servers

    self.topic_deserializers = topic_deserializers or {}
    self.default_deserializer = default_deserializer or JsonDeserializer()

    self.topic_policy = read_topic_policy(topic_policy)
    self.kwargs = with_security(kwargs, ssl, sasl_oauth_token_provider)
```

#### Methods:

##### `run(ingress_queue)`

Consumes parameter changes from Kafka and puts each on the ingress queue.

Each message is read as JSON with `path` and `value`; a message that cannot be read is logged and skipped.

Parameters:

| Name            | Type    | Description                                                             | Default    |
| --------------- | ------- | ----------------------------------------------------------------------- | ---------- |
| `ingress_queue` | `Queue` | The queue the environment applies changes from, as (path, value) pairs. | *required* |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
async def run(self, ingress_queue: queue.Queue) -> None:
    """Consumes parameter changes from Kafka and puts each on the ingress queue.

    Each message is read as JSON with `path` and `value`; a message that cannot be
    read is logged and skipped.

    Args:
        ingress_queue: The queue the environment applies changes from, as
            `(path, value)` pairs.
    """
    await asyncio.to_thread(self._create_topic)
    backoff = 1.0
    while True:
        try:
            consumer = AIOKafkaConsumer(
                self.topic, bootstrap_servers=self.bootstrap_servers, **self.kwargs
            )
            try:
                # Started inside the try, so a failed start also closes the
                # consumer: aiokafka leaves it open otherwise.
                await consumer.start()
                logger.info(f"Connected to Kafka Ingress topic: {self.topic}")
                backoff = 1.0

                async for msg in consumer:
                    try:
                        # Apply configured Deserializer strategy
                        deserializer = self.topic_deserializers.get(
                            msg.topic, self.default_deserializer
                        )

                        # Decode binary payload -> Python Dict
                        data = deserializer.deserialize(msg.topic, msg.value)

                        ingress_queue.put(parse_change(data))

                    except Exception as e:
                        logger.warning(f"Skipped a Kafka ingress message: {e}")
            finally:
                await consumer.stop()

        except asyncio.CancelledError:
            logger.info("Kafka Ingress shut down requested. Exiting loop.")
            break
        except Exception as e:
            logger.error(f"Kafka Ingress error: {e}. Retrying in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
```

### `desflow.connectors.ingress.local.LocalIngress(schedule)`

Bases: `BaseIngress`

Ingress provider for deterministic, time-scheduled parameter updates.

This connector is primarily used for testing, benchmarking, or local simulation runs where parameter changes need to occur at specific wall-clock intervals without requiring an external message broker like Kafka. It feeds a pre-defined sequence of updates into the simulation registry based on relative delays.

Attributes:

| Name       | Type                           | Description                                                                                       |
| ---------- | ------------------------------ | ------------------------------------------------------------------------------------------------- |
| `schedule` | `List[Tuple[float, str, Any]]` | A chronologically sorted list of updates, where each tuple contains (delay_seconds, path, value). |

Initializes the LocalIngress and sorts the provided schedule.

Parameters:

| Name       | Type                           | Description                                                                                                                                                         | Default    |
| ---------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `schedule` | `List[Tuple[float, str, Any]]` | A list of tuples representing scheduled updates. Format: [(delay_from_start, "registry_path", new_value), ...]. The list is automatically sorted by the delay time. | *required* |

Source code in `src/desflow/connectors/ingress/local.py`

```python
def __init__(self, schedule: List[Tuple[float, str, Any]]):
    """
    Initializes the LocalIngress and sorts the provided schedule.

    Args:
        schedule: A list of tuples representing scheduled updates.
            Format: [(delay_from_start, "registry_path", new_value), ...].
            The list is automatically sorted by the delay time.
    """
    # Sort schedule by delay to ensure chronological processing
    self.schedule = sorted(schedule, key=lambda x: x[0])
```

#### Methods:

##### `run(ingress_queue)`

Processes the schedule and pushes updates to the queue at the correct wall-clock times.

This method calculates the relative sleep intervals between scheduled events to ensure that updates are placed in the ingress queue precisely when requested. Because it uses `asyncio.sleep`, it does not block the main simulation execution.

Parameters:

| Name            | Type    | Description                                                                               | Default    |
| --------------- | ------- | ----------------------------------------------------------------------------------------- | ---------- |
| `ingress_queue` | `Queue` | A thread-safe queue used to transmit the (path, value) updates to the SimulationRegistry. | *required* |

Source code in `src/desflow/connectors/ingress/local.py`

```python
async def run(self, ingress_queue: queue.Queue) -> None:
    """
    Processes the schedule and pushes updates to the queue at the
    correct wall-clock times.

    This method calculates the relative sleep intervals between
    scheduled events to ensure that updates are placed in the
    ingress queue precisely when requested. Because it uses
    `asyncio.sleep`, it does not block the main simulation execution.

    Args:
        ingress_queue: A thread-safe queue used to transmit the
            (path, value) updates to the SimulationRegistry.
    """
    last_time = 0.0
    for delay, path, value in self.schedule:
        # Wait for the relative time difference
        await asyncio.sleep(delay - last_time)
        ingress_queue.put((path, value))
        last_time = delay
```

### `desflow.connectors.ingress.postgres.PostgresIngress(connection_dsn, table_name='simulation_params', poll_interval=2.0, **kwargs)`

Bases: `BaseIngress`

Asynchronous ingress provider for polling updates from a PostgreSQL database.

This connector polls a specified configuration table for rows where 'is_applied' is FALSE. It parses the parameter path and value, forwards them to the simulation registry, and then updates the row to mark it as applied. A row with a null path or value is logged as a warning, skipped and marked as applied, so it is not read again.

Initializes the PostgresIngress.

Parameters:

| Name             | Type    | Description                                                                                                                | Default               |
| ---------------- | ------- | -------------------------------------------------------------------------------------------------------------------------- | --------------------- |
| `connection_dsn` | `str`   | PostgreSQL connection string (DSN).                                                                                        | *required*            |
| `table_name`     | `str`   | Table to poll for parameter updates. Must have columns: id (serial), path (text), value (text/json), is_applied (boolean). | `'simulation_params'` |
| `poll_interval`  | `float` | Seconds to wait between database polls.                                                                                    | `2.0`                 |
| `**kwargs`       | `Any`   | Additional connection pool arguments for asyncpg.                                                                          | `{}`                  |

Source code in `src/desflow/connectors/ingress/postgres.py`

```python
def __init__(
    self,
    connection_dsn: str,
    table_name: str = "simulation_params",
    poll_interval: float = 2.0,
    **kwargs: Any,
):
    """
    Initializes the PostgresIngress.

    Args:
        connection_dsn: PostgreSQL connection string (DSN).
        table_name: Table to poll for parameter updates. Must have columns:
                    id (serial), path (text), value (text/json), is_applied (boolean).
        poll_interval: Seconds to wait between database polls.
        **kwargs: Additional connection pool arguments for asyncpg.
    """
    self.dsn = connection_dsn
    self.table_name = table_name
    self.poll_interval = poll_interval
    self.kwargs = kwargs
    self.pool: asyncpg.Pool | None = None
```

#### Methods:

##### `run(ingress_queue)`

Main execution loop fetching dynamic parameter updates from PostgreSQL.

Parameters:

| Name            | Type    | Description                                                                          | Default    |
| --------------- | ------- | ------------------------------------------------------------------------------------ | ---------- |
| `ingress_queue` | `Queue` | A thread-safe queue used to transmit (path, value) tuples to the SimulationRegistry. | *required* |

Source code in `src/desflow/connectors/ingress/postgres.py`

```python
async def run(self, ingress_queue: queue.Queue) -> None:
    """
    Main execution loop fetching dynamic parameter updates from PostgreSQL.

    Args:
        ingress_queue: A thread-safe queue used to transmit (path, value)
            tuples to the SimulationRegistry.
    """
    try:
        await self._init_pool()
        await self._ingest(ingress_queue)
    finally:
        await self._close_pool()
```

### `desflow.connectors.ingress.redis.RedisIngress(url, channel_name='params', poll_interval=0.5, **kwargs)`

Bases: `BaseIngress`

Asynchronous ingress provider for dynamic parameter updates via Redis Pub/Sub.

This connector utilizes `redis.asyncio` to listen on a specified Redis channel for parameter updates, parsing them from JSON, and pushing them to the simulation's internal update queue. Each message is a JSON object with `path` and `value`, such as `{"path": "Shop.arrival.orders.rate", "value": 10.0}`; a message that cannot be parsed or does not have both is logged as a warning and skipped.

Attributes:

| Name            | Type     | Description                                |
| --------------- | -------- | ------------------------------------------ |
| `url`           | `str`    | The connection URL for the Redis server.   |
| `channel_name`  | `str`    | The Redis Pub/Sub channel to subscribe to. |
| `poll_interval` | `float`  | Sleep interval when no messages are found. |
| `client`        | \`Redis  | None\`                                     |
| `pubsub`        | \`PubSub | None\`                                     |
| `**kwargs`      | \`PubSub | None\`                                     |

Initializes the RedisIngress with connection and channel settings.

Parameters:

| Name            | Type    | Description                                                   | Default    |
| --------------- | ------- | ------------------------------------------------------------- | ---------- |
| `url`           | `str`   | Redis connection URL (e.g., redis://127.0.0.1:6379/0).        | *required* |
| `channel_name`  | `str`   | Redis Pub/Sub channel to subscribe to.                        | `'params'` |
| `poll_interval` | `float` | Sleep interval when no messages are found.                    | `0.5`      |
| `**kwargs`      | `Any`   | Additional configuration dictionary passed to redis.from_url. | `{}`       |

Source code in `src/desflow/connectors/ingress/redis.py`

```python
def __init__(
    self,
    url: str,
    channel_name: str = "params",
    poll_interval: float = 0.5,
    **kwargs: Any,
):
    """
    Initializes the RedisIngress with connection and channel settings.

    Args:
        url: Redis connection URL (e.g., redis://127.0.0.1:6379/0).
        channel_name: Redis Pub/Sub channel to subscribe to.
        poll_interval: Sleep interval when no messages are found.
        **kwargs: Additional configuration dictionary passed to `redis.from_url`.
    """
    self.url = url
    self.channel_name = channel_name
    self.poll_interval = poll_interval
    self.kwargs = kwargs
    self.client: redis.Redis | None = None
    self.pubsub: redis.client.PubSub | None = None
```

#### Methods:

##### `run(update_queue)`

The main execution loop for subscribing to Redis and receiving updates.

Continuously polls the Pub/Sub channel for incoming JSON messages containing `path` and `value`, placing successfully parsed updates onto the internal update queue.

Parameters:

| Name           | Type    | Description                                                                                               | Default    |
| -------------- | ------- | --------------------------------------------------------------------------------------------------------- | ---------- |
| `update_queue` | `Queue` | A thread-safe queue where incoming parameters are pushed so they can be applied within the SimPy context. | *required* |

Source code in `src/desflow/connectors/ingress/redis.py`

```python
async def run(self, update_queue: queue.Queue) -> None:
    """
    The main execution loop for subscribing to Redis and receiving updates.

    Continuously polls the Pub/Sub channel for incoming JSON messages containing
    `path` and `value`, placing successfully parsed updates onto the
    internal update queue.

    Args:
        update_queue: A thread-safe queue where incoming parameters are pushed
            so they can be applied within the SimPy context.
    """
    self.client = redis.from_url(self.url, **self.kwargs)
    self.pubsub = self.client.pubsub()
    await self.pubsub.subscribe(self.channel_name)
    logger.info(f"RedisIngress subscribed to channel '{self.channel_name}'")

    try:
        while True:
            message = await self.pubsub.get_message(
                ignore_subscribe_messages=True, timeout=self.poll_interval
            )
            if message and message["type"] == "message":
                try:
                    update_queue.put(parse_change(orjson.loads(message["data"])))
                except Exception as e:
                    logger.warning(f"Skipped a Redis ingress message: {e}")
            else:
                await asyncio.sleep(self.poll_interval)
    except asyncio.CancelledError:
        pass
    finally:
        if self.pubsub:
            await self.pubsub.unsubscribe(self.channel_name)
        if self.client:
            await self.client.aclose()
```

## Deserializers

### `desflow.connectors.ingress.kafka.MessageDeserializer`

Bases: `Protocol`

Protocol defining the interface for all Kafka ingress deserializers.

Any custom deserializer passed to `KafkaIngress` must implement the `deserialize` method according to this signature.

#### Methods:

##### `deserialize(topic, payload)`

Deserializes a raw byte payload from Kafka into a Python object.

Parameters:

| Name      | Type    | Description                                          | Default    |
| --------- | ------- | ---------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed. | *required* |
| `payload` | `bytes` | The raw binary payload of the Kafka message.         | *required* |

Returns:

| Name  | Type  | Description                                                                    |
| ----- | ----- | ------------------------------------------------------------------------------ |
| `Any` | `Any` | The deserialized data, typically a dictionary containing a 'path' and 'value'. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes a raw byte payload from Kafka into a Python object.

    Args:
        topic (str): The Kafka topic from which the message was consumed.
        payload (bytes): The raw binary payload of the Kafka message.

    Returns:
        Any: The deserialized data, typically a dictionary containing a 'path' and 'value'.
    """
    ...
```

### `desflow.connectors.ingress.kafka.JsonDeserializer`

Default fallback deserializer providing backward compatibility via standard JSON.

This deserializer assumes the incoming payload is a UTF-8 encoded JSON string.

#### Methods:

##### `deserialize(topic, payload)`

Deserializes a JSON byte string.

Parameters:

| Name      | Type    | Description                                                   | Default    |
| --------- | ------- | ------------------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed (unused). | *required* |
| `payload` | `bytes` | The UTF-8 encoded JSON byte string.                           | *required* |

Returns:

| Name  | Type  | Description                                  |
| ----- | ----- | -------------------------------------------- |
| `Any` | `Any` | The parsed JSON data as a Python dictionary. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes a JSON byte string.

    Args:
        topic (str): The Kafka topic from which the message was consumed (unused).
        payload (bytes): The UTF-8 encoded JSON byte string.

    Returns:
        Any: The parsed JSON data as a Python dictionary.
    """
    return json.loads(payload.decode("utf-8"))
```

### `desflow.connectors.ingress.kafka.ConfluentAvroDeserializer(registry_url, *, auth=UNSET, verify=UNSET, cert=UNSET, timeout=UNSET, headers=UNSET, proxy=UNSET, max_retries=UNSET)`

Lazy-loaded deserializer for Confluent Schema Registry using fastavro and httpx.

This class converts Avro-encoded byte strings back into Python dictionaries. It fetches the writer schema from the registry using the schema id (or GUID) in the message's prefix, once for each id, so no `schema_str` is required during initialization.

Initializes the Confluent Avro deserializer. It makes no request to the registry.

`UNSET` as a default, in the signature or the Default column of the API page, means the argument was not given; each argument's description says its real default.

Parameters:

| Name           | Type                          | Description                                                                                                                              | Default      |
| -------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `registry_url` | `str`                         | The base URL of the Confluent Schema Registry. Several URLs separated by commas are tried in order.                                      | *required*   |
| `auth`         | `Optional[Tuple[str, str]]`   | A username and password for basic authentication, for example ("user", "pass"). The default is None.                                     | `UNSET`      |
| `verify`       | \`bool                        | str                                                                                                                                      | SSLContext\` |
| `cert`         | \`str                         | tuple                                                                                                                                    | None\`       |
| `timeout`      | `Optional[float]`             | Seconds each step of a request to the registry (connecting, sending, reading) may take. The default is 10.0. None waits without a limit. | `UNSET`      |
| `headers`      | `Optional[Mapping[str, str]]` | Headers sent with every request, for example {"Authorization": "Bearer <token>"}. The default is None.                                   | `UNSET`      |
| `proxy`        | `Optional[str]`               | The URL of a proxy. The default is None.                                                                                                 | `UNSET`      |
| `max_retries`  | `int`                         | How many times a request is repeated after a transport error or a status of 408, 429, 500, 502, 503 or 504. The default is 3.            | `UNSET`      |

Raises:

| Type          | Description                                            |
| ------------- | ------------------------------------------------------ |
| `ImportError` | If the 'fastavro' or 'httpx' package is not installed. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(
    self,
    registry_url: str,
    *,
    auth: Optional[Tuple[str, str]] = UNSET,
    verify: Union[bool, str, ssl.SSLContext] = UNSET,
    cert: Optional[
        Union[
            str,
            Tuple[str, Optional[str]],
            Tuple[str, Optional[str], Optional[str]],
        ]
    ] = UNSET,
    timeout: Optional[float] = UNSET,
    headers: Optional[Mapping[str, str]] = UNSET,
    proxy: Optional[str] = UNSET,
    max_retries: int = UNSET,
):
    """
    Initializes the Confluent Avro deserializer. It makes no request to the registry.

    `UNSET` as a default, in the signature or the Default column of the API page,
    means the argument was not given; each argument's description says its real
    default.

    Args:
        registry_url (str): The base URL of the Confluent Schema Registry.
            Several URLs separated by commas are tried in order.
        auth (Optional[Tuple[str, str]]): A username and password for basic
            authentication, for example `("user", "pass")`. The default is None.
        verify (bool | str | ssl.SSLContext): How to check the registry's
            certificate: True (the default), False, the path of a CA file or
            directory, or an SSL context.
        cert (str | tuple | None): A client certificate file, or a tuple of the
            certificate file, the key file and the key password. The default is None.
        timeout (Optional[float]): Seconds each step of a request to the registry
            (connecting, sending, reading) may take. The default is 10.0. None
            waits without a limit.
        headers (Optional[Mapping[str, str]]): Headers sent with every request,
            for example `{"Authorization": "Bearer <token>"}`. The default is None.
        proxy (Optional[str]): The URL of a proxy. The default is None.
        max_retries (int): How many times a request is repeated after a
            transport error or a status of 408, 429, 500, 502, 503 or 504. The
            default is 3.

    Raises:
        ImportError: If the 'fastavro' or 'httpx' package is not installed.
    """
    try:
        import fastavro  # noqa: F401
        import httpx  # noqa: F401

        from desflow.connectors._schema_registry import avro
        from desflow.connectors._schema_registry.confluent_client import (
            ConfluentRegistryClient,
            given_arguments,
        )
        from desflow.connectors._schema_registry.rules import refuse_rules
        from desflow.connectors._schema_registry.wire import unframe_confluent
    except ImportError:
        raise ImportError(
            "The 'fastavro' and 'httpx' packages are required to use "
            "ConfluentAvroDeserializer. "
            "Install them using: pip install desflow[confluent]"
        )

    options = given_arguments(
        auth=auth,
        verify=verify,
        cert=cert,
        timeout=timeout,
        headers=headers,
        proxy=proxy,
        max_retries=max_retries,
    )
    self._client = ConfluentRegistryClient(registry_url, **options)
    self._avro = avro
    self._refuse_rules = refuse_rules
    self._unframe = unframe_confluent
    # Parsed writer schemas by schema id or GUID. The client caches the
    # registry's responses; the parsed schema belongs to the Avro codec.
    self._parsed: Dict[Any, Any] = {}
    self._lock = threading.Lock()
```

#### Methods:

##### `deserialize(topic, payload)`

Deserializes an Avro-encoded byte string.

The first message with a schema id makes a request to the registry for the writer schema.

Parameters:

| Name      | Type    | Description                                               | Default    |
| --------- | ------- | --------------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed.      | *required* |
| `payload` | `bytes` | The Avro-encoded binary payload containing the schema ID. | *required* |

Returns:

| Name  | Type  | Description                                   |
| ----- | ----- | --------------------------------------------- |
| `Any` | `Any` | The deserialized data as a Python dictionary. |

Raises:

| Type                  | Description                                                                                                    |
| --------------------- | -------------------------------------------------------------------------------------------------------------- |
| `SerializationError`  | If the payload has no valid prefix, the writer schema is not Avro, or the schema has Data Contract read rules. |
| `SchemaRegistryError` | If the registry has no schema for the id.                                                                      |
| `TransportError`      | If no registry URL could be reached.                                                                           |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes an Avro-encoded byte string.

    The first message with a schema id makes a request to the registry for the
    writer schema.

    Args:
        topic (str): The Kafka topic from which the message was consumed.
        payload (bytes): The Avro-encoded binary payload containing the schema ID.

    Returns:
        Any: The deserialized data as a Python dictionary.

    Raises:
        SerializationError: If the payload has no valid prefix, the writer
            schema is not Avro, or the schema has Data Contract read rules.
        SchemaRegistryError: If the registry has no schema for the id.
        httpx.TransportError: If no registry URL could be reached.
    """
    schema_id, data = self._unframe(payload)
    return self._avro.decode(self._writer_schema(schema_id), data)
```

### `desflow.connectors.ingress.kafka.GlueAvroDeserializer(registry_name, **boto3_kwargs)`

Lazy-loaded deserializer for AWS Glue Schema Registry.

This class converts Avro-encoded byte strings back into Python dictionaries, integrating directly with AWS Glue Schema Registry. It resolves schemas dynamically based on the metadata embedded in the AWS Glue message payload. A schema version of the JSON data format is read as JSON and validated against its schema.

Initializes the AWS Glue Avro deserializer.

The constructor makes no call to Glue. The first message with a schema version id fetches that version.

Parameters:

| Name             | Type  | Description                                                                                                     | Default    |
| ---------------- | ----- | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_name`  | `str` | The name of the AWS Glue Schema Registry.                                                                       | *required* |
| `**boto3_kwargs` | `Any` | Additional arguments passed directly to the boto3.client initialization (e.g., region_name, aws_access_key_id). | `{}`       |

Raises:

| Type          | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| `ImportError` | If the 'boto3', 'fastavro' or 'fastjsonschema' package is not installed. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(self, registry_name: str, **boto3_kwargs: Any):
    """
    Initializes the AWS Glue Avro deserializer.

    The constructor makes no call to Glue. The first message with a schema version
    id fetches that version.

    Args:
        registry_name (str): The name of the AWS Glue Schema Registry.
        **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
            initialization (e.g., `region_name`, `aws_access_key_id`).

    Raises:
        ImportError: If the 'boto3', 'fastavro' or 'fastjsonschema' package is not
            installed.
    """
    try:
        import boto3  # noqa: F401
        import fastavro  # noqa: F401
        import fastjsonschema  # noqa: F401

        from desflow.connectors._schema_registry import avro, glue_json
        from desflow.connectors._schema_registry.glue_client import (
            GlueRegistryClient,
            no_references,
        )
        from desflow.connectors._schema_registry.wire import unframe_glue
    except ImportError:
        raise ImportError(
            "The 'boto3', 'fastavro' and 'fastjsonschema' packages are required. "
            "Install them using: pip install desflow[glue]"
        )

    self._client = GlueRegistryClient(registry_name, boto3_kwargs)
    self._avro = avro
    self._glue_json = glue_json
    self._unframe = unframe_glue
    self._no_references = no_references
    # A reader for each schema version id, built from the version once.
    self._readers: Dict[Any, Callable[[bytes], Any]] = {}
    self._lock = threading.Lock()
```

#### Methods:

##### `deserialize(topic, payload)`

Deserializes an Avro or JSON byte string using AWS Glue.

Parameters:

| Name      | Type    | Description                                          | Default    |
| --------- | ------- | ---------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed. | *required* |
| `payload` | `bytes` | The binary payload with the Glue prefix.             | *required* |

Returns:

| Name  | Type  | Description                                       |
| ----- | ----- | ------------------------------------------------- |
| `Any` | `Any` | The record, a dict for a record or object schema. |

Raises:

| Type                  | Description                                                                                                                                                                                                                      |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SerializationError`  | If the payload has no valid Glue prefix, the schema version is of a data format other than AVRO or JSON, the JSON Schema cannot be compiled, or the data is not JSON, does not match its schema or cannot be checked against it. |
| `SchemaRegistryError` | If Glue has no available schema version for the id.                                                                                                                                                                              |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes an Avro or JSON byte string using AWS Glue.

    Args:
        topic (str): The Kafka topic from which the message was consumed.
        payload (bytes): The binary payload with the Glue prefix.

    Returns:
        Any: The record, a dict for a record or object schema.

    Raises:
        SerializationError: If the payload has no valid Glue prefix, the schema
            version is of a data format other than AVRO or JSON, the JSON Schema
            cannot be compiled, or the data is not JSON, does not match its
            schema or cannot be checked against it.
        SchemaRegistryError: If Glue has no available schema version for the id.
    """
    version_id, data = self._unframe(payload)
    return self._reader(version_id)(data)
```

### `desflow.connectors.ingress.kafka.ConfluentJsonSchemaDeserializer(registry_url, *, auth=UNSET, verify=UNSET, cert=UNSET, timeout=UNSET, headers=UNSET, proxy=UNSET, max_retries=UNSET)`

Lazy-loaded deserializer for Confluent Schema Registry using jsonschema and httpx.

This class converts JSON in the Confluent wire format back into Python dictionaries. It fetches the writer schema from the registry using the schema id (or GUID) in the message's prefix, once for each id, and checks each record against it.

Initializes the Confluent JSON Schema deserializer. It makes no request to the registry.

`UNSET` as a default, in the signature or the Default column of the API page, means the argument was not given; each argument's description says its real default.

Parameters:

| Name           | Type                          | Description                                                                                                                              | Default      |
| -------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `registry_url` | `str`                         | The base URL of the Confluent Schema Registry. Several URLs separated by commas are tried in order.                                      | *required*   |
| `auth`         | `Optional[Tuple[str, str]]`   | A username and password for basic authentication, for example ("user", "pass"). The default is None.                                     | `UNSET`      |
| `verify`       | \`bool                        | str                                                                                                                                      | SSLContext\` |
| `cert`         | \`str                         | tuple                                                                                                                                    | None\`       |
| `timeout`      | `Optional[float]`             | Seconds each step of a request to the registry (connecting, sending, reading) may take. The default is 10.0. None waits without a limit. | `UNSET`      |
| `headers`      | `Optional[Mapping[str, str]]` | Headers sent with every request, for example {"Authorization": "Bearer <token>"}. The default is None.                                   | `UNSET`      |
| `proxy`        | `Optional[str]`               | The URL of a proxy. The default is None.                                                                                                 | `UNSET`      |
| `max_retries`  | `int`                         | How many times a request is repeated after a transport error or a status of 408, 429, 500, 502, 503 or 504. The default is 3.            | `UNSET`      |

Raises:

| Type          | Description                                              |
| ------------- | -------------------------------------------------------- |
| `ImportError` | If the 'jsonschema' or 'httpx' package is not installed. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(
    self,
    registry_url: str,
    *,
    auth: Optional[Tuple[str, str]] = UNSET,
    verify: Union[bool, str, ssl.SSLContext] = UNSET,
    cert: Optional[
        Union[
            str,
            Tuple[str, Optional[str]],
            Tuple[str, Optional[str], Optional[str]],
        ]
    ] = UNSET,
    timeout: Optional[float] = UNSET,
    headers: Optional[Mapping[str, str]] = UNSET,
    proxy: Optional[str] = UNSET,
    max_retries: int = UNSET,
):
    """
    Initializes the Confluent JSON Schema deserializer. It makes no request to the
    registry.

    `UNSET` as a default, in the signature or the Default column of the API page,
    means the argument was not given; each argument's description says its real
    default.

    Args:
        registry_url (str): The base URL of the Confluent Schema Registry.
            Several URLs separated by commas are tried in order.
        auth (Optional[Tuple[str, str]]): A username and password for basic
            authentication, for example `("user", "pass")`. The default is None.
        verify (bool | str | ssl.SSLContext): How to check the registry's
            certificate: True (the default), False, the path of a CA file or
            directory, or an SSL context.
        cert (str | tuple | None): A client certificate file, or a tuple of the
            certificate file, the key file and the key password. The default is None.
        timeout (Optional[float]): Seconds each step of a request to the registry
            (connecting, sending, reading) may take. The default is 10.0. None
            waits without a limit.
        headers (Optional[Mapping[str, str]]): Headers sent with every request,
            for example `{"Authorization": "Bearer <token>"}`. The default is None.
        proxy (Optional[str]): The URL of a proxy. The default is None.
        max_retries (int): How many times a request is repeated after a
            transport error or a status of 408, 429, 500, 502, 503 or 504. The
            default is 3.

    Raises:
        ImportError: If the 'jsonschema' or 'httpx' package is not installed.
    """
    try:
        import httpx  # noqa: F401
        import jsonschema  # noqa: F401

        from desflow.connectors._schema_registry import json_schema
        from desflow.connectors._schema_registry.confluent_client import (
            ConfluentRegistryClient,
            given_arguments,
        )
        from desflow.connectors._schema_registry.rules import refuse_rules
        from desflow.connectors._schema_registry.wire import unframe_confluent
    except ImportError:
        raise ImportError(
            "The 'jsonschema' and 'httpx' packages are required to use "
            "ConfluentJsonSchemaDeserializer. "
            "Install them using: pip install desflow[confluent]"
        )

    options = given_arguments(
        auth=auth,
        verify=verify,
        cert=cert,
        timeout=timeout,
        headers=headers,
        proxy=proxy,
        max_retries=max_retries,
    )
    self._client = ConfluentRegistryClient(registry_url, **options)
    self._json_schema = json_schema
    self._refuse_rules = refuse_rules
    self._unframe = unframe_confluent
    # A validator for each schema id or GUID, built from the schema once.
    self._validators: Dict[Any, Any] = {}
    self._lock = threading.Lock()
```

#### Methods:

##### `deserialize(topic, payload)`

Deserializes a JSON byte string with the Confluent prefix.

The first message with a schema id makes a request to the registry for the writer schema.

Parameters:

| Name      | Type    | Description                                          | Default    |
| --------- | ------- | ---------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed. | *required* |
| `payload` | `bytes` | The JSON payload with the Confluent prefix.          | *required* |

Returns:

| Name  | Type  | Description                              |
| ----- | ----- | ---------------------------------------- |
| `Any` | `Any` | The record, a dict for an object schema. |

Raises:

| Type                  | Description                                                                                                                                                     |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SerializationError`  | If the payload has no valid prefix, the writer schema is not JSON Schema or has Data Contract read rules, or the data is not JSON or does not match the schema. |
| `SchemaRegistryError` | If the registry has no schema for the id.                                                                                                                       |
| `TransportError`      | If no registry URL could be reached.                                                                                                                            |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes a JSON byte string with the Confluent prefix.

    The first message with a schema id makes a request to the registry for the
    writer schema.

    Args:
        topic (str): The Kafka topic from which the message was consumed.
        payload (bytes): The JSON payload with the Confluent prefix.

    Returns:
        Any: The record, a dict for an object schema.

    Raises:
        SerializationError: If the payload has no valid prefix, the writer schema
            is not JSON Schema or has Data Contract read rules, or the data is
            not JSON or does not match the schema.
        SchemaRegistryError: If the registry has no schema for the id.
        httpx.TransportError: If no registry URL could be reached.
    """
    schema_id, data = self._unframe(payload)
    return self._json_schema.decode(self._validator(schema_id), data)
```

### `desflow.connectors.ingress.kafka.ConfluentProtobufDeserializer(registry_url, message_type, *, auth=UNSET, verify=UNSET, cert=UNSET, timeout=UNSET, headers=UNSET, proxy=UNSET, max_retries=UNSET)`

Lazy-loaded deserializer for Confluent Schema Registry using protobuf and httpx.

This class converts Protobuf messages in the Confluent wire format back into Python dictionaries. It reads each message as the class it is given, and skips the message index. It fetches the writer schema only to check its type and rules, once for each id.

Initializes the Confluent Protobuf deserializer. It makes no request to the registry.

`UNSET` as a default, in the signature or the Default column of the API page, means the argument was not given; each argument's description says its real default.

Parameters:

| Name           | Type                          | Description                                                                                                                              | Default      |
| -------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `registry_url` | `str`                         | The base URL of the Confluent Schema Registry. Several URLs separated by commas are tried in order.                                      | *required*   |
| `message_type` | `type[Message]`               | The generated Protobuf message class that every message is read as.                                                                      | *required*   |
| `auth`         | `Optional[Tuple[str, str]]`   | A username and password for basic authentication, for example ("user", "pass"). The default is None.                                     | `UNSET`      |
| `verify`       | \`bool                        | str                                                                                                                                      | SSLContext\` |
| `cert`         | \`str                         | tuple                                                                                                                                    | None\`       |
| `timeout`      | `Optional[float]`             | Seconds each step of a request to the registry (connecting, sending, reading) may take. The default is 10.0. None waits without a limit. | `UNSET`      |
| `headers`      | `Optional[Mapping[str, str]]` | Headers sent with every request, for example {"Authorization": "Bearer <token>"}. The default is None.                                   | `UNSET`      |
| `proxy`        | `Optional[str]`               | The URL of a proxy. The default is None.                                                                                                 | `UNSET`      |
| `max_retries`  | `int`                         | How many times a request is repeated after a transport error or a status of 408, 429, 500, 502, 503 or 504. The default is 3.            | `UNSET`      |

Raises:

| Type          | Description                                            |
| ------------- | ------------------------------------------------------ |
| `ImportError` | If the 'protobuf' or 'httpx' package is not installed. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(
    self,
    registry_url: str,
    message_type: Any,
    *,
    auth: Optional[Tuple[str, str]] = UNSET,
    verify: Union[bool, str, ssl.SSLContext] = UNSET,
    cert: Optional[
        Union[
            str,
            Tuple[str, Optional[str]],
            Tuple[str, Optional[str], Optional[str]],
        ]
    ] = UNSET,
    timeout: Optional[float] = UNSET,
    headers: Optional[Mapping[str, str]] = UNSET,
    proxy: Optional[str] = UNSET,
    max_retries: int = UNSET,
):
    """
    Initializes the Confluent Protobuf deserializer. It makes no request to the
    registry.

    `UNSET` as a default, in the signature or the Default column of the API page,
    means the argument was not given; each argument's description says its real
    default.

    Args:
        registry_url (str): The base URL of the Confluent Schema Registry.
            Several URLs separated by commas are tried in order.
        message_type (type[Message]): The generated Protobuf message class that
            every message is read as.
        auth (Optional[Tuple[str, str]]): A username and password for basic
            authentication, for example `("user", "pass")`. The default is None.
        verify (bool | str | ssl.SSLContext): How to check the registry's
            certificate: True (the default), False, the path of a CA file or
            directory, or an SSL context.
        cert (str | tuple | None): A client certificate file, or a tuple of the
            certificate file, the key file and the key password. The default is None.
        timeout (Optional[float]): Seconds each step of a request to the registry
            (connecting, sending, reading) may take. The default is 10.0. None
            waits without a limit.
        headers (Optional[Mapping[str, str]]): Headers sent with every request,
            for example `{"Authorization": "Bearer <token>"}`. The default is None.
        proxy (Optional[str]): The URL of a proxy. The default is None.
        max_retries (int): How many times a request is repeated after a
            transport error or a status of 408, 429, 500, 502, 503 or 504. The
            default is 3.

    Raises:
        ImportError: If the 'protobuf' or 'httpx' package is not installed.
    """
    try:
        import google.protobuf  # noqa: F401
        import httpx  # noqa: F401

        from desflow.connectors._schema_registry import protobuf
        from desflow.connectors._schema_registry.confluent_client import (
            ConfluentRegistryClient,
            given_arguments,
        )
        from desflow.connectors._schema_registry.rules import refuse_rules
        from desflow.connectors._schema_registry.wire import unframe_confluent
    except ImportError:
        raise ImportError(
            "The 'protobuf' and 'httpx' packages are required to use "
            "ConfluentProtobufDeserializer. "
            "Install them using: pip install desflow[confluent]"
        )

    options = given_arguments(
        auth=auth,
        verify=verify,
        cert=cert,
        timeout=timeout,
        headers=headers,
        proxy=proxy,
        max_retries=max_retries,
    )
    self._client = ConfluentRegistryClient(registry_url, **options)
    self._protobuf = protobuf
    self._refuse_rules = refuse_rules
    self._unframe = unframe_confluent
    self._message_type = message_type
    # The schema ids and GUIDs whose type and rules were checked.
    self._checked: set = set()
    self._lock = threading.Lock()
```

#### Methods:

##### `deserialize(topic, payload)`

Deserializes a Protobuf byte string with the Confluent prefix.

The first message with a schema id makes a request to the registry for the writer schema.

Parameters:

| Name      | Type    | Description                                                           | Default    |
| --------- | ------- | --------------------------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed.                  | *required* |
| `payload` | `bytes` | The Protobuf payload with the Confluent prefix and the message index. | *required* |

Returns:

| Name  | Type  | Description                                                                                   |
| ----- | ----- | --------------------------------------------------------------------------------------------- |
| `Any` | `Any` | The record as a dict, with the field names of the schema. A 64 bit integer field is a string. |

Raises:

| Type                  | Description                                                                                                                                                |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SerializationError`  | If the payload has no valid prefix or index, the writer schema is not Protobuf or has Data Contract read rules, or the data is not a message of the class. |
| `SchemaRegistryError` | If the registry has no schema for the id.                                                                                                                  |
| `TransportError`      | If no registry URL could be reached.                                                                                                                       |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes a Protobuf byte string with the Confluent prefix.

    The first message with a schema id makes a request to the registry for the
    writer schema.

    Args:
        topic (str): The Kafka topic from which the message was consumed.
        payload (bytes): The Protobuf payload with the Confluent prefix and the
            message index.

    Returns:
        Any: The record as a dict, with the field names of the schema. A 64 bit
            integer field is a string.

    Raises:
        SerializationError: If the payload has no valid prefix or index, the
            writer schema is not Protobuf or has Data Contract read rules, or the
            data is not a message of the class.
        SchemaRegistryError: If the registry has no schema for the id.
        httpx.TransportError: If no registry URL could be reached.
    """
    schema_id, data = self._unframe(payload)
    self._check(schema_id)
    _, message = self._protobuf.read_confluent_indexes(data)
    return self._protobuf.decode(self._message_type, message)
```

### `desflow.connectors.ingress.kafka.GlueJsonSchemaDeserializer(registry_name, **boto3_kwargs)`

Lazy-loaded deserializer for AWS Glue Schema Registry using boto3 and fastjsonschema.

This class converts JSON with the AWS Glue prefix back into Python dictionaries. It fetches the schema version named in the prefix, once for each id, and checks each record against it as aws-glue-schema-registry 1.1.3 did, which fills in a missing property that has a `default`.

Initializes the AWS Glue JSON Schema deserializer.

The constructor makes no call to Glue. The first message with a schema version id fetches that version.

Parameters:

| Name             | Type  | Description                                                                                                     | Default    |
| ---------------- | ----- | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_name`  | `str` | The name of the AWS Glue Schema Registry.                                                                       | *required* |
| `**boto3_kwargs` | `Any` | Additional arguments passed directly to the boto3.client initialization (e.g., region_name, aws_access_key_id). | `{}`       |

Raises:

| Type          | Description                                                  |
| ------------- | ------------------------------------------------------------ |
| `ImportError` | If the 'boto3' or 'fastjsonschema' package is not installed. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(self, registry_name: str, **boto3_kwargs: Any):
    """
    Initializes the AWS Glue JSON Schema deserializer.

    The constructor makes no call to Glue. The first message with a schema version
    id fetches that version.

    Args:
        registry_name (str): The name of the AWS Glue Schema Registry.
        **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
            initialization (e.g., `region_name`, `aws_access_key_id`).

    Raises:
        ImportError: If the 'boto3' or 'fastjsonschema' package is not installed.
    """
    try:
        import boto3  # noqa: F401
        import fastjsonschema  # noqa: F401

        from desflow.connectors._schema_registry import glue_json
        from desflow.connectors._schema_registry.glue_client import (
            GlueRegistryClient,
        )
        from desflow.connectors._schema_registry.wire import unframe_glue
    except ImportError:
        raise ImportError(
            "The 'boto3' and 'fastjsonschema' packages are required. "
            "Install them using: pip install desflow[glue]"
        )

    self._client = GlueRegistryClient(registry_name, boto3_kwargs)
    self._glue_json = glue_json
    self._unframe = unframe_glue
    # A compiled schema for each schema version id, built from the version once.
    self._validators: Dict[Any, Any] = {}
    self._lock = threading.Lock()
```

#### Methods:

##### `deserialize(topic, payload)`

Deserializes a JSON byte string using AWS Glue.

Parameters:

| Name      | Type    | Description                                          | Default    |
| --------- | ------- | ---------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed. | *required* |
| `payload` | `bytes` | The JSON payload with the Glue prefix.               | *required* |

Returns:

| Name  | Type  | Description                              |
| ----- | ----- | ---------------------------------------- |
| `Any` | `Any` | The record, a dict for an object schema. |

Raises:

| Type                  | Description                                                                                                                                                                                                              |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SerializationError`  | If the payload has no valid Glue prefix, the schema version is of a data format other than JSON, the JSON Schema cannot be compiled, or the data is not JSON, does not match the schema or cannot be checked against it. |
| `SchemaRegistryError` | If Glue has no available schema version for the id.                                                                                                                                                                      |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes a JSON byte string using AWS Glue.

    Args:
        topic (str): The Kafka topic from which the message was consumed.
        payload (bytes): The JSON payload with the Glue prefix.

    Returns:
        Any: The record, a dict for an object schema.

    Raises:
        SerializationError: If the payload has no valid Glue prefix, the schema
            version is of a data format other than JSON, the JSON Schema cannot
            be compiled, or the data is not JSON, does not match the schema or
            cannot be checked against it.
        SchemaRegistryError: If Glue has no available schema version for the id.
    """
    version_id, data = self._unframe(payload)
    return self._glue_json.decode(self._validator(version_id), data)
```

### `desflow.connectors.ingress.kafka.GlueProtobufDeserializer(registry_name, message_type, **boto3_kwargs)`

Lazy-loaded deserializer for AWS Glue Schema Registry using boto3 and protobuf.

This class converts Protobuf messages with the AWS Glue prefix back into Python dictionaries. It reads each message as the class it is given, and skips the message index. It fetches the schema version only to check its data format, once for each id.

Initializes the AWS Glue Protobuf deserializer.

The constructor makes no call to Glue. The first message with a schema version id fetches that version.

Parameters:

| Name             | Type            | Description                                                                                                     | Default    |
| ---------------- | --------------- | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_name`  | `str`           | The name of the AWS Glue Schema Registry.                                                                       | *required* |
| `message_type`   | `type[Message]` | The generated Protobuf message class that every message is read as.                                             | *required* |
| `**boto3_kwargs` | `Any`           | Additional arguments passed directly to the boto3.client initialization (e.g., region_name, aws_access_key_id). | `{}`       |

Raises:

| Type          | Description                                            |
| ------------- | ------------------------------------------------------ |
| `ImportError` | If the 'boto3' or 'protobuf' package is not installed. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(self, registry_name: str, message_type: Any, **boto3_kwargs: Any):
    """
    Initializes the AWS Glue Protobuf deserializer.

    The constructor makes no call to Glue. The first message with a schema version
    id fetches that version.

    Args:
        registry_name (str): The name of the AWS Glue Schema Registry.
        message_type (type[Message]): The generated Protobuf message class that
            every message is read as.
        **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
            initialization (e.g., `region_name`, `aws_access_key_id`).

    Raises:
        ImportError: If the 'boto3' or 'protobuf' package is not installed.
    """
    try:
        import boto3  # noqa: F401
        import google.protobuf  # noqa: F401

        from desflow.connectors._schema_registry import protobuf
        from desflow.connectors._schema_registry.glue_client import (
            GlueRegistryClient,
        )
        from desflow.connectors._schema_registry.wire import unframe_glue
    except ImportError:
        raise ImportError(
            "The 'boto3' and 'protobuf' packages are required. "
            "Install them using: pip install desflow[glue]"
        )

    self._client = GlueRegistryClient(registry_name, boto3_kwargs)
    self._protobuf = protobuf
    self._unframe = unframe_glue
    self._message_type = message_type
    # The schema version ids whose data format was checked.
    self._checked: set = set()
    self._lock = threading.Lock()
```

#### Methods:

##### `deserialize(topic, payload)`

Deserializes a Protobuf byte string using AWS Glue.

Parameters:

| Name      | Type    | Description                                                      | Default    |
| --------- | ------- | ---------------------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed.             | *required* |
| `payload` | `bytes` | The Protobuf payload with the Glue prefix and the message index. | *required* |

Returns:

| Name  | Type  | Description                                                                                   |
| ----- | ----- | --------------------------------------------------------------------------------------------- |
| `Any` | `Any` | The record as a dict, with the field names of the schema. A 64 bit integer field is a string. |

Raises:

| Type                  | Description                                                                                                                                              |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SerializationError`  | If the payload has no valid Glue prefix or index, the schema version is of a data format other than PROTOBUF, or the data is not a message of the class. |
| `SchemaRegistryError` | If Glue has no available schema version for the id.                                                                                                      |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes a Protobuf byte string using AWS Glue.

    Args:
        topic (str): The Kafka topic from which the message was consumed.
        payload (bytes): The Protobuf payload with the Glue prefix and the message
            index.

    Returns:
        Any: The record as a dict, with the field names of the schema. A 64 bit
            integer field is a string.

    Raises:
        SerializationError: If the payload has no valid Glue prefix or index, the
            schema version is of a data format other than PROTOBUF, or the data
            is not a message of the class.
        SchemaRegistryError: If Glue has no available schema version for the id.
    """
    version_id, data = self._unframe(payload)
    self._check(version_id)
    _, message = self._protobuf.read_glue_index(data)
    return self._protobuf.decode(self._message_type, message)
```

## Connectors (Egress)

### `desflow.connectors.egress.base.BaseEgress`

Base class for all egress providers in the simulation.

Egress providers act as asynchronous bridges that consume processed simulation data (telemetry and events) from a thread-safe internal queue and transmit it to external destinations such as Kafka, databases, or the console.

Attributes:

| Name       | Type   | Description                                                                                                                                                                  |
| ---------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `envelope` | `bool` | False when the provider was added with envelope=False: it writes each event's value alone, and the environment sends it no telemetry record and no queued or started record. |

#### Methods:

##### `run(egress_queue)`

Listens to the internal queue and pushes data to an external sink.

This method should contain an asynchronous loop that polls the provided queue and handles the networking/I/O logic specific to the destination system.

Parameters:

| Name           | Type    | Description                                                            | Default    |
| -------------- | ------- | ---------------------------------------------------------------------- | ---------- |
| `egress_queue` | `Queue` | A thread-safe queue containing batches of dictionaries to be exported. | *required* |

Raises:

| Type                  | Description                                    |
| --------------------- | ---------------------------------------------- |
| `NotImplementedError` | If the subclass does not override this method. |

Source code in `src/desflow/connectors/egress/base.py`

```python
async def run(self, egress_queue: queue.Queue) -> None:
    """
    Listens to the internal queue and pushes data to an external sink.

    This method should contain an asynchronous loop that polls the
    provided queue and handles the networking/I/O logic specific
    to the destination system.

    Args:
        egress_queue: A thread-safe queue containing batches of
            dictionaries to be exported.

    Raises:
        NotImplementedError: If the subclass does not override this method.
    """
    raise NotImplementedError("Subclasses must implement the run method.")
```

### `desflow.connectors.egress.kafka.KafkaEgress(bootstrap_servers, telemetry_topic='sim-telemetry', event_topic='sim-events', topic_router=None, topic_serializers=None, default_serializer=None, include_stream_type=False, topic_policy=None, topics=None, serializer=None, ssl=None, sasl_oauth_token_provider=None, **kwargs)`

Bases: `BaseEgress`

High-throughput Kafka producer for simulation telemetry and events.

This connector utilizes `aiokafka` for asynchronous I/O and `orjson` for fast serialization. It implements a resilient connection loop with exponential backoff.

By default, data is routed to the `telemetry_topic` or `event_topic` based on its `stream_type`. If a `topic_router` callable is provided, topic selection is delegated to that function instead, allowing for advanced multiplexing (e.g., splitting ML vs. UI events).

Without a `topic_router`, the event and telemetry topics are created at start when they do not exist, with one partition and a replication factor of one. With a router the topics are left to the caller, because only the router knows which ones it uses.

Note

By default `stream_type` is treated as routing metadata and is left out of the published message, because the topic already identifies the stream. `ParquetStorageEgress` keeps `stream_type` as a column, so a downstream filter written against Parquet files matches nothing when it is pointed at a Kafka topic. Set `include_stream_type=True` to keep the field in the message and make both paths carry it.

An event whose `value` holds `__topic__` is sent to the topic it names, and the key is removed from the message. This replaces a `topic_router` for the common case. A `topic_router` wins over `__topic__`, and telemetry always goes to `telemetry_topic`. `topic_policy` and `topics` create the topics, `serializer` and `topics.<topic>.serializer` set the serializers from a mapping, `ssl` and `sasl_oauth_token_provider` build the connection settings from a mapping, and an entry added with `envelope=False` sends the event's value alone as the message. An event whose value is null is then sent as a tombstone: a message with the record's key and a null value, which a compacted topic reads as a delete of that key.

Attributes:

| Name                  | Type                                     | Description                                                 |
| --------------------- | ---------------------------------------- | ----------------------------------------------------------- |
| `bootstrap_servers`   | `str`                                    | Comma-separated list of Kafka brokers.                      |
| `telemetry_topic`     | `str`                                    | The default topic name for telemetry data.                  |
| `event_topic`         | `str`                                    | The default topic name for lifecycle events.                |
| `topic_router`        | `Callable`                               | Optional external logic to determine the topic.             |
| `topic_serializers`   | `Optional[Dict[str, MessageSerializer]]` | Optional mapping of topics to specific serializers.         |
| `default_serializer`  | `Optional[MessageSerializer]`            | Fallback serializer if a topic is not in topic_serializers. |
| `include_stream_type` | `bool`                                   | Whether the published message keeps the stream_type field.  |
| `producer_config`     | `dict`                                   | Configuration dictionary passed to AIOKafkaProducer.        |

Examples:

Defining a custom topic router to split machine learning events from standard telemetry:

```python
def custom_topic_router(data: dict) -> str:
    stream_type = data.get("stream_type")
    if stream_type == "telemetry":
        return "sim-telemetry"

    value = data.get("value", {})
    if isinstance(value, dict):
        event_type = value.get("event_type")
        if event_type == "prediction_request":
            return "mill-predictions"
        elif event_type == "ground_truth":
            return "mill-groundtruth"

    return "mill-lifecycle"

# Pass it to the egress connector
egress = KafkaEgress(
    bootstrap_servers="127.0.0.1:9092",
    topic_router=custom_topic_router
)
```

Initializes the KafkaEgress with topic and connection settings.

Parameters:

| Name                        | Type                                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Default           |
| --------------------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- |
| `bootstrap_servers`         | `str`                                       | Comma-separated list of Kafka brokers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | *required*        |
| `telemetry_topic`           | `str`                                       | Destination topic for telemetry stream.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `'sim-telemetry'` |
| `event_topic`               | `str`                                       | Destination topic for event stream.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `'sim-events'`    |
| `topic_router`              | `Optional[Callable[[dict], str]]`           | Optional callable to dynamically route payloads to specific topics.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `None`            |
| `topic_serializers`         | `Optional[Dict[str, MessageSerializer]]`    | Mapping of target topics to their specific MessageSerializer implementations.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | `None`            |
| `default_serializer`        | `Optional[MessageSerializer]`               | The fallback serializer to use if a topic lacks a specific mapping. Defaults to JsonSerializer.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `None`            |
| `include_stream_type`       | `bool`                                      | Keep stream_type in the published message. Defaults to False, which is the wire format every existing deployment already reads. Turn it on when the same consumer code has to work against both a Kafka topic and Parquet files, since the Parquet writer keeps the field. An Avro serializer only carries the field if the registered schema declares it: both ConfluentAvroSerializer and GlueAvroSerializer encode field by field from the schema, so an undeclared stream_type is dropped without an error rather than failing the send. The Protobuf serializers drop it in the same way. The JSON Schema serializers send it, and refuse the record when the schema does not allow other properties. | `False`           |
| `topic_policy`              | `Any`                                       | How the sink treats its topics when the run starts: a mode word (create, drop_and_create or manual) or a mapping of mode, partitions, replication_factor and topic_configs (a mapping of topic setting to value). create, the default, creates a missing topic with one partition, a replication factor of 1 and no settings, and leaves an existing topic as it is. drop_and_create deletes the topic and creates it again, so each run starts empty. manual creates nothing. The policy covers the event topic and the telemetry topic, and any topic named in topics that sets none of its own.                                                                                                         | `None`            |
| `topics`                    | `Optional[Mapping[str, Mapping[str, Any]]]` | Settings per topic, a mapping of topic name to a mapping with the keys of topic_policy and serializer. A topic named here is created by the sink according to its policy, also when events choose it with __topic__ or a topic_router routes to it.                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `None`            |
| `serializer`                | `Any`                                       | The serializer for every topic that topics or topic_serializers gives none, in place of default_serializer: a mapping {type, ...} that build_serializer builds, or a serializer object. The mapping's type is confluent_avro, confluent_json_schema, confluent_protobuf, glue_avro, glue_json_schema or glue_protobuf; the other keys are those of build_serializer.                                                                                                                                                                                                                                                                                                                                       | `None`            |
| `ssl`                       | `Optional[Mapping[str, Any]]`               | The keys cafile, certfile, keyfile and password, passed to aiokafka.helpers.create_ssl_context. The result is the producer's ssl_context; set security_protocol to SSL or SASL_SSL as well.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `None`            |
| `sasl_oauth_token_provider` | `Any`                                       | {type: aws_msk_iam, region: <region>} builds the token provider for Amazon MSK IAM, which needs the package aws-msk-iam-sasl-signer-python; a token provider object is used as it is. Set security_protocol to SASL_SSL and sasl_mechanism to OAUTHBEARER as well.                                                                                                                                                                                                                                                                                                                                                                                                                                         | `None`            |
| `**kwargs`                  | `Any`                                       | Additional overrides for the AIOKafkaProducer configuration.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `{}`              |

Raises:

| Type          | Description                                                                                                               |
| ------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `ValueError`  | If a policy, serializer, ssl or token provider setting is malformed, or serializer and default_serializer are both given. |
| `ImportError` | If sasl_oauth_token_provider asks for aws_msk_iam and aws-msk-iam-sasl-signer-python is not installed.                    |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def __init__(
    self,
    bootstrap_servers: str,
    telemetry_topic: str = "sim-telemetry",
    event_topic: str = "sim-events",
    topic_router: Optional[Callable[[dict], str]] = None,
    topic_serializers: Optional[Dict[str, MessageSerializer]] = None,
    default_serializer: Optional[MessageSerializer] = None,
    include_stream_type: bool = False,
    topic_policy: Any = None,
    topics: Optional[Mapping[str, Mapping[str, Any]]] = None,
    serializer: Any = None,
    ssl: Optional[Mapping[str, Any]] = None,
    sasl_oauth_token_provider: Any = None,
    **kwargs: Any,
):
    """
    Initializes the KafkaEgress with topic and connection settings.

    Args:
        bootstrap_servers: Comma-separated list of Kafka brokers.
        telemetry_topic: Destination topic for telemetry stream.
        event_topic: Destination topic for event stream.
        topic_router: Optional callable to dynamically route payloads to specific topics.
        topic_serializers: Mapping of target topics to their specific `MessageSerializer` implementations.
        default_serializer: The fallback serializer to use if a topic lacks a specific mapping. Defaults to `JsonSerializer`.
        include_stream_type: Keep `stream_type` in the published message. Defaults to
            False, which is the wire format every existing deployment already reads.
            Turn it on when the same consumer code has to work against both a Kafka
            topic and Parquet files, since the Parquet writer keeps the field.
            An Avro serializer only carries the field if the registered schema
            declares it: both `ConfluentAvroSerializer` and `GlueAvroSerializer`
            encode field by field from the schema, so an undeclared `stream_type`
            is dropped without an error rather than failing the send.
            The Protobuf serializers drop it in the same way. The JSON Schema
            serializers send it, and refuse the record when the schema does not
            allow other properties.
        topic_policy: How the sink treats its topics when the run starts: a mode word
            (`create`, `drop_and_create` or `manual`) or a mapping of `mode`,
            `partitions`, `replication_factor` and `topic_configs` (a mapping of
            topic setting to value). `create`, the default, creates a missing
            topic with one partition, a replication factor of 1 and no settings,
            and leaves an existing topic as it is. `drop_and_create` deletes the
            topic and creates it again, so each run starts empty. `manual`
            creates nothing. The policy covers the event topic and the telemetry
            topic, and any topic named in `topics` that sets none of its own.
        topics: Settings per topic, a mapping of topic name to a mapping with the
            keys of `topic_policy` and `serializer`. A topic named here is
            created by the sink according to its policy, also when events choose
            it with `__topic__` or a `topic_router` routes to it.
        serializer: The serializer for every topic that `topics` or
            `topic_serializers` gives none, in place of `default_serializer`: a
            mapping `{type, ...}` that `build_serializer` builds, or a serializer
            object. The mapping's `type` is `confluent_avro`,
            `confluent_json_schema`, `confluent_protobuf`, `glue_avro`,
            `glue_json_schema` or `glue_protobuf`; the other keys are those of
            `build_serializer`.
        ssl: The keys `cafile`, `certfile`, `keyfile` and `password`, passed to
            `aiokafka.helpers.create_ssl_context`. The result is the producer's
            `ssl_context`; set `security_protocol` to `SSL` or `SASL_SSL` as well.
        sasl_oauth_token_provider: `{type: aws_msk_iam, region: <region>}` builds
            the token provider for Amazon MSK IAM, which needs the package
            `aws-msk-iam-sasl-signer-python`; a token provider object is used as
            it is. Set `security_protocol` to `SASL_SSL` and `sasl_mechanism` to
            `OAUTHBEARER` as well.
        **kwargs: Additional overrides for the AIOKafkaProducer configuration.

    Raises:
        ValueError: If a policy, serializer, `ssl` or token provider setting is
            malformed, or `serializer` and `default_serializer` are both given.
        ImportError: If `sasl_oauth_token_provider` asks for `aws_msk_iam` and
            `aws-msk-iam-sasl-signer-python` is not installed.
    """
    self.telemetry_topic = telemetry_topic
    self.event_topic = event_topic
    self.topic_router = topic_router
    self.include_stream_type = include_stream_type

    self.topic_policy = read_topic_policy(topic_policy)
    self.topics: Dict[str, Dict[str, Any]] = {}
    # Initialize serializers in a backward-compatible way
    self.topic_serializers = dict(topic_serializers or {})
    if serializer is not None and default_serializer is not None:
        raise ValueError("give serializer or default_serializer, not both")
    if isinstance(serializer, Mapping):
        serializer = build_serializer(serializer)
    self.default_serializer = serializer or default_serializer or JsonSerializer()
    for name, settings in (topics or {}).items():
        if settings is not None and not isinstance(settings, Mapping):
            raise ValueError(
                f"topics.{name} must be a mapping of partitions, "
                f"replication_factor, topic_configs and serializer, got "
                f"{settings!r}"
            )
        settings = dict(settings or {})
        per_topic = settings.pop("serializer", None)
        read_topic_policy(settings, f"topics.{name}")
        self.topics[str(name)] = settings
        if per_topic is not None:
            if name in self.topic_serializers:
                raise ValueError(
                    f"topics.{name}.serializer and topic_serializers both give "
                    f"a serializer for '{name}'"
                )
            self.topic_serializers[str(name)] = (
                build_serializer(per_topic)
                if isinstance(per_topic, Mapping)
                else per_topic
            )

    kwargs = with_security(kwargs, ssl, sasl_oauth_token_provider)

    # High-performance defaults for 100k/sec
    self.producer_config = {
        "bootstrap_servers": bootstrap_servers,
        "linger_ms": 10,  # Batch messages for 10ms before sending
        "compression_type": "lz4",  # Fast compression for high volume
        "max_batch_size": 131072,  # 128KB batch size
        **kwargs,
    }
```

#### Methods:

##### `run(egress_queue)`

The main execution loop that consumes the egress queue and publishes to Kafka.

This method maintains a persistent connection to Kafka. If the connection is lost, it implements an exponential backoff retry strategy. It polls the internal `egress_queue` for batches of data, determines the target topic based on the 'stream_type', and performs asynchronous sends using the configured serializers. A record that its serializer refuses, with a `TypeError` or `ValueError` such as a `SerializationError`, is skipped with a warning that names its key and the reason, and the rest of the batch is sent.

Parameters:

| Name           | Type    | Description                                                                                           | Default    |
| -------------- | ------- | ----------------------------------------------------------------------------------------------------- | ---------- |
| `egress_queue` | `Queue` | A thread-safe queue containing lists of dictionaries or Pydantic models generated by the EgressMixIn. | *required* |

Note

The loop exits gracefully upon receiving an `asyncio.CancelledError`, ensuring the Kafka producer is stopped correctly.

Source code in `src/desflow/connectors/egress/kafka.py`

```python
async def run(self, egress_queue: queue.Queue) -> None:
    """
    The main execution loop that consumes the egress queue and publishes to Kafka.

    This method maintains a persistent connection to Kafka. If the connection
    is lost, it implements an exponential backoff retry strategy. It polls
    the internal `egress_queue` for batches of data, determines the target
    topic based on the 'stream_type', and performs asynchronous sends using
    the configured serializers. A record that its serializer refuses, with a
    `TypeError` or `ValueError` such as a `SerializationError`, is skipped with a
    warning that names its key and the reason, and the rest of the batch is sent.

    Args:
        egress_queue: A thread-safe queue containing lists of dictionaries
            or Pydantic models generated by the EgressMixIn.

    Note:
        The loop exits gracefully upon receiving an `asyncio.CancelledError`,
        ensuring the Kafka producer is stopped correctly.
    """
    if self.topic_router is None or self.topics:
        await asyncio.to_thread(self._create_topics)

    backoff = 1.0
    max_backoff = 60.0

    while True:
        try:
            producer = AIOKafkaProducer(**self.producer_config)
            try:
                # Started inside the try, so a failed start also closes the
                # producer: aiokafka leaves it open otherwise.
                await producer.start()
                logger.info("Kafka Egress producer connected successfully.")
                backoff = 1.0  # Reset backoff on successful connection

                while True:
                    try:
                        # 'batch' is a list of dictionaries from the EgressMixIn
                        batch = egress_queue.get_nowait()
                        with self._writing():
                            # Kafka routes by __topic__, and writes no other
                            # reserved key
                            batch = drop_reserved_keys(
                                batch, self, keep=("__topic__",)
                            )

                            for data in batch:
                                message = self._message(data)
                                if message is None:
                                    continue
                                topic, key, payload_bytes = message
                                await producer.send(
                                    topic, value=payload_bytes, key=key
                                )

                    except queue.Empty:
                        # Yield to loop if queue is empty
                        await asyncio.sleep(0.001)
            finally:
                await producer.stop()

        except asyncio.CancelledError:
            logger.info("Kafka Egress shut down requested. Exiting loop.")
            break
        except Exception as e:
            logger.error(
                f"Kafka Egress connection failed: {e}. Retrying in {backoff} seconds..."
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
```

### `desflow.connectors.egress.local.ConsoleEgress(format='log')`

Bases: `BaseEgress`

Local egress provider that outputs simulation data to the console.

This connector serves as a debugging and local development tool. The `log` format logs each batch, distinguishing telemetry metrics from discrete events by prefixes. The `json` format writes each record to stdout as one JSON line, with its `stream_type`, for a pipe such as `jq`.

Chooses how records are written.

Parameters:

| Name     | Type                     | Description                                                                                                                                                        | Default |
| -------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| `format` | `Literal['log', 'json']` | log logs each batch at INFO level, one prefixed line per record. json writes one JSON line per record to stdout at any log level, because it is output, not a log. | `'log'` |

Raises:

| Type         | Description                        |
| ------------ | ---------------------------------- |
| `ValueError` | If format is neither log nor json. |

Source code in `src/desflow/connectors/egress/local.py`

```python
def __init__(self, format: Literal["log", "json"] = "log"):
    """
    Chooses how records are written.

    Args:
        format: `log` logs each batch at INFO level, one prefixed line per
            record. `json` writes one JSON line per record to stdout at any
            log level, because it is output, not a log.

    Raises:
        ValueError: If `format` is neither `log` nor `json`.
    """
    if format not in _FORMATS:
        raise ValueError(f"format must be log or json, got {format!r}")
    self.format = format
```

#### Methods:

##### `run(egress_queue)`

Continuously polls the egress queue and writes each batch.

This method implements an asynchronous polling loop. It extracts batches from the internal queue. With the `json` format it writes each batch to stdout in one write, one JSON line per record as published. With the `log` format it writes each batch with one log call at INFO level, one line per record, each line starting with a prefix:

- \[TEL\]: Telemetry data (e.g., resource utilization, queue lengths).
- \[EVT\]: Event data (e.g., task lifecycle transitions).
- \[LBL\]: Ground-truth records, on a sink added with `when: labels`.

One call per batch rather than per record costs far less (issue #24), so the logging format's prefix, such as the time and level, appears on the first line of each batch only.

Parameters:

| Name           | Type    | Description                                                                                  | Default    |
| -------------- | ------- | -------------------------------------------------------------------------------------------- | ---------- |
| `egress_queue` | `Queue` | A thread-safe queue containing lists of dictionaries (batches) generated by the EgressMixIn. | *required* |

Note

If the queue is empty, the method yields control to the asyncio event loop for a short duration to prevent CPU pinning.

Source code in `src/desflow/connectors/egress/local.py`

```python
async def run(self, egress_queue: queue.Queue) -> None:
    """
    Continuously polls the egress queue and writes each batch.

    This method implements an asynchronous polling loop. It extracts batches
    from the internal queue. With the `json` format it writes each batch to
    stdout in one write, one JSON line per record as published. With the
    `log` format it writes each batch with one log call at INFO
    level, one line per record, each line starting with a prefix:
    - [TEL]: Telemetry data (e.g., resource utilization, queue lengths).
    - [EVT]: Event data (e.g., task lifecycle transitions).
    - [LBL]: Ground-truth records, on a sink added with `when: labels`.

    One call per batch rather than per record costs far less (issue #24), so
    the logging format's prefix, such as the time and level, appears on the
    first line of each batch only.

    Args:
        egress_queue: A thread-safe queue containing lists of dictionaries
            (batches) generated by the EgressMixIn.

    Note:
        If the queue is empty, the method yields control to the asyncio
        event loop for a short duration to prevent CPU pinning.
    """
    while True:
        try:
            # Receive a batch (list) of messages
            batch = egress_queue.get_nowait()
        except queue.Empty:
            # Yield to the event loop
            await asyncio.sleep(0.1)
            continue
        if not batch:
            continue
        if self.format == "json":
            # sys.stdout is read at each write, so a caller that replaces it,
            # such as a test runner, receives the lines
            batch = drop_reserved_keys(batch, self)
            try:
                text = "".join(
                    orjson.dumps(self._shown(r)).decode() + "\n" for r in batch
                )
            except Exception as exc:
                # A batch that cannot be encoded is logged, and the sink goes on
                logger.error(f"Console: batch not printed: {exc}")
                continue
            sys.stdout.write(text)
            sys.stdout.flush()
            continue
        if not logger.isEnabledFor(logging.INFO):
            continue
        batch = drop_reserved_keys(batch, self)
        # Each line is the record without its stream type, which the prefix
        # names. The record is not altered: every egress provider is handed the
        # same dictionary objects, so removing a key here would also remove it
        # from what another attached sink writes.
        lines = [
            f"{_PREFIXES.get(data.get('stream_type', 'unknown'), '[EVT]')} "
            f"{self._shown(data, {k: v for k, v in data.items() if k != 'stream_type'})}"
            for data in batch
        ]
        logger.info("\n".join(lines))
```

### `desflow.connectors.egress.postgres.PostgresEgress(connection_dsn, table_name=None, upsert_keys=None, tables=None, table_policy='create', **kwargs)`

Bases: `BaseEgress`

Asynchronous egress provider for persisting simulation data to PostgreSQL.

This connector handles long-term storage of simulation results or continuous generation of relational CDC data, performing bulk inserts of batched data into a specified PostgreSQL table using asyncpg.

By default a record whose key already exists is skipped (`ON CONFLICT DO NOTHING`). Name the key columns in `upsert_keys` to update the existing row instead, which a simulation needs when it changes rows it wrote earlier, such as an order moving from processing to shipped.

A payload field `__table__` names the table of that event, and is removed from the row. An entry writes the events of its own `table_name` and ignores the others, so one entry per table, all fed by the same events, splits a stream into tables. An entry with `upsert_keys` is keyed: it skips an event with a null or missing key column, and logs one warning at the end of each batch with the count. Before 1.0.0 such an event stopped the batch with an error.

An ISO time string bound for a timestamp or date column is converted to a `datetime` or `date` first, because asyncpg rejects the string itself.

Tables named in `tables` are created at start when they do not exist, so a run needs no separate schema step.

An event's row also carries the record's `key` as `event_id`, and its `task` and `instance`, unless the payload has fields of those names. A ground-truth record, sent to a sink added with `when: labels`, is written as `sim_ts`, `timestamp`, `path`, `old`, `new` and `label`, the last three as JSON text.

Examples:

Updating orders as their status changes:

```python
egress = PostgresEgress(
    "postgresql://user:password@127.0.0.1:5432/db",
    table_name="orders",
    upsert_keys=["order_id"],
)
```

Creating the table at start, in the shape a YAML file gives:

```python
egress = PostgresEgress(
    "postgresql://user:password@127.0.0.1:5432/db",
    tables={
        "orders": {
            "columns": {"order_id": "INT", "status": "TEXT"},
            "primary_key": ["order_id"],
        }
    },
)
```

Initializes the PostgresEgress with connection details.

Parameters:

| Name             | Type             | Description                                                                                                                                                                                                                                                                                   | Default                                                                                                                                                                                         |
| ---------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection_dsn` | `str`            | PostgreSQL connection string (DSN).                                                                                                                                                                                                                                                           | *required*                                                                                                                                                                                      |
| `table_name`     | \`str            | None\`                                                                                                                                                                                                                                                                                        | Target table for simulation records. Defaults to the one table in tables when it names exactly one, and to "simulation_data" when tables is not given.                                          |
| `upsert_keys`    | \`list[str]      | None\`                                                                                                                                                                                                                                                                                        | Optional key columns. When given, a record whose key already exists updates that row's other columns, and the columns need a unique index or constraint. When None, such a record is skipped.   |
| `tables`         | \`dict[str, Any] | None\`                                                                                                                                                                                                                                                                                        | Optional tables to create at start when they do not exist. Each table name maps to columns, a mapping of column name to SQL type, and an optional primary_key, a column name or a list of them. |
| `table_policy`   | `str`            | What the sink does about the tables in tables when the run starts. create, the default, creates the schema of a qualified name (CREATE SCHEMA IF NOT EXISTS) and the table when they do not exist. drop_and_create also drops the table first (DROP TABLE IF EXISTS). manual creates nothing. | `'create'`                                                                                                                                                                                      |
| `**kwargs`       | `Any`            | Additional connection pool arguments for asyncpg.                                                                                                                                                                                                                                             | `{}`                                                                                                                                                                                            |

Raises:

| Type         | Description                                                                                                                                                         |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If a tables entry is malformed, if table_policy is not one of its three words, or if tables names several tables and table_name does not say which one to write to. |

Source code in `src/desflow/connectors/egress/postgres.py`

```python
def __init__(
    self,
    connection_dsn: str,
    table_name: str | None = None,
    upsert_keys: list[str] | None = None,
    tables: dict[str, Any] | None = None,
    table_policy: str = "create",
    **kwargs: Any,
):
    """
    Initializes the PostgresEgress with connection details.

    Args:
        connection_dsn: PostgreSQL connection string (DSN).
        table_name: Target table for simulation records. Defaults to the one
            table in `tables` when it names exactly one, and to
            "simulation_data" when `tables` is not given.
        upsert_keys: Optional key columns. When given, a record whose key
            already exists updates that row's other columns, and the columns
            need a unique index or constraint. When None, such a record is
            skipped.
        tables: Optional tables to create at start when they do not exist. Each
            table name maps to `columns`, a mapping of column name to SQL type,
            and an optional `primary_key`, a column name or a list of them.
        table_policy: What the sink does about the tables in `tables` when the
            run starts. `create`, the default, creates the schema of a qualified
            name (`CREATE SCHEMA IF NOT EXISTS`) and the table when they do not
            exist. `drop_and_create` also drops the table first
            (`DROP TABLE IF EXISTS`). `manual` creates nothing.
        **kwargs: Additional connection pool arguments for asyncpg.

    Raises:
        ValueError: If a `tables` entry is malformed, if `table_policy` is not one
            of its three words, or if `tables` names several tables and
            `table_name` does not say which one to write to.
    """
    if table_policy not in TABLE_POLICIES:
        raise ValueError(
            f"table_policy '{table_policy}' is not one of {', '.join(TABLE_POLICIES)}"
        )
    self.table_policy = table_policy
    self.tables: dict[str, tuple[dict[str, str], list[str]]] = {
        str(name): _table_spec(str(name), spec)
        for name, spec in (tables or {}).items()
    }
    if table_name is None:
        if len(self.tables) > 1:
            raise ValueError(
                f"tables names {sorted(self.tables)}; set table_name to the one "
                "this egress writes to"
            )
        table_name = next(iter(self.tables), "simulation_data")
    self.dsn = connection_dsn
    self.table_name = table_name
    self.upsert_keys = list(upsert_keys) if upsert_keys else None
    self.kwargs = kwargs
    self.pool: asyncpg.Pool | None = None
    self.valid_columns: set[str] = set()
    self.time_columns: dict[str, str] = {}
```

#### Methods:

##### `run(egress_queue)`

Main execution loop for PostgreSQL data persistence.

Parameters:

| Name           | Type    | Description                                                | Default    |
| -------------- | ------- | ---------------------------------------------------------- | ---------- |
| `egress_queue` | `Queue` | A thread-safe queue containing batches of simulation data. | *required* |

Raises:

| Type         | Description                                     |
| ------------ | ----------------------------------------------- |
| `ValueError` | If a column in upsert_keys is not in the table. |

Source code in `src/desflow/connectors/egress/postgres.py`

```python
async def run(self, egress_queue: queue.Queue) -> None:
    """
    Main execution loop for PostgreSQL data persistence.

    Args:
        egress_queue: A thread-safe queue containing batches of simulation data.

    Raises:
        ValueError: If a column in `upsert_keys` is not in the table.
    """
    try:
        await self._init_pool()
        await self._write_forever(egress_queue)
    finally:
        await self._close_pool()
```

### `desflow.connectors.egress.redis.RedisEgress(url, stream_name='simulation_data', **kwargs)`

Bases: `BaseEgress`

High-throughput asynchronous egress provider for streaming simulation data to Redis.

This connector utilizes `redis.asyncio` for asynchronous I/O and publishes batched simulation results (events and telemetry) to Redis Streams (`XADD`) for real-time consumers.

By default, it uses a unified stream, but supports dynamic stream multiplexing by reading the `__stream__` key from the data payloads.

Attributes:

| Name          | Type    | Description                                                               |
| ------------- | ------- | ------------------------------------------------------------------------- |
| `url`         | `str`   | The connection URL for the Redis server (e.g., redis://127.0.0.1:6379/0). |
| `stream_name` | `str`   | The default target Redis Stream name.                                     |
| `client`      | \`Redis | None\`                                                                    |
| `**kwargs`    | \`Redis | None\`                                                                    |

Initializes the RedisEgress with topic and connection settings.

Parameters:

| Name          | Type  | Description                                            | Default             |
| ------------- | ----- | ------------------------------------------------------ | ------------------- |
| `url`         | `str` | Redis connection URL (e.g., redis://127.0.0.1:6379/0). | *required*          |
| `stream_name` | `str` | Target Redis Stream name.                              | `'simulation_data'` |
| `**kwargs`    | `Any` | Additional arguments for redis.from_url.               | `{}`                |

Source code in `src/desflow/connectors/egress/redis.py`

```python
def __init__(self, url: str, stream_name: str = "simulation_data", **kwargs: Any):
    """
    Initializes the RedisEgress with topic and connection settings.

    Args:
        url: Redis connection URL (e.g., redis://127.0.0.1:6379/0).
        stream_name: Target Redis Stream name.
        **kwargs: Additional arguments for `redis.from_url`.
    """
    self.url = url
    self.stream_name = stream_name
    self.kwargs = kwargs
    self.client: redis.Redis | None = None
```

#### Methods:

##### `run(egress_queue)`

The main execution loop that consumes the egress queue and publishes to Redis.

This method maintains a connection to Redis and uses pipeline (`XADD`) commands to insert batches of data efficiently.

Parameters:

| Name           | Type    | Description                                                                                           | Default    |
| -------------- | ------- | ----------------------------------------------------------------------------------------------------- | ---------- |
| `egress_queue` | `Queue` | A thread-safe queue containing lists of dictionaries or Pydantic models generated by the EgressMixIn. | *required* |

Source code in `src/desflow/connectors/egress/redis.py`

```python
async def run(self, egress_queue: queue.Queue) -> None:
    """
    The main execution loop that consumes the egress queue and publishes to Redis.

    This method maintains a connection to Redis and uses pipeline (`XADD`)
    commands to insert batches of data efficiently.

    Args:
        egress_queue: A thread-safe queue containing lists of dictionaries
            or Pydantic models generated by the EgressMixIn.
    """
    try:
        await self._init_client()
        await self._write_forever(egress_queue)
    finally:
        await self._close_client()
```

### `desflow.connectors.egress.storage.JsonlStorageEgress(default_path=None, filesystem=None, path_router=None)`

Bases: `BaseEgress`

High-throughput JSONL batch writer for local file systems or object storage.

This connector uses PyArrow's Virtual File System (VFS) to write data to local disks, S3-compatible storage such as AWS S3 or SeaweedFS, Google Cloud Storage or Azure Blob Storage. It writes a uniquely named file for every batch, so no two writes go to the same file and a file, once written, is never changed.

By default, events are appended to the `default_path` as flat rows: each event's `value` mapping is unpacked into top-level fields, and telemetry is left out. If a `path_router` callable is provided, destination logic is delegated to that function, allowing for advanced multiplexing (e.g., splitting logs vs. errors) or dynamically dropping specific records by returning `None`. Records are then written as the router leaves them.

An event whose `value` holds `__path__`, a file path such as `packed/orders.jsonl`, is written to that path, and the key is removed from the row. This replaces a `path_router` for the common case. A `path_router` wins over `__path__`, and a record with no `__path__` goes to `default_path`. The folder of each path is created. With `envelope=False` on the entry, the row is the event's `value` alone.

Attributes:

| Name           | Type                 | Description                                                                                                 |
| -------------- | -------------------- | ----------------------------------------------------------------------------------------------------------- |
| `default_path` | `Optional[str]`      | The fallback destination path if no router is provided.                                                     |
| `filesystem`   | `Optional[Any]`      | A PyArrow FileSystem instance, or the mapping it is built from when the run starts. Defaults to local disk. |
| `path_router`  | `Optional[Callable]` | Optional logic to dynamically route or drop payloads.                                                       |

Examples:

Routing errors to a specific file and dropping meaningless metrics:

```python
def custom_log_router(data: dict) -> str | None:
    # Drop lag metrics
    if data.get("path_id") == "system.simulation.lag_seconds":
        return None

    # Route errors to a separate JSONL file
    if data.get("status") == "error":
        return "data/error_logs.jsonl"

    return "data/standard_logs.jsonl"

egress = JsonlStorageEgress(path_router=custom_log_router)
```

Initializes the JsonlStorageEgress with routing and VFS settings.

Parameters:

| Name           | Type                                        | Description                                                                                                                                                                                                                                                                    | Default |
| -------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| `default_path` | `Optional[str]`                             | The target file path prefix (e.g., "data/logs.jsonl"), used when no router is given. Only events are written there, with their value mapping unpacked into top-level fields.                                                                                                   | `None`  |
| `filesystem`   | `Optional[Any]`                             | An instantiated PyArrow FileSystem (e.g., fs.S3FileSystem()), or a mapping such as {"type": "s3", "endpoint_override": ...} that is built into one when the run starts. If None, defaults to fs.LocalFileSystem(). The destination folder is created on the first write to it. | `None`  |
| `path_router`  | `Optional[Callable[[dict], Optional[str]]]` | A function taking a dict payload and returning a string path, or None to drop the record.                                                                                                                                                                                      | `None`  |

Raises:

| Type          | Description                                      |
| ------------- | ------------------------------------------------ |
| `ImportError` | If pyarrow is not installed (the parquet extra). |

Source code in `src/desflow/connectors/egress/storage.py`

```python
def __init__(
    self,
    default_path: Optional[str] = None,
    filesystem: Optional[Any] = None,
    path_router: Optional[Callable[[dict], Optional[str]]] = None,
):
    """
    Initializes the JsonlStorageEgress with routing and VFS settings.

    Args:
        default_path: The target file path prefix (e.g., "data/logs.jsonl"),
            used when no router is given. Only events are written there, with
            their `value` mapping unpacked into top-level fields.
        filesystem: An instantiated PyArrow FileSystem (e.g., `fs.S3FileSystem()`),
            or a mapping such as `{"type": "s3", "endpoint_override": ...}` that
            is built into one when the run starts. If None, defaults to
            `fs.LocalFileSystem()`. The destination folder is created on the
            first write to it.
        path_router: A function taking a dict payload and returning a string path,
            or None to drop the record.

    Raises:
        ImportError: If pyarrow is not installed (the `parquet` extra).
    """
    require_packages("JsonlStorageEgress", "parquet", "pyarrow")
    _check_filesystem(filesystem)
    self.default_path = default_path
    self.filesystem = filesystem
    self.path_router = path_router
    self._folders: set = set()
```

#### Methods:

##### `run(egress_queue)`

The main execution loop that consumes the egress queue and writes JSONL chunks.

This loop continuously polls the internal `egress_queue` for data batches. When a batch is received, it offloads the synchronous PyArrow file I/O operations to a background thread to prevent blocking the asyncio event loop.

Parameters:

| Name           | Type    | Description                                                                                        | Default    |
| -------------- | ------- | -------------------------------------------------------------------------------------------------- | ---------- |
| `egress_queue` | `Queue` | A thread-safe queue containing batches of dictionaries generated by the environment's EgressMixIn. | *required* |

Raises:

| Type          | Description                                |
| ------------- | ------------------------------------------ |
| `ImportError` | If the 'pyarrow' package is not installed. |

Note

The loop exits gracefully upon receiving an `asyncio.CancelledError`, ensuring background file writers have completed before shutting down.

Source code in `src/desflow/connectors/egress/storage.py`

```python
async def run(self, egress_queue: queue.Queue) -> None:
    """
    The main execution loop that consumes the egress queue and writes JSONL chunks.

    This loop continuously polls the internal `egress_queue` for data batches.
    When a batch is received, it offloads the synchronous PyArrow file I/O operations
    to a background thread to prevent blocking the asyncio event loop.

    Args:
        egress_queue: A thread-safe queue containing batches of dictionaries
            generated by the environment's EgressMixIn.

    Raises:
        ImportError: If the 'pyarrow' package is not installed.

    Note:
        The loop exits gracefully upon receiving an `asyncio.CancelledError`,
        ensuring background file writers have completed before shutting down.
    """
    try:
        from pyarrow import fs
    except ImportError:
        raise ImportError("pyarrow is required. pip install desflow[parquet]")

    self.filesystem = _build_filesystem(self.filesystem, fs)
    self._start_run()
    batches_processed = 0

    try:
        while True:
            try:
                batch = egress_queue.get_nowait()
                written = 0
                with self._writing():
                    try:
                        written = await asyncio.to_thread(self._write_batch, batch)
                    except Exception as exc:
                        # A batch that fails is logged, and the sink goes on
                        logger.error(f"JSONL Writer: batch not written: {exc}")
                        continue
                if not written:
                    continue
                # Only what was written counts; log a heartbeat every 10
                batches_processed += written
                if batches_processed % 10 == 0:
                    q_size = egress_queue.qsize()
                    logger.info(
                        f"JSONL Writer: Processed {batches_processed} batches. "
                        f"(~{q_size} batches waiting in queue)"
                    )
            except queue.Empty:
                await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        # This triggers during env.teardown()
        logger.info(
            f"JsonlStorageEgress shut down requested. "
            f"Successfully wrote {batches_processed} total chunks to storage."
        )
```

### `desflow.connectors.egress.storage.ParquetStorageEgress(default_path=None, filesystem=None, path_router=None)`

Bases: `BaseEgress`

Columnar Parquet writer for local disks or object storage.

This connector buffers simulation records and converts them into heavily compressed Parquet tables using `pyarrow`. It natively supports writing to local disks, S3-compatible object stores (AWS S3, SeaweedFS), Google Cloud Storage or Azure Blob Storage via PyArrow's VFS.

To support massive parallel processing (e.g., Athena, Databricks), it implements file rotation (chunking), creating a uniquely named Parquet part-file for every flush of the buffer. A file's columns come from every row of its batch; a column a later batch adds widens the path's schema, and each batch is cast to it, so a column keeps its type across chunks.

Without a `path_router`, events are written to `default_path` as flat rows: each event's `value` mapping becomes columns, and telemetry is left out. With a router, records are written as the router leaves them.

An event whose `value` holds `__path__`, a file path such as `orders/orders.parquet`, is written to that path, and the key is removed from the row. This replaces a `path_router` for the common case. A `path_router` wins over `__path__`, and a record with no `__path__` goes to `default_path`. The folder of each path is created. With `envelope=False` on the entry, the row is the event's `value` alone.

Attributes:

| Name           | Type                 | Description                                                                                            |
| -------------- | -------------------- | ------------------------------------------------------------------------------------------------------ |
| `default_path` | `Optional[str]`      | The fallback destination path.                                                                         |
| `filesystem`   | `Optional[Any]`      | A PyArrow FileSystem instance, or the mapping it is built from when the run starts. Defaults to local. |
| `path_router`  | `Optional[Callable]` | Optional logic to dynamically route or drop payloads.                                                  |
| `schemas`      | `Dict[str, Any]`     | Internal registry of inferred PyArrow schemas per file path.                                           |

Examples:

Splitting simulation events and telemetry into distinct Parquet datasets directly to S3:

```python
from pyarrow import fs

def datalake_router(data: dict) -> str | None:
    if data.get("stream_type") == "telemetry":
        return "simulation_data/telemetry.parquet"
    return "simulation_data/events.parquet"

# Connect directly to SeaweedFS or AWS S3
s3 = fs.S3FileSystem(endpoint_override="127.0.0.1:8333", scheme="http")

egress = ParquetStorageEgress(
    filesystem=s3,
    path_router=datalake_router
)
```

Initializes the ParquetStorageEgress with routing and VFS settings.

Parameters:

| Name           | Type                                        | Description                                                                                                                                                                                                                                                                    | Default |
| -------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| `default_path` | `Optional[str]`                             | The target file path prefix (e.g., "data/events.parquet"), used when no router is given. Only events are written there, with their value mapping unpacked into columns.                                                                                                        | `None`  |
| `filesystem`   | `Optional[Any]`                             | An instantiated PyArrow FileSystem (e.g., fs.S3FileSystem()), or a mapping such as {"type": "s3", "endpoint_override": ...} that is built into one when the run starts. If None, defaults to fs.LocalFileSystem(). The destination folder is created on the first write to it. | `None`  |
| `path_router`  | `Optional[Callable[[dict], Optional[str]]]` | A function taking a dict payload and returning a string path, or None to drop the record.                                                                                                                                                                                      | `None`  |

Raises:

| Type          | Description                                      |
| ------------- | ------------------------------------------------ |
| `ImportError` | If pyarrow is not installed (the parquet extra). |

Source code in `src/desflow/connectors/egress/storage.py`

```python
def __init__(
    self,
    default_path: Optional[str] = None,
    filesystem: Optional[Any] = None,
    path_router: Optional[Callable[[dict], Optional[str]]] = None,
):
    """
    Initializes the ParquetStorageEgress with routing and VFS settings.

    Args:
        default_path: The target file path prefix (e.g., "data/events.parquet"),
            used when no router is given. Only events are written there, with
            their `value` mapping unpacked into columns.
        filesystem: An instantiated PyArrow FileSystem (e.g., `fs.S3FileSystem()`),
            or a mapping such as `{"type": "s3", "endpoint_override": ...}` that
            is built into one when the run starts. If None, defaults to
            `fs.LocalFileSystem()`. The destination folder is created on the
            first write to it.
        path_router: A function taking a dict payload and returning a string path,
            or None to drop the record.

    Raises:
        ImportError: If pyarrow is not installed (the `parquet` extra).
    """
    require_packages("ParquetStorageEgress", "parquet", "pyarrow")
    _check_filesystem(filesystem)
    self.default_path = default_path
    self.filesystem = filesystem
    self.path_router = path_router
    self._folders: set = set()
    self.schemas: Dict[str, Any] = {}
```

#### Methods:

##### `run(egress_queue)`

The main execution loop that consumes the egress queue and writes Parquet chunks.

This loop continuously polls the internal `egress_queue` for data batches. When a batch is received, it offloads the synchronous PyArrow table conversion and file I/O operations to a background thread to prevent blocking the asyncio loop.

Parameters:

| Name           | Type    | Description                                                                                        | Default    |
| -------------- | ------- | -------------------------------------------------------------------------------------------------- | ---------- |
| `egress_queue` | `Queue` | A thread-safe queue containing batches of dictionaries generated by the environment's EgressMixIn. | *required* |

Raises:

| Type          | Description                                |
| ------------- | ------------------------------------------ |
| `ImportError` | If the 'pyarrow' package is not installed. |

Note

The loop exits gracefully upon receiving an `asyncio.CancelledError`, ensuring the final Parquet table has been fully flushed to disk.

Source code in `src/desflow/connectors/egress/storage.py`

```python
async def run(self, egress_queue: queue.Queue) -> None:
    """
    The main execution loop that consumes the egress queue and writes Parquet chunks.

    This loop continuously polls the internal `egress_queue` for data batches.
    When a batch is received, it offloads the synchronous PyArrow table conversion
    and file I/O operations to a background thread to prevent blocking the asyncio loop.

    Args:
        egress_queue: A thread-safe queue containing batches of dictionaries
            generated by the environment's EgressMixIn.

    Raises:
        ImportError: If the 'pyarrow' package is not installed.

    Note:
        The loop exits gracefully upon receiving an `asyncio.CancelledError`,
        ensuring the final Parquet table has been fully flushed to disk.
    """
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        from pyarrow import fs
    except ImportError:
        raise ImportError("pyarrow is required. pip install desflow[parquet]")

    self.filesystem = _build_filesystem(self.filesystem, fs)
    self._start_run()
    batches_processed = 0

    try:
        while True:
            try:
                batch = egress_queue.get_nowait()
                written = 0
                with self._writing():
                    try:
                        written = await asyncio.to_thread(
                            self._write_batch, batch, pa, pq
                        )
                    except Exception as exc:
                        # A batch that fails is logged, and the sink goes on
                        logger.error(f"Parquet Writer: batch not written: {exc}")
                if not written:
                    continue
                # Only what was written counts; log a heartbeat every 10
                batches_processed += written
                if batches_processed % 10 == 0:
                    q_size = egress_queue.qsize()
                    logger.info(
                        f"Parquet Writer: Processed {batches_processed} batches. "
                        f"(~{q_size} batches waiting in queue)"
                    )
            except queue.Empty:
                await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        # This triggers during env.teardown()
        logger.info(
            f"ParquetStorageEgress shut down requested. "
            f"Successfully wrote {batches_processed} total chunks to storage."
        )
```

### `desflow.connectors.egress.iceberg.IcebergStorageEgress(catalog, default_table=None, table_router=None, schemas=None, locations=None, upsert_keys=None, table_policy='create')`

Bases: `BaseEgress`

Apache Iceberg table writer for lakehouse ingestion patterns.

`ParquetStorageEgress` writes files and leaves registration to the caller. This connector appends straight into an Iceberg table instead, so one run can feed a stream sink and a queryable table in the same pass. Each flush of a provider's buffer becomes exactly one Iceberg commit, which is why the buffer size matters: every commit writes a manifest, a manifest list and a new `metadata.json`, and query planning degrades as the snapshot count grows. Give this provider a large `batch_size` on `add_egress` so the run produces a handful of snapshots rather than hundreds.

Without a `table_router`, events are written to `default_table` as flat rows: each event's `value` mapping becomes columns, and telemetry is left out. With a router, records are written as the router leaves them.

By default every record is appended. Name a table's key columns in `upsert_keys` to write it with PyIceberg's `upsert` instead: a record whose key exists updates that row, and a new key is inserted, so rerunning a seeded simulation over the same window does not duplicate rows. An upsert reads the matching rows before writing, so it is slower than an append, and it never deletes rows. It replaces the whole row, so every record must carry every column: a missing column is written as null. Within one flush, the last record for a key wins. An upsert flush is still one commit, but it can add up to three snapshots: an overwrite and two appends.

An event whose `value` holds `__table__`, a `namespace.table` identifier, is written to that table, and the key is removed from the row. This replaces a `table_router` for the common case. A `table_router` wins over `__table__`, and a record with no `__table__` goes to `default_table`. An entry with `upsert_keys` is keyed. It skips a record that has no destination, and a record with a null or missing key column of its table, and it logs one warning at the end of each flush with the counts skipped. Before 1.0.0 such a record stopped the batch with an error.

The catalog is supplied by the caller, either as a built `Catalog` or as a mapping of PyIceberg catalog properties that is passed to `load_catalog`. The supported catalogs, which the `iceberg` extra installs, each with the test that writes through it, are:

- Iceberg REST, in the integration tests against the odctl `catalog` profile;
- SQL on SQLite, in the unit tests;
- SQL on PostgreSQL, in the integration tests against the odctl `postgres` profile;
- Glue, in the integration tests against Floci, an AWS emulator.

A table without a schema in `schemas` is created with the columns of every row of its first batch, and a column a later batch adds is added to the table, as the Parquet writer widens its schema. Inference reads an ISO timestamp as a string, so a table whose consumer expects a real timestamp needs an explicit schema. An ISO string bound for a timestamp or date column of the table is converted to a `datetime` or `date` before the write, because PyArrow rejects the string itself.

Attributes:

| Name                 | Type                       | Description                                                                                                             |
| -------------------- | -------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `catalog`            | `Any`                      | An instantiated pyiceberg Catalog, or None until it is built from catalog_properties.                                   |
| `catalog_properties` | `Optional[Dict[str, Any]]` | The PyIceberg catalog properties given instead of a Catalog, or None.                                                   |
| `default_table`      | `Optional[str]`            | Fallback namespace.table when no router is given.                                                                       |
| `table_router`       | `Optional[Callable]`       | Logic returning namespace.table, or None to drop the record.                                                            |
| `schemas`            | `Dict[str, Any]`           | PyArrow schema per table identifier. Seeded with any explicit schemas, then filled in from the tables that get created. |
| `locations`          | `Dict[str, str]`           | Explicit storage location per table identifier.                                                                         |
| `upsert_keys`        | `Dict[str, list]`          | Key columns per table identifier, for the tables written with upsert rather than append.                                |

Examples:

Splitting events and telemetry into two tables on a REST catalog:

```python
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    "odctl", uri="http://127.0.0.1:8181", warehouse="s3://warehouse/"
)

def table_router(data: dict) -> str | None:
    if data.get("path_id") == "system.simulation.lag_seconds":
        return None  # drop
    if data.get("stream_type") == "telemetry":
        return "simulation.telemetry"
    return "simulation.events"

egress = IcebergStorageEgress(
    catalog=catalog,
    table_router=table_router,
    schemas={
        "simulation.events": pa.schema(
            [
                ("stream_type", pa.string()),
                ("sim_ts", pa.float64()),
                ("timestamp", pa.timestamp("us")),
                ("key", pa.string()),
            ]
        )
    },
)

app.add_egress(egress, batch_size=200_000)
```

Initializes the IcebergStorageEgress with a catalog and routing settings.

Parameters:

| Name            | Type                                        | Description                                                                                                                                                                                                                                                                                                                                                                                | Default    |
| --------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| `catalog`       | `Any`                                       | An instantiated pyiceberg Catalog, already configured with its URI, warehouse and credentials. A mapping of PyIceberg catalog properties (uri, warehouse, s3.endpoint and so on) is also accepted: the catalog is then built with PyIceberg's load_catalog on the first write, with an optional name key as the catalog name.                                                              | *required* |
| `default_table` | `Optional[str]`                             | The target namespace.table used when no router is given. Only events are written there, with their value mapping unpacked into columns.                                                                                                                                                                                                                                                    | `None`     |
| `table_router`  | `Optional[Callable[[dict], Optional[str]]]` | A function taking a dict payload and returning a namespace.table identifier, or None to drop the record.                                                                                                                                                                                                                                                                                   | `None`     |
| `schemas`       | `Optional[Dict[str, Any]]`                  | Optional PyArrow schema per table identifier. A table named here is created with that schema and every batch is cast to it, instead of the schema being inferred from the first batch. A schema can also be a mapping of column name to an Iceberg type name: string, int, long, float, double, boolean, timestamp, timestamptz, date or binary.                                           | `None`     |
| `locations`     | `Optional[Dict[str, str]]`                  | Optional storage location per table identifier. Pin this when a consumer reads the table by path, because a catalog with unique-table-location set otherwise appends a random suffix.                                                                                                                                                                                                      | `None`     |
| `upsert_keys`   | `Optional[Dict[str, list]]`                 | Optional key columns per table identifier. A table named here is written with upsert on those columns; any other table is appended to. An entry with upsert_keys is keyed: it skips a record that has no destination (no __table__, no router and no default_table), and a record with a null or missing key column of its table, and warns once at the end of each flush with the counts. | `None`     |
| `table_policy`  | `str`                                       | What the sink does about a table at its first write. create, the default, creates the namespace and the table when they do not exist. drop_and_create drops the table if it exists and creates it again, once per run. manual creates nothing and fails the write if the table does not exist.                                                                                             | `'create'` |

Raises:

| Type          | Description                                                                                                                                                                                  |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError`  | If table_policy is not one of its three words, or if none of default_table, table_router, schemas, locations and upsert_keys is given, since the connector would then have nowhere to write. |
| `ImportError` | If pyiceberg or pyarrow is not installed (the iceberg extra), or if upsert_keys is given and the installed pyiceberg is older than 0.9.0, which added Table.upsert.                          |

Source code in `src/desflow/connectors/egress/iceberg.py`

```python
def __init__(
    self,
    catalog: Any,
    default_table: Optional[str] = None,
    table_router: Optional[Callable[[dict], Optional[str]]] = None,
    schemas: Optional[Dict[str, Any]] = None,
    locations: Optional[Dict[str, str]] = None,
    upsert_keys: Optional[Dict[str, list]] = None,
    table_policy: str = "create",
):
    """
    Initializes the IcebergStorageEgress with a catalog and routing settings.

    Args:
        catalog: An instantiated pyiceberg `Catalog`, already configured with its
            URI, warehouse and credentials. A mapping of PyIceberg catalog
            properties (`uri`, `warehouse`, `s3.endpoint` and so on) is also
            accepted: the catalog is then built with PyIceberg's `load_catalog`
            on the first write, with an optional `name` key as the catalog name.
        default_table: The target `namespace.table` used when no router is given.
            Only events are written there, with their `value` mapping unpacked
            into columns.
        table_router: A function taking a dict payload and returning a
            `namespace.table` identifier, or None to drop the record.
        schemas: Optional PyArrow schema per table identifier. A table named here
            is created with that schema and every batch is cast to it, instead of
            the schema being inferred from the first batch. A schema can also be
            a mapping of column name to an Iceberg type name: string, int, long,
            float, double, boolean, timestamp, timestamptz, date or binary.
        locations: Optional storage location per table identifier. Pin this when a
            consumer reads the table by path, because a catalog with
            `unique-table-location` set otherwise appends a random suffix.
        upsert_keys: Optional key columns per table identifier. A table named
            here is written with `upsert` on those columns; any other table is
            appended to. An entry with `upsert_keys` is keyed: it skips a record
            that has no destination (no `__table__`, no router and no
            `default_table`), and a record with a null or missing key column of its
            table, and warns once at the end of each flush with the counts.
        table_policy: What the sink does about a table at its first write.
            `create`, the default, creates the namespace and the table when they
            do not exist. `drop_and_create` drops the table if it exists and
            creates it again, once per run. `manual` creates nothing and fails
            the write if the table does not exist.

    Raises:
        ValueError: If `table_policy` is not one of its three words, or if none
            of `default_table`, `table_router`, `schemas`, `locations` and
            `upsert_keys` is given, since the connector would then have nowhere
            to write.
        ImportError: If pyiceberg or pyarrow is not installed (the `iceberg`
            extra), or if `upsert_keys` is given and the installed pyiceberg is
            older than 0.9.0, which added `Table.upsert`.
    """
    require_packages("IcebergStorageEgress", "iceberg", "pyiceberg", "pyarrow")
    if table_policy not in TABLE_POLICIES:
        raise ValueError(
            f"table_policy '{table_policy}' is not one of {', '.join(TABLE_POLICIES)}"
        )
    # An entry that names its tables routes by `__table__` alone
    names_tables = bool(schemas or locations or upsert_keys)
    if default_table is None and table_router is None and not names_tables:
        raise ValueError(
            "IcebergStorageEgress needs default_table or table_router, or "
            "tables named in schemas, locations or upsert_keys to route "
            "__table__ to; without one it has no table to write to"
        )
    self.table_policy = table_policy
    # A mapping of properties is loaded on the first write rather than here,
    # because a REST catalog contacts its server as soon as it is built.
    self.catalog_properties: Optional[Dict[str, Any]] = None
    self.catalog = catalog
    if isinstance(catalog, Mapping):
        self.catalog_properties = dict(catalog)
        self.catalog = None
    self.default_table = default_table
    self.table_router = table_router
    self.schemas: Dict[str, Any] = {
        identifier: _arrow_schema(identifier, schema)
        for identifier, schema in (schemas or {}).items()
    }
    # A table whose schema the caller gave keeps it; an inferred one gains the
    # columns later batches add
    self._pinned = set(self.schemas)
    self._declared_schemas = dict(self.schemas)
    self.locations: Dict[str, str] = dict(locations or {})
    self.upsert_keys: Dict[str, list] = {
        identifier: list(keys) for identifier, keys in (upsert_keys or {}).items()
    }
    if self.upsert_keys:
        from pyiceberg.table import Table

        # Table.upsert arrived in pyiceberg 0.9.0; the package floor is lower
        # because appending works on older releases.
        if not hasattr(Table, "upsert"):
            raise ImportError(
                "upsert_keys needs pyiceberg 0.9.0 or later: "
                'pip install "pyiceberg>=0.9.0"'
            )
    # Loaded tables, kept so a batch costs one append rather than a catalog
    # round trip as well. pyiceberg retries the commit if the handle is stale,
    # so another writer changing the table does not lose this one's records.
    # `run` empties it at the start of each run.
    self._tables: Dict[str, Any] = {}
```

#### Methods:

##### `run(egress_queue)`

The main execution loop that consumes the egress queue and commits to Iceberg.

This loop continuously polls the internal `egress_queue` for data batches. When a batch is received, it offloads the synchronous PyArrow conversion and the Iceberg commit to a background thread, so the asyncio loop is not blocked while the catalog is written.

Parameters:

| Name           | Type    | Description                                                                                        | Default    |
| -------------- | ------- | -------------------------------------------------------------------------------------------------- | ---------- |
| `egress_queue` | `Queue` | A thread-safe queue containing batches of dictionaries generated by the environment's EgressMixIn. | *required* |

Raises:

| Type          | Description                                |
| ------------- | ------------------------------------------ |
| `ImportError` | If the 'pyarrow' package is not installed. |

Note

The loop exits gracefully upon receiving an `asyncio.CancelledError`, so the final commit has completed before shutdown returns.

Source code in `src/desflow/connectors/egress/iceberg.py`

```python
async def run(self, egress_queue: queue.Queue) -> None:
    """
    The main execution loop that consumes the egress queue and commits to Iceberg.

    This loop continuously polls the internal `egress_queue` for data batches.
    When a batch is received, it offloads the synchronous PyArrow conversion and
    the Iceberg commit to a background thread, so the asyncio loop is not blocked
    while the catalog is written.

    Args:
        egress_queue: A thread-safe queue containing batches of dictionaries
            generated by the environment's EgressMixIn.

    Raises:
        ImportError: If the 'pyarrow' package is not installed.

    Note:
        The loop exits gracefully upon receiving an `asyncio.CancelledError`, so
        the final commit has completed before shutdown returns.
    """
    try:
        import pyarrow as pa
    except ImportError:
        raise ImportError(
            "pyarrow is required to build the batches. pip install desflow[iceberg]"
        )

    self._start_run()
    batches_processed = 0

    try:
        while True:
            try:
                batch = egress_queue.get_nowait()
                written = 0
                with self._writing():
                    try:
                        written = await asyncio.to_thread(
                            self._write_batch, batch, pa
                        )
                    except Exception as exc:
                        # A batch that fails is logged, and the sink goes on
                        logger.error(f"Iceberg Writer: batch not written: {exc}")
                if not written:
                    continue
                # Only what was written counts; log a heartbeat every 10
                batches_processed += written
                if batches_processed % 10 == 0:
                    q_size = egress_queue.qsize()
                    logger.info(
                        f"Iceberg Writer: Committed {batches_processed} batches. "
                        f"(~{q_size} batches waiting in queue)"
                    )
            except queue.Empty:
                await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        # This triggers during env.teardown()
        logger.info(
            f"IcebergStorageEgress shut down requested. "
            f"Successfully committed {batches_processed} total snapshots."
        )
```

## Serializers

### `desflow.connectors.egress.kafka.MessageSerializer`

Bases: `Protocol`

Protocol defining the interface for all Kafka egress serializers.

Any custom serializer passed to `KafkaEgress` must implement the `serialize` method according to this signature.

#### Methods:

##### `serialize(topic, data)`

Serializes the given payload into a byte string for Kafka transmission.

Parameters:

| Name    | Type  | Description                                                                                                                                                                              | Default    |
| ------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `topic` | `str` | The destination Kafka topic. This is often required by schema registries (like Avro) to resolve the correct schema subject, even if simpler serializers (like JSON) choose to ignore it. | *required* |
| `data`  | `Any` | The payload to serialize. This is typically a raw dictionary or a Pydantic model instance.                                                                                               | *required* |

Returns:

| Name    | Type    | Description                                                   |
| ------- | ------- | ------------------------------------------------------------- |
| `bytes` | `bytes` | The fully encoded byte string ready to be published to Kafka. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given payload into a byte string for Kafka transmission.

    Args:
        topic (str): The destination Kafka topic. This is often required by
            schema registries (like Avro) to resolve the correct schema subject,
            even if simpler serializers (like JSON) choose to ignore it.
        data (Any): The payload to serialize. This is typically a raw dictionary
            or a Pydantic model instance.

    Returns:
        bytes: The fully encoded byte string ready to be published to Kafka.
    """
    ...
```

### `desflow.connectors.egress.kafka.JsonSerializer`

Default fallback serializer providing backward compatibility via orjson.

This serializer converts dictionaries or Pydantic models into standard JSON byte strings.

#### Methods:

##### `serialize(topic, data)`

Serializes the given data to a JSON byte string.

Parameters:

| Name    | Type  | Description                                                                  | Default    |
| ------- | ----- | ---------------------------------------------------------------------------- | ---------- |
| `topic` | `str` | The target Kafka topic (unused by this serializer but required by protocol). | *required* |
| `data`  | `Any` | The payload to serialize (dictionary or Pydantic model).                     | *required* |

Returns:

| Name    | Type    | Description                   |
| ------- | ------- | ----------------------------- |
| `bytes` | `bytes` | The JSON-encoded byte string. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given data to a JSON byte string.

    Args:
        topic (str): The target Kafka topic (unused by this serializer but required by protocol).
        data (Any): The payload to serialize (dictionary or Pydantic model).

    Returns:
        bytes: The JSON-encoded byte string.
    """
    payload = extract_dict(data)
    return orjson.dumps(payload)
```

### `desflow.connectors.egress.kafka.ConfluentAvroSerializer(registry_url, schema_str, *, auth=UNSET, verify=UNSET, cert=UNSET, timeout=UNSET, headers=UNSET, proxy=UNSET, max_retries=UNSET)`

Lazy-loaded serializer for Confluent Schema Registry using fastavro and httpx.

This class converts simulation data into Avro-encoded byte strings in the Confluent wire format. It registers the schema under the subject `<topic>-value` on the first record of each topic, and reuses the schema id after that.

Initializes the Confluent Avro serializer. It makes no request to the registry.

`UNSET` as a default, in the signature or the Default column of the API page, means the argument was not given; each argument's description says its real default.

Parameters:

| Name           | Type                          | Description                                                                                                                                                                | Default      |
| -------------- | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `registry_url` | `str`                         | The base URL of the Confluent Schema Registry. Several URLs separated by commas are tried in order.                                                                        | *required*   |
| `schema_str`   | `str`                         | The Avro schema defined as a JSON string (typically generated from a dataclasses-avroschema AvroBaseModel via .avro_schema(); a plain Pydantic model does not provide it). | *required*   |
| `auth`         | `Optional[Tuple[str, str]]`   | A username and password for basic authentication, for example ("user", "pass"). The default is None.                                                                       | `UNSET`      |
| `verify`       | \`bool                        | str                                                                                                                                                                        | SSLContext\` |
| `cert`         | \`str                         | tuple                                                                                                                                                                      | None\`       |
| `timeout`      | `Optional[float]`             | Seconds each step of a request to the registry (connecting, sending, reading) may take. The default is 10.0. None waits without a limit.                                   | `UNSET`      |
| `headers`      | `Optional[Mapping[str, str]]` | Headers sent with every request, for example {"Authorization": "Bearer <token>"}. The default is None.                                                                     | `UNSET`      |
| `proxy`        | `Optional[str]`               | The URL of a proxy. The default is None.                                                                                                                                   | `UNSET`      |
| `max_retries`  | `int`                         | How many times a request is repeated after a transport error or a status of 408, 429, 500, 502, 503 or 504. The default is 3.                                              | `UNSET`      |

Raises:

| Type          | Description                                            |
| ------------- | ------------------------------------------------------ |
| `ImportError` | If the 'fastavro' or 'httpx' package is not installed. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def __init__(
    self,
    registry_url: str,
    schema_str: str,
    *,
    auth: Optional[Tuple[str, str]] = UNSET,
    verify: Union[bool, str, ssl.SSLContext] = UNSET,
    cert: Optional[
        Union[
            str,
            Tuple[str, Optional[str]],
            Tuple[str, Optional[str], Optional[str]],
        ]
    ] = UNSET,
    timeout: Optional[float] = UNSET,
    headers: Optional[Mapping[str, str]] = UNSET,
    proxy: Optional[str] = UNSET,
    max_retries: int = UNSET,
):
    """
    Initializes the Confluent Avro serializer. It makes no request to the registry.

    `UNSET` as a default, in the signature or the Default column of the API page,
    means the argument was not given; each argument's description says its real
    default.

    Args:
        registry_url (str): The base URL of the Confluent Schema Registry.
            Several URLs separated by commas are tried in order.
        schema_str (str): The Avro schema defined as a JSON string (typically
            generated from a dataclasses-avroschema `AvroBaseModel` via
            `.avro_schema()`; a plain Pydantic model does not provide it).
        auth (Optional[Tuple[str, str]]): A username and password for basic
            authentication, for example `("user", "pass")`. The default is None.
        verify (bool | str | ssl.SSLContext): How to check the registry's
            certificate: True (the default), False, the path of a CA file or
            directory, or an SSL context.
        cert (str | tuple | None): A client certificate file, or a tuple of the
            certificate file, the key file and the key password. The default is None.
        timeout (Optional[float]): Seconds each step of a request to the registry
            (connecting, sending, reading) may take. The default is 10.0. None
            waits without a limit.
        headers (Optional[Mapping[str, str]]): Headers sent with every request,
            for example `{"Authorization": "Bearer <token>"}`. The default is None.
        proxy (Optional[str]): The URL of a proxy. The default is None.
        max_retries (int): How many times a request is repeated after a
            transport error or a status of 408, 429, 500, 502, 503 or 504. The
            default is 3.

    Raises:
        ImportError: If the 'fastavro' or 'httpx' package is not installed.
    """
    try:
        import fastavro  # noqa: F401
        import httpx  # noqa: F401

        from desflow.connectors._schema_registry import avro
        from desflow.connectors._schema_registry.confluent_client import (
            ConfluentRegistryClient,
            given_arguments,
        )
        from desflow.connectors._schema_registry.wire import frame_confluent
    except ImportError:
        raise ImportError(
            "The 'fastavro' and 'httpx' packages are required to use "
            "ConfluentAvroSerializer. "
            "Install them using: pip install desflow[confluent]"
        )

    options = given_arguments(
        auth=auth,
        verify=verify,
        cert=cert,
        timeout=timeout,
        headers=headers,
        proxy=proxy,
        max_retries=max_retries,
    )
    self._client = ConfluentRegistryClient(registry_url, **options)
    self._avro = avro
    self._frame = frame_confluent
    self._schema_str = avro.load_schema_str(schema_str)
    self._parsed = avro.parse_writer_schema(
        self._schema_str, (), self._client.subject_version
    )
```

#### Methods:

##### `serialize(topic, data)`

Serializes the given data into an Avro byte string.

The first call for a topic registers the schema under `<topic>-value`, which makes a request to the registry.

Parameters:

| Name    | Type  | Description                                              | Default    |
| ------- | ----- | -------------------------------------------------------- | ---------- |
| `topic` | `str` | The target Kafka topic, used for schema subject naming.  | *required* |
| `data`  | `Any` | The payload to serialize (dictionary or Pydantic model). | *required* |

Returns:

| Name    | Type    | Description                                              |
| ------- | ------- | -------------------------------------------------------- |
| `bytes` | `bytes` | The Avro-encoded byte string, with the Confluent prefix. |

Raises:

| Type                  | Description                          |
| --------------------- | ------------------------------------ |
| `SchemaRegistryError` | If the registry refuses the schema.  |
| `TransportError`      | If no registry URL could be reached. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given data into an Avro byte string.

    The first call for a topic registers the schema under `<topic>-value`, which
    makes a request to the registry.

    Args:
        topic (str): The target Kafka topic, used for schema subject naming.
        data (Any): The payload to serialize (dictionary or Pydantic model).

    Returns:
        bytes: The Avro-encoded byte string, with the Confluent prefix.

    Raises:
        SchemaRegistryError: If the registry refuses the schema.
        httpx.TransportError: If no registry URL could be reached.
    """
    payload = extract_dict(data)
    schema_id = self._client.register(f"{topic}-value", self._schema_str, "AVRO")
    return self._frame(schema_id, self._avro.encode(self._parsed, payload))
```

### `desflow.connectors.egress.kafka.GlueAvroSerializer(registry_name, schema_str, **boto3_kwargs)`

Lazy-loaded serializer for AWS Glue Schema Registry using boto3.

This class converts simulation data into Avro-encoded byte strings, integrating directly with AWS Glue Schema Registry for schema validation.

Initializes the AWS Glue Avro serializer.

The constructor makes no call to Glue. The first `serialize` for a topic finds or registers the schema.

Parameters:

| Name             | Type  | Description                                                                                                                                                                | Default    |
| ---------------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_name`  | `str` | The name of the AWS Glue Schema Registry.                                                                                                                                  | *required* |
| `schema_str`     | `str` | The Avro schema defined as a JSON string (typically generated from a dataclasses-avroschema AvroBaseModel via .avro_schema(); a plain Pydantic model does not provide it). | *required* |
| `**boto3_kwargs` | `Any` | Additional arguments passed directly to the boto3.client initialization (e.g., region_name, aws_access_key_id).                                                            | `{}`       |

Raises:

| Type          | Description                                            |
| ------------- | ------------------------------------------------------ |
| `ImportError` | If the 'boto3' or 'fastavro' package is not installed. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def __init__(self, registry_name: str, schema_str: str, **boto3_kwargs: Any):
    """
    Initializes the AWS Glue Avro serializer.

    The constructor makes no call to Glue. The first `serialize` for a topic finds
    or registers the schema.

    Args:
        registry_name (str): The name of the AWS Glue Schema Registry.
        schema_str (str): The Avro schema defined as a JSON string (typically
            generated from a dataclasses-avroschema `AvroBaseModel` via
            `.avro_schema()`; a plain Pydantic model does not provide it).
        **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
            initialization (e.g., `region_name`, `aws_access_key_id`).

    Raises:
        ImportError: If the 'boto3' or 'fastavro' package is not installed.
    """
    try:
        import boto3  # noqa: F401
        import fastavro  # noqa: F401

        from desflow.connectors._schema_registry import avro
        from desflow.connectors._schema_registry.glue_client import (
            GlueRegistryClient,
            no_references,
        )
        from desflow.connectors._schema_registry.wire import frame_glue
    except ImportError:
        raise ImportError(
            "The 'boto3' and 'fastavro' packages are required. "
            "Install them using: pip install desflow[glue]"
        )

    self._client = GlueRegistryClient(registry_name, boto3_kwargs)
    self._avro = avro
    self._frame = frame_glue
    # Glue finds a version by the exact definition string, so the same schema
    # written with other spacing or key order is the same version.
    self._definition = json.dumps(json.loads(schema_str))
    self._parsed = avro.parse_writer_schema(schema_str, (), no_references)
```

#### Methods:

##### `serialize(topic, data)`

Serializes the given data into an Avro byte string using AWS Glue.

The first call for a topic finds or registers the schema under `<topic>-value`, which calls Glue.

Parameters:

| Name    | Type  | Description                                              | Default    |
| ------- | ----- | -------------------------------------------------------- | ---------- |
| `topic` | `str` | The target Kafka topic.                                  | *required* |
| `data`  | `Any` | The payload to serialize (dictionary or Pydantic model). | *required* |

Returns:

| Name    | Type    | Description                                         |
| ------- | ------- | --------------------------------------------------- |
| `bytes` | `bytes` | The Avro-encoded byte string, with the Glue prefix. |

Raises:

| Type                  | Description                                                        |
| --------------------- | ------------------------------------------------------------------ |
| `SchemaRegistryError` | If a Glue call fails or the new version does not become available. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given data into an Avro byte string using AWS Glue.

    The first call for a topic finds or registers the schema under
    `<topic>-value`, which calls Glue.

    Args:
        topic (str): The target Kafka topic.
        data (Any): The payload to serialize (dictionary or Pydantic model).

    Returns:
        bytes: The Avro-encoded byte string, with the Glue prefix.

    Raises:
        SchemaRegistryError: If a Glue call fails or the new version does not
            become available.
    """
    payload = extract_dict(data)
    version_id = self._client.version_for(
        f"{topic}-value", self._definition, "AVRO"
    )
    return self._frame(
        version_id, self._avro.encode_standard(self._parsed, payload)
    )
```

### `desflow.connectors.egress.kafka.ConfluentJsonSchemaSerializer(registry_url, schema_str, *, auth=UNSET, verify=UNSET, cert=UNSET, timeout=UNSET, headers=UNSET, proxy=UNSET, max_retries=UNSET)`

Lazy-loaded serializer for Confluent Schema Registry using jsonschema and httpx.

This class converts simulation data into JSON in the Confluent wire format. It checks each record against the schema, registers the schema under the subject `<topic>-value` on the first record of each topic, and reuses the schema id after that.

Initializes the Confluent JSON Schema serializer. It makes no request to the registry.

`UNSET` as a default, in the signature or the Default column of the API page, means the argument was not given; each argument's description says its real default.

Parameters:

| Name           | Type                          | Description                                                                                                                              | Default      |
| -------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `registry_url` | `str`                         | The base URL of the Confluent Schema Registry. Several URLs separated by commas are tried in order.                                      | *required*   |
| `schema_str`   | `str`                         | The JSON Schema as a JSON string. A schema with no $schema is checked as Draft 2020-12, as confluent-kafka does.                         | *required*   |
| `auth`         | `Optional[Tuple[str, str]]`   | A username and password for basic authentication, for example ("user", "pass"). The default is None.                                     | `UNSET`      |
| `verify`       | \`bool                        | str                                                                                                                                      | SSLContext\` |
| `cert`         | \`str                         | tuple                                                                                                                                    | None\`       |
| `timeout`      | `Optional[float]`             | Seconds each step of a request to the registry (connecting, sending, reading) may take. The default is 10.0. None waits without a limit. | `UNSET`      |
| `headers`      | `Optional[Mapping[str, str]]` | Headers sent with every request, for example {"Authorization": "Bearer <token>"}. The default is None.                                   | `UNSET`      |
| `proxy`        | `Optional[str]`               | The URL of a proxy. The default is None.                                                                                                 | `UNSET`      |
| `max_retries`  | `int`                         | How many times a request is repeated after a transport error or a status of 408, 429, 500, 502, 503 or 504. The default is 3.            | `UNSET`      |

Raises:

| Type                 | Description                                              |
| -------------------- | -------------------------------------------------------- |
| `ImportError`        | If the 'jsonschema' or 'httpx' package is not installed. |
| `SerializationError` | If the schema is not valid JSON or not a valid schema.   |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def __init__(
    self,
    registry_url: str,
    schema_str: str,
    *,
    auth: Optional[Tuple[str, str]] = UNSET,
    verify: Union[bool, str, ssl.SSLContext] = UNSET,
    cert: Optional[
        Union[
            str,
            Tuple[str, Optional[str]],
            Tuple[str, Optional[str], Optional[str]],
        ]
    ] = UNSET,
    timeout: Optional[float] = UNSET,
    headers: Optional[Mapping[str, str]] = UNSET,
    proxy: Optional[str] = UNSET,
    max_retries: int = UNSET,
):
    """
    Initializes the Confluent JSON Schema serializer. It makes no request to the
    registry.

    `UNSET` as a default, in the signature or the Default column of the API page,
    means the argument was not given; each argument's description says its real
    default.

    Args:
        registry_url (str): The base URL of the Confluent Schema Registry.
            Several URLs separated by commas are tried in order.
        schema_str (str): The JSON Schema as a JSON string. A schema with no
            `$schema` is checked as Draft 2020-12, as confluent-kafka does.
        auth (Optional[Tuple[str, str]]): A username and password for basic
            authentication, for example `("user", "pass")`. The default is None.
        verify (bool | str | ssl.SSLContext): How to check the registry's
            certificate: True (the default), False, the path of a CA file or
            directory, or an SSL context.
        cert (str | tuple | None): A client certificate file, or a tuple of the
            certificate file, the key file and the key password. The default is None.
        timeout (Optional[float]): Seconds each step of a request to the registry
            (connecting, sending, reading) may take. The default is 10.0. None
            waits without a limit.
        headers (Optional[Mapping[str, str]]): Headers sent with every request,
            for example `{"Authorization": "Bearer <token>"}`. The default is None.
        proxy (Optional[str]): The URL of a proxy. The default is None.
        max_retries (int): How many times a request is repeated after a
            transport error or a status of 408, 429, 500, 502, 503 or 504. The
            default is 3.

    Raises:
        ImportError: If the 'jsonschema' or 'httpx' package is not installed.
        SerializationError: If the schema is not valid JSON or not a valid schema.
    """
    try:
        import httpx  # noqa: F401
        import jsonschema  # noqa: F401

        from desflow.connectors._schema_registry import json_schema
        from desflow.connectors._schema_registry.confluent_client import (
            ConfluentRegistryClient,
            given_arguments,
        )
        from desflow.connectors._schema_registry.wire import frame_confluent
    except ImportError:
        raise ImportError(
            "The 'jsonschema' and 'httpx' packages are required to use "
            "ConfluentJsonSchemaSerializer. "
            "Install them using: pip install desflow[confluent]"
        )

    options = given_arguments(
        auth=auth,
        verify=verify,
        cert=cert,
        timeout=timeout,
        headers=headers,
        proxy=proxy,
        max_retries=max_retries,
    )
    self._client = ConfluentRegistryClient(registry_url, **options)
    self._json_schema = json_schema
    self._frame = frame_confluent
    self._schema_str = schema_str
    self._validator = json_schema.validator(json_schema.load(schema_str))
```

#### Methods:

##### `serialize(topic, data)`

Serializes the given data into a JSON byte string.

The first call for a topic registers the schema under `<topic>-value`, which makes a request to the registry.

Parameters:

| Name    | Type  | Description                                              | Default    |
| ------- | ----- | -------------------------------------------------------- | ---------- |
| `topic` | `str` | The target Kafka topic, used for schema subject naming.  | *required* |
| `data`  | `Any` | The payload to serialize (dictionary or Pydantic model). | *required* |

Returns:

| Name    | Type    | Description                                |
| ------- | ------- | ------------------------------------------ |
| `bytes` | `bytes` | The JSON bytes, with the Confluent prefix. |

Raises:

| Type                  | Description                              |
| --------------------- | ---------------------------------------- |
| `SerializationError`  | If the record does not match the schema. |
| `SchemaRegistryError` | If the registry refuses the schema.      |
| `TransportError`      | If no registry URL could be reached.     |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given data into a JSON byte string.

    The first call for a topic registers the schema under `<topic>-value`, which
    makes a request to the registry.

    Args:
        topic (str): The target Kafka topic, used for schema subject naming.
        data (Any): The payload to serialize (dictionary or Pydantic model).

    Returns:
        bytes: The JSON bytes, with the Confluent prefix.

    Raises:
        SerializationError: If the record does not match the schema.
        SchemaRegistryError: If the registry refuses the schema.
        httpx.TransportError: If no registry URL could be reached.
    """
    body = self._json_schema.encode(self._validator, extract_dict(data))
    schema_id = self._client.register(f"{topic}-value", self._schema_str, "JSON")
    return self._frame(schema_id, body)
```

### `desflow.connectors.egress.kafka.ConfluentProtobufSerializer(registry_url, message_type, *, auth=UNSET, verify=UNSET, cert=UNSET, timeout=UNSET, headers=UNSET, proxy=UNSET, max_retries=UNSET)`

Lazy-loaded serializer for Confluent Schema Registry using protobuf and httpx.

This class converts simulation data into Protobuf messages in the Confluent wire format. The schema is the file of the message class. On the first record of each topic it registers the files that the schema imports, then the schema under the subject `<topic>-value`, and it reuses the schema id after that.

Initializes the Confluent Protobuf serializer. It makes no request to the registry.

`UNSET` as a default, in the signature or the Default column of the API page, means the argument was not given; each argument's description says its real default.

Parameters:

| Name           | Type                          | Description                                                                                                                              | Default      |
| -------------- | ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `registry_url` | `str`                         | The base URL of the Confluent Schema Registry. Several URLs separated by commas are tried in order.                                      | *required*   |
| `message_type` | `type[Message]`               | The generated Protobuf message class. Its file is registered as the schema.                                                              | *required*   |
| `auth`         | `Optional[Tuple[str, str]]`   | A username and password for basic authentication, for example ("user", "pass"). The default is None.                                     | `UNSET`      |
| `verify`       | \`bool                        | str                                                                                                                                      | SSLContext\` |
| `cert`         | \`str                         | tuple                                                                                                                                    | None\`       |
| `timeout`      | `Optional[float]`             | Seconds each step of a request to the registry (connecting, sending, reading) may take. The default is 10.0. None waits without a limit. | `UNSET`      |
| `headers`      | `Optional[Mapping[str, str]]` | Headers sent with every request, for example {"Authorization": "Bearer <token>"}. The default is None.                                   | `UNSET`      |
| `proxy`        | `Optional[str]`               | The URL of a proxy. The default is None.                                                                                                 | `UNSET`      |
| `max_retries`  | `int`                         | How many times a request is repeated after a transport error or a status of 408, 429, 500, 502, 503 or 504. The default is 3.            | `UNSET`      |

Raises:

| Type          | Description                                            |
| ------------- | ------------------------------------------------------ |
| `ImportError` | If the 'protobuf' or 'httpx' package is not installed. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def __init__(
    self,
    registry_url: str,
    message_type: Any,
    *,
    auth: Optional[Tuple[str, str]] = UNSET,
    verify: Union[bool, str, ssl.SSLContext] = UNSET,
    cert: Optional[
        Union[
            str,
            Tuple[str, Optional[str]],
            Tuple[str, Optional[str], Optional[str]],
        ]
    ] = UNSET,
    timeout: Optional[float] = UNSET,
    headers: Optional[Mapping[str, str]] = UNSET,
    proxy: Optional[str] = UNSET,
    max_retries: int = UNSET,
):
    """
    Initializes the Confluent Protobuf serializer. It makes no request to the
    registry.

    `UNSET` as a default, in the signature or the Default column of the API page,
    means the argument was not given; each argument's description says its real
    default.

    Args:
        registry_url (str): The base URL of the Confluent Schema Registry.
            Several URLs separated by commas are tried in order.
        message_type (type[Message]): The generated Protobuf message class. Its
            file is registered as the schema.
        auth (Optional[Tuple[str, str]]): A username and password for basic
            authentication, for example `("user", "pass")`. The default is None.
        verify (bool | str | ssl.SSLContext): How to check the registry's
            certificate: True (the default), False, the path of a CA file or
            directory, or an SSL context.
        cert (str | tuple | None): A client certificate file, or a tuple of the
            certificate file, the key file and the key password. The default is None.
        timeout (Optional[float]): Seconds each step of a request to the registry
            (connecting, sending, reading) may take. The default is 10.0. None
            waits without a limit.
        headers (Optional[Mapping[str, str]]): Headers sent with every request,
            for example `{"Authorization": "Bearer <token>"}`. The default is None.
        proxy (Optional[str]): The URL of a proxy. The default is None.
        max_retries (int): How many times a request is repeated after a
            transport error or a status of 408, 429, 500, 502, 503 or 504. The
            default is 3.

    Raises:
        ImportError: If the 'protobuf' or 'httpx' package is not installed.
    """
    try:
        import google.protobuf  # noqa: F401
        import httpx  # noqa: F401

        from desflow.connectors._schema_registry import protobuf
        from desflow.connectors._schema_registry.confluent_client import (
            ConfluentRegistryClient,
            given_arguments,
        )
        from desflow.connectors._schema_registry.wire import frame_confluent
    except ImportError:
        raise ImportError(
            "The 'protobuf' and 'httpx' packages are required to use "
            "ConfluentProtobufSerializer. "
            "Install them using: pip install desflow[confluent]"
        )

    options = given_arguments(
        auth=auth,
        verify=verify,
        cert=cert,
        timeout=timeout,
        headers=headers,
        proxy=proxy,
        max_retries=max_retries,
    )
    self._client = ConfluentRegistryClient(registry_url, **options)
    self._protobuf = protobuf
    self._frame = frame_confluent
    self._message_type = message_type
    self._schema_str = protobuf.file_descriptor_b64(message_type.DESCRIPTOR.file)
    self._indexes = protobuf.confluent_indexes(message_type.DESCRIPTOR)
    # The files the schema imports, registered on the first record.
    self._references: Optional[list] = None
    self._lock = threading.Lock()
```

#### Methods:

##### `serialize(topic, data)`

Serializes the given data into a Protobuf byte string.

The first call registers the imported files and, for a topic, the schema under `<topic>-value`, which makes requests to the registry.

Parameters:

| Name    | Type  | Description                                                                                                     | Default    |
| ------- | ----- | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `topic` | `str` | The target Kafka topic, used for schema subject naming.                                                         | *required* |
| `data`  | `Any` | An instance of the message class, or a dictionary or Pydantic model. A key the schema does not have is dropped. | *required* |

Returns:

| Name    | Type    | Description                                                      |
| ------- | ------- | ---------------------------------------------------------------- |
| `bytes` | `bytes` | The Protobuf bytes, with the Confluent prefix and message index. |

Raises:

| Type                  | Description                                   |
| --------------------- | --------------------------------------------- |
| `SerializationError`  | If the record does not fit the message class. |
| `SchemaRegistryError` | If the registry refuses a schema.             |
| `TransportError`      | If no registry URL could be reached.          |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given data into a Protobuf byte string.

    The first call registers the imported files and, for a topic, the schema under
    `<topic>-value`, which makes requests to the registry.

    Args:
        topic (str): The target Kafka topic, used for schema subject naming.
        data (Any): An instance of the message class, or a dictionary or Pydantic
            model. A key the schema does not have is dropped.

    Returns:
        bytes: The Protobuf bytes, with the Confluent prefix and message index.

    Raises:
        SerializationError: If the record does not fit the message class.
        SchemaRegistryError: If the registry refuses a schema.
        httpx.TransportError: If no registry URL could be reached.
    """
    body = self._protobuf.encode(self._message_type, extract_dict(data))
    schema_id = self._client.register(
        f"{topic}-value", self._schema_str, "PROTOBUF", self._imports()
    )
    return self._frame(schema_id, self._indexes + body)
```

### `desflow.connectors.egress.kafka.GlueJsonSchemaSerializer(registry_name, schema_str, **boto3_kwargs)`

Lazy-loaded serializer for AWS Glue Schema Registry using boto3 and fastjsonschema.

This class converts simulation data into JSON with the AWS Glue prefix. It checks each record against the schema as aws-glue-schema-registry 1.1.3 did, so a missing property that has a `default` is filled in.

Initializes the AWS Glue JSON Schema serializer.

The constructor makes no call to Glue. The first `serialize` for a topic finds or registers the schema.

Parameters:

| Name             | Type  | Description                                                                                                     | Default    |
| ---------------- | ----- | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_name`  | `str` | The name of the AWS Glue Schema Registry.                                                                       | *required* |
| `schema_str`     | `str` | The JSON Schema as a JSON string.                                                                               | *required* |
| `**boto3_kwargs` | `Any` | Additional arguments passed directly to the boto3.client initialization (e.g., region_name, aws_access_key_id). | `{}`       |

Raises:

| Type                 | Description                                                  |
| -------------------- | ------------------------------------------------------------ |
| `ImportError`        | If the 'boto3' or 'fastjsonschema' package is not installed. |
| `SerializationError` | If the schema is not valid JSON or cannot be compiled.       |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def __init__(self, registry_name: str, schema_str: str, **boto3_kwargs: Any):
    """
    Initializes the AWS Glue JSON Schema serializer.

    The constructor makes no call to Glue. The first `serialize` for a topic finds
    or registers the schema.

    Args:
        registry_name (str): The name of the AWS Glue Schema Registry.
        schema_str (str): The JSON Schema as a JSON string.
        **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
            initialization (e.g., `region_name`, `aws_access_key_id`).

    Raises:
        ImportError: If the 'boto3' or 'fastjsonschema' package is not installed.
        SerializationError: If the schema is not valid JSON or cannot be compiled.
    """
    try:
        import boto3  # noqa: F401
        import fastjsonschema  # noqa: F401

        from desflow.connectors._schema_registry import glue_json
        from desflow.connectors._schema_registry.glue_client import (
            GlueRegistryClient,
        )
        from desflow.connectors._schema_registry.wire import frame_glue
    except ImportError:
        raise ImportError(
            "The 'boto3' and 'fastjsonschema' packages are required. "
            "Install them using: pip install desflow[glue]"
        )

    self._client = GlueRegistryClient(registry_name, boto3_kwargs)
    self._glue_json = glue_json
    self._frame = frame_glue
    try:
        schema = json.loads(schema_str)
    except ValueError as error:
        raise SerializationError(
            f"The JSON Schema is not valid JSON: {error}"
        ) from error
    # aws-glue-schema-registry 1.1.3 sent the schema as orjson wrote it.
    self._definition = orjson.dumps(schema).decode()
    self._validate = glue_json.validator(self._definition)
```

#### Methods:

##### `serialize(topic, data)`

Serializes the given data into a JSON byte string using AWS Glue.

The first call for a topic finds or registers the schema under `<topic>-value`, which calls Glue.

Parameters:

| Name    | Type  | Description                                              | Default    |
| ------- | ----- | -------------------------------------------------------- | ---------- |
| `topic` | `str` | The target Kafka topic.                                  | *required* |
| `data`  | `Any` | The payload to serialize (dictionary or Pydantic model). | *required* |

Returns:

| Name    | Type    | Description                           |
| ------- | ------- | ------------------------------------- |
| `bytes` | `bytes` | The JSON bytes, with the Glue prefix. |

Raises:

| Type                  | Description                                                        |
| --------------------- | ------------------------------------------------------------------ |
| `SerializationError`  | If the record does not match the schema.                           |
| `SchemaRegistryError` | If a Glue call fails or the new version does not become available. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given data into a JSON byte string using AWS Glue.

    The first call for a topic finds or registers the schema under
    `<topic>-value`, which calls Glue.

    Args:
        topic (str): The target Kafka topic.
        data (Any): The payload to serialize (dictionary or Pydantic model).

    Returns:
        bytes: The JSON bytes, with the Glue prefix.

    Raises:
        SerializationError: If the record does not match the schema.
        SchemaRegistryError: If a Glue call fails or the new version does not
            become available.
    """
    body = self._glue_json.encode(self._validate, extract_dict(data))
    version_id = self._client.version_for(
        f"{topic}-value", self._definition, "JSON"
    )
    return self._frame(version_id, body)
```

### `desflow.connectors.egress.kafka.GlueProtobufSerializer(registry_name, message_type, schema_str, **boto3_kwargs)`

Lazy-loaded serializer for AWS Glue Schema Registry using boto3 and protobuf.

This class converts simulation data into Protobuf messages with the AWS Glue prefix and message index. Glue stores the schema as `.proto` text, which the caller passes in, because protobuf cannot print it from the message class.

Initializes the AWS Glue Protobuf serializer.

The constructor makes no call to Glue. The first `serialize` for a topic finds or registers the schema.

Parameters:

| Name             | Type            | Description                                                                                                                                                                   | Default    |
| ---------------- | --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_name`  | `str`           | The name of the AWS Glue Schema Registry.                                                                                                                                     | *required* |
| `message_type`   | `type[Message]` | The generated Protobuf message class.                                                                                                                                         | *required* |
| `schema_str`     | `str`           | The text of the .proto file that the class was generated from. It is registered as the schema as it is, so it must declare the same messages, in the same file, as the class. | *required* |
| `**boto3_kwargs` | `Any`           | Additional arguments passed directly to the boto3.client initialization (e.g., region_name, aws_access_key_id).                                                               | `{}`       |

Raises:

| Type          | Description                                            |
| ------------- | ------------------------------------------------------ |
| `ImportError` | If the 'boto3' or 'protobuf' package is not installed. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def __init__(
    self,
    registry_name: str,
    message_type: Any,
    schema_str: str,
    **boto3_kwargs: Any,
):
    """
    Initializes the AWS Glue Protobuf serializer.

    The constructor makes no call to Glue. The first `serialize` for a topic finds
    or registers the schema.

    Args:
        registry_name (str): The name of the AWS Glue Schema Registry.
        message_type (type[Message]): The generated Protobuf message class.
        schema_str (str): The text of the `.proto` file that the class was
            generated from. It is registered as the schema as it is, so it must
            declare the same messages, in the same file, as the class.
        **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
            initialization (e.g., `region_name`, `aws_access_key_id`).

    Raises:
        ImportError: If the 'boto3' or 'protobuf' package is not installed.
    """
    try:
        import boto3  # noqa: F401
        import google.protobuf  # noqa: F401

        from desflow.connectors._schema_registry import protobuf
        from desflow.connectors._schema_registry.glue_client import (
            GlueRegistryClient,
        )
        from desflow.connectors._schema_registry.wire import frame_glue
    except ImportError:
        raise ImportError(
            "The 'boto3' and 'protobuf' packages are required. "
            "Install them using: pip install desflow[glue]"
        )

    self._client = GlueRegistryClient(registry_name, boto3_kwargs)
    self._protobuf = protobuf
    self._frame = frame_glue
    self._message_type = message_type
    self._definition = schema_str
    self._index = protobuf.glue_index(message_type.DESCRIPTOR)
```

#### Methods:

##### `serialize(topic, data)`

Serializes the given data into a Protobuf byte string using AWS Glue.

The first call for a topic finds or registers the schema under `<topic>-value`, which calls Glue.

Parameters:

| Name    | Type  | Description                                                                                                     | Default    |
| ------- | ----- | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `topic` | `str` | The target Kafka topic.                                                                                         | *required* |
| `data`  | `Any` | An instance of the message class, or a dictionary or Pydantic model. A key the schema does not have is dropped. | *required* |

Returns:

| Name    | Type    | Description                                                 |
| ------- | ------- | ----------------------------------------------------------- |
| `bytes` | `bytes` | The Protobuf bytes, with the Glue prefix and message index. |

Raises:

| Type                  | Description                                                        |
| --------------------- | ------------------------------------------------------------------ |
| `SerializationError`  | If the record does not fit the message class.                      |
| `SchemaRegistryError` | If a Glue call fails or the new version does not become available. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given data into a Protobuf byte string using AWS Glue.

    The first call for a topic finds or registers the schema under
    `<topic>-value`, which calls Glue.

    Args:
        topic (str): The target Kafka topic.
        data (Any): An instance of the message class, or a dictionary or Pydantic
            model. A key the schema does not have is dropped.

    Returns:
        bytes: The Protobuf bytes, with the Glue prefix and message index.

    Raises:
        SerializationError: If the record does not fit the message class.
        SchemaRegistryError: If a Glue call fails or the new version does not
            become available.
    """
    body = self._protobuf.encode(self._message_type, extract_dict(data))
    version_id = self._client.version_for(
        f"{topic}-value", self._definition, "PROTOBUF"
    )
    return self._frame(version_id, self._index + body)
```

### `desflow.connectors.egress.kafka.SchemaRegistryError(http_status, error_code, message)`

Bases: `RuntimeError`

A schema registry call failed, or its answer was an error or an unusable version.

Attributes:

| Name          | Type  | Description                                                                                                                                                              |
| ------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `http_status` | `int` | The HTTP status of the response, or -1 when the call failed before the registry answered (credentials, endpoint) or the answer was a schema version that cannot be used. |
| `error_code`  | \`int | str\`                                                                                                                                                                    |
| `message`     | `str` | The registry's message, the raw body when it was not JSON, or desflow's description of the failure.                                                                      |

Source code in `src/desflow/connectors/_schema_registry/errors.py`

```python
def __init__(self, http_status: int, error_code: int | str, message: str):
    self.http_status = http_status
    self.error_code = error_code
    self.message = message
    super().__init__(
        f"Schema registry error {http_status} ({error_code}): {message}"
    )
```

### `desflow.connectors.egress.kafka.SerializationError`

Bases: `ValueError`

A payload or a schema cannot be written or read.

Raised for data too short to hold the registry's framing, a magic byte that is not known, a schema of a format the class does not handle, and a schema with rules that desflow does not run.

## Generated Values

### `desflow.values.Template(payload, name=None, vars=None, vars_once=None, check=True)`

A payload of generated values, for a builder or low-level script.

Return it from a task function and the task renders it when the finished event is published, or call `render` in your own code. It is the same mapping a blueprint's `payload` takes: a mapping whose values may be generators (`{"gen": "uniform", "low": 2, "high": 200}`), or one generator that gives a mapping or null. A null gives an event whose value is null, which a Kafka sink with `envelope` off writes as a tombstone.

Example

```python
order = Template(
    {"order_id": {"gen": "uuid"}, "amount": {"gen": "uniform", "low": 2, "high": 200}},
    name="place_order",
)

@app.task(service_id="packing", resource_id="packers")
def place_order(task_id):
    return order
```

Attributes:

| Name            | Type             | Description                                                                                                          |
| --------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------- |
| `name`          | `Optional[str]`  | What the template is called; it names the random sequences of its generators, so give two templates different names. |
| `payload`       | `Any`            | The validated mapping.                                                                                               |
| `vars`          | `Dict[str, Any]` | The named values a vars key would hold, evaluated before the payload.                                                |
| `vars_once`     | `Dict[str, Any]` | The same, evaluated once for each entity.                                                                            |
| `uses_previous` | `bool`           | Whether a generator in it is a previous, so the task that renders it keeps its last payload.                         |
| `looks_up`      | `Set[str]`       | The tasks a lookup in it names. A task that renders it does not go on before each of them has a row.                 |

Parameters:

| Name        | Type                          | Description                                                                                                           | Default    |
| ----------- | ----------------------------- | --------------------------------------------------------------------------------------------------------------------- | ---------- |
| `payload`   | `Any`                         | A mapping, or one generator that gives a mapping or null.                                                             | *required* |
| `name`      | `Optional[str]`               | What the template is called.                                                                                          | `None`     |
| `vars`      | `Optional[Mapping[str, Any]]` | Named values, each seeing the ones before it by name.                                                                 | `None`     |
| `vars_once` | `Optional[Mapping[str, Any]]` | Named values made once for each entity.                                                                               | `None`     |
| `check`     | `bool`                        | Whether to check the generators now, such as that a row file exists; a blueprint has checked them when it was loaded. | `True`     |

Raises:

| Type              | Description                                                                                                              |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `ValidationError` | If a generator or a mapping is not valid.                                                                                |
| `ValueError`      | If a check finds a problem, such as a row file that does not exist (a relative path is read from the working directory). |

Source code in `src/desflow/values/template.py`

```python
def __init__(
    self,
    payload: Any,
    name: Optional[str] = None,
    vars: Optional[Mapping[str, Any]] = None,
    vars_once: Optional[Mapping[str, Any]] = None,
    check: bool = True,
):
    """
    Args:
        payload: A mapping, or one generator that gives a mapping or null.
        name: What the template is called.
        vars: Named values, each seeing the ones before it by name.
        vars_once: Named values made once for each entity.
        check: Whether to check the generators now, such as that a `row` file
            exists; a blueprint has checked them when it was loaded.

    Raises:
        pydantic.ValidationError: If a generator or a mapping is not valid.
        ValueError: If a check finds a problem, such as a `row` file that does not
            exist (a relative path is read from the working directory).
    """
    # An unnamed template is numbered in creation order, so two never share a
    # random sequence or a counter. The first run that renders it numbers it from
    # the start of its build (`_renumber`), so two builds of one code give one key.
    self.name = name
    self._serial: Optional[int] = None if name else _next_serial()
    self._renumbered = name is not None
    self.key: str = name or f"template{self._serial}"
    base = (self.key,)
    self.payload: Any = payload_adapter().validate_python(payload)
    self.vars: Dict[str, Any] = values_adapter().validate_python(dict(vars or {}))
    self.vars_once: Dict[str, Any] = values_adapter().validate_python(
        dict(vars_once or {})
    )
    bind(self.payload, base + ("payload",), overwrite=False)
    bind(self.vars, base + ("vars",), overwrite=False)
    bind(self.vars_once, base + ("vars_once",), overwrite=False)
    # Whether a task that renders this template keeps its last payload for it
    self.uses_previous: bool = any(
        True
        for place in (self.payload, self.vars, self.vars_once)
        for _ in generators_in(place, (Previous,))
    )
    self.looks_up: Set[str] = {
        node.task
        for place in (self.payload, self.vars, self.vars_once)
        for _, node in generators_in(place, (Lookup,))
    }
    if check:
        self._check(base)
```

#### Methods:

##### `bind(entity=None, **fields)`

The template with an entity and fixed fields, rendered by the task that publishes it. `fields` are written over the generated ones.

Parameters:

| Name       | Type  | Description                                    | Default |
| ---------- | ----- | ---------------------------------------------- | ------- |
| `entity`   | `Any` | The entity whose attributes are read, or None. | `None`  |
| `**fields` | `Any` | Fields written over the generated ones.        | `{}`    |

Source code in `src/desflow/values/template.py`

```python
def bind(self, entity: Any = None, **fields: Any) -> "BoundPayload":
    """The template with an entity and fixed fields, rendered by the task that
    publishes it. `fields` are written over the generated ones.

    Args:
        entity: The entity whose attributes are read, or None.
        **fields: Fields written over the generated ones.
    """
    return BoundPayload(self, entity, fields)
```

##### `render(context, entity=None)`

Evaluates the template at the simulation time now.

Parameters:

| Name      | Type  | Description                                              | Default    |
| --------- | ----- | -------------------------------------------------------- | ---------- |
| `context` | `Any` | The SimulationContext of the run.                        | *required* |
| `entity`  | `Any` | The entity whose attributes attr reads; none by default. | `None`     |

Returns:

| Type                       | Description                                                                 |
| -------------------------- | --------------------------------------------------------------------------- |
| `Optional[Dict[str, Any]]` | Optional\[Dict[str, Any]\]: The mapping, as plain Python data, or None when |
| `Optional[Dict[str, Any]]` | the payload is one generator that gave null.                                |

Source code in `src/desflow/values/template.py`

```python
def render(self, context: Any, entity: Any = None) -> Optional[Dict[str, Any]]:
    """Evaluates the template at the simulation time now.

    Args:
        context: The `SimulationContext` of the run.
        entity: The entity whose attributes `attr` reads; none by default.

    Returns:
        Optional[Dict[str, Any]]: The mapping, as plain Python data, or None when
        the payload is one generator that gave null.
    """
    return _runtime(context).render(self, entity)
```

### `desflow.values.BoundPayload(template, entity=None, fields=None, attributes=None)`

A `Template` bound to an entity and to fields that overwrite generated ones, such as the `id_field` of a task. A task renders it where it would publish a plain payload.

Attributes:

| Name         | Type             | Description                                                                                                   |
| ------------ | ---------------- | ------------------------------------------------------------------------------------------------------------- |
| `template`   | `Template`       | The template.                                                                                                 |
| `entity`     |                  | The entity whose attributes attr reads, or None for the task's own.                                           |
| `fields`     | `Dict[str, Any]` | Fields written over the generated mapping.                                                                    |
| `attributes` | `List[str]`      | Names of entity attributes written over the mapping, each under its own name, read when the payload is built. |

Source code in `src/desflow/values/template.py`

```python
def __init__(
    self,
    template: Template,
    entity: Any = None,
    fields: Optional[Mapping[str, Any]] = None,
    attributes: Optional[List[str]] = None,
):
    self.template = template
    self.entity = entity
    self.fields: Dict[str, Any] = dict(fields or {})
    self.attributes: List[str] = list(attributes or [])
```

#### Methods:

##### `render(context)`

Evaluates the template, then writes `fields` over it.

Parameters:

| Name      | Type  | Description                                          | Default    |
| --------- | ----- | ---------------------------------------------------- | ---------- |
| `context` | `Any` | The simulation context that holds the value runtime. | *required* |

Returns:

| Type                       | Description                                                             |
| -------------------------- | ----------------------------------------------------------------------- |
| `Optional[Dict[str, Any]]` | Optional\[Dict[str, Any]\]: The mapping, or None when the template gave |
| `Optional[Dict[str, Any]]` | null and there are no fields or attributes to write over it.            |

Raises:

| Type                   | Description                                                                                                                        |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `ValueGenerationError` | If the template gives something other than a mapping or null, or gives null while there are fields or attributes to write over it. |

Source code in `src/desflow/values/template.py`

```python
def render(self, context: Any) -> Optional[Dict[str, Any]]:
    """Evaluates the template, then writes `fields` over it.

    Args:
        context: The simulation context that holds the value runtime.

    Returns:
        Optional[Dict[str, Any]]: The mapping, or None when the template gave
        null and there are no fields or attributes to write over it.

    Raises:
        ValueGenerationError: If the template gives something other than a
            mapping or null, or gives null while there are fields or attributes
            to write over it.
    """
    values = _runtime(context)
    return values.render(
        self.template, self.entity, extras=self.fields, attributes=self.attributes
    )
```

### `desflow.values.generator(spec)`

Validates one generator written as a mapping, such as `{"gen": "uniform", "low": 2, "high": 200}`, and gives the typed object.

Pass the result where a builder method takes a value, such as `attributes` of `add_arrival`, `items` of `add_store`, `set` of a scenario step, or the `value` of an `on` action. The place gives it its random sequence when it is added. A `row` is read from the working directory when the place is added, and a missing file raises `ValueError` there.

Parameters:

| Name   | Type  | Description                 | Default    |
| ------ | ----- | --------------------------- | ---------- |
| `spec` | `Any` | The generator as a mapping. | *required* |

Raises:

| Type              | Description                    |
| ----------------- | ------------------------------ |
| `ValidationError` | If the generator is not valid. |

Source code in `src/desflow/values/template.py`

```python
def generator(spec: Any) -> Any:
    """Validates one generator written as a mapping, such as `{"gen": "uniform",
    "low": 2, "high": 200}`, and gives the typed object.

    Pass the result where a builder method takes a value, such as `attributes` of
    `add_arrival`, `items` of `add_store`, `set` of a scenario step, or the `value`
    of an `on` action. The place gives it its random sequence when it is added.
    A `row` is read from the working directory when the place is added, and a missing
    file raises `ValueError` there.

    Args:
        spec: The generator as a mapping.

    Raises:
        pydantic.ValidationError: If the generator is not valid.
    """
    return generator_adapter().validate_python(spec)
```

### `desflow.values.ValueGenerationError`

Bases: `ValueError`

A generator could not give its value: a bad argument, a missing var, a draw from an empty choice. The message names the place in the file.

### `desflow.values.ValueRuntime(seed, env, sampler=None, context=None, live_scope=None, stop=None)`

The state the generators of one run share, and the means to evaluate them.

Attributes:

| Name      | Type            | Description                                                                                   |
| --------- | --------------- | --------------------------------------------------------------------------------------------- |
| `seed`    | `Optional[int]` | The seed every generator's random sequence comes from; None gives a different run each time.  |
| `context` |                 | The SimulationContext, which call passes to a function and sample of a service draws through. |

Parameters:

| Name         | Type                           | Description                                                                                                       | Default    |
| ------------ | ------------------------------ | ----------------------------------------------------------------------------------------------------------------- | ---------- |
| `seed`       | `Optional[int]`                | The run's seed, or None.                                                                                          | *required* |
| `env`        | `Any`                          | The environment, for the simulation clock, its start and its zone.                                                | *required* |
| `sampler`    | `Any`                          | The run's Sampler, for sample of a service.                                                                       | `None`     |
| `context`    | `Any`                          | The SimulationContext.                                                                                            | `None`     |
| `live_scope` | `Optional[LiveScope]`          | The run's LiveScope, for the scope a generator reads outside a task; None gives a scope of the environment alone. | `None`     |
| `stop`       | `Optional[Callable[[], None]]` | Stops the run, for a replay whose rows end with on_end: stop; None stops the environment.                         | `None`     |

Source code in `src/desflow/values/runtime.py`

```python
def __init__(
    self,
    seed: Optional[int],
    env: Any,
    sampler: Any = None,
    context: Any = None,
    live_scope: Optional["LiveScope"] = None,
    stop: Optional[Callable[[], None]] = None,
):
    """
    Args:
        seed: The run's seed, or None.
        env: The environment, for the simulation clock, its start and its zone.
        sampler: The run's `Sampler`, for `sample` of a service.
        context: The `SimulationContext`.
        live_scope: The run's `LiveScope`, for the scope a generator reads
            outside a task; None gives a scope of the environment alone.
        stop: Stops the run, for a replay whose rows end with `on_end: stop`;
            None stops the environment.
    """
    self.seed = seed
    self.env = env
    self.sampler = sampler
    self.context = context
    self._live = live_scope
    self._stop = stop
    # With no seed the sequences still differ from node to node
    self._entropy = seed if seed is not None else np.random.SeedSequence().entropy
    self._streams: Dict[str, np.random.Generator] = {}
    self._counters: Dict[str, int] = {}
    self._free = 0
    from desflow.values.template import start_window

    # The count of unnamed templates made before this run's build began
    self._template_start = start_window()
    self._once: Dict[Any, Dict[str, Dict[str, Any]]] = {}
    self._seen: Dict[str, Set[str]] = {}
    self._fakers: Dict[str, Any] = {}
    self._rows: Dict[str, "_RowCursor"] = {}
    self._expressions: Dict[str, Expression] = {}
    # The finished payloads of the tasks that something looks up, by task
    self._kept: Dict[str, "_Rows"] = {}
    # The tasks whose last payload `previous` reads, and that payload by stream
    self._previous_tasks: Set[str] = set()
    self._last: Dict[Tuple[str, Any], Mapping[str, Any]] = {}
    # What each `repeat` being evaluated has drawn with `distinct`, innermost last
    self._frames: List[Dict[str, Set[int]]] = []
    # For each task whose start was refused for want of a row: when the first was
    # and how many there have been
    self._refused: Dict[str, List[Any]] = {}
```

#### Attributes

##### `zone`

tzinfo: The run's time zone.

#### Methods:

##### `aggregate(node, scope)`

The value of an `aggregate`.

Parameters:

| Name    | Type        | Description                     | Default    |
| ------- | ----------- | ------------------------------- | ---------- |
| `node`  | `Aggregate` | The generator.                  | *required* |
| `scope` | `Scope`     | What the expressions here read. | *required* |

Returns:

| Name  | Type  | Description                                                |
| ----- | ----- | ---------------------------------------------------------- |
| `Any` | `Any` | The mean, sum, minimum, maximum or count of the rows kept. |

Raises:

| Type         | Description                                                                                                                 |
| ------------ | --------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If where or the path cannot be read from a row, a value is not a number, or there is no row for a mean, minimum or maximum. |

Source code in `src/desflow/values/runtime.py`

```python
def aggregate(self, node: Aggregate, scope: Scope) -> Any:
    """The value of an `aggregate`.

    Args:
        node: The generator.
        scope: What the expressions here read.

    Returns:
        Any: The mean, sum, minimum, maximum or count of the rows kept.

    Raises:
        ValueError: If `where` or the path cannot be read from a row, a value is not
            a number, or there is no row for a mean, minimum or maximum.
    """
    rows = self._rows_of(node, node.task)
    expression = None
    names: Dict[str, Any] = {}
    loops: Dict[str, Any] = {}
    reading = scope
    if node.where is not None:
        expression = self._expressions.get(node.where)
        if expression is None:
            expression = self._expressions[node.where] = Expression(
                node.where, names=ANY_NAME
            )
        # A field of the row is read before any reserved name, such as `t`
        reading = replace(scope, names=names, loops=loops)
    values: List[Any] = []
    for _, row in rows.newest():
        if expression is not None:
            names.clear()
            names.update(scope.names)
            names.update(row)
            loops.clear()
            loops.update(scope.loops)
            loops.update(row)
            try:
                if not expression.evaluate(reading):
                    continue
            except ValueError as exc:
                raise node.fail(f"where: {exc}") from None
        if node.function == "count":
            values.append(1)
        else:
            try:
                values.append(part_of(row, node.path))
            except KeyError as exc:
                raise node.fail(f"task '{node.task}': {exc.args[0]}") from None
        if len(values) == node.last:
            break
    return self._reduce(node, values)
```

##### `condition(node, text, scope)`

Whether an expression written in a generator holds.

Parameters:

| Name    | Type    | Description                             | Default    |
| ------- | ------- | --------------------------------------- | ---------- |
| `node`  | `_Gen`  | The generator, or the value, to act on. | *required* |
| `text`  | `str`   | The expression.                         | *required* |
| `scope` | `Scope` | What the expressions here read.         | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def condition(self, node: _Gen, text: str, scope: Scope) -> bool:
    """Whether an expression written in a generator holds.

    Args:
        node: The generator, or the value, to act on.
        text: The expression.
        scope: What the expressions here read.
    """
    expression = self._expressions.get(text)
    if expression is None:
        expression = self._expressions[text] = Expression(text, names=ANY_NAME)
    try:
        return bool(expression.evaluate(scope))
    except ValueError as exc:
        raise node.fail(str(exc)) from None
```

##### `copy(value)`

A mapping or list copied, so a payload never shares a var or an attribute with the entity.

Parameters:

| Name    | Type  | Description | Default    |
| ------- | ----- | ----------- | ---------- |
| `value` | `Any` | The value.  | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def copy(self, value: Any) -> Any:
    """A mapping or list copied, so a payload never shares a var or an attribute
    with the entity.

    Args:
        value: The value.
    """
    return copy.deepcopy(value) if isinstance(value, (dict, list)) else value
```

##### `count(node)`

How many times `node` has been asked for a count; the call counts.

Parameters:

| Name   | Type   | Description                             | Default    |
| ------ | ------ | --------------------------------------- | ---------- |
| `node` | `_Gen` | The generator, or the value, to act on. | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def count(self, node: _Gen) -> int:
    """How many times `node` has been asked for a count; the call counts.

    Args:
        node: The generator, or the value, to act on.
    """
    done = self._counters.get(node._path, 0)
    self._counters[node._path] = done + 1
    return done
```

##### `distinct_frame()`

Opens what a `repeat` is drawing without replacement: a `lookup` with `distinct` inside it does not give the same row twice until the `repeat` ends.

Source code in `src/desflow/values/runtime.py`

```python
@contextmanager
def distinct_frame(self) -> Iterator[None]:
    """Opens what a `repeat` is drawing without replacement: a `lookup` with
    `distinct` inside it does not give the same row twice until the `repeat` ends."""
    self._frames.append({})
    try:
        yield
    finally:
        self._frames.pop()
```

##### `draw_service(node, service)`

One draw from a service's live distribution.

Parameters:

| Name      | Type   | Description                             | Default    |
| --------- | ------ | --------------------------------------- | ---------- |
| `node`    | `_Gen` | The generator, or the value, to act on. | *required* |
| `service` | `str`  | The name of the service.                | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def draw_service(self, node: _Gen, service: str) -> Any:
    """One draw from a service's live distribution.

    Args:
        node: The generator, or the value, to act on.
        service: The name of the service.
    """
    if self.context is None or self.sampler is None:
        raise node.fail("there is no running simulation to draw a service from")
    try:
        config = self.env.registry.get_config(
            f"{self.context.sim_id}.service.{service}"
        )
    except KeyError:
        raise node.fail(f"'{service}' is not a service of this model") from None
    # The draw comes from this generator's own sequence, so it leaves the draws of
    # the service itself unchanged
    family = get_distribution(config.dist)
    return max(MIN_DELAY, family.draw(config, self.rng(node)))
```

##### `evaluate(node, scope)`

The value of a node: a generator is evaluated, a mapping and a list are walked, and anything else is a constant.

Parameters:

| Name    | Type    | Description                             | Default    |
| ------- | ------- | --------------------------------------- | ---------- |
| `node`  | `Any`   | The generator, or the value, to act on. | *required* |
| `scope` | `Scope` | What the expressions here read.         | *required* |

Raises:

| Type                   | Description                                                                                 |
| ---------------------- | ------------------------------------------------------------------------------------------- |
| `ValueGenerationError` | If a generator cannot give a value. The message names the file, the line and the generator. |

Source code in `src/desflow/values/runtime.py`

```python
def evaluate(self, node: Any, scope: Scope) -> Any:
    """The value of a node: a generator is evaluated, a mapping and a list are
    walked, and anything else is a constant.

    Args:
        node: The generator, or the value, to act on.
        scope: What the expressions here read.

    Raises:
        ValueGenerationError: If a generator cannot give a value. The message
            names the file, the line and the generator.
    """
    if isinstance(node, _Gen):
        return self._generate(node, scope)
    if isinstance(node, dict):
        out: Dict[Any, Any] = {}
        for key, item in node.items():
            if not self.omitted(item, scope):
                out[key] = self.evaluate(item, scope)
        return out
    if isinstance(node, list):
        return [self.evaluate(item, scope) for item in node]
    return node
```

##### `evaluate_vars(specs, scope, known=None)`

Evaluates named values in order, each seeing the ones before it as a bare name; returns the new values.

Parameters:

| Name    | Type                       | Description                              | Default    |
| ------- | -------------------------- | ---------------------------------------- | ---------- |
| `specs` | `Dict[str, Any]`           | The vars to evaluate, in order, by name. | *required* |
| `scope` | `Scope`                    | What the expressions here read.          | *required* |
| `known` | `Optional[Dict[str, Any]]` | The names that already have values.      | `None`     |

Source code in `src/desflow/values/runtime.py`

```python
def evaluate_vars(
    self,
    specs: Dict[str, Any],
    scope: Scope,
    known: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Evaluates named values in order, each seeing the ones before it as a bare
    name; returns the new values.

    Args:
        specs: The vars to evaluate, in order, by name.
        scope: What the expressions here read.
        known: The names that already have values.
    """
    names = dict(known or {})
    made: Dict[str, Any] = {}
    for name, spec in specs.items():
        value = self.evaluate(spec, replace(scope, names=names))
        names[name] = value
        made[name] = value
    return made
```

##### `faker(node, locale)`

The Faker of a `fake` generator, seeded from the generator's own sequence.

Parameters:

| Name     | Type   | Description                             | Default    |
| -------- | ------ | --------------------------------------- | ---------- |
| `node`   | `Fake` | The generator, or the value, to act on. | *required* |
| `locale` | `str`  | The Faker locale.                       | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def faker(self, node: Fake, locale: str) -> Any:
    """The Faker of a `fake` generator, seeded from the generator's own sequence.

    Args:
        node: The generator, or the value, to act on.
        locale: The Faker locale.
    """
    name = node._path
    faker = self._fakers.get(name)
    if faker is None:
        try:
            from faker import Faker
        except ImportError:
            raise node.fail(
                "needs the Faker package; install it with "
                "pip install 'desflow[fake]'"
            ) from None
        try:
            faker = Faker(locale)
        except (AttributeError, ValueError) as exc:
            raise node.fail(f"locale {locale!r} is not one Faker has: {exc}")
        faker.seed_instance(int(self.rng(node, "#fake").integers(2**32)))
        self._fakers[name] = faker
    return _FakerCall(faker, node)
```

##### `forget(key)`

Drops the `vars_once` kept for an entity whose flow has ended, so a long run does not keep one entry for each entity it has seen.

Parameters:

| Name  | Type  | Description            | Default    |
| ----- | ----- | ---------------------- | ---------- |
| `key` | `Any` | The key of the entity. | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def forget(self, key: Any) -> None:
    """Drops the `vars_once` kept for an entity whose flow has ended, so a long
    run does not keep one entry for each entity it has seen.

    Args:
        key: The key of the entity.
    """
    self._once.pop(key, None)
```

##### `iso_time(offset)`

The logical time now plus `offset` seconds, as the text of a timestamp.

Parameters:

| Name     | Type    | Description                     | Default    |
| -------- | ------- | ------------------------------- | ---------- |
| `offset` | `float` | Seconds to add to the time now. | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def iso_time(self, offset: float) -> str:
    """The logical time now plus `offset` seconds, as the text of a timestamp.

    Args:
        offset: Seconds to add to the time now.
    """
    return self.time_text(self.logical_time(self.env.now + offset))
```

##### `keep_previous(task)`

Has `task` keep its last finished payload for each stream, for `previous`.

Parameters:

| Name   | Type  | Description           | Default    |
| ------ | ----- | --------------------- | ---------- |
| `task` | `str` | The name of the task. | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def keep_previous(self, task: str) -> None:
    """Has `task` keep its last finished payload for each stream, for `previous`.

    Args:
        task: The name of the task.
    """
    self._previous_tasks.add(task)
```

##### `keep_rows(task, size)`

Has `task` keep the last `size` of its finished payloads, which `lookup` and `aggregate` read.

Parameters:

| Name   | Type  | Description            | Default    |
| ------ | ----- | ---------------------- | ---------- |
| `task` | `str` | The name of the task.  | *required* |
| `size` | `int` | How many rows to keep. | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def keep_rows(self, task: str, size: int) -> None:
    """Has `task` keep the last `size` of its finished payloads, which `lookup` and
    `aggregate` read.

    Args:
        task: The name of the task.
        size: How many rows to keep.
    """
    self._kept.setdefault(task, _Rows(size))
```

##### `logical_time(seconds)`

The logical time `seconds` of simulation time after the start.

Parameters:

| Name      | Type    | Description                 | Default    |
| --------- | ------- | --------------------------- | ---------- |
| `seconds` | `float` | Seconds of simulation time. | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def logical_time(self, seconds: float) -> dt.datetime:
    """The logical time `seconds` of simulation time after the start.

    Args:
        seconds: Seconds of simulation time.
    """
    start = self.env.start_datetime.astimezone(dt.timezone.utc)
    return (start + dt.timedelta(seconds=seconds)).astimezone(self.zone)
```

##### `lookup(node, scope)`

The value of a `lookup`.

Parameters:

| Name    | Type     | Description                     | Default    |
| ------- | -------- | ------------------------------- | ---------- |
| `node`  | `Lookup` | The generator.                  | *required* |
| `scope` | `Scope`  | What the expressions here read. | *required* |

Returns:

| Name  | Type  | Description           |
| ----- | ----- | --------------------- |
| `Any` | `Any` | The value, as a copy. |

Raises:

| Type         | Description                                                                                                 |
| ------------ | ----------------------------------------------------------------------------------------------------------- |
| `ValueError` | If the task has no row, distinct stands outside a repeat or has no row left, or the path is not in the row. |

Source code in `src/desflow/values/runtime.py`

```python
def lookup(self, node: Lookup, scope: Scope) -> Any:
    """The value of a `lookup`.

    Args:
        node: The generator.
        scope: What the expressions here read.

    Returns:
        Any: The value, as a copy.

    Raises:
        ValueError: If the task has no row, `distinct` stands outside a repeat or
            has no row left, or the path is not in the row.
    """
    rows = self._rows_of(node, node.task)
    if not len(rows):
        raise node.fail(f"task '{node.task}' has no finished row yet")
    used: Optional[Set[int]] = None
    if node.distinct:
        if not self._frames:
            raise node.fail(
                "distinct draws without replacement within one repeat, so the "
                "lookup stands inside a repeat"
            )
        used = self._frames[-1].setdefault(node._path, set())
    pool: _Pool = rows if node.latest_by is None else rows.latest(node.latest_by)
    if not len(pool):
        raise node.fail(
            f"no row of task '{node.task}' has the field '{node.latest_by}'"
        )
    seq, row = self._choose(node, pool, used)
    if used is not None:
        used.add(seq)
    try:
        return self.copy(part_of(row, node.path))
    except KeyError as exc:
        raise node.fail(f"task '{node.task}': {exc.args[0]}") from None
```

##### `note_started(task, waiting_for)`

Writes the one INFO line about a task whose starts were refused, once it starts.

Parameters:

| Name          | Type  | Description            | Default    |
| ------------- | ----- | ---------------------- | ---------- |
| `task`        | `str` | The task that starts.  | *required* |
| `waiting_for` | `Any` | The tasks it looks up. | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def note_started(self, task: str, waiting_for: Any) -> None:
    """Writes the one INFO line about a task whose starts were refused, once it
    starts.

    Args:
        task: The task that starts.
        waiting_for: The tasks it looks up.
    """
    found = self._refused.pop(task, None)
    if found is not None:
        logger.info(
            "task '%s' was not started from time %s until time %s, a wait of %s "
            "seconds of simulation time for a row of %s; %d starts were dropped%s",
            task,
            found[0],
            self.env.now,
            self.env.now - found[0],
            ", ".join(sorted(waiting_for)),
            found[1],
            _CONSUMED if found[3] else "",
        )
```

##### `now_time()`

The logical time now.

Source code in `src/desflow/values/runtime.py`

```python
def now_time(self) -> dt.datetime:
    """The logical time now."""
    return self.logical_time(self.env.now)
```

##### `omitted(node, scope)`

Whether a mapping field is left out: its generator's `omit_rate` or `omit_when` says so. The field is not evaluated then.

Parameters:

| Name    | Type    | Description                             | Default    |
| ------- | ------- | --------------------------------------- | ---------- |
| `node`  | `Any`   | The generator, or the value, to act on. | *required* |
| `scope` | `Scope` | What the expressions here read.         | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def omitted(self, node: Any, scope: Scope) -> bool:
    """Whether a mapping field is left out: its generator's `omit_rate` or
    `omit_when` says so. The field is not evaluated then.

    Args:
        node: The generator, or the value, to act on.
        scope: What the expressions here read.
    """
    if not isinstance(node, _Gen):
        return False
    if node.omit_rate is not None:
        if self.rng(node, "#omit").random() < node.omit_rate:
            return True
    if node.omit_when is not None:
        return self.condition(node, node.omit_when, scope)
    return False
```

##### `once(task, key, make)`

The `vars_once` of a task for an entity: made on the first call and the same afterwards.

Parameters:

| Name   | Type                           | Description                           | Default    |
| ------ | ------------------------------ | ------------------------------------- | ---------- |
| `task` | `str`                          | The name of the task.                 | *required* |
| `key`  | `Any`                          | The key of the entity.                | *required* |
| `make` | `Callable[[], Dict[str, Any]]` | Makes the values when none were kept. | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def once(
    self,
    task: str,
    key: Any,
    make: Callable[[], Dict[str, Any]],
) -> Dict[str, Any]:
    """The `vars_once` of a task for an entity: made on the first call and the
    same afterwards.

    Args:
        task: The name of the task.
        key: The key of the entity.
        make: Makes the values when none were kept.
    """
    kept = self._once.setdefault(key, {})
    values = kept.get(task)
    if values is None:
        values = make()
        kept[task] = values
    return values
```

##### `pick_row(node, rows)`

The next row of a `row` generator.

Parameters:

| Name   | Type                   | Description                             | Default    |
| ------ | ---------------------- | --------------------------------------- | ---------- |
| `node` | `Row`                  | The generator, or the value, to act on. | *required* |
| `rows` | `List[Dict[str, Any]]` | The rows of the file.                   | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def pick_row(self, node: Row, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """The next row of a `row` generator.

    Args:
        node: The generator, or the value, to act on.
        rows: The rows of the file.
    """
    cursor = self._rows.get(node._path)
    if cursor is None:
        cursor = _RowCursor(len(rows))
        self._rows[node._path] = cursor
    rng = self.rng(node)
    if node.order == "random":
        if node.weight is None:
            return dict(rows[int(rng.integers(len(rows)))])
        weights = cursor.weights(node, rows)
        return dict(rows[int(rng.choice(len(rows), p=weights))])
    if cursor.position >= len(rows):
        if node.order != "circular":
            if node.on_end == "error":
                raise node.fail(
                    f"the {len(rows)} rows of {node.file} ran out; set on_end "
                    "to loop or stop to say what happens then"
                )
            if node.on_end == "stop":
                if self._stop is None:
                    raise StopSimulation(None)
                self._stop()
                raise RunStopped()
        cursor.position = 0
        cursor.order = None
    if node.order == "shuffle":
        if cursor.order is None:
            cursor.order = [int(i) for i in rng.permutation(len(rows))]
        index = cursor.order[cursor.position]
    else:
        index = cursor.position
    cursor.position += 1
    return dict(rows[index])
```

##### `previous(node, scope)`

The value of a `previous`.

Parameters:

| Name    | Type       | Description                     | Default    |
| ------- | ---------- | ------------------------------- | ---------- |
| `node`  | `Previous` | The generator.                  | *required* |
| `scope` | `Scope`    | What the expressions here read. | *required* |

Returns:

| Name  | Type  | Description                                                                                                          |
| ----- | ----- | -------------------------------------------------------------------------------------------------------------------- |
| `Any` | `Any` | The value of the last record on the stream, or the default (null when none is written) when the stream has none yet. |

Raises:

| Type         | Description                                                    |
| ------------ | -------------------------------------------------------------- |
| `ValueError` | If it is read outside a task or the path is not in the record. |

Source code in `src/desflow/values/runtime.py`

```python
def previous(self, node: Previous, scope: Scope) -> Any:
    """The value of a `previous`.

    Args:
        node: The generator.
        scope: What the expressions here read.

    Returns:
        Any: The value of the last record on the stream, or the default (null when
            none is written) when the stream has none yet.

    Raises:
        ValueError: If it is read outside a task or the path is not in the record.
    """
    task = scope.process.get("name")
    if task is None or scope.stream is None:
        raise node.fail("previous is read inside a task")
    row = self._last.get((task, scope.stream))
    if row is None:
        return self.evaluate(node.default, scope)
    try:
        return self.copy(part_of(row, node.path))
    except KeyError as exc:
        raise node.fail(
            f"the last record of task '{task}': {exc.args[0]}"
        ) from None
```

##### `ready(tasks)`

Whether every task in `tasks` has a row.

Parameters:

| Name    | Type  | Description | Default    |
| ------- | ----- | ----------- | ---------- |
| `tasks` | `Any` | Task names. | *required* |

Returns:

| Name   | Type   | Description                                           |
| ------ | ------ | ----------------------------------------------------- |
| `bool` | `bool` | True when each task that keeps rows has at least one. |

Source code in `src/desflow/values/runtime.py`

```python
def ready(self, tasks: Any) -> bool:
    """Whether every task in `tasks` has a row.

    Args:
        tasks: Task names.

    Returns:
        bool: True when each task that keeps rows has at least one.
    """
    return all(len(self._kept[t]) for t in tasks if t in self._kept)
```

##### `record(task, payload, entity)`

Takes in the finished payload of a task, in the step its record is published. Only a mapping is kept, by reference.

Parameters:

| Name      | Type  | Description                                                 | Default    |
| --------- | ----- | ----------------------------------------------------------- | ---------- |
| `task`    | `str` | The name of the task.                                       | *required* |
| `payload` | `Any` | The payload that was published.                             | *required* |
| `entity`  | `Any` | The entity the task ran for, whose stream previous follows. | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def record(self, task: str, payload: Any, entity: Any) -> None:
    """Takes in the finished payload of a task, in the step its record is
    published. Only a mapping is kept, by reference.

    Args:
        task: The name of the task.
        payload: The payload that was published.
        entity: The entity the task ran for, whose stream `previous` follows.
    """
    if not isinstance(payload, Mapping):
        return
    rows = self._kept.get(task)
    if rows is not None:
        rows.add(payload)
    if task in self._previous_tasks:
        self._last[(task, stream_of(entity))] = payload
```

##### `refuse_start(task, waiting_for, consumed_item=False)`

Notes that a start of `task` was refused for want of a row.

Parameters:

| Name            | Type   | Description                                                                                      | Default    |
| --------------- | ------ | ------------------------------------------------------------------------------------------------ | ---------- |
| `task`          | `str`  | The task that was not started.                                                                   | *required* |
| `waiting_for`   | `Any`  | The tasks it looks up.                                                                           | *required* |
| `consumed_item` | `bool` | Whether the dropped start had already taken its get item, which the INFO line then says is lost. | `False`    |

Source code in `src/desflow/values/runtime.py`

```python
def refuse_start(
    self, task: str, waiting_for: Any, consumed_item: bool = False
) -> None:
    """Notes that a start of `task` was refused for want of a row.

    Args:
        task: The task that was not started.
        waiting_for: The tasks it looks up.
        consumed_item: Whether the dropped start had already taken its `get` item,
            which the INFO line then says is lost.
    """
    found = self._refused.get(task)
    if found is None:
        self._refused[task] = [self.env.now, 1, set(waiting_for), consumed_item]
    else:
        found[1] += 1
        found[2].update(waiting_for)
        found[3] = found[3] or consumed_item
```

##### `render(template, entity=None, scope=None, extras=None, attributes=None)`

Evaluates a `Template`: its `vars_once` once for the entity, its `vars`, then its payload, which must give a mapping or null.

A null gives an event whose value is null, which a Kafka sink with `envelope` off writes as a tombstone.

Parameters:

| Name         | Type                       | Description                                                                                                          | Default    |
| ------------ | -------------------------- | -------------------------------------------------------------------------------------------------------------------- | ---------- |
| `template`   | `Any`                      | The Template.                                                                                                        | *required* |
| `entity`     | `Any`                      | The entity whose attributes attr reads.                                                                              | `None`     |
| `scope`      | `Optional[Scope]`          | The scope of a running task, whose bare names the template's vars follow; without it a scope outside a task is made. | `None`     |
| `extras`     | `Optional[Dict[str, Any]]` | Fields written over the result.                                                                                      | `None`     |
| `attributes` | `Optional[List[str]]`      | Names of entity attributes written over the result, read now.                                                        | `None`     |

Returns:

| Type                       | Description                                                                  |
| -------------------------- | ---------------------------------------------------------------------------- |
| `Optional[Dict[str, Any]]` | Optional\[Dict[str, Any]\]: The mapping, or None when the payload gave null. |

Raises:

| Type                   | Description                                                                                                                         |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `ValueGenerationError` | If the payload gives something other than a mapping or null, or gives null while extras or attributes have fields to write over it. |

Source code in `src/desflow/values/runtime.py`

```python
def render(
    self,
    template: Any,
    entity: Any = None,
    scope: Optional[Scope] = None,
    extras: Optional[Dict[str, Any]] = None,
    attributes: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Evaluates a `Template`: its `vars_once` once for the entity, its `vars`,
    then its payload, which must give a mapping or null.

    A null gives an event whose value is null, which a Kafka sink with
    `envelope` off writes as a tombstone.

    Args:
        template: The `Template`.
        entity: The entity whose attributes `attr` reads.
        scope: The scope of a running task, whose bare names the template's vars
            follow; without it a scope outside a task is made.
        extras: Fields written over the result.
        attributes: Names of entity attributes written over the result, read now.

    Returns:
        Optional[Dict[str, Any]]: The mapping, or None when the payload gave null.

    Raises:
        ValueGenerationError: If the payload gives something other than a mapping
            or null, or gives null while `extras` or `attributes` have fields to
            write over it.
    """
    template._renumber(self._template_start)
    scope = scope or self.scope_of(entity, keyed=True)
    known = dict(scope.names)
    key = (
        f"template:{template.key}",
        getattr(entity, "key", None),
    )
    if template.vars_once:
        once = self.once(
            key[0],
            key[1],
            lambda: self.evaluate_vars(template.vars_once, scope, known),
        )
        known.update(once)
    if template.vars:
        known.update(self.evaluate_vars(template.vars, scope, known))
    result = self.evaluate(template.payload, replace(scope, names=known))
    located = getattr(template.payload, "_where", "")
    where = located or "the payload"
    if result is None and (extras or attributes):
        names = sorted({*(extras or {}), *(attributes or [])})
        # A blueprint's place has its file and line; a Builder template's place
        # is its name alone, and its fields come from `bind` or `BoundPayload`
        in_file = bool(located) and located != template.payload._path
        needs = (
            "id_field, item_field and attributes need a mapping"
            if in_file
            else "the fields and attributes written over it need a mapping"
        )
        raise ValueGenerationError(
            f"{where}: the payload gave null, so it has no field to write "
            f"{', '.join(names)} into ({needs})"
        )
    if result is None:
        return None
    if not isinstance(result, dict):
        raise ValueGenerationError(
            f"{where}: a payload must give a mapping or null, got {result!r}"
        )
    if extras:
        result.update(extras)
    if attributes:
        held = {} if entity is None else entity.attributes
        for name in attributes:
            result[name] = held.get(name)
    return result
```

##### `report_refused()`

Writes the INFO line for each task whose starts were all refused to the end of the run, so an empty task has its reason in the log.

Source code in `src/desflow/values/runtime.py`

```python
def report_refused(self) -> None:
    """Writes the INFO line for each task whose starts were all refused to the end
    of the run, so an empty task has its reason in the log."""
    for task, (since, dropped, waiting_for, consumed) in sorted(
        self._refused.items()
    ):
        logger.info(
            "task '%s' was not started from time %s to the end of the run at time "
            "%s, a wait of %s seconds of simulation time for a row of %s that "
            "never came; %d starts were dropped%s",
            task,
            since,
            self.env.now,
            self.env.now - since,
            ", ".join(sorted(waiting_for)),
            dropped,
            _CONSUMED if consumed else "",
        )
    self._refused.clear()
```

##### `rng(node, suffix='')`

The random sequence of a generator, made from the seed and the place of the generator in the file: `SeedSequence(seed, spawn_key=(crc32(place),))`. Adding or removing another field of a mapping leaves it unchanged; inserting a list item moves the streams of the items after it.

Parameters:

| Name     | Type   | Description                                                              | Default    |
| -------- | ------ | ------------------------------------------------------------------------ | ---------- |
| `node`   | `_Gen` | The generator, or the value, to act on.                                  | *required* |
| `suffix` | `str`  | Text added to the place, to make a second stream for the same generator. | `''`       |

Source code in `src/desflow/values/runtime.py`

```python
def rng(self, node: _Gen, suffix: str = "") -> np.random.Generator:
    """The random sequence of a generator, made from the seed and the place of
    the generator in the file: `SeedSequence(seed, spawn_key=(crc32(place),))`.
    Adding or removing another field of a mapping leaves it unchanged; inserting a
    list item moves the streams of the items after it.

    Args:
        node: The generator, or the value, to act on.
        suffix: Text added to the place, to make a second stream for the same generator.
    """
    name = node._path + suffix
    generator = self._streams.get(name)
    if generator is None:
        sequence = np.random.SeedSequence(
            self._entropy, spawn_key=(zlib.crc32(name.encode()),)
        )
        generator = np.random.default_rng(sequence)
        self._streams[name] = generator
    return generator
```

##### `scope_of(entity=None, names=None, keyed=False)`

The scope generators read outside a task: the registry, the clock, and the attributes of `entity` when given.

Parameters:

| Name     | Type                       | Description                                                                                                    | Default |
| -------- | -------------------------- | -------------------------------------------------------------------------------------------------------------- | ------- |
| `entity` | `Any`                      | The entity whose attributes are read, or None.                                                                 | `None`  |
| `names`  | `Optional[Dict[str, Any]]` | Bare names to add, by name.                                                                                    | `None`  |
| `keyed`  | `bool`                     | Whether gen: entity gives the key of entity. The places that make an entity's key and attributes leave it off. | `False` |

Source code in `src/desflow/values/runtime.py`

```python
def scope_of(
    self,
    entity: Any = None,
    names: Optional[Dict[str, Any]] = None,
    keyed: bool = False,
) -> Scope:
    """The scope generators read outside a task: the registry, the clock, and the
    attributes of `entity` when given.

    Args:
        entity: The entity whose attributes are read, or None.
        names: Bare names to add, by name.
        keyed: Whether `gen: entity` gives the key of `entity`. The places that
            make an entity's key and attributes leave it off.
    """
    attributes = {} if entity is None else entity.attributes
    key = entity.key if keyed and entity is not None else None
    if self._live is not None:
        return self._live.scope(None, attributes, names=names, entity=key)
    return Scope(
        attributes=attributes,
        registry=self.env.registry,
        now=self.env.now,
        names=names or {},
        entity=key,
        peek=self.env.peek,
    )
```

##### `time_text(moment)`

A time as the text a record's `timestamp` has, in the run's zone.

Parameters:

| Name     | Type       | Description | Default    |
| -------- | ---------- | ----------- | ---------- |
| `moment` | `datetime` | The time.   | *required* |

Source code in `src/desflow/values/runtime.py`

```python
def time_text(self, moment: dt.datetime) -> str:
    """A time as the text a record's `timestamp` has, in the run's zone.

    Args:
        moment: The time.
    """
    return moment.astimezone(self.zone).isoformat(timespec="milliseconds")
```

##### `value(spec, entity=None)`

Evaluates a generator now, for a low-level script: a typed generator from `desflow.values.generator`, or any value that holds one.

The generator is checked as a `Template` checks its payload, so a `row` file is read from the working folder and a generator that cannot work is refused. It takes its random stream from the first call, so call `value` with the same object each time: a `sequence` counts and a `unique` choice refuses on it.

Parameters:

| Name     | Type  | Description                                                     | Default    |
| -------- | ----- | --------------------------------------------------------------- | ---------- |
| `spec`   | `Any` | The generator, a mapping or list that holds one, or a constant. | *required* |
| `entity` | `Any` | The entity whose attributes attr reads, or None.                | `None`     |

Raises:

| Type         | Description                                                                                                        |
| ------------ | ------------------------------------------------------------------------------------------------------------------ |
| `ValueError` | If spec is a mapping with a gen key, which would make a new generator on every call, or the check finds a problem. |

Source code in `src/desflow/values/runtime.py`

```python
def value(self, spec: Any, entity: Any = None) -> Any:
    """Evaluates a generator now, for a low-level script: a typed generator from
    `desflow.values.generator`, or any value that holds one.

    The generator is checked as a `Template` checks its payload, so a `row` file is
    read from the working folder and a generator that cannot work is refused. It
    takes its random stream from the first call, so call `value` with the same
    object each time: a `sequence` counts and a `unique` choice refuses on it.

    Args:
        spec: The generator, a mapping or list that holds one, or a constant.
        entity: The entity whose attributes `attr` reads, or None.

    Raises:
        ValueError: If `spec` is a mapping with a `gen` key, which would make a
            new generator on every call, or the check finds a problem.
    """
    from pathlib import Path

    from desflow.values.check import Place, check_value
    from desflow.values.template import bind

    if isinstance(spec, Mapping) and "gen" in spec:
        raise ValueError(
            "value takes a generator made with generator(...), not a mapping: "
            "make it once with generator({...}) and pass that object on each "
            "call, so a sequence counts and a unique choice refuses across calls"
        )
    self._free += 1
    bind(spec, ("value", self._free), overwrite=False)
    place = Place(in_task=True, base_dir=Path.cwd(), vars_known=False)
    problems = [
        f"{'.'.join(str(p) for p in path)}: {message}"
        for path, message in check_value(
            spec, ("value", self._free), set(), place, mapping_field=False
        )
    ]
    if problems:
        raise ValueError("; ".join(problems))
    return self.evaluate(spec, self.scope_of(entity, keyed=True))
```

### `desflow.values.Aggregate`

Bases: `_Gen`

`aggregate`: `mean`, `sum`, `min`, `max` or `count` over the last `last` rows of the task `task` that `where` keeps. `where` is an expression over the row's own fields, such as `lead_days == 3 and precipitation > 0`; without it every row counts. `count` counts rows and takes no `path`; the others read the number at `path`. A reading looks back through the rows the task keeps (its `history`) until `last` rows are kept, and takes the rows there are when `where` keeps fewer.

#### Methods:

##### `evaluate(run, scope)`

The value of this generator.

Parameters:

| Name    | Type           | Description                                                   | Default    |
| ------- | -------------- | ------------------------------------------------------------- | ---------- |
| `run`   | `ValueRuntime` | The value runtime, which holds the generator's random stream. | *required* |
| `scope` | `Scope`        | What the expressions here read.                               | *required* |

Returns:

| Name  | Type  | Description |
| ----- | ----- | ----------- |
| `Any` | `Any` | The value.  |

Source code in `src/desflow/values/models.py`

```python
def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
    """The value of this generator.

    Args:
        run: The value runtime, which holds the generator's random stream.
        scope: What the expressions here read.

    Returns:
        Any: The value.
    """
    return run.aggregate(self, scope)
```

### `desflow.values.Fake`

Bases: `_Gen`

`fake`: a value from the Faker library. `method` names any method of Faker's providers, such as `name`, `email`, `isbn13` or `credit_card_number`, and `args` and `kwargs` are passed to it. `locale` picks the language and country, `en_US` by default. It needs the `fake` extra.

Each `fake` generator has a Faker of its own, seeded from the run's seed and its place in the file, so the same seed gives the same values and adding a field changes no other. With no seed, the values change on every run. A date, a time or a decimal result is written as text or a float.

#### Methods:

##### `evaluate(run, scope)`

The value of this generator.

Parameters:

| Name    | Type           | Description                                                   | Default    |
| ------- | -------------- | ------------------------------------------------------------- | ---------- |
| `run`   | `ValueRuntime` | The value runtime, which holds the generator's random stream. | *required* |
| `scope` | `Scope`        | What the expressions here read.                               | *required* |

Returns:

| Name  | Type  | Description |
| ----- | ----- | ----------- |
| `Any` | `Any` | The value.  |

Source code in `src/desflow/values/models.py`

```python
def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
    """The value of this generator.

    Args:
        run: The value runtime, which holds the generator's random stream.
        scope: What the expressions here read.

    Returns:
        Any: The value.
    """
    faker = run.faker(self, self.locale)
    args = [run.evaluate(a, scope) for a in self.args]
    kwargs = {k: run.evaluate(v, scope) for k, v in self.kwargs.items()}
    return getattr(faker, self.method)(*args, **kwargs)
```

### `desflow.values.HistogramBin`

Bases: `BaseModel`

One bin of a `lookup` histogram: `bin` is the share of the rows it covers, in the order they were written, and `frequency` is the chance that a draw falls in it. Both are scaled to add up to 1, so `{bin: 2, frequency: 8}, {bin: 8, frequency: 2}` means the same as 0.2 with 0.8, and 0.8 with 0.2.

### `desflow.values.Lookup`

Bases: `_Gen`

`lookup`: a row of the finished payloads of the task `task`, or the part of it at `path` (dotted; an integer part indexes a list).

A row is readable from the instant the task's finished record is published. The task keeps its last `history` rows (default 1,000,000).

`strategy` is `random` (the default), `first` (the oldest row kept) or `last` (the newest). `histogram` splits the rows kept, in the order they were written, into bins of the stated shares; a draw picks a bin by its frequency and then a row of the bin at random, so it is for `random` only. A bin that holds no row yet is left out of the draw, and while no bin with a frequency above 0 holds a row (a few rows and a last bin of frequency 0), the draw is over all the rows. `latest_by` keeps only the newest row for each value of that field of the row, so a lookup sees a customer's latest address; `strategy` and `histogram` then choose among those rows, oldest first by the time each was written. `distinct` draws without replacement within one `repeat`, so the items of one order differ.

#### Methods:

##### `evaluate(run, scope)`

The value of this generator.

Parameters:

| Name    | Type           | Description                                                   | Default    |
| ------- | -------------- | ------------------------------------------------------------- | ---------- |
| `run`   | `ValueRuntime` | The value runtime, which holds the generator's random stream. | *required* |
| `scope` | `Scope`        | What the expressions here read.                               | *required* |

Returns:

| Name  | Type  | Description |
| ----- | ----- | ----------- |
| `Any` | `Any` | The value.  |

Source code in `src/desflow/values/models.py`

```python
def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
    """The value of this generator.

    Args:
        run: The value runtime, which holds the generator's random stream.
        scope: What the expressions here read.

    Returns:
        Any: The value.
    """
    return run.lookup(self, scope)
```

### `desflow.values.Previous`

Bases: `_Gen`

`previous`: the last finished payload of the same task for the same stream, or the part of it at `path`. The stream is the arrival, so all the entities it starts share one, except for an arrival with `count` or `keys`, where each entity has its own. `default` is evaluated only when the stream has no finished payload yet, and without one the first reading is null.

#### Methods:

##### `evaluate(run, scope)`

The value of this generator.

Parameters:

| Name    | Type           | Description                                                   | Default    |
| ------- | -------------- | ------------------------------------------------------------- | ---------- |
| `run`   | `ValueRuntime` | The value runtime, which holds the generator's random stream. | *required* |
| `scope` | `Scope`        | What the expressions here read.                               | *required* |

Returns:

| Name  | Type  | Description |
| ----- | ----- | ----------- |
| `Any` | `Any` | The value.  |

Source code in `src/desflow/values/models.py`

```python
def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
    """The value of this generator.

    Args:
        run: The value runtime, which holds the generator's random stream.
        scope: What the expressions here read.

    Returns:
        Any: The value.
    """
    return run.previous(self, scope)
```

### `desflow.values.Row`

Bases: `_Gen`

`row`: one row of a CSV or Parquet file as a mapping, read once at load. A cell of digits is read as a number, so a postal code with leading zeros loses them; write them with `{gen: format, template: "{n:05d}"}`. `where` is an expression over the row's columns that keeps some rows; `weight` names a column of weights; `order` is `random` (the default), `sequential`, `shuffle` (every row once, in a random order) or `circular` (in file order, again from the first); `on_end` says what a `sequential` or `shuffle` order does when the rows run out: `error` (the default), `loop` or `stop`, which ends the run.

#### Methods:

##### `evaluate(run, scope)`

The value of this generator.

Parameters:

| Name    | Type           | Description                                                   | Default    |
| ------- | -------------- | ------------------------------------------------------------- | ---------- |
| `run`   | `ValueRuntime` | The value runtime, which holds the generator's random stream. | *required* |
| `scope` | `Scope`        | What the expressions here read.                               | *required* |

Returns:

| Name  | Type  | Description |
| ----- | ----- | ----------- |
| `Any` | `Any` | The value.  |

Source code in `src/desflow/values/models.py`

```python
def evaluate(self, run: "ValueRuntime", scope: Scope) -> Any:
    """The value of this generator.

    Args:
        run: The value runtime, which holds the generator's random stream.
        scope: What the expressions here read.

    Returns:
        Any: The value.
    """
    if self._rows is None:
        raise self.fail("the file was not read when the blueprint was loaded")
    return run.pick_row(self, self._rows)
```
