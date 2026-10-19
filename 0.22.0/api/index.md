# API Reference

## Simulation Context

### `SimulationContext(sim_id, factor=1.0, random_seed=None, logical_start_time=None, go_live_at=None, warmup=None, initial_time=0.0, lag_interval=None, timezone=None)`

Builder and Orchestrator for Event-Driven Digital Twins.

The `SimulationContext` implements the Builder Pattern to construct the foundational architecture of a Digital Twin. It acts as the central control plane, aggregating discrete machine capacities, continuous physical states (wear and tear), and statistical distributions into a unified `SimParameter` object.

Architectural Guarantees:

1. **Immutability During Configuration**: The underlying `DynamicRealtimeEnvironment` and its background asynchronous I/O threads are completely deferred until `.run()` is explicitly invoked. This prevents partial state leaks or unintended clock starts.
1. **One registry**: All registered configurations are flattened and managed by the internal `SimulationRegistry`, allowing external Kafka Admin commands to mutate these states safely during runtime.
1. **Deterministic Stochasticity**: The `Sampler` gives each distribution a random sequence of its own, made from `random_seed`, so a seeded run repeats exactly, and adding a distribution leaves the others' draws unchanged. A run without `random_seed` draws a seed and logs it, so it can be repeated.

Attributes:

| Name          | Type                 | Description                                                                                                                                                                                |
| ------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `sim_id`      | `str`                | The unique namespace for this simulation instance (e.g., 'HotRolling').                                                                                                                    |
| `factor`      | `float`              | The real-time synchronization multiplier. A factor of 1.0 syncs strictly with wall-clock time, while 0.0 executes instantly in a fast-forward loop (ideal for historical data processing). |
| `go_live_at`  | `Optional[datetime]` | Logical instant at which factor is replaced by real-time pacing, so one run can backfill history and then tail live.                                                                       |
| `warmup`      | `Optional[float]`    | The end of the warm-up, in simulation seconds.                                                                                                                                             |
| `random_seed` | `Optional[int]`      | The run's seed. When None, run() draws one, logs it and uses it; read it afterwards from sampler.seed.                                                                                     |
| `sampler`     | `Optional[Sampler]`  | The centralized Random Number Generator engine. Accessible to user-defined raw generators only after .run() is called.                                                                     |

Initializes the context builder with a designated namespace and temporal factor.

Parameters:

| Name                 | Type                      | Description                                                                                                                                                                                                                                                                                                                                   | Default    |
| -------------------- | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `sim_id`             | `str`                     | The unique prefix for this group of parameters. All generated telemetry and event paths will be prefixed with this ID.                                                                                                                                                                                                                        | *required* |
| `factor`             | `float`                   | The real-time synchronization factor. Defaults to 1.0.                                                                                                                                                                                                                                                                                        | `1.0`      |
| `random_seed`        | `Optional[int]`           | Optional seed for deterministic execution.                                                                                                                                                                                                                                                                                                    | `None`     |
| `logical_start_time` | `Optional[datetime]`      | Optional override for the environment's base clock, forwarded to DynamicRealtimeEnvironment. Crucial for historical backfilling (e.g., generating data from last week).                                                                                                                                                                       | `None`     |
| `go_live_at`         | `Optional[datetime]`      | Optional logical instant at which the run stops using factor and switches to real time, forwarded to DynamicRealtimeEnvironment. With factor=0.0 and a backdated logical_start_time, the history up to this instant is generated as fast as the machine allows and the rest arrives in real time, so a backfill and a live tail take one run. | `None`     |
| `warmup`             | `Union[float, str, None]` | The end of the warm-up, in simulation seconds or as a duration such as "10 min". No sink receives a record stamped before it, and the built-in statistics are reset at that time, so they measure the steady state only.                                                                                                                      | `None`     |
| `initial_time`       | `Union[float, str]`       | The simulation clock's starting value, in seconds or as a duration such as "8 h", so event times, sim_ts and until are readings of a clock that starts there. Defaults to 0.                                                                                                                                                                  | `0.0`      |
| `lag_interval`       | `Union[float, str, None]` | Simulation seconds, or a duration such as "10 s", between system.simulation.lag_seconds records, which measure how far the run is behind the wall clock. By default it is off at factor 0, where lag means nothing, and every simulation second otherwise; a run that goes live starts it then. 0 turns it off.                               | `None`     |
| `timezone`           | `Optional[str]`           | An IANA time zone name, such as "Australia/Melbourne", that record timestamps are written in, with their offset. By default the zone of logical_start_time, or UTC. A logical_start_time or go_live_at without an offset is read in it.                                                                                                       | `None`     |

Raises:

| Type         | Description                                                                                         |
| ------------ | --------------------------------------------------------------------------------------------------- |
| `ValueError` | If warmup or lag_interval is negative or not a duration, or timezone is not an IANA time zone name. |

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

    Raises:
        ValueError: If `warmup` or `lag_interval` is negative or not a duration,
            or `timezone` is not an IANA time zone name.
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
    self._breakdowns_config: Dict[str, BreakdownConfig] = {}
    self._scenario_steps: List[Dict[str, Any]] = []
    # Replayed arrivals: how to read each, and the rows left once read
    self._replays: Dict[str, Dict[str, Any]] = {}
    self._replay_rows: Dict[str, List[Row]] = {}
    self._replays_done: set = set()
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
    # True once start() has built and started everything, so a caller can tell
    # a value refused before the run from an error inside it
    self.started = False
    self.sampler: Optional[Sampler] = None
    self._resources_map: Dict[str, DynamicResource] = {}
    self._stores_map: Dict[str, DynamicStore] = {}
    self._containers_map: Dict[str, DynamicContainer] = {}
    self._entity_counters: Dict[str, "itertools.count[int]"] = {}
    self._instance_counters: Dict[str, "itertools.count[int]"] = {}
    self._startup_loops: List[Tuple[Callable, Optional[str]]] = []

    # Set by from_yaml from the blueprint's `run` section.
    self._default_until: Any = None
    self._default_stop: Any = None
    self._default_drain: Any = None
    # The run's end, entities in the model, and the drain state, set by start()
    self._until: Any = None
    self._end: Optional[simpy.Event] = None
    self._closed = True
    self._in_system = 0
    self._draining = False
    self._finish_run: Optional[Callable[..., None]] = None
    self._arrival_processes: List[simpy.Process] = []
    self._before_run: List[Callable[[], Any]] = []
    # How the log tells a run without a seed to repeat itself, with `{seed}`
    self._repeat_hint: Optional[str] = None
```

#### Attributes

##### `env`

Read-only handle to the active `DynamicRealtimeEnvironment`.

Grants raw generators access to environment primitives such as `publish_event`, `timeout`, `now`, and `start_datetime` without reaching into private builder state.

Raises:

| Type           | Description                                           |
| -------------- | ----------------------------------------------------- |
| `RuntimeError` | If accessed during the Builder Phase (before .run()). |

#### Functions

##### `add_arrival(name, dist, *, rate=None, mean=None, std=None, low=None, value=None, high=None, mode=None, shape=None, scale=None, values=None, weights=None, edges=None, counts=None, attributes=None, key=None)`

Registers a statistical distribution defining the inter-arrival times of entities.

Each arrival starts a new entity, which `wait_for_arrival` returns. Its key, the record key of every event it causes, is the arrival name and a counter (`orders-1`, `orders-2`) unless `key` gives another.

Parameters:

| Name         | Type                             | Description                                                                                               | Default    |
| ------------ | -------------------------------- | --------------------------------------------------------------------------------------------------------- | ---------- |
| `name`       | `str`                            | The internal identifier (e.g., 'orders').                                                                 | *required* |
| `dist`       | `str`                            | The family, as for add_service.                                                                           | *required* |
| `rate`       | `Optional[float]`                | Events per second, for exponential.                                                                       | `None`     |
| `mean`       | `Optional[float]`                | The mean, for normal, lognormal and truncated_normal.                                                     | `None`     |
| `std`        | `Optional[float]`                | The standard deviation, for the same three (0 when not given).                                            | `None`     |
| `low`        | `Optional[float]`                | The lowest value, for truncated_normal, uniform and triangular.                                           | `None`     |
| `value`      | `Optional[float]`                | The time a constant distribution always gives.                                                            | `None`     |
| `high`       | `Optional[float]`                | The highest value, for uniform and triangular.                                                            | `None`     |
| `mode`       | `Optional[float]`                | The most likely value, for triangular.                                                                    | `None`     |
| `shape`      | `Optional[float]`                | The shape, for gamma and weibull.                                                                         | `None`     |
| `scale`      | `Optional[float]`                | The scale, for gamma and weibull.                                                                         | `None`     |
| `values`     | `Optional[List[float]]`          | The values an empirical draws from, taken at load.                                                        | `None`     |
| `weights`    | `Optional[List[float]]`          | One weight per empirical value.                                                                           | `None`     |
| `edges`      | `Optional[List[float]]`          | The bin edges of an empirical histogram.                                                                  | `None`     |
| `counts`     | `Optional[List[float]]`          | One count per empirical histogram bin.                                                                    | `None`     |
| `attributes` | `Optional[Mapping[str, Any]]`    | The attributes each entity starts with, read in conditions as attr.<name>. Each entity gets its own copy. | `None`     |
| `key`        | `Optional[Callable[[int], str]]` | A function of the entity's counter, from 1, returning its key.                                            | `None`     |

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
```

##### `add_arrival_replay(name, source, *, time='time', unit='s', fields='all', on_invalid='error', attributes=None, key=None)`

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
| `key`        | `Optional[Callable[[int], str]]`                | A function of the entity's counter, from 1, returning its key.                                                                                                                                         | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                       |
| ------------ | --------------------------------- |
| `ValueError` | If unit or on_invalid is unknown. |

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
    key: Optional[Callable[[int], str]] = None,
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

    Returns:
        SimulationContext: The current instance for method chaining.

    Raises:
        ValueError: If `unit` or `on_invalid` is unknown.
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
    return self
```

##### `add_breakdown(resource, time_to_failure, repair, work='interrupt')`

Makes a resource break down at random and be repaired.

The failure clock runs on calendar time: after each repair, or the start, the time to the next failure is drawn, busy or idle. While the resource is down its capacity is 0, its `current_cap` keeps the planned value, and a change to it during the repair sets the capacity the resource returns to. Each failure and repair publishes an event keyed by the resource's name, `{"status": "down", "reason": "failure"}` and `{"status": "repaired", ...}`. The two distributions are registry paths, `<sim_id>.breakdown.<resource>.time_to_failure` and `.repair`.

Parameters:

| Name              | Type                | Description                                                                                                          | Default       |
| ----------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------- |
| `resource`        | `str`               | The resource's name.                                                                                                 | *required*    |
| `time_to_failure` | `Mapping[str, Any]` | A distribution, such as {"dist": "weibull", "shape": 1.5, "scale": 3600}.                                            | *required*    |
| `repair`          | `Mapping[str, Any]` | A distribution, such as {"dist": "exponential", "rate": 0.01}.                                                       | *required*    |
| `work`            | `str`               | interrupt (the default) interrupts the tasks being served, which follow their on_interrupt; finish lets them finish. | `'interrupt'` |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                           |
| ------------ | ------------------------------------- |
| `ValueError` | If a distribution or work is invalid. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `add_container(name, current_cap, max_cap, level=0.0)`

Registers a container: an amount that tasks take from and add to, such as stock.

`current_cap` is the capacity, the largest level, as for a resource. The level starts at `level` and has a live registry path of its own, `<sim_id>.containers.<name>.level`, so a scenario or an ingress message can set it during the run, such as a restock. Models that used `current_cap` as a level, such as wear, use `level` instead.

Parameters:

| Name          | Type    | Description                                         | Default    |
| ------------- | ------- | --------------------------------------------------- | ---------- |
| `name`        | `str`   | The internal identifier (e.g., 'stock').            | *required* |
| `current_cap` | `float` | The capacity at simulation start.                   | *required* |
| `max_cap`     | `float` | The largest capacity a live update can set.         | *required* |
| `level`       | `float` | The amount held at simulation start. Defaults to 0. | `0.0`      |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                       |
| ------------ | --------------------------------- |
| `ValueError` | If a store already has this name. |

Source code in `src/desflow/core/context.py`

```python
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
        current_cap=current_cap, max_cap=max_cap, level=level
    )
    return self
```

##### `add_egress(provider, when=None, batch_size=None, flush_interval=None)`

Registers an asynchronous egress provider for data exfiltration.

Every registered provider receives every record, so attaching a stream sink and a lake sink writes the same dataset to both in one pass. Pass `when` to send only some records to a provider, which reads the same way as the `path_router` that `ParquetStorageEgress` already accepts.

Parameters:

| Name             | Type                                       | Description                                                                                                                                                                                                                                                                         | Default    |
| ---------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `provider`       | `Any`                                      | An initialized connector subclassing BaseEgress (e.g., KafkaEgress or ParquetStorageEgress).                                                                                                                                                                                        | *required* |
| `when`           | `Union[Callable[[dict], bool], str, None]` | Optional predicate taking one record and returning True to send it to this provider. None sends every record, which is the usual case. "labels" makes it a labels sink: it receives the ground-truth records (env.publish_label) and nothing else, and no other sink receives them. | `None`     |
| `batch_size`     | `Optional[int]`                            | Records this provider buffers before flushing. None uses the context default from with_batching. A stream sink wants a small value to keep latency down; a file or lake sink wants a large one, because the batch is the file and, for Iceberg, the commit.                         | `None`     |
| `flush_interval` | `Optional[float]`                          | Simulation seconds this provider waits before forcing a flush. None uses the context default. Ignored when factor is 0, because simulation time is then detached from the wall clock.                                                                                               | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                              |
| ------------ | ---------------------------------------- |
| `ValueError` | If when is a string other than "labels". |

Each provider buffers separately, so memory is the sum of the buffers rather than one. A large lake buffer will dominate that sum.

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
    self._ingress_providers.append(provider)
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
    return self
````

##### `add_resource(name, current_cap, max_cap, queue=DEFAULT_QUEUE_ORDER, preempt=False)`

Registers a discrete logical resource capable of processing tasks.

Resources act as the primary gating mechanism in the simulation, blocking concurrent processes to prevent physical collisions (e.g., restricting the number of slabs inside a mill stand).

Parameters:

| Name          | Type   | Description                                                                                                                                                                                                                | Default               |
| ------------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------- |
| `name`        | `str`  | The internal identifier (e.g., 'mill_structural').                                                                                                                                                                         | *required*            |
| `current_cap` | `int`  | The active physical token capacity at simulation start.                                                                                                                                                                    | *required*            |
| `max_cap`     | `int`  | The absolute physical maximum bound of tokens allowed.                                                                                                                                                                     | *required*            |
| `queue`       | `str`  | The order waiting tasks are served in: fifo (the default), lifo or priority, which serves the lowest task priority first and the oldest first among equal priorities.                                                      | `DEFAULT_QUEUE_ORDER` |
| `preempt`     | `bool` | When True, a task of a lower priority value takes a unit from a task of a higher one when every unit is busy, as SimPy's PreemptiveResource does; the task that loses it follows its on_interrupt. Needs queue="priority". | `False`               |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                          |
| ------------ | ------------------------------------------------------------------------------------ |
| `ValueError` | If queue is not one of the three orders, or preempt is set without queue="priority". |

Source code in `src/desflow/core/context.py`

```python
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
    if not self._scenario_steps:
        self.add_process(self._run_scenario)
    self._scenario_steps.extend(checked)
    return self
```

##### `add_service(name, dist, *, rate=None, mean=None, std=None, low=None, value=None, high=None, mode=None, shape=None, scale=None, values=None, weights=None, edges=None, counts=None)`

Registers a statistical distribution defining a process service duration.

The parameters are keyword-only, so their order never changes the meaning. Give only those `dist` takes; `desflow.core.distributions.DISTRIBUTIONS` lists each family's parameters and rules.

Parameters:

| Name      | Type                    | Description                                                                                                               | Default    |
| --------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `name`    | `str`                   | The internal identifier (e.g., 'packing').                                                                                | *required* |
| `dist`    | `str`                   | The family: exponential, normal, lognormal, truncated_normal, constant, uniform, triangular, gamma, weibull or empirical. | *required* |
| `rate`    | `Optional[float]`       | Events per second, for exponential.                                                                                       | `None`     |
| `mean`    | `Optional[float]`       | The mean, for normal, lognormal and truncated_normal.                                                                     | `None`     |
| `std`     | `Optional[float]`       | The standard deviation, for the same three (0 when not given).                                                            | `None`     |
| `low`     | `Optional[float]`       | The lowest value, for truncated_normal (0 when not given), uniform and triangular.                                        | `None`     |
| `value`   | `Optional[float]`       | The time a constant distribution always gives.                                                                            | `None`     |
| `high`    | `Optional[float]`       | The highest value, for uniform and triangular.                                                                            | `None`     |
| `mode`    | `Optional[float]`       | The most likely value, for triangular.                                                                                    | `None`     |
| `shape`   | `Optional[float]`       | The shape, for gamma and weibull.                                                                                         | `None`     |
| `scale`   | `Optional[float]`       | The scale, for gamma and weibull.                                                                                         | `None`     |
| `values`  | `Optional[List[float]]` | The values an empirical draws from, taken at load.                                                                        | `None`     |
| `weights` | `Optional[List[float]]` | One weight per empirical value.                                                                                           | `None`     |
| `edges`   | `Optional[List[float]]` | The bin edges of an empirical histogram.                                                                                  | `None`     |
| `counts`  | `Optional[List[float]]` | One count per empirical histogram bin.                                                                                    | `None`     |

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
```

##### `add_signal(name)`

Declares a signal that tasks fire and wait for.

A task fires it with `signal=name` when it finishes, and fails it when the task fails; a task waits for it with `wait={"signal": name}`; a process reaches it with `context.signal(name)`.

Parameters:

| Name   | Type  | Description        | Default    |
| ------ | ----- | ------------------ | ---------- |
| `name` | `str` | The signal's name. | *required* |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Source code in `src/desflow/core/context.py`

```python
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

##### `add_store(name, current_cap, max_cap, priority=False, items=None)`

Registers a store: a place that holds items, which tasks put and get.

A task puts its finished payload as an item and gets the next item, waiting when the store is full or empty. A `fifo` store, the default, returns the oldest item; a priority store returns the item whose task gave the lowest `put` priority first, and the oldest among equal priorities.

Parameters:

| Name          | Type                  | Description                                                                                                             | Default    |
| ------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------- |
| `name`        | `str`                 | The internal identifier (e.g., 'parcels').                                                                              | *required* |
| `current_cap` | `int`                 | The number of items it can hold at simulation start.                                                                    | *required* |
| `max_cap`     | `int`                 | The largest capacity a live update can set.                                                                             | *required* |
| `priority`    | `bool`                | When True, items are returned by the priority each task's put gives, lowest first, instead of first in, first out.      | `False`    |
| `items`       | `Optional[List[Any]]` | The items it holds when the run starts, in the order a get returns them. A priority store takes (priority, item) pairs. | `None`     |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                                                                                            |
| ------------ | ---------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If a container already has this name, there are more items than current_cap, or a priority store's item is not a pair. |

Source code in `src/desflow/core/context.py`

```python
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
        current_cap=current_cap, max_cap=max_cap
    )
    self._store_priority[name] = priority
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
        self._env.teardown()
```

##### `compile_parameters()`

Compiles the builder state into the `SimParameter` that `run()` registers.

Useful before the run, to see which registry paths a configuration creates or to compare two configurations.

Returns:

| Name           | Type           | Description                                                    |
| -------------- | -------------- | -------------------------------------------------------------- |
| `SimParameter` | `SimParameter` | The parameters, sharing their config objects with the builder. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `finish()`

Runs a started run to its end, then closes it, as the rest of `run()` does.

The end is `until`, the `stop` or the end of the drain that `start()` set up. `close()` runs even when the run stops with an error or Ctrl+C.

Raises:

| Type           | Description                 |
| -------------- | --------------------------- |
| `RuntimeError` | If the run has not started. |

Source code in `src/desflow/core/context.py`

```python
def finish(self) -> None:
    """
    Runs a started run to its end, then closes it, as the rest of `run()` does.

    The end is `until`, the `stop` or the end of the drain that `start()` set
    up. `close()` runs even when the run stops with an error or Ctrl+C.

    Raises:
        RuntimeError: If the run has not started.
    """
    if self._env is None or not self.started:
        raise RuntimeError("finish() needs a run that start() has started")
    try:
        self._env.run(until=self._end if self._end is not None else self._until)
    finally:
        self.close()
```

##### `from_yaml(path)`

Builds a context from a YAML blueprint file.

The blueprint is validated before anything is built, and every error names the file and line. The context is built through the same builder methods a Python script calls, so the result is the same simulation.

Parameters:

| Name   | Type               | Description         | Default    |
| ------ | ------------------ | ------------------- | ---------- |
| `path` | `Union[str, Path]` | The blueprint file. | *required* |

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

    context, run = build(path)
    context._default_until = run.until
    context._default_stop = run.stop
    context._default_drain = run.drain_setting()
    context._before_run = list(run.before)
    context._repeat_hint = f"Repeat it with: desflow run {path} --seed {{seed}}"
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

| Type           | Description                         |
| -------------- | ----------------------------------- |
| `RuntimeError` | If called during the Builder Phase. |

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
    """
    if not self._env:
        raise RuntimeError("Containers are not instantiated until context is run.")
    return self._containers_map[container_id]
```

##### `get_resource(resource_id)`

Returns the `DynamicResource` a running model built for a resource.

Parameters:

| Name          | Type  | Description                                    | Default    |
| ------------- | ----- | ---------------------------------------------- | ---------- |
| `resource_id` | `str` | The resource's name, as given to add_resource. | *required* |

Returns:

| Name              | Type              | Description                                  |
| ----------------- | ----------------- | -------------------------------------------- |
| `DynamicResource` | `DynamicResource` | The resource, for raw requests and readings. |

Raises:

| Type           | Description                           |
| -------------- | ------------------------------------- |
| `RuntimeError` | If called before the run has started. |

Source code in `src/desflow/core/context.py`

```python
def get_resource(self, resource_id: str) -> DynamicResource:
    """Returns the `DynamicResource` a running model built for a resource.

    Args:
        resource_id: The resource's name, as given to `add_resource`.

    Returns:
        DynamicResource: The resource, for raw requests and readings.

    Raises:
        RuntimeError: If called before the run has started.
    """
    if not self._env:
        raise RuntimeError("Resources are not instantiated until context is run.")
    return self._resources_map[resource_id]
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

| Type           | Description                         |
| -------------- | ----------------------------------- |
| `RuntimeError` | If called during the Builder Phase. |

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
    """
    if not self._env:
        raise RuntimeError("Stores are not instantiated until context is run.")
    return self._stores_map[store_id]
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
    for run in list(self._running.get(task, [])):
        process = run.process
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
    counter = self._instance_counters.setdefault(task, itertools.count())
    return next(counter)
```

##### `peek()`

Returns the time of the next event of a started run, or infinity if none.

Raises:

| Type           | Description                 |
| -------------- | --------------------------- |
| `RuntimeError` | If the run has not started. |

Source code in `src/desflow/core/context.py`

```python
def peek(self) -> float:
    """
    Returns the time of the next event of a started run, or infinity if none.

    Raises:
        RuntimeError: If the run has not started.
    """
    return self.env.peek()
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

##### `run(until=None, stop=None, drain=None)`

Compiles the defined infrastructure architecture and triggers the simulation loop.

`run()` is `start()`, then SimPy's `env.run` until the end, then `close()`, which runs even when the run stops with an error or Ctrl+C.

Parameters:

| Name    | Type  | Description                                                                                                                                                                                                                                                                                                                                                                           | Default |
| ------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `until` | `Any` | The absolute simulation termination time. Can be a numeric float (representing base seconds) or a human-readable string parsed by the utility module (e.g., "1 week", "8 hours"). If None, a context built by from_yaml uses the blueprint's run.until, and otherwise the simulation runs infinitely. An event due at exactly until does not run, as in SimPy.                        | `None`  |
| `stop`  | `Any` | Ends the run before until when it happens: {"signal": name}, or the signal's name, for the next firing of a signal; {"condition": "..."} for the first moment a condition holds; or a function of the context, called once the run is built, that returns a SimPy event, such as lambda c: c.signal("closing").wait(). When it happens at the same time as until, until ends the run. | `None`  |
| `drain` | `Any` | At until, stop the arrivals and keep running until no entity is in the model: True, or {"limit": duration} to stop at the latest that long after until. Needs until.                                                                                                                                                                                                                  | `None`  |

Raises:

| Type         | Description                                                                                                        |
| ------------ | ------------------------------------------------------------------------------------------------------------------ |
| `ValueError` | If a task's next, priority, get or put names something the context does not define, or stop or drain is malformed. |

Source code in `src/desflow/core/context.py`

```python
def run(self, until: Any = None, stop: Any = None, drain: Any = None) -> None:
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

    Raises:
        ValueError: If a task's `next`, `priority`, `get` or `put` names something
            the context does not define, or `stop` or `drain` is malformed.
    """
    self.start(until, stop, drain)
    self.finish()
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
    return self._signals[name]
```

##### `spawn(process)`

Starts a generator as a SimPy process, such as a task instance.

This acts as the "escape hatch" for complex logic (like physical slab tracking) that outgrows the strict abstraction of the `@task` decorator.

Parameters:

| Name      | Type  | Description                                              | Default    |
| --------- | ----- | -------------------------------------------------------- | ---------- |
| `process` | `Any` | A generator, such as pack_order(task_id, entity=entity). | *required* |

Returns:

| Type      | Description                                                                                                                                                         |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Process` | simpy.Process: The process. Yield it to wait until it ends; its value is what the generator returns, and is_alive, triggered, ok and value inspect it, as in SimPy. |

Source code in `src/desflow/core/context.py`

```python
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
```

##### `start(until=None, stop=None, drain=None, *, seed=None, overrides=None, stats=None, egress=True, ingress=True, run_settings=True)`

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

Returns:

| Name                         | Type                         | Description                        |
| ---------------------------- | ---------------------------- | ---------------------------------- |
| `DynamicRealtimeEnvironment` | `DynamicRealtimeEnvironment` | The environment, also context.env. |

Raises:

| Type         | Description                                                                                          |
| ------------ | ---------------------------------------------------------------------------------------------------- |
| `ValueError` | As for run(); or stats is not a level, or an override's path does not exist or its value is refused. |

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
```

##### `step()`

Processes the next event of a started run, as SimPy's `env.step()` does.

Raises:

| Type            | Description                 |
| --------------- | --------------------------- |
| `RuntimeError`  | If the run has not started. |
| `EmptySchedule` | If no event is left.        |

Source code in `src/desflow/core/context.py`

```python
def step(self) -> None:
    """
    Processes the next event of a started run, as SimPy's `env.step()` does.

    Raises:
        RuntimeError: If the run has not started.
        simpy.core.EmptySchedule: If no event is left.
    """
    self.env.step()
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
    for process in getattr(self, "_arrival_processes", []):
        if process.is_alive:
            process.interrupt(_ARRIVALS_STOPPED)
```

##### `task(service_id=None, resource_id=None, *, name=None, priority=None, get=None, put=None, next=None, set=None, patience=None, balk=None, wait=None, signal=None, fail=None, fail_reason=None, on_fail=None, keep=None, preempt=True, on_interrupt='resume', on_interrupt_next=None, interrupt=None)`

Transforms a standard Python function into an event-driven SimPy task.

This decorator completely abstracts the boilerplate of resource acquisition, clock synchronization, and standardized telemetry lineage.

Lifecycle Execution:

1. Checks `balk`; when it holds, emits `balked` and stops.
1. Emits a `queued` event, when the task waits for anything: its `wait`, a resource or a `get`.
1. Waits for its `wait`, then for a unit of each resource in `resource_id`, in the order listed and each in its queue order, then for its `get`; `patience` counts the time spent waiting for all three, and pauses while the task is served.
1. Emits a `started` event.
1. Executes the wrapped user function to extract custom payload data.
1. Samples the live `service_id` distribution and yields the timeout.
1. Adds its `put`, waiting for room, and checks `fail`.
1. Emits its finished event with the user's payload, or `failed`.
1. Fires its `signal` (or fails it), sends its `interrupt` action, applies `set` to the entity's attributes and starts the `next` task, or `on_fail`.

Every event carries the entity's key as its record key, and the task's name and instance id as the record's `task` and `instance` fields.

A task with a service and no resource is a timed step: no `queued` event, so its wait is 0. A task with no service, resource or get takes no time: when spawned it emits the payload at once (after its `put`, if it has one), with no `queued` or `started` event.

Call the task as `task(task_id, ...)`. Pass `entity=` with the entity `wait_for_arrival` returned; without one the task starts an entity of its own, keyed by its name and a counter. The function receives `entity`, `upstream` (what the previous task handed on, or None) and `item` (what a store `get` took) by keyword, each only when it declares that parameter, so existing functions keep working. A task started by `next` is called with its own instance id and the same other arguments as the task before it. A function that declares `wait` receives the wait's branches, each a mapping of `done`, `ok` and `value`.

Parameters:

| Name                | Type                                                              | Description                                                                                                                                                                                                                                                                                                                                                                       | Default    |
| ------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `service_id`        | `Optional[str]`                                                   | The ID of the configured distribution dictating the execution time.                                                                                                                                                                                                                                                                                                               | `None`     |
| `resource_id`       | `Union[str, List[str], None]`                                     | The resource the task holds while it works, or a list of them, acquired in the order listed while the earlier ones are held.                                                                                                                                                                                                                                                      | `None`     |
| `name`              | `Optional[str]`                                                   | The task's name in records, statistics and next rules. Defaults to the function's name.                                                                                                                                                                                                                                                                                           | `None`     |
| `priority`          | `Union[float, str, Callable[[Any, Dict[str, Any]], float], None]` | For a resource whose queue order is priority, lowest first: a number, an expression such as "attr.priority", or a function of the task id and the entity's attributes. It is read when the task joins the queue, before its payload exists.                                                                                                                                       | `None`     |
| `get`               | `Union[Transfer, Mapping[str, Any], None]`                        | What the task takes before it starts: {"container": name, "amount": ...} or {"store": name}. The amount is a number, an expression such as "payload.quantity", or a function of the task id and the upstream payload. Defaults to an amount of 1.                                                                                                                                 | `None`     |
| `put`               | `Union[Transfer, Mapping[str, Any], None]`                        | What the task adds when it finishes, in the same form; a store receives the finished payload.                                                                                                                                                                                                                                                                                     | `None`     |
| `next`              | `NextSpec`                                                        | The task that follows: a task name, {"when": [{"condition": ..., "task": ...}], "default": ...}, {"weights": {name: weight}}, or a function of the Upstream returning a name or None.                                                                                                                                                                                             | `None`     |
| `set`               | `Optional[Mapping[str, Any]]`                                     | Attribute values applied to the entity when the task finishes.                                                                                                                                                                                                                                                                                                                    | `None`     |
| `patience`          | `Union[float, str, Callable[[Any, Dict[str, Any]], float], None]` | How long the task may wait, for its wait, its resources and its get together, before it leaves the queue and publishes reneged. It pauses while the task is served, so a preempted task that queues again goes on with the time it has left. Seconds, a duration such as "30 s", an expression such as "attr.patience", or a function of the task id and the entity's attributes. | `None`     |
| `balk`              | `Optional[str]`                                                   | A condition, such as "t >= 3300", checked before the task joins the queue; when it holds, the task publishes balked and never requests the resource.                                                                                                                                                                                                                              | `None`     |
| `wait`              | `Optional[Mapping[str, Any]]`                                     | What the task waits for before it queues for its resource: one branch, {"signal": name}, {"task": name}, {"timeout": seconds} or {"condition": "..."}, or {"any": {...}} or {"all": {...}} of named branches. After it, wait.<branch>.done, .ok and .value are read in next, fail and amounts.                                                                                    | `None`     |
| `signal`            | `Optional[str]`                                                   | A signal, added with add_signal, that the task fires with its finished payload, or fails with its reason when it fails.                                                                                                                                                                                                                                                           | `None`     |
| `fail`              | `Union[str, Callable[[Any, Any], bool], None]`                    | A condition checked after the service, such as "payload.defect == 1", where payload is the task's own payload, or a function of the task id and the payload. When it holds, the task publishes failed instead of its payload.                                                                                                                                                     | `None`     |
| `fail_reason`       | `Optional[str]`                                                   | The reason a failure publishes. Defaults to the fail condition's text.                                                                                                                                                                                                                                                                                                            | `None`     |
| `on_fail`           | `Optional[str]`                                                   | The task a failed instance goes to. Without it, a failure ends the entity's flow. A function that raises fails the task the same way, with the error as the reason.                                                                                                                                                                                                               | `None`     |
| `keep`              | `Optional[List[str]]`                                             | Resources in resource_id whose units the task hands to the next task of the flow instead of releasing them; that task releases them once it publishes started.                                                                                                                                                                                                                    | `None`     |
| `preempt`           | `bool`                                                            | On a resource added with preempt=True, whether the task's requests may take a unit from a task of a higher priority value.                                                                                                                                                                                                                                                        | `True`     |
| `on_interrupt`      | `str`                                                             | What an interrupt or a preemption does: resume (the default) gives back every unit, queues again and serves the remaining time; restart does the same with a new service time; abandon ends the flow, or goes to on_interrupt_next.                                                                                                                                               | `'resume'` |
| `on_interrupt_next` | `Optional[str]`                                                   | The task an abandoned instance goes to.                                                                                                                                                                                                                                                                                                                                           | `None`     |
| `interrupt`         | `Optional[Mapping[str, Any]]`                                     | An action applied when the task finishes: {"task": name, "where": condition, "cause": value} interrupts every running instance of that task whose condition holds.                                                                                                                                                                                                                | `None`     |

Raises:

| Type         | Description                                                                                                                   |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If a resource is given without a service, a priority without a resource, the name is taken, or get, put or next is malformed. |

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
````

##### `telemetry_loop(interval)`

Registers an infinite background daemon loop for periodic data publishing.

This is primarily used for observing and extracting continuous state (like wear containers) at strict intervals.

Parameters:

| Name       | Type    | Description                                                  | Default    |
| ---------- | ------- | ------------------------------------------------------------ | ---------- |
| `interval` | `float` | The simulation time (in seconds) to wait between iterations. | *required* |

Source code in `src/desflow/core/context.py`

```python
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

    if arrival_id in self._replays:
        return self._next_replayed(arrival_id)
    cfg = self._env.registry.get_config(f"{self.sim_id}.arrival.{arrival_id}")
    entity = self._new_entity(arrival_id, self._arrival_entities.get(arrival_id))
    return self._env.timeout(self.sampler.sample(cfg), value=entity)
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
    self._batch_size = batch_size
    self._flush_interval = flush_interval
    self._max_queued_batches = max_queued_batches
    self._drain_stall_seconds = drain_stall_seconds
    return self
```

##### `with_stats(level='basic', interval=DEFAULT_STATS_INTERVAL)`

Switches on the built-in statistics, published as telemetry.

Each level publishes what the levels before it publish, and more: `minimal` the readings of every resource, store and container right now; `basic` finished counts and time-weighted averages; `typical` the mean and maximum wait and service time of each task; `all` their 50th, 95th and 99th percentiles. Names look like `<sim_id>.stats.tasks.<task>.wait_mean`.

Parameters:

| Name       | Type    | Description                                                                                                  | Default                  |
| ---------- | ------- | ------------------------------------------------------------------------------------------------------------ | ------------------------ |
| `level`    | `str`   | none, minimal, basic, typical or all.                                                                        | `'basic'`                |
| `interval` | `float` | Simulation seconds between publications; they are also published once at the end of the run. Defaults to 10. | `DEFAULT_STATS_INTERVAL` |

Returns:

| Name                | Type                | Description                               |
| ------------------- | ------------------- | ----------------------------------------- |
| `SimulationContext` | `SimulationContext` | The current instance for method chaining. |

Raises:

| Type         | Description                                              |
| ------------ | -------------------------------------------------------- |
| `ValueError` | If the level is unknown or the interval is not positive. |

Source code in `src/desflow/core/context.py`

```python
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
```

## Task Flow

The classes a task's flow is built from, and the signals and failures tasks share.

### `Signal(env, name='signal')`

A named event that can fire many times, each firing waking every waiter.

Attributes:

| Name    | Type             | Description                                               |
| ------- | ---------------- | --------------------------------------------------------- |
| `env`   |                  | The environment.                                          |
| `name`  | `str`            | The signal's name.                                        |
| `count` | `int`            | How many times it has fired or failed.                    |
| `ok`    | `Optional[bool]` | Whether the last firing succeeded; None before the first. |
| `value` | `Any`            | The value of the last firing, or the failure reason.      |

Creates a signal that has not fired yet.

Parameters:

| Name   | Type          | Description                                                  | Default    |
| ------ | ------------- | ------------------------------------------------------------ | ---------- |
| `env`  | `Environment` | The environment.                                             | *required* |
| `name` | `str`         | Its name, as a blueprint's signals and signal.<name> use it. | `'signal'` |

Source code in `src/desflow/core/signals.py`

```python
def __init__(self, env: Environment, name: str = "signal"):
    """
    Creates a signal that has not fired yet.

    Args:
        env: The environment.
        name: Its name, as a blueprint's `signals` and `signal.<name>` use it.
    """
    self.env = env
    self.name = name
    self.count = 0
    self.ok: Optional[bool] = None
    self.value: Any = None
    self._event = env.event()
```

#### Functions

##### `fail(reason)`

Fails the signal: every waiter receives `SignalFailed` with the reason.

A failure nobody waits for is not an error: the event is defused, so the run goes on.

Parameters:

| Name     | Type  | Description                                     | Default    |
| -------- | ----- | ----------------------------------------------- | ---------- |
| `reason` | `Any` | Why it failed, such as a task's failure reason. | *required* |

Source code in `src/desflow/core/signals.py`

```python
def fail(self, reason: Any) -> None:
    """
    Fails the signal: every waiter receives `SignalFailed` with the reason.

    A failure nobody waits for is not an error: the event is defused, so the run
    goes on.

    Args:
        reason: Why it failed, such as a task's failure reason.
    """
    self.count += 1
    self.ok, self.value = False, reason
    event, self._event = self._event, self.env.event()
    event.defused = True
    event.fail(SignalFailed(self.name, reason))
    self._notify()
```

##### `fire(value=None)`

Fires the signal: every waiter resumes with `value`.

Parameters:

| Name    | Type  | Description                                                  | Default |
| ------- | ----- | ------------------------------------------------------------ | ------- |
| `value` | `Any` | What the waiters receive, such as a task's finished payload. | `None`  |

Source code in `src/desflow/core/signals.py`

```python
def fire(self, value: Any = None) -> None:
    """
    Fires the signal: every waiter resumes with `value`.

    Args:
        value: What the waiters receive, such as a task's finished payload.
    """
    self.count += 1
    self.ok, self.value = True, value
    event, self._event = self._event, self.env.event()
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

### `SignalFailed(signal, reason)`

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

### `TaskFailed(task, reason)`

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

### `Entity(key, arrival, attributes=dict(), held=dict(), entered=False)`

One thing moving through the model, such as an order, a slab or a session.

Each arrival starts a new entity. Its key is the record key of every event it causes, in every task it passes through.

Attributes:

| Name         | Type             | Description                                                                                                                                                                 |
| ------------ | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `key`        | `str`            | The record key: the arrival name and a counter, such as orders-1, unless the arrival has a key function.                                                                    |
| `arrival`    | `str`            | The arrival that started it, which names its flow in the statistics.                                                                                                        |
| `attributes` | `Dict[str, Any]` | State kept across tasks: set by the arrival's attributes and by each task's set, and read in conditions as attr.<name>. A payload includes them only when it asks for them. |
| `held`       | `Dict[str, Any]` | Resource units a task kept for the next task with keep, by resource name; the next task gives them back once it starts.                                                     |

### `Upstream(task_id, key, attributes, payload=None)`

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

### `Router(rules, rng=None)`

Chooses the task that follows a finished one.

The rules take one of these forms, in every API:

- a task name: always that task;
- `{"when": [{"condition": "...", "task": "..."}, ...], "default": "..."}`: the first rule whose condition holds wins, and `default`, if given, catches the rest; with no default and no rule holding, the flow ends;
- `{"weights": {"ship": 9, "rework": 1}}`: a task drawn with these weights from the routing task's own random sequence, so seeded runs repeat;
- a function taking the `Upstream` and returning a task name or None;
- None: the flow ends.

A route back to an earlier task (rework) is allowed.

Attributes:

| Name    | Type                  | Description                                                                           |
| ------- | --------------------- | ------------------------------------------------------------------------------------- |
| `tasks` | `Optional[Set[str]]`  | Every task the rules can name, so a loader can check they exist; None for a function. |
| `rng`   | `Optional[Generator]` | The generator weights draw from. The builder sets it when the run starts.             |

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
    self._cumulative: Optional[np.ndarray] = None
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
        self._when.append((Expression(rule["condition"]), str(rule["task"])))
    self._default = rules.get("default")
    if not self._when and self._default is None:
        raise ValueError("next needs when rules, a default or weights")
    self.tasks = {task for _, task in self._when} | (
        {self._default} if self._default is not None else set()
    )
```

#### Attributes

##### `conditions`

The parsed conditions, in rule order.

##### `weighted`

True when the rules are weights, which draw from `rng`.

#### Functions

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
    if self._function is not None:
        return self._function(upstream)
    if self._cumulative is not None:
        if self.rng is None:
            raise ValueError("weights need a random generator to draw from")
        index = int(np.searchsorted(self._cumulative, self.rng.random(), "right"))
        return self._names[min(index, len(self._names) - 1)]
    if self._when:
        if scope is None:
            scope = make_scope(upstream, env=env, levels=levels, item=item)
        for condition, task in self._when:
            if condition.evaluate(scope):
                return task
    return self._default
```

### `Transfer(container=None, store=None, amount=1.0, priority=0.0, filter=None)`

What a task takes from, or adds to, a store or a container.

A container transfer moves an amount: a number, an expression such as `payload.quantity`, or in Python a function of the task id and the upstream payload. A store transfer moves one item: a `get` takes the next item, and a `put` adds the task's finished payload.

Attributes:

| Name        | Type            | Description                  |
| ----------- | --------------- | ---------------------------- |
| `container` | `Optional[str]` | The container's name.        |
| `store`     | `Optional[str]` | The store's name.            |
| `amount`    |                 | The amount, for a container. |

Checks the transfer.

Parameters:

| Name        | Type                                      | Description                                                                                                                                                                                                                                    | Default |
| ----------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `container` | `Optional[str]`                           | The container's name.                                                                                                                                                                                                                          | `None`  |
| `store`     | `Optional[str]`                           | The store's name, instead of a container.                                                                                                                                                                                                      | `None`  |
| `amount`    | `Amount`                                  | For a container, a number, an expression or a function.                                                                                                                                                                                        | `1.0`   |
| `priority`  | `Amount`                                  | For a put into a priority store, a number, an expression or a function, lowest served first. Ignored by a fifo store.                                                                                                                          | `0.0`   |
| `filter`    | `Union[str, Callable[[Any], bool], None]` | For a store get, which item it takes: a condition such as "item.size == attr.size", where item.<field> reads the item offered, or a function of the item returning True. The get takes the oldest item that matches and waits while none does. | `None`  |

Raises:

| Type         | Description                                                                                                                                                      |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If not exactly one of container and store is given, an amount is given for a store, a priority is given for a container, or the amount or priority is malformed. |

Source code in `src/desflow/core/flow.py`

```python
def __init__(
    self,
    container: Optional[str] = None,
    store: Optional[str] = None,
    amount: Amount = 1.0,
    priority: Amount = 0.0,
    filter: Union[str, Callable[[Any], bool], None] = None,
):
    """
    Checks the transfer.

    Args:
        container: The container's name.
        store: The store's name, instead of a container.
        amount: For a container, a number, an expression or a function.
        priority: For a put into a priority store, a number, an expression
            or a function, lowest served first. Ignored by a `fifo` store.
        filter: For a store get, which item it takes: a condition such as
            `"item.size == attr.size"`, where `item.<field>` reads the item
            offered, or a function of the item returning True. The get takes
            the oldest item that matches and waits while none does.

    Raises:
        ValueError: If not exactly one of container and store is given, an
            amount is given for a store, a priority is given for a
            container, or the amount or priority is malformed.
    """
    if (container is None) == (store is None):
        raise ValueError("a get or put names one container or one store")
    if store is not None and amount != 1.0:
        raise ValueError("a store moves one item, so it takes no amount")
    if container is not None and priority != 0.0:
        raise ValueError("priority orders a store's items, so it needs a store")
    if container is not None and filter is not None:
        raise ValueError("a filter picks a store's item, so it needs a store")
    self.container = container
    self.store = store
    self.amount = amount
    self.priority = priority
    self._expression: Optional[Expression] = None
    if isinstance(amount, str):
        self._expression = Expression(amount)
    elif not callable(amount) and (
        isinstance(amount, bool) or not isinstance(amount, (int, float))
    ):
        raise ValueError(
            f"amount must be a number or an expression, got {amount!r}"
        )
    self.filter = filter
    self._filter_expression: Optional[Expression] = None
    if isinstance(filter, str):
        self._filter_expression = Expression(filter)
    elif filter is not None and not callable(filter):
        raise ValueError(
            f"filter must be a condition or a function, got {filter!r}"
        )
    self._priority_expression: Optional[Expression] = None
    if isinstance(priority, str):
        self._priority_expression = Expression(priority)
    elif not callable(priority) and (
        isinstance(priority, bool) or not isinstance(priority, (int, float))
    ):
        raise ValueError(
            f"priority must be a number or an expression, got {priority!r}"
        )
```

#### Attributes

##### `expressions`

Every expression the transfer holds, so a loader can check its names.

##### `target`

The name of the store or container.

#### Functions

##### `amount_for(task_id, upstream, scope)`

Returns the amount for one task instance.

Parameters:

| Name       | Type                 | Description                       | Default    |
| ---------- | -------------------- | --------------------------------- | ---------- |
| `task_id`  | `Any`                | The instance id.                  | *required* |
| `upstream` | `Optional[Upstream]` | What the previous task handed on. | *required* |
| `scope`    | `Scope`              | What an expression's names read.  | *required* |

Returns:

| Name    | Type    | Description |
| ------- | ------- | ----------- |
| `float` | `float` | The amount. |

Source code in `src/desflow/core/flow.py`

```python
def amount_for(
    self, task_id: Any, upstream: Optional[Upstream], scope: Scope
) -> float:
    """
    Returns the amount for one task instance.

    Args:
        task_id: The instance id.
        upstream: What the previous task handed on.
        scope: What an expression's names read.

    Returns:
        float: The amount.
    """
    if self._expression is not None:
        return float(self._expression.evaluate(scope))
    if callable(self.amount):
        return float(self.amount(task_id, payload_of(upstream)))
    return float(self.amount)
```

##### `filter_for(scope)`

Returns the function a store get offers each item to, or None for any item.

Parameters:

| Name    | Type    | Description                                                                                          | Default    |
| ------- | ------- | ---------------------------------------------------------------------------------------------------- | ---------- |
| `scope` | `Scope` | What the condition's other names read, taken when the get is made; item is set to each item offered. | *required* |

Returns:

| Type                              | Description                                                             |
| --------------------------------- | ----------------------------------------------------------------------- |
| `Optional[Callable[[Any], bool]]` | Optional\[Callable\[[Any], bool\]\]: True for an item the get may take. |

Source code in `src/desflow/core/flow.py`

```python
def filter_for(self, scope: Scope) -> Optional[Callable[[Any], bool]]:
    """
    Returns the function a store get offers each item to, or None for any item.

    Args:
        scope: What the condition's other names read, taken when the get is made;
            `item` is set to each item offered.

    Returns:
        Optional[Callable[[Any], bool]]: True for an item the get may take.
    """
    if self._filter_expression is not None:
        condition = self._filter_expression

        def matches(item: Any) -> bool:
            scope.item = item
            return bool(condition.evaluate(scope))

        return matches
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
    unknown = set(spec) - {"container", "store", "amount", "priority", "filter"}
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

| Name    | Type    | Description   |
| ------- | ------- | ------------- |
| `float` | `float` | The priority. |

Source code in `src/desflow/core/flow.py`

```python
def priority_for(
    self, task_id: Any, upstream: Optional[Upstream], scope: Scope
) -> float:
    """
    Returns the priority for one put into a priority store, lowest first.

    Args:
        task_id: The instance id.
        upstream: What the previous task handed on.
        scope: What an expression's names read.

    Returns:
        float: The priority.
    """
    if self._priority_expression is not None:
        return float(self._priority_expression.evaluate(scope))
    if callable(self.priority):
        return float(self.priority(task_id, payload_of(upstream)))
    return float(self.priority)
```

## YAML Blueprints

`SimulationContext.from_yaml` is the usual entry point. `build` is what it calls, and returns the `run` section as well.

### `build(path)`

Reads, validates and builds a blueprint file.

Parameters:

| Name   | Type               | Description    | Default    |
| ------ | ------------------ | -------------- | ---------- |
| `path` | `Union[str, Path]` | The YAML file. | *required* |

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
def build(path: Union[str, Path]) -> Tuple[SimulationContext, Run]:
    """Reads, validates and builds a blueprint file.

    Args:
        path: The YAML file.

    Returns:
        The built context, and the `run` section for whoever starts it.

    Raises:
        BlueprintError: If the file is invalid. The message names the file and line.
    """
    data, source = read_document(path)
    try:
        # One `now` for the whole file, so relative times agree with each other;
        # an instant, read in the run's zone
        now = datetime.now(timezone.utc)
        blueprint = Blueprint.model_validate(data, context={"now": now})
    except ValidationError as exc:
        raise _validation_error(exc, source) from None

    _check_references(blueprint, source)
    context = _build_context(blueprint, source)
    _check_scenario(blueprint, context, source)
    _check_registry_reads(blueprint, context, source)
    return context, blueprint.run
```

### `BlueprintError`

Bases: `ValueError`

A blueprint that cannot be built. The message names the file and line.

## Replications

### `replicate(model, n, *, seed, until=None, warmup=None, measures=None, level=0.95, workers=1, overrides=None, stop=None, drain=None)`

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
    if seed is None:
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

### `Replications(sim_id, seeds, measures, values, level=0.95, streams=list())`

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

#### Functions

##### `summary_text()`

The summary as a table: each measure's n, mean and half-width.

Source code in `src/desflow/analysis/runner.py`

```python
def summary_text(self) -> str:
    """The summary as a table: each measure's n, mean and half-width."""
    rows = self.summary
    width = max(len("measure"), *(len(row["measure"]) for row in rows))
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

### `LowLevelModel(function, sim_id)`

A model written with the low-level API, for `replicate` and `run_experiment`.

Attributes:

| Name       | Type       | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ---------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `function` | `Callable` | Called as function(env, sampler) once per run. It registers the parameters, builds the resources and starts the processes, and draws every random number from sampler. It does not set up the statistics or the sinks: the runner does. To be drained, it returns the processes that start its arrivals, which a drain interrupts at until, and reports its entities with env.stats.flow_started and flow_finished, so the drain knows when the model is empty. |
| `sim_id`   | `str`      | The sim id the parameters are registered under, which the statistics' names start with.                                                                                                                                                                                                                                                                                                                                                                         |

### `interval(values, level=0.95)`

Returns the mean of `values` and the Student t half-width of its interval.

The half-width is `t × s / √n`, where `s` is the sample standard deviation and `t` the quantile at `(1 + level) / 2` with `n - 1` degrees of freedom.

Parameters:

| Name     | Type              | Description                                                      | Default    |
| -------- | ----------------- | ---------------------------------------------------------------- | ---------- |
| `values` | `Sequence[float]` | Independent values, such as one statistic from each replication. | *required* |
| `level`  | `float`           | The confidence level, between 0 and 1. Defaults to 0.95.         | `0.95`     |

Returns:

| Name       | Type       | Description                  |
| ---------- | ---------- | ---------------------------- |
| `Interval` | `Interval` | The mean and its half-width. |

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
        Interval: The mean and its half-width.

    Raises:
        ValueError: If there are fewer than 2 values or the level is not between 0
            and 1.
    """
    if not 0.0 < level < 1.0:
        raise ValueError(f"level must be between 0 and 1, got {level}")
    n = len(values)
    if n < 2:
        raise ValueError(f"an interval needs at least 2 values, got {n}")
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

### `Interval(n, mean, std, half_width, level)`

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

### `t_quantile(p, df)`

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

### `run_experiment(spec, *, raise_on_failure=True, output=None)`

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
    from desflow.analysis import results as found
    from desflow.analysis.report import build_report
    from desflow.analysis.spec import ExperimentSpec, load_experiment

    source = None
    if isinstance(spec, (str, Path)):
        path = Path(spec)
        settings, source = load_experiment(path)
        folder = path.resolve().parent
    else:
        settings = (
            spec
            if isinstance(spec, ExperimentSpec)
            else ExperimentSpec.model_validate(spec)
        )
        folder = Path.cwd()

    def located(where: tuple, message: str) -> Exception:
        return source.error(where, message) if source else ValueError(message)

    model = settings.model
    if isinstance(model, str):
        model = str((folder / model).resolve())
        if not Path(model).is_file():
            raise located(
                ("model",),
                f"model {settings.model}: no such file; give a path from the "
                f"experiment file's folder",
            )
    if settings.observed is not None:
        observed_file = folder / settings.observed.file
        if not observed_file.is_file():
            raise located(
                ("observed", "file"),
                f"observed file {settings.observed.file}: no such file; give a path "
                f"from the experiment file's folder",
            )

    parameters = inspect_model(model, {})
    for name in settings.inputs:
        if name not in parameters or "dist" not in parameters[name]:
            raise located(
                ("inputs", name),
                f"inputs names {name}, which is not a distribution"
                + (
                    suggestion(name, [p for p, v in parameters.items() if "dist" in v])
                    or "; name a distribution's path, such as Shop.service.packing"
                ),
            )
    all_designs = found.designs(settings)
    for design in all_designs:
        try:
            inspect_model(model, design.overrides)
        except ValueError as exc:
            bad = next((p for p in design.overrides if f"'{p}'" in str(exc)), None)
            where = (
                ("sweep", bad)
                if bad in settings.sweep
                else ("variants", design.variant, bad or "")
            )
            raise located(where, f"{design.name}: {exc}") from None
    if settings.warmup is None:
        # The model's own warm-up, so the measured horizon is stated correctly
        context = context_of(model)
        settings = settings.model_copy(
            update={"warmup": context.warmup if context is not None else None}
        )
    observed = found.read_observed(settings, folder)

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
    runs = [run for design in all_designs for run in design.runs]
    select_measures(runs, settings.measures)
    # A count that is also a measure is summarised once, as a measure
    counts = [c for c in found.count_names(runs) if c not in settings.measures]
    rows = found.result_rows(all_designs, settings.measures, counts)
    summary = found.summary_rows(
        all_designs, rows, [*settings.measures, *counts], settings.level
    )
    comparisons = found.comparison_rows(settings, all_designs, rows)
    checks = found.check_rows(settings, summary)
    observed_rows = found.observed_rows(settings, observed, summary)

    if output is None:
        output = folder / (settings.output or f"results/{settings.experiment}")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    found.write_json(
        output / found.EXPERIMENT_FILE,
        _evidence(settings, model, runs[0].sim_id, parameters, all_designs)
        | {"counts": counts, "observed": observed_rows},
    )
    sweep = list(settings.sweep)
    for name, data in (
        (found.RESULTS_FILE, rows),
        (found.SUMMARY_FILE, summary),
        (found.COMPARISONS_FILE, comparisons),
        (found.CHECKS_FILE, checks),
    ):
        found.write_csv(output / name, data, found.columns(sweep, name))
    report = build_report(output)
    (output / found.REPORT_FILE).write_text(report, encoding="utf-8")

    result = found.ExperimentResult(
        spec=settings,
        output=output,
        designs=all_designs,
        results=rows,
        summary=summary,
        comparisons=comparisons,
        checks=checks,
        observed=observed_rows,
        report=report,
    )
    if result.failed and raise_on_failure:
        raise found.CheckFailed(result)
    return result
```

### `ExperimentSpec`

Bases: `_Model`

A whole experiment file, or the same settings given in Python.

`model` is a blueprint path, a builder factory (a function with no arguments returning a new `SimulationContext`), a `LowLevelModel`, or in a file a mapping of a low-level `function` and its `sim_id`. Relative paths are read from the experiment file's folder. `until` is where each replication's arrivals stop; `stop` and `drain` end each replication as `SimulationContext.run` does, and when they are not set a blueprint's own `run.stop` and `run.drain` apply.

### `load_experiment(path)`

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

### `ExperimentResult(spec, output, designs, results, summary, comparisons, checks, observed, report)`

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

### `CheckFailed(result)`

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

### `fit_column(values, *, unit='s', candidates=None, lower=None, upper=None, bins=None, min_count=20, tolerance=0.25, drop_invalid=False)`

Fits a column of measured times to desflow's distribution families.

Parameters:

| Name           | Type                      | Description                                                                                                                  | Default    |
| -------------- | ------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `values`       | `Sequence[Any]`           | The data, as numbers or text; an empty cell or None is missing.                                                              | *required* |
| `unit`         | `str`                     | The data's unit: ms, s, min or h. Settings are in seconds.                                                                   | `'s'`      |
| `candidates`   | `Optional[Sequence[str]]` | The families to try; None tries every family. empirical is always offered unless the list leaves it out.                     | `None`     |
| `lower`        | `Optional[float]`         | The lowest plausible value, in the data's unit; a value below it is invalid, and a family that can draw below it is flagged. | `None`     |
| `upper`        | `Optional[float]`         | The highest plausible value, in the same way.                                                                                | `None`     |
| `bins`         | `Optional[int]`           | Offer the empirical distribution as a histogram with this many bins, rather than as the values themselves.                   | `None`     |
| `min_count`    | `int`                     | The fewest values a family is fitted to. Defaults to 20.                                                                     | `20`       |
| `tolerance`    | `float`                   | How far a fitted family's mean and standard deviation may be from the data's, as a share of the data's. Defaults to 0.25.    | `0.25`     |
| `drop_invalid` | `bool`                    | Leave invalid values out rather than raising.                                                                                | `False`    |

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
        candidates: The families to try; None tries every family. `empirical` is
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
    names = list(candidates) if candidates is not None else list(DISTRIBUTIONS)
    unknown = [n for n in names if n not in DISTRIBUTIONS]
    if unknown:
        raise ValueError(
            f"no family called {', '.join(unknown)}; choose from "
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

### `FitResult(candidates, values, unit='s', rows=0, missing=0, invalid=0, lower=None, upper=None)`

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

#### Functions

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
        f"{'family':<17} {'status':<12} {'AIC':>10} {'KS':>7} {'p (approx.)':>11}"
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
            f"{_number(c.aic):>10} {_number(c.ks_statistic):>7} "
            f"{_number(c.ks_p_value):>11}  {detail}"
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

| Name  | Type  | Description                                                                                    |
| ----- | ----- | ---------------------------------------------------------------------------------------------- |
| `str` | `str` | A comment line, then dist and each parameter, one per line, with the numbers settings returns. |

Source code in `src/desflow/analysis/fit.py`

```python
def yaml(self, kind: Optional[str] = None, source: str = "") -> str:
    """
    The settings as a YAML mapping a blueprint loads with `!include`.

    Args:
        kind: The family; None takes the best.
        source: Where the data came from, for the comment at the top.

    Returns:
        str: A comment line, then `dist` and each parameter, one per line, with
            the numbers `settings` returns.
    """
    settings = self.settings(kind)
    lines = [
        f"# Fitted by desflow fit{' from ' + source if source else ''}: "
        f"{settings['dist']}, from {len(self.values)} values in {self.unit}, "
        "written in seconds."
    ]
    for key, value in settings.items():
        if isinstance(value, list):
            value = "[" + ", ".join(repr(v) for v in value) + "]"
        elif isinstance(value, float):
            value = repr(value)
        lines.append(f"{key}: {value}")
    return "\n".join(lines) + "\n"
```

### `Candidate(family, status, reason='', parameters=dict(), log_likelihood=None, aic=None, ks_statistic=None, ks_p_value=None, bounds='')`

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

## Environment

### `DynamicRealtimeEnvironment(initial_time=0, factor=1.0, strict=False, logical_start_time=None, go_live_at=None, timezone=None)`

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

| Name                 | Type       | Description                                                                                                                                                                                                                                                                                                  | Default |
| -------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| `initial_time`       | `float`    | The initial simulation time. Defaults to 0.                                                                                                                                                                                                                                                                  | `0`     |
| `factor`             | `float`    | The real-time factor (e.g., 1.0 = 1 sim second per real second). Defaults to 1.0.                                                                                                                                                                                                                            | `1.0`   |
| `strict`             | `bool`     | If True, raises RuntimeError if simulation falls too far behind real time. Defaults to False.                                                                                                                                                                                                                | `False` |
| `logical_start_time` | `datetime` | Overrides the environment's base clock. Crucial for historical backfilling (e.g., generating data from last week).                                                                                                                                                                                           | `None`  |
| `go_live_at`         | `datetime` | Logical instant at which pacing switches to real time. Until then the run uses factor, which is usually 0.0 so a backdated history is produced as fast as the machine allows. From that instant one simulated second takes one real second, so a backfill and a live tail run in one process instead of two. | `None`  |
| `timezone`           | `str`      | An IANA time zone name, such as Australia/Melbourne, that timestamps are written in, with their offset. By default the zone of logical_start_time, or UTC. A logical_start_time or go_live_at without an offset is read in it, and the default start is now in it.                                           | `None`  |

The clock runs in UTC, so a daylight-saving change in the zone never adds or drops simulation time. An instant at or before the start of the run makes the whole run real time, which is the degenerate case of the same rule.

Raises:

| Type         | Description                                |
| ------------ | ------------------------------------------ |
| `ValueError` | If timezone is not an IANA time zone name. |

Source code in `src/desflow/core/environment.py`

```python
def __init__(
    self,
    initial_time=0,
    factor=1.0,
    strict=False,
    logical_start_time: Optional[datetime] = None,
    go_live_at: Optional[datetime] = None,
    timezone: Optional[str] = None,
):
    """
    Initializes the real-time simulation environment.

    Args:
        initial_time (float, optional): The initial simulation time. Defaults to 0.
        factor (float, optional): The real-time factor (e.g., 1.0 = 1 sim second per real second). Defaults to 1.0.
        strict (bool, optional): If True, raises RuntimeError if simulation falls too far behind real time. Defaults to False.
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

    The clock runs in UTC, so a daylight-saving change in the zone never adds or
    drops simulation time. An instant at or before the start of the run makes the
    whole run real time, which is the degenerate case of the same rule.

    Raises:
        ValueError: If `timezone` is not an IANA time zone name.
    """
    self.zone: tzinfo = resolve_zone(timezone, logical_start_time)
    # The custom time, or the moment the script runs, in the run's zone
    self.start_datetime = (
        in_zone(logical_start_time, self.zone)
        if logical_start_time is not None
        else datetime.now(self.zone)
    )

    super().__init__(initial_time=initial_time, factor=factor, strict=strict)

    self._go_live_sim_time: Optional[float] = None
    if go_live_at is not None:
        self._go_live_sim_time = self._resolve_go_live_time(
            in_zone(go_live_at, self.zone), float(initial_time)
        )

    self.setup_registry()
    self._create_statistics()
    # Functions called after every change a shared condition can read
    self._change_watchers: List[Callable[[], None]] = []
```

#### Functions

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
    self.process(self._ingress_monitor())  # type: ignore[attr-defined]
```

##### `setup_egress(providers, batch_size=500, flush_interval=1.0, lag_monitor_interval='auto', max_queued_batches=2000, drain_stall_seconds=30.0, predicates=None, batch_sizes=None, flush_intervals=None, labels=None, warmup=None)`

Initializes the egress buffers and starts the background publisher threads.

Parameters:

| Name                   | Type               | Description                                                                                                                                                                                                                                               | Default    |
| ---------------------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `providers`            | `List[BaseEgress]` | A list of initialized egress connector instances.                                                                                                                                                                                                         | *required* |
| `batch_size`           | `int`              | The maximum number of events to buffer before flushing. Defaults to 500.                                                                                                                                                                                  | `500`      |
| `flush_interval`       | `float`            | Maximum simulation seconds to wait before forcing a flush. Defaults to 1.0. Ignored when factor is 0, because simulation time is then detached from the wall clock and the interval bounds nothing, so batch_size alone decides when a batch is sent.     | `1.0`      |
| `lag_monitor_interval` | `float`            | Simulation seconds between system.simulation.lag_seconds records. "auto", the default, is off at factor 0, where lag means nothing, and 1.0 otherwise; a run at factor 0 that goes live starts it at the go-live instant. None or 0 turns it off.         | `'auto'`   |
| `max_queued_batches`   | `int`              | Upper bound on batches waiting for the egress threads. A full queue blocks the producer, so a sink that cannot keep up slows the simulation rather than accumulating a backlog that teardown would have to discard. Defaults to 2000.                     | `2000`     |
| `drain_stall_seconds`  | `float`            | At teardown every queue is drained until empty. The wait is abandoned only if a queue stops shrinking for this long, so a slow sink finishes rather than losing its tail. Defaults to 30.0.                                                               | `30.0`     |
| `predicates`           | `List`             | One entry per provider, aligned by position. A callable takes a record and returns True to send it to that provider. None means that provider receives every record. Defaults to None, which is every provider receiving everything.                      | `None`     |
| `batch_sizes`          | `List`             | One entry per provider, aligned by position. None falls back to batch_size. A stream sink wants a small value and a lake sink a large one, because the batch is the file and, for Iceberg, the commit.                                                    | `None`     |
| `flush_intervals`      | `List`             | One entry per provider, aligned by position. None falls back to flush_interval.                                                                                                                                                                           | `None`     |
| `labels`               | `List`             | One entry per provider, aligned by position. True makes it a labels sink, which receives the ground-truth records from publish_label and nothing else; every other provider never receives them. Defaults to None, which is no labels sink.               | `None`     |
| `warmup`               | `float`            | The length of the warm-up, in simulation seconds from now, the start of the run when the egress is set up with the model. No provider receives a record stamped before it ends, and the built-in statistics are reset then. Defaults to None, no warm-up. | `None`     |

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
):
    """
    Initializes the egress buffers and starts the background publisher threads.

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

    Raises:
        ValueError: If a per-provider list has the wrong length.

    Each provider gets its own queue. They previously shared one, so several
    providers competed for batches instead of each receiving them, and a run with
    two sinks wrote part of the data to each with no error and exit code 0.
    """
    self.egress_drain_stall_seconds = drain_stall_seconds
    self.egress_providers = providers
    self.egress_queues: List[queue.Queue[Any]] = [
        queue.Queue(maxsize=max_queued_batches) for _ in providers
    ]
    self.egress_predicates: List[Optional[Callable[[Dict[str, Any]], bool]]] = (
        list(predicates) if predicates else [None] * len(providers)
    )
    if len(self.egress_predicates) != len(providers):
        raise ValueError(
            f"predicates has {len(self.egress_predicates)} entries for "
            f"{len(providers)} providers; they are matched by position"
        )
    # The fallback a provider gets when it names no value of its own. Read
    # egress_batch_sizes and egress_flush_intervals for what each one actually
    # uses, since either can differ from this.
    self.egress_batch_size = batch_size
    self.egress_flush_interval = flush_interval
    self.egress_batch_sizes: List[int] = self._resolve_per_provider(
        batch_sizes, batch_size, len(providers), "batch_sizes"
    )
    self.egress_flush_intervals: List[float] = self._resolve_per_provider(
        flush_intervals, flush_interval, len(providers), "flush_intervals"
    )
    self.egress_labels: List[bool] = [
        bool(flag)
        for flag in self._resolve_per_provider(
            labels, False, len(providers), "labels"
        )
    ]
    self.egress_warmup = warmup
    if warmup is not None:
        # The warm-up counts from the start of the run, as the statistics reset
        # below does, so a run with an initial_time drops the right records
        self.egress_predicates = [
            _after_warmup(self.now + float(warmup), predicate)  # type: ignore[attr-defined]
            for predicate in self.egress_predicates
        ]
        self.process(self._end_warmup(float(warmup)))  # type: ignore[attr-defined]
    # "auto": off while the clock is detached from the wall clock, once a second
    # otherwise; a run that goes live starts it when it does
    self._lag_on_go_live = False
    if isinstance(lag_monitor_interval, str):
        if lag_monitor_interval != "auto":
            raise ValueError(
                "lag_monitor_interval must be a number of seconds, None or "
                f"'auto', got {lag_monitor_interval!r}"
            )
        if getattr(self, "factor", 1.0):
            lag_monitor_interval = 1.0
        else:
            lag_monitor_interval = None
            self._lag_on_go_live = True
    self.egress_lag_monitor_interval: Optional[float] = lag_monitor_interval
    # One buffer per provider. They shared one until batching moved behind the
    # fan-out, which meant a single cadence for every sink and no way to give a
    # stream sink low latency while a lake sink built large files.
    self._event_buffers: List[List[Dict[str, Any]]] = [[] for _ in providers]
    # Counts consecutive timer-driven flushes that came in far below batch_size,
    # so a configuration where the size never governs is reported once per run.
    self._starved_flushes: List[int] = [0] * len(providers)
    self._starved_totals: List[int] = [0] * len(providers)
    self._starvation_warned: List[bool] = [False] * len(providers)
    self._egress_loop: Optional[asyncio.AbstractEventLoop] = (
        None  # Store loop reference
    )

    # Start background threads for providers
    self._egress_thread = threading.Thread(
        target=self._run_egress_loop, daemon=True
    )
    self._egress_thread.start()

    # Start background processes.
    # The periodic flush waits in simulation time, so at factor=0 it fires as fast
    # as the machine advances the clock and batch_size never governs. A 5,000
    # second run with batch_size=500 produced about 5,000 batches of 33 records
    # instead of 330 of 500, measured in issue #5. There is no wall clock to bound
    # latency against when factor is 0, so the timer has nothing to achieve and is
    # not started. Teardown flushes whatever is left in the buffer.
    self._periodic_flush_running = bool(getattr(self, "factor", 1.0))
    if self._periodic_flush_running:
        for index in range(len(providers)):
            self.process(self._periodic_flush(index))  # type: ignore[attr-defined]
    # Only start the lag monitor if an interval > 0 is provided
    if self.egress_lag_monitor_interval and self.egress_lag_monitor_interval > 0:
        self.process(self._lag_monitor())  # type: ignore[attr-defined]
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
    if not hasattr(self, "egress_queues"):
        return  # Fail silently if no egress is configured

    # Built directly rather than through TelemetryPayload. See publish_event.
    self._append_record(
        {
            "stream_type": "telemetry",
            "sim_ts": float(round(self.now, 3)),  # type: ignore[attr-defined]
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

##### `publish_event(event_key, value, task=None, instance=None)`

Buffers a high-volume discrete event (e.g., a task starting or finishing).

Events are buffered and flushed either when `batch_size` is reached or when `flush_interval` occurs to optimize external I/O throughput.

Parameters:

| Name        | Type  | Description                                                                                                                                                                                         | Default    |
| ----------- | ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `event_key` | `str` | The record key: the entity's key, such as orders-1.                                                                                                                                                 | *required* |
| `value`     | `Any` | A dictionary or a Pydantic model containing the event payload. It is made JSON-serializable on the way out, so a nested model is flattened and a datetime becomes an ISO string, exactly as before. | *required* |
| `task`      | `str` | The name of the task the event belongs to, written as the record's task field, so the payload stays the user's own.                                                                                 | `None`     |
| `instance`  | `Any` | The task's instance id, written as the record's instance field.                                                                                                                                     | `None`     |

Source code in `src/desflow/core/environment.py`

```python
def publish_event(
    self,
    event_key: str,
    value: Any,
    task: Optional[str] = None,
    instance: Any = None,
):
    """
    Buffers a high-volume discrete event (e.g., a task starting or finishing).

    Events are buffered and flushed either when `batch_size` is reached
    or when `flush_interval` occurs to optimize external I/O throughput.

    Args:
        event_key (str): The record key: the entity's key, such as `orders-1`.
        value (Any): A dictionary or a Pydantic model containing the event
            payload. It is made JSON-serializable on the way out, so a nested model
            is flattened and a datetime becomes an ISO string, exactly as before.
        task (str, optional): The name of the task the event belongs to, written
            as the record's `task` field, so the payload stays the user's own.
        instance (Any, optional): The task's instance id, written as the record's
            `instance` field.
    """
    if not hasattr(self, "_event_buffers"):
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
    self._append_record(
        {
            "stream_type": "event",
            "sim_ts": float(round(self.now, 3)),  # type: ignore[attr-defined]
            "timestamp": self._get_iso_timestamp(self.start_datetime, self.now),  # type: ignore[attr-defined]
            "key": (
                event_key
                if type(event_key) is str
                else _KEY_ADAPTER.validate_python(event_key)
            ),
            "task": task,
            "instance": instance,
            "value": _jsonable(value),
        }
    )
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

## Registry & Parameters

### `SimulationRegistry(env)`

A central registry that maps dot-notation paths to dynamic simulation parameters.

The Registry acts as the single source of truth for the simulation's state. It allows external streams (like Kafka or Redis) to update parameters while the simulation runs, and passes each change to the SimPy processes that use it.

Attributes:

| Name  | Type          | Description                   |
| ----- | ------------- | ----------------------------- |
| `env` | `Environment` | The active SimPy environment. |

Source code in `src/desflow/core/registry.py`

```python
def __init__(self, env: Environment):
    self.env = env
    self._values: Dict[str, DynamicValue] = {}
    self._configs: Dict[str, Any] = {}
```

#### Functions

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
        self._register_dist(path, dist_config)

    # Register Service Steps
    for name, dist_config in param.service.items():
        path = f"{prefix}.service.{name}"
        self._configs[path] = dist_config
        self._register_dist(path, dist_config)

    # Register Resources
    for name, cap_config in param.resources.items():
        path = f"{prefix}.resources.{name}"
        self._configs[path] = cap_config
        self._register_cap(path, cap_config)

    # Register Containers
    for name, cap_config in param.containers.items():
        path = f"{prefix}.containers.{name}"
        self._configs[path] = cap_config
        # A container level is continuous, so an integer starting value must not
        # make later updates integers through the type cast in update()
        self._register_cap(path, cap_config, cast=float)
        level = float(getattr(cap_config, "level", 0.0))
        self._values[f"{path}.level"] = DynamicValue(
            self.env, f"{path}.level", level
        )

    # Register Stores
    for name, cap_config in param.stores.items():
        path = f"{prefix}.stores.{name}"
        self._configs[path] = cap_config
        self._register_cap(path, cap_config)

    # Register Breakdowns: each resource's time to failure and repair time
    for name, breakdown in param.breakdowns.items():
        for part in ("time_to_failure", "repair"):
            path = f"{prefix}.breakdown.{name}.{part}"
            dist_config = getattr(breakdown, part)
            self._configs[path] = dist_config
            self._register_dist(path, dist_config)

    # Register Custom Variables
    self._configs[f"{prefix}.variables"] = param.variables
    for name, value in param.variables.items():
        path = f"{prefix}.variables.{name}"
        self._values[path] = DynamicValue(self.env, path, value)
```

##### `set_exactly(path, value)`

Sets a path to exactly `value`, raising where `update` would log and go on.

Replications and experiments set their variants' values this way before the clock starts, so a misspelt path or a refused value stops the experiment rather than leaving the model's own value in place.

Parameters:

| Name    | Type  | Description             | Default    |
| ------- | ----- | ----------------------- | ---------- |
| `path`  | `str` | The dot-notation path.  | *required* |
| `value` | `Any` | The value it must hold. | *required* |

Raises:

| Type         | Description                                                                                                                                          |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If the path does not exist, or the registry refuses, casts to another value or clamps the value, or a max_cap would clamp the current_cap beside it. |

Source code in `src/desflow/core/registry.py`

```python
def set_exactly(self, path: str, value: Any) -> None:
    """
    Sets a path to exactly `value`, raising where `update` would log and go on.

    Replications and experiments set their variants' values this way before the
    clock starts, so a misspelt path or a refused value stops the experiment rather
    than leaving the model's own value in place.

    Args:
        path (str): The dot-notation path.
        value (Any): The value it must hold.

    Raises:
        ValueError: If the path does not exist, or the registry refuses, casts to
            another value or clamps the value, or a `max_cap` would clamp the
            `current_cap` beside it.
    """
    if path not in self._values:
        raise ValueError(
            f"'{path}' is not a registry path" + suggestion(path, self._values)
        )
    sibling = f"{path.rsplit('.', 1)[0]}.current_cap"
    if path.endswith(".max_cap") and sibling in self._values:
        current = self._values[sibling].value
        if value is not None and current is not None and current > value:
            # update would lower current_cap to it without a word
            raise ValueError(
                f"'{path}' cannot be set to {value!r}: '{sibling}' is "
                f"{current!r}; set current_cap first, to {value!r} or less"
            )
    self.update(path, value)
    kept = self._values[path].value
    if kept != value:
        raise ValueError(
            f"'{path}' cannot be set to {value!r}; the registry kept {kept!r}"
        )
```

##### `update(path, new_value)`

Update a value safely and synchronize its parent configuration object.

Includes dynamic type casting to protect the simulation from crashing if external systems send improperly typed data (e.g., sending the string "5" instead of the integer 5).

A distribution parameter outside its range (for example a negative std) is refused with an error log line, as a value of the wrong type is, and the old value is kept. A `current_cap` outside `[0, max_cap]` is clamped to that range with a warning log line, also when a lower `max_cap` arrives, and so is a container `level` outside `[0, current_cap]`. A path the model does not have, such as a parameter the distribution's family does not take, is refused with a warning log line.

Parameters:

| Name        | Type  | Description                      | Default    |
| ----------- | ----- | -------------------------------- | ---------- |
| `path`      | `str` | The dot-notation path to update. | *required* |
| `new_value` | `Any` | The new value to apply.          | *required* |

Source code in `src/desflow/core/registry.py`

```python
def update(self, path: str, new_value: Any):
    """
    Update a value safely and synchronize its parent configuration object.

    Includes dynamic type casting to protect the simulation from crashing if
    external systems send improperly typed data (e.g., sending the string "5"
    instead of the integer 5).

    A distribution parameter outside its range (for example a negative std) is
    refused with an error log line, as a value of the wrong type is, and the old
    value is kept. A `current_cap` outside `[0, max_cap]` is clamped to that range
    with a warning log line, also when a lower `max_cap` arrives, and so is a
    container `level` outside `[0, current_cap]`. A path the model does not have,
    such as a parameter the distribution's family does not take, is refused with a
    warning log line.

    Args:
        path (str): The dot-notation path to update.
        new_value (Any): The new value to apply.
    """
    if path in self._values:
        current_val = self._values[path].value
        validated_value = new_value

        # Type Validation & Casting
        if current_val is not None:
            expected_type = type(current_val)
            # If types don't match, attempt a safe cast (e.g., "5" -> 5)
            if not isinstance(new_value, expected_type):
                try:
                    validated_value = expected_type(new_value)
                    logger.debug(
                        f"Cast '{new_value}' to {expected_type.__name__} for '{path}'"
                    )
                except (ValueError, TypeError):
                    logger.error(
                        f"Type mismatch for '{path}'. Expected {expected_type.__name__}, "
                        f"got {type(new_value).__name__} with value '{new_value}'. Update ignored."
                    )
                    return  # Exit early to prevent crashing the simulation

        parent_path, attr = path.rsplit(".", 1) if "." in path else (path, "")
        parent_obj = self._configs.get(parent_path)

        # Check a distribution parameter with the same rules as at load
        if isinstance(parent_obj, DistributionConfig):
            candidate = copy.copy(parent_obj)
            setattr(candidate, attr, validated_value)
            try:
                check_distribution(candidate)
            except ValueError as exc:
                logger.error(f"Invalid value for '{path}': {exc}. Update ignored.")
                return

        # Keep a container level within [0, current_cap], what the container holds
        if (
            isinstance(parent_obj, CapacityConfig)
            and attr == "level"
            and validated_value is not None
        ):
            cap = self._values[f"{parent_path}.current_cap"].value
            clamped = type(validated_value)(min(max(validated_value, 0), cap))
            if clamped != validated_value:
                logger.warning(
                    f"level {validated_value} for '{path}' is outside "
                    f"[0, current_cap {cap}]. Stored as {clamped}."
                )
                validated_value = clamped

        # Keep current_cap within [0, max_cap], the range the resource uses
        if isinstance(parent_obj, CapacityConfig) and attr == "current_cap":
            max_cap = self._values[f"{parent_path}.max_cap"].value
            clamped = type(validated_value)(min(max(validated_value, 0), max_cap))
            if clamped != validated_value:
                logger.warning(
                    f"current_cap {validated_value} for '{path}' is outside "
                    f"[0, max_cap {max_cap}]. Stored as {clamped}."
                )
                validated_value = clamped

        # A lower max_cap clamps current_cap first, so no reader sees it above
        if (
            isinstance(parent_obj, CapacityConfig)
            and attr == "max_cap"
            and validated_value is not None
        ):
            self._clamp_current_cap(parent_path, parent_obj, validated_value)

        # Update the DynamicValue (triggers SimPy events)
        self._values[path].update(validated_value)

        # Synchronize the attribute on the original config object
        # Check if the parent is a dictionary (like `variables`)
        if isinstance(parent_obj, dict):
            parent_obj[attr] = validated_value
        elif parent_obj is not None:
            setattr(parent_obj, attr, validated_value)

        # Waits on a shared condition check it again after every change
        notify = getattr(self.env, "notify_change", None)
        if notify is not None:
            notify()
    else:
        logger.warning(f"Attempted to update non-existent path: {path}")
```

### `SimParameter(sim_id, arrival=dict(), service=dict(), resources=dict(), containers=dict(), stores=dict(), breakdowns=dict(), variables=dict())`

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

### `CapacityConfig(current_cap, max_cap)`

Configuration for the capacity of a simulated resource, container, or store.

Attributes:

| Name          | Type                | Description                             |
| ------------- | ------------------- | --------------------------------------- |
| `current_cap` | `Union[int, float]` | The currently active capacity.          |
| `max_cap`     | `Union[int, float]` | The absolute physical maximum capacity. |

### `DistributionConfig(dist, rate=None, mean=None, std=None, low=None, value=None, high=None, mode=None, shape=None, scale=None, values=None, weights=None, edges=None, counts=None)`

Configuration for a statistical distribution used in the simulation.

This config is dynamically updatable. For example, you can change the `rate` of an 'exponential' distribution via Kafka, and the `Sampler` will instantly use the new rate for the next event. The family (`dist`) is fixed at load, and only its parameters change live.

Numbers are stored as floats, so a live update to 0.5 is not cast to an integer. The family and its parameters are checked when the config is built, against the registry of distributions in `desflow.core.distributions`.

Attributes:

| Name      | Type                    | Description                                                                                                                                                                                 |
| --------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dist`    | `str`                   | The family: exponential, normal, lognormal, truncated_normal, constant, uniform, triangular, gamma, weibull or empirical.                                                                   |
| `rate`    | `Optional[float]`       | Events per second, for exponential. Greater than 0.                                                                                                                                         |
| `mean`    | `Optional[float]`       | The mean, for normal, lognormal and truncated_normal. Greater than 0 for lognormal.                                                                                                         |
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

### `ContainerConfig(current_cap, max_cap, level=0)`

Bases: `CapacityConfig`

Configuration for a container: its capacity and the level it starts at.

`current_cap` is the container's capacity, its largest level, as it is a resource's capacity. The level has a registry path of its own, `.level`, so a scenario or an ingress message can set it during a run, such as a restock.

Attributes:

| Name          | Type                | Description                                                                                              |
| ------------- | ------------------- | -------------------------------------------------------------------------------------------------------- |
| `current_cap` | `Union[int, float]` | The capacity: the largest level.                                                                         |
| `max_cap`     | `Union[int, float]` | The largest capacity a live update can set.                                                              |
| `level`       | `Union[int, float]` | The amount held when the run starts, kept as the type given, as simpy.Container keeps it. Defaults to 0. |

### `BreakdownConfig(time_to_failure, repair, work='interrupt')`

How a resource breaks down: the time to each failure, the repair time, and what happens to the work in progress.

The failure clock runs on calendar time: the next failure is drawn when a repair ends, busy or idle. While down, the resource's capacity is 0; its `current_cap` keeps the planned value, which a change during the repair sets for its return.

Attributes:

| Name              | Type                 | Description                                                                                                                                                        |
| ----------------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `time_to_failure` | `DistributionConfig` | The time from a repair, or the start, to the next failure.                                                                                                         |
| `repair`          | `DistributionConfig` | How long a repair takes.                                                                                                                                           |
| `work`            | `str`                | interrupt (the default) interrupts the tasks being served, which follow their on_interrupt; finish lets them finish, and no new task starts until the repair ends. |

## Data Payloads

### `TelemetryPayload`

Bases: `BaseStreamPayload`

Schema for low-volume, single-metric telemetry updates.

This payload is used for publishing continuous system state variables, such as resource utilization, queue lengths, or system lag, typically used for real-time dashboards.

Attributes:

| Name          | Type                   | Description                                                             |
| ------------- | ---------------------- | ----------------------------------------------------------------------- |
| `stream_type` | `Literal['telemetry']` | Hardcoded to "telemetry" for downstream routing.                        |
| `path_id`     | `str`                  | The dot-notation path of the metric (e.g., 'Shop.packers.utilization'). |
| `value`       | `Any`                  | The scalar value of the metric at the given simulation time.            |

### `EventPayload`

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

### `LabelPayload`

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

### `BaseStreamPayload`

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

### `Sampler(rng=None, *, seed=None)`

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

#### Functions

##### `sample(config, min_delay=1e-05)`

Draws a random sample using the current parameters in the provided config.

A config the registry has registered draws from its own sequence; one built by hand and never registered draws from `rng`.

Parameters:

| Name        | Type                 | Description                                                | Default    |
| ----------- | -------------------- | ---------------------------------------------------------- | ---------- |
| `config`    | `DistributionConfig` | The live configuration object retrieved from the Registry. | *required* |
| `min_delay` | `float`              | The smallest value returned.                               | `1e-05`    |

Returns:

| Name    | Type    | Description                                                                                                                                                                        |
| ------- | ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `float` | `float` | A sampled float representing a time duration (e.g., arrival gap or service time). Returns min_delay if the sample is smaller than min_delay. A std of 0 gives the mean every time. |

Raises:

| Type         | Description                               |
| ------------ | ----------------------------------------- |
| `ValueError` | If the dist type string is not supported. |

Source code in `src/desflow/core/sampler.py`

```python
def sample(self, config: DistributionConfig, min_delay: float = 0.00001) -> float:
    """
    Draws a random sample using the current parameters in the provided config.

    A config the registry has registered draws from its own sequence; one built
    by hand and never registered draws from `rng`.

    Args:
        config (DistributionConfig): The live configuration object retrieved from the Registry.
        min_delay (float): The smallest value returned.

    Returns:
        float: A sampled float representing a time duration (e.g., arrival gap or service time).
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

### `time_to_seconds(time_str)`

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

### `DynamicResource(env, sim_id, resource_id, queue=DEFAULT_QUEUE_ORDER, preempt=False)`

Bases: `BaseDynamicResource`

A SimPy Resource with a dynamically adjustable capacity.

Unlike a standard `simpy.Resource` where capacity is fixed at creation, the `DynamicResource` listens to a `SimulationRegistry` path (e.g., 'Shop.resources.packers.current_cap'). When the registry updates the capacity, the resource adjusts at once, granting waiting requests if the capacity rises.

Grants follow SimPy's `Resource`: a request is granted when it is made if a unit is free and nothing waits ahead of it, and a release grants the head of the queue one event later, when its release event is processed; a release that gives back nothing tries the head too. Equal-time grants therefore happen in the order SimPy gives them.

Waiting requests are served in the resource's queue order: `fifo` (the default) serves the oldest first, `lifo` the newest first, and `priority` by the request's key, `(priority, time, not preempt)`, as SimPy's `PriorityResource` does: the lowest `priority` value first, then the earliest, then one that may preempt. A sequence number breaks the remaining ties, so such requests are served first in, first out.

Attributes:

| Name          | Type                           | Description                                              |
| ------------- | ------------------------------ | -------------------------------------------------------- |
| `env`         | `DynamicRealtimeEnvironment`   | The active simulation environment.                       |
| `sim_id`      | `str`                          | The prefix ID (e.g., 'Shop').                            |
| `resource_id` | `str`                          | The specific resource name (e.g., 'packers').            |
| `queue_order` | `str`                          | fifo, lifo or priority.                                  |
| `queue`       |                                | The waiting requests; len(queue.items) is how many wait. |
| `users`       | `List[DynamicResourceRequest]` | The granted requests, oldest first.                      |

Initializes the DynamicResource and binds it to the registry.

Parameters:

| Name          | Type          | Description                                                                                                                                                                                                                                                                                                              | Default               |
| ------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------- |
| `env`         | `Environment` | The SimPy environment (must include the RegistryMixIn).                                                                                                                                                                                                                                                                  | *required*            |
| `sim_id`      | `str`         | The parent simulation ID.                                                                                                                                                                                                                                                                                                | *required*            |
| `resource_id` | `str`         | The name of the resource.                                                                                                                                                                                                                                                                                                | *required*            |
| `queue`       | `str`         | The queue order: fifo, lifo or priority. Defaults to fifo. A request's priority counts only under priority.                                                                                                                                                                                                              | `DEFAULT_QUEUE_ORDER` |
| `preempt`     | `bool`        | When True, a request that finds every unit busy takes one from the user SimPy's PreemptiveResource would choose: the one whose key (priority, request time, not preempt) sorts last, if that key sorts after the request's. That user's process receives simpy.Interrupt with a Preempted cause. Needs queue="priority". | `False`               |

Raises:

| Type         | Description                                                                          |
| ------------ | ------------------------------------------------------------------------------------ |
| `ValueError` | If queue is not one of the three orders, or preempt is set without queue="priority". |

Source code in `src/desflow/resources/resource.py`

```python
def __init__(
    self,
    env: Environment,
    sim_id: str,
    resource_id: str,
    queue: str = DEFAULT_QUEUE_ORDER,
    preempt: bool = False,
):
    """
    Initializes the DynamicResource and binds it to the registry.

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
    self.preempt = preempt
    self.queue_order = queue
    # Counts requests, so equal ranks keep their arrival order
    self._sequence = itertools.count()
    self.queue = _RequestQueue()
    self.users: List[DynamicResourceRequest] = []
    # The down state of a breakdown: no unit is granted while it is down
    self.down = False
    self.failures = 0
    self.has_breakdowns = False
    self._down_since: Optional[float] = None
    self._downtime = 0.0
    super().__init__(env, sim_id, resource_id, "resources")

    self._capacity = self._target()
    self._register()
```

#### Attributes

##### `in_use`

Units currently held by tasks, including units above a lowered capacity.

##### `capacity`

The current active capacity of the resource.

This value reflects the live state from the Registry and may change during the simulation.

#### Functions

##### `request(priority=1, preempt=True)`

Returns a context-manager enabled request.

Parameters:

| Name       | Type    | Description                                                                                                                                                           | Default |
| ---------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `priority` | `float` | Lower values are served first when the queue order is priority; ignored under fifo and lifo.                                                                          | `1`     |
| `preempt`  | `bool`  | On a resource built with preempt=True, whether this request may take a unit from a user of a higher priority value. Ignored otherwise. Defaults to True, as in SimPy. | `True`  |

Returns:

| Name                     | Type                     | Description                                    |
| ------------------------ | ------------------------ | ---------------------------------------------- |
| `DynamicResourceRequest` | `DynamicResourceRequest` | An event that succeeds when a unit is granted. |

Source code in `src/desflow/resources/resource.py`

```python
def request(
    self, priority: float = 1, preempt: bool = True
) -> DynamicResourceRequest:
    """
    Returns a context-manager enabled request.

    Args:
        priority: Lower values are served first when the queue order is
            `priority`; ignored under `fifo` and `lifo`.
        preempt: On a resource built with `preempt=True`, whether this request
            may take a unit from a user of a higher priority value. Ignored
            otherwise. Defaults to True, as in SimPy.

    Returns:
        DynamicResourceRequest: An event that succeeds when a unit is granted.
    """
    return DynamicResourceRequest(self, priority, preempt)
```

##### `release(request=None)`

Gives a granted unit back, and tries the head of the queue one event later.

Releasing a request twice, or one never granted, gives back nothing, as SimPy's `Release` does; the queue is still tried when the release event is processed, as SimPy tries it after every release. While units are busy above a lowered capacity, a release brings the busy count down and grants nothing until it is within the capacity.

Parameters:

| Name      | Type                               | Description                                                                                             | Default |
| --------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------- | ------- |
| `request` | `Optional[DynamicResourceRequest]` | The granted request. None gives back the oldest unit held, the form 0.21.0 code without a request uses. | `None`  |

Returns:

| Name    | Type    | Description                           |
| ------- | ------- | ------------------------------------- |
| `Event` | `Event` | The release event, already triggered. |

Source code in `src/desflow/resources/resource.py`

```python
def release(self, request: Optional[DynamicResourceRequest] = None) -> Event:
    """
    Gives a granted unit back, and tries the head of the queue one event later.

    Releasing a request twice, or one never granted, gives back nothing, as
    SimPy's `Release` does; the queue is still tried when the release event is
    processed, as SimPy tries it after every release. While units are busy above
    a lowered capacity, a release brings the busy count down and grants nothing
    until it is within the capacity.

    Args:
        request: The granted request. None gives back the oldest unit held, the
            form 0.21.0 code without a request uses.

    Returns:
        Event: The release event, already triggered.
    """
    released = self.env.event()
    if request is None and self.users:
        request = self.users[0]
    if request is not None and request in self.users:
        self.users.remove(request)
        self._changed()
    # SimPy tries the head of the queue when any release event is processed,
    # one that gave back no unit too, such as the release of a request that
    # reneged; on a preemptive resource that try can preempt a user
    released.callbacks.append(self._grant_from_release)
    released.succeed()
    return released
```

### `DynamicContainer(env, sim_id, container_id, init=None)`

Bases: `BaseDynamicResource`

A wrapper for SimPy Container with dynamic capacity updates.

Useful for modeling continuous bulk materials (fluids, gases, battery charge) and stock. Unlike a standard `simpy.Container`, the capacity of this container listens to a `SimulationRegistry` path and can be mutated at runtime.

`current_cap` is the capacity, the largest level, as it is for a resource. The level has a registry path of its own, `<sim_id>.containers.<name>.level`: the registry follows every put and get, and a live update to the path sets the level, such as a restock, waking the tasks it lets through.

**Capacity Shrinkage Behavior:** If the capacity is dynamically shrunk below the current `level` of the container, the material is NOT destroyed. The container will temporarily exist in an "overflow" state. All pending and future `put` requests will be strictly blocked until downstream processes `get` enough material to drain the `level` back below the new, smaller capacity.

Attributes:

| Name     | Type          | Description                              |
| -------- | ------------- | ---------------------------------------- |
| `env`    | `Environment` | The active SimPy simulation environment. |
| `sim_id` | `str`         | The parent simulation ID prefix.         |
| `obj_id` | `str`         | The specific container name/identifier.  |

Initializes the DynamicContainer and binds its capacity to the registry.

Parameters:

| Name           | Type          | Description                                                                                                                                                               | Default    |
| -------------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `env`          | `Environment` | The SimPy environment (must include Registry interface).                                                                                                                  | *required* |
| `sim_id`       | `str`         | The parent simulation ID.                                                                                                                                                 | *required* |
| `container_id` | `str`         | The unique name of this container.                                                                                                                                        | *required* |
| `init`         | `float`       | The amount in the container at simulation start. None, the default, takes the registry's level, which is the level of a ContainerConfig and 0 for a plain CapacityConfig. | `None`     |

Source code in `src/desflow/resources/container.py`

```python
def __init__(
    self,
    env: Environment,
    sim_id: str,
    container_id: str,
    init: Optional[float] = None,
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
    """
    super().__init__(env, sim_id, container_id, "containers")

    self._level_val = env.registry.get(f"{sim_id}.containers.{container_id}.level")
    max_cap = float(self._max_cap_val.value)
    # Prevent initialization out-of-bounds
    initial_capacity = max(0.0, min(float(self._current_cap_val.value), max_cap))
    if init is None:
        # The registry holds the level as a float, so later updates stay
        # fractional; the configured level keeps the type it was given
        config = env.registry.get_config(f"{sim_id}.containers.{container_id}")
        init = getattr(config, "level", self._level_val.value)
    start = init
    # A number keeps its type, so a whole level stays an int, as SimPy keeps it
    if isinstance(start, bool) or not isinstance(start, (int, float)):
        start = float(start)

    if start > initial_capacity:
        # SimPy's own check, which the capacity of 1 below would not make
        raise ValueError('"init" must be <= "capacity".')
    # SimPy refuses a capacity of 0, which desflow accepts because a capacity
    # changes during a run; the container is built with 1 and held at 0, as a
    # capacity change sets it
    self._container = _Container(
        env, self._level_changed, init=start, capacity=initial_capacity or 1.0
    )
    self._container._capacity = initial_capacity
    self._level_val.track(self._container.level)
    self.env.process(self._watch_level())
    self._register()
```

#### Attributes

##### `level`

float: The current amount of bulk material stored inside the container.

##### `capacity`

float: The current active maximum capacity of the container. This value reflects the live state from the Registry.

#### Functions

##### `put(amount)`

Request to put a specific amount of material into the container.

Parameters:

| Name     | Type    | Description                    | Default    |
| -------- | ------- | ------------------------------ | ---------- |
| `amount` | `float` | The amount of material to add. | *required* |

Returns:

| Type    | Description                                                                 |
| ------- | --------------------------------------------------------------------------- |
| `Event` | simpy.events.Event: A SimPy event that triggers when capacity is available. |

Source code in `src/desflow/resources/container.py`

```python
def put(self, amount: float) -> Event:
    """
    Request to put a specific amount of material into the container.

    Args:
        amount (float): The amount of material to add.

    Returns:
        simpy.events.Event: A SimPy event that triggers when capacity is available.
    """
    return self._container.put(amount)
```

##### `get(amount)`

Request to get a specific amount of material from the container.

Parameters:

| Name     | Type    | Description                         | Default    |
| -------- | ------- | ----------------------------------- | ---------- |
| `amount` | `float` | The amount of material to withdraw. | *required* |

Returns:

| Type           | Description                                                                                            |
| -------------- | ------------------------------------------------------------------------------------------------------ |
| `ContainerGet` | simpy.resources.container.ContainerGet: A SimPy event that triggers when enough material is available. |

Source code in `src/desflow/resources/container.py`

```python
def get(self, amount: float) -> ContainerGet:
    """
    Request to get a specific amount of material from the container.

    Args:
        amount (float): The amount of material to withdraw.

    Returns:
        simpy.resources.container.ContainerGet: A SimPy event that triggers
            when enough material is available.
    """
    return self._container.get(amount)
```

### `DynamicStore(env, sim_id, store_id, priority=False, items=None)`

Bases: `BaseDynamicResource`

A wrapper for SimPy FilterStore and PriorityStore with dynamic capacity updates.

Useful for modeling buffers, queues, or staging areas that hold discrete, heterogeneous items. By setting `priority=True`, the store will automatically sort incoming items (which must be sortable or wrapped in `simpy.PriorityItem`) so high-priority items are retrieved first.

**Capacity Shrinkage Behavior:** If the capacity is dynamically shrunk below the current number of `items` in the store, the existing items are NOT destroyed. The store will temporarily exist in an "over-capacity" state. All pending and future `put` requests will be blocked until downstream processes `get` enough items to bring the total item count below the new capacity.

Attributes:

| Name     | Type          | Description                              |
| -------- | ------------- | ---------------------------------------- |
| `env`    | `Environment` | The active SimPy simulation environment. |
| `sim_id` | `str`         | The parent simulation ID prefix.         |
| `obj_id` | `str`         | The specific store name/identifier.      |

Initializes the DynamicStore and binds its capacity to the registry.

Parameters:

| Name       | Type          | Description                                                                                                                                                                                                                   | Default    |
| ---------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `env`      | `Environment` | The SimPy environment (must include Registry interface).                                                                                                                                                                      | *required* |
| `sim_id`   | `str`         | The parent simulation ID.                                                                                                                                                                                                     | *required* |
| `store_id` | `str`         | The unique name of this store.                                                                                                                                                                                                | *required* |
| `priority` | `bool`        | If True, uses simpy.PriorityStore instead of simpy.FilterStore, which serves first in, first out as simpy.Store does and also takes a filter. Defaults to False.                                                              | `False`    |
| `items`    | `Iterable`    | The items it holds when the run starts, in the order a get returns them. A priority store takes (priority, item) pairs and holds each as a PriorityItem, lowest priority first and in the order given among equal priorities. | `None`     |

Raises:

| Type         | Description                                                                                           |
| ------------ | ----------------------------------------------------------------------------------------------------- |
| `ValueError` | If there are more items than the capacity, or a priority store's item is not a (priority, item) pair. |

Source code in `src/desflow/resources/store.py`

```python
def __init__(
    self,
    env: Environment,
    sim_id: str,
    store_id: str,
    priority: bool = False,
    items: Optional[Iterable[Any]] = None,
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
            first and in the order given among equal priorities.

    Raises:
        ValueError: If there are more items than the capacity, or a priority
            store's item is not a `(priority, item)` pair.
    """
    super().__init__(env, sim_id, store_id, "stores")

    max_cap = int(self._max_cap_val.value)
    # Prevent initialization out-of-bounds
    initial_capacity = max(0, min(int(self._current_cap_val.value), max_cap))

    # Dynamically select the underlying SimPy storage engine
    self.priority = priority
    # A rising number for every priority item, so equal priorities keep the
    # order they were added in and their items are never compared
    self._order = itertools.count()
    kind = _PriorityStore if priority else _Store
    # SimPy refuses a capacity of 0, which desflow accepts because a capacity
    # changes during a run; the store is built with 1 and held at 0, as a
    # capacity change sets it
    self._store = kind(env, self._changed, capacity=initial_capacity or 1)
    self._store._capacity = initial_capacity
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
            heapq.heappush(self._store.items, self.wrap(item[1], item[0]))
        else:
            self._store.items.append(item)
    self._register()
```

#### Attributes

##### `items`

list: The actual list of distinct item objects currently residing in the store.

##### `capacity`

int: The current active maximum slot capacity of the store. This value reflects the live state from the Registry.

#### Functions

##### `put(item)`

Request to put a specific distinct item into the store.

Parameters:

| Name   | Type  | Description                                | Default    |
| ------ | ----- | ------------------------------------------ | ---------- |
| `item` | `Any` | The Python object to place into the store. | *required* |

Returns:

| Type    | Description                                                               |
| ------- | ------------------------------------------------------------------------- |
| `Event` | simpy.events.Event: A SimPy event that triggers when a slot is available. |

Source code in `src/desflow/resources/store.py`

```python
def put(self, item: Any) -> Event:
    """
    Request to put a specific distinct item into the store.

    Args:
        item (Any): The Python object to place into the store.

    Returns:
        simpy.events.Event: A SimPy event that triggers when a slot is available.
    """
    return self._store.put(item)
```

##### `get(filter=None)`

Request to get the next available item from the store.

Parameters:

| Name     | Type                              | Description                                                                                                                                                                                                                                                                                                   | Default |
| -------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `filter` | `Optional[Callable[[Any], bool]]` | For a store without priority, a function of an item that returns True for an item this get may take, as SimPy's FilterStore.get takes. The get takes the oldest matching item and waits while none matches; a waiting get that matches nothing does not stop a later one from taking an item that matches it. | `None`  |

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
def get(self, filter: Optional[Callable[[Any], bool]] = None) -> StoreGet:
    """
    Request to get the next available item from the store.

    Args:
        filter: For a store without priority, a function of an item that
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
    if filter is None:
        return self._store.get()
    if self.priority:
        raise ValueError(
            "a priority store takes no filter: SimPy has no store that both "
            "ranks and filters its items"
        )
    return self._store.get(filter)
```

## Breakdowns, Schedules and Replay

SimPy processes and readers the builder and YAML use, which low-level code starts itself.

### `run_breakdowns(env, resource, sampler, work='interrupt')`

Breaks a resource down and repairs it, for as long as the run lasts.

The time to failure and the repair time are read from the registry, at `<sim_id>.breakdown.<resource>.time_to_failure` and `.repair`, which a `SimParameter`'s `breakdowns` registers, so they can change during the run. Each failure publishes an event with the resource's name as its key and `{"status": "down", "reason": "failure"}`, and each repair one with `{"status": "repaired", "reason": "failure"}`; neither has a task.

Parameters:

| Name       | Type              | Description                                                                                            | Default       |
| ---------- | ----------------- | ------------------------------------------------------------------------------------------------------ | ------------- |
| `env`      | `Any`             | The environment.                                                                                       | *required*    |
| `resource` | `DynamicResource` | The resource that breaks down.                                                                         | *required*    |
| `sampler`  | `Sampler`         | The sampler the times are drawn with.                                                                  | *required*    |
| `work`     | `str`             | interrupt interrupts the tasks being served, which follow their on_interrupt; finish lets them finish. | `'interrupt'` |

Raises:

| Type         | Description                              |
| ------------ | ---------------------------------------- |
| `ValueError` | If work is neither interrupt nor finish. |

Source code in `src/desflow/core/breakdowns.py`

```python
def run_breakdowns(
    env: Any, resource: DynamicResource, sampler: Sampler, work: str = "interrupt"
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

    Raises:
        ValueError: If `work` is neither `interrupt` nor `finish`.
    """
    if work not in BREAKDOWN_WORK:
        raise ValueError(f"work must be interrupt or finish, got '{work}'")
    path = f"{resource.sim_id}.breakdown.{resource.obj_id}"
    resource.has_breakdowns = True
    while True:
        failure = env.registry.get_config(f"{path}.time_to_failure")
        yield env.timeout(sampler.sample(failure))
        resource.fail(interrupt_work=work == "interrupt")
        _publish(env, resource, "down")
        repair = env.registry.get_config(f"{path}.repair")
        yield env.timeout(sampler.sample(repair))
        resource.repair()
        _publish(env, resource, "repaired")
```

### `run_scenario(env, steps, until=None, interrupt=None)`

Applies each step at its time, in time order and, at equal times, in the order given. A step's `at` counts from when this process starts, the start of the run when it is started with the model, so it does not depend on `initial_time`.

A step with a `path` and `value` updates the registry; one with a `record` publishes a ground-truth record of the change (`env.publish_label`); one with an `interrupt`, `{task, where, cause}`, calls `interrupt(task, where=..., cause=...)`, which the builder and YAML pass as `context.interrupt`.

Parameters:

| Name        | Type                           | Description                                                                                                                 | Default    |
| ----------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `env`       | `Any`                          | The environment.                                                                                                            | *required* |
| `steps`     | `Iterable[Mapping[str, Any]]`  | The steps, each a mapping as check_step takes.                                                                              | *required* |
| `until`     | `Optional[float]`              | The run's end, a reading of the clock; steps due at or after it are dropped, as an event due at exactly until does not run. | `None`     |
| `interrupt` | `Optional[Callable[..., Any]]` | What an interrupt step calls; such steps need it.                                                                           | `None`     |

Source code in `src/desflow/core/scenario.py`

```python
def run_scenario(
    env: Any,
    steps: Iterable[Mapping[str, Any]],
    until: Optional[float] = None,
    interrupt: Optional[Callable[..., Any]] = None,
):
    """
    Applies each step at its time, in time order and, at equal times, in the order
    given. A step's `at` counts from when this process starts, the start of the run
    when it is started with the model, so it does not depend on `initial_time`.

    A step with a `path` and `value` updates the registry; one with a `record`
    publishes a ground-truth record of the change (`env.publish_label`); one with an
    `interrupt`, `{task, where, cause}`, calls `interrupt(task, where=..., cause=...)`,
    which the builder and YAML pass as `context.interrupt`.

    Args:
        env: The environment.
        steps: The steps, each a mapping as `check_step` takes.
        until: The run's end, a reading of the clock; steps due at or after it are
            dropped, as an event due at exactly `until` does not run.
        interrupt: What an `interrupt` step calls; such steps need it.
    """
    ordered: List[dict] = sorted(
        (check_step(step) for step in steps), key=lambda step: step["at"]
    )
    registry = env.registry
    # `at` counts from the start of the run, the moment this process starts, so a
    # run with an initial_time of 8 h applies a step at 30 min at 8.5 h
    origin = env.now
    for step in ordered:
        at = origin + step["at"]
        if until is not None and at >= until:
            return
        if at > env.now:
            yield _urgent_timeout(env, at - env.now)
        path, record = step.get("path"), step.get("record")
        old = new = None
        if path is not None:
            old = registry.get(path).value
            registry.update(path, step["value"])
            new = registry.get(path).value
        if record is not None:
            env.publish_label(record, path=path, old=old, new=new)
        action = step.get("interrupt")
        if action is not None:
            if interrupt is None:
                raise ValueError("an interrupt step needs an interrupt function")
            interrupt(
                action["task"], where=action.get("where"), cause=action.get("cause")
            )
```

### `read_trace(source, time='time', unit='s', fields='all', on_invalid='error', start=None, initial_time=0.0)`

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

| Type          | Description                                                                                                                                                                                       |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ValueError`  | If the unit or on_invalid is unknown; a row is out of time order, naming the first one; a row's time is before the start; a field is missing; or a row's time is invalid and on_invalid is error. |
| `ImportError` | If a Parquet file is read without pyarrow.                                                                                                                                                        |

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
        ValueError: If the unit or `on_invalid` is unknown; a row is out of time
            order, naming the first one; a row's time is before the start; a
            field is missing; or a row's time is invalid and `on_invalid` is
            `error`.
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
    rows: List[Row] = []
    last: Optional[float] = None
    for number, record in enumerate(_records(source), start=1):
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
            kept = {key: _value(value) for key, value in record.items()}
        else:
            missing = [name for name in fields if name not in record]
            if missing:
                raise ValueError(
                    f"row {number} has no field {', '.join(missing)}; add it to the "
                    f"row, or leave it out of fields"
                )
            kept = {name: _value(record[name]) for name in fields}
        rows.append(Row(when, number, kept))
    return rows
```

## Admin & Infrastructure

### `KafkaAdminConnector(bootstrap_servers, max_tasks=100, **kwargs)`

Unified Kafka Admin and Monitoring Connector.

This connector acts as a management layer for the simulation's Kafka infrastructure. It handles synchronous administrative tasks (topic creation) using `kafka-python` and asynchronous data operations (sending config, collecting telemetry/events) using `aiokafka`.

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
    """
    self.bootstrap_servers = bootstrap_servers
    self.max_tasks = max_tasks
    self.kwargs = kwargs

    # Event State: sim_id -> service -> task_id -> {status: timestamp}
    self._state: DefaultDict[str, DefaultDict[str, Dict[str, Any]]] = defaultdict(
        lambda: defaultdict(dict)
    )
    # Telemetry State: path_id -> latest_value
    self._vitals: Dict[str, Any] = {}
```

#### Functions

##### `create_topics(topics_config)`

Creates Kafka topics required for the simulation.

This is a synchronous call to ensure that all necessary infrastructure (config, telemetry, and event topics) is ready before the simulation environment starts.

Parameters:

| Name            | Type                   | Description                                                                                                     | Default    |
| --------------- | ---------------------- | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `topics_config` | `List[Dict[str, Any]]` | A list of topic configurations. Each dict should contain 'name', and optionally 'partitions' and 'replication'. | *required* |

Source code in `src/desflow/connectors/admin/kafka.py`

```python
def create_topics(self, topics_config: List[Dict[str, Any]]):
    """
    Creates Kafka topics required for the simulation.

    This is a synchronous call to ensure that all necessary infrastructure
    (config, telemetry, and event topics) is ready before the simulation
    environment starts.

    Args:
        topics_config: A list of topic configurations.
            Each dict should contain 'name', and optionally 'partitions'
            and 'replication'.
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers=self.bootstrap_servers,
        client_id="sim_admin",
        **self.kwargs,
    )

    new_topics = [
        NewTopic(
            name=cfg["name"],
            num_partitions=cfg.get("partitions", 1),
            replication_factor=cfg.get("replication", 1),
        )
        for cfg in topics_config
    ]

    try:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin_client.close()
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

This loop continuously listens to the simulation's egress and updates the connector's internal `_vitals` and `_state` attributes.

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
    the connector's internal `_vitals` and `_state` attributes.

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
            data = json.loads(msg.value.decode("utf-8"))
            self._process_message(data)
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

### `BaseIngress`

Base class for all ingress providers in the simulation.

Ingress providers act as asynchronous listeners that bridge external data sources (such as Kafka topics, Redis channels, or local schedules) with the simulation's internal state. They are responsible for fetching updates and placing them into a thread-safe queue for the registry to process.

#### Functions

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

### `KafkaIngress(topic, bootstrap_servers, topic_deserializers=None, default_deserializer=None, **kwargs)`

Bases: `BaseIngress`

Resilient Kafka consumer for dynamic simulation configuration updates.

This connector subscribes to a specified Kafka topic and decodes incoming messages into state updates for the SimulationRegistry. It supports pluggable deserialization (Avro/JSON) while remaining 100% backward compatible.

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

| Name                   | Type                                       | Description                                                            | Default    |
| ---------------------- | ------------------------------------------ | ---------------------------------------------------------------------- | ---------- |
| `topic`                | `str`                                      | The Kafka topic to subscribe to.                                       | *required* |
| `bootstrap_servers`    | `str`                                      | Kafka broker address or list of addresses.                             | *required* |
| `topic_deserializers`  | `Optional[Dict[str, MessageDeserializer]]` | Mapping of topics to their specific deserializers.                     | `None`     |
| `default_deserializer` | `Optional[MessageDeserializer]`            | Fallback deserializer if the topic isn't explicitly mapped.            | `None`     |
| `**kwargs`             | `Any`                                      | Arbitrary keyword arguments passed to the underlying AIOKafkaConsumer. | `{}`       |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(
    self,
    topic: str,
    bootstrap_servers: str,
    topic_deserializers: Optional[Dict[str, MessageDeserializer]] = None,
    default_deserializer: Optional[MessageDeserializer] = None,
    **kwargs: Any,
):
    """
    Initializes the KafkaIngress with connection and subscription details.

    Args:
        topic: The Kafka topic to subscribe to.
        bootstrap_servers: Kafka broker address or list of addresses.
        topic_deserializers: Mapping of topics to their specific deserializers.
        default_deserializer: Fallback deserializer if the topic isn't explicitly mapped.
        **kwargs: Arbitrary keyword arguments passed to the underlying AIOKafkaConsumer.
    """
    self.topic = topic
    self.bootstrap_servers = bootstrap_servers

    self.topic_deserializers = topic_deserializers or {}
    self.default_deserializer = default_deserializer or JsonDeserializer()

    self.kwargs = kwargs
```

#### Functions

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

### `LocalIngress(schedule)`

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

#### Functions

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

### `PostgresIngress(connection_dsn, table_name='simulation_params', poll_interval=2.0, **kwargs)`

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

#### Functions

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
    await self._init_pool()

    # Phase 1: Load the most recently applied state, so a restart resumes from it
    try:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            # Get the latest applied value for each distinct path
            rows = await conn.fetch(f"""
                SELECT DISTINCT ON (path) path, value
                FROM {self.table_name}
                WHERE is_applied = TRUE
                ORDER BY path, id DESC
            """)
            for row in rows:
                try:
                    path, val = _row_change(row)
                except ValueError as e:
                    logger.warning(f"Skipped a PostgreSQL ingress row: {e}")
                    continue
                ingress_queue.put((path, val))
                logger.info(f"Loaded historical parameter state: {path} = {val}")
    except Exception as e:
        logger.error(
            f"Error loading historical state from {self.table_name}: "
            f"{self._describe(e)}"
        )

    # Phase 2: Continuously poll for new unapplied updates
    while True:
        try:
            assert self.pool is not None
            async with self.pool.acquire() as conn:
                # Fetch all unapplied parameter updates
                rows = await conn.fetch(f"""
                    SELECT id, path, value
                    FROM {self.table_name}
                    WHERE is_applied = FALSE
                    ORDER BY id ASC
                """)

                if rows:
                    applied_ids = []
                    for row in rows:
                        # A bad row is marked applied too, so it is skipped once
                        applied_ids.append(row["id"])
                        try:
                            path, val = _row_change(row)
                        except ValueError as e:
                            logger.warning(
                                f"Skipped a PostgreSQL ingress row {row['id']}: {e}"
                            )
                            continue

                        # Send to the simulation registry queue
                        ingress_queue.put((path, val))
                        logger.info(f"Ingested parameter update: {path} = {val}")

                    # Mark the rows as applied so we don't process them again
                    await conn.execute(
                        f"""
                        UPDATE {self.table_name}
                        SET is_applied = TRUE
                        WHERE id = ANY($1::int[])
                    """,
                        applied_ids,
                    )

        except Exception as e:
            logger.error(
                f"Error polling PostgresIngress table {self.table_name}: "
                f"{self._describe(e)}"
            )

        # Wait before polling again
        await asyncio.sleep(self.poll_interval)
```

### `RedisIngress(url, channel_name='params', poll_interval=0.5, **kwargs)`

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
| `url`           | `str`   | Redis connection URL (e.g., redis://localhost:6379/0).        | *required* |
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
        url: Redis connection URL (e.g., redis://localhost:6379/0).
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

#### Functions

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

### `MessageDeserializer`

Bases: `Protocol`

Protocol defining the interface for all Kafka ingress deserializers.

Any custom deserializer passed to `KafkaIngress` must implement the `deserialize` method according to this signature.

#### Functions

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

### `JsonDeserializer`

Default fallback deserializer providing backward compatibility via standard JSON.

This deserializer assumes the incoming payload is a UTF-8 encoded JSON string.

#### Functions

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

### `ConfluentAvroDeserializer(registry_url, **registry_kwargs)`

Lazy-loaded deserializer for Confluent Schema Registry.

This class converts Avro-encoded byte strings back into Python dictionaries. It automatically fetches the appropriate schema from the registry using the schema ID embedded within the message's binary payload, so no `schema_str` is required during initialization.

Initializes the Confluent Avro deserializer.

Parameters:

| Name                | Type  | Description                                                                                                                   | Default    |
| ------------------- | ----- | ----------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_url`      | `str` | The base URL of the Confluent Schema Registry.                                                                                | *required* |
| `**registry_kwargs` | `Any` | Additional configuration arguments passed directly to the SchemaRegistryClient (e.g., {'basic.auth.user.info': 'user:pass'}). | `{}`       |

Raises:

| Type          | Description                                        |
| ------------- | -------------------------------------------------- |
| `ImportError` | If the 'confluent-kafka' package is not installed. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(self, registry_url: str, **registry_kwargs: Any):
    """
    Initializes the Confluent Avro deserializer.

    Args:
        registry_url (str): The base URL of the Confluent Schema Registry.
        **registry_kwargs: Additional configuration arguments passed directly
            to the `SchemaRegistryClient` (e.g., `{'basic.auth.user.info': 'user:pass'}`).

    Raises:
        ImportError: If the 'confluent-kafka' package is not installed.
    """
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        from confluent_kafka.serialization import MessageField, SerializationContext
    except ImportError:
        raise ImportError(
            "The 'confluent-kafka' package is required to use ConfluentAvroDeserializer. "
            "Install it using: pip install desflow[confluent]"
        )

    conf = {"url": registry_url, **registry_kwargs}
    client = SchemaRegistryClient(conf)

    # No schema string required! It reads the Schema ID from the message bytes.
    self.avro_deserializer = AvroDeserializer(client)
    self.SerializationContext = SerializationContext
    self.MessageField = MessageField
```

#### Functions

##### `deserialize(topic, payload)`

Deserializes an Avro-encoded byte string.

Parameters:

| Name      | Type    | Description                                               | Default    |
| --------- | ------- | --------------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed.      | *required* |
| `payload` | `bytes` | The Avro-encoded binary payload containing the schema ID. | *required* |

Returns:

| Name  | Type  | Description                                   |
| ----- | ----- | --------------------------------------------- |
| `Any` | `Any` | The deserialized data as a Python dictionary. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes an Avro-encoded byte string.

    Args:
        topic (str): The Kafka topic from which the message was consumed.
        payload (bytes): The Avro-encoded binary payload containing the schema ID.

    Returns:
        Any: The deserialized data as a Python dictionary.
    """
    ctx = self.SerializationContext(topic, self.MessageField.VALUE)
    return self.avro_deserializer(payload, ctx)
```

### `GlueAvroDeserializer(registry_name, **boto3_kwargs)`

Lazy-loaded deserializer for AWS Glue Schema Registry.

This class converts Avro-encoded byte strings back into Python dictionaries, integrating directly with AWS Glue Schema Registry. It resolves schemas dynamically based on the metadata embedded in the AWS Glue message payload.

Initializes the AWS Glue Avro deserializer.

Parameters:

| Name             | Type  | Description                                                                                                     | Default    |
| ---------------- | ----- | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_name`  | `str` | The name of the AWS Glue Schema Registry.                                                                       | *required* |
| `**boto3_kwargs` | `Any` | Additional arguments passed directly to the boto3.client initialization (e.g., region_name, aws_access_key_id). | `{}`       |

Raises:

| Type          | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| `ImportError` | If the 'aws-glue-schema-registry' or 'boto3' packages are not installed. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def __init__(self, registry_name: str, **boto3_kwargs: Any):
    """
    Initializes the AWS Glue Avro deserializer.

    Args:
        registry_name (str): The name of the AWS Glue Schema Registry.
        **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
            initialization (e.g., `region_name`, `aws_access_key_id`).

    Raises:
        ImportError: If the 'aws-glue-schema-registry' or 'boto3' packages are not installed.
    """
    try:
        import boto3
        from aws_schema_registry import SchemaRegistryClient
        from aws_schema_registry.adapter.kafka import KafkaDeserializer
    except ImportError:
        raise ImportError(
            "The 'aws-glue-schema-registry' and 'boto3' packages are required. "
            "Install them using: pip install desflow[glue]"
        )

    if "region_name" not in boto3_kwargs:
        boto3_kwargs["region_name"] = "us-east-1"

    glue_client = boto3.client("glue", **boto3_kwargs)
    registry_client = SchemaRegistryClient(glue_client, registry_name=registry_name)

    self.deserializer = KafkaDeserializer(registry_client)
```

#### Functions

##### `deserialize(topic, payload)`

Deserializes an Avro-encoded byte string using AWS Glue.

Parameters:

| Name      | Type    | Description                                          | Default    |
| --------- | ------- | ---------------------------------------------------- | ---------- |
| `topic`   | `str`   | The Kafka topic from which the message was consumed. | *required* |
| `payload` | `bytes` | The Avro-encoded binary payload.                     | *required* |

Returns:

| Name  | Type  | Description                                                          |
| ----- | ----- | -------------------------------------------------------------------- |
| `Any` | `Any` | The raw data dictionary extracted from the AWS DataAndSchema object. |

Source code in `src/desflow/connectors/ingress/kafka.py`

```python
def deserialize(self, topic: str, payload: bytes) -> Any:
    """
    Deserializes an Avro-encoded byte string using AWS Glue.

    Args:
        topic (str): The Kafka topic from which the message was consumed.
        payload (bytes): The Avro-encoded binary payload.

    Returns:
        Any: The raw data dictionary extracted from the AWS DataAndSchema object.
    """
    # AWS returns a DataAndSchema object, we just want the raw data dictionary
    result = self.deserializer.deserialize(topic, payload)
    return result.data
```

## Connectors (Egress)

### `BaseEgress`

Base class for all egress providers in the simulation.

Egress providers act as asynchronous bridges that consume processed simulation data (telemetry and events) from a thread-safe internal queue and transmit it to external destinations such as Kafka, databases, or the console.

#### Functions

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

### `KafkaEgress(bootstrap_servers, telemetry_topic='sim-telemetry', event_topic='sim-events', topic_router=None, topic_serializers=None, default_serializer=None, include_stream_type=False, **kwargs)`

Bases: `BaseEgress`

High-throughput Kafka producer for simulation telemetry and events.

This connector utilizes `aiokafka` for asynchronous I/O and `orjson` for fast serialization. It implements a resilient connection loop with exponential backoff.

By default, data is routed to the `telemetry_topic` or `event_topic` based on its `stream_type`. If a `topic_router` callable is provided, topic selection is delegated to that function instead, allowing for advanced multiplexing (e.g., splitting ML vs. UI events).

Without a `topic_router`, the event and telemetry topics are created at start when they do not exist, with one partition and a replication factor of one. With a router the topics are left to the caller, because only the router knows which ones it uses.

Note

By default `stream_type` is treated as routing metadata and is left out of the published message, because the topic already identifies the stream. `ParquetStorageEgress` keeps `stream_type` as a column, so a downstream filter written against Parquet files matches nothing when it is pointed at a Kafka topic. Set `include_stream_type=True` to keep the field in the message and make both paths carry it.

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
    bootstrap_servers="localhost:9092",
    topic_router=custom_topic_router
)
```

Initializes the KafkaEgress with topic and connection settings.

Parameters:

| Name                  | Type                                     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Default           |
| --------------------- | ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- |
| `bootstrap_servers`   | `str`                                    | Comma-separated list of Kafka brokers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | *required*        |
| `telemetry_topic`     | `str`                                    | Destination topic for telemetry stream.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `'sim-telemetry'` |
| `event_topic`         | `str`                                    | Destination topic for event stream.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `'sim-events'`    |
| `topic_router`        | `Optional[Callable[[dict], str]]`        | Optional callable to dynamically route payloads to specific topics.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `None`            |
| `topic_serializers`   | `Optional[Dict[str, MessageSerializer]]` | Mapping of target topics to their specific MessageSerializer implementations.                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `None`            |
| `default_serializer`  | `Optional[MessageSerializer]`            | The fallback serializer to use if a topic lacks a specific mapping. Defaults to JsonSerializer.                                                                                                                                                                                                                                                                                                                                                                                                                                              | `None`            |
| `include_stream_type` | `bool`                                   | Keep stream_type in the published message. Defaults to False, which is the wire format every existing deployment already reads. Turn it on when the same consumer code has to work against both a Kafka topic and Parquet files, since the Parquet writer keeps the field. An Avro serializer only carries the field if the registered schema declares it: both ConfluentAvroSerializer and GlueAvroSerializer encode field by field from the schema, so an undeclared stream_type is dropped without an error rather than failing the send. | `False`           |
| `**kwargs`            | `Any`                                    | Additional overrides for the AIOKafkaProducer configuration.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `{}`              |

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
        **kwargs: Additional overrides for the AIOKafkaProducer configuration.
    """
    self.telemetry_topic = telemetry_topic
    self.event_topic = event_topic
    self.topic_router = topic_router
    self.include_stream_type = include_stream_type

    # Initialize serializers in a backward-compatible way
    self.topic_serializers = topic_serializers or {}
    self.default_serializer = default_serializer or JsonSerializer()

    # High-performance defaults for 100k/sec
    self.producer_config = {
        "bootstrap_servers": bootstrap_servers,
        "linger_ms": 10,  # Batch messages for 10ms before sending
        "compression_type": "lz4",  # Fast compression for high volume
        "max_batch_size": 131072,  # 128KB batch size
        **kwargs,
    }
```

#### Functions

##### `run(egress_queue)`

The main execution loop that consumes the egress queue and publishes to Kafka.

This method maintains a persistent connection to Kafka. If the connection is lost, it implements an exponential backoff retry strategy. It polls the internal `egress_queue` for batches of data, determines the target topic based on the 'stream_type', and performs asynchronous sends using the configured serializers.

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
    the configured serializers.

    Args:
        egress_queue: A thread-safe queue containing lists of dictionaries
            or Pydantic models generated by the EgressMixIn.

    Note:
        The loop exits gracefully upon receiving an `asyncio.CancelledError`,
        ensuring the Kafka producer is stopped correctly.
    """
    if self.topic_router is None:
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
                        # Kafka routes by no reserved key, so it writes none
                        batch = drop_reserved_keys(batch, self)

                        for data in batch:
                            if self.topic_router:
                                # Use externalized logic if provided
                                topic = self.topic_router(data)
                                stream = data.get("stream_type", "event")
                            else:
                                # Fallback to standard desflow routing
                                stream = data["stream_type"]
                                topic = (
                                    self.telemetry_topic
                                    if stream == "telemetry"
                                    else self.event_topic
                                )

                            # Extract the Kafka Key
                            key = None
                            if stream == "telemetry":
                                key = (
                                    str(data["path_id"]).encode()
                                    if "path_id" in data
                                    else None
                                )
                            else:
                                key = (
                                    str(data["key"]).encode()
                                    if "key" in data
                                    else None
                                )

                            # Build the payload without touching the record itself.
                            # Every egress provider receives the same dictionary
                            # objects, so removing a key here would also remove it
                            # from what a Parquet or Redis sink writes.
                            if self.include_stream_type:
                                payload = data
                            else:
                                payload = {
                                    k: v
                                    for k, v in data.items()
                                    if k != "stream_type"
                                }

                            # Apply configured Serializer strategy
                            serializer = self.topic_serializers.get(
                                topic, self.default_serializer
                            )
                            payload_bytes = serializer.serialize(topic, payload)

                            await producer.send(topic, value=payload_bytes, key=key)

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

### `ConsoleEgress`

Bases: `BaseEgress`

Local egress provider that outputs simulation data to the standard console.

This connector serves as a debugging and local development tool. It processes batched data from the simulation and prints formatted entries to stdout, distinguishing between telemetry metrics and discrete events via prefixes.

#### Functions

##### `run(egress_queue)`

Continuously polls the egress queue and prints formatted results.

This method implements an asynchronous polling loop. It extracts batches from the internal queue and writes each batch with one log call at INFO level, one line per record, each line starting with a prefix:

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
    Continuously polls the egress queue and prints formatted results.

    This method implements an asynchronous polling loop. It extracts batches
    from the internal queue and writes each batch with one log call at INFO
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
        if not batch or not logger.isEnabledFor(logging.INFO):
            continue
        batch = drop_reserved_keys(batch, self)
        # Each line is the record without its stream type, which the prefix
        # names. The record is not altered: every egress provider is handed the
        # same dictionary objects, so removing a key here would also remove it
        # from what another attached sink writes.
        lines = [
            f"{_PREFIXES.get(data.get('stream_type', 'unknown'), '[EVT]')} "
            f"{ {k: v for k, v in data.items() if k != 'stream_type'} }"
            for data in batch
        ]
        logger.info("\n".join(lines))
```

### `PostgresEgress(connection_dsn, table_name=None, upsert_keys=None, tables=None, **kwargs)`

Bases: `BaseEgress`

Asynchronous egress provider for persisting simulation data to PostgreSQL.

This connector handles long-term storage of simulation results or continuous generation of relational CDC data, performing bulk inserts of batched data into a specified PostgreSQL table using asyncpg.

By default a record whose key already exists is skipped (`ON CONFLICT DO NOTHING`). Name the key columns in `upsert_keys` to update the existing row instead, which a simulation needs when it changes rows it wrote earlier, such as an order moving from processing to shipped.

An ISO time string bound for a timestamp or date column is converted to a `datetime` or `date` first, because asyncpg rejects the string itself.

Tables named in `tables` are created at start when they do not exist, so a run needs no separate schema step.

An event's row also carries the record's `key` as `event_id`, and its `task` and `instance`, unless the payload has fields of those names. A ground-truth record, sent to a sink added with `when: labels`, is written as `sim_ts`, `timestamp`, `path`, `old`, `new` and `label`, the last three as JSON text.

Examples:

Updating orders as their status changes:

```python
egress = PostgresEgress(
    "postgresql://user:password@localhost:5432/db",
    table_name="orders",
    upsert_keys=["order_id"],
)
```

Creating the table at start, in the shape a YAML file gives:

```python
egress = PostgresEgress(
    "postgresql://user:password@localhost:5432/db",
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

| Name             | Type             | Description                                       | Default                                                                                                                                                                                         |
| ---------------- | ---------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection_dsn` | `str`            | PostgreSQL connection string (DSN).               | *required*                                                                                                                                                                                      |
| `table_name`     | \`str            | None\`                                            | Target table for simulation records. Defaults to the one table in tables when it names exactly one, and to "simulation_data" when tables is not given.                                          |
| `upsert_keys`    | \`list[str]      | None\`                                            | Optional key columns. When given, a record whose key already exists updates that row's other columns, and the columns need a unique index or constraint. When None, such a record is skipped.   |
| `tables`         | \`dict[str, Any] | None\`                                            | Optional tables to create at start when they do not exist. Each table name maps to columns, a mapping of column name to SQL type, and an optional primary_key, a column name or a list of them. |
| `**kwargs`       | `Any`            | Additional connection pool arguments for asyncpg. | `{}`                                                                                                                                                                                            |

Raises:

| Type         | Description                                                                                                          |
| ------------ | -------------------------------------------------------------------------------------------------------------------- |
| `ValueError` | If a tables entry is malformed, or if tables names several tables and table_name does not say which one to write to. |

Source code in `src/desflow/connectors/egress/postgres.py`

```python
def __init__(
    self,
    connection_dsn: str,
    table_name: str | None = None,
    upsert_keys: list[str] | None = None,
    tables: dict[str, Any] | None = None,
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
        **kwargs: Additional connection pool arguments for asyncpg.

    Raises:
        ValueError: If a `tables` entry is malformed, or if `tables` names
            several tables and `table_name` does not say which one to write to.
    """
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

#### Functions

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
    await self._init_pool()

    while True:
        try:
            # Receive a batch (list) of dictionaries
            batch = egress_queue.get_nowait()
            if not batch:
                continue

            # Support multi-table multiplexing using a __table__ key
            clean_batch = []
            for item in batch:
                if item.get("stream_type") == "label":
                    clean_batch.append(_label_row(item))
                    continue
                # `item` is an EventPayload or TelemetryPayload dict.
                # The actual user data is in `item["value"]`.
                payload = item.get("value")

                if not isinstance(payload, dict):
                    # PostgresEgress requires dictionaries (or Pydantic models dumped to dicts)
                    continue

                # Merge the simulation metadata into the payload so it CAN be inserted if the user configured their schema to accept it
                payload_copy = payload.copy()
                payload_copy["sim_ts"] = item.get("sim_ts")
                payload_copy["timestamp"] = item.get("timestamp")

                if "key" in item:
                    payload_copy["event_id"] = item["key"]
                    # A payload field of the same name wins, as in flat files
                    for field in ("task", "instance"):
                        payload_copy.setdefault(field, item.get(field))
                if "path_id" in item:
                    payload_copy["path_id"] = item["path_id"]

                # Determine target table, defaulting to self.table_name if not specified
                target = payload_copy.get("__table__", self.table_name)
                if target == self.table_name:
                    payload_copy.pop("__table__", None)
                    clean_batch.append(payload_copy)

            if not clean_batch:
                continue

            # The records carry times as ISO strings, which asyncpg rejects for
            # a timestamp or date column.
            clean_batch = parse_iso_columns(clean_batch, self.time_columns)

            # Each run of records with the same columns is one statement, in the
            # order they arrived. A statement built from one record's columns
            # would set another record's missing columns to NULL on an upsert.
            runs = self._column_runs(clean_batch)
            if not runs:
                logger.warning(
                    f"No matching columns for table {self.table_name}. Skipping."
                )
                continue

            assert self.pool is not None
            async with self.pool.acquire() as conn:
                for keys, records in runs:
                    values = [tuple(data.get(k) for k in keys) for data in records]
                    await conn.executemany(self._insert_query(keys), values)

            written = sum(len(records) for _, records in runs)
            logger.debug(f"Inserted {written} records into {self.table_name}")

        except queue.Empty:
            # Yield to the event loop if the queue is empty
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error inserting batch into {self.table_name}: {e}")
            # Back-off on database failures to prevent rapid crash looping
            await asyncio.sleep(1)
```

### `RedisEgress(url, stream_name='simulation_data', **kwargs)`

Bases: `BaseEgress`

High-throughput asynchronous egress provider for streaming simulation data to Redis.

This connector utilizes `redis.asyncio` for asynchronous I/O and publishes batched simulation results (events and telemetry) to Redis Streams (`XADD`) for real-time consumers.

By default, it uses a unified stream, but supports dynamic stream multiplexing by reading the `__stream__` key from the data payloads.

Attributes:

| Name          | Type    | Description                                                               |
| ------------- | ------- | ------------------------------------------------------------------------- |
| `url`         | `str`   | The connection URL for the Redis server (e.g., redis://localhost:6379/0). |
| `stream_name` | `str`   | The default target Redis Stream name.                                     |
| `client`      | \`Redis | None\`                                                                    |
| `**kwargs`    | \`Redis | None\`                                                                    |

Initializes the RedisEgress with topic and connection settings.

Parameters:

| Name          | Type  | Description                                            | Default             |
| ------------- | ----- | ------------------------------------------------------ | ------------------- |
| `url`         | `str` | Redis connection URL (e.g., redis://localhost:6379/0). | *required*          |
| `stream_name` | `str` | Target Redis Stream name.                              | `'simulation_data'` |
| `**kwargs`    | `Any` | Additional arguments for redis.from_url.               | `{}`                |

Source code in `src/desflow/connectors/egress/redis.py`

```python
def __init__(self, url: str, stream_name: str = "simulation_data", **kwargs: Any):
    """
    Initializes the RedisEgress with topic and connection settings.

    Args:
        url: Redis connection URL (e.g., redis://localhost:6379/0).
        stream_name: Target Redis Stream name.
        **kwargs: Additional arguments for `redis.from_url`.
    """
    self.url = url
    self.stream_name = stream_name
    self.kwargs = kwargs
    self.client: redis.Redis | None = None
```

#### Functions

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
    await self._init_client()
    assert self.client is not None

    while True:
        try:
            batch = egress_queue.get_nowait()
            if not batch:
                continue

            pipe = self.client.pipeline()
            count = 0
            for item in batch:
                # Multi-stream multiplexing. `item` is an EventPayload or
                # TelemetryPayload dict, and the caller's own data sits under
                # `value`, so `__stream__` is looked for there first. Reading only
                # the top level found nothing, because publish_event never puts it
                # there, so every record went to the default stream and the
                # routing silently did nothing. PostgresEgress unwraps the same
                # way for `__table__`, which is why table routing always worked.
                value = item.get("value")
                nested = value if isinstance(value, dict) else {}
                target = nested.get(
                    "__stream__", item.get("__stream__", self.stream_name)
                )

                item_copy = item.copy()
                item_copy.pop("__stream__", None)
                if "__stream__" in nested:
                    item_copy["value"] = {
                        k: v for k, v in nested.items() if k != "__stream__"
                    }

                # Redis Streams expect a dict of string fields.
                # We serialize the entire item into a single 'payload' field.
                payload = orjson.dumps(item_copy).decode("utf-8")
                pipe.xadd(target, {"payload": payload})
                count += 1

            if count > 0:
                await pipe.execute()
            logger.debug(f"Inserted {count} records into Redis Streams")

        except queue.Empty:
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error publishing batch to Redis: {e}")
            await asyncio.sleep(1)
```

### `JsonlStorageEgress(default_path=None, filesystem=None, path_router=None)`

Bases: `BaseEgress`

High-throughput JSONL batch writer for local file systems or object storage.

This connector uses PyArrow's Virtual File System (VFS) to write data to local disks or S3-compatible storage such as AWS S3 or SeaweedFS. It writes a uniquely named file for every batch, so no two writes go to the same file and a file, once written, is never changed.

By default, events are appended to the `default_path` as flat rows: each event's `value` mapping is unpacked into top-level fields, and telemetry is left out. If a `path_router` callable is provided, destination logic is delegated to that function, allowing for advanced multiplexing (e.g., splitting logs vs. errors) or dynamically dropping specific records by returning `None`. Records are then written as the router leaves them.

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
    """
    _check_filesystem(filesystem)
    self.default_path = default_path
    self.filesystem = filesystem
    self.path_router = path_router
    self._folders: set = set()
```

#### Functions

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
    batches_processed = 0

    try:
        while True:
            try:
                batch = egress_queue.get_nowait()
                self.active_tasks = getattr(self, "active_tasks", 0) + 1
                try:
                    await asyncio.to_thread(self._write_batch, batch)
                finally:
                    self.active_tasks -= 1
                # Log a progress heartbeat every 10 batches
                batches_processed += 1
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
            f"JsonlStorageEgress shut down requested. "
            f"Successfully wrote {batches_processed} total chunks to storage."
        )
```

### `ParquetStorageEgress(default_path=None, filesystem=None, path_router=None)`

Bases: `BaseEgress`

Columnar Parquet writer for local disks or object storage.

This connector buffers simulation records and converts them into heavily compressed Parquet tables using `pyarrow`. It natively supports writing to local disks or S3-compatible object stores (AWS S3, SeaweedFS) via PyArrow's VFS.

To support massive parallel processing (e.g., Athena, Databricks), it implements file rotation (chunking), creating a uniquely named Parquet part-file for every flush of the buffer. A file's columns come from every row of its batch; a column a later batch adds widens the path's schema, and each batch is cast to it, so a column keeps its type across chunks.

Without a `path_router`, events are written to `default_path` as flat rows: each event's `value` mapping becomes columns, and telemetry is left out. With a router, records are written as the router leaves them.

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
s3 = fs.S3FileSystem(endpoint_override="localhost:8333", scheme="http")

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
    """
    _check_filesystem(filesystem)
    self.default_path = default_path
    self.filesystem = filesystem
    self.path_router = path_router
    self._folders: set = set()
    self.schemas: Dict[str, Any] = {}
```

#### Functions

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
    batches_processed = 0

    try:
        while True:
            try:
                batch = egress_queue.get_nowait()
                self.active_tasks = getattr(self, "active_tasks", 0) + 1
                written = 0
                try:
                    written = await asyncio.to_thread(
                        self._write_batch, batch, pa, pq
                    )
                except Exception as exc:
                    # A batch that fails is logged, and the sink goes on
                    logger.error(f"Parquet Writer: batch not written: {exc}")
                finally:
                    self.active_tasks -= 1
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

### `IcebergStorageEgress(catalog, default_table=None, table_router=None, schemas=None, locations=None, upsert_keys=None)`

Bases: `BaseEgress`

Apache Iceberg table writer for lakehouse ingestion patterns.

`ParquetStorageEgress` writes files and leaves registration to the caller. This connector appends straight into an Iceberg table instead, so one run can feed a stream sink and a queryable table in the same pass. Each flush of a provider's buffer becomes exactly one Iceberg commit, which is why the buffer size matters: every commit writes a manifest, a manifest list and a new `metadata.json`, and query planning degrades as the snapshot count grows. Give this provider a large `batch_size` on `add_egress` so the run produces a handful of snapshots rather than hundreds.

Without a `table_router`, events are written to `default_table` as flat rows: each event's `value` mapping becomes columns, and telemetry is left out. With a router, records are written as the router leaves them.

By default every record is appended. Name a table's key columns in `upsert_keys` to write it with PyIceberg's `upsert` instead: a record whose key exists updates that row, and a new key is inserted, so rerunning a seeded simulation over the same window does not duplicate rows. An upsert reads the matching rows before writing, so it is slower than an append, and it never deletes rows. It replaces the whole row, so every record must carry every column: a missing column is written as null. Within one flush, the last record for a key wins. An upsert flush is still one commit, but it can add up to three snapshots: an overwrite and two appends.

The catalog is supplied by the caller, either as a built `Catalog` or as a mapping of PyIceberg catalog properties that is passed to `load_catalog`. The supported catalog is Iceberg REST, which is what the integration tests run against.

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
    "odctl", uri="http://localhost:8181", warehouse="s3://warehouse/"
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

| Name            | Type                                        | Description                                                                                                                                                                                                                                                                                                                                      | Default    |
| --------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- |
| `catalog`       | `Any`                                       | An instantiated pyiceberg Catalog, already configured with its URI, warehouse and credentials. A mapping of PyIceberg catalog properties (uri, warehouse, s3.endpoint and so on) is also accepted: the catalog is then built with PyIceberg's load_catalog on the first write, with an optional name key as the catalog name.                    | *required* |
| `default_table` | `Optional[str]`                             | The target namespace.table used when no router is given. Only events are written there, with their value mapping unpacked into columns.                                                                                                                                                                                                          | `None`     |
| `table_router`  | `Optional[Callable[[dict], Optional[str]]]` | A function taking a dict payload and returning a namespace.table identifier, or None to drop the record.                                                                                                                                                                                                                                         | `None`     |
| `schemas`       | `Optional[Dict[str, Any]]`                  | Optional PyArrow schema per table identifier. A table named here is created with that schema and every batch is cast to it, instead of the schema being inferred from the first batch. A schema can also be a mapping of column name to an Iceberg type name: string, int, long, float, double, boolean, timestamp, timestamptz, date or binary. | `None`     |
| `locations`     | `Optional[Dict[str, str]]`                  | Optional storage location per table identifier. Pin this when a consumer reads the table by path, because a catalog with unique-table-location set otherwise appends a random suffix.                                                                                                                                                            | `None`     |
| `upsert_keys`   | `Optional[Dict[str, list]]`                 | Optional key columns per table identifier. A table named here is written with upsert on those columns; any other table is appended to.                                                                                                                                                                                                           | `None`     |

Raises:

| Type          | Description                                                                                               |
| ------------- | --------------------------------------------------------------------------------------------------------- |
| `ValueError`  | If neither default_table nor table_router is given, since the connector would then have nowhere to write. |
| `ImportError` | If upsert_keys is given and the installed pyiceberg is older than 0.9.0, which added Table.upsert.        |

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
            appended to.

    Raises:
        ValueError: If neither `default_table` nor `table_router` is given, since
            the connector would then have nowhere to write.
        ImportError: If `upsert_keys` is given and the installed pyiceberg is
            older than 0.9.0, which added `Table.upsert`.
    """
    if default_table is None and table_router is None:
        raise ValueError(
            "IcebergStorageEgress needs default_table or table_router; "
            "without one it has no table to write to"
        )
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
    self._tables: Dict[str, Any] = {}
```

#### Functions

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

    batches_processed = 0

    try:
        while True:
            try:
                batch = egress_queue.get_nowait()
                self.active_tasks = getattr(self, "active_tasks", 0) + 1
                written = 0
                try:
                    written = await asyncio.to_thread(self._write_batch, batch, pa)
                except Exception as exc:
                    # A batch that fails is logged, and the sink goes on
                    logger.error(f"Iceberg Writer: batch not written: {exc}")
                finally:
                    self.active_tasks -= 1
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

### `MessageSerializer`

Bases: `Protocol`

Protocol defining the interface for all Kafka egress serializers.

Any custom serializer passed to `KafkaEgress` must implement the `serialize` method according to this signature.

#### Functions

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

### `JsonSerializer`

Default fallback serializer providing backward compatibility via orjson.

This serializer converts dictionaries or Pydantic models into standard JSON byte strings.

#### Functions

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

### `ConfluentAvroSerializer(registry_url, schema_str, **registry_kwargs)`

Lazy-loaded serializer for Confluent Schema Registry using confluent-kafka.

This class converts simulation data into Avro-encoded byte strings, automatically fetching and validating against the schema from a Confluent-compatible Schema Registry.

Initializes the Confluent Avro serializer.

Parameters:

| Name                | Type  | Description                                                                                                                                                                | Default    |
| ------------------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_url`      | `str` | The base URL of the Confluent Schema Registry.                                                                                                                             | *required* |
| `schema_str`        | `str` | The Avro schema defined as a JSON string (typically generated from a dataclasses-avroschema AvroBaseModel via .avro_schema(); a plain Pydantic model does not provide it). | *required* |
| `**registry_kwargs` | `Any` | Additional configuration arguments passed directly to the SchemaRegistryClient (e.g., {'basic.auth.user.info': 'user:pass'}).                                              | `{}`       |

Raises:

| Type          | Description                                        |
| ------------- | -------------------------------------------------- |
| `ImportError` | If the 'confluent-kafka' package is not installed. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def __init__(self, registry_url: str, schema_str: str, **registry_kwargs: Any):
    """
    Initializes the Confluent Avro serializer.

    Args:
        registry_url (str): The base URL of the Confluent Schema Registry.
        schema_str (str): The Avro schema defined as a JSON string (typically
            generated from a dataclasses-avroschema `AvroBaseModel` via
            `.avro_schema()`; a plain Pydantic model does not provide it).
        **registry_kwargs: Additional configuration arguments passed directly
            to the `SchemaRegistryClient` (e.g., `{'basic.auth.user.info': 'user:pass'}`).

    Raises:
        ImportError: If the 'confluent-kafka' package is not installed.
    """
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext
    except ImportError:
        raise ImportError(
            "The 'confluent-kafka' package is required to use ConfluentAvroSerializer. "
            "Install it using: pip install desflow[confluent]"
        )

    # Merge the mandatory URL with any additional args (like basic.auth.user.info)
    conf = {"url": registry_url, **registry_kwargs}
    client = SchemaRegistryClient(conf)

    self.avro_serializer = AvroSerializer(client, schema_str)
    self.SerializationContext = SerializationContext
    self.MessageField = MessageField
```

#### Functions

##### `serialize(topic, data)`

Serializes the given data into an Avro byte string.

Parameters:

| Name    | Type  | Description                                              | Default    |
| ------- | ----- | -------------------------------------------------------- | ---------- |
| `topic` | `str` | The target Kafka topic, used for schema subject naming.  | *required* |
| `data`  | `Any` | The payload to serialize (dictionary or Pydantic model). | *required* |

Returns:

| Name    | Type    | Description                   |
| ------- | ------- | ----------------------------- |
| `bytes` | `bytes` | The Avro-encoded byte string. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given data into an Avro byte string.

    Args:
        topic (str): The target Kafka topic, used for schema subject naming.
        data (Any): The payload to serialize (dictionary or Pydantic model).

    Returns:
        bytes: The Avro-encoded byte string.
    """
    payload = extract_dict(data)
    ctx = self.SerializationContext(topic, self.MessageField.VALUE)
    return self.avro_serializer(payload, ctx)
```

### `GlueAvroSerializer(registry_name, schema_str, **boto3_kwargs)`

Lazy-loaded serializer for AWS Glue Schema Registry using boto3.

This class converts simulation data into Avro-encoded byte strings, integrating directly with AWS Glue Schema Registry for schema validation.

Initializes the AWS Glue Avro serializer.

Parameters:

| Name             | Type  | Description                                                                                                                                                                | Default    |
| ---------------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `registry_name`  | `str` | The name of the AWS Glue Schema Registry.                                                                                                                                  | *required* |
| `schema_str`     | `str` | The Avro schema defined as a JSON string (typically generated from a dataclasses-avroschema AvroBaseModel via .avro_schema(); a plain Pydantic model does not provide it). | *required* |
| `**boto3_kwargs` | `Any` | Additional arguments passed directly to the boto3.client initialization (e.g., region_name, aws_access_key_id).                                                            | `{}`       |

Raises:

| Type          | Description                                                              |
| ------------- | ------------------------------------------------------------------------ |
| `ImportError` | If the 'aws-glue-schema-registry' or 'boto3' packages are not installed. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def __init__(self, registry_name: str, schema_str: str, **boto3_kwargs: Any):
    """
    Initializes the AWS Glue Avro serializer.

    Args:
        registry_name (str): The name of the AWS Glue Schema Registry.
        schema_str (str): The Avro schema defined as a JSON string (typically
            generated from a dataclasses-avroschema `AvroBaseModel` via
            `.avro_schema()`; a plain Pydantic model does not provide it).
        **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
            initialization (e.g., `region_name`, `aws_access_key_id`).

    Raises:
        ImportError: If the 'aws-glue-schema-registry' or 'boto3' packages are not installed.
    """
    try:
        import boto3
        from aws_schema_registry import SchemaRegistryClient
        from aws_schema_registry.adapter.kafka import KafkaSerializer
        from aws_schema_registry.avro import AvroSchema
    except ImportError:
        raise ImportError(
            "The 'aws-glue-schema-registry' and 'boto3' packages are required. "
            "Install them using: pip install desflow[glue]"
        )

    # Provide a default region if the user didn't explicitly pass one
    if "region_name" not in boto3_kwargs:
        boto3_kwargs["region_name"] = "us-east-1"

    # Pass the kwargs directly into boto3
    glue_client = boto3.client("glue", **boto3_kwargs)

    registry_client = SchemaRegistryClient(glue_client, registry_name=registry_name)
    schema = AvroSchema(schema_str)
    self.serializer = KafkaSerializer(registry_client, schema)
```

#### Functions

##### `serialize(topic, data)`

Serializes the given data into an Avro byte string using AWS Glue.

Parameters:

| Name    | Type  | Description                                              | Default    |
| ------- | ----- | -------------------------------------------------------- | ---------- |
| `topic` | `str` | The target Kafka topic.                                  | *required* |
| `data`  | `Any` | The payload to serialize (dictionary or Pydantic model). | *required* |

Returns:

| Name    | Type    | Description                   |
| ------- | ------- | ----------------------------- |
| `bytes` | `bytes` | The Avro-encoded byte string. |

Source code in `src/desflow/connectors/egress/kafka.py`

```python
def serialize(self, topic: str, data: Any) -> bytes:
    """
    Serializes the given data into an Avro byte string using AWS Glue.

    Args:
        topic (str): The target Kafka topic.
        data (Any): The payload to serialize (dictionary or Pydantic model).

    Returns:
        bytes: The Avro-encoded byte string.
    """
    payload = extract_dict(data)
    return self.serializer.serialize(topic, payload)
```
