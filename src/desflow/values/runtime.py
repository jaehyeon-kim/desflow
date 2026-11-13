"""Evaluates the value generators of a run.

One `ValueRuntime` serves a run. It holds what the generators share: the seed their
random sequences come from, the counters of `sequence`, the values a task's
`vars_once` gave to each entity, the values a `unique` generator has given, and the
position in the rows of a `row` generator. `SimulationContext._build` creates it, so
every run starts with fresh state.
"""

import copy
import datetime as dt
import decimal
import logging
from contextlib import contextmanager
from dataclasses import replace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)

import numpy as np
from simpy.core import StopSimulation

from desflow.config import MIN_DELAY
from desflow.models.distributions import get_distribution
from desflow.core.expressions import ANY_NAME, Expression, Scope
from desflow.core.sampler import stream_key
from desflow.values.models import (
    Aggregate,
    Fake,
    Lookup,
    Previous,
    Row,
    ValueGenerationError,
    Gen,
    _is_number,
    part_of,
)

if TYPE_CHECKING:
    from desflow.builder.flow import LiveScope

logger = logging.getLogger(__name__)

# How many draws a `unique` generator makes before it reports that its values ran out
_UNIQUE_TRIES = 1000

# Added to the INFO line of a dropped start that had already taken its `get` item
_CONSUMED = "; the get item of each of them was consumed"

# How many draws a `distinct` lookup makes before it counts the rows left
_DISTINCT_TRIES = 50

# The rows a task keeps when its `history` is not set
DEFAULT_HISTORY = 1_000_000


def _plain(value: Any) -> Any:
    """A generator's result as plain Python data, so every sink writes it as YAML or
    JSON does: NumPy numbers as int, float or bool, dates and times as ISO text."""
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return [_plain(v) for v in value.tolist()]
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain(v) for v in value]
    return value


class RunStopped(BaseException):
    """A `row` with `on_end: stop` has used its last row: the run is ending, so the
    process that asked for the row ends without a record. It is not an `Exception`, so
    no handler for a failed task or a failed draw takes it."""


class ValueRuntime:
    """The state the generators of one run share, and the means to evaluate them.

    Attributes:
        seed (Optional[int]): The seed every generator's random sequence comes from;
            None gives a different run each time.
        context: The `SimulationContext`, which `call` passes to a function and
            `sample` of a service draws through.
    """

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
        from desflow.values.template import numbers_of

        # The keys of the unnamed templates, shared by every run of the context
        self._template_numbers = numbers_of(context)
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

    # ------------------------------------------------------------------ clock

    @property
    def zone(self) -> dt.tzinfo:
        """tzinfo: The run's time zone."""
        return self.env.zone

    def logical_time(self, seconds: float) -> dt.datetime:
        """The logical time `seconds` of simulation time after the start.

        Args:
            seconds: Seconds of simulation time.
        """
        start = self.env.start_datetime.astimezone(dt.timezone.utc)
        return (start + dt.timedelta(seconds=seconds)).astimezone(self.zone)

    def now_time(self) -> dt.datetime:
        """The logical time now."""
        return self.logical_time(self.env.now)

    def time_text(self, moment: dt.datetime) -> str:
        """A time as the text a record's `timestamp` has, in the run's zone.

        Args:
            moment: The time.
        """
        return moment.astimezone(self.zone).isoformat(timespec="milliseconds")

    def iso_time(self, offset: float) -> str:
        """The logical time now plus `offset` seconds, as the text of a timestamp.

        Args:
            offset: Seconds to add to the time now.
        """
        return self.time_text(self.logical_time(self.env.now + offset))

    # ----------------------------------------------------------------- random

    def rng(self, node: Gen, suffix: str = "") -> np.random.Generator:
        """The random sequence of a generator, made from the seed and the place of
        the generator in the file: `SeedSequence(seed, spawn_key=stream_key(place))`,
        the same key a `Sampler` uses for a distribution of that name.
        Adding or removing another field of a mapping leaves it unchanged; inserting a
        list item moves the streams of the items after it.

        Args:
            node: The generator, or the value, to act on.
            suffix: Text added to the place, to make a second stream for the same generator.
        """
        name = node._path + suffix
        generator = self._streams.get(name)
        if generator is None:
            sequence = np.random.SeedSequence(self._entropy, spawn_key=stream_key(name))
            generator = np.random.default_rng(sequence)
            self._streams[name] = generator
        return generator

    def count(self, node: Gen) -> int:
        """How many times `node` has been asked for a count; the call counts.

        Args:
            node: The generator, or the value, to act on.
        """
        done = self._counters.get(node._path, 0)
        self._counters[node._path] = done + 1
        return done

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

    # ----------------------------------------------------------- evaluation

    def copy(self, value: Any) -> Any:
        """A mapping or list copied, so a payload never shares a var or an attribute
        with the entity.

        Args:
            value: The value.
        """
        return copy.deepcopy(value) if isinstance(value, (dict, list)) else value

    def condition(self, node: Gen, text: str, scope: Scope) -> bool:
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

    def omitted(self, node: Any, scope: Scope) -> bool:
        """Whether a mapping field is left out: its generator's `omit_rate` or
        `omit_when` says so. The field is not evaluated then.

        Args:
            node: The generator, or the value, to act on.
            scope: What the expressions here read.
        """
        if not isinstance(node, Gen):
            return False
        if node.omit_rate is not None:
            if self.rng(node, "#omit").random() < node.omit_rate:
                return True
        if node.omit_when is not None:
            return self.condition(node, node.omit_when, scope)
        return False

    def evaluate(self, node: Any, scope: Scope) -> Any:
        """The value of a node: a generator is evaluated, a mapping, a list and a tuple
        are walked (a tuple gives a list), and anything else is a constant.

        Args:
            node: The generator, or the value, to act on.
            scope: What the expressions here read.

        Raises:
            ValueGenerationError: If a generator cannot give a value. The message
                names the file, the line and the generator.
        """
        if isinstance(node, Gen):
            return self._generate(node, scope)
        if isinstance(node, dict):
            out: Dict[Any, Any] = {}
            for key, item in node.items():
                if not self.omitted(item, scope):
                    out[key] = self.evaluate(item, scope)
            return out
        if isinstance(node, (list, tuple)):
            return [self.evaluate(item, scope) for item in node]
        return node

    def _draw(self, node: Gen, scope: Scope) -> Any:
        try:
            return node.evaluate(self, scope)
        except ArithmeticError as exc:
            raise node.fail(f"the result cannot be computed: {exc}") from None

    def _generate(self, node: Gen, scope: Scope) -> Any:
        if node.null_rate is not None:
            if self.rng(node, "#null").random() < node.null_rate:
                return None
        if not node.unique:
            return _plain(self._draw(node, scope))
        seen = self._seen.setdefault(node._path, set())
        for _ in range(_UNIQUE_TRIES):
            value = _plain(self._draw(node, scope))
            key = repr(value)
            if key not in seen:
                seen.add(key)
                return value
        raise node.fail(
            f"unique: no new value in {_UNIQUE_TRIES} draws; it has given "
            f"{len(seen)} different values"
        )

    def draw_service(self, node: Gen, service: str) -> Any:
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

    # ------------------------------------------------------------------ vars

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

    def forget(self, key: Any) -> None:
        """Drops the `vars_once` kept for an entity whose flow has ended, so a long
        run does not keep one entry for each entity it has seen.

        Args:
            key: The key of the entity.
        """
        self._once.pop(key, None)

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
        self._template_numbers.number(template)
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

    # ----------------------------------------------------- relationships

    def keep_rows(self, task: str, size: int) -> None:
        """Has `task` keep the last `size` of its finished payloads, which `lookup` and
        `aggregate` read.

        Args:
            task: The name of the task.
            size: How many rows to keep.
        """
        self._kept.setdefault(task, _Rows(size))

    def keep_previous(self, task: str) -> None:
        """Has `task` keep its last finished payload for each stream, for `previous`.

        Args:
            task: The name of the task.
        """
        self._previous_tasks.add(task)

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

    def ready(self, tasks: Any) -> bool:
        """Whether every task in `tasks` has a row.

        Args:
            tasks: Task names.

        Returns:
            bool: True when each task that keeps rows has at least one.
        """
        return all(len(self._kept[t]) for t in tasks if t in self._kept)

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

    @contextmanager
    def distinct_frame(self) -> Iterator[None]:
        """Opens what a `repeat` is drawing without replacement: a `lookup` with
        `distinct` inside it does not give the same row twice until the `repeat` ends."""
        self._frames.append({})
        try:
            yield
        finally:
            self._frames.pop()

    def _rows_of(self, node: Any, task: str) -> "_Rows":
        rows = self._kept.get(task)
        if rows is None:
            raise node.fail(
                f"task '{task}' keeps no rows; a blueprint keeps them for a task a "
                "lookup or aggregate names, and a builder task asks for them with "
                "history="
            )
        return rows

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

    def _choose(
        self, node: Lookup, pool: "_Pool", used: Optional[Set[int]]
    ) -> Tuple[int, Mapping[str, Any]]:
        size = len(pool)
        at: Callable[[int], Tuple[int, Mapping[str, Any]]] = pool.at
        if isinstance(pool, _Latest) and (
            node.strategy != "random" or node.histogram is not None
        ):
            # The newest row of each value, oldest first by when it was written
            ordered = sorted((pool.at(i) for i in range(size)), key=lambda e: e[0])
            at = ordered.__getitem__
        if node.strategy != "random":
            order = range(size) if node.strategy == "first" else range(size - 1, -1, -1)
            for index in order:
                entry = at(index)
                if used is None or entry[0] not in used:
                    return entry
            raise node.fail(
                f"task '{node.task}' has no row left that this repeat has not drawn"
            )
        rng = self.rng(node)
        for _ in range(_DISTINCT_TRIES if used else 1):
            if node.histogram is not None:
                index = self._histogram_index(node, size, rng)
            else:
                index = int(rng.integers(size))
            entry = at(index)
            if used is None or entry[0] not in used:
                return entry
        left = [i for i in range(size) if at(i)[0] not in used]  # type: ignore[operator]
        if not left:
            raise node.fail(
                f"task '{node.task}' has no row left that this repeat has not drawn"
            )
        return at(left[int(rng.integers(len(left)))])

    @staticmethod
    def _histogram_index(node: Lookup, size: int, rng: np.random.Generator) -> int:
        """The index of a row: a bin by frequency, then a row of the bin.

        Args:
            node: The generator, which holds the bins.
            size: How many rows there are to draw from.
            rng: The generator's random stream.

        Returns:
            int: The index of the row, oldest first.
        """
        bins = node.histogram or []
        total = sum(b.bin for b in bins)
        edges, begin, share = [], 0, 0.0
        for item in bins:
            share += item.bin / total
            end = size if item is bins[-1] else int(round(share * size))
            edges.append((begin, end, item.frequency))
            begin = end
        open_bins = [(b, e, f) for b, e, f in edges if e > b and f > 0]
        if not open_bins:
            # Few rows and the bins that hold them have frequency 0: draw over all
            return int(rng.integers(size))
        weights = np.array([f for _, _, f in open_bins], dtype=float)
        low, high, _ = open_bins[
            int(rng.choice(len(open_bins), p=weights / weights.sum()))
        ]
        return low + int(rng.integers(high - low))

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

    @staticmethod
    def _reduce(node: Aggregate, values: List[Any]) -> Any:
        if node.function == "count":
            return len(values)
        bad = [v for v in values if not _is_number(v)]
        if bad:
            raise node.fail(
                f"{node.path} must be a number in every row, got {bad[0]!r}"
            )
        if node.function == "sum":
            return sum(values)
        if not values:
            raise node.fail(
                f"{node.function} of no rows: task '{node.task}' has none to read"
            )
        if node.function == "mean":
            return sum(values) / len(values)
        return min(values) if node.function == "min" else max(values)

    # ------------------------------------------------------------------ rows

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


def stream_of(entity: Any) -> Tuple[Any, ...]:
    """The stream `previous` follows for an entity.

    Args:
        entity: The entity a task runs for.

    Returns:
        Tuple[Any, ...]: The entity (its arrival and key) when its arrival has `count`
            or `keys`, which is one entity that repeats, otherwise the arrival.
    """
    if entity.loop is not None:
        return ("entity", entity.arrival, entity.key)
    return ("arrival", entity.arrival)


_MISSING = object()


class _Pool:
    """The rows a `lookup` draws from: a count and the entry (sequence number, row) at
    each index."""

    def __len__(self) -> int:  # pragma: no cover
        raise NotImplementedError

    def at(self, index: int) -> Tuple[int, Mapping[str, Any]]:  # pragma: no cover
        """The entry at an index, oldest first."""
        raise NotImplementedError


class _Rows(_Pool):
    """The last rows of one task, oldest first, each with a sequence number."""

    def __init__(self, size: int):
        self.size = size
        self._entries: List[Tuple[int, Mapping[str, Any]]] = []
        self._start = 0
        self._written = 0
        self._latest: Dict[str, "_Latest"] = {}

    def __len__(self) -> int:
        return len(self._entries) - self._start

    def at(self, index: int) -> Tuple[int, Mapping[str, Any]]:
        """The entry at an index, oldest first."""
        return self._entries[self._start + index]

    def add(self, row: Mapping[str, Any]) -> None:
        """Adds a row as the newest, and drops the oldest past `size`."""
        entry = (self._written, row)
        self._written += 1
        self._entries.append(entry)
        for index in self._latest.values():
            index.add(entry)
        if len(self) > self.size:
            oldest = self._entries[self._start]
            self._start += 1
            for index in self._latest.values():
                index.drop(oldest)
            # Rows that left stay referenced until the list is cut, which happens when
            # they are an eighth of `size`, so at most 9/8 of `size` rows are held, or
            # one row over for a `size` below 8
            if self._start > max(self.size // 8, 1):
                del self._entries[: self._start]
                self._start = 0

    def newest(self) -> Iterator[Tuple[int, Mapping[str, Any]]]:
        """The entries, newest first."""
        for index in range(len(self._entries) - 1, self._start - 1, -1):
            yield self._entries[index]

    def latest(self, field: str) -> "_Latest":
        """The newest row for each value of `field`, kept up to date from now on."""
        found = self._latest.get(field)
        if found is None:
            found = self._latest[field] = _Latest(field)
            for index in range(len(self)):
                found.add(self.at(index))
        return found


class _Latest(_Pool):
    """The newest row for each value of a field. The oldest row kept is the only one
    that can leave, so a value goes when its newest row is the one that leaves."""

    def __init__(self, field: str):
        self.field = field
        self._by_value: Dict[Any, Tuple[int, Mapping[str, Any]]] = {}
        self._values: List[Any] = []
        self._position: Dict[Any, int] = {}

    def _key(self, row: Mapping[str, Any]) -> Any:
        value = row.get(self.field, _MISSING)
        if value is _MISSING:
            return _MISSING
        try:
            hash(value)
        except TypeError:
            return repr(value)
        return value

    def __len__(self) -> int:
        return len(self._values)

    def at(self, index: int) -> Tuple[int, Mapping[str, Any]]:
        """The entry at an index; the order is the order the values first appeared in,
        with a swap where a value left."""
        return self._by_value[self._values[index]]

    def add(self, entry: Tuple[int, Mapping[str, Any]]) -> None:
        """Takes in a new newest row."""
        key = self._key(entry[1])
        if key is _MISSING:
            return
        if key not in self._by_value:
            self._position[key] = len(self._values)
            self._values.append(key)
        self._by_value[key] = entry

    def drop(self, entry: Tuple[int, Mapping[str, Any]]) -> None:
        """Takes out the oldest row if it is the newest of its value."""
        key = self._key(entry[1])
        found = self._by_value.get(key)
        if found is None or found[0] != entry[0]:
            return
        del self._by_value[key]
        index = self._position.pop(key)
        last = self._values.pop()
        if index < len(self._values):
            self._values[index] = last
            self._position[last] = index


class _RowCursor:
    """Where a `row` generator is in its rows."""

    def __init__(self, size: int):
        self.size = size
        self.position = 0
        self.order: Optional[List[int]] = None
        self._weights: Optional[np.ndarray] = None

    def weights(self, node: Row, rows: List[Dict[str, Any]]) -> np.ndarray:
        """The probability of each row, from the weight column."""
        if self._weights is None:
            try:
                raw = np.array([float(r[node.weight]) for r in rows])  # type: ignore[index]
            except (KeyError, TypeError, ValueError) as exc:
                raise node.fail(
                    f"the weight column {node.weight!r} is missing or not a number "
                    f"in a row of {node.file}: {exc}"
                ) from None
            if (raw < 0).any() or not raw.sum() > 0:
                raise node.fail(
                    f"the weights in {node.weight!r} must be 0 or more with a sum "
                    "above 0"
                )
            self._weights = raw / raw.sum()
        return self._weights


class _FakerCall:
    """A Faker that checks a method exists and gives plain data."""

    def __init__(self, faker: Any, node: Fake):
        self._faker = faker
        self._node = node

    def __getattr__(self, name: str) -> Callable[..., Any]:
        method = getattr(self._faker, name, None)
        if method is None or not callable(method) or name.startswith("_"):
            raise self._node.fail(f"Faker has no method '{name}'")

        def call(*args: Any, **kwargs: Any) -> Any:
            try:
                return _plain(method(*args, **kwargs))
            except (TypeError, ValueError, AttributeError) as exc:
                raise self._node.fail(f"fake '{name}': {exc}") from None

        return call


__all__ = ["ValueRuntime", "ValueGenerationError"]
