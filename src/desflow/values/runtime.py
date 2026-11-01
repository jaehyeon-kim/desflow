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
import zlib
from dataclasses import replace
from typing import Any, Callable, Dict, List, Optional, Set

import numpy as np
from simpy.core import StopSimulation

from desflow.config import MIN_DELAY
from desflow.core.distributions import get_distribution
from desflow.core.expressions import ANY_NAME, Expression, Scope
from desflow.values.models import (
    Fake,
    Row,
    ValueGenerationError,
    _Gen,
)

# How many draws a `unique` generator makes before it reports that its values ran out
_UNIQUE_TRIES = 1000


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
    ):
        """
        Args:
            seed: The run's seed, or None.
            env: The environment, for the simulation clock, its start and its zone.
            sampler: The run's `Sampler`, for `sample` of a service.
            context: The `SimulationContext`.
        """
        self.seed = seed
        self.env = env
        self.sampler = sampler
        self.context = context
        # With no seed the sequences still differ from node to node
        self._entropy = seed if seed is not None else np.random.SeedSequence().entropy
        self._streams: Dict[str, np.random.Generator] = {}
        self._counters: Dict[str, int] = {}
        self._free = 0
        self._once: Dict[Any, Dict[str, Dict[str, Any]]] = {}
        self._seen: Dict[str, Set[str]] = {}
        self._fakers: Dict[str, Any] = {}
        self._rows: Dict[str, "_RowCursor"] = {}
        self._expressions: Dict[str, Expression] = {}

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

    def count(self, node: _Gen) -> int:
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

    def _draw(self, node: _Gen, scope: Scope) -> Any:
        try:
            return node.evaluate(self, scope)
        except ArithmeticError as exc:
            raise node.fail(f"the result cannot be computed: {exc}") from None

    def _generate(self, node: _Gen, scope: Scope) -> Any:
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
        from desflow.core.flow import make_scope

        context = self.context
        return make_scope(
            None,
            {} if entity is None else entity.attributes,
            self.env,
            context._levels() if context is not None else None,
            signals=None if context is None else context._signals,
            resources=None if context is None else context._resources_map,
            names=names,
            entity=entity.key if keyed and entity is not None else None,
        )

    def value(self, spec: Any, entity: Any = None) -> Any:
        """Evaluates a generator now, for a low-level script: a typed generator from
        `desflow.values.generator`, or any value that holds one.

        Args:
            spec: The generator, a mapping or list that holds one, or a constant.
            entity: The entity whose attributes `attr` reads, or None.
        """
        from desflow.values.template import bind

        if isinstance(spec, dict) and "gen" in spec:
            from desflow.values.models import generator_adapter

            spec = generator_adapter().validate_python(spec)
        self._free += 1
        bind(spec, ("value", self._free), overwrite=False)
        return self.evaluate(spec, self.scope_of(entity, keyed=True))

    def render(
        self,
        template: Any,
        entity: Any = None,
        scope: Optional[Scope] = None,
        extras: Optional[Dict[str, Any]] = None,
        attributes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Evaluates a `Template`: its `vars_once` once for the entity, its `vars`,
        then its payload, which must give a mapping.

        Args:
            template: The `Template`.
            entity: The entity whose attributes `attr` reads.
            scope: The scope of a running task, whose bare names the template's vars
                follow; without it a scope outside a task is made.
            extras: Fields written over the result.
            attributes: Names of entity attributes written over the result, read now.
        """
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
        if not isinstance(result, dict):
            raise ValueGenerationError(
                f"{getattr(template.payload, '_where', '') or 'the payload'}: a "
                f"payload must give a mapping, got {result!r}"
            )
        if extras:
            result.update(extras)
        if attributes:
            held = {} if entity is None else entity.attributes
            for name in attributes:
                result[name] = held.get(name)
        return result

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
                    if self.context is None:
                        raise StopSimulation(None)
                    self.context._stop_run()
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
