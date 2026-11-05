"""The YAML blueprint format is frozen: 1.x may only widen what a 1.0 blueprint can write.

The 1.0 schema is kept as a file, `tests/unit/frozen/blueprint.schema.1.0.json`, and
every difference between it and the committed `src/desflow/blueprint/blueprint.schema.json`
must be a listed widening. The test walks the two schemas side by side. At each place
a difference passes only when it is one of the rules of `WIDENINGS`; any other
difference fails, with the path to the place and both values. A summary of the schema
is not compared, because every summary loses something, and a narrowing it cannot see
would pass.

The frozen file is the 1.0 contract. It is never regenerated, not even when a release
adds a key, because the next release is compared with 1.0, not with its predecessor.
`test_committed_schema_is_current` (elsewhere) keeps the committed schema equal to the
models.

`FROZEN_SAMPLES` lists strings that each of the six patterns of the schema must keep
accepting, so a pattern may widen but not narrow.

`test_each_narrowing_fails` applies one narrowing for each rule and for each keyword
the schema uses, to a copy of the frozen schema in memory, and each must fail.
`test_each_widening_passes` applies one widening for each rule, and each must pass.
A kind of narrowing the walk misses is added there as a mutation, with the fix.
"""

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Tuple

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "src" / "desflow" / "blueprint" / "blueprint.schema.json"
FROZEN_SCHEMA = Path(__file__).resolve().parent / "frozen" / "blueprint.schema.1.0.json"

FROZEN_MESSAGE = (
    "The YAML blueprint format is frozen from 1.0: a 1.x release may only widen what "
    "a 1.0 blueprint can write, and every difference from "
    "tests/unit/frozen/blueprint.schema.1.0.json must be a listed widening "
    "(Reference, YAML Keys; Compatibility Policy). "
)

# The widenings a 1.x release may make. Any other difference from the 1.0 schema fails.
WIDENINGS = {
    "new key or definition": "a key of `properties` or a definition of `$defs` is one that no 1.0 blueprint uses",
    "new member or value": "a member of `anyOf` or `oneOf`, or a value of `enum`, only adds what is accepted",
    "wider limit": "`minimum`, `exclusiveMinimum`, `ge`, `gt` and `minItems` fall or go; `maximum` and `maxItems` rise or go",
    "required key removed": "a key that is no longer required accepts a blueprint that omits it",
    "additionalProperties opened": "`additionalProperties` from `false` to `true` or to a schema accepts more keys",
    "pattern kept": "a pattern changes while every frozen sample still matches it",
    "free keyword": "`$id`, `$schema`, `title`, `description` and `default` change no blueprint's validity",
}

FREE = ("$id", "$schema", "title", "description", "default")
LOWER_BOUNDS = ("minimum", "exclusiveMinimum", "ge", "gt", "minItems")
UPPER_BOUNDS = ("maximum", "maxItems")
# Keywords whose subschemas are walked with the same rules. `if` is compared as it is,
# because a widening inside the condition changes which branch a value takes.
WALKED_SUBSCHEMAS = ("items", "then", "else")
# Keywords that hold data or names, which the walk does not enter as schemas.
DATA = ("const", "enum", "default", "required")

# Strings each pattern accepts at 1.0, written out, under the place of the pattern in
# the frozen schema. A 1.x release may accept more, and each of these must still match
# the committed pattern, so a removed unit, a removed time form or a narrowed pattern
# fails the test. The six patterns are `Duration`, the three forms of `Time`,
# `Placeholder` and `PythonReference`.
FROZEN_SAMPLES = {
    "#/$defs/Duration": [
        "10 s",
        "10 sec",
        "10 secs",
        "10 second",
        "10 seconds",
        "10 m",
        "10 min",
        "10 mins",
        "10 minute",
        "10 minutes",
        "10 h",
        "10 hr",
        "10 hrs",
        "10 hour",
        "10 hours",
        "10 d",
        "10 day",
        "10 days",
        "10 w",
        "10 wk",
        "10 week",
        "10 weeks",
        "10 mo",
        "10 month",
        "10 months",
        "10 y",
        "10 yr",
        "10 year",
        "10 years",
        "2.5 h",
        "3min",
        "1.5d",
    ],
    "#/$defs/Time/anyOf/0": ["now", " NOW "],
    "#/$defs/Time/anyOf/1": ["-1d", "+2 h", "-30 min", "+1.5 hours"],
    "#/$defs/Time/anyOf/2": [
        "2026-01-31",
        "2026-01-31T09:30",
        "2026-01-31T09:30:15.5Z",
        "2026-01-31 09:30:15+10:00",
    ],
    "#/$defs/Placeholder": [
        "${HOST}",
        "${HOST:-localhost}",
        "http://${HOST}:9092/x",
    ],
    "#/$defs/PythonReference": ["module.attribute", "pkg.module.attribute"],
}


def _load(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


ABSENT = object()  # a keyword or member that one of the two schemas does not have


def _show(value: Any) -> str:
    if value is ABSENT:
        return "absent"
    text = json.dumps(value, sort_keys=True)
    return text if len(text) <= 160 else text[:157] + "..."


def _differs(where: str, frozen: Any, now: Any, what: str = "") -> str:
    return f"{where}: {what + ': ' if what else ''}frozen {_show(frozen)}, now {_show(now)}"


def _equal(where: str, frozen: Any, now: Any) -> List[str]:
    return [] if frozen == now else [_differs(where, frozen, now, "changed")]


def _problems(frozen: Any, now: Any, where: str = "#") -> List[str]:
    """Each difference between a frozen schema and the current one at `where` that is
    not a widening of `WIDENINGS`, with the path to the place and both values."""
    if not isinstance(frozen, dict) or not isinstance(now, dict):
        return _equal(where, frozen, now)
    found: List[str] = []
    for keyword in sorted(frozen.keys() | now.keys()):
        place = f"{where}/{keyword}"
        if keyword in FREE:
            continue  # free keyword
        if keyword not in now:
            if keyword == "required" or keyword in LOWER_BOUNDS + UPPER_BOUNDS:
                continue  # required key removed, wider limit (gone)
            found.append(_differs(place, frozen[keyword], ABSENT, "removed"))
        elif keyword not in frozen:
            found.append(
                _differs(
                    place, ABSENT, now[keyword], "a new keyword at an existing place"
                )
            )
        else:
            found += _keyword(keyword, frozen[keyword], now[keyword], place)
    return found


def _keyword(keyword: str, frozen: Any, now: Any, where: str) -> List[str]:
    if keyword in ("properties", "$defs"):  # new key or definition
        if not isinstance(now, dict):
            return _equal(where, frozen, now)
        found = []
        for name, member in frozen.items():
            if name in now:
                found += _problems(member, now[name], f"{where}/{name}")
            else:
                found.append(_differs(f"{where}/{name}", member, ABSENT, "removed"))
        return found
    if keyword in ("anyOf", "oneOf"):  # new member
        if not isinstance(now, list):
            return _equal(where, frozen, now)
        return [
            _differs(
                f"{where}/{index}",
                member,
                now,
                "no member of the current list is it or a widening of it",
            )
            for index, member in enumerate(frozen)
            if not any(
                not _problems(member, other, f"{where}/{index}") for other in now
            )
        ]
    if keyword == "enum":  # new value
        if not isinstance(now, list):
            return _equal(where, frozen, now)
        lost = [value for value in frozen if value not in now]
        return [_differs(where, lost, now, "values removed")] if lost else []
    if keyword in LOWER_BOUNDS or keyword in UPPER_BOUNDS:  # wider limit
        rose = keyword in LOWER_BOUNDS and now > frozen
        fell = keyword in UPPER_BOUNDS and now < frozen
        return [_differs(where, frozen, now, "narrower limit")] if rose or fell else []
    if keyword == "required":  # required key removed
        added = sorted(set(now) - set(frozen))
        return [_differs(where, frozen, now, f"new required {added}")] if added else []
    if keyword == "additionalProperties":  # opened
        if frozen is False and (now is True or isinstance(now, dict)):
            return []
        if isinstance(frozen, dict) and isinstance(now, dict):
            return _problems(frozen, now, where)
        return _equal(where, frozen, now)
    if keyword == "pattern":  # kept while every frozen sample matches
        if frozen == now:
            return []
        samples = FROZEN_SAMPLES.get(where.removesuffix("/pattern"))
        if samples is None:
            return [_differs(where, frozen, now, "a pattern with no frozen samples")]
        lost = [sample for sample in samples if not re.search(now, sample)]
        return (
            [_differs(where, frozen, now, f"no longer accepts {lost}")] if lost else []
        )
    if keyword in WALKED_SUBSCHEMAS:
        if isinstance(frozen, dict) and isinstance(now, dict):
            return _problems(frozen, now, where)
    # `$ref`, `type`, `const`, `if`, `allOf` and any other keyword: no change allowed
    return _equal(where, frozen, now)


def _keywords(schema: Any) -> Iterator[str]:
    """Each keyword the schema uses, at any place."""
    if isinstance(schema, dict):
        for keyword, value in schema.items():
            yield keyword
            if keyword in ("properties", "$defs"):
                for member in value.values():
                    yield from _keywords(member)
            elif keyword in ("anyOf", "oneOf"):
                for member in value:
                    yield from _keywords(member)
            elif keyword not in DATA:
                yield from _keywords(value)


def test_the_committed_schema_only_widens_the_frozen_1_0_schema():
    problems = _problems(_load(FROZEN_SCHEMA), _load(SCHEMA))
    assert not problems, FROZEN_MESSAGE + "; ".join(problems)


def test_every_frozen_pattern_has_samples_and_each_sample_matches_it():
    frozen = _load(FROZEN_SCHEMA)
    patterns: Dict[str, str] = {}

    def collect(schema: Any, where: str) -> None:
        if not isinstance(schema, dict):
            return
        if "pattern" in schema:
            patterns[where] = schema["pattern"]
        for keyword, value in schema.items():
            if keyword in ("properties", "$defs"):
                for name, member in value.items():
                    collect(member, f"{where}/{keyword}/{name}")
            elif keyword in ("anyOf", "oneOf"):
                for index, member in enumerate(value):
                    collect(member, f"{where}/{keyword}/{index}")
            elif keyword not in DATA:
                collect(value, f"{where}/{keyword}")

    collect(frozen, "#")
    assert len(patterns) == 6
    assert set(patterns) == set(FROZEN_SAMPLES)
    for where, samples in FROZEN_SAMPLES.items():
        lost = [sample for sample in samples if not re.search(patterns[where], sample)]
        assert not lost, f"{where}: the frozen pattern does not accept {lost}"


def _at(schema: Dict[str, Any], path: str) -> Any:
    """The part of a schema at a `/` path, as `$defs/Add/properties/args`."""
    node: Any = schema
    for step in filter(None, path.split("/")):
        node = node[int(step)] if isinstance(node, list) else node[step]
    return node


def _set(path: str, key: str, value: Any) -> Callable[[Dict[str, Any]], None]:
    def mutate(schema: Dict[str, Any]) -> None:
        _at(schema, path)[key] = value

    return mutate


def _drop(path: str, key: str) -> Callable[[Dict[str, Any]], None]:
    def mutate(schema: Dict[str, Any]) -> None:
        del _at(schema, path)[key]

    return mutate


def _pop(path: str, index: int) -> Callable[[Dict[str, Any]], None]:
    def mutate(schema: Dict[str, Any]) -> None:
        _at(schema, path).pop(index)

    return mutate


def _add(path: str, value: Any) -> Callable[[Dict[str, Any]], None]:
    def mutate(schema: Dict[str, Any]) -> None:
        _at(schema, path).append(copy.deepcopy(value))

    return mutate


def _replace(
    path: str, key: str, old: str, new: str
) -> Callable[[Dict[str, Any]], None]:
    def mutate(schema: Dict[str, Any]) -> None:
        node = _at(schema, path)
        assert old in node[key]
        node[key] = node[key].replace(old, new)

    return mutate


ADD = "$defs/Add/properties"
WHEN = "$defs/WhenWait/properties/when"
VALUE = "$defs/Value"
TIME = "$defs/Time/anyOf"
DURATION = "$defs/Duration"
GAP = "$defs/Arrival/properties/gap/anyOf/0"
RATE = f"{ADD}/null_rate/anyOf/0"
BATCH = "$defs/Batching/properties/batch_size/anyOf/0"
WITHIN = "$defs/ExpectedWithin/properties/within"
INTERVAL = "$defs/Stats/properties/interval"
BOUNDS = "$defs/Integer/properties/bounds"
FUNCTION = "$defs/Aggregate/properties/function/anyOf/0"

# One narrowing for each keyword of the schema and each rule of `WIDENINGS`, with the
# earlier findings that a summary of the schema let through (S3, R2, T1). Each is
# applied to a copy of the frozen schema, and each must fail the walk.
NARROWINGS: List[Tuple[str, str, Callable[[Dict[str, Any]], None]]] = [
    ("properties", "a key removed", _drop(ADD, "args")),
    (
        "properties",
        "a key renamed",
        lambda s: _at(s, ADD).update(args2=_at(s, ADD).pop("args")),
    ),
    ("$defs", "a definition removed", _drop("$defs", "Duration")),
    ("anyOf", "a member removed", _pop(TIME, 0)),
    ("anyOf", "a member narrowed", _set(f"{TIME}/1", "type", "integer")),
    ("oneOf", "a member removed", _pop(f"{GAP}/oneOf", 0)),
    ("enum", "a value removed", _pop(f"{FUNCTION}/enum", 0)),
    ("enum", "an empty enum", _set(FUNCTION, "enum", [])),
    ("enum", "an enum beside type (S3)", _set(WHEN, "enum", ["x"])),
    ("const", "a const beside type (S3)", _set(WHEN, "const", "x")),
    ("const", "a const changed", _set(f"{ADD}/gen/anyOf/0", "const", "added")),
    ("$ref", "a $ref changed", _set(f"{ADD}/gen/anyOf/1", "$ref", "#/$defs/Duration")),
    ("type", "a type changed", _set(WHEN, "type", "integer")),
    ("items", "an items narrowed", _set(f"{ADD}/args", "items", {"type": "string"})),
    ("items", "an items removed", _drop(f"{ADD}/args", "items")),
    (
        "additionalProperties",
        "a true closed (T1)",
        _set("$defs/Connector/properties/config", "additionalProperties", False),
    ),
    (
        "additionalProperties",
        "a schema closed",
        _set("properties/resources", "additionalProperties", False),
    ),
    (
        "additionalProperties",
        "a schema opened to true",
        _set("properties/resources", "additionalProperties", True),
    ),
    (
        "additionalProperties",
        "a false removed",
        _drop("$defs/Add", "additionalProperties"),
    ),
    ("required", "a key made required", _add("$defs/Add/required", "decimals")),
    ("minimum", "a minimum raised", _set(f"{ADD}/decimals/anyOf/0", "minimum", 1)),
    ("minimum", "a new minimum", _set(f"{ADD}/unique", "minimum", 0)),
    ("maximum", "a maximum lowered", _set(RATE, "maximum", 0.5)),
    (
        "exclusiveMinimum",
        "an exclusiveMinimum raised",
        _set(BATCH, "exclusiveMinimum", 1),
    ),
    ("ge", "a ge raised", _set(WITHIN, "ge", 1)),
    ("gt", "a gt raised", _set(INTERVAL, "gt", 1)),
    ("minItems", "a minItems raised", _set(f"{ADD}/args", "minItems", 3)),
    ("maxItems", "a maxItems lowered", _set(BOUNDS, "maxItems", 1)),
    ("maxItems", "a new maxItems", _set(f"{ADD}/args", "maxItems", 9)),
    (
        "pattern",
        "a pattern narrowed (Duration without weeks)",
        _replace(DURATION, "pattern", "[wW][eE][eE][kK][sS]|", ""),
    ),
    (
        "pattern",
        "a pattern changed to reject its samples",
        _set("$defs/Placeholder", "pattern", r"^\$\{HOST\}$"),
    ),
    ("pattern", "a pattern removed", _drop("$defs/Placeholder", "pattern")),
    ("pattern", "a new pattern", _set(WHEN, "pattern", "^x$")),
    (
        "if",
        "an if changed",
        _set(VALUE, "if", {"type": "object", "required": ["kind"]}),
    ),
    ("if", "an if widened", _set(VALUE, "if", {"type": "object", "required": []})),
    ("then", "a then changed", _set(f"{VALUE}/then", "$ref", "#/$defs/Payload")),
    (
        "then",
        "a then narrowed",
        _set(f"{VALUE}/else/then", "additionalProperties", {"type": "string"}),
    ),
    (
        "else",
        "an else replaced by a string (R2)",
        _set(VALUE, "else", {"type": "string"}),
    ),
    ("else", "an else replaced by a not (R2)", _set(VALUE, "else", {"not": {}})),
    (
        "else",
        "an empty enum as the last else (R2)",
        _set(f"{VALUE}/else/else", "else", {"enum": []}),
    ),
    ("else", "an else removed", _drop(f"{VALUE}/else", "else")),
    ("allOf", "an allOf beside type (T1)", _set(WHEN, "allOf", [{"const": "x"}])),
    ("multipleOf", "an unknown keyword", _set(BATCH, "multipleOf", 2)),
]

# One widening for each rule of `WIDENINGS`, each of which must pass the walk.
WIDENINGS_APPLIED: List[Tuple[str, str, Callable[[Dict[str, Any]], None]]] = [
    ("new key or definition", "a new key", _set(ADD, "extra", {"type": "string"})),
    (
        "new key or definition",
        "a new definition",
        _set("$defs", "NewWait", {"type": "object"}),
    ),
    ("new member or value", "an anyOf member", _add(TIME, {"type": "integer"})),
    (
        "new member or value",
        "a oneOf member",
        _add(f"{GAP}/oneOf", {"type": "boolean"}),
    ),
    ("new member or value", "an enum value", _add(f"{FUNCTION}/enum", "median")),
    (
        "wider limit",
        "a minimum lowered",
        _set(f"{ADD}/decimals/anyOf/0", "minimum", -1),
    ),
    ("wider limit", "a minimum removed", _drop(f"{ADD}/decimals/anyOf/0", "minimum")),
    ("wider limit", "a maximum raised", _set(RATE, "maximum", 2)),
    ("wider limit", "a maximum removed", _drop(RATE, "maximum")),
    ("wider limit", "an exclusiveMinimum lowered", _set(BATCH, "exclusiveMinimum", -1)),
    ("wider limit", "a ge lowered", _set(WITHIN, "ge", -1)),
    ("wider limit", "a gt removed", _drop(INTERVAL, "gt")),
    ("wider limit", "a minItems lowered", _set(f"{ADD}/args", "minItems", 1)),
    ("wider limit", "a maxItems raised", _set(BOUNDS, "maxItems", 3)),
    ("required key removed", "a required key removed", _pop("$defs/Add/required", 0)),
    (
        "required key removed",
        "the required list removed",
        _drop("$defs/Add", "required"),
    ),
    (
        "additionalProperties opened",
        "a false opened to true",
        _set("$defs/Add", "additionalProperties", True),
    ),
    (
        "additionalProperties opened",
        "a false opened to a schema",
        _set("$defs/Add", "additionalProperties", {"type": "string"}),
    ),
    (
        "pattern kept",
        "a pattern widened",
        lambda s: _at(s, DURATION).update(
            pattern=f"(?:{_at(s, DURATION)['pattern']})|^fortnight$"
        ),
    ),
    ("free keyword", "$id changed", _set("", "$id", "https://example.org/other")),
    (
        "free keyword",
        "$schema changed",
        _set("", "$schema", "https://json-schema.org/draft/2019-09/schema"),
    ),
    ("free keyword", "title changed", _set(DURATION, "title", "A new title")),
    (
        "free keyword",
        "description changed",
        _set(DURATION, "description", "A new description"),
    ),
    ("free keyword", "default changed", _set(f"{ADD}/null_rate", "default", 0.5)),
    ("free keyword", "default removed", _drop(f"{ADD}/null_rate", "default")),
]


def _mutated(mutation: Callable[[Dict[str, Any]], None]) -> Dict[str, Any]:
    schema = copy.deepcopy(_load(FROZEN_SCHEMA))
    mutation(schema)
    return schema


@pytest.mark.parametrize(
    "keyword, name, mutation", NARROWINGS, ids=[f"{n[0]}: {n[1]}" for n in NARROWINGS]
)
def test_each_narrowing_fails(keyword, name, mutation):
    assert _problems(_load(FROZEN_SCHEMA), _mutated(mutation)), (
        f"{keyword}, {name}: a narrowing of the 1.0 schema passes the freeze"
    )


@pytest.mark.parametrize(
    "rule, name, mutation",
    WIDENINGS_APPLIED,
    ids=[f"{n[0]}: {n[1]}" for n in WIDENINGS_APPLIED],
)
def test_each_widening_passes(rule, name, mutation):
    assert rule in WIDENINGS
    problems = _problems(_load(FROZEN_SCHEMA), _mutated(mutation))
    assert not problems, f"{rule}, {name}: a widening fails the freeze: {problems}"


def test_every_rule_and_every_keyword_of_the_schema_has_its_mutations():
    narrowed = {keyword for keyword, _, _ in NARROWINGS}
    used = set(_keywords(_load(FROZEN_SCHEMA)))
    assert used - narrowed - set(FREE) == set(), (
        "a keyword the schema uses has no narrowing"
    )
    assert {rule for rule, _, _ in WIDENINGS_APPLIED} == set(WIDENINGS)
    assert set(FREE) <= {name.split()[0] for _, name, _ in WIDENINGS_APPLIED}
