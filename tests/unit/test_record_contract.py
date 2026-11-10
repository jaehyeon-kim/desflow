"""The record contract is frozen: 1.x keeps every record field, its type and its position.

`FROZEN` lists the fields of each record kind as they are at 1.0, written out as
literals. A 1.x release may add a field after the last one of its kind, and a record
kind that reaches only the sinks that ask for it (as `trace` does at 1.0, MF5), and
nothing else. The `trace` record keeps its keys like the other three kinds. The tests
here also pin the two statements the contract makes about values (a capacity with no
limit reads as `null`, and a subscribed task's interrupt `cause.value` is a mapping
with `event` and `value`) and the reserved `__name__` payload keys.
`tests/docs/test_docs_record_contract.py` keeps the page equal to the models and the
published records equal to the models.
"""

import typing
from typing import Any, Dict, List, Tuple

import pytest

from desflow import DynamicRealtimeEnvironment
from desflow.connectors.egress import base as egress_base
from desflow.models.schemas import EventPayload, LabelPayload, TelemetryPayload
from tests.conformance.harness import RecordingEgress, run_yaml_text

WHERE = (
    "The record contract is frozen from 1.0: a 1.x release keeps every field with its "
    "name, type and position, and may add a field only after the last one of its kind "
    "(Reference, Records and Telemetry; Compatibility Policy). "
)

# The fields of each record kind at 1.0, in the order a record carries them, with the
# type of each as `_annotation` writes it.
FROZEN: Dict[str, List[Tuple[str, str]]] = {
    "TelemetryPayload": [
        ("stream_type", "Literal['telemetry']"),
        ("sim_ts", "float"),
        ("timestamp", "str"),
        ("path_id", "str"),
        ("value", "Any"),
    ],
    "EventPayload": [
        ("stream_type", "Literal['event']"),
        ("sim_ts", "float"),
        ("timestamp", "str"),
        ("key", "str"),
        ("task", "Optional[str]"),
        ("instance", "Optional[Any]"),
        ("value", "Any"),
    ],
    "LabelPayload": [
        ("stream_type", "Literal['label']"),
        ("sim_ts", "float"),
        ("timestamp", "str"),
        ("path", "Optional[str]"),
        ("old", "Any"),
        ("new", "Any"),
        ("label", "Dict[str, Any]"),
    ],
}
MODELS = {
    "TelemetryPayload": TelemetryPayload,
    "EventPayload": EventPayload,
    "LabelPayload": LabelPayload,
}

# The record kinds every sink may receive, and the kind that only a sink which asks for
# it receives (MF5).
FROZEN_KINDS = ("telemetry", "event", "label")
KINDS_FOR_SINKS_THAT_ASK = ("trace",)
# The keys a `trace` record begins with, and the keys of its value.
TRACE_KEYS = ["stream_type", "sim_ts", "timestamp", "path_id", "value"]
TRACE_VALUE_KEYS = {"priority", "event_id", "event"}
# The type of each: `sim_ts` a float, `timestamp` and `path_id` strings, `priority` and
# `event_id` integers, `event` a string.
TRACE_TYPES = {
    "sim_ts": float,
    "timestamp": str,
    "path_id": str,
    "priority": int,
    "event_id": int,
    "event": str,
}

# The payload keys sinks route by, and the sinks that read each. A key that starts and
# ends with two underscores belongs to desflow: a sink that routes by one reads and
# removes it, and every other sink drops it. Each sink's own unit test runs its routing
# with fakes and is the guard of its key:
#   __topic__  test_egress_kafka.py::test_topic_field_selects_the_topic_and_is_removed
#   __table__  test_egress_iceberg.py::test_table_field_routes_and_is_removed
#              test_egress_postgres.py::test_postgres_egress_run
#   __path__   test_egress_storage.py::test_path_field_selects_the_file_and_is_removed
#   __stream__ test_egress_redis.py::test_stream_key_is_read_from_the_nested_payload
ROUTING_KEYS = ("__topic__", "__table__", "__path__", "__stream__")


def _annotation(annotation: Any) -> str:
    """A model field's type as text, the same on every Python version."""
    if annotation is Any:
        return "Any"
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Literal:
        return "Literal[" + ", ".join(repr(a) for a in args) + "]"
    if origin is typing.Union and type(None) in args:
        inner = [a for a in args if a is not type(None)]
        return f"Optional[{_annotation(inner[0])}]"
    if origin is dict:
        return f"Dict[{_annotation(args[0])}, {_annotation(args[1])}]"
    return annotation.__name__


def _fields(model: Any) -> List[Tuple[str, str]]:
    return [(n, _annotation(f.annotation)) for n, f in model.model_fields.items()]


def test_frozen_fields_keep_name_type_and_order():
    for name, frozen in FROZEN.items():
        current = _fields(MODELS[name])
        assert current[: len(frozen)] == frozen, (
            WHERE + f"{name} begins {current[: len(frozen)]}, but 1.0 froze {frozen}."
        )


def test_the_trace_kind_keeps_its_keys_and_reaches_only_the_sinks_that_ask():
    """`trace` is a 1.0 kind, like the three frozen ones, and MF5 lets a record kind
    reach only the sinks that ask for it: a labels sink and a plain sink get no `trace`
    record, and a `trace` sink gets nothing else."""
    env = DynamicRealtimeEnvironment(factor=0)
    plain, labels, tracer = RecordingEgress(), RecordingEgress(), RecordingEgress()
    env.setup_egress(
        [plain, labels, tracer],
        labels=[False, True, False],
        traces=[False, False, True],
    )
    env.trace_events(True)

    def work(env):
        yield env.timeout(1)
        env.publish_telemetry("Shop.x", 1)
        env.publish_event("orders-1", {"status": "placed"}, task="t", instance=0)
        env.publish_label({"fault": "f"})

    env.process(work(env))
    env.run(until=2)
    env.teardown()
    kinds = {
        name: {r["stream_type"] for r in sink.records}
        for name, sink in (("plain", plain), ("labels", labels), ("tracer", tracer))
    }
    assert kinds["tracer"] == set(KINDS_FOR_SINKS_THAT_ASK), kinds
    assert kinds["plain"] == {"telemetry", "event"}, kinds
    assert kinds["labels"] == {"label"}, kinds
    assert set(kinds["plain"] | kinds["labels"]) <= set(FROZEN_KINDS)
    record = tracer.records[0]
    assert list(record)[: len(TRACE_KEYS)] == TRACE_KEYS, (
        WHERE + f"A trace record has the keys {list(record)}."
    )
    assert TRACE_VALUE_KEYS <= set(record["value"]), (
        WHERE + f"A trace record's value has the keys {sorted(record['value'])}."
    )
    for key, kind in TRACE_TYPES.items():
        found = record[key] if key in TRACE_KEYS else record["value"][key]
        assert isinstance(found, kind) and not isinstance(found, bool), (
            WHERE
            + f"The trace record's {key} is {type(found).__name__}, "
            + f"not {kind.__name__}."
        )


def _value_of(records: List[Dict[str, Any]], task: str) -> Dict[str, Any]:
    return next(r["value"] for r in records if r.get("task") == task)


@pytest.mark.parametrize("section", ["containers", "stores"])
def test_a_capacity_with_no_limit_reads_as_null_in_a_record(tmp_path, section):
    """A JSON record cannot hold infinity, so a read of an unbounded capacity is
    `null` in the published record, not `inf`."""
    path = {
        "containers": "T.containers.c.current_cap",
        "stores": "T.stores.c.current_cap",
    }
    text = f"""\
simulation: {{sim_id: T, factor: 0, random_seed: 1}}
{section}: {{c: {{}}}}
arrivals: {{a: {{initial: 1, spawn: read}}}}
tasks:
  read: {{payload: {{capacity: {{gen: expr, expr: "registry['{path[section]}']"}}}}}}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, outcome.error
    value = _value_of([r for r in outcome.records if "task" in r], "read")
    assert "capacity" in value and value["capacity"] is None, (
        WHERE + f"A capacity with no limit must read as null, not {value}."
    )


def test_a_status_event_names_its_sim_id_and_service_in_fields_of_their_own(tmp_path):
    """A `queued` or `started` value holds `path_id`, `sim_id` and `service`, in that
    order before `status`, so a reader takes the two names with no split of `path_id`,
    which cannot tell a name that holds `.service.` from the separator."""
    text = """\
simulation: {sim_id: S.service.v2, factor: 0, random_seed: 1}
resources: {desk: {current_cap: 1}}
services: {front.service.desk: {dist: constant, value: 1}}
arrivals: {a: {initial: 1, spawn: serve}}
tasks:
  serve: {service: front.service.desk, resource: desk, payload: {done: 1}}
"""
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, outcome.error
    values = [r["value"] for r in outcome.records if r.get("task") == "serve"]
    statuses = [v for v in values if "status" in v]
    assert [v["status"] for v in statuses] == ["queued", "started"], statuses
    for value in statuses:
        assert list(value) == ["path_id", "sim_id", "service", "status"], (
            WHERE + f"A status value has the keys {list(value)}."
        )
        assert value["sim_id"] == "S.service.v2", value
        assert value["service"] == "front.service.desk", value
        assert value["path_id"] == "S.service.v2.service.front.service.desk", value
    finished = [v for v in values if "status" not in v]
    assert finished == [{"done": 1}], finished


SUBSCRIBE = """\
simulation: {{sim_id: T, factor: 0, random_seed: 1}}
signals: {{to: {{once: true}}, never: {{once: true}}}}
arrivals: {{m: {{initial: 1, spawn: make}}, p: {{initial: 1, spawn: pem}}}}
tasks:
  make: {{service_time: 1, payload: {{kind: made}}, on: [{{fire: {{signal: to, after: 2, value: ohai}}}}]}}
  other: {{service_time: 3, payload: {{kind: other_done}}}}
  pem: {{subscribe: {{{subscribe}}}, wait: {{signal: never}}, on_interrupt: abandon, on_interrupt_next: told, payload: {{kind: lost}}}}
  told: {{payload: {{cause: {{gen: expr, expr: "cause.value"}}}}}}
"""


def test_a_subscribed_interrupt_cause_value_is_a_mapping_with_event_and_value(
    tmp_path,
):
    """On a signal the mapping is `{event: null, value: <the signal's value>}`."""
    outcome = run_yaml_text(tmp_path, SUBSCRIBE.format(subscribe="signal: to"), {})
    assert outcome.error is None, outcome.error
    told = _value_of([r for r in outcome.records if "task" in r], "told")
    assert told["cause"] == {"event": None, "value": "ohai"}, (
        WHERE + f"cause.value is {told['cause']}."
    )


def test_a_task_subscription_names_the_event_in_the_cause_value(tmp_path):
    text = SUBSCRIBE.format(subscribe="task: other").replace(
        "arrivals: {m:", "arrivals: {o: {initial: 1, spawn: other}, m:"
    )
    outcome = run_yaml_text(tmp_path, text, {})
    assert outcome.error is None, outcome.error
    told = _value_of([r for r in outcome.records if "task" in r], "told")
    cause = told["cause"]
    assert cause == {"event": "other", "value": {"kind": "other_done"}}, (
        WHERE + f"cause.value is {cause}."
    )


def test_reserved_keys_are_kept():
    """A payload key that starts and ends with two underscores belongs to desflow: a
    sink that routes by one reads and removes it, and every other sink drops it.

    The rule and the drop are checked here. That each sink still routes by its key is
    guarded by the sink's own unit test, named beside `ROUTING_KEYS`."""
    message = (
        "The reserved payload keys are frozen from 1.0 (Reference, Records and "
        "Telemetry, Reserved Payload Keys; Compatibility Policy): "
    )
    # The rule: two underscores before and after at least one character
    keys = sorted(ROUTING_KEYS)
    for key in keys:
        assert egress_base.is_reserved(key), message + f"{key} is no longer reserved."
    assert egress_base.is_reserved("__anything__")
    for plain in ("_x_", "__x", "x__", "x", "____", 3):
        assert not egress_base.is_reserved(plain), message + f"{plain!r} is reserved."

    # A sink that routes by none drops every reserved key, and keeps the rest
    value = {key: 1 for key in keys} | {"__other__": 2, "_x_": 3, "n": 4}
    batch = [{"stream_type": "event", "key": "k", "value": value}]
    sink = RecordingEgress()
    dropped = egress_base.drop_reserved_keys(batch, sink)
    assert dropped[0]["value"] == {"_x_": 3, "n": 4}, message + str(dropped)
    assert batch[0]["value"] == value, "the record other sinks receive is unchanged"

    # A sink that routes by one keeps that key for itself and drops the others
    for key in keys:
        kept = egress_base.drop_reserved_keys(batch, RecordingEgress(), keep=(key,))
        assert set(kept[0]["value"]) == {key, "_x_", "n"}, message + key
