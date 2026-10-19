"""The Record Shape table on Records and Telemetry matches the payload models.

The table names every field of `TelemetryPayload`, `EventPayload` and
`LabelPayload`, in the order the records carry them, with each field's type. A
field added, removed, reordered or retyped in a model fails here until the page is
updated, and so does a published record whose keys differ from its model.
"""

import typing
from pathlib import Path
from typing import Any

from desflow import DynamicRealtimeEnvironment
from desflow.models.schemas import EventPayload, LabelPayload, TelemetryPayload

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "docs" / "reference" / "records-and-telemetry.md"
MODELS = {"Telemetry": TelemetryPayload, "Event": EventPayload, "Label": LabelPayload}


def _table() -> list[dict[str, str]]:
    """The rows of the Record Shape table, one per field, in page order."""
    text = PAGE.read_text(encoding="utf-8")
    section = text.split("## Record Shape", 1)[1].split("\n## ", 1)[0]
    lines = [line for line in section.splitlines() if line.startswith("|")]
    header = [cell.strip() for cell in lines[0].strip("|").split("|")]
    rows = []
    for line in lines[2:]:
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        rows.append(dict(zip(header, cells)))
    return rows


def _type(annotation: Any) -> str:
    """The page's name for a model field's type."""
    if annotation is Any:
        return "any JSON value"
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Literal:
        return "string"
    if origin is typing.Union and type(None) in args:
        inner = [a for a in args if a is not type(None)]
        return f"{_type(inner[0])} or null"
    if origin is dict or annotation is dict:
        return "object"
    return {str: "string", float: "float", int: "integer"}[annotation]


def test_every_field_is_on_the_page_in_order_with_its_type():
    rows = _table()
    by_field = {row["Field"].strip("`"): row for row in rows}
    for column, model in MODELS.items():
        on_page = [row["Field"].strip("`") for row in rows if row[column]]
        assert on_page == list(model.model_fields), column
        for name, field in model.model_fields.items():
            expected = _type(field.annotation)
            # A field that defaults to None can be null whatever its annotation
            if field.default is None and not expected.endswith(" or null"):
                expected += " or null"
            assert by_field[name]["Type"] == expected, (column, name)


def test_published_records_have_the_models_keys_in_order():
    class Keep:
        def __init__(self):
            self.records: list = []

        async def run(self, egress_queue):
            import asyncio
            import queue

            while True:
                try:
                    self.records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)

    env = DynamicRealtimeEnvironment(factor=0)
    sink, labels = Keep(), Keep()
    env.setup_egress([sink, labels], labels=[False, True])
    env.publish_telemetry("Shop.x", 1)
    env.publish_event("orders-1", {"status": "placed"}, task="t", instance=0)
    env.publish_label({"fault": "f"}, path="Shop.x", old=1, new=2)
    env.teardown()
    records = sink.records + labels.records
    by_kind = {r["stream_type"]: r for r in records}
    assert list(by_kind["telemetry"]) == list(TelemetryPayload.model_fields)
    assert list(by_kind["event"]) == list(EventPayload.model_fields)
    assert list(by_kind["label"]) == list(LabelPayload.model_fields)
