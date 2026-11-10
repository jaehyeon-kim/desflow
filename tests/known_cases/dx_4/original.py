"""DX-4: A payload that is constants and the instance number.

The original is the snippets' own Python, in two parts.

The order record: `original.yaml` is the model with
`payload: !python iceberg_tables_logic.order_record`. The test runs it, and the YAML form
has its `order_id` and `amount` at the same times for the same keys.

The prediction: `prediction` of `avro_and_pydantic_logic.py` returns `MLPrediction`, an
`AvroBaseModel` of `dataclasses-avroschema`, which is in the dev dependency group. The
guide's own file (`docs/snippets/guides/avro_and_pydantic_models.py`) defines the model.
The YAML form's record has the model's fields and no others, with the model's default
`event_type`, and the model's `confidence` and `task_id` as the snippet gives them for the
record's entity.
"""

import importlib
from types import SimpleNamespace

from tests.known_cases.harness import guide_avro_models

PYTHON_FORM = "tests/known_cases/dx_4/original.yaml"
LOGIC = "docs/snippets/yaml"  # the folder of iceberg_tables_logic.py
ORDER_FIELDS = ("order_id", "amount")


def _results(records):
    return [
        r
        for r in records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and "status" not in r["value"]
    ]


def compare(snippet_records, yaml_records):
    """The order record equals the snippet's, and the rest of the value is the model's."""
    wanted = [
        (r["sim_ts"], r["key"], r["task"], {k: r["value"][k] for k in ORDER_FIELDS})
        for r in _results(snippet_records)
    ]
    got = _results(yaml_records)
    assert wanted
    assert wanted == [
        (r["sim_ts"], r["key"], r["task"], {k: r["value"][k] for k in ORDER_FIELDS})
        for r in got
    ]
    with guide_avro_models() as models:
        fields = models.MLPrediction.model_fields
        logic = importlib.import_module("avro_and_pydantic_logic")
        made = logic.prediction(0, None, SimpleNamespace(key="orders-1")).model_dump()
        assert set(made) == set(fields) == {"event_type", "task_id", "confidence"}
        for record in got:
            value = record["value"]
            assert set(value) == set(ORDER_FIELDS) | set(fields), value
            assert value["event_type"] == made["event_type"] == "prediction"
            assert value["confidence"] == made["confidence"] == 0.9
            entity = SimpleNamespace(key=record["key"])
            assert value["task_id"] == logic.prediction(0, None, entity).task_id
