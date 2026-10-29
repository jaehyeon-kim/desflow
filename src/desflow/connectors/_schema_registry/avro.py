"""The Avro codec: schema strings, schemas with references, and record bytes."""

import io
import json
from collections.abc import Callable, Iterable, Mapping
from typing import Any

import fastavro


def load_schema_str(schema_str: str) -> str:
    """Strips a schema string and wraps a bare primitive as a schema object.

    Args:
        schema_str (str): An Avro schema as JSON. A primitive may be written as
            `"string"`, which becomes `{"type":"string"}`.

    Returns:
        str: The schema string that the registry stores and the codec parses.
    """
    schema_str = schema_str.strip()
    if schema_str[:1] not in ("{", "["):
        schema_str = '{"type":' + schema_str + "}"
    return schema_str


def parse_writer_schema(
    schema_str: str,
    references: Iterable[Mapping[str, Any]],
    fetch_reference: Callable[[str, int], dict[str, Any]],
) -> Any:
    """Parses a schema together with the named schemas it references.

    Args:
        schema_str (str): The Avro schema as JSON.
        references (Iterable[Mapping[str, Any]]): The registry's `references` of the
            schema: dicts with `name`, `subject` and `version`.
        fetch_reference (Callable[[str, int], dict[str, Any]]): Returns the registry
            response for a subject and version, with `schema` and `references`.

    Returns:
        Any: The schema parsed by fastavro, which holds the schemas it references.
    """
    named_schemas: dict[str, Any] = {}
    _parse_references(references, fetch_reference, named_schemas, set())
    schema = json.loads(load_schema_str(schema_str))
    return fastavro.parse_schema(schema, named_schemas)


def _parse_references(
    references: Iterable[Mapping[str, Any]],
    fetch_reference: Callable[[str, int], dict[str, Any]],
    named_schemas: dict[str, Any],
    seen: set[tuple[str, int]],
) -> None:
    """Parses each referenced schema, depth first, into `named_schemas`."""
    for reference in references:
        key = (reference["subject"], reference["version"])
        if key in seen:
            # Two schemas can reference the same one; parsing it twice is an error.
            continue
        seen.add(key)
        response = fetch_reference(*key)
        _parse_references(
            response.get("references") or [], fetch_reference, named_schemas, seen
        )
        schema = json.loads(load_schema_str(response["schema"]))
        parsed = fastavro.parse_schema(schema, named_schemas)
        named_schemas.setdefault(reference["name"], parsed)


def _is_bytes_schema(parsed: Any) -> bool:
    """Tells whether a parsed schema is the primitive `bytes`, as confluent-kafka does."""
    return parsed == "bytes" or (
        isinstance(parsed, dict) and parsed.get("type") == "bytes"
    )


def encode(parsed: Any, record: Any) -> bytes:
    """Writes a record as Avro binary, with no framing.

    A key that the schema does not declare is dropped without an error, because
    `strict` is off. For a `bytes` schema the value is returned as it is, with no
    length prefix, and a `str` is encoded as UTF-8, as confluent-kafka does.

    Args:
        parsed (Any): A schema from `parse_writer_schema`.
        record (Any): The value to write, usually a dict.

    Returns:
        bytes: The Avro binary encoding of the record.
    """
    if _is_bytes_schema(parsed):
        return record if isinstance(record, bytes) else record.encode()
    buffer = io.BytesIO()
    fastavro.schemaless_writer(
        buffer, parsed, record, strict=False, strict_allow_default=False
    )
    return buffer.getvalue()


def decode(parsed: Any, data: bytes | memoryview) -> Any:
    """Reads one record of Avro binary, with the writer schema as the reader schema.

    Args:
        parsed (Any): The writer's schema from `parse_writer_schema`.
        data (bytes | memoryview): The Avro binary, with no framing.

    Returns:
        Any: The record, a dict for a record schema. For a `bytes` schema, all of
            `data` as `bytes`, as confluent-kafka reads it.
    """
    if _is_bytes_schema(parsed):
        return bytes(data)
    return fastavro.schemaless_reader(
        io.BytesIO(data), parsed, parsed, return_record_name=False
    )
