"""The Avro codec: schema strings, schemas with references, and record bytes."""

import io
import json
import math
from collections.abc import Callable, Iterable, Mapping
from typing import Any

import fastavro

from desflow.connectors._schema_registry.errors import SerializationError

_INTEGER_RANGES = {"int": (-(2**31), 2**31 - 1), "long": (-(2**63), 2**63 - 1)}


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
    """Writes a record as Avro binary, with no framing, as confluent-kafka does.

    A key that the schema does not declare is dropped without an error, because
    `strict` is off. A fraction, or a number outside the range of its field, for an
    `int` or `long` field is refused. For a `bytes` schema the value is returned as it is, with no
    length prefix, and a `str` is encoded as UTF-8. `encode_standard` is the
    function for AWS Glue, which writes the length prefix.

    Args:
        parsed (Any): A schema from `parse_writer_schema`.
        record (Any): The value to write, usually a dict.

    Returns:
        bytes: The Avro binary encoding of the record.

    Raises:
        SerializationError: An `int` or `long` field holds a fraction, `nan` or
            infinity, or a whole number outside its range.
    """
    if _is_bytes_schema(parsed):
        return record if isinstance(record, bytes) else record.encode()
    return encode_standard(parsed, record)


def encode_standard(parsed: Any, record: Any) -> bytes:
    """Writes a record as Avro binary for every schema, as the Avro specification says.

    A `bytes` schema is written with its length prefix, as aws-glue-schema-registry
    does. A key that the schema does not declare is dropped without an error. A
    fraction, or a number outside the range of its field, for an `int` or `long`
    field is refused.

    Args:
        parsed (Any): A schema from `parse_writer_schema`.
        record (Any): The value to write, usually a dict.

    Returns:
        bytes: The Avro binary encoding of the record.

    Raises:
        SerializationError: An `int` or `long` field holds a fraction, `nan` or
            infinity, or a whole number outside its range.
    """
    named = parsed.get("__named_schemas", {}) if isinstance(parsed, dict) else {}
    _check_integers(parsed, record, named, "the value")
    buffer = io.BytesIO()
    fastavro.schemaless_writer(
        buffer, parsed, record, strict=False, strict_allow_default=False
    )
    return buffer.getvalue()


def _check_integers(schema: Any, value: Any, named: Mapping[str, Any], at: str) -> None:
    """Refuses a number that an `int` or `long` field would write as another number.

    `fastavro` writes 1.5 into a `long` field as 1, and 2**40 into an `int` field
    without an error. A whole float such as 3.0 is written as 3, as the Iceberg sink
    does. A union is left to the writer, which refuses a value that no branch takes.

    Args:
        schema (Any): The parsed schema, or one part of it.
        value (Any): The value for that part.
        named (Mapping[str, Any]): The named schemas of the parsed schema.
        at (str): Where `value` sits, for the message.

    Raises:
        SerializationError: `value` is not a whole number in the range of the type.
    """
    if isinstance(schema, str) and schema in named:
        schema = named[schema]
    kind = schema.get("type") if isinstance(schema, dict) else schema
    if not isinstance(kind, str):
        return
    if kind in _INTEGER_RANGES:
        if isinstance(value, float) and not math.isfinite(value):
            raise SerializationError(
                f"{at} holds {value!r} for a field of the Avro type {kind}, "
                f"which an integer cannot hold"
            )
        if isinstance(value, float) and not value.is_integer():
            raise SerializationError(
                f"{at} holds {value!r} for a field of the Avro type {kind}, "
                f"which would lose its fraction"
            )
        low, high = _INTEGER_RANGES[kind]
        if isinstance(value, (int, float)) and not low <= value <= high:
            raise SerializationError(
                f"{at} holds {value!r}, outside the range of the Avro type {kind}"
            )
    elif kind == "record" and isinstance(value, Mapping):
        for field in schema["fields"]:
            if field["name"] in value:
                _check_integers(
                    field["type"],
                    value[field["name"]],
                    named,
                    f"field {field['name']!r}",
                )
    elif kind == "array" and isinstance(value, list):
        for item in value:
            _check_integers(schema["items"], item, named, at)
    elif kind == "map" and isinstance(value, Mapping):
        for item in value.values():
            _check_integers(schema["values"], item, named, at)


def decode(parsed: Any, data: bytes | memoryview) -> Any:
    """Reads one record of Avro binary, with the writer schema as the reader schema.

    Args:
        parsed (Any): The writer's schema from `parse_writer_schema`.
        data (bytes | memoryview): The Avro binary, with no framing.

    Returns:
        Any: The record, a dict for a record schema. For a `bytes` schema, all of
            `data` as `bytes`, as confluent-kafka reads it. `decode_standard` is
            the function for AWS Glue, which reads the length prefix.
    """
    if _is_bytes_schema(parsed):
        return bytes(data)
    return decode_standard(parsed, data)


def decode_standard(parsed: Any, data: bytes | memoryview) -> Any:
    """Reads one record of Avro binary for every schema, as the Avro specification says.

    A `bytes` schema is read after its length prefix, as aws-glue-schema-registry
    does.

    Args:
        parsed (Any): The writer's schema from `parse_writer_schema`.
        data (bytes | memoryview): The Avro binary, with no framing.

    Returns:
        Any: The record, a dict for a record schema.
    """
    return fastavro.schemaless_reader(
        io.BytesIO(data), parsed, parsed, return_record_name=False
    )
