"""The Glue JSON Schema codec: schema versions and record bytes."""

from collections.abc import Callable
from typing import Any

import fastjsonschema
import orjson

from desflow.connectors._schema_registry.errors import SerializationError


def validator(definition: str) -> Callable[[Any], Any]:
    """Compiles a JSON schema version as aws-glue-schema-registry 1.1.3 did.

    1.1.3 (`aws_schema_registry/jsonschema.py`, `JsonSchema.__init__`) parsed the
    definition with orjson and called `fastjsonschema.compile` with its defaults,
    which fill the `default` of a missing property and check formats. A `$ref` to
    another document is fetched while the schema compiles.

    Args:
        definition (str): The schema version's definition, a JSON Schema as JSON.

    Returns:
        Callable[[Any], Any]: The compiled function, which validates a record and
            fills its defaults in place.

    Raises:
        SerializationError: If the definition is not JSON or the schema cannot be
            compiled.
    """
    try:
        schema = orjson.loads(definition)
    except orjson.JSONDecodeError as error:
        raise SerializationError(
            f"The JSON Schema is not valid JSON: {error}"
        ) from error
    try:
        return fastjsonschema.compile(schema)
    # fastjsonschema raises JsonSchemaDefinitionException for most schemas it cannot
    # compile, and other errors for the rest: a TypeError for a keyword of the wrong
    # type, re.error for a bad pattern, an error from urlopen for a failed fetch.
    except Exception as error:
        raise SerializationError(
            f"The JSON Schema cannot be compiled: {error}"
        ) from error


def encode(validate: Callable[[Any], Any], data: Any) -> bytes:
    """Checks a record against the schema and writes it as JSON, as 1.1.3's `write` did.

    1.1.3 validated the record and wrote it with orjson, so the bytes hold the
    defaults that fastjsonschema filled. The record is copied through JSON first, so
    the caller's dict is not changed and a value orjson writes as a string, such as
    a datetime, is checked as that string.

    Args:
        validate (Callable[[Any], Any]): A function from `validator`.
        data (Any): The record.

    Returns:
        bytes: The JSON bytes, with no framing.

    Raises:
        SerializationError: If the record cannot be written as JSON, does not match
            the schema, or cannot be checked against it.
    """
    try:
        record = orjson.loads(orjson.dumps(data))
    except TypeError as error:
        raise SerializationError(
            f"The record cannot be written as JSON: {error}"
        ) from error
    _validate(validate, record)
    return orjson.dumps(record)


def decode(validate: Callable[[Any], Any], data: bytes | memoryview) -> Any:
    """Reads one JSON record and validates it, as 1.1.3's `JsonSchema.read` did.

    Args:
        validate (Callable[[Any], Any]): A function from `validator`.
        data (bytes | memoryview): The JSON bytes, with no framing.

    Returns:
        Any: The record with its defaults filled, a dict for an object schema.

    Raises:
        SerializationError: If the data is not JSON, does not match the schema, or
            cannot be checked against it, such as a record nested too deeply or a
            `multipleOf` of 0.
    """
    try:
        record = orjson.loads(data)
    except orjson.JSONDecodeError as error:
        raise SerializationError(f"The data is not valid JSON: {error}") from error
    _validate(validate, record)
    return record


def _validate(validate: Callable[[Any], Any], record: Any) -> None:
    """Runs the compiled function and turns every failure into a SerializationError."""
    try:
        validate(record)
    except fastjsonschema.JsonSchemaValueException as error:
        raise SerializationError(
            f"The record does not match the schema: {error.message}"
        ) from error
    # The compiled code raises other errors for a record it cannot check: a
    # RecursionError for a record nested too deeply, a ZeroDivisionError for a
    # `multipleOf` of 0.
    except Exception as error:
        raise SerializationError(
            f"The record cannot be checked against the schema: {error!r}"
        ) from error
