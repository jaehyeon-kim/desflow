"""The Confluent JSON Schema codec: schemas, references and record bytes.

It follows confluent-kafka 2.16.0 (`schema_registry/_sync/json_schema.py`), which checks
a record with `jsonschema` against the schema's draft. The Glue classes use
`glue_json.py` instead, which checks with fastjsonschema as aws-glue-schema-registry
1.1.3 did.
"""

from collections.abc import Callable, Iterable, Mapping
from typing import Any

import orjson
import referencing.jsonschema
from jsonschema import SchemaError, ValidationError
from jsonschema.protocols import Validator
from jsonschema.validators import validator_for
from referencing import Registry, Resource

from desflow.connectors._schema_registry.errors import SerializationError

# The draft of a referenced schema that has no `$schema`, as confluent-kafka reads it
# (`schema_registry/common/json_schema.py`, `DEFAULT_SPEC`).
_REFERENCE_DRAFT = referencing.jsonschema.DRAFT7  # type: ignore[attr-defined]


def load(schema_str: str) -> dict[str, Any]:
    """Parses a JSON Schema document.

    Args:
        schema_str (str): A JSON Schema as JSON.

    Returns:
        dict[str, Any]: The schema.

    Raises:
        SerializationError: If the text is not JSON, or is JSON but not an object.
    """
    try:
        schema = orjson.loads(schema_str)
    except orjson.JSONDecodeError as error:
        raise SerializationError(
            f"The JSON Schema is not valid JSON: {error}"
        ) from error
    if not isinstance(schema, dict):
        raise SerializationError(
            f"The JSON Schema must be a JSON object, not {type(schema).__name__}"
        )
    return schema


def resolve_references(
    references: Iterable[Mapping[str, Any]],
    fetch_reference: Callable[[str, int], dict[str, Any]],
    registry: Registry | None = None,
) -> Registry:
    """Fetches the schemas a schema refers to and collects them under their names.

    A referenced schema's own references are fetched first, as confluent-kafka does
    in `_resolve_named_schema`. A `$ref` to a name in the registry resolves to that
    schema. A `$ref` to anything else, such as a web address, is not fetched.

    Args:
        references (Iterable[Mapping[str, Any]]): The registry's `references` of the
            schema: dicts with `name`, `subject` and `version`.
        fetch_reference (Callable[[str, int], dict[str, Any]]): Returns the registry
            response for a subject and version, with `schema` and `references`.
        registry (Registry | None): The schemas collected so far. Used by the
            recursion.

    Returns:
        Registry: The `referencing` registry that holds every referenced schema.

    Raises:
        SerializationError: If a referenced schema is not a JSON object.
    """
    if registry is None:
        registry = Registry()
    for reference in references:
        response = fetch_reference(reference["subject"], reference["version"])
        registry = resolve_references(
            response.get("references") or [], fetch_reference, registry
        )
        resource = Resource.from_contents(
            load(response["schema"]), default_specification=_REFERENCE_DRAFT
        )
        registry = registry.with_resource(reference["name"], resource)
    return registry


def validator(schema: dict[str, Any], registry: Registry | None = None) -> Validator:
    """Builds the validator for a schema, in the draft its `$schema` names.

    A schema with no `$schema` is validated as the latest draft jsonschema supports
    (Draft 2020-12 in jsonschema 4.18.0 and later), as confluent-kafka does.

    Args:
        schema (dict[str, Any]): The parsed schema.
        registry (Registry | None): The referenced schemas. When none is given, an
            empty registry is used, so a `$ref` never makes a request.

    Returns:
        Validator: A jsonschema validator.

    Raises:
        SerializationError: If the schema is not a valid schema of its draft.
    """
    validator_class = validator_for(schema)
    try:
        validator_class.check_schema(schema)
    except SchemaError as error:
        raise SerializationError(
            f"The JSON Schema is not valid: {error.message}"
        ) from error
    return validator_class(
        schema, registry=Registry() if registry is None else registry
    )


def encode(validator: Validator, data: dict[str, Any]) -> bytes:
    """Writes a record as JSON after checking it against the schema.

    The record is written first and checked as read back, so a value that orjson
    writes as a string, such as a datetime, is checked as the string that goes on
    the wire.

    Args:
        validator (Validator): A validator from `validator`.
        data (dict[str, Any]): The record.

    Returns:
        bytes: The JSON bytes, with no framing.

    Raises:
        SerializationError: If the record cannot be written as JSON, does not match
            the schema, or cannot be checked against it.
    """
    try:
        encoded = orjson.dumps(data)
    except TypeError as error:
        raise SerializationError(
            f"The record cannot be written as JSON: {error}"
        ) from error
    _check(validator, orjson.loads(encoded))
    return encoded


def decode(validator: Validator, data: bytes | memoryview) -> dict[str, Any]:
    """Reads one JSON record and checks it against the schema.

    Args:
        validator (Validator): A validator from `validator`.
        data (bytes | memoryview): The JSON bytes, with no framing.

    Returns:
        dict[str, Any]: The record.

    Raises:
        SerializationError: If the data is not JSON, does not match the schema, or
            cannot be checked against it.
    """
    try:
        record = orjson.loads(data)
    except orjson.JSONDecodeError as error:
        raise SerializationError(f"The data is not valid JSON: {error}") from error
    _check(validator, record)
    return record  # type: ignore[no-any-return]


def _check(validator: Validator, record: Any) -> None:
    """Validates a record, turning every failure into a SerializationError."""
    try:
        validator.validate(record)
    except ValidationError as error:
        raise SerializationError(
            f"The record does not match the schema: {error.message}"
        ) from error
    # A `$ref` that is not in the registry raises referencing's Unresolvable, and a
    # record nested too deeply raises RecursionError.
    except Exception as error:
        raise SerializationError(
            f"The record cannot be checked against the schema: {error!r}"
        ) from error
