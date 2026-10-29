"""The AWS Glue Schema Registry client: finds, registers and fetches schema versions."""

import threading
import time
from typing import Any
from uuid import UUID

from desflow.connectors._schema_registry.errors import (
    SchemaRegistryError,
    SerializationError,
)

_DEFAULT_REGION = "us-east-1"
_VERSION_NOT_FOUND = "Schema version is not found."
_SCHEMA_NOT_FOUND = "Schema is not found."
_PENDING = "PENDING"
_AVAILABLE = "AVAILABLE"
# Glue checks a new version against the schema's compatibility mode before it is
# AVAILABLE. A check takes a few seconds, so the wait is up to 30 seconds.
_CHECKS = 10
_CHECK_INTERVAL_SECONDS = 3


def no_references(subject: str, version: int) -> dict[str, Any]:
    """Refuses a schema reference, for the Avro codec's `fetch_reference` argument.

    Args:
        subject (str): The subject a reference names.
        version (int): Its version.

    Raises:
        SerializationError: Always, because a Glue schema has no references.
    """
    raise SerializationError(
        f"The schema refers to {subject} version {version}, "
        "and Glue schemas have no references"
    )


class GlueRegistryClient:
    """The Glue calls of one registry, with the schema versions it has seen cached.

    The constructor makes no call to Glue. The client knows no format: the caller
    passes the definition as a string and the data format as Glue names it.
    """

    def __init__(self, registry_name: str, boto3_kwargs: dict[str, Any]):
        """Builds the boto3 Glue client.

        Args:
            registry_name (str): The name of the Glue registry.
            boto3_kwargs (dict[str, Any]): The arguments of `boto3.client`, such as
                `region_name` or `endpoint_url`. The region is `us-east-1` when
                the caller gave none.
        """
        import boto3

        self._registry_name = registry_name
        kwargs = dict(boto3_kwargs)
        if "region_name" not in kwargs:
            kwargs["region_name"] = _DEFAULT_REGION
        self._glue = boto3.client("glue", **kwargs)
        self._lock = threading.Lock()
        self._versions: dict[tuple[str, str, str], UUID] = {}
        self._schemas: dict[UUID, dict[str, str]] = {}

    def version_for(self, schema_name: str, definition: str, data_format: str) -> UUID:
        """Finds the version of a schema with this definition, registering it if new.

        The call is made once for each schema name, definition and data format. A
        failed call is not cached, so the next call repeats it. A schema that does
        not exist is created with the BACKWARD compatibility mode.

        Args:
            schema_name (str): The Glue schema name, such as `orders-value`.
            definition (str): The schema definition as a string.
            data_format (str): The format Glue names: `AVRO`, `JSON` or `PROTOBUF`.
                It is used only when the schema is created.

        Returns:
            UUID: The id of the schema version.

        Raises:
            SchemaRegistryError: If a Glue call fails, or a new version does not
                become AVAILABLE within 10 checks 3 seconds apart.
        """
        key = (schema_name, definition, data_format)
        with self._lock:
            if key not in self._versions:
                self._versions[key] = self._find_or_register(
                    schema_name, definition, data_format
                )
            return self._versions[key]

    def schema_version(self, version_id: UUID) -> dict[str, str]:
        """Fetches a schema version by its id, once for each id.

        Args:
            version_id (UUID): The id from the prefix of a message.

        Returns:
            dict[str, str]: `DataFormat` and `SchemaDefinition` of the version.

        Raises:
            SchemaRegistryError: If the Glue call fails or the version is not
                AVAILABLE.
        """
        with self._lock:
            if version_id not in self._schemas:
                response = self._call(
                    "get_schema_version", SchemaVersionId=str(version_id)
                )
                if (
                    response["SchemaVersionId"] is None
                    or response["Status"] != _AVAILABLE
                ):
                    raise SchemaRegistryError(
                        -1,
                        -1,
                        f"Schema version {version_id} was found but its status "
                        f"is {response['Status']}",
                    )
                self._schemas[version_id] = {
                    "DataFormat": response["DataFormat"],
                    "SchemaDefinition": response["SchemaDefinition"],
                }
            return self._schemas[version_id]

    def _find_or_register(
        self, schema_name: str, definition: str, data_format: str
    ) -> UUID:
        """Gets the version by definition, registering or creating what is missing."""
        schema_id = {"SchemaName": schema_name, "RegistryName": self._registry_name}
        try:
            response = self._call(
                "get_schema_by_definition",
                SchemaId=schema_id,
                SchemaDefinition=definition,
            )
        except SchemaRegistryError as error:
            # Glue answers EntityNotFoundException for both cases; only its message
            # tells a missing version of a known schema from a missing schema.
            cause = str(error.__cause__)
            if _VERSION_NOT_FOUND in cause:
                return self._register_version(schema_id, definition)
            if _SCHEMA_NOT_FOUND in cause:
                return self._create_schema(schema_name, definition, data_format)
            raise
        if response["SchemaVersionId"] is None or response["Status"] != _AVAILABLE:
            raise SchemaRegistryError(
                -1,
                -1,
                f"Schema version {response['SchemaVersionId']} was found but its "
                f"status is {response['Status']}",
            )
        return UUID(response["SchemaVersionId"])

    def _register_version(self, schema_id: dict[str, str], definition: str) -> UUID:
        """Adds a version to an existing schema and waits until it is AVAILABLE."""
        response = self._call(
            "register_schema_version", SchemaId=schema_id, SchemaDefinition=definition
        )
        return self._available(response["SchemaVersionId"], response["Status"])

    def _create_schema(
        self, schema_name: str, definition: str, data_format: str
    ) -> UUID:
        """Creates a schema with its first version.

        Another producer can create the schema between the lookup and the create;
        the definition is then registered as a version of the schema it created.
        """
        try:
            response = self._call(
                "create_schema",
                SchemaName=schema_name,
                RegistryId={"RegistryName": self._registry_name},
                DataFormat=data_format,
                Compatibility="BACKWARD",
                Description="",
                Tags={},
                SchemaDefinition=definition,
            )
        except SchemaRegistryError as error:
            if error.error_code != "AlreadyExistsException":
                raise
            schema_id = {"SchemaName": schema_name, "RegistryName": self._registry_name}
            return self._register_version(schema_id, definition)
        return self._available(
            response["SchemaVersionId"], response["SchemaVersionStatus"]
        )

    def _available(self, version_text: str, status: str) -> UUID:
        """Returns a new version's id, waiting while its status is PENDING.

        The status comes from the response that made the version: Glue names it
        `Status` in RegisterSchemaVersion and `SchemaVersionStatus` in CreateSchema.
        """
        version_id = UUID(version_text)
        for _ in range(_CHECKS):
            if status != _PENDING:
                break
            time.sleep(_CHECK_INTERVAL_SECONDS)
            status = self._call("get_schema_version", SchemaVersionId=str(version_id))[
                "Status"
            ]
        if status == _PENDING:
            raise SchemaRegistryError(
                -1,
                -1,
                f"Schema version {version_id} is still PENDING after {_CHECKS} "
                f"checks, {_CHECK_INTERVAL_SECONDS} seconds apart",
            )
        if status != _AVAILABLE:
            raise SchemaRegistryError(
                -1,
                -1,
                f"Schema evolution check failed. Schema version {version_id} "
                f"is in {status} status.",
            )
        return version_id

    def _call(self, operation: str, **kwargs: Any) -> Any:
        """Makes one Glue call and turns every exception into `SchemaRegistryError`.

        Raises:
            SchemaRegistryError: With the HTTP status and error code of a botocore
                `ClientError`, and -1 for both for any other exception.
        """
        try:
            return getattr(self._glue, operation)(**kwargs)
        except Exception as error:
            response = getattr(error, "response", None)
            if isinstance(response, dict) and "Error" in response:
                status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", -1)
                code = response["Error"].get("Code", -1)
                raise SchemaRegistryError(status, code, str(error)) from error
            raise SchemaRegistryError(-1, -1, str(error)) from error
