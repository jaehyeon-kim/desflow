import asyncio
import json
import logging
import queue
import ssl
import threading
from collections.abc import Mapping
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    Union,
)

import orjson
from aiokafka import AIOKafkaProducer

from desflow.connectors.admin.kafka import (
    KafkaAdminConnector,
    admin_settings,
    apply_topic_policies,
    with_security,
)
from desflow.connectors.admin.kafka import topic_policy as read_topic_policy
from desflow.connectors._schema_registry.unset import UNSET
from desflow.connectors._retry import FIRST_DELAY, next_delay
from desflow.connectors._schema_registry.errors import (  # noqa: F401
    SchemaRegistryError,
    SerializationError,
    is_temporary,
    refuses_access,
)
from desflow.connectors.egress.base import (
    BaseEgress,
    drop_reserved_keys,
    extract_dict,
    take_route,
)

logger = logging.getLogger(__name__)

# ==========================================
# Pluggable Serializer Interfaces
# ==========================================


class MessageSerializer(Protocol):
    """
    Protocol defining the interface for all Kafka egress serializers.

    Any custom serializer passed to `KafkaEgress` must implement the
    `serialize` method according to this signature.
    """

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given payload into a byte string for Kafka transmission.

        Args:
            topic (str): The destination Kafka topic. This is often required by
                schema registries (like Avro) to resolve the correct schema subject,
                even if simpler serializers (like JSON) choose to ignore it.
            data (Any): The payload to serialize. This is typically a raw dictionary
                or a Pydantic model instance.

        Returns:
            bytes: The fully encoded byte string ready to be published to Kafka.
        """
        ...


class JsonSerializer:
    """
    Default fallback serializer providing backward compatibility via orjson.

    This serializer converts dictionaries or Pydantic models into standard
    JSON byte strings.
    """

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data to a JSON byte string.

        Args:
            topic (str): The target Kafka topic (unused by this serializer but required by protocol).
            data (Any): The payload to serialize (dictionary or Pydantic model).

        Returns:
            bytes: The JSON-encoded byte string.
        """
        payload = extract_dict(data)
        return orjson.dumps(payload)


class ConfluentAvroSerializer:
    """
    Lazy-loaded serializer for Confluent Schema Registry using fastavro and httpx.

    This class converts simulation data into Avro-encoded byte strings in the
    Confluent wire format. It registers the schema under the subject
    `<topic>-value` on the first record of each topic, and reuses the schema id
    after that.
    """

    def __init__(
        self,
        registry_url: str,
        schema_str: str,
        *,
        auth: Optional[Tuple[str, str]] = UNSET,
        verify: Union[bool, str, ssl.SSLContext] = UNSET,
        cert: Optional[
            Union[
                str,
                Tuple[str, Optional[str]],
                Tuple[str, Optional[str], Optional[str]],
            ]
        ] = UNSET,
        timeout: Optional[float] = UNSET,
        headers: Optional[Mapping[str, str]] = UNSET,
        proxy: Optional[str] = UNSET,
        max_retries: int = UNSET,
    ):
        """
        Initializes the Confluent Avro serializer. It makes no request to the registry.

        `UNSET` as a default, in the signature or the Default column of the API page,
        means the argument was not given; each argument's description says its real
        default.

        Args:
            registry_url (str): The base URL of the Confluent Schema Registry.
                Several URLs separated by commas are tried in order.
            schema_str (str): The Avro schema defined as a JSON string (typically
                generated from a dataclasses-avroschema `AvroBaseModel` via
                `.avro_schema()`; a plain Pydantic model does not provide it).
            auth (Optional[Tuple[str, str]]): A username and password for basic
                authentication, for example `("user", "pass")`. The default is None.
            verify (bool | str | ssl.SSLContext): How to check the registry's
                certificate: True (the default), False, the path of a CA file or
                directory, or an SSL context.
            cert (str | tuple | None): A client certificate file, or a tuple of the
                certificate file, the key file and the key password. The default is None.
            timeout (Optional[float]): Seconds each step of a request to the registry
                (connecting, sending, reading) may take. The default is 10.0. None
                waits without a limit.
            headers (Optional[Mapping[str, str]]): Headers sent with every request,
                for example `{"Authorization": "Bearer <token>"}`. The default is None.
            proxy (Optional[str]): The URL of a proxy. The default is None.
            max_retries (int): How many times a request is repeated after a
                transport error or a status of 408, 429, 500, 502, 503 or 504. The
                default is 3.

        Raises:
            ImportError: If the 'fastavro' or 'httpx' package is not installed.
        """
        try:
            import fastavro  # noqa: F401
            import httpx  # noqa: F401

            from desflow.connectors._schema_registry import avro
            from desflow.connectors._schema_registry.confluent_client import (
                ConfluentRegistryClient,
                given_arguments,
            )
            from desflow.connectors._schema_registry.wire import frame_confluent
        except ImportError:
            raise ImportError(
                "The 'fastavro' and 'httpx' packages are required to use "
                "ConfluentAvroSerializer. "
                "Install them using: pip install desflow[confluent]"
            )

        options = given_arguments(
            auth=auth,
            verify=verify,
            cert=cert,
            timeout=timeout,
            headers=headers,
            proxy=proxy,
            max_retries=max_retries,
        )
        self._client = ConfluentRegistryClient(registry_url, **options)
        self._avro = avro
        self._frame = frame_confluent
        self._schema_str = avro.load_schema_str(schema_str)
        self._parsed = avro.parse_writer_schema(
            self._schema_str, (), self._client.subject_version
        )

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data into an Avro byte string.

        The first call for a topic registers the schema under `<topic>-value`, which
        makes a request to the registry.

        Args:
            topic (str): The target Kafka topic, used for schema subject naming.
            data (Any): The payload to serialize (dictionary or Pydantic model).

        Returns:
            bytes: The Avro-encoded byte string, with the Confluent prefix.

        Raises:
            SchemaRegistryError: If the registry refuses the schema.
            httpx.TransportError: If no registry URL could be reached.
        """
        payload = extract_dict(data)
        schema_id = self._client.register(f"{topic}-value", self._schema_str, "AVRO")
        return self._frame(schema_id, self._avro.encode(self._parsed, payload))


class GlueAvroSerializer:
    """
    Lazy-loaded serializer for AWS Glue Schema Registry using boto3.

    This class converts simulation data into Avro-encoded byte strings,
    integrating directly with AWS Glue Schema Registry for schema validation.
    """

    def __init__(self, registry_name: str, schema_str: str, **boto3_kwargs: Any):
        """
        Initializes the AWS Glue Avro serializer.

        The constructor makes no call to Glue. The first `serialize` for a topic finds
        or registers the schema.

        Args:
            registry_name (str): The name of the AWS Glue Schema Registry.
            schema_str (str): The Avro schema defined as a JSON string (typically
                generated from a dataclasses-avroschema `AvroBaseModel` via
                `.avro_schema()`; a plain Pydantic model does not provide it).
            **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
                initialization (e.g., `region_name`, `aws_access_key_id`).

        Raises:
            ImportError: If the 'boto3' or 'fastavro' package is not installed.
        """
        try:
            import boto3  # noqa: F401
            import fastavro  # noqa: F401

            from desflow.connectors._schema_registry import avro
            from desflow.connectors._schema_registry.glue_client import (
                GlueRegistryClient,
                no_references,
            )
            from desflow.connectors._schema_registry.wire import frame_glue
        except ImportError:
            raise ImportError(
                "The 'boto3' and 'fastavro' packages are required. "
                "Install them using: pip install desflow[glue]"
            )

        self._client = GlueRegistryClient(registry_name, boto3_kwargs)
        self._avro = avro
        self._frame = frame_glue
        # Glue finds a version by the exact definition string, so the same schema
        # written with other spacing or key order is the same version.
        self._definition = json.dumps(json.loads(schema_str))
        self._parsed = avro.parse_writer_schema(schema_str, (), no_references)

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data into an Avro byte string using AWS Glue.

        The first call for a topic finds or registers the schema under
        `<topic>-value`, which calls Glue.

        Args:
            topic (str): The target Kafka topic.
            data (Any): The payload to serialize (dictionary or Pydantic model).

        Returns:
            bytes: The Avro-encoded byte string, with the Glue prefix.

        Raises:
            SchemaRegistryError: If a Glue call fails or the new version does not
                become available.
        """
        payload = extract_dict(data)
        version_id = self._client.version_for(
            f"{topic}-value", self._definition, "AVRO"
        )
        return self._frame(
            version_id, self._avro.encode_standard(self._parsed, payload)
        )


class ConfluentJsonSchemaSerializer:
    """
    Lazy-loaded serializer for Confluent Schema Registry using jsonschema and httpx.

    This class converts simulation data into JSON in the Confluent wire format. It
    checks each record against the schema, registers the schema under the subject
    `<topic>-value` on the first record of each topic, and reuses the schema id after
    that.
    """

    def __init__(
        self,
        registry_url: str,
        schema_str: str,
        *,
        auth: Optional[Tuple[str, str]] = UNSET,
        verify: Union[bool, str, ssl.SSLContext] = UNSET,
        cert: Optional[
            Union[
                str,
                Tuple[str, Optional[str]],
                Tuple[str, Optional[str], Optional[str]],
            ]
        ] = UNSET,
        timeout: Optional[float] = UNSET,
        headers: Optional[Mapping[str, str]] = UNSET,
        proxy: Optional[str] = UNSET,
        max_retries: int = UNSET,
    ):
        """
        Initializes the Confluent JSON Schema serializer. It makes no request to the
        registry.

        `UNSET` as a default, in the signature or the Default column of the API page,
        means the argument was not given; each argument's description says its real
        default.

        Args:
            registry_url (str): The base URL of the Confluent Schema Registry.
                Several URLs separated by commas are tried in order.
            schema_str (str): The JSON Schema as a JSON string. A schema with no
                `$schema` is checked as Draft 2020-12, as confluent-kafka does.
            auth (Optional[Tuple[str, str]]): A username and password for basic
                authentication, for example `("user", "pass")`. The default is None.
            verify (bool | str | ssl.SSLContext): How to check the registry's
                certificate: True (the default), False, the path of a CA file or
                directory, or an SSL context.
            cert (str | tuple | None): A client certificate file, or a tuple of the
                certificate file, the key file and the key password. The default is None.
            timeout (Optional[float]): Seconds each step of a request to the registry
                (connecting, sending, reading) may take. The default is 10.0. None
                waits without a limit.
            headers (Optional[Mapping[str, str]]): Headers sent with every request,
                for example `{"Authorization": "Bearer <token>"}`. The default is None.
            proxy (Optional[str]): The URL of a proxy. The default is None.
            max_retries (int): How many times a request is repeated after a
                transport error or a status of 408, 429, 500, 502, 503 or 504. The
                default is 3.

        Raises:
            ImportError: If the 'jsonschema' or 'httpx' package is not installed.
            SerializationError: If the schema is not valid JSON or not a valid schema.
        """
        try:
            import httpx  # noqa: F401
            import jsonschema  # noqa: F401

            from desflow.connectors._schema_registry import json_schema
            from desflow.connectors._schema_registry.confluent_client import (
                ConfluentRegistryClient,
                given_arguments,
            )
            from desflow.connectors._schema_registry.wire import frame_confluent
        except ImportError:
            raise ImportError(
                "The 'jsonschema' and 'httpx' packages are required to use "
                "ConfluentJsonSchemaSerializer. "
                "Install them using: pip install desflow[confluent]"
            )

        options = given_arguments(
            auth=auth,
            verify=verify,
            cert=cert,
            timeout=timeout,
            headers=headers,
            proxy=proxy,
            max_retries=max_retries,
        )
        self._client = ConfluentRegistryClient(registry_url, **options)
        self._json_schema = json_schema
        self._frame = frame_confluent
        self._schema_str = schema_str
        self._validator = json_schema.validator(json_schema.load(schema_str))

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data into a JSON byte string.

        The first call for a topic registers the schema under `<topic>-value`, which
        makes a request to the registry.

        Args:
            topic (str): The target Kafka topic, used for schema subject naming.
            data (Any): The payload to serialize (dictionary or Pydantic model).

        Returns:
            bytes: The JSON bytes, with the Confluent prefix.

        Raises:
            SerializationError: If the record does not match the schema.
            SchemaRegistryError: If the registry refuses the schema.
            httpx.TransportError: If no registry URL could be reached.
        """
        body = self._json_schema.encode(self._validator, extract_dict(data))
        schema_id = self._client.register(f"{topic}-value", self._schema_str, "JSON")
        return self._frame(schema_id, body)


class ConfluentProtobufSerializer:
    """
    Lazy-loaded serializer for Confluent Schema Registry using protobuf and httpx.

    This class converts simulation data into Protobuf messages in the Confluent wire
    format. The schema is the file of the message class. On the first record of each
    topic it registers the files that the schema imports, then the schema under the
    subject `<topic>-value`, and it reuses the schema id after that.
    """

    def __init__(
        self,
        registry_url: str,
        message_type: Any,
        *,
        auth: Optional[Tuple[str, str]] = UNSET,
        verify: Union[bool, str, ssl.SSLContext] = UNSET,
        cert: Optional[
            Union[
                str,
                Tuple[str, Optional[str]],
                Tuple[str, Optional[str], Optional[str]],
            ]
        ] = UNSET,
        timeout: Optional[float] = UNSET,
        headers: Optional[Mapping[str, str]] = UNSET,
        proxy: Optional[str] = UNSET,
        max_retries: int = UNSET,
    ):
        """
        Initializes the Confluent Protobuf serializer. It makes no request to the
        registry.

        `UNSET` as a default, in the signature or the Default column of the API page,
        means the argument was not given; each argument's description says its real
        default.

        Args:
            registry_url (str): The base URL of the Confluent Schema Registry.
                Several URLs separated by commas are tried in order.
            message_type (type[Message]): The generated Protobuf message class. Its
                file is registered as the schema.
            auth (Optional[Tuple[str, str]]): A username and password for basic
                authentication, for example `("user", "pass")`. The default is None.
            verify (bool | str | ssl.SSLContext): How to check the registry's
                certificate: True (the default), False, the path of a CA file or
                directory, or an SSL context.
            cert (str | tuple | None): A client certificate file, or a tuple of the
                certificate file, the key file and the key password. The default is None.
            timeout (Optional[float]): Seconds each step of a request to the registry
                (connecting, sending, reading) may take. The default is 10.0. None
                waits without a limit.
            headers (Optional[Mapping[str, str]]): Headers sent with every request,
                for example `{"Authorization": "Bearer <token>"}`. The default is None.
            proxy (Optional[str]): The URL of a proxy. The default is None.
            max_retries (int): How many times a request is repeated after a
                transport error or a status of 408, 429, 500, 502, 503 or 504. The
                default is 3.

        Raises:
            ImportError: If the 'protobuf' or 'httpx' package is not installed.
        """
        try:
            import google.protobuf  # noqa: F401
            import httpx  # noqa: F401

            from desflow.connectors._schema_registry import protobuf
            from desflow.connectors._schema_registry.confluent_client import (
                ConfluentRegistryClient,
                given_arguments,
            )
            from desflow.connectors._schema_registry.wire import frame_confluent
        except ImportError:
            raise ImportError(
                "The 'protobuf' and 'httpx' packages are required to use "
                "ConfluentProtobufSerializer. "
                "Install them using: pip install desflow[confluent]"
            )

        options = given_arguments(
            auth=auth,
            verify=verify,
            cert=cert,
            timeout=timeout,
            headers=headers,
            proxy=proxy,
            max_retries=max_retries,
        )
        self._client = ConfluentRegistryClient(registry_url, **options)
        self._protobuf = protobuf
        self._frame = frame_confluent
        self._message_type = message_type
        self._schema_str = protobuf.file_descriptor_b64(message_type.DESCRIPTOR.file)
        self._indexes = protobuf.confluent_indexes(message_type.DESCRIPTOR)
        # The files the schema imports, registered on the first record.
        self._references: Optional[list] = None
        self._lock = threading.Lock()

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data into a Protobuf byte string.

        The first call registers the imported files and, for a topic, the schema under
        `<topic>-value`, which makes requests to the registry.

        Args:
            topic (str): The target Kafka topic, used for schema subject naming.
            data (Any): An instance of the message class, or a dictionary or Pydantic
                model. A key the schema does not have is dropped.

        Returns:
            bytes: The Protobuf bytes, with the Confluent prefix and message index.

        Raises:
            SerializationError: If the record does not fit the message class.
            SchemaRegistryError: If the registry refuses a schema.
            httpx.TransportError: If no registry URL could be reached.
        """
        body = self._protobuf.encode(self._message_type, extract_dict(data))
        schema_id = self._client.register(
            f"{topic}-value", self._schema_str, "PROTOBUF", self._imports()
        )
        return self._frame(schema_id, self._indexes + body)

    def _imports(self) -> list:
        """Registers the imported files once and returns them as references."""
        with self._lock:
            if self._references is None:
                self._references = self._protobuf.register_dependencies(
                    self._message_type.DESCRIPTOR.file, self._client
                )
            return self._references


class GlueJsonSchemaSerializer:
    """
    Lazy-loaded serializer for AWS Glue Schema Registry using boto3 and fastjsonschema.

    This class converts simulation data into JSON with the AWS Glue prefix. It checks
    each record against the schema as aws-glue-schema-registry 1.1.3 did, so a
    missing property that has a `default` is filled in.
    """

    def __init__(self, registry_name: str, schema_str: str, **boto3_kwargs: Any):
        """
        Initializes the AWS Glue JSON Schema serializer.

        The constructor makes no call to Glue. The first `serialize` for a topic finds
        or registers the schema.

        Args:
            registry_name (str): The name of the AWS Glue Schema Registry.
            schema_str (str): The JSON Schema as a JSON string.
            **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
                initialization (e.g., `region_name`, `aws_access_key_id`).

        Raises:
            ImportError: If the 'boto3' or 'fastjsonschema' package is not installed.
            SerializationError: If the schema is not valid JSON or cannot be compiled.
        """
        try:
            import boto3  # noqa: F401
            import fastjsonschema  # noqa: F401

            from desflow.connectors._schema_registry import glue_json
            from desflow.connectors._schema_registry.glue_client import (
                GlueRegistryClient,
            )
            from desflow.connectors._schema_registry.wire import frame_glue
        except ImportError:
            raise ImportError(
                "The 'boto3' and 'fastjsonschema' packages are required. "
                "Install them using: pip install desflow[glue]"
            )

        self._client = GlueRegistryClient(registry_name, boto3_kwargs)
        self._glue_json = glue_json
        self._frame = frame_glue
        try:
            schema = json.loads(schema_str)
        except ValueError as error:
            raise SerializationError(
                f"The JSON Schema is not valid JSON: {error}"
            ) from error
        # aws-glue-schema-registry 1.1.3 sent the schema as orjson wrote it.
        self._definition = orjson.dumps(schema).decode()
        self._validate = glue_json.validator(self._definition)

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data into a JSON byte string using AWS Glue.

        The first call for a topic finds or registers the schema under
        `<topic>-value`, which calls Glue.

        Args:
            topic (str): The target Kafka topic.
            data (Any): The payload to serialize (dictionary or Pydantic model).

        Returns:
            bytes: The JSON bytes, with the Glue prefix.

        Raises:
            SerializationError: If the record does not match the schema.
            SchemaRegistryError: If a Glue call fails or the new version does not
                become available.
        """
        body = self._glue_json.encode(self._validate, extract_dict(data))
        version_id = self._client.version_for(
            f"{topic}-value", self._definition, "JSON"
        )
        return self._frame(version_id, body)


class GlueProtobufSerializer:
    """
    Lazy-loaded serializer for AWS Glue Schema Registry using boto3 and protobuf.

    This class converts simulation data into Protobuf messages with the AWS Glue
    prefix and message index. Glue stores the schema as `.proto` text, which the
    caller passes in, because protobuf cannot print it from the message class.
    """

    def __init__(
        self,
        registry_name: str,
        message_type: Any,
        schema_str: str,
        **boto3_kwargs: Any,
    ):
        """
        Initializes the AWS Glue Protobuf serializer.

        The constructor makes no call to Glue. The first `serialize` for a topic finds
        or registers the schema.

        Args:
            registry_name (str): The name of the AWS Glue Schema Registry.
            message_type (type[Message]): The generated Protobuf message class.
            schema_str (str): The text of the `.proto` file that the class was
                generated from. It is registered as the schema as it is, so it must
                declare the same messages, in the same file, as the class.
            **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
                initialization (e.g., `region_name`, `aws_access_key_id`).

        Raises:
            ImportError: If the 'boto3' or 'protobuf' package is not installed.
        """
        try:
            import boto3  # noqa: F401
            import google.protobuf  # noqa: F401

            from desflow.connectors._schema_registry import protobuf
            from desflow.connectors._schema_registry.glue_client import (
                GlueRegistryClient,
            )
            from desflow.connectors._schema_registry.wire import frame_glue
        except ImportError:
            raise ImportError(
                "The 'boto3' and 'protobuf' packages are required. "
                "Install them using: pip install desflow[glue]"
            )

        self._client = GlueRegistryClient(registry_name, boto3_kwargs)
        self._protobuf = protobuf
        self._frame = frame_glue
        self._message_type = message_type
        self._definition = schema_str
        self._index = protobuf.glue_index(message_type.DESCRIPTOR)

    def serialize(self, topic: str, data: Any) -> bytes:
        """
        Serializes the given data into a Protobuf byte string using AWS Glue.

        The first call for a topic finds or registers the schema under
        `<topic>-value`, which calls Glue.

        Args:
            topic (str): The target Kafka topic.
            data (Any): An instance of the message class, or a dictionary or Pydantic
                model. A key the schema does not have is dropped.

        Returns:
            bytes: The Protobuf bytes, with the Glue prefix and message index.

        Raises:
            SerializationError: If the record does not fit the message class.
            SchemaRegistryError: If a Glue call fails or the new version does not
                become available.
        """
        body = self._protobuf.encode(self._message_type, extract_dict(data))
        version_id = self._client.version_for(
            f"{topic}-value", self._definition, "PROTOBUF"
        )
        return self._frame(version_id, self._index + body)


# ==========================================
# Core Egress Class
# ==========================================


# The six serializer types of a `serializer` setting, with the registry they use
SERIALIZER_TYPES = (
    "confluent_avro",
    "confluent_json_schema",
    "confluent_protobuf",
    "glue_avro",
    "glue_json_schema",
    "glue_protobuf",
)
_REGISTRY_ARGUMENTS = (
    "auth",
    "verify",
    "cert",
    "timeout",
    "headers",
    "proxy",
    "max_retries",
)


def _schema_text(spec: Mapping, kind: str) -> str:
    """Reads `schema` (text, or a mapping that is written as JSON) or `schema_file`."""
    given = [key for key in ("schema", "schema_file") if spec.get(key) is not None]
    if len(given) > 1:
        raise ValueError(f"serializer {kind} takes schema or schema_file, not both")
    if not given:
        raise ValueError(f"serializer {kind} needs schema or schema_file")
    if given[0] == "schema_file":
        with open(spec["schema_file"], encoding="utf-8") as handle:
            return handle.read()
    schema = spec["schema"]
    return schema if isinstance(schema, str) else json.dumps(schema)


def _message_class(spec: Mapping) -> Any:
    """Builds a Protobuf message class from `descriptor_file` and `message`.

    The descriptor file is a descriptor set written by
    `protoc --descriptor_set_out=<file> --include_imports <proto file>`, and `message`
    is the full name of one message in it, such as `shop.Order`.
    """
    if not spec.get("descriptor_file") or not spec.get("message"):
        raise ValueError("a Protobuf serializer needs descriptor_file and message")
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    with open(spec["descriptor_file"], "rb") as handle:
        descriptors = descriptor_pb2.FileDescriptorSet.FromString(handle.read())
    pool = descriptor_pool.DescriptorPool()
    for proto in descriptors.file:
        pool.Add(proto)
    try:
        descriptor = pool.FindMessageTypeByName(spec["message"])
    except KeyError:
        raise ValueError(
            f"the descriptor file has no message '{spec['message']}'"
        ) from None
    return message_factory.GetMessageClass(descriptor)


def build_serializer(spec: Mapping[str, Any]) -> Any:
    """
    Builds a message serializer from the mapping a blueprint's `serializer` holds.

    The mapping has a `type`, one of `confluent_avro`, `confluent_json_schema`,
    `confluent_protobuf`, `glue_avro`, `glue_json_schema` and `glue_protobuf`. The
    Confluent types take `registry_url` and the registry arguments `auth`, `verify`,
    `cert`, `timeout`, `headers`, `proxy` and `max_retries`. The Glue types take
    `registry_name` and `boto3`, a mapping passed to `boto3.client`. Avro and JSON
    Schema take the schema as `schema` (text) or `schema_file`. Protobuf takes
    `descriptor_file` and `message`, and Glue Protobuf also the `.proto` text as
    `schema` or `schema_file`. Nothing contacts the registry here.

    Args:
        spec: The `serializer` mapping.

    Returns:
        Any: The serializer object, such as a `ConfluentAvroSerializer`.

    Raises:
        ValueError: If `type` is missing or unknown, a key does not belong to the
            type, or a required key is missing.
    """
    if not isinstance(spec, Mapping):
        raise ValueError(f"serializer must be a mapping, got {spec!r}")
    kind = spec.get("type")
    if kind not in SERIALIZER_TYPES:
        raise ValueError(
            f"serializer type '{kind}' is not one of {', '.join(SERIALIZER_TYPES)}"
        )
    protobuf = kind.endswith("protobuf")
    glue = kind.startswith("glue")
    allowed = {"type", "schema", "schema_file"} if not protobuf else {"type"}
    if protobuf:
        allowed |= {"descriptor_file", "message"}
        if glue:
            allowed |= {"schema", "schema_file"}
    allowed |= (
        {"registry_name", "boto3"} if glue else {"registry_url", *_REGISTRY_ARGUMENTS}
    )
    unknown = sorted(set(spec) - allowed)
    if unknown:
        raise ValueError(
            f"serializer {kind} has unknown key(s) {unknown}; the keys are "
            f"{', '.join(sorted(allowed))}"
        )
    if glue:
        name = spec.get("registry_name")
        if not name:
            raise ValueError(f"serializer {kind} needs registry_name")
        boto = dict(spec.get("boto3") or {})
        if kind == "glue_avro":
            return GlueAvroSerializer(name, _schema_text(spec, kind), **boto)
        if kind == "glue_json_schema":
            return GlueJsonSchemaSerializer(name, _schema_text(spec, kind), **boto)
        return GlueProtobufSerializer(
            name, _message_class(spec), _schema_text(spec, kind), **boto
        )
    url = spec.get("registry_url")
    if not url:
        raise ValueError(f"serializer {kind} needs registry_url")
    options: Dict[str, Any] = {
        key: spec[key] for key in _REGISTRY_ARGUMENTS if key in spec
    }
    for key in ("auth", "cert"):
        if isinstance(options.get(key), list):
            options[key] = tuple(options[key])
    if kind == "confluent_avro":
        return ConfluentAvroSerializer(url, _schema_text(spec, kind), **options)
    if kind == "confluent_json_schema":
        return ConfluentJsonSchemaSerializer(url, _schema_text(spec, kind), **options)
    return ConfluentProtobufSerializer(url, _message_class(spec), **options)


class KafkaEgress(BaseEgress):
    """
    High-throughput Kafka producer for simulation telemetry and events.

    This connector utilizes `aiokafka` for asynchronous I/O and `orjson` for fast
    serialization. It implements a resilient connection loop with exponential
    backoff.

    By default, data is routed to the `telemetry_topic` or `event_topic` based on
    its `stream_type`. If a `topic_router` callable is provided, topic selection
    is delegated to that function instead, allowing for advanced multiplexing
    (e.g., splitting ML vs. UI events).

    Without a `topic_router`, the event and telemetry topics are created at start
    when they do not exist, with one partition and a replication factor of one.
    With a router the topics are left to the caller, because only the router knows
    which ones it uses.

    Note:
        By default `stream_type` is treated as routing metadata and is left out of
        the published message, because the topic already identifies the stream.
        `ParquetStorageEgress` keeps `stream_type` as a column, so a downstream
        filter written against Parquet files matches nothing when it is pointed at
        a Kafka topic. Set `include_stream_type=True` to keep the field in the
        message and make both paths carry it.

    An event whose `value` holds `__topic__` is sent to the topic it names, and the key
    is removed from the message. This replaces a `topic_router` for the common case.
    A `topic_router` wins over `__topic__`, and telemetry always goes to
    `telemetry_topic`. `topic_policy` and `topics` create the topics, `serializer`
    and `topics.<topic>.serializer` set the serializers from a mapping, `ssl` and
    `sasl_oauth_token_provider` build the connection settings from a mapping, and
    an entry added with `envelope=False` sends the event's value alone as the
    message. An event whose value is null is then sent as a tombstone: a message
    with the record's key and a null value, which a compacted topic reads as a
    delete of that key.

    Attributes:
        bootstrap_servers (str): Comma-separated list of Kafka brokers.
        telemetry_topic (str): The default topic name for telemetry data.
        event_topic (str): The default topic name for lifecycle events.
        topic_router (Callable): Optional external logic to determine the topic.
        topic_serializers (Optional[Dict[str, MessageSerializer]]): Optional mapping of topics to specific serializers.
        default_serializer (Optional[MessageSerializer]): Fallback serializer if a topic is not in `topic_serializers`.
        include_stream_type (bool): Whether the published message keeps the `stream_type` field.
        producer_config (dict): Configuration dictionary passed to AIOKafkaProducer.

    Examples:
        Defining a custom topic router to split machine learning events from standard telemetry:

        ```python
        def custom_topic_router(data: dict) -> str:
            stream_type = data.get("stream_type")
            if stream_type == "telemetry":
                return "sim-telemetry"

            value = data.get("value", {})
            if isinstance(value, dict):
                event_type = value.get("event_type")
                if event_type == "prediction_request":
                    return "mill-predictions"
                elif event_type == "ground_truth":
                    return "mill-groundtruth"

            return "mill-lifecycle"

        # Pass it to the egress connector
        egress = KafkaEgress(
            bootstrap_servers="127.0.0.1:9092",
            topic_router=custom_topic_router
        )
        ```
    """

    def __init__(
        self,
        bootstrap_servers: str,
        telemetry_topic: str = "sim-telemetry",
        event_topic: str = "sim-events",
        topic_router: Optional[Callable[[dict], str]] = None,
        topic_serializers: Optional[Dict[str, MessageSerializer]] = None,
        default_serializer: Optional[MessageSerializer] = None,
        include_stream_type: bool = False,
        topic_policy: Any = None,
        topics: Optional[Mapping[str, Mapping[str, Any]]] = None,
        serializer: Any = None,
        ssl: Optional[Mapping[str, Any]] = None,
        sasl_oauth_token_provider: Any = None,
        **kwargs: Any,
    ):
        """
        Initializes the KafkaEgress with topic and connection settings.

        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers.
            telemetry_topic: Destination topic for telemetry stream.
            event_topic: Destination topic for event stream.
            topic_router: Optional callable to dynamically route payloads to specific topics.
            topic_serializers: Mapping of target topics to their specific `MessageSerializer` implementations.
            default_serializer: The fallback serializer to use if a topic lacks a specific mapping. Defaults to `JsonSerializer`.
            include_stream_type: Keep `stream_type` in the published message. Defaults to
                False, which is the wire format every existing deployment already reads.
                Turn it on when the same consumer code has to work against both a Kafka
                topic and Parquet files, since the Parquet writer keeps the field.
                An Avro serializer only carries the field if the registered schema
                declares it: both `ConfluentAvroSerializer` and `GlueAvroSerializer`
                encode field by field from the schema, so an undeclared `stream_type`
                is dropped without an error rather than failing the send.
                The Protobuf serializers drop it in the same way. The JSON Schema
                serializers send it, and refuse the record when the schema does not
                allow other properties.
            topic_policy: How the sink treats its topics when the run starts: a mode word
                (`create`, `drop_and_create` or `manual`) or a mapping of `mode`,
                `partitions`, `replication_factor` and `topic_configs` (a mapping of
                topic setting to value). `create`, the default, creates a missing
                topic with one partition, a replication factor of 1 and no settings,
                and leaves an existing topic as it is. `drop_and_create` deletes the
                topic and creates it again, so each run starts empty. `manual`
                creates nothing. The policy covers the event topic and the telemetry
                topic, and any topic named in `topics` that sets none of its own.
            topics: Settings per topic, a mapping of topic name to a mapping with the
                keys of `topic_policy` and `serializer`. A topic named here is
                created by the sink according to its policy, also when events choose
                it with `__topic__` or a `topic_router` routes to it.
            serializer: The serializer for every topic that `topics` or
                `topic_serializers` gives none, in place of `default_serializer`: a
                mapping `{type, ...}` that `build_serializer` builds, or a serializer
                object. The mapping's `type` is `confluent_avro`,
                `confluent_json_schema`, `confluent_protobuf`, `glue_avro`,
                `glue_json_schema` or `glue_protobuf`; the other keys are those of
                `build_serializer`.
            ssl: The keys `cafile`, `certfile`, `keyfile` and `password`, passed to
                `aiokafka.helpers.create_ssl_context`. The result is the producer's
                `ssl_context`; set `security_protocol` to `SSL` or `SASL_SSL` as well.
            sasl_oauth_token_provider: `{type: aws_msk_iam, region: <region>}` builds
                the token provider for Amazon MSK IAM, which needs the package
                `aws-msk-iam-sasl-signer-python`; a token provider object is used as
                it is. Set `security_protocol` to `SASL_SSL` and `sasl_mechanism` to
                `OAUTHBEARER` as well.
            **kwargs: Additional overrides for the AIOKafkaProducer configuration.

        Raises:
            ValueError: If a policy, serializer, `ssl` or token provider setting is
                malformed, or `serializer` and `default_serializer` are both given.
            ImportError: If `sasl_oauth_token_provider` asks for `aws_msk_iam` and
                `aws-msk-iam-sasl-signer-python` is not installed.
        """
        self.telemetry_topic = telemetry_topic
        self.event_topic = event_topic
        self.topic_router = topic_router
        self.include_stream_type = include_stream_type

        self.topic_policy = read_topic_policy(topic_policy)
        self.topics: Dict[str, Dict[str, Any]] = {}
        # Initialize serializers in a backward-compatible way
        self.topic_serializers = dict(topic_serializers or {})
        if serializer is not None and default_serializer is not None:
            raise ValueError("give serializer or default_serializer, not both")
        if isinstance(serializer, Mapping):
            serializer = build_serializer(serializer)
        self.default_serializer = serializer or default_serializer or JsonSerializer()
        for name, settings in (topics or {}).items():
            if settings is not None and not isinstance(settings, Mapping):
                raise ValueError(
                    f"topics.{name} must be a mapping of partitions, "
                    f"replication_factor, topic_configs and serializer, got "
                    f"{settings!r}"
                )
            settings = dict(settings or {})
            per_topic = settings.pop("serializer", None)
            read_topic_policy(settings, f"topics.{name}")
            self.topics[str(name)] = settings
            if per_topic is not None:
                if name in self.topic_serializers:
                    raise ValueError(
                        f"topics.{name}.serializer and topic_serializers both give "
                        f"a serializer for '{name}'"
                    )
                self.topic_serializers[str(name)] = (
                    build_serializer(per_topic)
                    if isinstance(per_topic, Mapping)
                    else per_topic
                )

        kwargs = with_security(kwargs, ssl, sasl_oauth_token_provider)

        # High-performance defaults for 100k/sec
        self.producer_config = {
            "bootstrap_servers": bootstrap_servers,
            "linger_ms": 10,  # Batch messages for 10ms before sending
            "compression_type": "lz4",  # Fast compression for high volume
            "max_batch_size": 131072,  # 128KB batch size
            **kwargs,
        }

    def _create_topics(self) -> None:
        """
        Creates, recreates or leaves alone the sink's topics, as `topic_policy` and
        `topics` say. They are the event and telemetry topics, when no `topic_router`
        chooses the topics, and the topics named in `topics`.

        Uses `KafkaAdminConnector`, which ignores a topic that already exists when
        the mode is `create`. The
        producer settings the admin client also understands, such as the security
        settings, are passed on to it. A failure is logged rather than raised,
        because the producer loop that follows retries the connection itself.
        """
        # The event and telemetry topics, unless a topic_router names the topics, and
        # every topic in `topics`; each follows the sink's policy with its own
        # settings on top
        named = [] if self.topic_router else [self.event_topic, self.telemetry_topic]
        topics = list(dict.fromkeys([*named, *self.topics]))
        policies = {
            name: read_topic_policy(
                {**self.topic_policy, **self.topics.get(name, {})}, f"topics.{name}"
            )
            for name in topics
        }
        try:
            apply_topic_policies(
                KafkaAdminConnector(
                    bootstrap_servers=self.producer_config["bootstrap_servers"],
                    **admin_settings(self.producer_config),
                ),
                policies,
            )
        except Exception as e:
            logger.warning(f"Could not create the topics {topics}: {e}")

    def _warn_topic_field(self, chosen: Any) -> None:
        """Warns once that `__topic__` held something other than a topic name."""
        if not self.__dict__.get("_topic_field_warned"):
            self._topic_field_warned = True
            logger.warning(
                "KafkaEgress ignores __topic__ %r: it must be the name of a topic, "
                "so the event goes to the event topic.",
                chosen,
            )

    def _warn_refused(self, key: Optional[bytes], topic: str, error: Exception) -> None:
        """Warns that the serializer refused one record, which is not sent."""
        logger.warning(
            "KafkaEgress skipped the record with key %s for topic %s: the "
            "serializer refused it: %s",
            None if key is None else key.decode(errors="replace"),
            topic,
            error,
        )

    def _message(
        self, data: dict
    ) -> Optional[Tuple[str, Optional[bytes], Optional[bytes]]]:
        """
        Returns the topic, the key and the value of the message for one record.

        The topic is the router's, or the telemetry topic, the record's `__topic__`
        or the event topic. The key is the `path_id` of a telemetry record and the
        `key` of any other. The value is the record encoded by its topic's
        serializer, or None, a tombstone, for a null value with the envelope off.

        Args:
            data: One record, with the reserved keys other than `__topic__`
                dropped.

        Returns:
            Optional[Tuple[str, Optional[bytes], Optional[bytes]]]: The topic, the
                key and the value, or None when the serializer refuses the record,
                which is then skipped with a warning.

        Raises:
            Exception: The serializer's error when it is temporary, as
                `is_temporary` decides, so the caller can keep the record.
            SchemaRegistryError: When the registry refuses the sink's credentials
                or access, as `refuses_access` decides.
        """
        data, chosen = take_route(data, "__topic__")
        if chosen is not None and (type(chosen) is not str or not chosen):
            self._warn_topic_field(chosen)
            chosen = None
        if self.topic_router:
            # Use externalized logic if provided
            topic = self.topic_router(data)
            stream = data.get("stream_type", "event")
        else:
            # Fallback to standard desflow routing
            stream = data["stream_type"]
            topic = (
                self.telemetry_topic
                if stream == "telemetry"
                else chosen or self.event_topic
            )

        # Extract the Kafka Key
        key = None
        if stream == "telemetry":
            key = str(data["path_id"]).encode() if "path_id" in data else None
        else:
            key = str(data["key"]).encode() if "key" in data else None

        # Build the payload without touching the record itself.
        # Every egress provider receives the same dictionary
        # objects, so removing a key here would also remove it
        # from what a Parquet or Redis sink writes.
        if not self.envelope and stream != "label":
            # The event's value alone, whatever its type
            payload = data.get("value")
        elif self.include_stream_type:
            payload = data
        else:
            payload = {k: v for k, v in data.items() if k != "stream_type"}

        # Apply configured Serializer strategy. With the
        # envelope off, a null value is a tombstone: a
        # message with the key and no value, which no
        # serializer encodes
        serializer = self.topic_serializers.get(topic, self.default_serializer)
        try:
            payload_bytes = (
                None if payload is None else serializer.serialize(topic, payload)
            )
        except Exception as error:
            if is_temporary(error) or refuses_access(error):
                raise
            # A record the serializer cannot encode is skipped, as the other sinks
            # skip a record they cannot write: a refused value, an integer too
            # large for the schema or a registry that refuses it with a 4xx status.
            # The connection to Kafka is not the cause, so it is not restarted,
            # and the rest of the batch is sent. A registry that fails for a time
            # is a temporary error, which `_messages_with_retry` waits out. A
            # registry that refuses the sink's credentials would refuse every
            # record, so `run` ends with that error.
            self._warn_refused(key, topic, error)
            return None

        return topic, key, payload_bytes

    def _messages(
        self, batch: list
    ) -> List[Tuple[str, Optional[bytes], Optional[bytes]]]:
        """
        Returns the messages of a batch, without the records the serializer refuses.

        It runs in a worker thread, because a serializer may call a schema registry,
        which waits and retries, and must not hold the loop that every other sink of
        the run shares.

        Args:
            batch: The records, with the reserved keys other than `__topic__` dropped.

        Returns:
            List[Tuple[str, Optional[bytes], Optional[bytes]]]: The topic, the key
                and the value of each record that was encoded.

        Raises:
            Exception: A serializer's temporary error, as `is_temporary` decides, or
                a registry's refusal of the sink's credentials or access.
        """
        messages = (self._message(data) for data in batch)
        return [message for message in messages if message is not None]

    async def _messages_with_retry(
        self, batch: list
    ) -> List[Tuple[str, Optional[bytes], Optional[bytes]]]:
        """
        Returns the messages of a batch, waiting while the schema registry is down.

        A temporary registry error (a 5xx answer, a throttled call, a timeout or a
        lost connection) keeps the batch: nothing of it is sent yet, so it is encoded again after a
        longer wait, and the Kafka producer is not restarted.

        Args:
            batch: The records, with the reserved keys other than `__topic__` dropped.

        Returns:
            List[Tuple[str, Optional[bytes], Optional[bytes]]]: The topic, the key
                and the value of each record that was encoded.

        Raises:
            Exception: A serializer's error that is not temporary and that
                `_message` does not skip, such as a record without `stream_type` or
                a registry that refuses the sink's credentials or access.
        """
        delay = FIRST_DELAY
        while True:
            try:
                return await asyncio.to_thread(self._messages, batch)
            except Exception as error:
                if not is_temporary(error):
                    raise
                logger.warning(
                    "KafkaEgress could not reach the schema registry: %s. "
                    "Trying the batch of %d records again in %ss.",
                    error,
                    len(batch),
                    delay,
                )
                await asyncio.sleep(delay)
                delay = next_delay(delay)

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        The main execution loop that consumes the egress queue and publishes to Kafka.

        This method maintains a persistent connection to Kafka. If the connection
        is lost, it implements an exponential backoff retry strategy. It polls
        the internal `egress_queue` for batches of data, determines the target
        topic based on the 'stream_type', and performs asynchronous sends using
        the configured serializers, which run in a worker thread. A record that its
        serializer refuses, with a `SerializationError`, a value error or a
        `SchemaRegistryError` with a 4xx status other than a throttled call, is
        skipped with a warning that names its key and the reason, and the rest of
        the batch is sent. A registry that fails for a time (a 5xx status, a
        throttled call, a timeout or a lost connection) keeps the batch, which is
        tried again after a longer wait. A registry that refuses the sink's
        credentials or access (a 401 or 403 status, or the Glue error
        `AccessDeniedException`) ends the task with that error, and nothing of that
        batch is sent.

        Args:
            egress_queue: A thread-safe queue containing lists of dictionaries
                or Pydantic models generated by the EgressMixIn.

        Raises:
            SchemaRegistryError: If the registry refuses the sink's credentials or
                access.

        Note:
            The loop exits gracefully upon receiving an `asyncio.CancelledError`,
            ensuring the Kafka producer is stopped correctly.
        """
        if self.topic_router is None or self.topics:
            await asyncio.to_thread(self._create_topics)

        backoff = 1.0
        max_backoff = 60.0

        while True:
            try:
                producer = AIOKafkaProducer(**self.producer_config)
                try:
                    # Started inside the try, so a failed start also closes the
                    # producer: aiokafka leaves it open otherwise.
                    await producer.start()
                    logger.info("Kafka Egress producer connected successfully.")
                    backoff = 1.0  # Reset backoff on successful connection

                    while True:
                        try:
                            # 'batch' is a list of dictionaries from the EgressMixIn
                            batch = egress_queue.get_nowait()
                            with self._writing():
                                # Kafka routes by __topic__, and writes no other
                                # reserved key
                                batch = drop_reserved_keys(
                                    batch, self, keep=("__topic__",)
                                )

                                messages = await self._messages_with_retry(batch)
                                for topic, key, payload_bytes in messages:
                                    await producer.send(
                                        topic, value=payload_bytes, key=key
                                    )

                        except queue.Empty:
                            # Yield to loop if queue is empty
                            await asyncio.sleep(0.001)
                finally:
                    await producer.stop()

            except asyncio.CancelledError:
                logger.info("Kafka Egress shut down requested. Exiting loop.")
                break
            except Exception as e:
                if refuses_access(e):
                    raise
                logger.error(
                    f"Kafka Egress connection failed: {e}. Retrying in {backoff} seconds..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
