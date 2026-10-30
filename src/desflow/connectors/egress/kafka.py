import asyncio
import inspect
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
    Optional,
    Protocol,
    Tuple,
    Union,
)

import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient

from desflow.connectors.admin.kafka import KafkaAdminConnector
from desflow.connectors._schema_registry.unset import UNSET
from desflow.connectors._schema_registry.errors import (  # noqa: F401
    SchemaRegistryError,
    SerializationError,
)
from desflow.connectors.egress.base import (
    BaseEgress,
    drop_reserved_keys,
    extract_dict,
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
        **registry_kwargs: Any,
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
            **registry_kwargs: Deprecated confluent-kafka keys, such as
                `{'basic.auth.user.info': 'user:pass'}`, which desflow accepts with a
                `FutureWarning` and removes in 0.30.0. Use the named arguments.

        Raises:
            ImportError: If the 'fastavro' or 'httpx' package is not installed.
            ValueError: If a deprecated key is not accepted, or is given together with
                its named argument.
        """
        try:
            import fastavro  # noqa: F401
            import httpx  # noqa: F401

            from desflow.connectors._schema_registry import avro
            from desflow.connectors._schema_registry.confluent_client import (
                ConfluentRegistryClient,
                resolve_legacy_options,
            )
            from desflow.connectors._schema_registry.wire import frame_confluent
        except ImportError:
            raise ImportError(
                "The 'fastavro' and 'httpx' packages are required to use "
                "ConfluentAvroSerializer. "
                "Install them using: pip install desflow[confluent]"
            )

        options = resolve_legacy_options(
            registry_kwargs,
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
                resolve_legacy_options,
            )
            from desflow.connectors._schema_registry.wire import frame_confluent
        except ImportError:
            raise ImportError(
                "The 'jsonschema' and 'httpx' packages are required to use "
                "ConfluentJsonSchemaSerializer. "
                "Install them using: pip install desflow[confluent]"
            )

        options = resolve_legacy_options(
            {},
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
                resolve_legacy_options,
            )
            from desflow.connectors._schema_registry.wire import frame_confluent
        except ImportError:
            raise ImportError(
                "The 'protobuf' and 'httpx' packages are required to use "
                "ConfluentProtobufSerializer. "
                "Install them using: pip install desflow[confluent]"
            )

        options = resolve_legacy_options(
            {},
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
            **kwargs: Additional overrides for the AIOKafkaProducer configuration.
        """
        self.telemetry_topic = telemetry_topic
        self.event_topic = event_topic
        self.topic_router = topic_router
        self.include_stream_type = include_stream_type

        # Initialize serializers in a backward-compatible way
        self.topic_serializers = topic_serializers or {}
        self.default_serializer = default_serializer or JsonSerializer()

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
        Creates the event and telemetry topics if they do not exist yet.

        Uses `KafkaAdminConnector`, which ignores a topic that already exists. The
        producer settings the admin client also understands, such as the security
        settings, are passed on to it. A failure is logged rather than raised,
        because the producer loop that follows retries the connection itself.
        """
        # bootstrap_servers and client_id are set by KafkaAdminConnector itself.
        accepted = inspect.signature(AIOKafkaAdminClient.__init__).parameters
        admin_settings = {
            key: value
            for key, value in self.producer_config.items()
            if key in accepted and key not in ("bootstrap_servers", "client_id")
        }
        topics = list(dict.fromkeys([self.event_topic, self.telemetry_topic]))
        try:
            KafkaAdminConnector(
                bootstrap_servers=self.producer_config["bootstrap_servers"],
                **admin_settings,
            ).create_topics(topics_config=[{"name": name} for name in topics])
        except Exception as e:
            logger.warning(f"Could not create the topics {topics}: {e}")

    async def run(self, egress_queue: queue.Queue) -> None:
        """
        The main execution loop that consumes the egress queue and publishes to Kafka.

        This method maintains a persistent connection to Kafka. If the connection
        is lost, it implements an exponential backoff retry strategy. It polls
        the internal `egress_queue` for batches of data, determines the target
        topic based on the 'stream_type', and performs asynchronous sends using
        the configured serializers.

        Args:
            egress_queue: A thread-safe queue containing lists of dictionaries
                or Pydantic models generated by the EgressMixIn.

        Note:
            The loop exits gracefully upon receiving an `asyncio.CancelledError`,
            ensuring the Kafka producer is stopped correctly.
        """
        if self.topic_router is None:
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
                            # Kafka routes by no reserved key, so it writes none
                            batch = drop_reserved_keys(batch, self)

                            for data in batch:
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
                                        else self.event_topic
                                    )

                                # Extract the Kafka Key
                                key = None
                                if stream == "telemetry":
                                    key = (
                                        str(data["path_id"]).encode()
                                        if "path_id" in data
                                        else None
                                    )
                                else:
                                    key = (
                                        str(data["key"]).encode()
                                        if "key" in data
                                        else None
                                    )

                                # Build the payload without touching the record itself.
                                # Every egress provider receives the same dictionary
                                # objects, so removing a key here would also remove it
                                # from what a Parquet or Redis sink writes.
                                if self.include_stream_type:
                                    payload = data
                                else:
                                    payload = {
                                        k: v
                                        for k, v in data.items()
                                        if k != "stream_type"
                                    }

                                # Apply configured Serializer strategy
                                serializer = self.topic_serializers.get(
                                    topic, self.default_serializer
                                )
                                payload_bytes = serializer.serialize(topic, payload)

                                await producer.send(topic, value=payload_bytes, key=key)

                        except queue.Empty:
                            # Yield to loop if queue is empty
                            await asyncio.sleep(0.001)
                finally:
                    await producer.stop()

            except asyncio.CancelledError:
                logger.info("Kafka Egress shut down requested. Exiting loop.")
                break
            except Exception as e:
                logger.error(
                    f"Kafka Egress connection failed: {e}. Retrying in {backoff} seconds..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
