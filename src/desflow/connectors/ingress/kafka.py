import asyncio
import json
import logging
import queue
import ssl
import threading
from collections.abc import Mapping
from functools import partial
from typing import Any, Callable, Dict, Optional, Protocol, Tuple, Union

from aiokafka import AIOKafkaConsumer

from desflow.connectors._schema_registry.unset import UNSET
from desflow.connectors._schema_registry.errors import (  # noqa: F401
    SchemaRegistryError,
    SerializationError,
)
from desflow.connectors.ingress.base import BaseIngress, parse_change

logger = logging.getLogger(__name__)

# ==========================================
# Pluggable Deserializer Interfaces
# ==========================================


class MessageDeserializer(Protocol):
    """
    Protocol defining the interface for all Kafka ingress deserializers.

    Any custom deserializer passed to `KafkaIngress` must implement the
    `deserialize` method according to this signature.
    """

    def deserialize(self, topic: str, payload: bytes) -> Any:
        """
        Deserializes a raw byte payload from Kafka into a Python object.

        Args:
            topic (str): The Kafka topic from which the message was consumed.
            payload (bytes): The raw binary payload of the Kafka message.

        Returns:
            Any: The deserialized data, typically a dictionary containing a 'path' and 'value'.
        """
        ...


class JsonDeserializer:
    """
    Default fallback deserializer providing backward compatibility via standard JSON.

    This deserializer assumes the incoming payload is a UTF-8 encoded JSON string.
    """

    def deserialize(self, topic: str, payload: bytes) -> Any:
        """
        Deserializes a JSON byte string.

        Args:
            topic (str): The Kafka topic from which the message was consumed (unused).
            payload (bytes): The UTF-8 encoded JSON byte string.

        Returns:
            Any: The parsed JSON data as a Python dictionary.
        """
        return json.loads(payload.decode("utf-8"))


class ConfluentAvroDeserializer:
    """
    Lazy-loaded deserializer for Confluent Schema Registry using fastavro and httpx.

    This class converts Avro-encoded byte strings back into Python dictionaries.
    It fetches the writer schema from the registry using the schema id (or GUID)
    in the message's prefix, once for each id, so no `schema_str` is required
    during initialization.
    """

    def __init__(
        self,
        registry_url: str,
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
        Initializes the Confluent Avro deserializer. It makes no request to the registry.

        `UNSET` as a default, in the signature or the Default column of the API page,
        means the argument was not given; each argument's description says its real
        default.

        Args:
            registry_url (str): The base URL of the Confluent Schema Registry.
                Several URLs separated by commas are tried in order.
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
            from desflow.connectors._schema_registry.rules import refuse_rules
            from desflow.connectors._schema_registry.wire import unframe_confluent
        except ImportError:
            raise ImportError(
                "The 'fastavro' and 'httpx' packages are required to use "
                "ConfluentAvroDeserializer. "
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
        self._refuse_rules = refuse_rules
        self._unframe = unframe_confluent
        # Parsed writer schemas by schema id or GUID. The client caches the
        # registry's responses; the parsed schema belongs to the Avro codec.
        self._parsed: Dict[Any, Any] = {}
        self._lock = threading.Lock()

    def deserialize(self, topic: str, payload: bytes) -> Any:
        """
        Deserializes an Avro-encoded byte string.

        The first message with a schema id makes a request to the registry for the
        writer schema.

        Args:
            topic (str): The Kafka topic from which the message was consumed.
            payload (bytes): The Avro-encoded binary payload containing the schema ID.

        Returns:
            Any: The deserialized data as a Python dictionary.

        Raises:
            SerializationError: If the payload has no valid prefix, the writer
                schema is not Avro, or the schema has Data Contract read rules.
            SchemaRegistryError: If the registry has no schema for the id.
            httpx.TransportError: If no registry URL could be reached.
        """
        schema_id, data = self._unframe(payload)
        return self._avro.decode(self._writer_schema(schema_id), data)

    def _writer_schema(self, schema_id: Any) -> Any:
        """Returns the parsed writer schema of an id or GUID, fetching it once."""
        with self._lock:
            if schema_id not in self._parsed:
                if isinstance(schema_id, int):
                    response = self._client.schema_by_id(schema_id)
                else:
                    response = self._client.schema_by_guid(str(schema_id))
                if response["schemaType"] != "AVRO":
                    raise SerializationError(
                        f"The schema is of type {response['schemaType']}, "
                        "and ConfluentAvroDeserializer reads AVRO only"
                    )
                self._refuse_rules(response)
                self._parsed[schema_id] = self._avro.parse_writer_schema(
                    response["schema"],
                    response.get("references") or [],
                    self._client.subject_version,
                )
            return self._parsed[schema_id]


class GlueAvroDeserializer:
    """
    Lazy-loaded deserializer for AWS Glue Schema Registry.

    This class converts Avro-encoded byte strings back into Python dictionaries,
    integrating directly with AWS Glue Schema Registry. It resolves schemas dynamically
    based on the metadata embedded in the AWS Glue message payload. A schema version
    of the JSON data format is read as JSON and validated against its schema.
    """

    def __init__(self, registry_name: str, **boto3_kwargs: Any):
        """
        Initializes the AWS Glue Avro deserializer.

        The constructor makes no call to Glue. The first message with a schema version
        id fetches that version.

        Args:
            registry_name (str): The name of the AWS Glue Schema Registry.
            **boto3_kwargs: Additional arguments passed directly to the `boto3.client`
                initialization (e.g., `region_name`, `aws_access_key_id`).

        Raises:
            ImportError: If the 'boto3', 'fastavro' or 'fastjsonschema' package is not
                installed.
        """
        try:
            import boto3  # noqa: F401
            import fastavro  # noqa: F401
            import fastjsonschema  # noqa: F401

            from desflow.connectors._schema_registry import avro, glue_json
            from desflow.connectors._schema_registry.glue_client import (
                GlueRegistryClient,
                no_references,
            )
            from desflow.connectors._schema_registry.wire import unframe_glue
        except ImportError:
            raise ImportError(
                "The 'boto3', 'fastavro' and 'fastjsonschema' packages are required. "
                "Install them using: pip install desflow[glue]"
            )

        self._client = GlueRegistryClient(registry_name, boto3_kwargs)
        self._avro = avro
        self._glue_json = glue_json
        self._unframe = unframe_glue
        self._no_references = no_references
        # A reader for each schema version id, built from the version once.
        self._readers: Dict[Any, Callable[[bytes], Any]] = {}
        self._lock = threading.Lock()

    def deserialize(self, topic: str, payload: bytes) -> Any:
        """
        Deserializes an Avro or JSON byte string using AWS Glue.

        Args:
            topic (str): The Kafka topic from which the message was consumed.
            payload (bytes): The binary payload with the Glue prefix.

        Returns:
            Any: The record, a dict for a record or object schema.

        Raises:
            SerializationError: If the payload has no valid Glue prefix, the schema
                version is of a data format other than AVRO or JSON, the JSON Schema
                cannot be compiled, or the data is not JSON, does not match its
                schema or cannot be checked against it.
            SchemaRegistryError: If Glue has no available schema version for the id.
        """
        version_id, data = self._unframe(payload)
        return self._reader(version_id)(data)

    def _reader(self, version_id: Any) -> Callable[[bytes], Any]:
        """Returns the function that reads the data of a schema version."""
        with self._lock:
            if version_id not in self._readers:
                version = self._client.schema_version(version_id)
                data_format = version["DataFormat"]
                definition = version["SchemaDefinition"]
                if data_format == "AVRO":
                    parsed = self._avro.parse_writer_schema(
                        definition, (), self._no_references
                    )
                    self._readers[version_id] = partial(
                        self._avro.decode_standard, parsed
                    )
                elif data_format == "JSON":
                    validate = self._glue_json.validator(definition)
                    self._readers[version_id] = partial(
                        self._glue_json.decode, validate
                    )
                else:
                    raise SerializationError(
                        f"The schema version is of data format {data_format}, "
                        "and GlueAvroDeserializer reads AVRO and JSON only"
                    )
            return self._readers[version_id]


# ==========================================
# Core Ingress Class
# ==========================================


class KafkaIngress(BaseIngress):
    """
    Resilient Kafka consumer for dynamic simulation configuration updates.

    This connector subscribes to a specified Kafka topic and decodes incoming
    messages into state updates for the SimulationRegistry. It supports pluggable
    deserialization (Avro/JSON) while remaining 100% backward compatible.

    The expected resulting dictionary format must contain a 'path' string
    and a non-null 'value' of any serializable type. A message that cannot be
    decoded or does not have both is logged as a warning and skipped.

    Attributes:
        topic (str): The Kafka topic name used for configuration signals.
        bootstrap_servers (str): Comma-separated string of Kafka broker addresses.
        topic_deserializers (Optional[Dict[str, MessageDeserializer]]): Mapping of topics to specific deserializers.
        default_deserializer (Optional[MessageDeserializer]): Fallback deserializer. Defaults to `JsonDeserializer`.
        kwargs (dict): Additional configuration parameters for the AIOKafkaConsumer.
    """

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str,
        topic_deserializers: Optional[Dict[str, MessageDeserializer]] = None,
        default_deserializer: Optional[MessageDeserializer] = None,
        **kwargs: Any,
    ):
        """
        Initializes the KafkaIngress with connection and subscription details.

        Args:
            topic: The Kafka topic to subscribe to.
            bootstrap_servers: Kafka broker address or list of addresses.
            topic_deserializers: Mapping of topics to their specific deserializers.
            default_deserializer: Fallback deserializer if the topic isn't explicitly mapped.
            **kwargs: Arbitrary keyword arguments passed to the underlying AIOKafkaConsumer.
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

        self.topic_deserializers = topic_deserializers or {}
        self.default_deserializer = default_deserializer or JsonDeserializer()

        self.kwargs = kwargs

    async def run(self, ingress_queue: queue.Queue) -> None:
        """Consumes parameter changes from Kafka and puts each on the ingress queue.

        Each message is read as JSON with `path` and `value`; a message that cannot be
        read is logged and skipped.

        Args:
            ingress_queue: The queue the environment applies changes from, as
                `(path, value)` pairs.
        """
        backoff = 1.0
        while True:
            try:
                consumer = AIOKafkaConsumer(
                    self.topic, bootstrap_servers=self.bootstrap_servers, **self.kwargs
                )
                try:
                    # Started inside the try, so a failed start also closes the
                    # consumer: aiokafka leaves it open otherwise.
                    await consumer.start()
                    logger.info(f"Connected to Kafka Ingress topic: {self.topic}")
                    backoff = 1.0

                    async for msg in consumer:
                        try:
                            # Apply configured Deserializer strategy
                            deserializer = self.topic_deserializers.get(
                                msg.topic, self.default_deserializer
                            )

                            # Decode binary payload -> Python Dict
                            data = deserializer.deserialize(msg.topic, msg.value)

                            ingress_queue.put(parse_change(data))

                        except Exception as e:
                            logger.warning(f"Skipped a Kafka ingress message: {e}")
                finally:
                    await consumer.stop()

            except asyncio.CancelledError:
                logger.info("Kafka Ingress shut down requested. Exiting loop.")
                break
            except Exception as e:
                logger.error(f"Kafka Ingress error: {e}. Retrying in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
