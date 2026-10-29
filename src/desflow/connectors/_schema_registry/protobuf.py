"""The Protobuf codec: message indexes, schema strings, dependencies and record bytes.

Confluent writes the path of the message in the file as zigzag varints
(confluent-kafka 2.16.0, `schema_registry/common/serde.py`, `SchemaId`). AWS Glue
writes one unsigned varint, the position of the message among all messages of the
file sorted by full name (AWS Glue Schema Registry library v2.0.1, `MessageIndexFinder`
and `ProtobufWireFormatEncoder`).
"""

import base64
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from google.protobuf import json_format
from google.protobuf.descriptor import Descriptor, FileDescriptor
from google.protobuf.message import DecodeError, Message

from desflow.connectors._schema_registry.errors import SerializationError

# confluent-kafka refuses an index array longer than this.
_MAX_INDEX_PATH = 100000
# A 64 bit value needs at most 10 bytes as a varint.
_MAX_VARINT_BYTES = 10
# Files that every Protobuf reader has, which confluent-kafka does not register
# (`schema_registry/common/protobuf.py`, `_is_builtin`).
_BUILTIN_PREFIXES = ("confluent/", "google/protobuf/", "google/type/")


class _DependencyClient(Protocol):
    """The two registry calls that dependency registration makes."""

    def register(
        self,
        subject: str,
        schema_str: str,
        schema_type: str,
        references: Sequence[Mapping[str, Any]] = (),
    ) -> int: ...

    def lookup(
        self,
        subject: str,
        schema_str: str,
        schema_type: str,
        references: Sequence[Mapping[str, Any]] = (),
    ) -> int: ...


def _read_varint(buf: bytes | memoryview) -> tuple[int, int]:
    """Reads an unsigned varint and returns it with the number of bytes it took."""
    value = 0
    for position in range(min(len(buf), _MAX_VARINT_BYTES)):
        byte = buf[position]
        value |= (byte & 0x7F) << (7 * position)
        if not byte & 0x80:
            return value, position + 1
    raise SerializationError("The message index is cut short or too long")


def _write_varint(value: int) -> bytes:
    """Writes an unsigned varint."""
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _zigzag_decode(value: int) -> int:
    return (value >> 1) ^ -(value & 1)


def confluent_indexes(descriptor: Descriptor) -> bytes:
    """Writes the Confluent message index of a message type.

    The index is the path to the message from the top of its file: its position
    among the file's messages, or for a nested message the position of each message
    on the way down. The path is written as its length, then each position, all as
    zigzag varints. The path `[0]` is the single byte 00.

    Args:
        descriptor (Descriptor): The descriptor of the message type.

    Returns:
        bytes: The index.
    """
    path: list[int] = []
    current = descriptor
    while current.containing_type is not None:
        parent = current.containing_type
        path.insert(0, list(parent.nested_types).index(current))
        current = parent
    path.insert(0, list(descriptor.file.message_types_by_name.values()).index(current))
    if path == [0]:
        return b"\x00"
    return b"".join(_write_varint(value << 1) for value in (len(path), *path))


def read_confluent_indexes(buf: bytes | memoryview) -> tuple[list[int], memoryview]:
    """Reads the Confluent message index at the start of a message.

    Args:
        buf (bytes | memoryview): The bytes after the Confluent prefix.

    Returns:
        tuple[list[int], memoryview]: The path of the message, and the bytes after
            the index.

    Raises:
        SerializationError: If the index is cut short or claims an impossible length.
    """
    view = memoryview(buf)
    raw, used = _read_varint(view)
    size = _zigzag_decode(raw)
    if size < 0 or size > _MAX_INDEX_PATH:
        raise SerializationError("Invalid message index length")
    if size == 0:
        return [0], view[used:]
    path = []
    for _ in range(size):
        raw, taken = _read_varint(view[used:])
        used += taken
        path.append(_zigzag_decode(raw))
    return path, view[used:]


def _all_messages(descriptor: Descriptor) -> list[Descriptor]:
    """Returns a message and every message nested in it, at any depth."""
    found = [descriptor]
    for nested in descriptor.nested_types:
        found.extend(_all_messages(nested))
    return found


def glue_index(descriptor: Descriptor) -> bytes:
    """Writes the Glue message index of a message type.

    The index is the position of the message among all the messages of its file,
    nested ones included, sorted by full name. It is written as one unsigned varint.

    Args:
        descriptor (Descriptor): The descriptor of the message type.

    Returns:
        bytes: The index.
    """
    messages = [
        message
        for top in descriptor.file.message_types_by_name.values()
        for message in _all_messages(top)
    ]
    names = sorted(message.full_name for message in messages)
    return _write_varint(names.index(descriptor.full_name))


def read_glue_index(buf: bytes | memoryview) -> tuple[int, memoryview]:
    """Reads the Glue message index at the start of a message.

    Args:
        buf (bytes | memoryview): The bytes after the Glue prefix.

    Returns:
        tuple[int, memoryview]: The index, and the bytes after it.

    Raises:
        SerializationError: If the index is cut short.
    """
    view = memoryview(buf)
    index, used = _read_varint(view)
    return index, view[used:]


def encode(message_type: type[Message], data: Any) -> bytes:
    """Writes a record as a Protobuf message, with no index.

    Args:
        message_type (type[Message]): The generated message class.
        data (Any): An instance of `message_type`, written as it is, or a dict. A
            dict is parsed with the field names of the schema, and a key the schema
            does not have is dropped, as the Avro classes drop it.

    Returns:
        bytes: The serialized message.

    Raises:
        SerializationError: If the record is neither of those, or a value does not
            fit its field.
    """
    if isinstance(data, message_type):
        return data.SerializeToString()
    if not isinstance(data, dict):
        raise SerializationError(
            f"Expected a {message_type.__name__} or a dict, not {type(data).__name__}"
        )
    try:
        message = json_format.ParseDict(
            data, message_type(), ignore_unknown_fields=True
        )
    except json_format.ParseError as error:
        raise SerializationError(
            f"The record does not fit {message_type.__name__}: {error}"
        ) from error
    return message.SerializeToString()


def decode(message_type: type[Message], data: bytes | memoryview) -> dict[str, Any]:
    """Reads a Protobuf message into a dict.

    Field names are those of the schema, and a field with no value is present with
    its default. A 64 bit integer is a string on every protobuf release: protobuf's
    JSON mapping writes it so, and 5.27.0, the oldest release desflow supports, has no
    option to unquote it.

    Args:
        message_type (type[Message]): The generated message class.
        data (bytes | memoryview): The serialized message, with no index.

    Returns:
        dict[str, Any]: The record.

    Raises:
        SerializationError: If the bytes are not a message of that type.
    """
    message = message_type()
    try:
        message.ParseFromString(bytes(data))
    except DecodeError as error:
        raise SerializationError(
            f"The data is not a {message_type.__name__} message: {error}"
        ) from error
    return json_format.MessageToDict(
        message,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )


def file_descriptor_b64(file_descriptor: FileDescriptor) -> str:
    """Writes a file descriptor as Confluent stores a Protobuf schema.

    Args:
        file_descriptor (FileDescriptor): The file of the message type.

    Returns:
        str: The serialized descriptor, base64 encoded.
    """
    return base64.standard_b64encode(file_descriptor.serialized_pb).decode("ascii")


def register_dependencies(
    file_descriptor: FileDescriptor, client: _DependencyClient
) -> list[dict[str, Any]]:
    """Registers the files a file imports, and returns them as schema references.

    As confluent-kafka does with its defaults: each imported file is handled first,
    with its own imports; it is registered under its file name as the subject, then
    looked up for its version. Files that every reader has are skipped.

    Args:
        file_descriptor (FileDescriptor): The file of the message type.
        client (_DependencyClient): The registry client, with `register` and `lookup`.

    Returns:
        list[dict[str, Any]]: One reference for each import, with `name`, `subject`
            and `version`.

    Raises:
        SchemaRegistryError: If the registry refuses a file.
    """
    references = []
    for dependency in file_descriptor.dependencies:
        if dependency.name.startswith(_BUILTIN_PREFIXES):
            continue
        inner = register_dependencies(dependency, client)
        schema_str = file_descriptor_b64(dependency)
        client.register(dependency.name, schema_str, "PROTOBUF", inner)
        version = client.lookup(dependency.name, schema_str, "PROTOBUF", inner)
        references.append(
            {"name": dependency.name, "subject": dependency.name, "version": version}
        )
    return references
