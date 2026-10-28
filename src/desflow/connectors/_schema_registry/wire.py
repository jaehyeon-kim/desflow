"""The Confluent wire format prefix: a version byte, then a schema id or a GUID."""

import struct
import uuid

from desflow.connectors._schema_registry.errors import SerializationError

_MAGIC_ID = 0
_MAGIC_GUID = 1
_ID_PREFIX = 5
_GUID_PREFIX = 17


def frame_confluent(schema_id: int, data: bytes) -> bytes:
    """Puts the Confluent prefix before encoded data.

    Args:
        schema_id (int): The id the registry gave the schema.
        data (bytes): The encoded record.

    Returns:
        bytes: Magic byte 0, the id as 4 big-endian bytes, then the data.
    """
    return struct.pack(">bI", _MAGIC_ID, schema_id) + data


def unframe_confluent(payload: bytes) -> tuple[int | uuid.UUID, memoryview]:
    """Splits a Confluent payload into its schema id and the encoded data.

    The data is returned unread, so each format decides how to decode it.

    Args:
        payload (bytes): A message value that starts with the Confluent prefix.

    Returns:
        tuple[int | uuid.UUID, memoryview]: The schema id (magic byte 0) or the
            schema GUID (magic byte 1), and the bytes after the prefix.

    Raises:
        SerializationError: If the payload is shorter than the prefix, or its magic
            byte is neither 0 nor 1.
    """
    if len(payload) <= _ID_PREFIX:
        raise SerializationError(
            "Expecting data framing of length 6 bytes or more "
            f"but total data size is {len(payload)} bytes"
        )
    view = memoryview(payload)
    magic = payload[0]
    if magic == _MAGIC_ID:
        (schema_id,) = struct.unpack(">I", view[1:_ID_PREFIX])
        return schema_id, view[_ID_PREFIX:]
    if magic == _MAGIC_GUID:
        if len(payload) < _GUID_PREFIX:
            raise SerializationError(
                "Expecting data framing of length 17 bytes or more "
                f"but total data size is {len(payload)} bytes"
            )
        return uuid.UUID(bytes=bytes(view[1:_GUID_PREFIX])), view[_GUID_PREFIX:]
    raise SerializationError(f"Invalid magic byte {magic}, expected 0 or 1")
