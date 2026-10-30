"""The wire format prefixes: Confluent's schema id or GUID, and AWS Glue's version UUID."""

import struct
import uuid
import zlib

from desflow.connectors._schema_registry.errors import SerializationError

_MAGIC_ID = 0
_MAGIC_GUID = 1
_ID_PREFIX = 5
_GUID_PREFIX = 17
_GLUE_VERSION = 3
_GLUE_UNCOMPRESSED = 0
_GLUE_ZLIB = 5
_GLUE_PREFIX = 18


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


def frame_glue(version_id: uuid.UUID, data: bytes) -> bytes:
    """Puts the AWS Glue prefix before encoded data, uncompressed.

    Args:
        version_id (uuid.UUID): The id Glue gave the schema version.
        data (bytes): The encoded record.

    Returns:
        bytes: Header version 3, compression byte 0, the 16 bytes of the version id,
            then the data.
    """
    return bytes((_GLUE_VERSION, _GLUE_UNCOMPRESSED)) + version_id.bytes + data


def unframe_glue(payload: bytes) -> tuple[uuid.UUID, bytes]:
    """Splits an AWS Glue payload into its schema version id and the encoded data.

    Args:
        payload (bytes): A message value that starts with the Glue prefix.

    Returns:
        tuple[uuid.UUID, bytes]: The schema version id, and the data after the
            prefix, decompressed when the compression byte is 5.

    Raises:
        SerializationError: If the payload is shorter than the 18 byte prefix, its
            header version is not 3, its compression byte is neither 0 nor 5, or
            compressed data cannot be decompressed.
    """
    if len(payload) < _GLUE_PREFIX:
        raise SerializationError(
            "Expecting data framing of length 18 bytes or more "
            f"but total data size is {len(payload)} bytes"
        )
    if payload[0] != _GLUE_VERSION:
        raise SerializationError(
            f"Invalid header version byte {payload[0]}, expected {_GLUE_VERSION}"
        )
    compression = payload[1]
    data = bytes(payload[_GLUE_PREFIX:])
    if compression == _GLUE_ZLIB:
        try:
            data = zlib.decompress(data)
        except zlib.error as error:
            raise SerializationError(f"Cannot decompress the data: {error}") from error
    elif compression != _GLUE_UNCOMPRESSED:
        raise SerializationError(
            f"Invalid compression byte {compression}, expected 0 or 5"
        )
    return uuid.UUID(bytes=bytes(payload[2:_GLUE_PREFIX])), data
