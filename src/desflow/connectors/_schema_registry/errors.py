"""Errors of the schema registry code, shared by every registry and format."""


class SchemaRegistryError(RuntimeError):
    """A schema registry call failed, or its answer was an error or an unusable version.

    Attributes:
        http_status (int): The HTTP status of the response, or -1 when the call
            failed before the registry answered (credentials, endpoint) or the
            answer was a schema version that cannot be used.
        error_code (int | str): The registry's own error code: an int from a
            Confluent registry, or the name of the error from AWS Glue, such as
            `EntityNotFoundException`. It is -1 when the registry sent none.
        message (str): The registry's message, the raw body when it was not JSON, or
            desflow's description of the failure.
    """

    def __init__(self, http_status: int, error_code: int | str, message: str):
        self.http_status = http_status
        self.error_code = error_code
        self.message = message
        super().__init__(
            f"Schema registry error {http_status} ({error_code}): {message}"
        )


class SerializationError(ValueError):
    """A payload or a schema cannot be written or read.

    Raised for data too short to hold the registry's framing, a magic byte that is
    not known, a schema of a format the class does not handle, and a schema with
    rules that desflow does not run.
    """
