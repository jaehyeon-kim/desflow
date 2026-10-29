"""Errors of the schema registry code, shared by every registry and format."""


class SchemaRegistryError(RuntimeError):
    """A schema registry answered a request with an error status.

    Attributes:
        http_status (int): The HTTP status of the response.
        error_code (int): The registry's own error code, or -1 when the body of the
            response held none.
        message (str): The registry's message, or the raw body when it was not JSON.
    """

    def __init__(self, http_status: int, error_code: int, message: str):
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
