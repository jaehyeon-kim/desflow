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


def is_temporary(error: BaseException) -> bool:
    """Returns True when `error` is a registry failure that a later call may not meet.

    A temporary failure is a 5xx answer, a timeout or a lost connection, to a
    Confluent registry (an `httpx.TransportError`) or to AWS Glue (a botocore
    connection error, which `GlueSchemaRegistryClient` wraps in a
    `SchemaRegistryError` with the status -1). A throttled call is temporary too: a
    408 or 429 answer, or the Glue error `ThrottlingException`. Any other 4xx
    answer, a payload the schema cannot encode and any other error are permanent:
    the same record fails again.

    Args:
        error: The exception a serializer raised.

    Returns:
        bool: Whether waiting and trying the same record again can succeed.
    """
    transport = _transport_errors()
    if isinstance(error, transport):
        return True
    if not isinstance(error, SchemaRegistryError):
        return False
    if 500 <= error.http_status <= 599 or error.http_status in _THROTTLED_STATUS:
        return True
    if error.error_code == "ThrottlingException":
        return True
    return isinstance(error.__cause__, transport)


def refuses_access(error: BaseException) -> bool:
    """Returns True when `error` says the registry refused the credentials or access.

    A Confluent registry answers 401 when it does not accept the credentials and 403
    when they do not give access. AWS Glue answers 401 or 403 for a key it does not
    know, an expired token or a bad signature, and `AccessDeniedException` with 400
    for an IAM policy that does not allow the call. The same failure meets every
    record, and no wait fixes it, so a connector ends on it rather than skipping
    each record.

    Args:
        error: The exception a serializer or deserializer raised.

    Returns:
        bool: Whether the registry refused the client's credentials or access.
    """
    if not isinstance(error, SchemaRegistryError):
        return False
    return (
        error.http_status in _REFUSED_STATUS or error.error_code in _GLUE_REFUSED_CODES
    )


# Unauthorized and forbidden.
_REFUSED_STATUS = (401, 403)

# The credential and access errors of Glue's Common Error Types page. Glue's pages
# of the schema operations give AccessDeniedException the status 400, which is why
# the code is matched as well as the status.
_GLUE_REFUSED_CODES = (
    "AccessDeniedException",
    "ExpiredTokenException",
    "IncompleteSignature",
    "NotAuthorized",
    "UnrecognizedClientException",
)


# The answers a registry gives when it asks the caller to wait: a request timeout and
# too many requests. The Confluent client retries them a few times first.
_THROTTLED_STATUS = (408, 429)


def _transport_errors() -> tuple[type[BaseException], ...]:
    """Returns the connection and timeout errors of the installed HTTP libraries."""
    classes: list[type[BaseException]] = []
    try:
        import httpx

        classes.append(httpx.TransportError)
    except ImportError:
        pass
    try:
        from botocore.exceptions import ConnectionError as BotocoreConnectionError
        from botocore.exceptions import HTTPClientError

        classes += [BotocoreConnectionError, HTTPClientError]
    except ImportError:
        pass
    return tuple(classes)
