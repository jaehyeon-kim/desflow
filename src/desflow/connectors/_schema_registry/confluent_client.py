"""The REST client of a Confluent Schema Registry, with no knowledge of any format."""

import json
import os
import random
import ssl
import threading
import time
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import quote, unquote, urlparse, urlunparse

import certifi
import httpx

from desflow.connectors._schema_registry.errors import SchemaRegistryError
from desflow.connectors._schema_registry.unset import UNSET
from desflow.connectors._checks import checked_poll, checked_whole

_ACCEPT = (
    "application/vnd.schemaregistry.v1+json, "
    "application/vnd.schemaregistry+json, application/json"
)
_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
_RETRIABLE_STATUS = (408, 429, 500, 502, 503, 504)
_BASE_DELAY_MS = 1000
_MAX_DELAY_MS = 20000

_DEFAULT_TIMEOUT = 10.0
_DEFAULT_MAX_RETRIES = 3

CertType = str | tuple[str, str | None] | tuple[str, str | None, str | None]
VerifyType = bool | str | ssl.SSLContext


def build_ssl_context(
    verify: VerifyType, cert: CertType | None
) -> bool | ssl.SSLContext:
    """Builds what httpx takes as `verify` from desflow's `verify` and `cert`.

    httpx 0.28 deprecates a path as `verify` and a `cert` argument, and asks for an
    `ssl.SSLContext` instead.

    Args:
        verify (bool | str | ssl.SSLContext): True checks certificates against
            `SSL_CERT_FILE`, then `SSL_CERT_DIR`, then certifi. False turns the
            check off. A path to a CA file or directory checks against it. A context
            is used as it is.
        cert (CertType | None): A client certificate file, or a tuple of the
            certificate file, the key file and the key password.

    Returns:
        bool | ssl.SSLContext: True when the default check needs no context,
            otherwise the context to pass to `httpx.Client`.

    Raises:
        OSError: If a CA file, a certificate or a key cannot be read.
    """
    if verify is True and cert is None:
        return True
    if isinstance(verify, ssl.SSLContext):
        context = verify
    elif verify is False:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    elif isinstance(verify, str):
        if os.path.isdir(verify):
            context = ssl.create_default_context(capath=verify)
        else:
            context = ssl.create_default_context(cafile=verify)
    elif os.environ.get("SSL_CERT_FILE"):
        context = ssl.create_default_context(cafile=os.environ["SSL_CERT_FILE"])
    elif os.environ.get("SSL_CERT_DIR"):
        context = ssl.create_default_context(capath=os.environ["SSL_CERT_DIR"])
    else:
        context = ssl.create_default_context(cafile=certifi.where())
    if cert is not None:
        if isinstance(cert, str):
            certfile, keyfile, password = cert, None, None
        else:
            certfile, keyfile, password = (*cert, None, None)[:3]
        context.load_cert_chain(certfile=certfile, keyfile=keyfile, password=password)
    return context


def _full_jitter_seconds(attempt: int) -> float:
    """Returns a random wait in seconds, up to a limit that doubles each attempt."""
    return random.random() * min(_BASE_DELAY_MS * 2.0**attempt, _MAX_DELAY_MS) / 1000


class ConfluentRegistryClient:
    """A client for the REST calls that desflow makes to a Confluent Schema Registry.

    It knows the registry, not any schema format: a caller names the format when it
    registers a schema, and the client returns what the registry sent. It caches
    those responses, never a parsed schema. It uses a synchronous `httpx.Client`, so
    a call blocks the caller, and it makes a call only on a cache miss.
    """

    def __init__(
        self,
        url: str,
        *,
        auth: tuple[str, str] | None = None,
        verify: VerifyType = True,
        cert: CertType | None = None,
        timeout: float | None = _DEFAULT_TIMEOUT,
        headers: Mapping[str, str] | None = None,
        proxy: str | None = None,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        transport: httpx.BaseTransport | None = None,
    ):
        """Builds the client. It makes no request.

        Args:
            url (str): The base URL of the registry. Several URLs separated by
                commas are tried in order. A URL may hold `user:password@` for
                basic authentication.
            auth (tuple[str, str] | None): A username and password for basic
                authentication.
            verify (bool | str | ssl.SSLContext): How to check the registry's
                certificate: True (the default), False, the path of a CA file or
                directory, or an SSL context.
            cert (CertType | None): A client certificate file, or a tuple of the
                certificate file, the key file and the key password.
            timeout (float | None): Seconds a request may take to connect, write,
                read and get a connection from the pool. None waits without a limit.
            headers (Mapping[str, str] | None): Headers added to every request, for
                example `{"Authorization": "Bearer <token>"}`.
            proxy (str | None): The URL of a proxy.
            max_retries (int): How many times a request is repeated after a transport
                error or a status of 408, 429, 500, 502, 503 or 504.
            transport (httpx.BaseTransport | None): A transport for tests, for example
                `httpx.MockTransport`.

        Raises:
            ValueError: If a URL's scheme is not `http` or `https` (in any case), if
                `auth` and a URL with `user:password@` are both given, if `cert` is
                invalid, if `timeout` is not None or a number of seconds above 0 that
                is not infinite, or if `max_retries` is not a whole number of 0 or
                more.
            OSError: If a CA file, a certificate or a key cannot be read.
        """
        self._base_urls = [part.strip().rstrip("/") for part in url.split(",")]
        for base_url in self._base_urls:
            if urlparse(base_url).scheme not in ("http", "https"):
                raise ValueError(f"Invalid url {base_url}")
            if auth is not None and urlparse(base_url).username is not None:
                raise ValueError(
                    "auth is given and the url holds credentials. "
                    "Remove the credentials from the url or remove auth"
                )
        auth = self._take_url_credentials(auth)
        if timeout is not None:
            timeout = checked_poll(timeout, "timeout")
        self._max_retries = checked_whole(max_retries, "max_retries", 0)
        self._headers = dict(headers or {})
        self._client = httpx.Client(
            verify=build_ssl_context(verify, cert),
            auth=auth,
            proxy=proxy,
            timeout=timeout,
            transport=transport,
        )
        self._lock = threading.Lock()
        self._ids: dict[tuple[str, str, str, str], int] = {}
        self._lookups: dict[tuple[str, str, str, str], int] = {}
        self._by_id: dict[int, dict[str, Any]] = {}
        self._by_guid: dict[str, dict[str, Any]] = {}
        self._versions: dict[tuple[str, int], dict[str, Any]] = {}

    def _take_url_credentials(
        self, auth: tuple[str, str] | None
    ) -> tuple[str, str] | None:
        """Takes `user:password@` from the first URL as `auth` for every URL.

        confluent-kafka 2.14.0 does the same: the credentials of the first URL
        apply to all of them. They are unquoted and removed from the URLs.
        """
        if auth is not None:
            return auth
        first = urlparse(self._base_urls[0])
        if first.username is None or first.password is None:
            return None
        self._base_urls = [
            urlunparse(parsed._replace(netloc=parsed.netloc.rpartition("@")[2]))
            for parsed in map(urlparse, self._base_urls)
        ]
        return unquote(first.username), unquote(first.password)

    def register(
        self,
        subject: str,
        schema_str: str,
        schema_type: str,
        references: Sequence[Mapping[str, Any]] = (),
    ) -> int:
        """Registers a schema under a subject, or returns the id it already has.

        The call is made once for each subject, schema type, schema and references.
        A failed call is not cached, so the next call repeats it.

        Args:
            subject (str): The subject, such as `orders-value`.
            schema_str (str): The schema as a string.
            schema_type (str): The format, such as `AVRO`. There is no default, so a
                caller never registers a schema under the wrong format.
            references (Sequence[Mapping[str, Any]]): Schemas this one refers to,
                each with `name`, `subject` and `version`.

        Returns:
            int: The id of the schema.

        Raises:
            SchemaRegistryError: If the registry answers with an error status, for
                example 409 for an incompatible schema.
            httpx.TransportError: If no registry URL could be reached.
        """
        key = (
            subject,
            schema_type,
            schema_str,
            json.dumps(list(references), sort_keys=True),
        )
        with self._lock:
            if key not in self._ids:
                body = {
                    "schema": schema_str,
                    "schemaType": schema_type,
                    "references": list(references),
                }
                response = self._request(
                    "POST", f"subjects/{quote(subject, safe='')}/versions", body
                )
                self._ids[key] = response["id"]
            return self._ids[key]

    def lookup(
        self,
        subject: str,
        schema_str: str,
        schema_type: str,
        references: Sequence[Mapping[str, Any]] = (),
    ) -> int:
        """Finds the version of a subject that holds a schema, without registering it.

        The call is made once for each subject, schema type, schema and references.
        A failed call is not cached, so the next call repeats it.

        Args:
            subject (str): The subject.
            schema_str (str): The schema as a string.
            schema_type (str): The format, such as `PROTOBUF`.
            references (Sequence[Mapping[str, Any]]): Schemas this one refers to,
                each with `name`, `subject` and `version`.

        Returns:
            int: The version of the subject that holds the schema.

        Raises:
            SchemaRegistryError: If the registry answers with an error status, for
                example 404 when the subject has no such schema.
            httpx.TransportError: If no registry URL could be reached.
        """
        key = (
            subject,
            schema_type,
            schema_str,
            json.dumps(list(references), sort_keys=True),
        )
        with self._lock:
            if key not in self._lookups:
                body = {
                    "schema": schema_str,
                    "schemaType": schema_type,
                    "references": list(references),
                }
                response = self._request(
                    "POST", f"subjects/{quote(subject, safe='')}?normalize=false", body
                )
                self._lookups[key] = response["version"]
            return self._lookups[key]

    def schema_by_id(self, schema_id: int) -> dict[str, Any]:
        """Fetches a schema by its id.

        Args:
            schema_id (int): The id from the prefix of a message.

        Returns:
            dict[str, Any]: The registry's response: `schema`, `schemaType` (AVRO
                when the registry sent none), and `references` and `ruleSet` when
                present.

        Raises:
            SchemaRegistryError: If the registry answers with an error status, for
                example 404 for an unknown id.
            httpx.TransportError: If no registry URL could be reached.
        """
        with self._lock:
            if schema_id not in self._by_id:
                response = self._request("GET", f"schemas/ids/{schema_id}")
                self._by_id[schema_id] = _with_schema_type(response)
            return self._by_id[schema_id]

    def schema_by_guid(self, guid: str) -> dict[str, Any]:
        """Fetches a schema by its GUID.

        Args:
            guid (str): The GUID in 8-4-4-4-12 form.

        Returns:
            dict[str, Any]: The same fields as `schema_by_id`.

        Raises:
            SchemaRegistryError: If the registry answers with an error status.
            httpx.TransportError: If no registry URL could be reached.
        """
        with self._lock:
            if guid not in self._by_guid:
                response = self._request("GET", f"schemas/guids/{quote(guid, safe='')}")
                self._by_guid[guid] = _with_schema_type(response)
            return self._by_guid[guid]

    def subject_version(self, subject: str, version: int) -> dict[str, Any]:
        """Fetches one version of a subject, to resolve a schema reference.

        Args:
            subject (str): The subject named by the reference.
            version (int): The version named by the reference.

        Returns:
            dict[str, Any]: The registry's response: `schema` and, when present,
                `references`, with `schemaType` set to AVRO when absent.

        Raises:
            SchemaRegistryError: If the registry answers with an error status, for
                example 404 for an unknown subject or version.
            httpx.TransportError: If no registry URL could be reached.
        """
        key = (subject, version)
        with self._lock:
            if key not in self._versions:
                response = self._request(
                    "GET", f"subjects/{quote(subject, safe='')}/versions/{version}"
                )
                self._versions[key] = _with_schema_type(response)
            return self._versions[key]

    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> Any:
        """Sends a request to each registry URL in turn until one answers.

        A URL that raises a transport error, or answers with a status that is
        retried, passes the request to the next URL once its own retries are used.
        """
        headers = {"Accept": _ACCEPT}
        content = None
        if body is not None:
            headers["Content-Type"] = _CONTENT_TYPE
            content = json.dumps(body)
        headers.update(self._headers)

        response: httpx.Response | None = None
        for index, base_url in enumerate(self._base_urls):
            is_last = index == len(self._base_urls) - 1
            try:
                response = self._send_with_retries(
                    method, f"{base_url}/{path}", headers, content
                )
            except httpx.TransportError:
                if is_last:
                    raise
                continue
            if response.is_success:
                return _json_of(response)
            if response.status_code not in _RETRIABLE_STATUS or is_last:
                break
        assert response is not None
        raise _error_of(response)

    def _send_with_retries(
        self, method: str, url: str, headers: dict[str, str], content: str | None
    ) -> httpx.Response:
        """Sends one request, repeating it up to `max_retries` times."""
        for attempt in range(self._max_retries + 1):
            try:
                response = self._client.request(
                    method, url, headers=headers, content=content
                )
            except httpx.TransportError:
                if attempt >= self._max_retries:
                    raise
            else:
                retry = (
                    not response.is_success
                    and response.status_code in _RETRIABLE_STATUS
                )
                if not retry or attempt >= self._max_retries:
                    return response
            time.sleep(_full_jitter_seconds(attempt))
        raise AssertionError("unreachable")  # pragma: no cover


def _with_schema_type(response: Any) -> dict[str, Any]:
    """Sets `schemaType` to AVRO when the registry left it out, as its API docs say."""
    response.setdefault("schemaType", "AVRO")
    return response  # type: ignore[no-any-return]


def _json_of(response: httpx.Response) -> Any:
    """Returns the JSON body of a successful response."""
    if response.status_code == 204 or not response.content:
        return None
    try:
        return response.json()
    except ValueError:
        raise SchemaRegistryError(
            response.status_code, -1, "The response body is not JSON: " + response.text
        ) from None


def _error_of(response: httpx.Response) -> SchemaRegistryError:
    """Builds the error for a failed response from the registry's JSON body."""
    try:
        body = response.json()
        return SchemaRegistryError(
            response.status_code, body["error_code"], body["message"]
        )
    except (ValueError, KeyError, TypeError):
        return SchemaRegistryError(
            response.status_code,
            -1,
            "Unknown Schema Registry Error: " + response.text,
        )


def given_arguments(
    *,
    auth: tuple[str, str] | None = UNSET,
    verify: VerifyType = UNSET,
    cert: CertType | None = UNSET,
    timeout: float | None = UNSET,
    headers: Mapping[str, str] | None = UNSET,
    proxy: str | None = UNSET,
    max_retries: int = UNSET,
) -> dict[str, Any]:
    """Returns the named arguments a caller gave.

    An argument that is not given holds `UNSET`, and `ConfluentRegistryClient` fills
    in its default.

    Args:
        auth (tuple[str, str] | None): The named argument of the same name.
        verify (VerifyType): The named argument of the same name.
        cert (CertType | None): The named argument of the same name.
        timeout (float | None): The named argument of the same name.
        headers (Mapping[str, str] | None): The named argument of the same name.
        proxy (str | None): The named argument of the same name.
        max_retries (int): The named argument of the same name.

    Returns:
        dict[str, Any]: The arguments for `ConfluentRegistryClient` that were given,
            among `auth`, `verify`, `cert`, `timeout`, `headers`, `proxy` and
            `max_retries`.
    """
    arguments: dict[str, Any] = {
        "auth": auth,
        "verify": verify,
        "cert": cert,
        "timeout": timeout,
        "headers": headers,
        "proxy": proxy,
        "max_retries": max_retries,
    }
    given: dict[str, Any] = {}
    for name, value in arguments.items():
        if value is not UNSET:
            given[name] = value
    return given
