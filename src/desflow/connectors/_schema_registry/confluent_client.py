"""The REST client of a Confluent Schema Registry, with no knowledge of any format."""

import json
import os
import random
import ssl
import threading
import time
import warnings
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import quote, unquote, urlparse, urlunparse

import certifi
import httpx

from desflow.connectors._schema_registry.errors import SchemaRegistryError
from desflow.connectors._schema_registry.unset import UNSET

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
            ValueError: If a URL does not start with `http`, if `auth` and a URL with
                `user:password@` are both given, or if `cert` is invalid.
            OSError: If a CA file, a certificate or a key cannot be read.
        """
        self._base_urls = [part.strip().rstrip("/") for part in url.split(",")]
        for base_url in self._base_urls:
            if not base_url.startswith("http"):
                raise ValueError(f"Invalid url {base_url}")
            if auth is not None and urlparse(base_url).username is not None:
                raise ValueError(
                    "auth is given and the url holds credentials. "
                    "Remove the credentials from the url or remove auth"
                )
        auth = self._take_url_credentials(auth)
        self._max_retries = max_retries
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


def resolve_legacy_options(
    options: Mapping[str, Any],
    *,
    auth: tuple[str, str] | None = UNSET,
    verify: VerifyType = UNSET,
    cert: CertType | None = UNSET,
    timeout: float | None = UNSET,
    headers: Mapping[str, str] | None = UNSET,
    proxy: str | None = UNSET,
    max_retries: int = UNSET,
) -> dict[str, Any]:
    """Maps the confluent-kafka keys of `**registry_kwargs` to named arguments.

    An argument that is not given holds `UNSET`, so a key is refused whenever its
    argument is given, even with its default value. It returns only the values
    that were given; `ConfluentRegistryClient` fills in the defaults.

    Each accepted key warns with a `FutureWarning`. The warning names the key and
    its replacement, never the value, so a secret is not printed. The keys are
    removed in desflow 0.30.0.

    Args:
        options (Mapping[str, Any]): The keys that were passed as `**registry_kwargs`.
        auth (tuple[str, str] | None): The named argument of the same name.
        verify (VerifyType): The named argument of the same name.
        cert (CertType | None): The named argument of the same name.
        timeout (float | None): The named argument of the same name.
        headers (Mapping[str, str] | None): The named argument of the same name.
        proxy (str | None): The named argument of the same name.
        max_retries (int): The named argument of the same name.

    Returns:
        dict[str, Any]: The arguments for `ConfluentRegistryClient` that were given,
            by argument or by key, among `auth`, `verify`, `cert`, `timeout`,
            `headers`, `proxy` and `max_retries`.

    Raises:
        ValueError: If a key is given and so is its named argument, if a key has no
            replacement, if a key holds an invalid value, or if a key is unknown.
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
    given = {name: value is not UNSET for name, value in arguments.items()}
    resolved = {name: value for name, value in arguments.items() if given[name]}
    remaining = dict(options)
    notices: list[str] = []

    def take(key: str, argument: str) -> Any:
        """Removes a key, notes it, and refuses it when its argument is also given."""
        if given[argument]:
            raise ValueError(f"'{key}' and {argument}= are both given. Use {argument}=")
        notices.append(_notice(key, f"{argument}="))
        return remaining.pop(key)

    if "url" in remaining:
        raise ValueError("The key 'url' is not accepted. Pass registry_url")
    for key in ("retries.wait.ms", "retries.max.wait.ms"):
        if key in remaining:
            raise ValueError(
                f"The key '{key}' is not accepted. desflow waits with a random "
                "backoff; set the number of tries with max_retries="
            )

    if "basic.auth.user.info" in remaining:
        user_info = take("basic.auth.user.info", "auth")
        parts = str(user_info).split(":", 1)
        if len(parts) != 2:
            raise ValueError("basic.auth.user.info must be in the form user:password")
        resolved["auth"] = (parts[0], parts[1])
    if "ssl.ca.location" in remaining:
        resolved["verify"] = take("ssl.ca.location", "verify")
    notices += _resolve_certificate(remaining, resolved, given["cert"])
    if "max.retries" in remaining:
        resolved["max_retries"] = int(take("max.retries", "max_retries"))
    for key in ("cache.capacity", "cache.latest.ttl.sec"):
        if key in remaining:
            notices.append(_notice(key, None))
            del remaining[key]

    notices += _resolve_bearer(remaining, resolved, given["headers"])

    if remaining:
        raise ValueError(f"Unrecognized properties: {', '.join(sorted(remaining))}")
    for notice in notices:
        warnings.warn(notice, FutureWarning, stacklevel=3)
    return resolved


def _resolve_certificate(
    remaining: dict[str, Any], resolved: dict[str, Any], cert_given: bool
) -> list[str]:
    """Turns the `ssl.certificate.location` and key keys into `cert`.

    Returns:
        list[str]: One deprecation notice for each key it took.
    """
    certificate = remaining.get("ssl.certificate.location")
    key_file = remaining.get("ssl.key.location")
    key_password = remaining.get("ssl.key.password")
    if certificate is None and (key_file is not None or key_password is not None):
        raise ValueError(
            "ssl.certificate.location is required with ssl.key.location "
            "or ssl.key.password"
        )
    if certificate is None:
        return []
    if cert_given:
        raise ValueError("The ssl.* keys and cert= are both given. Use cert=")
    notices = []
    for key in ("ssl.certificate.location", "ssl.key.location", "ssl.key.password"):
        if key in remaining:
            notices.append(_notice(key, "cert="))
            del remaining[key]
    resolved["cert"] = (
        (certificate, key_file, key_password)
        if key_password is not None
        else (certificate, key_file)
    )
    return notices


def _resolve_bearer(
    remaining: dict[str, Any], resolved: dict[str, Any], headers_given: bool
) -> list[str]:
    """Turns the static bearer token keys into `headers`, and refuses other sources.

    Returns:
        list[str]: One deprecation notice for each key it took.
    """
    bearer = [key for key in remaining if key.startswith("bearer.auth.")]
    if not bearer:
        return []
    source = remaining.get("bearer.auth.credentials.source")
    if source != "STATIC_TOKEN":
        raise ValueError(
            "Only bearer.auth.credentials.source STATIC_TOKEN is accepted. "
            "Obtain the token yourself and pass it in headers="
        )
    if headers_given:
        raise ValueError(
            "The bearer.auth.* keys and headers= are both given. Use headers="
        )
    token = remaining.get("bearer.auth.token")
    cluster = remaining.get("bearer.auth.logical.cluster")
    if token is None or cluster is None:
        raise ValueError(
            "bearer.auth.token and bearer.auth.logical.cluster are required "
            "with STATIC_TOKEN"
        )
    pool = remaining.get("bearer.auth.identity.pool.id")
    unknown = set(bearer) - {
        "bearer.auth.credentials.source",
        "bearer.auth.token",
        "bearer.auth.logical.cluster",
        "bearer.auth.identity.pool.id",
    }
    if unknown:
        raise ValueError(
            "These keys are not accepted with STATIC_TOKEN: "
            f"{', '.join(sorted(unknown))}. Pass the token in headers="
        )
    resolved["headers"] = {
        "Authorization": f"Bearer {token}",
        "target-sr-cluster": cluster,
        **({"Confluent-Identity-Pool-Id": pool} if pool is not None else {}),
    }
    notices = [_notice(key, "headers=") for key in bearer]
    for key in bearer:
        del remaining[key]
    return notices


def _notice(key: str, replacement: str | None) -> str:
    """Words the deprecation of a confluent-kafka key, without its value."""
    if replacement is None:
        text = f"The registry option '{key}' has no effect in desflow"
    else:
        text = f"The registry option '{key}' is replaced by {replacement}"
    return f"{text}, and desflow 0.30.0 removes it."
