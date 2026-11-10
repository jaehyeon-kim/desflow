"""The fixtures of every known case.

A case that does not end fails after a minute instead of holding the suite. A CA and a
key pair are written once for the run, for the documents that read certificate files
(DX-23), and the import of `aws_msk_iam_sasl_signer` is answered by a stand-in for each
test.
"""

import faulthandler
import os
import signal
import sys
import types
from typing import Iterator

import pytest

from tests.known_cases import harness

LIMIT_SECONDS = 60


@pytest.fixture(autouse=True)
def _a_case_that_does_not_end_fails() -> Iterator[None]:
    if not hasattr(signal, "SIGALRM"):
        # Windows has no alarm: after the limit the run stops with the traceback of
        # every thread, so a case that does not end cannot hold the run for ever
        faulthandler.dump_traceback_later(LIMIT_SECONDS, exit=True)
        try:
            yield
        finally:
            faulthandler.cancel_dump_traceback_later()
        return

    def stop(signum: int, frame: object) -> None:
        raise TimeoutError(f"the case did not end in {LIMIT_SECONDS} seconds")

    previous = signal.signal(signal.SIGALRM, stop)
    signal.alarm(LIMIT_SECONDS)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


@pytest.fixture(scope="session", autouse=True)
def _tls_files(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """A CA and a key pair in a folder of the run, named by the variables
    `KAFKA_CA_FILE`, `KAFKA_CERT_FILE` and `KAFKA_KEY_FILE`, which the document of
    DX-23 and its Builder form read."""
    folder = tmp_path_factory.mktemp("tls")
    files = harness.write_tls_files(folder)
    names = {
        "KAFKA_CA_FILE": files["ca"],
        "KAFKA_CERT_FILE": files["cert"],
        "KAFKA_KEY_FILE": files["key"],
    }
    before = {name: os.environ.get(name) for name in names}
    os.environ.update({name: str(path) for name, path in names.items()})
    try:
        yield
    finally:
        for name, value in before.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@pytest.fixture(autouse=True)
def _msk_signer() -> Iterator[None]:
    """A stand-in for `aws_msk_iam_sasl_signer`, which the token provider of
    `sasl_oauth_token_provider: {type: aws_msk_iam}` imports when it is built. Its token
    is `token-for-<region>`. The module is in `sys.modules` for one test only, so a
    test elsewhere still finds the package missing."""
    module = types.ModuleType("aws_msk_iam_sasl_signer")

    class MSKAuthTokenProvider:
        @staticmethod
        def generate_auth_token(region: str) -> tuple[str, int]:
            return f"token-for-{region}", 0

    module.MSKAuthTokenProvider = MSKAuthTokenProvider  # type: ignore[attr-defined]
    before = sys.modules.get("aws_msk_iam_sasl_signer")
    sys.modules["aws_msk_iam_sasl_signer"] = module
    try:
        yield
    finally:
        if before is None:
            sys.modules.pop("aws_msk_iam_sasl_signer", None)
        else:
            sys.modules["aws_msk_iam_sasl_signer"] = before
