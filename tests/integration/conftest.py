import asyncio
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

# Example infrastructure comes from odctl (https://github.com/jaehyeon-kim/odctl).
# Install it with `uv tool install "odctl>=0.5.1"` or `pip install "odctl>=0.5.1"`.
# 0.5.1 is the floor: earlier versions created the Valkey user without a channel
# grant, so RedisIngress could not subscribe (odctl#76).
ODCTL = "odctl"

# The emulators are single containers that a session fixture starts with
# `docker run`, with no odctl profile. Each version below was the latest release
# when it was pinned. To move one, change the tag and run the tests of its group:
# `pytest tests/integration -m floci`, `-m azure` or `-m gcp`.
FLOCI_IMAGE = "floci/floci:2.3.0"
AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.37.0"
FAKE_GCS_IMAGE = "fsouza/fake-gcs-server:1.56.1"


def _run_odctl(*args: str) -> None:
    """Run an odctl command and raise with its output when it fails."""
    subprocess.run([ODCTL, *args], check=True, capture_output=True, text=True)


GROUPS = ("services", "storage", "floci", "azure", "gcp", "examples")


def pytest_collection_modifyitems(items):
    """Refuse a test here without the `integration` mark and exactly one group mark.

    CI runs one job per group with `-m <group>`, so a test with no group would run
    in no job, and a test with two groups would run in two. The hook selects tests
    by their path under this folder, not by the `integration` mark, so a module
    written with no marks at all is refused as well.
    """
    folder = Path(__file__).parent
    for item in items:
        if folder not in item.path.parents:
            continue
        groups = [group for group in GROUPS if item.get_closest_marker(group)]
        if item.get_closest_marker("integration") is None or len(groups) != 1:
            raise pytest.UsageError(
                f"{item.nodeid} needs the integration mark and exactly one of the "
                f"marks {', '.join(GROUPS)}; it has {len(groups)} group mark(s) and "
                f"{'an' if item.get_closest_marker('integration') else 'no'} "
                "integration mark"
            )


def _unavailable(reason: str) -> None:
    """Stop a test whose infrastructure is missing.

    Locally the test is skipped. In CI, where GitHub Actions sets `CI` to `true`, it
    fails, so a job whose infrastructure is missing cannot pass by skipping every
    test.
    """
    if os.environ.get("CI") == "true":
        pytest.fail(reason)
    pytest.skip(f"{reason} Skipping integration tests.")


@pytest.fixture(scope="session")
def check_docker():
    """Verify Docker is available, skip integration tests if not (fail in CI)."""
    try:
        subprocess.run(
            ["docker", "version"], check=True, capture_output=True, text=True
        )
    except Exception:
        _unavailable("Docker is not available.")


@pytest.fixture(scope="session")
def check_odctl(check_docker):
    """Verify odctl is installed, skip integration tests if not (fail in CI)."""
    if shutil.which(ODCTL) is None:
        _unavailable("odctl is not installed.")


def _wait_for_http(url: str, name: str, seconds: int = 60) -> None:
    """Wait until `url` answers with any HTTP status, or fail the test."""
    for _ in range(seconds):
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except urllib.error.HTTPError:
            return
        except Exception:
            time.sleep(1)
    pytest.fail(f"{name} did not start in time.")


def _run_container(image: str, port: int, *arguments: str):
    """Start `image` with its `port` published on 127.0.0.1, then yield the address.

    The container is removed when it stops, and the stop runs at the end of the
    session. Address it as 127.0.0.1 rather than localhost, for the reason
    schema_registry gives.
    """
    started = subprocess.run(
        ["docker", "run", "-d", "--rm", "-p", f"127.0.0.1:{port}:{port}", image]
        + list(arguments),
        capture_output=True,
        text=True,
    )
    if started.returncode != 0:
        pytest.fail(f"Could not start {image}: {started.stderr.strip()}")
    container_id = started.stdout.strip()
    try:
        _wait_for_http(f"http://127.0.0.1:{port}/", image)
        yield f"127.0.0.1:{port}"
    finally:
        subprocess.run(["docker", "stop", "-t", "2", container_id], capture_output=True)


@pytest.fixture(scope="session")
def profile_running():
    """Returns a function that stops a test when a profile is not ready.

    The `service-examples` job starts the odctl profile itself, so its tests start
    nothing. Locally the test is skipped when the profile is down, and in CI it fails.
    odctl waits for the health checks of postgres and valkey, but catalog and the
    Kafka brokers have none, so `odctl up` returns before their servers answer. For
    those two, an open port is followed by up to 60 seconds of waiting for the server.
    """

    def ready(profile: str, port: int) -> bool:
        if profile == "catalog":
            url = f"http://127.0.0.1:{port}/v1/config"
            with urllib.request.urlopen(url, timeout=2) as response:
                return response.status == 200
        if profile.startswith("kafka"):
            from aiokafka.admin import AIOKafkaAdminClient

            async def start() -> None:
                admin = AIOKafkaAdminClient(
                    bootstrap_servers=f"127.0.0.1:{port}", request_timeout_ms=2000
                )
                try:
                    await admin.start()
                finally:
                    await admin.close()

            asyncio.run(start())
        return True

    def check(profile: str, port: int) -> None:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=2).close()
        except OSError:
            _unavailable(
                f"The odctl {profile} profile is not listening on port {port}. "
                f"Start it with `odctl up {profile}`."
            )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if ready(profile, port):
                    return
            except Exception:
                pass
            time.sleep(1)
        _unavailable(f"The odctl {profile} profile did not answer within 60 seconds.")

    return check


@pytest.fixture(scope="session")
def odctl_profile(check_odctl):
    """
    Bring odctl profiles up on demand and tear down every started profile at the
    end of the session.

    A profile is started once per session. Restarting it between modules costs a
    full image pull and health-check cycle for no benefit, because each test
    creates the topics and tables it needs.
    """
    started: list[str] = []

    def _up(profile: str) -> None:
        if profile not in started:
            _run_odctl("up", profile)
            started.append(profile)

    yield _up

    for profile in reversed(started):
        _run_odctl("down", profile, "--volumes")


@pytest.fixture(scope="session")
def postgres_container(odctl_profile):
    """Starts the odctl `postgres` profile and yields its DSN."""
    odctl_profile("postgres")

    # odctl names the default database `odctl`, with the user/password pair
    # reported by `odctl explain postgres`.
    dsn = "postgresql://user:password@127.0.0.1:5432/odctl"
    import asyncpg

    # Wait for Postgres to be ready
    for _ in range(30):
        try:

            async def check():
                conn = await asyncpg.connect(dsn)
                await conn.close()

            asyncio.run(check())
            break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("Postgres did not start in time.")

    yield dsn


@pytest.fixture(scope="session")
def kafka_container(odctl_profile, profile_running):
    """Starts the odctl `kafka-lite` profile and yields its bootstrap servers."""
    odctl_profile("kafka-lite")

    # 127.0.0.1, not localhost, for the reason schema_registry gives
    bootstrap_servers = "127.0.0.1:9092"

    # The kafka-lite broker has no health check, so `odctl up` returns before it
    # answers; wait for it as the service examples do
    profile_running("kafka-lite", 9092)
    yield bootstrap_servers


@pytest.fixture(scope="session")
def schema_registry(kafka_container):
    """Yields the Karapace URL from the odctl `kafka-lite` profile.

    `kafka-lite` carries Karapace on 8081 next to the broker, so no second profile
    is needed. Karapace has no health check either, so `odctl up` returns before it
    answers: poll the registry itself before handing the URL over.

    Address it as 127.0.0.1 rather than localhost. Karapace binds IPv4 only, while
    localhost resolves to ::1 first on macOS, so every request is reset before it
    reaches the container.
    """
    url = "http://127.0.0.1:8081"

    for _ in range(60):
        try:
            with urllib.request.urlopen(f"{url}/subjects", timeout=2) as response:
                if response.status == 200:
                    break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("Karapace did not start in time.")

    yield url


@pytest.fixture(scope="session")
def redis_container(odctl_profile):
    """Starts the odctl `valkey` profile and yields its connection URL."""
    odctl_profile("valkey")

    # odctl disables the unauthenticated default Valkey user, so the URL has to
    # carry the `user` / `password` pair.
    yield "redis://user:password@127.0.0.1:6379/0"


@pytest.fixture(scope="session")
def seaweedfs(odctl_profile):
    """Starts the odctl `storage` profile and yields SeaweedFS's S3 settings.

    The endpoint, user and password are the ones `odctl explain storage` reports.
    odctl returns once the health check passes, so the S3 port already answers.
    """
    odctl_profile("storage")
    yield {
        "endpoint": "http://127.0.0.1:8333",
        "access_key": "user",
        "secret_key": "password",
        "region": "us-east-1",
    }


@pytest.fixture(scope="session")
def floci(check_docker):
    """Starts a Floci container, the AWS emulator, and yields its endpoint URL.

    Floci accepts any non-empty credentials.
    """
    for address in _run_container(FLOCI_IMAGE, 4566):
        yield f"http://{address}"


@pytest.fixture(scope="session")
def azurite(check_docker):
    """Starts an Azurite container, the Azure Blob Storage emulator.

    Yields a `filesystem` mapping for it. The account and key are Azurite's
    well-known development account, published in Azurite's README and the same on
    every installation, so the key is not a secret.
    """
    for address in _run_container(AZURITE_IMAGE, 10000):
        yield {
            "type": "azure",
            "account_name": "devstoreaccount1",
            "account_key": (
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
                "K1SZFPTOtr/KBHBeksoGMGw=="
            ),
            "blob_storage_authority": address,
            "blob_storage_scheme": "http",
        }


@pytest.fixture(scope="session")
def fake_gcs(check_docker):
    """Starts a fake-gcs-server container, the Google Cloud Storage emulator.

    Yields the address of its HTTP endpoint.
    """
    for address in _run_container(FAKE_GCS_IMAGE, 4443, "-scheme", "http"):
        yield address


@pytest.fixture(scope="session")
def iceberg_catalog(odctl_profile, seaweedfs):
    """Starts the odctl `catalog` profile and yields a pyiceberg REST catalog.

    The profile pulls in `deps`, `postgres` and `storage` itself, so the REST
    fixture is backed by Postgres and writes its data files to SeaweedFS.

    pyiceberg reads `s3.force-virtual-addressing`, not the `s3.path-style-access`
    name the catalog container uses. It defaults to path style for S3FileIO, which
    is what SeaweedFS needs, so the property is left out rather than set wrongly.
    """
    odctl_profile("catalog")

    uri = "http://127.0.0.1:8181"
    for _ in range(60):
        try:
            with urllib.request.urlopen(f"{uri}/v1/config", timeout=2) as response:
                if response.status == 200:
                    break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("The Iceberg REST catalog did not start in time.")

    from pyiceberg.catalog.rest import RestCatalog

    yield RestCatalog(
        "odctl",
        **{
            "uri": uri,
            "warehouse": "s3://warehouse/",
            "s3.endpoint": seaweedfs["endpoint"],
            "s3.access-key-id": seaweedfs["access_key"],
            "s3.secret-access-key": seaweedfs["secret_key"],
            "s3.region": seaweedfs["region"],
        },
    )
