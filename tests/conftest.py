import asyncio
import logging
import queue
from pathlib import Path

import pytest
import yaml

from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.models.params import CapacityConfig, DistributionConfig, SimParameter


@pytest.fixture(autouse=True)
def restore_desflow_log_level():
    """
    Puts the levels of `desflow` and its child loggers back after each test.

    Several doc snippets and examples quieten a logger with, for example,
    `logging.getLogger("desflow").setLevel(logging.WARNING)` when imported. The docs
    tests import them in the worker's process, and without this a later test that
    reads INFO records with caplog finds none.
    """

    def levels():
        return {
            name: logger.level
            for name, logger in logging.Logger.manager.loggerDict.items()
            if (name == "desflow" or name.startswith("desflow."))
            and isinstance(logger, logging.Logger)
        }

    before = levels()
    yield
    for name in levels():
        logging.getLogger(name).setLevel(before.get(name, logging.NOTSET))


@pytest.fixture
def registry_transport(monkeypatch):
    """
    Returns `install(handler)`, which sends the requests of every `httpx.Client`
    built afterwards to `handler` through `httpx.MockTransport`.

    `ConfluentAvroSerializer` and `ConfluentAvroDeserializer` take no transport, so a
    test installs its fake registry before it builds one. `install` returns the
    transport.
    """
    import httpx

    real_client = httpx.Client

    def install(handler):
        transport = httpx.MockTransport(handler)

        def client(**kwargs):
            return real_client(**{**kwargs, "transport": transport})

        monkeypatch.setattr(httpx, "Client", client)
        return transport

    return install


@pytest.fixture
def run_until():
    """
    Runs a coroutine as a task until `done()` is true, then cancels it.

    Egress tests used to cancel after a fixed sleep, which a slow CI runner can
    outlast before the first write. Waiting on the result itself, with a time limit,
    removes that race; the test's own assertions then report what went wrong.
    """

    async def _run(coro, done, timeout=5.0):
        task = asyncio.create_task(coro)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not done() and loop.time() < deadline:
            await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    return _run


@pytest.fixture
def env():
    """Provides a fresh standard SimPy environment for each test."""
    return DynamicRealtimeEnvironment(factor=0.0)


@pytest.fixture
def registry(env):
    """Provides a SimulationRegistry attached to the test environment."""
    return env.registry


@pytest.fixture
def sample_params():
    """Provides a standard 'Shop' SimParameter set for testing."""
    return SimParameter(
        sim_id="Shop",
        arrival={
            "standard": DistributionConfig(dist="exponential", rate=1 / 10.0),
            "priority": DistributionConfig(dist="exponential", rate=1 / 50.0),
        },
        service={
            "setup": DistributionConfig(dist="normal", mean=2.0, std=0.5),
            "packing": DistributionConfig(dist="normal", mean=5.0, std=1.2),
        },
        resources={
            "packers": CapacityConfig(current_cap=2, max_cap=5),
            "operator": CapacityConfig(current_cap=1, max_cap=3),
        },
        containers={
            "tank1": CapacityConfig(current_cap=50.0, max_cap=100.0),
        },
        stores={
            "buffer1": CapacityConfig(current_cap=5, max_cap=10),
        },
    )


@pytest.fixture
def mock_ingress_queue():
    """
    Async Testing Fixtures

    Provides a thread-safe queue to simulate data arriving from Kafka/Redis.
    This is required for aiokafka and other async providers.
    """
    return queue.Queue()


@pytest.fixture
def mock_egress_queue():
    """
    Async Testing Fixtures

    Provides a thread-safe queue to simulate data stresming to Kafka/Redis.
    This is required for aiokafka and other async providers.
    """
    return queue.Queue()


class _MkDocsLoader(yaml.SafeLoader):
    """A safe loader that reads `!!python/name:` tags in mkdocs.yml as None."""


_MkDocsLoader.add_multi_constructor(
    "tag:yaml.org,2002:python/", lambda loader, suffix, node: None
)


@pytest.fixture
def mkdocs_nav():
    """The `nav` of mkdocs.yml, which a safe load refuses because of its Python tags."""
    text = (Path(__file__).resolve().parents[1] / "mkdocs.yml").read_text(
        encoding="utf-8"
    )
    return yaml.load(text, Loader=_MkDocsLoader)["nav"]
