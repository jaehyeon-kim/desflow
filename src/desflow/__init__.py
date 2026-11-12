"""
desflow: Real-time SimPy control plane for event-driven digital twins.

Bridges operations research (SimPy discrete-event simulation) with modern data engineering
(Kafka, Redis, Parquet, S3). Features the `SimulationContext` builder pattern to support
digital twin configuration, dynamic registry updates, and asynchronous I/O egress.

Standard Usage Example:
    from desflow import SimulationContext, ConsoleEgress

    app = (
        SimulationContext(sim_id="Shop", factor=1.0)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_arrival("orders", dist="exponential", rate=2.0)
        .add_service("packing", dist="normal", mean=1.0)
        .add_egress(ConsoleEgress())
    )

    @app.arrival_loop("orders")
    def order_generator(context):
        order_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(order_id, entity=entity))
            order_id += 1

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(order_id):
        return {"order_id": order_id}

    app.run(until=10.0)

A YAML blueprint builds the same kind of context: `SimulationContext.from_yaml(path)`,
or `desflow run <file>.yaml` from a shell.
"""

import contextlib
import logging
from importlib.metadata import PackageNotFoundError, version
from typing import Iterator

# ---------------------------------------------------------
# Core & Local Components (Always Available)
# ---------------------------------------------------------
from desflow.connectors.egress.local import ConsoleEgress
from desflow.connectors.ingress.file import FileIngress
from desflow.connectors.ingress.local import LocalIngress
from desflow.builder.context import SimulationContext
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.expressions import Expression, Scope
from desflow.builder.flow import Router, Transfer
from desflow.core.registry import (
    DynamicValue,
    ParamChange,
    ParamRejected,
    SimulationRegistry,
)
from desflow.core.sampler import Sampler
from desflow.builder.signals import Signal, SignalFailed
from desflow.builder.task import NO_RECORD, TaskFailed
from desflow.models.flow import Entity, Upstream
from desflow.builder.breakdowns import run_breakdowns
from desflow.core.replay import Row, read_trace
from desflow.builder.scenario import run_scenario
from desflow.models.params import (
    BreakdownConfig,
    CapacityConfig,
    ContainerConfig,
    DistributionConfig,
    SimParameter,
)
from desflow.models.schemas import EventPayload, LabelPayload, TelemetryPayload
from desflow.resources.container import ContainerBase, DynamicContainer
from desflow.resources.resource import DynamicResource, ResourceBase
from desflow.resources.store import DynamicStore, StoreBase
from desflow.analysis.fit import FitResult, fit_column
from desflow.analysis.models import LowLevelModel
from desflow.analysis.results import CheckFailed, ExperimentResult
from desflow.analysis.runner import Replications, replicate, run_experiment
from desflow.analysis.spec import ExperimentSpec
from desflow.loading import load_context
from desflow.utils import time_to_seconds

try:
    __version__ = version("desflow")
except PackageNotFoundError:
    __version__ = "0.0.0-dev"

logging.getLogger(__name__).addHandler(logging.NullHandler())

# Base exports that require no extra dependencies
__all__ = [
    "DynamicRealtimeEnvironment",
    "Sampler",
    "Signal",
    "SignalFailed",
    "TaskFailed",
    "NO_RECORD",
    "SimulationContext",
    "Expression",
    "Scope",
    "DynamicValue",
    "Row",
    "load_context",
    "ParamChange",
    "ParamRejected",
    "SimulationRegistry",
    "DynamicResource",
    "ResourceBase",
    "DynamicContainer",
    "ContainerBase",
    "DynamicStore",
    "StoreBase",
    "SimParameter",
    "DistributionConfig",
    "CapacityConfig",
    "BreakdownConfig",
    "run_breakdowns",
    "run_scenario",
    "read_trace",
    "ContainerConfig",
    "Entity",
    "Upstream",
    "Router",
    "Transfer",
    "LocalIngress",
    "FileIngress",
    "ConsoleEgress",
    "EventPayload",
    "TelemetryPayload",
    "LabelPayload",
    "time_to_seconds",
    "replicate",
    "Replications",
    "LowLevelModel",
    "run_experiment",
    "ExperimentSpec",
    "ExperimentResult",
    "CheckFailed",
    "fit_column",
    "FitResult",
]


# ---------------------------------------------------------
# Optional connectors, each in its own extra. A connector is left out only when a
# package of its extra is missing; any other failure to import it is raised, so a
# fault in a connector module does not look like a missing extra.
# ---------------------------------------------------------
@contextlib.contextmanager
def _without_extra() -> Iterator[None]:
    """Skips the imports in the block when a package outside desflow is missing."""
    try:
        yield
    except ModuleNotFoundError as exc:
        if exc.name is None or exc.name.split(".")[0] == "desflow":
            raise


# Requires `pip install desflow[kafka]`
with _without_extra():
    from desflow.connectors.admin.kafka import KafkaAdminConnector  # noqa: F401

    __all__.append("KafkaAdminConnector")
with _without_extra():
    from desflow.connectors.egress.kafka import KafkaEgress  # noqa: F401

    __all__.append("KafkaEgress")
with _without_extra():
    from desflow.connectors.ingress.kafka import KafkaIngress  # noqa: F401

    __all__.append("KafkaIngress")

# Requires `pip install desflow[parquet]`
with _without_extra():
    from desflow.connectors.egress.storage import (
        JsonlStorageEgress,  # noqa: F401
        ParquetStorageEgress,  # noqa: F401
    )

    __all__.extend(["JsonlStorageEgress", "ParquetStorageEgress"])

# Requires `pip install desflow[iceberg]`
with _without_extra():
    from desflow.connectors.egress.iceberg import IcebergStorageEgress  # noqa: F401

    __all__.append("IcebergStorageEgress")

# Requires `pip install desflow[postgres]`
with _without_extra():
    from desflow.connectors.egress.postgres import PostgresEgress  # noqa: F401

    __all__.append("PostgresEgress")
with _without_extra():
    from desflow.connectors.ingress.postgres import PostgresIngress  # noqa: F401

    __all__.append("PostgresIngress")

# Requires `pip install desflow[redis]`
with _without_extra():
    from desflow.connectors.egress.redis import RedisEgress  # noqa: F401

    __all__.append("RedisEgress")
with _without_extra():
    from desflow.connectors.ingress.redis import RedisIngress  # noqa: F401

    __all__.append("RedisIngress")
