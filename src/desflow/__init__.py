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

import logging
from importlib.metadata import PackageNotFoundError, version

# ---------------------------------------------------------
# Core & Local Components (Always Available)
# ---------------------------------------------------------
from desflow.connectors.egress.local import ConsoleEgress
from desflow.connectors.ingress.local import LocalIngress
from desflow.core.context import SimulationContext
from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.flow import Router, Transfer
from desflow.core.registry import ParamChange, ParamRejected, SimulationRegistry
from desflow.core.sampler import Sampler
from desflow.core.signals import Signal, SignalFailed
from desflow.core.task import TaskFailed
from desflow.models.flow import Entity, Upstream
from desflow.core.breakdowns import run_breakdowns
from desflow.core.replay import read_trace
from desflow.core.scenario import run_scenario
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
    "SimulationContext",
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
# Optional: Kafka Connectors (Requires `pip install desflow[kafka]`)
# ---------------------------------------------------------
try:
    from desflow.connectors.admin.kafka import KafkaAdminConnector  # noqa: F401
    from desflow.connectors.egress.kafka import KafkaEgress  # noqa: F401
    from desflow.connectors.ingress.kafka import KafkaIngress  # noqa: F401

    __all__.extend(["KafkaAdminConnector", "KafkaEgress", "KafkaIngress"])
except ImportError:
    pass

# ---------------------------------------------------------
# Optional: Storage Connectors (Requires `pip install desflow[parquet]`)
# ---------------------------------------------------------
try:
    from desflow.connectors.egress.storage import (
        JsonlStorageEgress,  # noqa: F401
        ParquetStorageEgress,  # noqa: F401
    )

    __all__.extend(["JsonlStorageEgress", "ParquetStorageEgress"])
except ImportError:
    pass

# ---------------------------------------------------------
# Optional: Iceberg Connector (Requires `pip install desflow[iceberg]`)
# ---------------------------------------------------------
try:
    from desflow.connectors.egress.iceberg import (
        IcebergStorageEgress,  # noqa: F401
    )

    __all__.extend(["IcebergStorageEgress"])
except ImportError:
    pass

# ---------------------------------------------------------
# Optional: Postgres Connectors (Requires `pip install desflow[postgres]`)
# ---------------------------------------------------------
try:
    from desflow.connectors.egress.postgres import PostgresEgress  # noqa: F401
    from desflow.connectors.ingress.postgres import PostgresIngress  # noqa: F401

    __all__.extend(["PostgresEgress", "PostgresIngress"])
except ImportError:
    pass

# ---------------------------------------------------------
# Optional: Redis Connectors (Requires `pip install desflow[redis]`)
# ---------------------------------------------------------
try:
    from desflow.connectors.egress.redis import RedisEgress  # noqa: F401
    from desflow.connectors.ingress.redis import RedisIngress  # noqa: F401

    __all__.extend(["RedisEgress", "RedisIngress"])
except ImportError:
    pass
