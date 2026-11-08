"""A `PostgresIngress` restart gives the registry what the live run held.

For each builder model of the capture (the cases of `test_second_run.py`) with a
resource, the applied history holds a `max_cap` above the declared one (id 1) and then a
`current_cap` above the declared one (id 2). The fake pool returns the rows in the order
PostgreSQL gives for the query it is sent, so a query that puts the latest change of
each path in path order puts `current_cap` before `max_cap`, and the registry clamps it
to the declared maximum. The registry must be given the changes in id order, which is the order that
gave the live run its state.
"""

import contextlib
import importlib.util
import logging
import os
import threading
from pathlib import Path
from typing import Any, List
from unittest.mock import MagicMock, patch

import pytest

from desflow.connectors.ingress.postgres import PostgresIngress
from desflow.core.environment import IngressMixIn
from desflow.core.registry import SimulationRegistry

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "second_run_cases", ROOT / "tests" / "unit" / "test_second_run.py"
)
assert _spec is not None and _spec.loader is not None
second_run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(second_run)
regression = second_run.regression

MODELS = [
    case
    for case in second_run.CASES
    if case[0].endswith(":builder") and "simpy/" not in case[0]
]
IDS = [case[0] for case in MODELS]


class _Conn:
    def __init__(self, history: List[dict], done: threading.Event):
        self.history = history
        self.done = done

    async def execute(self, *args: Any) -> None:
        return None

    async def fetch(self, sql: str, *args: Any) -> list:
        if "is_applied = TRUE" not in sql:
            return []
        last = sql.rsplit("ORDER BY", 1)[1].split()
        latest = {}
        for row in self.history:  # DISTINCT ON (path) with id DESC keeps the last
            latest[row["path"]] = row
        rows = list(latest.values())
        key = "id" if last == ["id"] else "path"
        self.done.set()
        return sorted(rows, key=lambda row: row[key])


class _Pool:
    def __init__(self, history: List[dict], done: threading.Event):
        self.conn = _Conn(history, done)

    def acquire(self) -> Any:
        conn = self.conn

        class Context:
            async def __aenter__(self) -> Any:
                return conn

            async def __aexit__(self, *exc: Any) -> None:
                return None

        return Context()

    async def close(self) -> None:
        return None


def _restart(make, until, run, history_for) -> Any:
    """Runs a model with a PostgresIngress over a history, and returns the changes the
    registry received from the ingress, in the order it received them."""
    done = threading.Event()
    original = IngressMixIn.setup_ingress

    def setup(self: Any, providers: Any) -> None:
        """Starts the run only after the connector has read the history."""
        original(self, providers)
        done.wait(10)

    logging.disable(logging.CRITICAL)
    try:
        with (
            regression._restored_imports(),
            contextlib.chdir(ROOT),
            patch.dict(os.environ, {"PWD": str(ROOT)}),
            patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
            patch.object(IngressMixIn, "setup_ingress", setup),
        ):
            app = make()
            resources = app.compile_parameters().resources
            if not resources:
                pytest.skip("the model has no resource")
            name = next(iter(resources))
            config = resources[name]
            sim_id = app.sim_id
            history = history_for(f"{sim_id}.resources.{name}", config)

            async def create_pool(*args: Any, **kwargs: Any) -> Any:
                return _Pool(history, done)

            app.factor = 0.0
            app.go_live_at = None
            if app.random_seed is None:
                app.random_seed = 11
            app._sinks.egress = []
            app._sinks.ingress = [PostgresIngress("postgresql://u:p@h/db")]
            received: List[Any] = []
            update = SimulationRegistry.update

            def recording(self: Any, path: str, value: Any, *args: Any, **kw: Any):
                if kw.get("source") == "ingress":
                    received.append((path, value))
                return update(self, path, value, *args, **kw)

            with (
                patch(
                    "desflow.connectors.ingress.postgres.asyncpg.create_pool",
                    create_pool,
                ),
                patch.object(SimulationRegistry, "update", recording),
            ):
                app.run(until=until, **run)
            return received
    finally:
        logging.disable(logging.NOTSET)


@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_a_restart_gives_the_registry_the_live_runs_values(name, make, until, run):
    def history_for(prefix: str, config: Any) -> List[dict]:
        top = int(config.max_cap) + 4
        return [
            {"id": 1, "path": f"{prefix}.max_cap", "value": str(top)},
            {"id": 2, "path": f"{prefix}.current_cap", "value": str(top - 2)},
        ]

    received = _restart(make, until, run, history_for)
    paths = [path.rsplit(".", 1)[1] for path, _ in received]
    assert paths == ["max_cap", "current_cap"], f"{name}: {received}"
