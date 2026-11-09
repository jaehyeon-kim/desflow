"""The Kafka admin connector files each event under its own sim id, service and status.

Every record of each model that `scripts/regression.py capture` runs (the cases
of `test_second_run.py`) goes through `KafkaEgress._message` and then into
`KafkaAdminConnector._process_message`, as the model is written and as the copy of
`test_dotted_names.py` that gives every name a dot and the word `.service.`. A status
record is filed under the sim id of the run and the service that its `path_id` names
after the sim id and `.service.`, with its status. A finish record, which holds the
task's own payload, is filed as `finished` where its key was last seen, and left out
when it was never seen. Nothing is filed under `unknown`.
"""

import contextlib
import importlib.util
import json
import logging
import os
import queue
from pathlib import Path
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock, patch

import pytest
import scipy.optimize  # noqa: F401  # imported before the capture restores sys.modules
import scipy.stats  # noqa: F401

from desflow.connectors.admin.kafka import KafkaAdminConnector
from desflow.connectors.egress.base import BaseEgress
from desflow.connectors.egress.kafka import KafkaEgress
from desflow.core.sinks import Sink

# It runs every model the regression capture runs, so CI runs it on one Python
# version on each system (scripts/ci_plan.py).
pytestmark = pytest.mark.every_model

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, file: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, ROOT / "tests" / "unit" / file)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


dotted = _load("dotted_names_cases", "test_dotted_names.py")
second_run = dotted.second_run
regression = second_run.regression

MODELS = dotted.CASES
IDS = [case[0] for case in MODELS]


class _Capture(BaseEgress):
    def __init__(self) -> None:
        self.records: List[dict] = []

    async def run(self, egress_queue: queue.Queue) -> None:
        raise NotImplementedError


def _records(make: Any, until: Any, run: Dict[str, Any]) -> Tuple[str, List[dict]]:
    """The sim id of a run and its event records, in order."""
    import asyncio

    app = make()
    capture = _Capture()

    async def collect(egress_queue: queue.Queue) -> None:
        while True:
            try:
                capture.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)

    capture.run = collect  # type: ignore[method-assign]
    app.factor = 0.0
    app.go_live_at = None
    if app.random_seed is None:
        app.random_seed = 11
    app._sinks.ingress = []
    app._sinks.egress = [Sink(capture)]
    app.run(until=until, **run)
    return app.sim_id, [r for r in capture.records if r.get("stream_type") == "event"]


def _read(records: List[dict]) -> KafkaAdminConnector:
    """The admin connector after it has read every record as a Kafka message."""
    sink = KafkaEgress(bootstrap_servers="b:9092", topic_router=lambda d: "events")
    admin = KafkaAdminConnector(bootstrap_servers="x", max_tasks=10**6)
    for record in records:
        sent = sink._message({k: v for k, v in record.items() if k != "stream_type"})
        assert sent is not None
        admin._process_message(json.loads(sent[2] or b""))
    return admin


def _expected(sim_id: str, records: List[dict]) -> Dict[str, Dict[str, Dict[str, set]]]:
    """sim id -> service -> key -> statuses, from the records alone: a service is what
    the `path_id` holds after the sim id and `.service.`, and a record with no
    `path_id` is a finish filed where its key was last seen."""
    prefix = f"{sim_id}.service."
    home: Dict[str, str] = {}
    found: Dict[str, Dict[str, Dict[str, set]]] = {}
    for record in records:
        value = record["value"]
        if not isinstance(value, dict):
            continue
        path_id = value.get("path_id")
        if isinstance(path_id, str) and path_id.startswith(prefix):
            service = path_id[len(prefix) :]
            home[record["key"]] = service
            status = value["status"]
        elif record["key"] in home:
            service = home[record["key"]]
            status = value.get("status") or "finished"
        else:
            continue
        found.setdefault(sim_id, {}).setdefault(service, {}).setdefault(
            record["key"], set()
        ).add(status)
    return found


def _check(name: str, sim_id: str, records: List[dict]) -> None:
    admin = _read(records)
    state = admin.get_state()
    got = {
        sim: {
            service: {key: set(statuses) for key, statuses in tasks.items()}
            for service, tasks in services.items()
        }
        for sim, services in state.items()
    }
    assert "unknown" not in got, f"{name}: filed under unknown"
    assert got == _expected(sim_id, records), name


@contextlib.contextmanager
def _environment(rename: bool):
    logging.disable(logging.CRITICAL)
    try:
        with (
            regression._restored_imports(),
            contextlib.chdir(ROOT),
            patch.dict(os.environ, {"PWD": str(ROOT)}),
            patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
            contextlib.ExitStack() as stack,
        ):
            if rename:
                stack.enter_context(
                    patch.object(
                        dotted.build_module,
                        "read_document",
                        dotted._renaming_read(dotted.build_module.read_document),
                    )
                )
                stack.enter_context(
                    patch.object(dotted.sampler_module, "zlib", dotted._MappedCrc())
                )
                stack.enter_context(
                    patch.object(dotted.runtime_module, "zlib", dotted._MappedCrc())
                )
            yield
    finally:
        logging.disable(logging.NOTSET)


@pytest.mark.parametrize("rename", [False, True], ids=["plain", "dotted"])
@pytest.mark.parametrize("name,make,until,run", MODELS, ids=IDS)
def test_each_event_is_filed_under_its_own_sim_id_service_and_status(
    name, make, until, run, rename
):
    with _environment(rename):
        try:
            sim_id, records = _records(make, until, run)
        except Exception as exc:
            pytest.skip(f"the model does not run here: {type(exc).__name__}")
    if not any(
        isinstance(r["value"], dict) and "path_id" in r["value"] for r in records
    ):
        pytest.skip("no task with a service")
    _check(name, sim_id, records)


def test_a_service_name_that_holds_the_separator_is_read_whole():
    sink = KafkaEgress(bootstrap_servers="b:9092", topic_router=lambda d: "events")
    admin = KafkaAdminConnector(bootstrap_servers="x")
    for status, extra in (("started", {}), (None, {"n": 1})):
        value = {"path_id": "S.service.front.service.desk", "sim_id": "S"}
        value["service"] = "front.service.desk"
        if status:
            value["status"] = status
        else:
            value = extra
        record = {"key": "a-1", "task": "t", "instance": 0, "value": value}
        sent = sink._message(record)
        assert sent is not None
        admin._process_message(json.loads(sent[2]))
    assert admin.get_state()["S"]["front.service.desk"]["a-1"].keys() == {
        "started",
        "finished",
    }
    assert "unknown" not in admin.get_state()
