import asyncio
import json
import queue
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from desflow.connectors.admin.kafka import KafkaAdminConnector
from desflow.connectors.ingress.kafka import KafkaIngress


def test_admin_process_telemetry():
    """Verify the admin connector properly routes telemetry messages to _vitals."""
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    data = {
        "path_id": "Shop.packers.utilization",
        "value": 99.9,
        "timestamp": "2023-01-01T00:00:00",
    }
    admin._process_message(data)
    assert admin.get_vitals()["Shop.packers.utilization"] == 99.9


def test_admin_process_events_and_pruning():
    """Verify the admin connector routes events and properly prunes old tasks."""
    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092", max_tasks=2)

    # Send 3 tasks for the same service
    for i in range(3):
        data = {
            "key": f"task-{i}",
            "value": {"path_id": "Shop.service.packing", "status": "started"},
            "timestamp": f"2023-01-01T00:00:0{i}",
        }
        admin._process_message(data)

    state = admin.get_state()
    service_tasks = state["Shop"]["packing"]

    # Since max_tasks is 2, task-0 should have been pruned to prevent memory leaks
    assert len(service_tasks) == 2
    assert "task-0" not in service_tasks
    assert "task-1" in service_tasks
    assert "task-2" in service_tasks


@pytest.mark.asyncio
@patch("desflow.connectors.ingress.kafka.AIOKafkaConsumer")
@patch("desflow.connectors.admin.kafka.AIOKafkaProducer")
async def test_send_config_message_is_read_by_kafka_ingress(MockProducer, MockConsumer):
    """send_config writes {"path": ..., "value": ...}, and KafkaIngress reads it back."""
    producer = MockProducer.return_value
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send_and_wait = AsyncMock()

    admin = KafkaAdminConnector(bootstrap_servers="localhost:9092")
    await admin.send_config("sim-config", "Shop.resources.packers.current_cap", 4)

    topic, payload = producer.send_and_wait.call_args.args
    assert topic == "sim-config"
    assert json.loads(payload) == {
        "path": "Shop.resources.packers.current_cap",
        "value": 4,
    }

    class FakeConsumer:
        def __init__(self):
            self.start = AsyncMock()
            self.stop = AsyncMock()

        async def __aiter__(self):
            yield MagicMock(topic=topic, value=payload)
            await asyncio.sleep(999)

    MockConsumer.return_value = FakeConsumer()
    ingress_queue = queue.Queue()
    task = asyncio.create_task(
        KafkaIngress(topic, bootstrap_servers="localhost:9092").run(ingress_queue)
    )
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert ingress_queue.get_nowait() == ("Shop.resources.packers.current_cap", 4)
