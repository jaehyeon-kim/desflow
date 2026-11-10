"""A sink reports the batch it holds, so teardown does not cancel the last one.

Teardown ends a sink once its queue is empty and its `active_tasks` is 0. A batch
taken off the queue and still being sent was lost when neither said so.
"""

import asyncio
import queue
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from desflow.connectors.egress.kafka import KafkaEgress


async def _held_during(egress, entered, release, batch):
    """Runs `egress` over `batch`; the active count while sending and afterwards."""
    q = queue.Queue()
    q.put(batch)
    task = asyncio.create_task(egress.run(q))
    await asyncio.wait_for(entered.wait(), 5)
    during = egress.active_tasks
    release.set()
    for _ in range(100):
        if egress.active_tasks == 0:
            break
        await asyncio.sleep(0.01)
    after = egress.active_tasks
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    return during, after


def _gate():
    entered, release = asyncio.Event(), asyncio.Event()

    async def wait(*args, **kwargs):
        entered.set()
        await release.wait()

    return entered, release, wait


@pytest.mark.asyncio
@patch("desflow.connectors.egress.kafka.AIOKafkaProducer")
async def test_kafka_reports_the_batch_it_is_sending(producer):
    entered, release, wait = _gate()
    producer.return_value = AsyncMock(send=wait)
    egress = KafkaEgress("localhost:9092", topic_policy="manual")
    record = {"stream_type": "event", "key": "a", "value": 1}
    assert await _held_during(egress, entered, release, [record]) == (1, 0)


@pytest.mark.asyncio
async def test_redis_reports_the_batch_it_is_sending():
    from desflow.connectors.egress.redis import RedisEgress

    entered, release, wait = _gate()
    egress = RedisEgress("redis://localhost:6379", stream_name="s")
    pipe = MagicMock(execute=wait)
    egress.client = MagicMock(pipeline=MagicMock(return_value=pipe))
    egress._init_client = AsyncMock()
    record = {"stream_type": "event", "key": "a", "value": {"n": 1}}
    assert await _held_during(egress, entered, release, [record]) == (1, 0)


@pytest.mark.asyncio
async def test_postgres_reports_the_batch_it_is_sending():
    from desflow.connectors.egress.postgres import PostgresEgress

    entered, release, wait = _gate()
    conn = MagicMock(executemany=wait, fetch=AsyncMock(return_value=[]))
    acquire = MagicMock()
    acquire.__aenter__ = AsyncMock(return_value=conn)
    acquire.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock(acquire=MagicMock(return_value=acquire), close=AsyncMock())
    egress = PostgresEgress("postgresql://u:p@h/d", table_name="t")
    with patch(
        "desflow.connectors.egress.postgres.asyncpg.create_pool",
        AsyncMock(return_value=pool),
    ):
        task_args = {"n": 1}
        with patch.object(egress, "_column_runs", return_value=[(("n",), [task_args])]):
            record = {"stream_type": "event", "key": "a", "value": task_args}
            assert await _held_during(egress, entered, release, [record]) == (1, 0)
