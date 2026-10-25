"""Python for write_a_connector.yaml: an egress that writes events to a CSV file and
an ingress that follows a JSON Lines file of changes, as the Write a Connector guide
builds them."""

import asyncio
import csv
import json
import logging
import queue
from pathlib import Path
from typing import List

from desflow.connectors.egress.base import BaseEgress
from desflow.connectors.ingress.base import BaseIngress, parse_change

logger = logging.getLogger(__name__)


# --8<-- [start:egress]
class CsvEgress(BaseEgress):
    """Writes one CSV row per event: its time, key and task, then the payload
    fields that `columns` names."""

    def __init__(self, path: str, columns: List[str]):
        self.path = Path(path)
        self.columns = list(columns)
        self.active_tasks = 0  # teardown waits while a write is in progress

    async def run(self, egress_queue: queue.Queue) -> None:
        with self.path.open("w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["sim_ts", "key", "task", *self.columns])
            while True:
                try:
                    batch = egress_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.1)  # let the other connectors run
                    continue
                self.active_tasks += 1
                try:
                    # A batch also holds telemetry; this sink writes only the events
                    rows = [
                        self._row(record)
                        for record in batch
                        if record["stream_type"] == "event"
                    ]
                    # A file write blocks, so it runs on a worker thread
                    await asyncio.to_thread(self._write, file, writer, rows)
                finally:
                    self.active_tasks -= 1

    def _row(self, record: dict) -> list:
        value = record["value"] if isinstance(record.get("value"), dict) else {}
        fields = [value.get(column) for column in self.columns]
        return [record["sim_ts"], record.get("key"), record.get("task"), *fields]

    @staticmethod
    def _write(file, writer, rows: list) -> None:
        writer.writerows(rows)
        file.flush()


# --8<-- [end:egress]


# --8<-- [start:ingress]
class JsonlIngress(BaseIngress):
    """Follows a JSON Lines file and applies each line it adds, such as
    `{"path": "Shop.resources.packers.current_cap", "value": 2}`."""

    def __init__(self, path: str, poll: float = 0.1):
        self.path = Path(path)
        self.poll = poll

    async def run(self, ingress_queue: queue.Queue) -> None:
        read = 0  # the lines already applied
        while True:
            if self.path.exists():
                # A file read blocks, so it runs on a worker thread
                text = await asyncio.to_thread(self.path.read_text)
                # Only whole lines: a writer may be partway through the last one
                lines = text[: text.rfind("\n") + 1].splitlines()
                for line in lines[read:]:
                    try:
                        ingress_queue.put(parse_change(json.loads(line)))
                    except ValueError as error:  # also a line that is not JSON
                        logger.warning("Skipped a line of %s: %s", self.path, error)
                read = len(lines)
            await asyncio.sleep(self.poll)


# --8<-- [end:ingress]
