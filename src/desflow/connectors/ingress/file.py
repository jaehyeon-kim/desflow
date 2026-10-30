import asyncio
import logging
import queue
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml

from desflow.connectors.ingress.base import BaseIngress, parse_change

logger = logging.getLogger(__name__)


class FileIngress(BaseIngress):
    """
    Ingress provider that applies the entries of a local YAML file when you edit it.

    The file is a mapping of registry paths to values, one `path: value` line each:

        Shop.resources.packers.current_cap: 3
        Shop.arrival.orders.rate: 0.5

    The connector needs no service and no extra. It checks the file's modification
    time every `poll` wall-clock seconds and reads the file only when the time has
    changed. The file as it stands when the run starts counts as a first read.

    An entry is applied when it first appears or when its value differs from the
    last valid read. An entry that is removed changes nothing. A file that does not
    exist yet, or is empty, is waited for without a log line. A file that is not valid
    YAML, or is not a mapping, is logged as one WARNING line and skipped, and the next valid read
    is compared with the last valid one. An entry with an empty path or a null value
    is logged as one WARNING line and skipped, as the other ingress connectors do.

    Attributes:
        path (Path): The file to watch.
        poll (float): Wall-clock seconds between checks of the file's modification
            time.
    """

    def __init__(self, path: Union[str, Path], poll: float = 1.0):
        """
        Initializes the FileIngress.

        Args:
            path: The YAML file to watch. A relative path is relative to the
                working directory of the run.
            poll: Wall-clock seconds between checks of the file's modification
                time. The default is 1.0 second, so a saved edit lands within
                about one second; pass a smaller value for a faster reaction.
        """
        self.path = Path(path)
        self.poll = poll

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Watches the file and puts each new or changed entry on the queue.

        Args:
            ingress_queue: A thread-safe queue used to transmit the (path, value)
                updates to the SimulationRegistry.
        """
        modified: Optional[int] = None
        previous: Dict[Any, Any] = {}
        try:
            while True:
                try:
                    current = self.path.stat().st_mtime_ns
                except FileNotFoundError:
                    # Waited for without a log line; a file created later is read
                    modified = None
                else:
                    if current != modified:
                        modified = current
                        entries = self._read()
                        if entries is not None:
                            self._put_changes(entries, previous, ingress_queue)
                            previous = entries
                await asyncio.sleep(self.poll)
        except asyncio.CancelledError:
            pass

    def _read(self) -> Optional[Dict[Any, Any]]:
        """Returns the file's mapping, or None after logging why it is not valid."""
        try:
            data = yaml.safe_load(self.path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
            logger.warning(f"Skipped the ingress file {self.path}: {exc}")
            return None
        if data is None:
            # An empty file, which an editor can leave for a moment while it saves,
            # holds no entries and is not an error.
            return None
        if not isinstance(data, dict):
            logger.warning(
                f"Skipped the ingress file {self.path}: expected a mapping of"
                f" paths to values, got {type(data).__name__}"
            )
            return None
        return data

    def _put_changes(
        self,
        entries: Dict[Any, Any],
        previous: Dict[Any, Any],
        ingress_queue: queue.Queue,
    ) -> None:
        """Puts the entries that are new or differ from the last valid read."""
        for path, value in entries.items():
            if path in previous and previous[path] == value:
                continue
            try:
                ingress_queue.put(parse_change({"path": path, "value": value}))
            except ValueError as exc:
                logger.warning(
                    f"Skipped an entry of the ingress file {self.path}: {exc}"
                )
