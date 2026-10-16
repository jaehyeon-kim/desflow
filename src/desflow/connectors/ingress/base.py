import queue
from typing import Any, Tuple

_OLD_KEYS = ("path_id", "param_path", "param_value")


def parse_change(data: Any) -> Tuple[str, Any]:
    """
    Reads a parameter change from a decoded ingress message.

    Every ingress connector takes a change as a mapping with `path`, the registry
    path, and `value`, its new value: `{"path": "Shop.arrival.orders.rate",
    "value": 10.0}`. These are the names a scenario step uses.

    Args:
        data: The decoded message, such as a parsed JSON object or a table row.

    Returns:
        The `(path, value)` tuple the SimulationRegistry applies.

    Raises:
        ValueError: If `data` is not a mapping, `path` is not a non-empty string,
            or `value` is missing or null. The message names the old keys when
            `data` uses them.
    """
    if not isinstance(data, dict):
        raise ValueError(
            f"expected an object with path and value, got {type(data).__name__}"
        )
    path = data.get("path")
    value = data.get("value")
    if not isinstance(path, str) or not path or value is None:
        reason = f"expected a non-empty path and a non-null value, got {data!r}"
        # A sender written for desflow 0.20.0 or earlier still uses the old keys.
        if any(key in data for key in _OLD_KEYS):
            reason += (
                "; path_id, param_path and param_value were renamed to path and"
                " value in desflow 0.21.0"
            )
        raise ValueError(reason)
    return path, value


class BaseIngress:
    """
    Base class for all ingress providers in the simulation.

    Ingress providers act as asynchronous listeners that bridge external
    data sources (such as Kafka topics, Redis channels, or local schedules)
    with the simulation's internal state. They are responsible for fetching
    updates and placing them into a thread-safe queue for the registry to process.
    """

    async def run(self, ingress_queue: queue.Queue) -> None:
        """
        Listens to an external source and pushes updates to the ingress queue.

        This method should be implemented as an asynchronous loop that waits
        for external signals or data and converts them into (path, value)
        tuples suitable for the SimulationRegistry.

        Args:
            ingress_queue: A thread-safe queue used to transmit updates
                to the main simulation environment.

        Raises:
            NotImplementedError: If the subclass does not override this method.
        """
        raise NotImplementedError("Subclasses must implement the run method.")
