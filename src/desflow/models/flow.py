"""What an entity carries through a flow of tasks, and what one task hands the next."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class Entity:
    """
    One thing moving through the model, such as an order, a slab or a session.

    Each arrival starts a new entity. Its key is the record key of every event it
    causes, in every task it passes through.

    Attributes:
        key (str): The record key: the arrival name and a counter, such as
            `orders-1`, unless the arrival has a key function.
        arrival (str): The arrival that started it, which names its flow in the
            statistics.
        attributes (Dict[str, Any]): State kept across tasks: set by the arrival's
            `attributes` and by each task's `set`, and read in conditions as
            `attr.<name>`. A payload includes them only when it asks for them.
    """

    key: str
    arrival: str
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Upstream:
    """
    What a finished task hands the task routed after it.

    Attributes:
        task_id (Any): The instance id of the finished task.
        key (str): The entity's key.
        attributes (Dict[str, Any]): The entity's attributes, after the finished
            task's `set`.
        payload (Any): The finished task's payload, read in conditions as
            `payload.<field>`.
    """

    task_id: Any
    key: str
    attributes: Dict[str, Any]
    payload: Any = None

    @property
    def fields(self) -> Dict[str, Any]:
        """The payload when it is a mapping, and an empty mapping otherwise."""
        return self.payload if isinstance(self.payload, dict) else {}


def payload_of(upstream: Optional[Upstream]) -> Dict[str, Any]:
    """
    Returns the payload a task received as a mapping, empty for the first task.

    Args:
        upstream: What the previous task handed on, or None.

    Returns:
        Dict[str, Any]: The upstream payload's fields.
    """
    return {} if upstream is None else upstream.fields
