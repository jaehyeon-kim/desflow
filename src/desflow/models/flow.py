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
        held (Dict[str, Any]): Resource units a task kept for the next task with
            `keep`, by resource name; the next task gives them back once it starts.
    """

    key: str
    arrival: str
    attributes: Dict[str, Any] = field(default_factory=dict)
    # Units a task kept for the next task of the flow, by resource name
    held: Dict[str, Any] = field(default_factory=dict, repr=False, compare=False)
    # Whether its first task has started, so it counts as in the model
    entered: bool = field(default=False, repr=False, compare=False)
    # The running instances of its tracked tasks, by task name, for a `running`
    # wait or a `subscribe`; None until the first is listed
    instances: Optional[Dict[str, Any]] = field(default=None, repr=False, compare=False)
    # The entity whose flow spawned this one, so a `running` wait on the spawner
    # sees the instances it started; None for an arrival or a spawn from a signal
    spawned_by: Optional["Entity"] = field(default=None, repr=False, compare=False)
    # End events still to be processed before the entity leaves (Counting out)
    ends_pending: int = field(default=0, repr=False, compare=False)
    # Whether the entity's own flow has ended, so no new own-flow instance comes
    ending: bool = field(default=False, repr=False, compare=False)
    # The end event of the last-ended instance of each copied task, for `fire from`
    last_ended: Optional[Dict[str, Any]] = field(
        default=None, repr=False, compare=False
    )

    @property
    def left(self) -> bool:
        """True once `_entity_left` has run: the flow has ended and nothing is
        pending."""
        return self.ending and self.ends_pending == 0


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
