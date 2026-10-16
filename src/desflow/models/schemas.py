from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field


class BaseStreamPayload(BaseModel):
    """
    Base schema for all egress data emitted by the simulation engine.

    This schema guarantees that all outgoing data streams share a common temporal context,
    allowing downstream systems to accurately synchronize simulation time with real-world time.

    Attributes:
        stream_type (Literal["telemetry", "event", "label"]): An identifier indicating the nature of the payload.
        sim_ts (float): The simulation clock time (in seconds) when the payload was generated.
        timestamp (str): The real-world ISO 8601 timestamp indicating when the payload was generated.
    """

    stream_type: Literal["telemetry", "event", "label"]
    sim_ts: float = Field(..., description="The simulation clock time (in seconds).")
    timestamp: str = Field(..., description="The real-world ISO 8601 timestamp.")


class TelemetryPayload(BaseStreamPayload):
    """
    Schema for low-volume, single-metric telemetry updates.

    This payload is used for publishing continuous system state variables, such as
    resource utilization, queue lengths, or system lag, typically used for real-time
    dashboards.

    Attributes:
        stream_type (Literal["telemetry"]): Hardcoded to "telemetry" for downstream routing.
        path_id (str): The dot-notation path of the metric (e.g., 'Shop.packers.utilization').
        value (Any): The scalar value of the metric at the given simulation time.
    """

    stream_type: Literal["telemetry"] = "telemetry"
    path_id: str = Field(
        ...,
        description="The dot-notation path of the metric (e.g., 'Shop.packers.utilization').",
    )
    value: Any = Field(..., description="The scalar value of the metric.")


class EventPayload(BaseStreamPayload):
    """
    Schema for high-volume discrete simulation events.

    This payload tracks state transitions of specific entities (like tasks or orders) throughout
    the simulation lifecycle. The `key` attribute allows message brokers like Kafka to maintain
    strict chronological ordering for specific tasks.

    Attributes:
        stream_type (Literal["event"]): Hardcoded to "event" for downstream routing.
        key (str): The entity key, the record and partition key (e.g., 'orders-1').
        task (Optional[str]): The name of the task the event belongs to, or None for
            an event published outside a task.
        instance (Optional[Any]): The task's instance id, or None.
        value (Dict[str, Any]): A dictionary containing the event's detailed payload.
    """

    stream_type: Literal["event"] = "event"
    key: str = Field(
        ...,
        description="The entity key, the record and partition key (e.g., 'orders-1').",
    )
    task: Optional[str] = Field(None, description="The task's name.")
    instance: Optional[Any] = Field(None, description="The task's instance id.")
    value: Any = Field(
        ..., description="A dictionary or Pydantic model containing the event payload."
    )


class LabelPayload(BaseStreamPayload):
    """
    Schema for ground-truth records: what a scenario step changed, and why.

    They are sent only to sinks with `when: labels`, which receive nothing else, so
    data and labels always reach separate sinks.

    Attributes:
        stream_type (Literal["label"]): Hardcoded to "label" for downstream routing.
        path (Optional[str]): The registry path the step changed, or None for a step
            with only a `record`.
        old (Any): The value before the step, or None.
        new (Any): The value after the step, or None.
        label (Dict[str, Any]): The step's `record` mapping.
    """

    stream_type: Literal["label"] = "label"
    path: Optional[str] = Field(None, description="The registry path the step changed.")
    old: Any = Field(None, description="The value before the step.")
    new: Any = Field(None, description="The value after the step.")
    label: Dict[str, Any] = Field(..., description="The step's record mapping.")
