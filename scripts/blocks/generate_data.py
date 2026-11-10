"""The blocks of the Generate Data pages: the records of a snippet's blueprint."""

import json
from typing import Callable, Dict, List

from .commands import sample

_FOLDER = "docs/snippets/generate-data"


def _first(slug: str, events: int) -> Callable[[], List[str]]:
    """The `events` events that `desflow run --sample` prints first."""

    def lines() -> List[str]:
        return sample(f"{_FOLDER}/{slug}.yaml", events)

    return lines


def _chosen(slug: str, events: int, wanted: Callable[[dict], bool]) -> Callable:
    """The records of the first `events` events that `wanted` keeps, in the order
    `desflow run --sample` prints them."""

    def lines() -> List[str]:
        return [
            line
            for line in sample(f"{_FOLDER}/{slug}.yaml", events)
            if wanted(json.loads(line))
        ]

    return lines


def _look_up() -> List[str]:
    """The first order, the payment that follows it, and the first report."""
    records = sample(f"{_FOLDER}/look_up_other_records.yaml", 40)
    parsed = [json.loads(line) for line in records]
    picked = {"order": "orders-1", "payment": "orders-1", "report": "reports-1"}
    return [
        line
        for line, record in zip(records, parsed)
        if picked.get(record.get("task")) == record.get("key")
    ]


def _join() -> List[str]:
    """The first customer and the first two orders, each from its own task."""
    records = sample(f"{_FOLDER}/join_across_sinks.yaml", 8)
    counts: Dict[str, int] = {}
    kept = []
    for line in records:
        task = json.loads(line)["task"]
        counts[task] = counts.get(task, 0) + 1
        if counts[task] <= {"customer": 1, "order": 2}[task]:
            kept.append(line)
    return kept


def _schema_orders(record: dict) -> bool:
    """An order next to each change, and every label."""
    if record["stream_type"] == "label":
        return True
    return record["value"]["order_id"] in (5, 6, 11, 12, 17, 18)


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "gd-send-data-to-a-sink": _first("send_data_to_a_sink", 5),
    "gd-realistic-fields": _first("realistic_fields", 3),
    "gd-choices-and-numbers": _first("choices_and_numbers", 5),
    "gd-timestamps-and-gaps": _first("timestamps_and_gaps", 3),
    "gd-stop-after-a-count": _first("stop_after_a_count", 5),
    "gd-many-entities": _first("many_entities", 3),
    "gd-many-entities-named": _first("many_entities_named", 4),
    "gd-build-on-the-last-value": _first("build_on_the_last_value", 6),
    "gd-skewed-keys": _first("skewed_keys", 6),
    "gd-change-state-over-time": _first("change_state_over_time", 8),
    "gd-look-up-other-records": _look_up,
    "gd-join-across-sinks": _join,
    "gd-change-the-schema": _chosen(
        "change_the_schema_during_a_run", 19, _schema_orders
    ),
}
