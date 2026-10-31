"""The YAML tab of each tutorial Result, and of each concept page that reads a tutorial's
run: the statistics `desflow run --stdout` prints.

Each block is what the commands on the page print: `desflow run --stdout` for each
setting, `grep -E` for the statistics the page states, and `tail` for the last of
them, since a run publishes its statistics at each interval and at the end.
"""

import os
import re
import tempfile
from pathlib import Path
from typing import Callable, Dict, List, Optional
from unittest import mock

from .capture import ROOT
from .commands import command

_YAML = "docs/snippets/yaml"


def readings(
    path: str,
    pattern: str,
    count: int,
    edit: Optional[Callable[[str], str]] = None,
    **variables: str,
) -> List[str]:
    """The last `count` lines of `desflow run --stdout` that match `pattern`.

    Args:
        path: The blueprint, relative to the repository.
        pattern: The extended regular expression, as `grep -E` takes it.
        count: How many of the last matching lines to keep, as `tail -n` would.
        edit: A change to the blueprint's text, for a setting the page makes by hand.
        **variables: The environment variables set for the run.

    Returns:
        List[str]: The lines, with each timestamp replaced by `...`.
    """
    with mock.patch.dict(os.environ, variables), tempfile.TemporaryDirectory() as tmp:
        if edit is not None:
            source = ROOT / path
            changed = Path(tmp) / source.name
            text = edit(source.read_text(encoding="utf-8"))
            assert text != source.read_text(encoding="utf-8"), (
                "the edit changed nothing"
            )
            changed.write_text(text, encoding="utf-8")
            # A blueprint names its data files from its own folder
            for other in source.parent.iterdir():
                if other.is_file() and not (Path(tmp) / other.name).exists():
                    (Path(tmp) / other.name).symlink_to(other)
            path = str(changed)
        lines = [
            line
            for line in command("run", path, "--stdout")
            if re.search(pattern, line)
        ]
    return [
        re.sub(r'"timestamp":"[^"]*"', '"timestamp":"..."', line)
        for line in lines[-count:]
    ]


def _each(path, pattern, count, name, values, edit=None):
    lines = []
    for value in values:
        variables = {} if value is None else {name: value}
        lines += readings(f"{_YAML}/{path}", pattern, count, edit, **variables)
    return lines


def queues_yaml() -> List[str]:
    """Queues and Capacity: the mean wait with 1 to 4 packers."""
    return _each(
        "queues_and_capacity.yaml",
        r'pack_order\.wait_mean"',
        1,
        "PACKERS",
        ["1", "2", "3", "4"],
    )


def end_yaml() -> List[str]:
    """End Conditions: the fixed horizon, then the drain."""
    return _each(
        "end_conditions.yaml",
        r'orders\.(started|finished|in_system)"|pack_order\.wait_mean"',
        4,
        "DRAIN",
        [None, "true"],
    )


def choosing_yaml() -> List[str]:
    """Choosing Inputs: the mean wait of each candidate packing time."""
    return _each(
        "choosing_inputs.yaml",
        r'pack_order\.wait_mean"',
        1,
        "PACKING",
        ["constant", "triangular", "empirical", "exponential"],
    )


def rush_yaml() -> List[str]:
    """Changing Demand: one packer in the rush, then two."""
    return _each(
        "changing_demand.yaml",
        r'pack_order\.(finished|wait_mean|wait_max)"',
        3,
        "RUSH_PACKERS",
        [None, "2"],
    )


def _modelled(text: str) -> str:
    start = text.index("    replay:")
    end = text.index("    spawn: pack_order", start)
    return text[:start] + "    dist: exponential\n    rate: 0.545\n" + text[end:]


def replay_yaml() -> List[str]:
    """Local Replay: the recorded morning, then exponential gaps at its mean rate."""
    path = f"{_YAML}/local_replay.yaml"
    pattern = r'pack_order\.(finished|wait_mean|wait_max)"'
    return readings(path, pattern, 3) + readings(path, pattern, 3, edit=_modelled)


def stock_yaml() -> List[str]:
    """Stock: deliveries of 60, then of 40."""
    return _each("stock.yaml", r'pack_order\.wait_mean"', 1, "DELIVERY", [None, "40"])


def courier_yaml() -> List[str]:
    """Process Interaction: a limit of 5 minutes, then of 8."""
    return _each(
        "process_interaction.yaml",
        r'(by_courier|by_post)\.finished"|dispatch\.wait_mean"',
        3,
        "LIMIT",
        [None, "8 min"],
    )


def patience_yaml() -> List[str]:
    """Patience: a patience of 30 seconds, then of an hour, which no order reaches."""
    return _each(
        "patience.yaml",
        r'pack_order\.(finished|reneged|balked|wait_mean)"',
        4,
        "PATIENCE",
        [None, "1 h"],
    )


def interrupt_yaml() -> List[str]:
    """Interruptions: resume, restart and abandon."""
    return _each(
        "interruptions.yaml",
        r'pack_order\.(preempted|interrupted|finished|wait_mean|service_mean)"',
        5,
        "POLICY",
        [None, "restart", "abandon"],
    )


def _frees_the_picker(text: str) -> str:
    return re.sub(r"\n +keep: \[picker\]", "", text, count=1)


def handoff_yaml() -> List[str]:
    """Competing Resources: without `keep`, then with it."""
    path = f"{_YAML}/competing_resources.yaml"
    pattern = r'(pick_order|pack_order)\.wait_mean"|picker\.utilization_avg"'
    return readings(path, pattern, 3, edit=_frees_the_picker) + readings(
        path, pattern, 3
    )


def filtered_yaml() -> List[str]:
    """Filtered Inventory: the orders filled for each size, and those cancelled."""
    return readings(
        f"{_YAML}/filtered_inventory.yaml",
        r'orders_[sml]\.finished"|take_shirt\.reneged"',
        4,
    )


def trusting_yaml() -> List[str]:
    """Trusting Results: the mean wait without the warm-up, then with it."""
    return _each(
        "trusting_results.yaml",
        r'pack_order\.wait_mean"',
        1,
        "WARMUP",
        ["0", None],
    )


def _fifo(text: str) -> str:
    text = text.replace(", queue: priority}", "}")
    return "\n".join(
        line for line in text.split("\n") if not line.strip().startswith("priority:")
    )


def priorities_yaml() -> List[str]:
    """Priorities: the mean wait of each kind of order under fifo, then priority."""
    path = f"{_YAML}/priorities.yaml"
    pattern = r'pack_(order|express)\.wait_mean"'
    return readings(path, pattern, 2, edit=_fifo) + readings(path, pattern, 2)


def littles_yaml() -> List[str]:
    """Queueing Basics: the statistics of Measuring a Run that Little's law needs."""
    return readings(
        f"{_YAML}/measuring_a_run.yaml",
        r'utilization_avg"|queue_length_avg"|(wait|service)_mean"|orders\.finished"',
        5,
    )


def streaming_rows() -> List[str]:
    """Streaming Out: the rows of each file the YAML run writes, smallest first."""
    from .commands import in_folder

    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        source = ROOT / _YAML / "streaming_out.yaml"
        for name in (source.name, "streaming_out_logic.py"):
            text = (ROOT / _YAML / name).read_text(encoding="utf-8")
            (folder / name).write_text(text)
        in_folder(folder, "run", source.name)
        counts = sorted(
            len([line for line in path.read_text().splitlines() if line])
            for path in (folder / "packed").glob("*.jsonl")
        )
    return [str(count) for count in counts]


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "queues-yaml": queues_yaml,
    "end-yaml": end_yaml,
    "choosing-yaml": choosing_yaml,
    "rush-yaml": rush_yaml,
    "replay-yaml": replay_yaml,
    "stock-yaml": stock_yaml,
    "courier-yaml": courier_yaml,
    "patience-yaml": patience_yaml,
    "interrupt-yaml": interrupt_yaml,
    "handoff-yaml": handoff_yaml,
    "filtered-yaml": filtered_yaml,
    "trusting-yaml": trusting_yaml,
    "streaming-rows": streaming_rows,
    "priorities-yaml": priorities_yaml,
    "littles-yaml": littles_yaml,
}
