"""The heading of each group of the compatibility register."""

from typing import Dict

# The heading of each group of the register, which the tests read; the page shows
# SimPy's areas, not these groups.
GROUPS: Dict[str, str] = {
    "C1": "Events and Processes",
    "C2": "Wait for One or All",
    "C3": "Interrupts and Preemption",
    "C4": "Resources",
    "C5": "Stores and Containers",
    "C6": "Run Control",
    "C7": "Patience",
    "S1": "Breakdowns",
    "S2": "Schedules",
    "S3": "Replay",
    "D": "Differences Kept by Design",
}
