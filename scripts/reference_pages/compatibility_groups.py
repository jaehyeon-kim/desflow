"""The heading and the introduction of each group of the compatibility register."""

from typing import Dict

# The heading of each group of the register.
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

# One or two sentences under each group's heading: what the group covers.
GROUP_INTROS: Dict[str, str] = {
    "C1": (
        "Waiting a fixed time, events a task can fire or fail, including an event "
        "that triggers once, and processes: starting one, with or without a delay "
        "and without waiting for it, waiting for it, reading its return value or "
        "its failure, being told when it ends, running actions when an event "
        "happens, and inspecting an event, a process or the running process while "
        "it runs."
    ),
    "C2": (
        "Waiting for the first of several events, or for all of them, as SimPy's "
        "`AnyOf` and `AllOf` conditions do, with conditions inside conditions, a "
        "rule of your own such as at least some of them (SimPy's condition with a "
        "count rule), a condition over no events, and a request, put or get as "
        "one of the events."
    ),
    "C3": (
        "Interrupting a task while it waits or while it is in service, and a "
        "higher-priority request taking a resource unit from a lower-priority user, "
        "with the cause that tells the user who took it and since when it held it."
    ),
    "C4": (
        "Requesting and releasing resource units, in order of arrival or of "
        "priority, with a default capacity of one unit or no limit, reading the "
        "count, users and queue, limiting the waiting line, cancelling a request "
        "before or after its grant, holding several resources at once, and "
        "resources of your own classes."
    ),
    "C5": (
        "Putting and getting items in a store and amounts in a container, with "
        "capacity limits or none, priority items of any orderable type, filtered "
        "gets, cancelled puts and gets, the order of puts and gets at the same "
        "time, reading what a store holds and its put and get queues, a put or get "
        "that the task does not wait for, and stores and containers of your own "
        "classes."
    ),
    "C6": (
        "Starting a run from a given time and state, reading the current time, "
        "ending it at a time, at an event or on a condition and reading the value "
        "that ended it, draining it, running it in segments, stepping through it "
        "one model event at a time, scheduling an event with a priority, an "
        "environment class of your own, and the pace of the run: its factor, "
        "strict pacing and syncing with the wall clock."
    ),
    "C7": (
        "Customers who give up after waiting too long (reneging) and customers "
        "who do not join a queue that is too long (balking)."
    ),
    "S1": "A resource that fails and is repaired, with the work it interrupts.",
    "S2": (
        "A schedule of times at which an arrival rate or a capacity changes during "
        "a run."
    ),
    "S3": "Arrivals read from a local CSV or Parquet file instead of drawn at random.",
}
