"""Settings shared by more than one module.

The names here are part of the YAML format and the builder's arguments, so a value
changes only with an Upgrading line in the Changelog.
"""

# How a resource orders the requests waiting for it. `fifo` serves the oldest first,
# `lifo` the newest first, and `priority` the lowest task priority first, oldest
# first among equal priorities.
QUEUE_ORDERS = ("fifo", "lifo", "priority")
DEFAULT_QUEUE_ORDER = "fifo"

# What a task does when it is interrupted or preempted: `resume` queues again and
# serves the remaining time, `restart` queues again with a new service time, and
# `abandon` ends the task.
INTERRUPT_POLICIES = ("resume", "restart", "abandon")

# The built-in statistics levels, from none to every statistic. Each level publishes
# what the levels before it publish, and more.
STATS_LEVELS = ("none", "minimal", "basic", "typical", "all")
DEFAULT_STATS_LEVEL = "none"
# Simulation seconds between two publications of the built-in statistics.
DEFAULT_STATS_INTERVAL = 10.0

# The egress `when` value that makes a sink receive ground-truth records, and only
# those.
LABELS = "labels"
