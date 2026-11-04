"""GA-7: A day of history is written in seconds, then events continue in real time.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The start is a day ago and the live tail begins now.
"""

import itertools
from datetime import datetime, timedelta, timezone

from desflow import DynamicRealtimeEnvironment

UNTIL = 86403
NOW = datetime.now(timezone.utc)
ENVIRONMENT = {"logical_start_time": NOW - timedelta(days=1), "go_live_at": NOW}


def build(env: DynamicRealtimeEnvironment) -> None:
    def source():
        for number in itertools.count(1):
            yield env.timeout(1)
            env.publish_event(
                f"visits-{number}",
                {"event_type": "visit"},
                task="visit",
                instance=number - 1,
            )

    env.process(source())
