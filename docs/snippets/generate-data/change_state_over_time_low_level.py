"""Change state over time: stations move between three states, low-level API.

The low-level version of `change_state_over_time.yaml`: plain SimPy on a
`DynamicRealtimeEnvironment`. Each station is one process that holds a state for its
dwell time, publishes the record of the state, and asks a `Router` for the next state.

Needs: nothing.
Run it with: python docs/snippets/generate-data/change_state_over_time_low_level.py
"""

import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DynamicRealtimeEnvironment,
    Router,
    Sampler,
    Upstream,
)

UNTIL = 7200
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT: dict = {"logical_start_time": START, "timezone": "UTC"}

# --8<-- [start:states]
DWELL = {"running": 300, "slow": 120, "stopped": 240}  # seconds in each state
NEXT = {
    "running": {"weights": {"running": 85, "slow": 15}},
    "slow": {"weights": {"running": 55, "slow": 25, "stopped": 20}},
    "stopped": "slow",
}
# --8<-- [end:states]


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the three stations on `env`."""
    # --8<-- [start:arrival]
    sampler = Sampler(seed=42)
    routers = {
        state: Router(rule, rng=sampler.generator(f"route.{state}"))
        for state, rule in NEXT.items()
        if isinstance(rule, dict)
    }
    counts = {state: 0 for state in DWELL}
    # --8<-- [end:arrival]

    # --8<-- [start:payload]
    def station(number):
        key = f"station-{number}"
        state = "running"
        while True:
            instance = counts[state]
            counts[state] += 1
            env.publish_event(key, {"status": "started"}, task=state, instance=instance)
            yield env.timeout(DWELL[state])
            env.publish_event(
                key, {"station": key, "state": state}, task=state, instance=instance
            )
            if state in routers:
                state = routers[state].choose(
                    Upstream(task_id=0, key=key, attributes={})
                )
            else:
                state = NEXT[state]

    for number in range(1, 4):
        env.process(station(number))
    # --8<-- [end:payload]


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
