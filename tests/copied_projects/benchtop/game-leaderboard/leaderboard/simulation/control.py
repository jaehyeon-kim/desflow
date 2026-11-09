"""Changes a simulation parameter while the simulation runs.

The new value goes to the control topic, where desflow's Kafka ingress reads it and
updates the registry. The next arrival, round or share drawn uses it.
Run: python -m leaderboard.simulation.control game.variables.robot_share 0.5
"""

import argparse
import asyncio

from desflow import KafkaAdminConnector

from leaderboard.core.config import BOOTSTRAP_SERVERS, CONTROL_TOPIC

PARAMETERS = {
    "game.arrival.player.rate": "players arriving per second (default 0.5)",
    "game.service.session.mean": "mean session length in seconds (default 300)",
    "game.service.round.mean": "mean time between a person's rounds in seconds (default 6)",
    "game.service.robot_round.mean": "mean time between a robot's rounds in seconds (default 1.5)",
    "game.service.offline.mean": "mean time a late score is held in seconds (default 420)",
    "game.variables.robot_share": "share of arriving players who are robots (default 0.05)",
    "game.variables.late_share": "share of scores sent late (default 0.01)",
}  # fmt: skip

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="parameters:\n"
        + "\n".join(f"  {p}: {d}" for p, d in PARAMETERS.items()),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "path", choices=list(PARAMETERS), help="the parameter to change"
    )
    parser.add_argument("value", type=float, help="its new value")
    args = parser.parse_args()
    asyncio.run(KafkaAdminConnector(BOOTSTRAP_SERVERS).send_config(CONTROL_TOPIC, args.path, args.value))  # fmt: skip
    print(f"Sent {args.path} = {args.value} to {CONTROL_TOPIC}")
