"""The game as a desflow model, sending each score to Kafka in Avro.

Players arrive at random, and each plays a session of rounds; robots play far faster. A
score earned while a device is offline arrives minutes later, with the time it was
earned. Every rate, time and share is a registry parameter that `control` changes live.
Run: python -m leaderboard.simulation.run   (Ctrl + C to stop)
"""

import logging
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

from desflow import KafkaEgress, KafkaIngress, SimulationContext

from leaderboard.core.config import BOOTSTRAP_SERVERS, CONTROL_TOPIC, SIM_ID, TOPIC
from leaderboard.core.models import ScoreEvent
from leaderboard.simulation.game import Game
from leaderboard.stores import kafka

logger = logging.getLogger(
    "leaderboard.simulation.run"
)  # __name__ is "__main__" under -m


def _draw(ctx: SimulationContext, service: str) -> float:
    """Draws a duration from a service, with its live parameters."""
    config = ctx.env.registry.get_config(f"{SIM_ID}.service.{service}")
    return ctx.sampler.sample(config)  # type: ignore[union-attr]


def _share(ctx: SimulationContext, name: str) -> float:
    """Reads a share variable's live value."""
    return ctx.env.registry.get(f"{SIM_ID}.variables.{name}").value


def _send_late(ctx: SimulationContext, event: ScoreEvent) -> Iterator[Any]:
    """Holds a score while the player's device is offline, then sends it."""
    yield ctx.env.timeout(_draw(ctx, "offline"))
    ctx.env.publish_event(event.user_id, event)


def _session(
    ctx: SimulationContext, game: Game, player: str, robot: bool
) -> Iterator[Any]:
    """Plays one session: joins a team, plays rounds until the session ends, and leaves."""
    team = game.join(player)
    ends = ctx.env.now + _draw(ctx, "session")
    while True:
        yield ctx.env.timeout(_draw(ctx, "robot_round" if robot else "round"))
        if ctx.env.now > ends:
            break
        earned = ctx.env.start_datetime + timedelta(seconds=ctx.env.now)
        late = game.rng.random() < _share(ctx, "late_share")
        event = ScoreEvent(
            user_id=player,
            team_id=team.team_id,
            team_name=team.name,
            score=int(game.rng.integers(0, 21)),
            event_time_millis=int(earned.timestamp() * 1000),
            event_type="late" if late else "normal",
        )
        if late:
            ctx.spawn(_send_late(ctx, event))
        else:
            ctx.env.publish_event(player, event)
    game.leave(team, player)


def build(app: SimulationContext) -> None:
    """
    Adds the game's parameters and processes to a simulation.

    Args:
        app (SimulationContext): The simulation, before it runs.
    """
    app.add_arrival("player", dist="exponential", rate=0.5)  # a player every 2 s
    app.add_service("session", dist="lognormal", mean=300, std=120)  # 5 minutes
    app.add_service("round", dist="lognormal", mean=6, std=2)
    app.add_service("robot_round", dist="normal", mean=1.5, std=0.3)
    app.add_service("offline", dist="normal", mean=420, std=120)  # 7 minutes
    app.add_variable("robot_share", 0.05)
    app.add_variable("late_share", 0.01)

    @app.arrival_loop("player")
    def players(ctx: SimulationContext) -> Iterator[Any]:
        game = Game(ctx.sampler.rng)  # type: ignore[union-attr]
        count = 0
        while True:
            yield ctx.wait_for_arrival("player")
            count += 1
            robot = game.rng.random() < _share(ctx, "robot_share")
            ctx.spawn(
                _session(ctx, game, f"{'RBT' if robot else 'USR'}-{count:06}", robot)
            )


def run() -> None:
    """Runs the game in real time, sending each score to Kafka, until stopped."""
    kafka.create_topics()
    app = SimulationContext(SIM_ID, factor=1.0)
    build(app)
    app.add_ingress(
        KafkaIngress(topic=CONTROL_TOPIC, bootstrap_servers=BOOTSTRAP_SERVERS)
    )
    app.add_egress(
        KafkaEgress(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            event_topic=TOPIC,
            default_serializer=kafka.ScoreSerializer(),
        ),
        when=lambda r: r["stream_type"] == "event",
        batch_size=25,  # about a second of scores
    )
    logger.info("Sending scores to %s; Ctrl + C to stop", TOPIC)
    app.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per registry call
    run()
