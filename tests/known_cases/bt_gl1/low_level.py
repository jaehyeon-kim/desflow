"""BT-GL1 to BT-GL10: game-leaderboard.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The players arrive by a gap drawn with `Sampler`. Each
generator is the one of `builder.py`, given the place the document writes it at, and
read through the `ValueRuntime`, so it draws from the random sequence the document's
generator draws from. The teams are a mapping in the registry, changed as the
document's `on` does.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
    SimParameter,
)
from desflow.values import Template, ValueRuntime
from desflow.values.template import bind

from tests.known_cases.bt_gl1.builder import ARRIVALS, SERVICES, TASKS, VARIABLES
from tests.known_cases.bt_support import Entity, deep

UNTIL = 600
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}


def _params() -> SimParameter:
    return SimParameter(
        sim_id="game",
        arrival={"player": DistributionConfig(dist="exponential", rate=0.5)},
        service={name: DistributionConfig(**spec) for name, spec in SERVICES.items()},
        variables={
            name: (dict(v) if isinstance(v, dict) else v)
            for name, v in VARIABLES.items()
        },
    )


def __getattr__(name: str) -> Any:
    """`PARAMS`, new for each run: the registry changes the `teams` mapping."""
    if name == "PARAMS":
        return _params()
    raise AttributeError(name)


class _Run:
    """What the value runtime asks of a running model: its id and no signals."""

    sim_id = "game"
    _signals = None
    _resources_map = None
    _families = None
    _instance_scopes = None

    def __init__(self) -> None:
        self.values: Any = None

    def _levels(self) -> None:
        return None

    def _stop_run(self) -> None:
        pass


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    run = _Run()
    values = ValueRuntime(42, env, sampler, run)
    run.values = values
    arrival = ARRIVALS["player"]
    once = deep(arrival["vars"])
    bind(once, ("arrivals", "player", "vars"))
    key = deep(arrival["key"])
    bind(key, ("arrivals", "player", "key"))
    session_len = deep(arrival["attributes"]["session_len"])
    bind(session_len, ("arrivals", "player", "attributes", "session_len"))
    service_time = deep(TASKS["tick"]["service_time"])
    bind(service_time, ("tasks", "tick", "service_time"))
    join = Template(
        {"team": {"gen": "var", "name": "team"}},
        name="tasks.join_team",
        vars=TASKS["join_team"]["vars"],
    )
    tick = Template(
        {
            "is_late": {"gen": "var", "name": "is_late"},
            "score": {"gen": "var", "name": "score"},
        },
        name="tasks.tick",
        vars={k: v for k, v in TASKS["tick"]["vars"].items() if k != "ended"},
    )
    counts: dict[str, int] = {}
    late_config = env.registry.get_config("game.service.offline")

    def publish(player: Entity, task: str, payload: dict) -> None:
        env.publish_event(player.key, payload, task=task, instance=counts.get(task, 0))
        counts[task] = counts.get(task, 0) + 1

    def epoch_ms() -> int:
        return int((START + timedelta(seconds=env.now)).timestamp() * 1000)

    def teams() -> dict:
        return dict(env.registry.get("game.variables.teams").value)

    def late(player: Entity, score: int, earned: int, team: str) -> Any:
        yield env.timeout(sampler.sample(late_config))
        publish(
            player,
            "late",
            {
                "event_type": "late",
                "player": player.key,
                "score": score,
                "earned_ms": earned,
                "team": team,
            },
        )

    def session(player: Entity) -> Any:
        team = join.render(run, player)["team"]
        player.attributes["team"] = team
        counts_now = teams()
        counts_now[team] = counts_now.get(team, 0) + 1
        env.registry.update("game.variables.teams", counts_now)
        publish(player, "join_team", {"event_type": "joined", "team": team})
        while True:
            yield env.timeout(values.evaluate(service_time, values.scope_of(player)))
            drawn = tick.render(run, player)
            ended = (
                env.now - player.attributes["born"] > player.attributes["session_len"]
            )
            player.attributes["score"] = drawn["score"]
            publish(player, "tick", {})
            if drawn["is_late"] and not ended:
                env.process(late(player, drawn["score"], epoch_ms(), team))
            if ended:
                break
            if drawn["is_late"]:
                continue
            publish(
                player,
                "round",
                {
                    "player": player.key,
                    "score": drawn["score"],
                    "earned_ms": epoch_ms(),
                    "team": team,
                },
            )
        now = teams()
        if now[team] == 1:
            del now[team]
        else:
            now[team] -= 1
        env.registry.update("game.variables.teams", now)
        publish(player, "leave_team", {"event_type": "left", "team": team})

    def players() -> Any:
        config = env.registry.get_config("game.arrival.player")
        while True:
            yield env.timeout(sampler.sample(config))
            names = values.evaluate_vars(once, values.scope_of())
            player = Entity("")
            player.key = str(values.evaluate(key, values.scope_of(player, names=names)))
            player.attributes.update(
                robot=names["robot"],
                born=env.now,
                session_len=values.evaluate(
                    session_len, values.scope_of(player, names=names)
                ),
            )
            env.process(session(player))

    def scenario() -> Any:
        yield env.timeout(300)
        env.registry.update("game.variables.robot_share", 0.5)

    env.process(players())
    env.process(scenario())
