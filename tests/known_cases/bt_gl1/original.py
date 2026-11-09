"""BT-GL1 to BT-GL10: the game-leaderboard simulation.

The original is the project's own model, `leaderboard.simulation.run.build` and its
`Game` (its copy in `tests/copied_projects/benchtop/game-leaderboard`, read only), run unpaced to the document's end with a stand-in sampler that
gives fixed draws, and with `publish_event` recorded. Its imports of `confluent_kafka`
and `httpx`, which only its sinks need, are answered with stand-in modules.

The fixed draws are numbers `(player * a + round * b) % 1` of the player's number and
the round, with its own `a` and `b` for each draw, and every duration is the mean of its
service. The YAML takes the same numbers (`fix`), so the two run the same game. A draw
that depends on the order of the program's own calls (the project draws a team's
chance only when a team has room) depends on the player instead.

`compare` checks that the YAML's scores equal the project's, in time, player, team,
score, earned time and kind, and that the run reaches what the rows name: robots and
people, a robot share that changed live, teams that formed and dissolved, and late
scores.
"""

import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest import mock

from desflow import SimulationContext
from desflow.blueprint.loader import read_document
from desflow.connectors.egress.kafka import ConfluentAvroSerializer
from desflow.core.sampler import Sampler
from desflow.core.environment import DynamicRealtimeEnvironment

from tests.known_cases.bt_support import (
    in_project,
    fix_draws,
    sequence_of,
    stand_in_modules,
)

HERE = Path(__file__).parent
VARIABLES = {"FACTOR": "0"}
UNTIL = 600
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
CONFIGS = [{"variables": {}, "args": {}}]
# the constants of the fixed draws: (a, b, c) of `(player * a + round * b + c) % 1`
SITES = {
    "robot": (0.6180339887, 0.0, 0.0),
    # the first 19 players find no reason to form a team, so the first team fills to 15
    # and the 16th player finds it full
    "new_team": (0.025, 0.0, 0.5),
    "open": (0.5698402909, 0.0, 0.0),
    "free": (0.3247179572, 0.0, 0.0),
    "late": (0.4142135623, 0.7320508075, 0.0),
    "score": (0.2360679775, 0.4494897427, 0.0),
}
PAUSED_AT = 61  # the gap after the 60th player is a quiet time, so every team dissolves
PAUSE = 402.0
SERVICES = {
    300: 300.0,
    6: 6.0,
    1.5: 1.5,
    420: 420.0,
}  # a service's mean to its fixed draw
TEAM_SIZE = 15


def _draw(site: str, player: int, round_: int = 0) -> float:
    """The fixed draw of a site for a player and a round."""
    a, b, c = SITES[site]
    return (player * a + round_ * b + c) % 1.0


def _fraction(path: tuple[Any, ...]) -> dict[str, Any]:
    """The YAML's form of `_draw` for the node at `path`: the players' number comes from
    a count of the arrivals, or from the entity's key; a round from the entity's ticks."""
    site = {
        "robot": "robot",
        "form_new": "new_team",
        "u1": "open",
        "u2": "free",
        "is_late": "late",
        "score": "score",
    }[path[-1]]
    a, b, c = SITES[site]
    if path[1] == "player":
        return {
            "gen": "expr",
            "expr": f"(fix_player * {a}) % 1.0",
            "names": {"fix_player": sequence_of(1)},
        }
    return {
        "gen": "expr",
        "expr": f"(fix_player * {a} + fix_round * {b} + {c}) % 1.0",
        "names": {
            "fix_player": {
                "gen": "expr",
                "expr": "int(e[-6:])",
                "names": {"e": {"gen": "entity"}},
            },
            "fix_round": {"gen": "expr", "expr": "attr.ticks + 1"},
        },
    }


def fix(document: dict[str, Any]) -> dict[str, Any]:
    """The document with its draws fixed (`fix_draws`): each draw node takes the value of
    `_draw` from its own chance or bounds, constant gaps and durations. The rounds of a
    player are counted in an attribute `ticks`, which the draws of a round read.

    Args:
        document: The loaded `model.yaml`.

    Returns:
        dict[str, Any]: The same document with the draws fixed.

    Raises:
        AssertionError: When the gap or a service differs from the project's in its
            distribution, rate, mean or spread, or a variable of the project's has
            another value in the document.
    """
    arrivals, services, variables = _settings()
    arrival = document["arrivals"]["player"]
    own = arrivals["player"]
    assert (arrival.pop("dist"), arrival["rate"]) == (own.dist, own.rate), (
        "the document's arrival player is not the project's"
    )
    arrival["gap"] = {
        "gen": "expr",
        "expr": f"{PAUSE} if n == {PAUSED_AT} else {1.0 / arrival.pop('rate')}",
        "names": {"n": {"gen": "sequence", "start": 1}},
    }
    arrival["attributes"]["ticks"] = 0
    for name, service in document["services"].items():
        own = services[name]
        assert (service["dist"], service["mean"], service["std"]) == (
            own.dist,
            own.mean,
            own.std,
        ), f"the document's service {name} is not the project's"
        document["services"][name] = {
            "dist": "constant",
            "value": SERVICES[service["mean"]],
        }
    for name, value in variables.items():
        assert document["variables"][name] == value, (
            f"the document's variable {name} is not the project's"
        )
    document["tasks"]["tick"]["set"]["ticks"] = {
        "gen": "expr",
        "expr": "attr.ticks + 1",
    }
    document["tasks"] = fix_draws(
        document["tasks"], fraction=_fraction, path=("tasks",)
    )
    document["arrivals"] = fix_draws(
        document["arrivals"], fraction=_fraction, path=("arrivals",)
    )
    return document


@in_project("game-leaderboard")
def _settings() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """The arrivals, services and variables the project's `build` adds, by name."""
    with stand_in_modules("confluent_kafka", "confluent_kafka.admin", "httpx"):
        from leaderboard.simulation import run as project
    app = SimulationContext("game", factor=0.0)
    project.build(app)
    return app._arrivals.configs, app._services_config, app._variables_config


class _Rng:
    """The project's `rng` (`sampler.rng`), with the fixed draws: it finds the player
    and the site of a call in the calling function's variables."""

    def __init__(self) -> None:
        self.rounds: dict[tuple[str, str], int] = {}

    def random(self) -> float:
        frame = sys._getframe(1)
        name = frame.f_code.co_name
        local = frame.f_locals
        if name == "players":
            return _draw("robot", int(local["count"]))
        player = int(local["player"][-6:])
        if name == "join":
            return _draw("new_team", player)
        key = ("late", local["player"])
        self.rounds[key] = self.rounds.get(key, 0) + 1
        return _draw("late", player, self.rounds[key])

    def integers(self, low: int, high: int | None = None) -> int:
        frame = sys._getframe(1)
        name = frame.f_code.co_name
        local = frame.f_locals
        player = int(local["player"][-6:])
        if name == "join":
            site = "free" if "free" in local else "open"
            return int(_draw(site, player) * low)  # the one bound is a count
        key = ("score", local["player"])
        self.rounds[key] = self.rounds.get(key, 0) + 1
        return int(_draw("score", player, self.rounds[key]) * (high - low)) + low


class _Sampler:
    """The project's `sampler`: durations are the means, the arrival gap is 1/rate."""

    def __init__(self, seed: Any = None) -> None:
        self.rng = _Rng()
        self.gaps = 0
        self._real = Sampler(seed=seed)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    def sample(self, config: Any) -> float:
        if config.dist == "exponential":
            self.gaps += 1
            return PAUSE if self.gaps == PAUSED_AT else 1.0 / config.rate
        return SERVICES[
            int(config.mean) if config.mean == int(config.mean) else config.mean
        ]


@in_project("game-leaderboard")
def generate(config: dict[str, Any]) -> types.SimpleNamespace:
    """Runs the project's `build` with the fixed draws and the document's scenario steps
    to the end of the document's run.

    Args:
        config: An entry of `CONFIGS`.

    Returns:
        types.SimpleNamespace: `scores` (each score the project sent: the time, the
            player and the `ScoreEvent`), `models` (its `ScoreEvent` and schema).
    """
    sent: list[tuple[float, str, Any]] = []

    def record(env: Any, key: str, event: Any, **extra: Any) -> None:
        sent.append((env.now, key, event))

    with stand_in_modules("confluent_kafka", "confluent_kafka.admin", "httpx"):
        from leaderboard.core import models
        from leaderboard.simulation import control
        from leaderboard.simulation import run as project

    games: list[Any] = []
    real_game = project.Game

    def make_game(rng: Any) -> Any:
        games.append(real_game(rng))
        return games[-1]

    build = project.build
    document = read_document(HERE / "model.yaml", variables=VARIABLES)[0]
    steps = [(s["at"], s["path"], s["value"]) for s in document["scenario"]]

    with (
        mock.patch.object(DynamicRealtimeEnvironment, "publish_event", record),
        mock.patch("desflow.core.context.Sampler", _Sampler),
        mock.patch.object(project, "Game", make_game),
    ):
        app = SimulationContext(
            "game", factor=0.0, random_seed=42, logical_start_time=START
        )
        build(app)

        # the document's scenario steps, made in the registry as `control.py` sends them
        def scenario(context: Any) -> Any:
            for at, path, value in steps:
                yield context.env.timeout(at - context.env.now)
                context.env.registry.update(path, value)

        app.add_process(scenario)
        app.run(until=UNTIL)
    return types.SimpleNamespace(
        scores=sent, models=models, game=games[0], parameters=control.PARAMETERS
    )


def compare(
    records: list[dict[str, Any]], result: types.SimpleNamespace, config: dict[str, Any]
) -> None:
    """Asserts that the YAML's scores and the project's agree, for BT-GL1 to BT-GL10.

    Args:
        records: The records of the YAML with the fixed draws.
        result: What `generate` returned.
        config: The entry of `CONFIGS` both ran.

    Raises:
        AssertionError: At the first check that fails.
    """
    want = sorted(
        (round(t, 6), key, e.team_name, e.score, e.event_time_millis, e.event_type)
        for t, key, e in result.scores
    )
    got = sorted(
        (
            round(r["sim_ts"], 6),
            r["key"],
            r["value"]["team"],
            r["value"]["score"],
            r["value"]["earned_ms"],
            r["value"].get("event_type", "normal"),
        )
        for r in records
        if r["stream_type"] == "event"
        and r["task"] in ("round", "late")
        and "status" not in r["value"]
    )
    assert len(want) > 1000 and got == want
    # the rows' rules, reached by the fixed draws
    players = {key for _, key, *_ in want}
    robots = {p for p in players if p.startswith("RBT-")}
    assert (
        robots and players - robots
    )  # BT-GL1, BT-GL2: robots and people, in one counter
    assert all(len(p) == 10 and p[3] == "-" for p in players)
    before = {
        p for p in robots if int(p[-6:]) < PAUSED_AT
    }  # the share is 0.05 until 300 s
    after = {p for p in robots if int(p[-6:]) >= PAUSED_AT}  # and 0.5 after it
    assert len(after) > 3 * len(before) > 0  # BT-GL1: it changed live
    kinds = {k for *_, k in want}
    assert kinds == {"normal", "late"}  # BT-GL7
    late = [w for w in want if w[-1] == "late"]
    assert all(
        w[0] - (w[4] - _got_start()) / 1000 >= 420 - 1e-6 for w in late
    )  # the offline delay
    game = result.game
    assert (
        game._formed > len(game.teams) > 0
    )  # BT-GL6: teams formed, dissolved, and formed again
    # BT-GL5: each score of the YAML is a valid `ScoreEvent` (a score of 0 to 20 and an
    # earned time in ms); the YAML carries no team id, which comes from the project's
    # score at the same time, player and earned time
    team_id = {
        (round(t, 6), key, e.event_time_millis): e.team_id
        for t, key, e in result.scores
    }
    for record in records:
        value = record["value"]
        if record["stream_type"] != "event" or record["task"] not in ("round", "late"):
            continue
        if "status" in value:
            continue
        result.models.ScoreEvent(
            user_id=record["key"],
            team_id=team_id[
                (round(record["sim_ts"], 6), record["key"], value["earned_ms"])
            ],
            team_name=value["team"],
            score=value["score"],
            event_time_millis=value["earned_ms"],
            event_type=value.get("event_type", "normal"),
        )
    # BT-GL6: a team of `TEAM_SIZE` was refused a player: a join happened while a team
    # was full, and went to another team
    size: dict[str, int] = {}
    refused = False
    for record in sorted(records, key=lambda r: r["sim_ts"]):
        value = record["value"]
        if record["stream_type"] != "event" or record["task"] not in (
            "join_team",
            "leave_team",
        ):
            continue
        if value.get("event_type") == "joined":
            refused |= (
                TEAM_SIZE in size.values() and size.get(value["team"]) != TEAM_SIZE
            )
            size[value["team"]] = size.get(value["team"], 0) + 1
            assert size[value["team"]] <= TEAM_SIZE, (record, size)
        elif value.get("event_type") == "left":
            size[value["team"]] -= 1
    assert refused
    # BT-GL1: the document's names are the project's, so the live change the project's
    # control tool sends reaches the YAML's registry
    document = read_document(HERE / "model.yaml", variables=VARIABLES)[0]
    assert document["simulation"]["sim_id"] == "game"
    steps = [(s["at"], s["path"], s["value"]) for s in document["scenario"]]
    assert steps == [(300, "game.variables.robot_share", 0.5)], steps
    assert all(path in result.parameters for _, path, _ in steps)
    assert f"game.arrival.{next(iter(document['arrivals']))}.rate" in result.parameters
    # BT-GL9 and BT-GL10: the sinks the project has, as the file builds them
    app = SimulationContext.from_yaml(HERE / "model.yaml", variables=VARIABLES)
    (sink,) = [sink.provider for sink in app._sinks.egress]
    assert [sink.envelope for sink in app._sinks.egress] == [False]
    serializer = (
        sink.default_serializer
    )  # BT-GL9: Avro through the registry, the project's schema
    assert isinstance(serializer, ConfluentAvroSerializer)
    assert json.loads(serializer._schema_str) == json.loads(result.models.AVRO_SCHEMA)
    assert [p.topic for p in app._sinks.ingress] == ["game-control"]


def _got_start() -> int:
    """The start of the run in milliseconds since the Unix epoch."""
    return int(START.timestamp() * 1000)
