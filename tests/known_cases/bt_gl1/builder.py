"""BT-GL1 to BT-GL10: game-leaderboard

Builder form of `model.yaml`.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from desflow import KafkaEgress, KafkaIngress, SimulationContext
from desflow.values import Template

from tests.known_cases.bt_support import deep

HERE = Path(__file__).parent

SERVICES = {
    "session": {"dist": "lognormal", "mean": 300, "std": 120},
    "round": {"dist": "lognormal", "mean": 6, "std": 2},
    "robot_round": {"dist": "normal", "mean": 1.5, "std": 0.3},
    "offline": {"dist": "normal", "mean": 420, "std": 120},
}

RESOURCES = {}

CONTAINERS = {}

VARIABLES = {
    "robot_share": 0.05,
    "late_share": 0.01,
    "teams": {},
    "team_names": [
        "Amber-Bilby",
        "Amber-Dingo",
        "Amber-Emu",
        "Amber-Koala",
        "Amber-Kookaburra",
        "Amber-Numbat",
        "Amber-Platypus",
        "Amber-Quokka",
        "Amber-Wallaby",
        "Amber-Wombat",
        "Azure-Bilby",
        "Azure-Dingo",
        "Azure-Emu",
        "Azure-Koala",
        "Azure-Kookaburra",
        "Azure-Numbat",
        "Azure-Platypus",
        "Azure-Quokka",
        "Azure-Wallaby",
        "Azure-Wombat",
        "Coral-Bilby",
        "Coral-Dingo",
        "Coral-Emu",
        "Coral-Koala",
        "Coral-Kookaburra",
        "Coral-Numbat",
        "Coral-Platypus",
        "Coral-Quokka",
        "Coral-Wallaby",
        "Coral-Wombat",
        "Fuchsia-Bilby",
        "Fuchsia-Dingo",
        "Fuchsia-Emu",
        "Fuchsia-Koala",
        "Fuchsia-Kookaburra",
        "Fuchsia-Numbat",
        "Fuchsia-Platypus",
        "Fuchsia-Quokka",
        "Fuchsia-Wallaby",
        "Fuchsia-Wombat",
        "Indigo-Bilby",
        "Indigo-Dingo",
        "Indigo-Emu",
        "Indigo-Koala",
        "Indigo-Kookaburra",
        "Indigo-Numbat",
        "Indigo-Platypus",
        "Indigo-Quokka",
        "Indigo-Wallaby",
        "Indigo-Wombat",
        "Jade-Bilby",
        "Jade-Dingo",
        "Jade-Emu",
        "Jade-Koala",
        "Jade-Kookaburra",
        "Jade-Numbat",
        "Jade-Platypus",
        "Jade-Quokka",
        "Jade-Wallaby",
        "Jade-Wombat",
        "Magenta-Bilby",
        "Magenta-Dingo",
        "Magenta-Emu",
        "Magenta-Koala",
        "Magenta-Kookaburra",
        "Magenta-Numbat",
        "Magenta-Platypus",
        "Magenta-Quokka",
        "Magenta-Wallaby",
        "Magenta-Wombat",
        "Olive-Bilby",
        "Olive-Dingo",
        "Olive-Emu",
        "Olive-Koala",
        "Olive-Kookaburra",
        "Olive-Numbat",
        "Olive-Platypus",
        "Olive-Quokka",
        "Olive-Wallaby",
        "Olive-Wombat",
        "Ruby-Bilby",
        "Ruby-Dingo",
        "Ruby-Emu",
        "Ruby-Koala",
        "Ruby-Kookaburra",
        "Ruby-Numbat",
        "Ruby-Platypus",
        "Ruby-Quokka",
        "Ruby-Wallaby",
        "Ruby-Wombat",
        "Teal-Bilby",
        "Teal-Dingo",
        "Teal-Emu",
        "Teal-Koala",
        "Teal-Kookaburra",
        "Teal-Numbat",
        "Teal-Platypus",
        "Teal-Quokka",
        "Teal-Wallaby",
        "Teal-Wombat",
    ],
}

ARRIVALS = {
    "player": {
        "dist": "exponential",
        "rate": 0.5,
        "spawn": "join_team",
        "vars": {
            "robot": {
                "gen": "boolean",
                "p": {"gen": "param", "path": "game.variables.robot_share"},
            }
        },
        "key": {
            "gen": "format",
            "template": "{p}-{n:06d}",
            "names": {
                "p": {
                    "gen": "when",
                    "condition": "robot",
                    "then": "RBT",
                    "else": "USR",
                },
                "n": {"gen": "sequence", "start": 1},
            },
        },
        "attributes": {
            "robot": {"gen": "var", "name": "robot"},
            "born": {"gen": "expr", "expr": "t"},
            "session_len": {"gen": "sample", "service": "session"},
        },
    }
}

SCENARIO = [{"at": 300, "path": "game.variables.robot_share", "value": 0.5}]

TASKS = {
    "join_team": {
        "vars": {
            "teams": {"gen": "param", "path": "game.variables.teams"},
            "room": {"gen": "expr", "expr": "[k for k in teams if teams[k] < 15]"},
            "names": {"gen": "param", "path": "game.variables.team_names"},
            "free2": {"gen": "expr", "expr": "[f for f in names if f not in teams]"},
            "form_new": {"gen": "boolean", "p": 0.1},
            "u1": {"gen": "uniform", "low": 0, "high": 1},
            "u2": {"gen": "uniform", "low": 0, "high": 1},
            "team": {
                "gen": "expr",
                "expr": "room[int(u1 * len(room))] if len(room) > 0 and not "
                "form_new else free2[int(u2 * len(free2))]",
            },
        },
        "payload": {"event_type": "joined", "team": {"gen": "var", "name": "team"}},
        "set": {"team": {"gen": "var", "name": "team"}},
        "on": [
            {
                "path": "game.variables.teams",
                "value": {
                    "gen": "expr",
                    "expr": "set_key(teams, team, get_key(teams, team, 0) + 1)",
                },
            }
        ],
        "next": "tick",
    },
    "tick": {
        "service_time": {
            "gen": "expr",
            "expr": "robot_round if attr.robot else human_round",
            "names": {
                "robot_round": {"gen": "sample", "service": "robot_round"},
                "human_round": {"gen": "sample", "service": "round"},
            },
        },
        "vars": {
            "is_late": {
                "gen": "boolean",
                "p": {"gen": "param", "path": "game.variables.late_share"},
            },
            "score": {"gen": "integer", "bounds": [0, 20]},
            "ended": {"gen": "expr", "expr": "t - attr.born > attr.session_len"},
        },
        "payload": {},
        "set": {"score": {"gen": "var", "name": "score"}},
        "spawn": {
            "task": "late",
            "count": {"gen": "expr", "expr": "1 if is_late and not ended else 0"},
            "args": {
                "player": {"gen": "entity"},
                "score": {"gen": "var", "name": "score"},
                "earned_ms": {
                    "gen": "format_time",
                    "format": "epoch_ms",
                    "time": {"gen": "now"},
                },
                "team": {"gen": "attr", "name": "team"},
            },
        },
        "next": {
            "when": [
                {"condition": "ended", "task": "leave_team"},
                {"condition": "is_late", "task": "tick"},
            ],
            "default": "round",
        },
    },
    "round": {
        "payload": {
            "player": {"gen": "entity"},
            "score": {"gen": "attr", "name": "score"},
            "earned_ms": {
                "gen": "format_time",
                "format": "epoch_ms",
                "time": {"gen": "now"},
            },
            "team": {"gen": "attr", "name": "team"},
        },
        "next": "tick",
    },
    "late": {
        "service": "offline",
        "key": {"gen": "expr", "expr": "arg.player"},
        "payload": {
            "event_type": "late",
            "player": {"gen": "expr", "expr": "arg.player"},
            "score": {"gen": "expr", "expr": "arg.score"},
            "earned_ms": {"gen": "expr", "expr": "arg.earned_ms"},
            "team": {"gen": "expr", "expr": "arg.team"},
        },
    },
    "leave_team": {
        "vars": {"teams": {"gen": "param", "path": "game.variables.teams"}},
        "payload": {"event_type": "left", "team": {"gen": "attr", "name": "team"}},
        "on": [
            {
                "path": "game.variables.teams",
                "value": {
                    "gen": "expr",
                    "expr": "drop_key(teams, attr.team) if teams[attr.team] == 1 "
                    "else set_key(teams, attr.team, teams[attr.team] - "
                    "1)",
                },
            }
        ],
    },
}

KEYS = {
    "service": "service_id",
    "resource": "resource_id",
}  # YAML keys the builder names otherwise


def _returns(payload: Any, task: str) -> Any:
    """The function of a task that returns `payload` as a template named for the task,
    so its generators draw from the streams the document's `payload` does."""
    template = Template(payload, name=f"tasks.{task}")

    def body(task_id):
        return template

    body.__name__ = task
    return body


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="game",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_ingress(
        KafkaIngress(bootstrap_servers="localhost:9092", topic="game-control")
    )
    app.add_egress(
        KafkaEgress(
            bootstrap_servers="localhost:9092",
            event_topic="game-scores",
            serializer={
                "type": "confluent_avro",
                "registry_url": "http://localhost:8081",
                "schema_file": str(HERE / "score.avsc"),
            },
        ),
        envelope=False,
    )
    for name, spec in SERVICES.items():
        app.add_service(name, **spec)
    for name, spec in RESOURCES.items():
        app.add_resource(name, **spec)
    for name, spec in CONTAINERS.items():
        app.add_container(name, **spec)
    for name, value in VARIABLES.items():
        app.add_variable(name, value)
    for name, spec in ARRIVALS.items():
        spec = dict(spec)
        app.add_arrival(name, spec.pop("dist", None), **deep(spec))
    app.add_scenario(deep(SCENARIO))
    for name, spec in TASKS.items():
        spec = dict(spec)
        payload = spec.pop("payload", {})
        app.task(name=name, **{KEYS.get(k, k): deep(v) for k, v in spec.items()})(
            _returns(deep(payload), name)
        )
    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=600)
