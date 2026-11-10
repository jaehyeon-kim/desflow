"""HR-01: the hot rolling twin, three product lines.

Builder form of `model.yaml`. A line is written once, with the grade's name, arrival
rate and hardness, and added for each of the three grades.
"""

from datetime import datetime, timezone
from typing import Any

from desflow import ConsoleEgress, KafkaEgress, KafkaIngress, SimulationContext
from desflow.values import Template, generator

from tests.known_cases.bt_support import deep

BROKER = "localhost:9092"
GRADES = (
    # name, the name in a slab id, the arrival rate, the hardness of the baseline
    ("structural", "STRUCTURAL", 0.2, 18000.0),
    ("microalloyed", "MICROALLOYED", 0.15, 28000.0),
    ("high_alloy", "HIGH_ALLOY", 0.1, 45000.0),
)
PHASES = {
    "roughing": "pass_roughing",
    "intermediate": "pass_intermediate",
    "finishing": "pass_finishing",
}
PASS_PATH = (
    "['HotRolling.service.pass_roughing', 'HotRolling.service.pass_intermediate', "
    "'HotRolling.service.pass_finishing'][0 if attr.pass_number <= 2 else 1 if "
    "attr.pass_number <= 4 else 2]"
)
# The step of a gradual velocity on a second divisible by its `freq`, and 0 otherwise
STEP = (
    "0 if v['type'] != 'gradual' or t < 1 or floor(t) % get_key(v, 'freq', 1) != 0 "
    "else v['value'] if sg == 0 else abs(v['value']) * sg"
)


def _param(path: str, **extra: Any) -> dict[str, Any]:
    return {"gen": "param", "path": path, **extra}


def _attributes(grade: str, label: str) -> dict[str, Any]:
    low_high = {
        "temperature_c": (1180.0, 1220.0),
        "thickness_mm": (200.0, 250.0),
        "width_mm": (1800.0, 3000.0),
        "speed_m_s": (1.5, 2.0),
        "roll_diameter_mm": (1050.0, 1100.0),
        "reheating_time_min": (150.0, 300.0),
        "roll_crown_mm": (30.0, 150.0),
        "length_mm": (2000.0, 3000.0),
    }
    drawn = {
        "gen": "expr",
        "expr": "drawn if vp else mp",
        "names": {
            "drawn": {
                "gen": "integer",
                "bounds": [3, _param("HotRolling.variables.max_passes")],
            },
            "vp": _param("HotRolling.variables.variable_passes"),
            "mp": _param("HotRolling.variables.max_passes"),
        },
    }
    out: dict[str, Any] = {
        "slab_id": {"gen": "sequence", "format": f"SLAB-{label}-{{:08X}}"},
        "total_passes": drawn,
        "pass_number": 0,
    }
    for name, (low, high) in low_high.items():
        out[name] = {"gen": "uniform", "low": low, "high": high, "decimals": 3}
    return out


def _pass_spec(grade: str, label: str, hardness: float) -> dict[str, Any]:
    """The arguments of a pass task of a grade, the same for the three phases."""

    def attr(name: str) -> dict[str, Any]:
        return {"gen": "attr", "name": name}

    def var(name: str) -> dict[str, Any]:
        return {"gen": "var", "name": name}

    wear = f"HotRolling.containers.wear_{grade}.current_cap"
    return {
        "key": generator(
            {
                "gen": "format",
                "template": "{s}-P{n}",
                "names": {
                    "s": attr("slab_id"),
                    "n": {"gen": "expr", "expr": "attr.pass_number + 1"},
                },
            }
        ),
        "vars": {
            "n": {"gen": "expr", "expr": "attr.pass_number + 1"},
            "ratio": {"gen": "expr", "expr": "n / attr.total_passes"},
            "reduction": {
                "gen": "uniform",
                "low": {
                    "gen": "expr",
                    "expr": "0.20 if ratio < 0.4 else 0.10 if ratio < 0.8 else 0.02",
                },
                "high": {
                    "gen": "expr",
                    "expr": "0.35 if ratio < 0.4 else 0.20 if ratio < 0.8 else 0.10",
                },
            },
            "exit_h": {"gen": "expr", "expr": "attr.thickness_mm * (1 - reduction)"},
            "strain": {"gen": "expr", "expr": "log(attr.thickness_mm / exit_h)"},
            "wait_s": {"gen": "uniform", "low": 10.0, "high": 40.0},
            "temp": {
                "gen": "expr",
                "expr": "max(889.0, attr.temperature_c - (wait_s * 0.5 + reduction * 30.0 + e))",
                "names": {"e": {"gen": "normal", "mean": 0, "std": 2}},
            },
            "spd": {
                "gen": "expr",
                "expr": "min(5.6, attr.speed_m_s + reduction * 2.0)",
            },
            "exit_len": {
                "gen": "expr",
                "expr": "attr.length_mm * (attr.thickness_mm / exit_h)",
            },
            "contact": {
                "gen": "expr",
                "expr": "sqrt(attr.roll_diameter_mm / 2.0 * (attr.thickness_mm - exit_h))",
            },
            "srate": {
                "gen": "expr",
                "expr": "max(0.5, min(16.2, spd * 1000.0 / contact * strain))",
            },
            "flow": {
                "gen": "expr",
                "expr": "max(5.0, min(178.0, 20.0 * (1200.0 / temp) ** 4.0 * strain ** 0.2 * srate ** 0.1))",
            },
            "h0": {"gen": "expr", "expr": "attr.thickness_mm"},
            "baseline": {
                "gen": "expr",
                "expr": f"{hardness} * attr.width_mm * attr.thickness_mm * reduction / max(temp, 1.0)",
                "decimals": 2,
            },
        },
        "on_start": {
            "__topic__": "mill-predictions",
            "event_type": "prediction_request",
            "timestamp": {"gen": "now"},
            "identifiers": {
                "slab_id": attr("slab_id"),
                "pass_number": var("n"),
                "steel_grade": grade,
                "routing_key": f"{grade}_mill",
            },
            "features": {
                "reheating_time_min": attr("reheating_time_min"),
                "roll_diameter_mm": attr("roll_diameter_mm"),
                "roll_crown_mm": attr("roll_crown_mm"),
                "entry_thickness_mm": var("h0"),
                "width_mm": attr("width_mm"),
                "length_mm": attr("length_mm"),
                "temperature_c": var("temp"),
                "speed_m_s": var("spd"),
                "wait_time_sec": var("wait_s"),
                "reduction_pct": var("reduction"),
                "strain": var("strain"),
                "strain_rate": var("srate"),
                "flow_stress_mpa": var("flow"),
            },
            "baseline_prediction": {"baseline_roll_force_kn": var("baseline")},
        },
        "finish_vars": {
            "wear": {
                "gen": "expr",
                "expr": "min(100.0, (0.0 if raw <= 0.001 else raw) * spread)",
                "names": {
                    "raw": _param(wear),
                    "spread": {"gen": "uniform", "low": 0.98, "high": 1.02},
                },
            }
        },
        "set": {
            "pass_number": var("n"),
            "temperature_c": var("temp"),
            "thickness_mm": var("exit_h"),
            "speed_m_s": var("spd"),
            "length_mm": var("exit_len"),
        },
    }


def _ground_truth(grade: str) -> dict[str, Any]:

    def var(name: str) -> dict[str, Any]:
        return {"gen": "var", "name": name}

    return {
        "__topic__": "mill-groundtruth",
        "event_type": "ground_truth",
        "timestamp": {"gen": "now"},
        "identifiers": {
            "slab_id": {"gen": "attr", "name": "slab_id"},
            "pass_number": var("n"),
        },
        "ground_truth": {
            "actual_roll_force_kn": {
                "gen": "expr",
                "decimals": 2,
                "expr": "max(0.0, baseline * (1 + (wear / 100) ** 1.5 * k * max(0.1, (1250.0 - temp) / 200.0) * (100.0 / max(h0, 10.0)) * (1 + reduction)) + e)",
                "names": {
                    "k": _param("HotRolling.variables.wear_penalty_rate"),
                    "e": {"gen": "normal", "mean": 0, "std": 500},
                },
            },
            "wear_level": var("wear"),
        },
    }


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="HotRolling",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_ingress(KafkaIngress(bootstrap_servers=BROKER, topic="sim-controls"))
    app.add_egress(
        KafkaEgress(
            bootstrap_servers=BROKER,
            event_topic="sim-lifecycle",
            topics={
                "sim-telemetry": {"partitions": 1},
                "sim-lifecycle": {"partitions": 1},
                "mill-predictions": {"partitions": 3},
                "mill-groundtruth": {"partitions": 3},
            },
        )
    )
    app.add_egress(ConsoleEgress())
    app.add_service("pass_roughing", dist="normal", mean=2.0, std=0.2)
    app.add_service("pass_intermediate", dist="normal", mean=4.5, std=0.5)
    app.add_service("pass_finishing", dist="normal", mean=8.0, std=1.2)
    for grade, _, _, _ in GRADES:
        app.add_resource(f"mill_{grade}", current_cap=4, max_cap=10)
    for grade, _, _, _ in GRADES:
        app.add_container(f"wear_{grade}", current_cap=0.001, max_cap=100.0)
    app.add_variable("max_passes", 5)
    app.add_variable("variable_passes", 0)
    app.add_variable("wear_penalty_rate", 0.40)
    for grade, _, _, _ in GRADES:
        app.add_variable(f"velocity_{grade}", {"type": "abrupt", "value": 0.0})
        app.add_variable(f"drift_sign_{grade}", 0)
    app.add_initial_arrival("drift_clock", 1, spawn="drift")
    app.add_initial_arrival("wear_clock", 1, spawn="wear_reading")
    app.add_initial_arrival(
        "wear_clock_microalloyed", 1, spawn="wear_reading_microalloyed"
    )
    app.add_initial_arrival("wear_clock_high_alloy", 1, spawn="wear_reading_high_alloy")
    for grade, label, rate, _ in GRADES:
        app.add_arrival(
            grade,
            "exponential",
            rate=rate,
            spawn=f"roughing_{grade}",
            first_at=0,
            attributes=deep(_attributes(grade, label)),
        )

    for grade, label, _, hardness in GRADES:
        for phase, service in PHASES.items():
            spec = _pass_spec(grade, label, hardness)  # new generators for each task
            for name in ("vars", "on_start", "finish_vars", "set"):
                spec[name] = deep(spec[name])
            task = f"{phase}_{grade}"
            app.task(
                name=task,
                service_id=service,
                resource_id=f"mill_{grade}",
                next=f"pass_done_{grade}",
                **spec,
            )(_returns(_ground_truth(grade), task))
    for grade, _, _, _ in GRADES:
        app.task(
            name=f"pass_done_{grade}",
            key=generator(
                {
                    "gen": "format",
                    "template": "{s}-P{n}",
                    "names": {
                        "s": {"gen": "attr", "name": "slab_id"},
                        "n": {"gen": "expr", "expr": "attr.pass_number"},
                    },
                }
            ),
            next={
                "when": [
                    {"condition": "attr.pass_number < 2", "task": f"roughing_{grade}"},
                    {
                        "condition": "attr.pass_number < 4 and attr.pass_number < attr.total_passes",
                        "task": f"intermediate_{grade}",
                    },
                    {
                        "condition": "attr.pass_number < attr.total_passes",
                        "task": f"finishing_{grade}",
                    },
                ]
            },
        )(
            _returns(
                {
                    "path_id": {"gen": "expr", "expr": PASS_PATH},
                    "status": "finished",
                },
                f"pass_done_{grade}",
            )
        )

    drift_on = []
    for grade, _, _, _ in GRADES:
        wear = f"HotRolling.containers.wear_{grade}.current_cap"
        velocity = f"HotRolling.variables.velocity_{grade}"
        sign = f"HotRolling.variables.drift_sign_{grade}"
        step = {
            "gen": "expr",
            "expr": STEP,
            "names": {"v": _param(velocity), "sg": _param(sign)},
        }
        drift_on.append(
            {
                "path": wear,
                "value": {
                    "gen": "expr",
                    "expr": "w if d == 0 else min(60.0, max(0.001, w + d))",
                    "names": {"w": _param(wear), "d": step},
                },
            }
        )
        drift_on.append(
            {
                "path": sign,
                "value": {
                    "gen": "expr",
                    "expr": "-1 if d != 0 and w >= 60.0 else 1 if d != 0 and w <= 0.001 else sg",
                    "names": {"w": _param(wear), "sg": _param(sign), "d": step},
                },
            }
        )
    app.task(name="drift", wait={"timeout": 1}, on=deep(drift_on), next="drift")(
        _returns({"event_type": "drift_tick"}, "drift")
    )
    for grade, _, _, _ in GRADES:
        suffix = "" if grade == "structural" else f"_{grade}"
        wear = f"HotRolling.containers.wear_{grade}.current_cap"
        app.task(
            name=f"wear_reading{suffix}",
            service_time=2.0,
            on_start=deep(
                {"__topic__": "sim-telemetry", "path_id": wear, "value": _param(wear)}
            ),
            next=f"wear_reading{suffix}",
        )(_returns({"event_type": "wear_reading_done"}, f"wear_reading{suffix}"))
    app.add_scenario(
        [
            {
                "at": 300,
                "path": "HotRolling.variables.velocity_structural",
                "value": {"type": "gradual", "value": 1.5, "freq": 3},
            },
            {
                "at": 450,
                "path": "HotRolling.variables.velocity_microalloyed",
                "value": {"type": "abrupt", "value": 0.0},
            },
            {
                "at": 450,
                "path": "HotRolling.containers.wear_microalloyed.current_cap",
                "value": 80.0,
            },
        ]
    )
    return app


def _returns(payload: Any, task: str):
    """The function of a task that returns `payload` as a template named for the task,
    so its generators draw from the streams the document's `payload` does."""
    template = Template(payload, name=f"tasks.{task}")

    def body(task_id):
        return template

    body.__name__ = task
    return body


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=600)
