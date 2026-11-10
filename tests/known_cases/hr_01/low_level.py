"""HR-01: the hot rolling twin, three product lines.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. Each draw takes the random sequence the document's
generator at that place takes (`Sampler(seed=42).generator(<place>)`), so the draws,
and with them every record, are those of `model.yaml`.
"""

import math
from datetime import datetime, timezone
from typing import Any

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

UNTIL = 600
GRADES = (
    # name, the name in a slab id, the arrival rate, the hardness of the baseline
    ("structural", "STRUCTURAL", 0.2, 18000.0),
    ("microalloyed", "MICROALLOYED", 0.15, 28000.0),
    ("high_alloy", "HIGH_ALLOY", 0.1, 45000.0),
)
SERVICES = {
    "pass_roughing": (2.0, 0.2),
    "pass_intermediate": (4.5, 0.5),
    "pass_finishing": (8.0, 1.2),
}
RANGES = {  # the attributes of a slab: low, high
    "temperature_c": (1180.0, 1220.0),
    "thickness_mm": (200.0, 250.0),
    "width_mm": (1800.0, 3000.0),
    "speed_m_s": (1.5, 2.0),
    "roll_diameter_mm": (1050.0, 1100.0),
    "reheating_time_min": (150.0, 300.0),
    "roll_crown_mm": (30.0, 150.0),
    "length_mm": (2000.0, 3000.0),
}
PARAMS = SimParameter(
    sim_id="HotRolling",
    arrival={
        name: DistributionConfig(dist="exponential", rate=rate)
        for name, _, rate, _ in GRADES
    },
    service={
        name: DistributionConfig(dist="normal", mean=mean, std=std)
        for name, (mean, std) in SERVICES.items()
    },
    resources={
        f"mill_{name}": CapacityConfig(current_cap=4, max_cap=10)
        for name, _, _, _ in GRADES
    },
    containers={
        f"wear_{name}": CapacityConfig(current_cap=0.001, max_cap=100.0)
        for name, _, _, _ in GRADES
    },
    variables={
        "max_passes": 5,
        "variable_passes": 0,
        "wear_penalty_rate": 0.40,
        **{
            f"velocity_{name}": {"type": "abrupt", "value": 0.0}
            for name, _, _, _ in GRADES
        },
    },
)
ENVIRONMENT = {"logical_start_time": datetime(2026, 1, 1, tzinfo=timezone.utc)}


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)

    def stream(place: str) -> Any:
        return sampler.generator(place)

    def uniform(place: str, low: float, high: float, decimals: Any = None) -> float:
        value = low + (high - low) * float(stream(place).random())
        return value if decimals is None else round(value, decimals)

    def configured(path: str, place: str) -> Any:
        config = env.registry.get_config(path)
        config.stream = place  # the random sequence the document's draw uses
        return config

    def now() -> str:
        return env._get_iso_timestamp(env.start_datetime, env.now)

    def sample(config: Any) -> float:
        return sampler.sample(config)

    def wear_of(grade: str) -> Any:
        return env.registry.get(f"HotRolling.containers.wear_{grade}.current_cap")

    def slab(grade: str, label: str, hardness: float, mill: Any, number: int) -> Any:
        place = f"arrivals.{grade}.attributes"
        slab_id = f"SLAB-{label}-{number:08X}"
        drawn = int(stream(f"{place}.total_passes.names.drawn").integers(3, 5 + 1))
        variables = env.registry
        max_passes = variables.get("HotRolling.variables.max_passes").value
        total = (
            drawn
            if variables.get("HotRolling.variables.variable_passes").value
            else max_passes
        )
        state = {name: uniform(f"{place}.{name}", *RANGES[name], 3) for name in RANGES}
        state["pass_number"] = 0
        instance = 0
        while True:
            done = state["pass_number"]
            if done < 2:
                phase = "roughing"
            elif done < 4 and done < total:
                phase = "intermediate"
            elif done < total:
                phase = "finishing"
            else:
                return
            task = f"{phase}_{grade}"
            service = f"pass_{phase}"
            path_id = f"HotRolling.service.{service}"
            n = done + 1
            key = f"{slab_id}-P{n}"
            env.publish_event(
                key,
                {"path_id": path_id, "status": "queued"},
                task=task,
                instance=instance,
            )
            with mill.request() as request:
                yield request
                env.publish_event(
                    key,
                    {"path_id": path_id, "status": "started"},
                    task=task,
                    instance=instance,
                )
                here = f"tasks.{task}"
                ratio = n / total
                reduction = uniform(
                    f"{here}.vars.reduction",
                    0.20 if ratio < 0.4 else 0.10 if ratio < 0.8 else 0.02,
                    0.35 if ratio < 0.4 else 0.20 if ratio < 0.8 else 0.10,
                )
                exit_h = state["thickness_mm"] * (1 - reduction)
                strain = math.log(state["thickness_mm"] / exit_h)
                wait_s = uniform(f"{here}.vars.wait_s", 10.0, 40.0)
                noise = float(stream(f"{here}.vars.temp.names.e").normal(0, 2))
                temp = max(
                    889.0,
                    state["temperature_c"] - (wait_s * 0.5 + reduction * 30.0 + noise),
                )
                spd = min(5.6, state["speed_m_s"] + reduction * 2.0)
                exit_len = state["length_mm"] * (state["thickness_mm"] / exit_h)
                contact = math.sqrt(
                    state["roll_diameter_mm"] / 2.0 * (state["thickness_mm"] - exit_h)
                )
                srate = max(0.5, min(16.2, spd * 1000.0 / contact * strain))
                flow = max(
                    5.0,
                    min(
                        178.0, 20.0 * (1200.0 / temp) ** 4.0 * strain**0.2 * srate**0.1
                    ),
                )
                h0 = state["thickness_mm"]
                baseline = round(
                    hardness
                    * state["width_mm"]
                    * state["thickness_mm"]
                    * reduction
                    / max(temp, 1.0),
                    2,
                )
                env.publish_event(
                    key,
                    {
                        "__topic__": "mill-predictions",
                        "event_type": "prediction_request",
                        "timestamp": now(),
                        "identifiers": {
                            "slab_id": slab_id,
                            "pass_number": n,
                            "steel_grade": grade,
                            "routing_key": f"{grade}_mill",
                        },
                        "features": {
                            "reheating_time_min": state["reheating_time_min"],
                            "roll_diameter_mm": state["roll_diameter_mm"],
                            "roll_crown_mm": state["roll_crown_mm"],
                            "entry_thickness_mm": h0,
                            "width_mm": state["width_mm"],
                            "length_mm": state["length_mm"],
                            "temperature_c": temp,
                            "speed_m_s": spd,
                            "wait_time_sec": wait_s,
                            "reduction_pct": reduction,
                            "strain": strain,
                            "strain_rate": srate,
                            "flow_stress_mpa": flow,
                        },
                        "baseline_prediction": {"baseline_roll_force_kn": baseline},
                    },
                    task=task,
                    instance=instance,
                )
                yield env.timeout(sample(configured(path_id, f"service.{service}")))
                raw = wear_of(grade).value
                spread = uniform(f"{here}.finish_vars.wear.names.spread", 0.98, 1.02)
                wear = min(100.0, (0.0 if raw <= 0.001 else raw) * spread)
                force_noise = float(
                    stream(
                        f"{here}.payload.ground_truth.actual_roll_force_kn.names.e"
                    ).normal(0, 500)
                )
                rate = env.registry.get("HotRolling.variables.wear_penalty_rate").value
                force = round(
                    max(
                        0.0,
                        baseline
                        * (
                            1
                            + (wear / 100) ** 1.5
                            * rate
                            * max(0.1, (1250.0 - temp) / 200.0)
                            * (100.0 / max(h0, 10.0))
                            * (1 + reduction)
                        )
                        + force_noise,
                    ),
                    2,
                )
                env.publish_event(
                    key,
                    {
                        "__topic__": "mill-groundtruth",
                        "event_type": "ground_truth",
                        "timestamp": now(),
                        "identifiers": {"slab_id": slab_id, "pass_number": n},
                        "ground_truth": {
                            "actual_roll_force_kn": force,
                            "wear_level": wear,
                        },
                    },
                    task=task,
                    instance=instance,
                )
                state.update(
                    pass_number=n,
                    temperature_c=temp,
                    thickness_mm=exit_h,
                    speed_m_s=spd,
                    length_mm=exit_len,
                )
            env.publish_event(
                key,
                {"path_id": path_id, "status": "finished"},
                task=f"pass_done_{grade}",
                instance=instance,
            )
            instance += 1

    def arrivals(grade: str, label: str, hardness: float) -> Any:
        mill = DynamicResource(env, "HotRolling", f"mill_{grade}")
        gap = configured(f"HotRolling.arrival.{grade}", f"arrival.{grade}")
        number = 0
        while True:
            env.process(slab(grade, label, hardness, mill, number))
            number += 1
            yield env.timeout(sample(gap))

    def drift() -> Any:
        tick = 0
        while True:
            yield env.timeout(1)
            for grade, _, _, _ in GRADES:
                velocity = env.registry.get(f"HotRolling.variables.velocity_{grade}")
                value = velocity.value
                if value["type"] != "gradual" or value["value"] == 0.0:
                    continue
                if env.now < 1 or int(env.now) % int(value.get("freq", 1)) != 0:
                    continue
                step = value["value"]
                current = wear_of(grade).value
                new = current + step
                if new >= 60.0:
                    new = 60.0
                    value["value"] = -abs(step)
                elif new <= 0.001:
                    new = 0.001
                    value["value"] = abs(step)
                env.registry.update(f"HotRolling.variables.velocity_{grade}", value)
                env.registry.update(
                    f"HotRolling.containers.wear_{grade}.current_cap", new
                )
            env.publish_event(
                "drift_clock-1",
                {"event_type": "drift_tick"},
                task="drift",
                instance=tick,
            )
            tick += 1

    def reading(grade: str, key: str, task: str) -> Any:
        path = f"HotRolling.containers.wear_{grade}.current_cap"
        tick = 0
        while True:
            env.publish_event(
                key,
                {
                    "__topic__": "sim-telemetry",
                    "path_id": path,
                    "value": wear_of(grade).value,
                },
                task=task,
                instance=tick,
            )
            yield env.timeout(2.0)
            env.publish_event(
                key, {"event_type": "wear_reading_done"}, task=task, instance=tick
            )
            tick += 1

    def scenario() -> Any:
        yield env.timeout(300)
        env.registry.update(
            "HotRolling.variables.velocity_structural",
            {"type": "gradual", "value": 1.5, "freq": 3},
        )
        yield env.timeout(150)
        env.registry.update(
            "HotRolling.variables.velocity_microalloyed",
            {"type": "abrupt", "value": 0.0},
        )
        env.registry.update("HotRolling.containers.wear_microalloyed.current_cap", 80.0)

    env.process(scenario())
    env.process(drift())
    env.process(reading("structural", "wear_clock-1", "wear_reading"))
    env.process(
        reading(
            "microalloyed", "wear_clock_microalloyed-1", "wear_reading_microalloyed"
        )
    )
    env.process(
        reading("high_alloy", "wear_clock_high_alloy-1", "wear_reading_high_alloy")
    )
    for grade, label, _, hardness in GRADES:
        env.process(arrivals(grade, label, hardness))
