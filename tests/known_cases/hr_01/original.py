"""HR-01 to HR-26, DX-16, DX-17 and DX-19: the hot rolling twin's own generator.

The original is `sim_control/generator.py` of the hot rolling twin (its copy in
`tests/copied_projects/oml-digital-twin-hotrolling`, read only). `generate` runs its
`run()` with what needs a broker or the wall clock replaced, so that the twin's own
processes (`drift_engine`, `telemetry_monitor`, `roll_slab` and `arrival_process`) make
the records:

- `DynamicRealtimeEnvironment` is unpaced, has the document's start time and takes the
  document's scenario steps as registry updates, as the dashboard's control topic does;
- `Sampler` gives the fixed draws of the configuration below, and `uuid` a count, so a
  slab id is a count;
- the Kafka admin connector, ingress and egress are recorders, and the egress is the
  recording egress of the other cases.

`fix` checks the document's gaps and service times against the twin's `mill_params`,
then gives the YAML the same draws, so the YAML and the twin run the same physics and
`compare` holds them equal: the twin publishes its features rounded to 3 decimals, so
numbers are equal within that rounding (`harness.same_within`, a relative 2.5e-5 and an
absolute 5e-4, the measured need) and the time of an item is equal exactly.

What differs between the twin and the YAML, each stated by the row that chose it:

- HR-04: a slab id is a sequence in the YAML and a random hash in the twin, so ids are
  compared by the order in which they first appear in a grade;
- HR-16: the YAML has one key for a pass (`SLAB-...-P1`) where the twin prefixes the
  prediction and the ground truth with `PRED-` and `GT-`;
- HR-13 and HR-21: the clock tasks publish a drift record and a wear reading a task
  record each, which the twin has no counterpart of, so only the wear readings are
  compared, with the twin's telemetry;
- the order of records at one time follows each program's own order of processes, so
  they are compared in the order of time and key, and by key in publication order.
"""

import contextlib
import copy
import logging
import re
import sys
import types
from pathlib import Path
from typing import Any
from unittest import mock

import desflow
from desflow import SimulationContext
from desflow.blueprint.loader import read_document
from tests.conformance.harness import RecordingEgress
from tests.known_cases import harness
from tests.known_cases.bt_support import importable

HERE = Path(__file__).parent
TWIN = (
    Path(__file__).resolve().parents[2]
    / "copied_projects"
    / "oml-digital-twin-hotrolling"
    / "sim_control"
)

# The values of `${NAME}` of model.yaml for every run: unpaced.
VARIABLES = {"FACTOR": "0"}

# The draws, fixed for the comparison with the twin. A uniform draw takes `FRACTION` of
# its range, or the fraction a configuration's `fractions` gives for the range (the
# wear spread, 0.98 to 1.02, takes 0.75 in every configuration, so it is not 1). A
# normal draw is its mean plus `Z` standard deviations, or the number of standard
# deviations a configuration's `z` gives for the standard deviation. An integer takes
# the middle of its range. Each reduction this gives has 3 decimals, which the twin's
# rounded features keep.
FRACTION = 0.5
FRACTIONS = {(0.98, 1.02): 0.75}
Z = 0.25
GAP = {0.2: 5.0173, 0.15: 6.6619, 0.1: 10.0331}  # an arrival's gap, by its rate
SERVICE = {2.0: 2.0371, 4.5: 4.5113, 8.0: 8.0227}  # a pass's time, by its mean

# The twin's limits of the entry temperature, the speed and the strain rate
# (`src/slab.py`), which the third configuration reaches.
LIMITS = {"temperature_c": 889.0, "speed_m_s": 5.6, "strain_rate": 16.2}
# The temperature above which the thermal factor (1250 - t) / 200 is below its floor of
# 0.1 (`src/physics.py`), which the second configuration reaches.
WARM = 1230.0
# The three bands of reduction of `src/slab.py`: up to 0.4, 0.8 and 1 of the passes.
BANDS = ((0.20, 0.35), (0.10, 0.20), (0.02, 0.10))

# What each run changes: the YAML's `${NAME}`, the twin's `run` arguments and the fixed
# draws, and what the run must reach (`passes`: the passes of a slab; `reaches`: a limit
# of the physics).
CONFIGS = [
    {
        "variables": {},
        "args": {"seed": 42, "variable_passes": False, "max_passes": 5},
        "passes": 5,
    },
    {
        "variables": {"SEED": "7", "VARIABLE_PASSES": "1", "MAX_PASSES": "4"},
        "args": {"seed": 7, "variable_passes": True, "max_passes": 4},
        "passes": 4,  # the integer draw takes the middle of 3 to 4 plus one
        # The shortest wait and a temperature noise of -8.8 standard deviations warm a
        # slab at each pass, to 1231.35 at its fourth.
        "fractions": {(10.0, 40.0): 0.0},
        "z": {2: -8.8},
        "reaches": "warm",
    },
    {
        "variables": {"SEED": "3", "MAX_PASSES": "10"},
        "args": {"seed": 3, "variable_passes": False, "max_passes": 10},
        "passes": 10,  # the twin's records allow 10 passes at most
        # A cool, thin and fast slab, the longest wait, heavy reductions and a
        # temperature noise of 2 standard deviations: ten passes cool a slab to 889,
        # speed it to 5.6 and take its strain rate to 16.2, and keep its entry
        # thickness above the twin's 15.
        "fractions": {
            (1180.0, 1220.0): 0.0,
            (200.0, 250.0): 0.0,
            (1.5, 2.0): 0.99,
            (10.0, 40.0): 0.99,
            (0.20, 0.35): 0.9,
            (0.10, 0.20): 0.9,
            (0.02, 0.10): 0.9875,
        },
        "z": {2: 2.0},
        "reaches": "limits",
    },
]
CLOCKS = (
    "drift",
    "wear_reading",
    "wear_reading_microalloyed",
    "wear_reading_high_alloy",
)
ID = re.compile(r"SLAB-([A-Z_]+)-([0-9A-F]{8})")


def _fractions(config: dict[str, Any]) -> dict[tuple[float, float], float]:
    """The fraction of each range the configuration names."""
    return {**FRACTIONS, **config.get("fractions", {})}


def _fraction(low: float, high: float, config: dict[str, Any]) -> float:
    """The fraction of its range a uniform draw from `low` to `high` takes."""
    return _fractions(config).get((low, high), FRACTION)


def _z(std: float, config: dict[str, Any]) -> float:
    """The standard deviations from its mean a normal draw of `std` takes."""
    return config.get("z", {}).get(std, Z)


def _config_of(document: dict[str, Any]) -> dict[str, Any]:
    """The entry of `CONFIGS` whose seed the document's `random_seed` is."""
    seed = int(document["simulation"]["random_seed"])
    configs = [c for c in CONFIGS if c["args"]["seed"] == seed]
    assert configs, f"no configuration has the document's seed {seed}"
    return configs[0]


def fix(document: dict[str, Any]) -> dict[str, Any]:
    """The document with the draws of the fixed values: constant gaps and service times,
    and every `uniform`, `normal` and `integer` generator an expression of the value
    the twin's stand-in sampler gives for the configuration of the document's seed.

    Args:
        document: The loaded `model.yaml`.

    Returns:
        dict[str, Any]: The same document with the draws fixed.

    Raises:
        AssertionError: When a gap or a service time differs from the twin's
            `mill_params` in its distribution, rate, mean or spread.
    """
    _twin()
    own = _PRISTINE[0]
    for name, arrival in document["arrivals"].items():
        if arrival.get("dist") == "exponential":
            twin = own.arrival[name]
            assert (arrival["dist"], arrival["rate"]) == (twin.dist, twin.rate), (
                f"the document's arrival {name} is not the twin's"
            )
            arrival["value"] = GAP[arrival.pop("rate")]
            arrival["dist"] = "constant"
    for name, service in document["services"].items():
        twin = own.service[name]
        assert (service["dist"], service["mean"], service["std"]) == (
            twin.dist,
            twin.mean,
            twin.std,
        ), f"the document's service {name} is not the twin's"
        document["services"][name] = {
            "dist": "constant",
            "value": SERVICE[service["mean"]],
        }
    config = _config_of(document)
    document["tasks"] = _fixed(document["tasks"], config)
    document["arrivals"] = _fixed(document["arrivals"], config)
    return document


def _fixed(node: Any, config: dict[str, Any]) -> Any:
    """The tree with each draw replaced by the twin's fixed draw for `config`."""
    if isinstance(node, list):
        return [_fixed(item, config) for item in node]
    if not isinstance(node, dict):
        return node
    node = {key: _fixed(value, config) for key, value in node.items()}
    kind = node.get("gen")
    if kind == "uniform":
        # The bounds of a reduction are expressions, so the fraction is chosen by the
        # bounds the run gives them.
        fraction = " else ".join(
            f"{f!r} if lo == {low!r} and hi == {high!r}"
            for (low, high), f in _fractions(config).items()
        )
        fixed = {
            "gen": "expr",
            "expr": f"lo + (hi - lo) * ({fraction} else {FRACTION!r})",
            "names": {"lo": node["low"], "hi": node["high"]},
        }
        if "decimals" in node:
            fixed["decimals"] = node["decimals"]
        return fixed
    if kind == "normal":
        z = " else ".join(
            f"{v!r} if sd == {s!r}" for s, v in config.get("z", {}).items()
        )
        return {
            "gen": "expr",
            "expr": f"m + sd * ({z + ' else ' if z else ''}{Z!r})",
            "names": {"m": node["mean"], "sd": node["std"]},
        }
    if kind == "integer":
        low, high = node["bounds"]
        return {
            "gen": "expr",
            "expr": "lo + (hi + 1 - lo) // 2",
            "names": {"lo": low, "hi": high},
        }
    return node


class _Rng:
    """The twin's `sampler.rng` with the fixed draws of a configuration."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def uniform(self, low: float, high: float) -> float:
        return low + (high - low) * _fraction(low, high, self.config)

    def normal(self, loc: float = 0.0, scale: float = 1.0) -> float:
        return loc + scale * _z(scale, self.config)

    def integers(self, low: int, high: int) -> int:
        return low + (high - low) // 2


class _Sampler:
    """The twin's `Sampler` with a fixed gap and service time."""

    seeds: list[Any] = []
    config: dict[str, Any] = {}

    def __init__(self, rng: Any = None) -> None:
        self.seeds.append(rng.bit_generator.state if rng is not None else None)
        self.rng = _Rng(self.config)

    def sample(self, config: Any) -> float:
        if config.dist == "exponential":
            return GAP[config.rate]
        return SERVICE[config.mean]


class _Uuid:
    """`uuid.uuid4().hex` as a count, so the slabs have ids in the order they start."""

    count = 0

    @classmethod
    def uuid4(cls) -> Any:
        number = cls.count
        cls.count += 1
        return types.SimpleNamespace(hex=f"{number:08x}" + "0" * 24)


_PRISTINE: list[Any] = []


def _twin() -> tuple[Any, Any, Any]:
    """The twin's `generator`, `routing` and `sim_logic` modules, read from its folder."""
    if not TWIN.is_dir():
        raise FileNotFoundError(f"the hot rolling twin is not at {TWIN}")
    # The twin imports `dynamic_des`, the name before the rename, which 1.0 removes.
    # Its import is answered with `desflow` itself, so the test needs no old name.
    before = sys.modules.get("dynamic_des")
    sys.modules["dynamic_des"] = desflow
    try:
        with importable(TWIN):
            import generator
            import routing
            import sim_logic
    finally:
        if before is None:
            del sys.modules["dynamic_des"]
        else:
            sys.modules["dynamic_des"] = before
    logging.getLogger("generator").setLevel(logging.WARNING)
    if not _PRISTINE:  # a run changes `mill_params` in place, so each run gets a copy
        _PRISTINE.append(copy.deepcopy(generator.mill_params))
    return generator, routing, sim_logic


def _steps() -> tuple[float, list[tuple[float, str, Any]]]:
    """The run's end and the scenario steps of `model.yaml`, which the dashboard sends."""
    document = read_document(HERE / "model.yaml", variables=VARIABLES)[0]
    steps = [(s["at"], s["path"], s["value"]) for s in document["scenario"]]
    return document["run"]["until"], steps


def generate(config: dict[str, Any]) -> types.SimpleNamespace:
    """Runs the twin's `run()` with the fixed draws to the end of the document's run.

    Args:
        config: An entry of `CONFIGS`; its `args` are the arguments of `run`.

    Returns:
        types.SimpleNamespace: `records` (what the twin published), `topics` (the
            topics and partitions it created, a name to a count) and `router` (its
            `custom_topic_router`).
    """
    generator, routing, sim_logic = _twin()
    until, steps = _steps()
    recorder = RecordingEgress()
    created: list[dict[str, Any]] = []
    _Uuid.count = 0
    _Sampler.seeds = []
    _Sampler.config = config

    class Environment(generator.DynamicRealtimeEnvironment):
        def __init__(self, factor: float = 1.0) -> None:
            super().__init__(
                factor=0.0,
                logical_start_time=_document_start(),
            )
            for at, path, value in steps:
                self.process(self._step(at, path, value))

        def _step(self, at: float, path: str, value: Any) -> Any:
            yield self.timeout(at)
            self.registry.update(path, value)

        def setup_ingress(self, connectors: Any) -> None:
            return None

        def setup_egress(self, connectors: Any) -> None:
            super().setup_egress([recorder], lag_monitor_interval=None)

        def run(self, until_: Any = None) -> Any:
            return super().run(until=until)

    class Admin:
        def __init__(self, **settings: Any) -> None:
            pass

        def create_topics(self, topics_config: list[dict[str, Any]]) -> None:
            created.extend(topics_config)

    with contextlib.ExitStack() as stack:
        for module, name, value in (
            (generator, "DynamicRealtimeEnvironment", Environment),
            (generator, "Sampler", _Sampler),
            (generator, "KafkaAdminConnector", Admin),
            (generator, "KafkaIngress", lambda **settings: None),
            (generator, "KafkaEgress", lambda **settings: None),
            (generator, "time", types.SimpleNamespace(sleep=lambda seconds: None)),
            (generator, "mill_params", copy.deepcopy(_PRISTINE[0])),
            (generator, "uuid", _Uuid),
            (sim_logic, "uuid", _Uuid),
        ):
            stack.enter_context(mock.patch.object(module, name, value))
        generator.run(
            seed=config["args"]["seed"],
            factor=0.0,
            variable_passes=config["args"]["variable_passes"],
            max_passes=config["args"]["max_passes"],
        )
    return types.SimpleNamespace(
        records=recorder.records,
        topics={t["name"]: t.get("partitions", 1) for t in created},
        router=routing.custom_topic_router,
        seeds=_Sampler.seeds,
    )


def _document_start() -> Any:
    """The `logical_start_time` of `model.yaml`, which the twin's environment takes too."""
    from datetime import datetime

    document = read_document(HERE / "model.yaml", variables=VARIABLES)[0]
    return datetime.fromisoformat(document["simulation"]["logical_start_time"])


def _project(
    records: list[dict[str, Any]], router: Any = None
) -> list[tuple[Any, ...]]:
    """The records of the twin (with its `router`) or of the YAML (none) as items that
    compare: (time, kind, key, ...) for a lifecycle record, a prediction or ground
    truth, and a wear reading, each with its topic, in the order of time and key.
    Slab ids are renamed by the order they first appear in a grade."""
    items: list[tuple[Any, ...]] = []
    for record in records:
        at = round(record["sim_ts"], 6)
        value = record["value"]
        if record["stream_type"] == "telemetry":
            if str(record["path_id"]).endswith(".current_cap"):
                topic = router(record) if router else None
                items.append(
                    (
                        at,
                        record["path_id"],
                        ("reading", record["path_id"], value, topic),
                    )
                )
            continue
        if record["stream_type"] != "event":
            continue
        key = re.sub(r"^(PRED|GT)-", "", record["key"])
        if router is None and record["task"] in CLOCKS:
            if "__topic__" in value:  # a wear reading of a clock task
                items.append(
                    (
                        at,
                        value["path_id"],
                        (
                            "reading",
                            value["path_id"],
                            value["value"],
                            value["__topic__"],
                        ),
                    )
                )
            continue
        if "status" in value:
            topic = router(record) if router else "sim-lifecycle"
            items.append(
                (at, key, ("life", key, value["path_id"], value["status"], topic))
            )
        else:
            topic = router(record) if router else value.get("__topic__")
            body = {k: v for k, v in value.items() if k != "__topic__"}
            items.append((at, key, ("event", key, body, topic)))
    items.sort(key=lambda item: (item[0], item[1]))  # stable: a key keeps its order
    ranks: dict[str, dict[str, int]] = {}

    def renamed(node: Any) -> Any:
        if isinstance(node, str):
            return ID.sub(
                lambda m: "SLAB-{}-{:08X}".format(
                    m.group(1),
                    ranks.setdefault(m.group(1), {}).setdefault(
                        m.group(2), len(ranks.get(m.group(1), {}))
                    ),
                ),
                node,
            )
        if isinstance(node, dict):
            return {k: renamed(v) for k, v in node.items()}
        if isinstance(node, tuple):
            return tuple(renamed(v) for v in node)
        return node

    return [(item[0], *renamed(item[2])) for item in items]


def _topics_of_the_yaml(config: dict[str, Any]) -> dict[str, int]:
    """The topics and partitions the YAML's Kafka sinks create when the run starts, as
    `KafkaIngress` and `KafkaEgress` ask for them (HR-23)."""
    app = SimulationContext.from_yaml(
        HERE / "model.yaml", variables={**VARIABLES, **config["variables"]}
    )
    created: list[dict[str, Any]] = []

    class Admin:
        def __init__(self, **settings: Any) -> None:
            pass

        def create_topics(self, topics_config: list[dict[str, Any]]) -> None:
            created.extend(topics_config)

    with (
        mock.patch("desflow.connectors.egress.kafka.KafkaAdminConnector", Admin),
        mock.patch("desflow.connectors.ingress.kafka.KafkaAdminConnector", Admin),
    ):
        for ingress in app._sinks.ingress:
            ingress._create_topic()
        for egress in [sink.provider for sink in app._sinks.egress]:
            if type(egress).__name__ == "KafkaEgress":
                egress._create_topics()
    return {t["name"]: t.get("partitions", 1) for t in created}


def compare(
    records: list[dict[str, Any]], result: types.SimpleNamespace, config: dict[str, Any]
) -> None:
    """Asserts that the YAML's records equal the twin's, and that the run reaches what
    the rows name.

    Args:
        records: The records of the YAML with the fixed draws.
        result: What `generate` returned.
        config: The entry of `CONFIGS` both ran.

    Raises:
        AssertionError: At the first item that differs, with both items.
    """
    mine = _project(records)
    theirs = _project(result.records, result.router)
    for number, (a, b) in enumerate(zip(mine, theirs)):
        assert a[0] == b[0] and harness.same_within(a[1:], b[1:]), (number, a, b)
    assert len(mine) == len(theirs), (len(mine), len(theirs))
    # HR-23: the same topics with the same partitions
    assert _topics_of_the_yaml(config) == result.topics
    assert result.topics["mill-predictions"] == 3
    # HR-24 and HR-25: every record of the YAML is valid for the twin's own record
    # models, with their range checks
    _, _, sim_logic = _twin()
    for record in records:
        value = record["value"]
        if record["stream_type"] == "event" and "event_type" in value:
            body = {k: v for k, v in value.items() if k != "__topic__"}
            if body["event_type"] == "prediction_request":
                sim_logic.PredictionRequestEvent.model_validate(body)
            elif body["event_type"] == "ground_truth":
                sim_logic.GroundTruthEvent.model_validate(body)
    # HR-26: the YAML's seed is the seed of the twin's run, and reaches its sampler
    document = read_document(
        HERE / "model.yaml", variables={**VARIABLES, **config["variables"]}
    )[0]
    assert int(document["simulation"]["random_seed"]) == config["args"]["seed"]
    import numpy

    assert result.seeds == [
        numpy.random.default_rng(config["args"]["seed"]).bit_generator.state
    ]
    _reaches(mine, config)


def _reaches(items: list[tuple[Any, ...]], config: dict[str, Any]) -> None:
    """The fixed draws reach every branch a row names (a check of the test, not of the
    twin): all four topics, every pass of a slab and its three bands of reduction, a
    wear of 0 and of more than 0, a wear that turns at 60 and at 0.001, a gradual step
    with a `freq` above 1, and the dashboard's abrupt step, whose wear above 60 stays as
    set. The configuration's `reaches` names a limit of the physics its draws reach:
    `limits`, each of `LIMITS`; `warm`, a ground truth with a wear above 0 for a pass
    whose entry temperature is above `WARM`."""
    topics = {item[-1] for item in items if item[1] in ("event", "reading", "life")}
    assert topics == {
        "mill-predictions",
        "mill-groundtruth",
        "sim-lifecycle",
        "sim-telemetry",
    }
    events = [item for item in items if item[1] == "event"]
    predictions = [
        item[3] for item in events if item[3]["event_type"] == "prediction_request"
    ]
    truths = [item for item in events if item[3]["event_type"] == "ground_truth"]
    passes = {p["identifiers"]["pass_number"] for p in predictions}
    assert passes == set(range(1, config["passes"] + 1))
    reductions = {round(p["features"]["reduction_pct"], 3) for p in predictions}
    assert reductions == {
        round(low + (high - low) * _fraction(low, high, config), 3)
        for low, high in BANDS
    }
    wear = {item[3]["ground_truth"]["wear_level"] for item in truths}
    assert 0.0 in wear and max(wear) > 0.0
    structural = [
        item[3]
        for item in items
        if item[1] == "reading" and "wear_structural" in item[2]
    ]
    peak = structural.index(max(structural))
    assert max(structural) == 60.0 and min(structural[peak:]) == 0.001
    # HR-18 and HR-19: a gradual step that acts only every `freq` seconds (above 1), and
    # the dashboard's abrupt step: a velocity with no `freq`, then a wear above 60 that
    # stays as set, because an abrupt velocity stops the drift of its grade
    values = [step[2] for step in _steps()[1] if isinstance(step[2], dict)]
    assert any(v["type"] == "gradual" and v["freq"] > 1 for v in values)
    assert {"type": "abrupt", "value": 0.0} in values
    microalloyed = [
        item[3]
        for item in items
        if item[1] == "reading" and "wear_microalloyed" in item[2]
    ]
    abrupt = [
        v for _, path, v in _steps()[1] if path.endswith("microalloyed.current_cap")
    ]
    assert abrupt[0] > 60.0 and abrupt[0] in microalloyed
    assert set(microalloyed[microalloyed.index(abrupt[0]) :]) == {abrupt[0]}
    if config.get("reaches") == "limits":
        for name, limit in LIMITS.items():
            assert any(p["features"][name] == limit for p in predictions), name
    if config.get("reaches") == "warm":
        temperature = {
            (p["identifiers"]["slab_id"], p["identifiers"]["pass_number"]): p[
                "features"
            ]["temperature_c"]
            for p in predictions
        }
        assert any(
            item[3]["ground_truth"]["wear_level"] > 0.0
            and temperature[
                (
                    item[3]["identifiers"]["slab_id"],
                    item[3]["identifiers"]["pass_number"],
                )
            ]
            > WARM
            for item in truths
        )
    ids = {m.group(2) for item in items for m in ID.finditer(str(item))}
    assert "00000000" in ids  # a slab id is a sequence of 8 hexadecimal digits
