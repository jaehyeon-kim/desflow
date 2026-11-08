"""Every time place refuses NaN, a negative number and `true` by name.

Infinity is refused where a time must end (`factor`, `initial_time`, a scenario
`at`, a segment `every`), and kept as no limit or never elsewhere, as a patience, a
wait timeout, a warm-up or a lag interval of `.inf` ran before.

One check, `desflow.utils.checked_seconds`, is called from every place that takes
seconds, so the YAML load, the Builder and the CLI give the same refusal. Each case
runs under a wall-clock limit: a run that hangs fails the test.
"""

import pytest
import yaml as pyyaml

from desflow.core.context import SimulationContext
from desflow.utils import _seconds

BAD = [float("nan"), float("inf"), -1, True]

BASE = {
    "simulation": {"sim_id": "S", "factor": 0, "random_seed": 1},
    "resources": {"desk": {"current_cap": 1, "max_cap": 4}},
    "services": {"work": {"dist": "exponential", "rate": 1.0}},
    "arrivals": {"orders": {"dist": "exponential", "rate": 0.5, "spawn": "serve"}},
    "tasks": {"serve": {"service": "work", "resource": "desk", "payload": {}}},
    "run": {"until": 20},
}


def _model(edit):
    import copy

    model = copy.deepcopy(BASE)
    edit(model)
    return model


YAML_PLACES = {
    "factor": lambda v: lambda m: m["simulation"].update(factor=v),
    "initial_time": lambda v: lambda m: m["simulation"].update(initial_time=v),
    "warmup": lambda v: lambda m: m["simulation"].update(warmup=v),
    "lag_interval": lambda v: lambda m: m["simulation"].update(lag_interval=v),
    "patience": lambda v: lambda m: m["tasks"]["serve"].update(patience=v),
    "wait timeout": lambda v: lambda m: m["tasks"]["serve"].update(wait={"timeout": v}),
    "scenario at": lambda v: (
        lambda m: m.update(
            scenario=[{"at": v, "path": "S.resources.desk.current_cap", "value": 2}]
        )
    ),
    "segment until": lambda v: (
        lambda m: m["run"].update(segments=[{"until": v}, {"until": 8}])
    ),
    "segment every": lambda v: lambda m: m["run"].update(segments={"every": v}),
}


# Places where infinity is no limit or never, and loads
NO_LIMIT = {"warmup", "lag_interval", "patience", "wait timeout"}


@pytest.mark.parametrize("place", sorted(YAML_PLACES))
@pytest.mark.parametrize("bad", BAD, ids=["nan", "inf", "minus1", "true"])
def test_yaml_refuses_a_bad_time(tmp_path, place, bad):
    if place == "initial_time" and bad == -1:
        pytest.skip("initial_time may be below 0")
    path = tmp_path / "m.yaml"
    path.write_text(pyyaml.safe_dump(_model(YAML_PLACES[place](bad))))
    if place in NO_LIMIT and bad == float("inf"):
        SimulationContext.from_yaml(path)
        return
    with pytest.raises(Exception) as caught:
        SimulationContext.from_yaml(path)
    text = str(caught.value)
    assert "m.yaml:" in text or "m.yaml" in text
    assert "SimPy" not in text


@pytest.mark.parametrize("bad", BAD, ids=["nan", "inf", "minus1", "true"])
@pytest.mark.parametrize("kwarg", ["factor", "warmup", "lag_interval"])
def test_builder_constructor_refuses_a_bad_time(kwarg, bad):
    if kwarg != "factor" and bad == float("inf"):
        assert getattr(SimulationContext("S", **{kwarg: bad}), kwarg) == bad
        return
    with pytest.raises(ValueError, match=kwarg):
        SimulationContext("S", **{kwarg: bad})


@pytest.mark.parametrize("bad", BAD, ids=["nan", "inf", "minus1", "true"])
def test_builder_initial_time_refuses_nan_infinity_and_true(bad):
    if bad == -1:
        SimulationContext("S", initial_time=bad)
        return
    with pytest.raises(ValueError, match="initial_time"):
        SimulationContext("S", initial_time=bad)


def _app():
    app = SimulationContext("S", factor=0, random_seed=1)
    app.add_arrival("orders", "constant", value=1, spawn="job", max_events=3)
    app.task(service_time=0, name="job")(lambda task_id: {})
    return app


@pytest.mark.parametrize("bad", [float("nan"), -1, True], ids=["nan", "minus1", "true"])
def test_builder_task_refuses_a_bad_literal_time(bad):
    app = SimulationContext("S", factor=0)
    with pytest.raises(ValueError, match="service_time"):
        app.task(name="a", service_time=bad)(lambda task_id: {})
    with pytest.raises(ValueError, match="patience"):
        app.task(name="b", service_time=1, resource_id=None, patience=bad)(
            lambda task_id: {}
        )


@pytest.mark.parametrize("bad", [0, -1, float("nan")], ids=["zero", "minus1", "nan"])
def test_telemetry_loop_refuses_a_bad_interval(bad):
    with pytest.raises(ValueError, match="interval"):
        SimulationContext("S", factor=0).telemetry_loop(bad)


@pytest.mark.parametrize("bad", [0, -1, float("nan")], ids=["zero", "minus1", "nan"])
def test_drain_limit_refuses_a_bad_limit(bad):
    app = _app()
    with pytest.raises(ValueError, match="drain limit"):
        app.start(until=3, drain={"limit": bad})


@pytest.mark.parametrize("bad", [float("nan"), True, -1])
def test_start_and_advance_refuse_a_bad_until(bad):
    app = _app()
    with pytest.raises(ValueError, match="until"):
        app.run(until=bad)
    app = _app()
    app.start(until=10)
    with pytest.raises(ValueError, match="until"):
        app.advance(bad)
    app.finish()


def test_signal_fire_refuses_a_bad_after():
    app = SimulationContext("S", factor=0)
    app.add_signal("go")
    app.start(until=5)
    for bad in (float("nan"), True):
        with pytest.raises(ValueError, match="after"):
            app.signal("go").fire(1, after=bad)
    app.finish()


def test_a_replay_time_that_is_not_finite_is_refused():
    from desflow.core.replay import read_trace

    with pytest.raises(ValueError, match="no valid time"):
        read_trace([{"time": "5"}, {"time": "nan"}, {"time": "1"}])


def test_a_plain_number_in_text_is_seconds():
    from desflow.utils import checked_seconds

    assert _seconds("1e-3") == 0.001
    assert _seconds("5") == 5.0
    assert checked_seconds("10 min", "x") == 600


@pytest.mark.parametrize(
    "setting, bad",
    [
        ("flush_interval", 0),
        ("flush_interval", float("nan")),
        ("flush_interval", -1),
        ("drain_stall_seconds", -1),
        ("batch_size", 0),
        ("batch_size", -1),
        ("batch_size", True),
        ("max_queued_batches", 0),
    ],
)
def test_builder_batching_refuses_a_bad_setting(setting, bad):
    given = {"batch_size": 10, "flush_interval": 1.0}
    given[setting] = bad
    with pytest.raises(ValueError, match=setting):
        SimulationContext("S", factor=0).with_batching(**given)


@pytest.mark.parametrize(
    "setting, bad", [("flush_interval", 0), ("batch_size", True), ("batch_size", 0)]
)
def test_builder_egress_refuses_a_bad_setting(setting, bad):
    with pytest.raises(ValueError, match=setting):
        SimulationContext("S", factor=0).add_egress(object(), **{setting: bad})


@pytest.mark.parametrize(
    "edit",
    [
        lambda m: m.update(telemetry=[{"interval": True, "readings": {"x": "1"}}]),
        lambda m: m.update(batching={"batch_size": True, "flush_interval": 1}),
        lambda m: m.update(
            egress=[{"type": "Console", "config": {}, "batch_size": True}]
        ),
    ],
    ids=["telemetry interval", "batching batch_size", "egress batch_size"],
)
def test_yaml_refuses_true_for_a_size_or_an_interval(tmp_path, edit):
    path = tmp_path / "m.yaml"
    path.write_text(pyyaml.safe_dump(_model(edit)))
    with pytest.raises(Exception, match="m.yaml"):
        SimulationContext.from_yaml(path)
