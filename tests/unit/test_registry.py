import logging

import pytest

from desflow.core.registry import ParamChange, ParamRejected
from desflow.resources.container import DynamicContainer
from desflow.models.params import CapacityConfig, DistributionConfig, SimParameter


def test_registry_registration_paths(registry, sample_params):
    """Verify that SimParameter is flattened into the correct dot-notation paths."""
    registry.register_sim_parameter(sample_params)

    # Shop.arrival.standard (Exponential) -> .rate
    assert registry.get("Shop.arrival.standard.rate").value == 1 / 10.0

    # Shop.arrival.priority (Exponential) -> .rate
    assert registry.get("Shop.arrival.priority.rate").value == 1 / 50.0

    # Shop.service.setup (Normal) -> .mean, .std
    assert registry.get("Shop.service.setup.mean").value == 2.0
    assert registry.get("Shop.service.setup.std").value == 0.5

    # Shop.service.packing (Normal) -> .mean, .std
    assert registry.get("Shop.service.packing.mean").value == 5.0
    assert registry.get("Shop.service.packing.std").value == 1.2

    # Shop.resources.packers -> .current_cap, .max_cap
    assert registry.get("Shop.resources.packers.current_cap").value == 2
    assert registry.get("Shop.resources.packers.max_cap").value == 5

    # Shop.resources.operator -> .current_cap, .max_cap
    assert registry.get("Shop.resources.operator.current_cap").value == 1
    assert registry.get("Shop.resources.operator.max_cap").value == 3


def test_registry_config_sync(registry, sample_params):
    """Verify that updating a path also updates the parent config object attributes."""
    registry.register_sim_parameter(sample_params)

    # Get reference to the config object
    packing_cfg = registry.get_config("Shop.service.packing")
    packers_cfg = registry.get_config("Shop.resources.packers")

    # Update through registry
    registry.update("Shop.service.packing.mean", 99.0)
    registry.update("Shop.resources.packers.current_cap", 4)

    # The config objects themselves should be updated via reference
    assert packing_cfg.mean == 99.0
    assert packers_cfg.current_cap == 4


def test_registry_distribution_update_and_event(registry, sample_params):
    """Verify that updating a service path changes the value and signals the store."""
    registry.register_sim_parameter(sample_params)

    target_path = "Shop.service.setup.mean"
    dyn_val = registry.get(target_path)

    # Initially, the signal store should be empty
    assert len(dyn_val._signal.items) == 0

    # Update the value
    registry.update(target_path, 10.0)

    # 1. Value must be updated
    assert dyn_val.value == 10.0
    # 2. The signal store should now contain a signal
    assert len(dyn_val._signal.items) == 1


def test_registry_resource_update_and_event(registry, sample_params):
    """Verify that updating a resource path changes the value and signals the store."""
    registry.register_sim_parameter(sample_params)

    target_path = "Shop.resources.packers.current_cap"
    dyn_val = registry.get(target_path)

    # Initially, the signal store should be empty
    assert len(dyn_val._signal.items) == 0

    # Perform surgical update
    registry.update(target_path, 5)

    # 1. Value must be updated
    assert dyn_val.value == 5
    # 2. The signal store should now contain a signal
    assert len(dyn_val._signal.items) == 1


def test_registry_missing_path(registry):
    """Ensure that getting or updating a non-existent path behaves correctly."""
    with pytest.raises(KeyError):
        registry.get("Invalid.Path")

    with pytest.raises(KeyError):
        registry.get_config("Invalid.Config.Path")

    # Updating a missing path should not raise an error but should be handled/logged
    registry.update("Missing.Path", 10)  # Should print warning based on our logic


def test_registry_variables_registration_and_sync(registry):
    """Verify that custom variables are registered, synced via dict assignment, and signal events."""
    # Create a localized parameter with custom variables to avoid depending on sample_params
    params = SimParameter(
        sim_id="Warehouse",
        variables={"is_active": True, "speed_limit": 1.5, "mode": "auto"},
    )
    registry.register_sim_parameter(params)

    # 1. Check Registration Paths
    assert registry.get("Warehouse.variables.is_active").value is True
    assert registry.get("Warehouse.variables.speed_limit").value == 1.5
    assert registry.get("Warehouse.variables.mode").value == "auto"

    # 2. Check Config Retrieval (Should return the raw dictionary)
    var_dict = registry.get_config("Warehouse.variables")
    assert isinstance(var_dict, dict)
    assert var_dict["mode"] == "auto"

    # 3. Check Sync and Events
    target_path = "Warehouse.variables.speed_limit"
    dyn_val = registry.get(target_path)

    # Initially, the signal store should be empty
    assert len(dyn_val._signal.items) == 0

    # Update via registry
    registry.update(target_path, 2.5)

    # Validate DynamicValue updated and signaled
    assert dyn_val.value == 2.5
    assert len(dyn_val._signal.items) == 1

    # Validate underlying dictionary was synced correctly (testing the `isinstance(dict)` block)
    assert var_dict["speed_limit"] == 2.5


def test_registry_rate_update_keeps_its_fraction(registry):
    """A rate given as a whole number is not truncated by a fractional update."""
    params = SimParameter(
        sim_id="Line_C",
        arrival={"a": DistributionConfig(dist="exponential", rate=2)},
    )
    registry.register_sim_parameter(params)

    registry.update("Line_C.arrival.a.rate", 0.5)

    assert registry.get("Line_C.arrival.a.rate").value == 0.5
    assert registry.get_config("Line_C.arrival.a").rate == 0.5


@pytest.mark.parametrize(
    "path,value,old",
    [
        ("Shop.service.setup.std", -1.0, 0.5),
        ("Shop.arrival.standard.rate", 0.0, 0.1),
        ("Shop.arrival.standard.rate", -2.0, 0.1),
        ("Line_D.service.cast.mean", 0.0, 3.0),
        ("Line_D.service.cast.std", -0.5, 1.0),
    ],
)
def test_registry_refuses_an_invalid_distribution_update(
    registry, sample_params, caplog, path, value, old
):
    """An invalid parameter is refused with an error log line and the old value kept."""
    registry.register_sim_parameter(sample_params)
    registry.register_sim_parameter(
        SimParameter(
            sim_id="Line_D",
            service={"cast": DistributionConfig(dist="lognormal", mean=3.0, std=1.0)},
        )
    )
    parent_path, attr = path.rsplit(".", 1)
    dyn_val = registry.get(path)

    with caplog.at_level(logging.ERROR, logger="desflow.core.registry"):
        registry.update(path, value)

    assert dyn_val.value == old
    assert getattr(registry.get_config(parent_path), attr) == old
    assert len(dyn_val._signal.items) == 0
    assert path in caplog.text
    assert "Update ignored" in caplog.text


def test_registry_accepts_a_zero_std(registry, sample_params):
    """A std of 0 is valid: the distribution becomes a constant."""
    registry.register_sim_parameter(sample_params)
    registry.update("Shop.service.setup.std", 0)

    assert registry.get("Shop.service.setup.std").value == 0.0


def test_registry_clamps_current_cap_to_max_cap(registry, sample_params, caplog):
    """A current_cap above max_cap is stored as max_cap, which is what the resource uses."""
    registry.register_sim_parameter(sample_params)

    with caplog.at_level(logging.WARNING, logger="desflow.core.registry"):
        registry.update("Shop.resources.packers.current_cap", 7)

    assert registry.get("Shop.resources.packers.current_cap").value == 5
    assert registry.get_config("Shop.resources.packers").current_cap == 5
    assert "7" in caplog.text and "5" in caplog.text


def test_a_current_cap_above_max_cap_is_clamped_when_registered(registry, caplog):
    """A low-level SimParameter with current_cap above max_cap is clamped with a
    warning when it registers, as a live update is."""
    params = SimParameter(
        sim_id="Shop", resources={"packers": CapacityConfig(current_cap=2, max_cap=1)}
    )
    with caplog.at_level(logging.WARNING, logger="desflow.core.registry"):
        registry.register_sim_parameter(params)

    assert registry.get("Shop.resources.packers.current_cap").value == 1
    assert params.resources["packers"].current_cap == 1
    assert "current_cap 2" in caplog.text and "Stored as 1" in caplog.text


def test_registry_lowers_current_cap_with_a_lower_max_cap(
    registry, sample_params, caplog
):
    """A max_cap below current_cap clamps current_cap, and a later level reads it."""
    registry.register_sim_parameter(sample_params)

    with caplog.at_level(logging.WARNING, logger="desflow.core.registry"):
        registry.update("Shop.containers.tank1.max_cap", 30.0)
    registry.update("Shop.containers.tank1.level", 40.0)

    assert registry.get("Shop.containers.tank1.current_cap").value == 30.0
    assert registry.get_config("Shop.containers.tank1").current_cap == 30.0
    assert registry.get("Shop.containers.tank1.level").value == 30.0
    assert "above the new max_cap 30.0" in caplog.text


def test_registry_clamps_negative_current_cap_to_zero(registry, sample_params, caplog):
    """A negative current_cap is stored as 0."""
    registry.register_sim_parameter(sample_params)

    with caplog.at_level(logging.WARNING, logger="desflow.core.registry"):
        registry.update("Shop.containers.tank1.current_cap", -3.5)

    assert registry.get("Shop.containers.tank1.current_cap").value == 0.0
    assert "-3.5" in caplog.text


def test_registry_keeps_a_container_fraction_within_max_cap(registry, sample_params):
    """A container keeps a fractional capacity that is within max_cap."""
    registry.register_sim_parameter(sample_params)
    registry.update("Shop.containers.tank1.current_cap", 42.5)

    assert registry.get("Shop.containers.tank1.current_cap").value == 42.5


@pytest.mark.parametrize(
    "config,paths",
    [
        (DistributionConfig(dist="exponential", rate=1.0), {"rate"}),
        (DistributionConfig(dist="normal", mean=3.0), {"mean", "std"}),
        (DistributionConfig(dist="truncated_normal", mean=3.0), {"mean", "std", "low"}),
        (DistributionConfig(dist="constant", value=3.0), {"value"}),
        (DistributionConfig(dist="uniform", low=1, high=3), {"low", "high"}),
        (
            DistributionConfig(dist="triangular", low=1, mode=2, high=4),
            {"low", "mode", "high"},
        ),
        (DistributionConfig(dist="gamma", shape=2, scale=1), {"shape", "scale"}),
        (DistributionConfig(dist="weibull", shape=2, scale=1), {"shape", "scale"}),
        # Its lists are taken at load, so nothing changes live
        (DistributionConfig(dist="empirical", values=[1, 2], weights=[1, 1]), set()),
    ],
    ids=lambda value: value.dist if isinstance(value, DistributionConfig) else "",
)
def test_each_family_registers_one_path_per_parameter(registry, config, paths):
    registry.register_sim_parameter(SimParameter(sim_id="Shop", service={"x": config}))

    registered = {
        path.rsplit(".", 1)[1]
        for path in registry._values
        if path.startswith("Shop.service.x.")
    }
    assert registered == paths
    # The family is fixed at load, so a parameter it does not take has no path
    registry.update("Shop.service.x.dist", "normal")
    assert registry.get_config("Shop.service.x").dist == config.dist
    for name in paths:
        current = getattr(config, name)
        registry.update(f"Shop.service.x.{name}", current + 1)
        assert getattr(registry.get_config("Shop.service.x"), name) == current + 1
    # Its sequence is named by its path without the sim id
    assert config.stream == "service.x"


def test_registry_casts_or_ignores_a_wrong_type_update(registry, sample_params, caplog):
    """A wrong-type live update is cast to the stored value's type, or ignored when
    it cannot be cast, keeping the old value."""
    registry.register_sim_parameter(sample_params)
    path = "Shop.resources.packers.max_cap"  # an int, 5
    registry.update(path, "4")
    assert registry.get(path).value == 4
    with caplog.at_level(logging.ERROR):
        registry.update(path, "not a number")
    assert registry.get(path).value == 4
    assert "Update ignored" in caplog.text


@pytest.mark.parametrize(
    "config,expected",
    [
        (CapacityConfig(), (1, 1)),
        (CapacityConfig(current_cap=3), (3, 3)),
        (CapacityConfig(max_cap=4), (1, 4)),
        (CapacityConfig(current_cap=2, max_cap=5), (2, 5)),
    ],
)
def test_a_resource_left_without_a_capacity_has_one_unit(registry, config, expected):
    """A resource's `current_cap` defaults to 1 and its `max_cap` to `current_cap`;
    the config keeps the values in effect."""
    registry.register_resource("Shop", "bench", config)
    assert (config.current_cap, config.max_cap) == expected
    assert registry.get("Shop.resources.bench.current_cap").value == expected[0]
    assert registry.get("Shop.resources.bench.max_cap").value == expected[1]


def test_a_resource_with_only_a_max_cap_below_one_is_refused(registry):
    with pytest.raises(ValueError, match="max_cap 0.5 and no current_cap"):
        registry.register_resource("Shop", "bench", CapacityConfig(max_cap=0.5))


def test_a_raise_above_the_default_max_cap_is_clamped(registry, caplog):
    registry.register_resource("Shop", "bench", CapacityConfig())
    with caplog.at_level(logging.WARNING, logger="desflow.core.registry"):
        registry.update("Shop.resources.bench.current_cap", 4)
    assert registry.get("Shop.resources.bench.current_cap").value == 1
    assert "outside [0, max_cap 1]" in caplog.text


def test_update_logs_each_refusal_at_its_level(registry, sample_params, caplog):
    """Each refusal writes one log line, at its level, with its exact text."""
    registry.register_sim_parameter(sample_params)

    with caplog.at_level(logging.DEBUG, logger="desflow.core.registry"):
        registry.update("Shop.resources.lath.current_cap", 3)
        registry.update("Shop.resources.packers.max_cap", "not a number")
        registry.update("Shop.service.packing.std", -1.0)

    lines = [(r.levelno, r.getMessage()) for r in caplog.records]
    assert lines == [
        (
            logging.WARNING,
            "Attempted to update non-existent path: Shop.resources.lath.current_cap",
        ),
        (
            logging.ERROR,
            "Type mismatch for 'Shop.resources.packers.max_cap'. Expected int, "
            "got str with value 'not a number'. Update ignored.",
        ),
        (
            logging.ERROR,
            "Invalid value for 'Shop.service.packing.std': "
            "normal std must be 0 or more, got -1.0. Update ignored.",
        ),
    ]


def test_update_returns_none(registry, sample_params):
    """`update` returns None for an accepted, a clamped and a refused value."""
    registry.register_sim_parameter(sample_params)

    assert registry.update("Shop.resources.packers.current_cap", 3) is None
    assert registry.update("Shop.resources.packers.current_cap", 99) is None
    assert registry.update("Shop.service.packing.std", -1.0) is None


def _record(registry):
    """Adds a callback to `registry` and returns the list it fills."""
    seen: list = []
    registry.on_change.append(seen.append)
    return seen


def test_change_returns_what_was_applied(env, registry, sample_params):
    """`change` reports the old value, the stored value and the time."""
    registry.register_sim_parameter(sample_params)
    env.run(until=3)

    change = registry.change(
        "Shop.resources.packers.current_cap",
        4,
        source="user",
        actor="alice",
        reason="busy day",
    )

    assert change == ParamChange(
        path="Shop.resources.packers.current_cap",
        old=2,
        new=4,
        requested=4,
        at=env.now,
        source="user",
        actor="alice",
        reason="busy day",
    )
    assert change.at == 3
    assert change.changed is True
    assert registry.get("Shop.resources.packers.current_cap").value == 4


def test_change_accepts_a_cast_and_a_clamp(registry, sample_params):
    """A cast or a clamp is accepted, and `new` shows the value stored."""
    registry.register_sim_parameter(sample_params)

    cast = registry.change("Shop.resources.packers.max_cap", "4")
    assert (cast.requested, cast.new) == ("4", 4)

    clamped = registry.change("Shop.resources.packers.current_cap", 99)
    assert (clamped.requested, clamped.new) == (99, 4)


@pytest.mark.parametrize(
    "path,value,fragment",
    [
        ("Shop.resources.lath.current_cap", 3, "is not a registry path"),
        ("Shop.resources.packers.max_cap", "x", "cannot be converted"),
        ("Shop.service.packing.std", -1.0, "invalid value for"),
    ],
)
def test_change_raises_for_each_refusal(
    registry, sample_params, caplog, path, value, fragment
):
    """Each refusal raises `ParamRejected`, keeps the value and logs nothing."""
    registry.register_sim_parameter(sample_params)
    seen = _record(registry)
    before = registry._values[path].value if path in registry._values else None

    with caplog.at_level(logging.DEBUG, logger="desflow.core.registry"):
        with pytest.raises(ParamRejected) as raised:
            registry.change(path, value)

    exc = raised.value
    assert isinstance(exc, ValueError)
    assert (exc.path, exc.value) == (path, value)
    assert fragment in exc.reason and str(exc) == exc.reason
    assert path in exc.reason
    assert caplog.records == []
    assert seen == []
    if path in registry._values:
        assert registry.get(path).value == before


def test_change_names_the_closest_path_for_a_misspelling(registry, sample_params):
    """The reason of an unknown path carries the suggestion `set_exactly` gives."""
    registry.register_sim_parameter(sample_params)
    with pytest.raises(ParamRejected) as raised:
        registry.change("Shop.resources.packer.current_cap", 3)
    assert "Shop.resources.packers.current_cap" in raised.value.reason


def test_update_keeps_its_behaviour(registry, sample_params, caplog):
    """`update` still logs a refusal and returns None, with keywords it ignores."""
    registry.register_sim_parameter(sample_params)
    with caplog.at_level(logging.WARNING, logger="desflow.core.registry"):
        result = registry.update("Shop.nowhere.x", 1, source="user", actor="a")
    assert result is None
    assert [r.getMessage() for r in caplog.records] == [
        "Attempted to update non-existent path: Shop.nowhere.x"
    ]


def test_on_change_receives_every_accepted_change(registry, sample_params):
    """An equal value is reported with `changed` False; a refused update is not."""
    registry.register_sim_parameter(sample_params)
    seen = _record(registry)
    path = "Shop.resources.packers.current_cap"

    registry.update(path, 3, source="ingress", actor="LocalIngress")
    registry.update(path, 3)
    registry.update("Shop.nowhere.x", 1)
    registry.update("Shop.service.packing.std", -1.0)

    assert [(c.old, c.new, c.changed) for c in seen] == [
        (2, 3, True),
        (3, 3, False),
    ]
    assert (seen[0].source, seen[0].actor) == ("ingress", "LocalIngress")
    assert (seen[1].source, seen[1].actor) == (None, None)


def test_a_callback_reads_the_registry_already_changed(registry, sample_params):
    """The value and its config attribute are stored before a callback runs."""
    registry.register_sim_parameter(sample_params)
    path = "Shop.resources.packers.current_cap"
    config = registry.get_config("Shop.resources.packers")
    read: list = []
    registry.on_change.append(
        lambda change: read.append((registry.get(path).value, config.current_cap))
    )

    registry.change(path, 4)

    assert read == [(4, 4)]


def test_a_lower_max_cap_reports_current_cap_first(registry, sample_params):
    """A lower max_cap that lowers current_cap gives two changes, current_cap first."""
    registry.register_sim_parameter(sample_params)
    seen = _record(registry)

    returned = registry.change(
        "Shop.resources.packers.max_cap", 1, source="user", reason="cut"
    )

    assert [c.path for c in seen] == [
        "Shop.resources.packers.current_cap",
        "Shop.resources.packers.max_cap",
    ]
    first, second = seen
    assert (first.old, first.new, first.requested) == (2, 1, 1)
    assert (first.source, first.reason) == ("user", "cut")
    assert (second.old, second.new) == (5, 1)
    assert returned == second


def test_a_failing_callback_is_logged_and_the_rest_run(registry, sample_params, caplog):
    """A callback that raises is logged, and the next one runs, for `change` and `update`."""
    registry.register_sim_parameter(sample_params)

    def broken(change):
        raise RuntimeError("boom")

    registry.on_change.append(broken)
    seen = _record(registry)
    path = "Shop.resources.packers.current_cap"

    with caplog.at_level(logging.ERROR, logger="desflow.core.registry"):
        registry.change(path, 3)
        registry.update(path, 4)

    assert [c.new for c in seen] == [3, 4]
    assert registry.get(path).value == 4
    failures = [r for r in caplog.records if "on_change callback" in r.getMessage()]
    assert len(failures) == 2
    assert all(f"failed for '{path}'" in r.getMessage() for r in failures)
    assert all(r.exc_info for r in failures)


def test_a_callback_that_changes_the_registry(registry, sample_params):
    """A nested change and its callbacks run before the outer change's remaining ones."""
    registry.register_sim_parameter(sample_params)
    order: list = []

    def first(change):
        order.append(("first", change.path))
        if change.path == "Shop.resources.packers.current_cap":
            registry.change("Shop.resources.operator.current_cap", 2)

    registry.on_change.append(first)
    registry.on_change.append(lambda change: order.append(("second", change.path)))

    registry.change("Shop.resources.packers.current_cap", 3)

    assert order == [
        ("first", "Shop.resources.packers.current_cap"),
        ("first", "Shop.resources.operator.current_cap"),
        ("second", "Shop.resources.operator.current_cap"),
        ("second", "Shop.resources.packers.current_cap"),
    ]


def test_a_hook_error_is_an_exception_and_never_ends_the_process(
    registry, sample_params
):
    """A refused `change` raises an ordinary exception, never `SystemExit`."""
    registry.register_sim_parameter(sample_params)
    with pytest.raises(Exception) as raised:
        registry.change("Shop.service.packing.std", -1.0)
    assert isinstance(raised.value, ParamRejected)
    assert not isinstance(raised.value, SystemExit)


def test_set_exactly_passes_its_source(registry, sample_params):
    """`set_exactly` hands its source, actor and reason to the change."""
    registry.register_sim_parameter(sample_params)
    seen = _record(registry)

    registry.set_exactly(
        "Shop.resources.packers.current_cap", 3, source="s", actor="a", reason="r"
    )

    assert [(c.source, c.actor, c.reason) for c in seen] == [("s", "a", "r")]


def test_a_container_level_move_calls_no_callback(env, registry, sample_params):
    """A container's own put changes its level without a parameter change."""
    registry.register_sim_parameter(sample_params)
    cont = DynamicContainer(env, "Shop", "tank1", init=10.0)
    seen = _record(registry)

    def put():
        yield cont.put(20.0)

    env.process(put())
    env.run(until=1)

    assert cont.level == 30.0
    assert seen == []


def test_param_rejected_survives_pickling():
    """A worker process of `replicate` sends the exception back by pickling it."""
    import pickle

    exc = ParamRejected("Shop.arrival.orders.rate", 0, "rate must be greater than 0")
    exc._level = logging.WARNING
    exc._log = "the update log line"

    copy = pickle.loads(pickle.dumps(exc))

    assert isinstance(copy, ParamRejected)
    assert (copy.path, copy.value, copy.reason) == (exc.path, exc.value, exc.reason)
    assert str(copy) == "rate must be greater than 0"
    assert (copy._level, copy._log) == (logging.WARNING, "the update log line")


def test_a_refusal_inside_a_process_ends_the_run_as_param_rejected(
    env, registry, sample_params
):
    """SimPy rebuilds the exception that ends a process from its `args`, so the
    caller of `env.run` still gets a `ParamRejected` with its path, value and reason."""
    registry.register_sim_parameter(sample_params)

    def model():
        yield env.timeout(1)
        registry.change("Shop.service.packing.std", -1)

    env.process(model())
    with pytest.raises(ParamRejected) as raised:
        env.run(until=5)

    assert raised.value.path == "Shop.service.packing.std"
    assert raised.value.value == -1
    assert str(raised.value) == raised.value.reason
    assert "std" in raised.value.reason
