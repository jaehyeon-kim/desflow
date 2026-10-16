import logging

import pytest

from desflow.models.params import DistributionConfig, SimParameter


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
