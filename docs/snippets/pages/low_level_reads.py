"""The reads of the Low-level API page: a registry value, an expression and a trace.

tests/docs runs the file, so every call shown on the page is checked.
"""

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicValue,
    Expression,
    Row,
    Scope,
    SimParameter,
    read_trace,
)

# --8<-- [start:value]
env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop",
        resources={"packers": CapacityConfig(current_cap=2, max_cap=5)},
    )
)
packers: DynamicValue = env.registry.get("Shop.resources.packers.current_cap")
print(packers.name, packers.value)
# --8<-- [end:value]

# --8<-- [start:expression]
rule = Expression("payload.total > 100 and t >= 60")
scope = Scope(payload={"total": 120}, now=75.0)
print(rule.evaluate(scope))
# --8<-- [end:expression]

# --8<-- [start:trace]
rows: list[Row] = read_trace(
    [{"time": 0.5, "id": "a"}, {"time": 2.0, "id": "b"}], fields=["id"]
)
for row in rows:
    print(row.number, row.time, row.fields)
# --8<-- [end:trace]
