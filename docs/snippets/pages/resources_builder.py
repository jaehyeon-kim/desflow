"""A container built in a process, as the Resources page shows it for the builder.

tests/unit runs this file unpaced.
"""

# --8<-- [start:container]
from dynamic_des import DynamicContainer, SimulationContext

# run() creates the lathe; the container is only registered
app = (
    SimulationContext(sim_id="Line_A")
    .add_resource("lathe", current_cap=2, max_cap=5)
    .add_container("fuel_tank", current_cap=100.0, max_cap=500.0)
)


def refuel(context):
    # The container reads its capacity from Line_A.containers.fuel_tank
    tank = DynamicContainer(context.env, "Line_A", "fuel_tank", init=50.0)
    while True:
        yield context.env.timeout(10.0)
        yield tank.put(25.0)
        context.publish("fuel_tank.level", tank.level)


app.add_process(refuel)
# --8<-- [end:container]

if __name__ == "__main__":
    app.factor = 0.0
    app.run(until=25)
