"""A container built in a process, as the Resources page shows it for the builder.

tests/unit runs this file unpaced.
"""

# --8<-- [start:container]
from desflow import DynamicContainer, SimulationContext

# run() creates the packers; the container is only registered
app = (
    SimulationContext(sim_id="Shop")
    .add_resource("packers", current_cap=2, max_cap=5)
    .add_container("box_stock", current_cap=100.0, max_cap=500.0)
)


def restock(context):
    # The container reads its capacity from Shop.containers.box_stock
    stock = DynamicContainer(context.env, "Shop", "box_stock", init=50.0)
    while True:
        yield context.env.timeout(10.0)
        yield stock.put(25.0)
        context.publish("box_stock.level", stock.level)


app.add_process(restock)
# --8<-- [end:container]

if __name__ == "__main__":
    app.factor = 0.0
    app.run(until=25)
