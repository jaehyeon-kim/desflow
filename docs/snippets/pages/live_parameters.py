"""The registry paths and telemetry of the Change Parameters guide.

Builds a small `Shop`, prints the parameters it registers, and runs it unpaced for a
few seconds so the telemetry loop publishes the packers's capacity.
"""

from desflow import ConsoleEgress, SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=0.0)
    .add_resource("packers", current_cap=1, max_cap=10)
    .add_service("packing", dist="normal", mean=2.0, std=0.2)
    .add_arrival("orders", dist="exponential", rate=0.5)
    .add_egress(ConsoleEgress())
)

# --8<-- [start:compile]
print(app.compile_parameters())
# --8<-- [end:compile]


# --8<-- [start:telemetry]
@app.telemetry_loop(interval=2.0)
def packer_metrics(context):
    context.publish("packers.capacity", context.get_resource("packers").capacity)


# --8<-- [end:telemetry]


if __name__ == "__main__":
    app.run(until=6)
