"""The registry paths and telemetry of the Change Parameters guide.

Builds a small `Line_A`, prints the parameters it registers, and runs it unpaced for a
few seconds so the telemetry loop publishes the lathe's capacity.
"""

from dynamic_des import ConsoleEgress, SimulationContext

app = (
    SimulationContext(sim_id="Line_A", factor=0.0)
    .add_resource("lathe", current_cap=1, max_cap=10)
    .add_service("milling", dist="normal", mean=2.0, std=0.2)
    .add_arrival("standard", dist="exponential", rate=0.5)
    .add_egress(ConsoleEgress())
)

# --8<-- [start:compile]
print(app.compile_parameters())
# --8<-- [end:compile]


# --8<-- [start:telemetry]
@app.telemetry_loop(interval=2.0)
def lathe_metrics(context):
    context.publish("lathe.capacity", context.get_resource("lathe").capacity)


# --8<-- [end:telemetry]


if __name__ == "__main__":
    app.run(until=6)
