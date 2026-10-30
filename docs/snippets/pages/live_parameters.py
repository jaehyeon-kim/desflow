"""The registry paths and readings of the Live Parameters page.

Builds a small `Shop`, prints the parameters it registers, and runs it unpaced for a
few seconds so the statistics publish the packers' capacity.
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

# --8<-- [start:paths]
print(app.registry_paths())
# --8<-- [end:paths]


# --8<-- [start:telemetry]
# The packers' capacity, in use, queue and utilization every 2 seconds
app.with_stats("minimal", interval=2.0)
# --8<-- [end:telemetry]


if __name__ == "__main__":
    app.run(until=6)
