"""The publishing calls of the Records and Telemetry page.

tests/docs runs this file unpaced with a console sink, so every record shown is
built for real.
"""

from datetime import datetime

from pydantic import BaseModel

from desflow import ConsoleEgress, DynamicRealtimeEnvironment, SimulationContext

app = SimulationContext(sim_id="Shop", factor=0.0).add_egress(ConsoleEgress())
app.add_resource("packers", current_cap=1, max_cap=1)


# --8<-- [start:telemetry]
@app.telemetry_loop(interval=10.0)
def report(context: SimulationContext):
    # path_id is Shop.packers_free: the sim_id is added in front
    packers = context.get_resource("packers")
    context.publish("packers_free", packers.capacity - packers.in_use)


# --8<-- [end:telemetry]


# --8<-- [start:values]
class Reading(BaseModel):
    sensor: str
    celsius: float
    taken_at: datetime


def readings(context: SimulationContext):
    while True:
        yield context.env.timeout(30.0)
        reading = Reading(
            sensor="spindle", celsius=float("nan"), taken_at=datetime.now()
        )
        # The model becomes a dict, taken_at an ISO string, and NaN becomes None
        context.env.publish_event("sensor-spindle", reading)


# --8<-- [end:values]


app.add_process(readings)

# --8<-- [start:low-level]
env = DynamicRealtimeEnvironment(factor=0.0)
env.setup_egress([ConsoleEgress()])
env.publish_telemetry("plant.power_kw", 412.5)  # path_id exactly as given
# key, task, instance and value as given; task and instance are optional
env.publish_event("orders-17", {"status": "shipped"}, task="ship", instance=16)
# A ground-truth label, which only a when: labels sink receives
env.publish_label({"fault": "slow_packing"}, path="Shop.service.packing.scale")
# --8<-- [end:low-level]

if __name__ == "__main__":
    try:
        env.run(until=1)
    finally:
        env.teardown()
    app.run(until=60)
