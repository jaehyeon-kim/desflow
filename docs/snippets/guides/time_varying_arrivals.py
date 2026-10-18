"""The Time-varying Arrivals guide: what a scenario step does at its boundary.

python time_varying_arrivals.py prints the arrival times around a step, and the
gap in force after a run whose until is the step's time.
"""

from desflow import SimulationContext


# --8<-- [start:boundary]
def arrivals(until: float) -> tuple:
    """Orders every 10 s, every 5 s from 30 s: the arrival times, and the gap in
    force at the end."""
    app = (
        SimulationContext("Shop", factor=0)
        .add_arrival("orders", dist="constant", value=10)
        .add_step(30, path="Shop.arrival.orders.value", value=5)
    )
    times = []

    @app.arrival_loop("orders")
    def order_loop(context):
        while True:
            yield context.wait_for_arrival("orders")
            times.append(context.env.now)

    app.run(until=until)
    return times, app.env.registry.get("Shop.arrival.orders.value").value


# --8<-- [end:boundary]

if __name__ == "__main__":
    print("until 50:", arrivals(50))
    print("until 30:", arrivals(30))
