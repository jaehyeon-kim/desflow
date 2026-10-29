"""Check Against Theory: the shop of check_against_theory.yaml as a builder file.

Check it with:
desflow explain check_against_theory_shop.py --verify --replications 10 --until 20000
"""

import os

from desflow import SimulationContext

# --8<-- [start:shop]
# An order every 1.33 s on average (rate 0.75) with one packer; RATE=1.5 with PACKERS=2
rate = float(os.environ.get("RATE", "0.75"))
packers = int(os.environ.get("PACKERS", "1"))

app = (
    # warmup: seconds left out of every measure, while the queue fills
    SimulationContext(sim_id="Shop", factor=0, random_seed=42, warmup=2000)
    .add_arrival("orders", dist="exponential", rate=rate, spawn="pack_order")
    .add_service("packing", dist="exponential", rate=1)  # 1 second on average
    .add_resource("packers", current_cap=packers, max_cap=4)
)


@app.task(service_id="packing", resource_id="packers")
def pack_order(task_id):
    return {"event_type": "order_packed"}


# --8<-- [end:shop]

if __name__ == "__main__":
    app.run(until=20000)
