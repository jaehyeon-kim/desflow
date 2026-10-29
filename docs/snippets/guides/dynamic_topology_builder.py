"""The Add Resources Mid-run guide, builder API: a supervisor adds gift wrappers at
12 seconds.

Run it with: python dynamic_topology_builder.py, beside dynamic_topology_logic.py
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from dynamic_topology_logic import supervisor

logging.basicConfig(level=logging.INFO)

# --8<-- [start:shop]
app = (
    SimulationContext(sim_id="Shop", factor=1.0, strict=False)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=1, max_cap=5)
    # Called as supervisor(context); it adds the wrappers during the run
    .add_process(supervisor)
)
# --8<-- [end:shop]

if __name__ == "__main__":
    app.run(until=20.0)
