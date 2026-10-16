"""The distributions of the Arrivals and Services page: builder and low-level.

tests/docs runs this file: the builder model runs unpaced, and the low-level part
draws from each distribution and checks the rules the page states.
"""

import logging
from typing import Any, Dict, List

import simpy

from desflow import DistributionConfig, Sampler, SimParameter, SimulationContext
from desflow.core.registry import SimulationRegistry

# --8<-- [start:builder-seed]
# The same draws on every run; leave it out to draw a seed and log it
app = SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
# --8<-- [end:builder-seed]
app.add_resource("packers", current_cap=2, max_cap=5)

# --8<-- [start:builder-exponential]
app.add_arrival("orders", dist="exponential", rate=0.5)  # an order every 2 s on average
# --8<-- [end:builder-exponential]

# --8<-- [start:builder-normal]
app.add_service("packing", dist="normal", mean=3.0, std=0.5)  # 3 s, give or take 0.5 s
# --8<-- [end:builder-normal]

# --8<-- [start:builder-truncated-normal]
app.add_service("labelling", dist="truncated_normal", mean=1.0, std=1.0, low=0.2)
# --8<-- [end:builder-truncated-normal]

# --8<-- [start:builder-lognormal]
# Now and then much longer
app.add_service("repair", dist="lognormal", mean=4.0, std=3.0)
# --8<-- [end:builder-lognormal]

# --8<-- [start:builder-constant]
app.add_service("inspection", dist="constant", value=0.5)  # always 0.5 s, no draw
# --8<-- [end:builder-constant]

# --8<-- [start:builder-uniform]
app.add_service("scanning", dist="uniform", low=1.0, high=2.0)  # any time from 1 to 2 s
# --8<-- [end:builder-uniform]

# --8<-- [start:builder-triangular]
# At least 1 s, usually 1.25 s, at most 2.25 s
app.add_service("wrapping", dist="triangular", low=1.0, mode=1.25, high=2.25)
# --8<-- [end:builder-triangular]

# --8<-- [start:builder-gamma]
# Three exponential steps of 0.5 s each: a mean of 1.5 s
app.add_service("checking", dist="gamma", shape=3, scale=0.5)
# --8<-- [end:builder-gamma]

# --8<-- [start:builder-weibull]
# Time to a printer jam: a shape above 1 means it wears out
app.add_service("jamming", dist="weibull", shape=1.5, scale=3600)
# --8<-- [end:builder-weibull]

# --8<-- [start:builder-empirical]
# Measured times, each equally likely; or weights=[...], one per value
app.add_service("loading", dist="empirical", values=[1.2, 1.5, 1.5, 2.1, 3.0])
# A histogram: 10 times from 0 to 1 s, 30 from 1 to 2 s, 5 from 2 to 4 s
app.add_service("sealing", dist="empirical", edges=[0, 1, 2, 4], counts=[10, 30, 5])
# --8<-- [end:builder-empirical]


@app.task(service_id="packing", resource_id="packers")
def pack(task_id: int):
    return {"event_type": "order_packed"}


@app.arrival_loop("orders")
def orders(context: SimulationContext):
    task_id = 0
    while True:
        order = yield context.wait_for_arrival("orders")
        context.spawn(pack(task_id, entity=order))
        task_id += 1


# --8<-- [start:low-level-seed]
sampler = Sampler(seed=42)  # the same draws on every run
# --8<-- [end:low-level-seed]

# --8<-- [start:low-level-exponential]
gap = DistributionConfig(dist="exponential", rate=0.5)  # an order every 2 s on average
# --8<-- [end:low-level-exponential]

# --8<-- [start:low-level-normal]
# 3 s, give or take 0.5 s
packing = DistributionConfig(dist="normal", mean=3.0, std=0.5)
# --8<-- [end:low-level-normal]

# --8<-- [start:low-level-truncated-normal]
labelling = DistributionConfig(dist="truncated_normal", mean=1.0, std=1.0, low=0.2)
# --8<-- [end:low-level-truncated-normal]

# --8<-- [start:low-level-lognormal]
# Now and then much longer
repair = DistributionConfig(dist="lognormal", mean=4.0, std=3.0)
# --8<-- [end:low-level-lognormal]

# --8<-- [start:low-level-constant]
inspection = DistributionConfig(dist="constant", value=0.5)  # always 0.5 s, no draw
# --8<-- [end:low-level-constant]

# --8<-- [start:low-level-uniform]
scanning = DistributionConfig(dist="uniform", low=1.0, high=2.0)  # from 1 to 2 s
# --8<-- [end:low-level-uniform]

# --8<-- [start:low-level-triangular]
# At least 1 s, usually 1.25 s, at most 2.25 s
wrapping = DistributionConfig(dist="triangular", low=1.0, mode=1.25, high=2.25)
# --8<-- [end:low-level-triangular]

# --8<-- [start:low-level-gamma]
# Three exponential steps of 0.5 s each: a mean of 1.5 s
checking = DistributionConfig(dist="gamma", shape=3, scale=0.5)
# --8<-- [end:low-level-gamma]

# --8<-- [start:low-level-weibull]
# Time to a printer jam: a shape above 1 means it wears out
jamming = DistributionConfig(dist="weibull", shape=1.5, scale=3600)
# --8<-- [end:low-level-weibull]

# --8<-- [start:low-level-empirical]
# Measured times, each equally likely; or weights=[...], one per value
loading = DistributionConfig(dist="empirical", values=[1.2, 1.5, 1.5, 2.1, 3.0])
# A histogram: 10 times from 0 to 1 s, 30 from 1 to 2 s, 5 from 2 to 4 s
sealing = DistributionConfig(dist="empirical", edges=[0, 1, 2, 4], counts=[10, 30, 5])
# --8<-- [end:low-level-empirical]

# --8<-- [start:low-level-draw]
seconds = sampler.sample(packing)  # one draw, in seconds, from the current values
# --8<-- [end:low-level-draw]

# --8<-- [start:streams]
# Each registered distribution draws from the stream named by its path without
# the sim id; register the parameters, and sample() picks the stream
registry = SimulationRegistry(simpy.Environment())
registry.register_sim_parameter(SimParameter(sim_id="Shop", arrival={"orders": gap}))
assert gap.stream == "arrival.orders"
stream = Sampler(seed=42).generator("arrival.orders")  # the same seed, the same name
assert stream is not None and sampler.sample(gap) == stream.exponential(2.0)
# --8<-- [end:streams]

# --8<-- [start:no-generator]
fixed = Sampler()  # no random generator: every draw is the mean, or the value
assert fixed.sample(packing) == 3.0 and fixed.sample(gap) == 2.0
assert fixed.sample(checking) == 1.5  # shape × scale
# --8<-- [end:no-generator]

# --8<-- [start:invalid]
refused: List[Dict[str, Any]] = [
    {"dist": "normal", "mean": 3.0, "std": -1.0},  # out of range
    {"dist": "normal", "mean": 3.0, "rate": 2.0},  # a parameter normal does not take
    {"dist": "triangular", "low": 1.0, "mode": 3.0, "high": 2.0},  # mode above high
]
for parameters in refused:
    try:
        DistributionConfig(**parameters)
    except ValueError as error:
        print(error)
# --8<-- [end:invalid]

# --8<-- [start:refused]
packing_path = "Shop.service.packing.std"
registry.register_sim_parameter(
    SimParameter(sim_id="Shop", service={"packing": packing})
)
registry.update(packing_path, -1.0)  # refused with an error log line
assert registry.get(packing_path).value == 0.5  # the old value stays
# --8<-- [end:refused]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    draws = [sampler.sample(labelling) for _ in range(20_000)]
    assert min(draws) >= 0.2
    assert all(sampler.sample(inspection) == 0.5 for _ in range(10))
    assert all(1.0 <= sampler.sample(scanning) <= 2.0 for _ in range(1000))
    assert {sampler.sample(loading) for _ in range(1000)} <= {1.2, 1.5, 2.1, 3.0}
    assert all(0 <= sampler.sample(sealing) <= 4 for _ in range(1000))
    app.run(until=60)
