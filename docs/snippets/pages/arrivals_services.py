"""The distributions of the Arrivals and Services page: builder and low-level.

tests/unit runs this file: the builder model runs unpaced, and the low-level part
draws from each distribution and checks the rules the page states.
"""

import numpy as np

from dynamic_des import DistributionConfig, Sampler, SimulationContext

# --8<-- [start:builder-seed]
app = SimulationContext(sim_id="Line_A", factor=0.0, random_seed=42)  # same draws every run
# --8<-- [end:builder-seed]
app.add_resource("lathe", current_cap=2, max_cap=5)

# --8<-- [start:builder-exponential]
app.add_arrival("parts", dist="exponential", rate=0.5)  # a part every 2 s on average
# --8<-- [end:builder-exponential]

# --8<-- [start:builder-normal]
app.add_service("milling", dist="normal", mean=3.0, std=0.5)  # 3 s, give or take 0.5 s
# --8<-- [end:builder-normal]

# --8<-- [start:builder-lognormal]
app.add_service("repair", dist="lognormal", mean=4.0, std=3.0)  # now and then much longer
# --8<-- [end:builder-lognormal]

# --8<-- [start:builder-truncated-normal]
app.add_service("drilling", dist="truncated_normal", mean=1.0, std=1.0, low=0.2)
# --8<-- [end:builder-truncated-normal]

# --8<-- [start:builder-constant]
app.add_service("inspection", dist="constant", value=0.5)  # always 0.5 s, no draw
# --8<-- [end:builder-constant]


@app.task(service_id="milling", resource_id="lathe")
def mill(part_id: int):
    return {"part_id": part_id}


@app.arrival_loop("parts")
def parts(context: SimulationContext):
    part_id = 0
    while True:
        yield context.wait_for_arrival("parts")
        context.spawn(mill(part_id))
        part_id += 1


# --8<-- [start:low-level-seed]
sampler = Sampler(rng=np.random.default_rng(42))  # same draws every run
# --8<-- [end:low-level-seed]

# --8<-- [start:low-level-exponential]
gap = DistributionConfig(dist="exponential", rate=0.5)  # a part every 2 s on average
# --8<-- [end:low-level-exponential]

# --8<-- [start:low-level-normal]
milling = DistributionConfig(dist="normal", mean=3.0, std=0.5)  # 3 s, give or take 0.5 s
# --8<-- [end:low-level-normal]

# --8<-- [start:low-level-lognormal]
repair = DistributionConfig(dist="lognormal", mean=4.0, std=3.0)  # now and then much longer
# --8<-- [end:low-level-lognormal]

# --8<-- [start:low-level-truncated-normal]
drilling = DistributionConfig(dist="truncated_normal", mean=1.0, std=1.0, low=0.2)
# --8<-- [end:low-level-truncated-normal]

# --8<-- [start:low-level-constant]
inspection = DistributionConfig(dist="constant", value=0.5)  # always 0.5 s, no draw
# --8<-- [end:low-level-constant]

# --8<-- [start:low-level-draw]
seconds = sampler.sample(milling)  # one draw, in seconds, from the current values
# --8<-- [end:low-level-draw]

# --8<-- [start:no-generator]
fixed = Sampler()  # no random generator: every draw is the mean, or the value
assert fixed.sample(milling) == 3.0 and fixed.sample(gap) == 2.0
# --8<-- [end:no-generator]

# --8<-- [start:invalid]
try:
    DistributionConfig(dist="normal", mean=3.0, std=-1.0)
except ValueError as error:
    print(error)  # normal std must be 0 or more, got -1.0
# --8<-- [end:invalid]

if __name__ == "__main__":
    draws = np.array([sampler.sample(drilling) for _ in range(20_000)])
    assert draws.min() >= 0.2
    assert all(sampler.sample(inspection) == 0.5 for _ in range(10))
    app.run(until=60)
