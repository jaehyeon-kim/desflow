"""The clock settings of the Time and Pacing page.

tests/docs runs this file. Every run in it is unpaced or very short, so it finishes
in under a second.
"""

from datetime import datetime, timedelta

from desflow import DynamicRealtimeEnvironment, SimulationContext

# --8<-- [start:logical-start]
app = SimulationContext(
    sim_id="Shop",
    factor=0.0,
    logical_start_time=datetime.now() - timedelta(days=7),  # a week of history
)
# --8<-- [end:logical-start]

# --8<-- [start:factor]
live = DynamicRealtimeEnvironment(factor=1.0)  # 1 simulated second takes 1 real second
twice_as_fast = DynamicRealtimeEnvironment(factor=0.5)  # takes half a real second
unpaced = DynamicRealtimeEnvironment(factor=0.0)  # takes no real time at all
# --8<-- [end:factor]

# --8<-- [start:go-live]
go_live_at = datetime.now()

env = DynamicRealtimeEnvironment(
    factor=0.0,  # until go_live_at
    logical_start_time=go_live_at - timedelta(days=7),
    go_live_at=go_live_at,  # from here, 1 simulated second per real second
)
# --8<-- [end:go-live]

# --8<-- [start:warmup]
# No sink receives a record stamped before 10 minutes, and the statistics restart then
measured = SimulationContext(sim_id="Shop", factor=0.0, warmup="10 min")
# In the low-level API, the warm-up is set with the sinks:
# env.setup_egress(sinks, warmup=600)
# --8<-- [end:warmup]

# --8<-- [start:run-length]
app.add_resource("packers", current_cap=1, max_cap=1)
app.run(until="1 week")  # 604,800 simulation seconds; a number is seconds too
# --8<-- [end:run-length]

assert measured.warmup == 600

for e in (live, twice_as_fast, unpaced, env):
    e.teardown()
