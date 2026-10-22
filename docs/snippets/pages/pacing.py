"""The clock settings of the Time and Pacing page.

tests/docs runs this file. Every run in it is unpaced or very short, so it finishes
in under a second.
"""

from datetime import datetime, timedelta, timezone

from desflow import DynamicRealtimeEnvironment, SimulationContext

# --8<-- [start:logical-start]
app = SimulationContext(
    sim_id="Shop",
    factor=0.0,
    logical_start_time=datetime.now(timezone.utc) - timedelta(days=7),  # a week
)
# --8<-- [end:logical-start]

# --8<-- [start:factor]
live = DynamicRealtimeEnvironment(factor=1.0)  # 1 simulated second takes 1 real second
twice_as_fast = DynamicRealtimeEnvironment(factor=0.5)  # takes half a real second
unpaced = DynamicRealtimeEnvironment(factor=0.0)  # takes no real time at all
# --8<-- [end:factor]

# --8<-- [start:go-live]
go_live_at = datetime.now(timezone.utc)

env = DynamicRealtimeEnvironment(
    factor=0.0,  # until go_live_at
    logical_start_time=go_live_at - timedelta(days=7),
    go_live_at=go_live_at,  # from here, 1 simulated second per real second
)
# --8<-- [end:go-live]

# --8<-- [start:timezone]
# Timestamps in Melbourne time, with their offset: 2026-10-03T12:00:00.000+10:00
melbourne = SimulationContext(
    sim_id="Shop",
    factor=0.0,
    timezone="Australia/Melbourne",
    logical_start_time=datetime(2026, 10, 3, 12, 0),  # read in Melbourne time
)
# --8<-- [end:timezone]
# --8<-- [start:low-level-timezone]
zoned = DynamicRealtimeEnvironment(
    factor=0.0,
    timezone="Australia/Melbourne",
    logical_start_time=datetime(2026, 10, 3, 12, 0),
)
print(zoned.start_datetime)  # 2026-10-03 12:00:00+10:00
# --8<-- [end:low-level-timezone]
# 24 simulation hours later, after the clocks went forward an hour
assert zoned._get_iso_timestamp(zoned.start_datetime, 86_400) == (
    "2026-10-04T13:00:00.000+11:00"
)
assert str(zoned.start_datetime) == "2026-10-03 12:00:00+10:00"

# --8<-- [start:lag]
# system.simulation.lag_seconds every 10 simulation seconds; 0 turns it off
paced = SimulationContext(sim_id="Shop", factor=1.0, lag_interval="10 s")
# --8<-- [end:lag]
# --8<-- [start:low-level-lag]
# The low-level API sets it with the sinks; the default "auto" is off at factor 0
# env.setup_egress(sinks, lag_monitor_interval=10)
# --8<-- [end:low-level-lag]
assert paced.lag_interval == 10

# --8<-- [start:warmup]
# No sink receives a record stamped before 10 minutes, and the statistics restart then
measured = SimulationContext(sim_id="Shop", factor=0.0, warmup="10 min")
# In the low-level API, the warm-up is set with the sinks:
# env.setup_egress(sinks, warmup=600)
# --8<-- [end:warmup]

# --8<-- [start:initial-time]
# The clock starts at 8 hours: event times, sim_ts and until all read it
shift = SimulationContext(sim_id="Shop", factor=0.0, initial_time="8 h")
# --8<-- [end:initial-time]
# --8<-- [start:low-level-initial-time]
# The clock starts at 8 hours, in seconds, as in SimPy's Environment(initial_time=)
morning = DynamicRealtimeEnvironment(factor=0.0, initial_time=8 * 3600)
# --8<-- [end:low-level-initial-time]
assert shift.initial_time == morning.now == 28800

# --8<-- [start:run-length]
app.add_resource("packers", current_cap=1, max_cap=1)
app.run(until="1 week")  # 604,800 simulation seconds; a number is seconds too
# --8<-- [end:run-length]

assert measured.warmup == 600

for e in (live, twice_as_fast, unpaced, env, zoned):
    e.teardown()


# --8<-- [start:step]
def every_ten(context):
    while True:
        yield context.env.timeout(10)


stepped = SimulationContext(sim_id="Shop", factor=0.0).add_process(every_ten)
stepped.start()  # builds everything run() builds; the clock is still at 0
while stepped.peek() < 30:  # the time of the next event
    stepped.step()  # processes the next model event, after desflow's own events due before it
print(f"stopped before {stepped.peek()} at {stepped.env.now}")
stepped.close()  # the teardown run() does at its end
# --8<-- [end:step]
assert stepped.env.now == 20 and stepped.peek() == 30


# --8<-- [start:strict]
# A paced step more than factor seconds behind the wall clock raises RuntimeError;
# the builder's default is False
checked = SimulationContext(sim_id="Shop", factor=1.0, strict=True)
# --8<-- [end:strict]
# --8<-- [start:low-level-strict]
# The low-level API is strict unless you pass strict=False, as SimPy's is
raising = DynamicRealtimeEnvironment(factor=1.0)
tolerant = DynamicRealtimeEnvironment(factor=1.0, strict=False)
# --8<-- [end:low-level-strict]
assert not SimulationContext(sim_id="Shop").strict
assert checked.strict and raising.strict and not tolerant.strict
for e in (raising, tolerant):
    e.teardown()

# --8<-- [start:resume-pacing]
paused = DynamicRealtimeEnvironment(factor=0.01, strict=False)  # 1.0 follows the clock
paused.run(until=10)
# ... a pause, such as a stop in a debugger: the wall clock is now ahead of the run
paused.resume_pacing()  # the next event waits factor seconds per second from here
paused.run(until=20)
# --8<-- [end:resume-pacing]
assert paused.now == 20
paused.teardown()


# --8<-- [start:segments]
def pulse(context):
    while True:
        yield context.env.timeout(10)


segmented = SimulationContext(sim_id="Shop", factor=0.0).add_process(pulse)
segmented.start(until=60)
segmented.advance(25)  # runs the model to 25; the events at 25 wait for the next call
segmented.advance("40 s")  # a duration is seconds from the start, not from now
print(f"paused at {segmented.env.now}")
segmented.finish()  # on to start()'s until, 60, and closes
# --8<-- [end:segments]
assert segmented.env.now == 60
