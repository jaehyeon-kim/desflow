"""Cancelling and releasing low-level requests, for the Resources page.

tests/docs runs this file: a granted request's cancel releases nothing, a waiting
request's cancel leaves the queue at once, and a second release does nothing.
"""

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    SimParameter,
)

env = DynamicRealtimeEnvironment(factor=0.0)
env.registry.register_sim_parameter(
    SimParameter(
        sim_id="Shop", resources={"packers": CapacityConfig(current_cap=1, max_cap=1)}
    )
)
packers = DynamicResource(env, "Shop", "packers")

# --8<-- [start:cancel]
held = packers.request()  # granted at once: the packer was free
waiting = packers.request()  # waits behind it

waiting.cancel()  # leaves the queue at once; it is never granted
held.cancel()  # granted already, so this releases nothing, as in SimPy
assert packers.in_use == 1 and packers.queue.items == []

packers.release(held)  # gives the packer back
packers.release(held)  # a second release does nothing
# --8<-- [end:cancel]
env.run(until=1)
assert packers.in_use == 0 and not waiting.triggered
