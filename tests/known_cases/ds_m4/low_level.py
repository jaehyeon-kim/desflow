"""DS-M4: Several entities start at one instant, then a source goes on.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
    SimParameter,
)

PROCS: dict = {}  # entity key -> its process
UNTIL = 12
PARAMS = SimParameter(
    sim_id="M4", arrival={"cars": DistributionConfig(dist="exponential", rate=1)}
)


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    PROCS.clear()
    sampler = Sampler(seed=42)

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    def proc(first, second, step):

        def p(k):
            L(k, first, {"event_type": "first_step"})
            yield env.timeout(step)
            L(k, second, {"event_type": "goes_on"})

        return p

    def begin(key, make):
        PROCS[key] = env.process(make(key))

    for n in range(1, size + 1):
        begin(f"control-{n}", proc("check", "check_on", 0))
    car = proc("car", "car_on", 5)

    def source():
        # The source starts before the arrival's own entities, so its first gap ends
        # before a car step that ends at the same time
        config = env.registry.get_config("M4.arrival.cars")
        config.stream = "arrival.cars"
        made = 4
        while True:
            yield env.timeout(sampler.sample(config))
            made += 1
            begin(f"cars-{made}", car)

    env.process(source())
    for n in range(1, 5):
        begin(f"cars-{n}", car)
