"""GA-6: Five sensors each drift from their previous value.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The draw is made with `Sampler`, from the stream the
blueprint's `uniform` uses.
"""

from desflow import DynamicRealtimeEnvironment, Sampler

UNTIL = 100


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    step = sampler.generator("tasks.sensor_reading.payload.value.args.1")
    last = {}  # sensor id -> its previous reading, 0 before the first

    def sensor(number):
        key = f"sensor-{number}"
        for count in range(1, 100):
            yield env.timeout(1)
            value = last.get(key, 0) + float(step.uniform(-1, 1))
            last[key] = value
            env.publish_event(
                key,
                {"sensor_id": key, "value": value},
                task="sensor_reading",
                instance=count - 1,
            )

    for number in range(5):
        env.process(sensor(number))
