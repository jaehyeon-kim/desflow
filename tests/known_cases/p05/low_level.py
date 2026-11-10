"""P05: A group of members arrives together at each arrival.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The group size is drawn from the sequence the blueprint's `group` uses.
"""

from desflow import DynamicRealtimeEnvironment, Sampler

UNTIL = 50


def build(env: DynamicRealtimeEnvironment) -> None:
    made = [0]
    group = Sampler(seed=42).generator("arrivals.a.group")

    def member(key):
        env.publish_event(key, {"event_type": "member"}, task="member")
        yield env.timeout(0)

    def source():
        while True:
            yield env.timeout(5)
            for _ in range(int(group.poisson(2))):
                made[0] += 1
                env.process(member(f"a-{made[0]}"))

    env.process(source())
