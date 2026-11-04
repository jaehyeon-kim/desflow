"""DS-A3: A group size drawn for each arrival.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`.
"""

from desflow import DynamicRealtimeEnvironment, Sampler

PROCS: dict = {}  # entity key -> its process
UNTIL = 50


def build(env: DynamicRealtimeEnvironment) -> None:
    PROCS.clear()

    def L(key, task, payload):
        env.publish_event(key, payload, task=task)

    sizes = Sampler(seed=42).generator("arrivals.a.group")
    n = [0]

    def member(k):
        L(k, "member", {"event_type": "member"})
        yield env.timeout(0)

    def src():
        for a in range(3):
            yield env.timeout(5)
            for i in range(int(sizes.poisson(2))):
                n[0] += 1
                env.process(member(f"a-{n[0]}"))

    env.process(src())
