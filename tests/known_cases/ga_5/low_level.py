"""GA-5: One option gets 60 percent of votes.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The choice is drawn with `Sampler`, from the stream the
blueprint's `weighted` uses.
"""

import itertools

from desflow import DynamicRealtimeEnvironment, Sampler

UNTIL = 10000


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    choice = sampler.generator("tasks.vote.payload.choice")

    def source():
        for number in itertools.count(1):
            yield env.timeout(1)
            # a point in the weights 0.6 and 0.4: below 0.6 is the first option
            point = choice.random() * (0.6 + 0.4)
            env.publish_event(
                f"votes-{number}",
                {"choice": "a" if point < 0.6 else "b"},
                task="vote",
                instance=number - 1,
            )

    env.process(source())
