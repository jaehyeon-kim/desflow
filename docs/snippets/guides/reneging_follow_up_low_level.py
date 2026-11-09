"""The Add Reneging and Balking guide: an order that leaves the line goes on to a
follow-up task, low-level form of docs/snippets/yaml/reneging_follow_up.yaml.

Run it with: python reneging_follow_up_low_level.py
"""

import logging

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)

UNTIL = 40


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the twelve orders on `env`."""
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
            service={"packing": DistributionConfig(dist="constant", value=3)},
        )
    )
    packers = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=42)
    sent = {"follow_up": 0}

    # --8<-- [start:tasks]
    def follow_up(key, how):
        """Records how the order left the line."""
        value = {"event_type": "order_left", "how": how}
        env.publish_event(key, value, task="follow_up", instance=sent["follow_up"])
        sent["follow_up"] += 1

    def pack(number):
        key = f"orders-{number}"
        if env.now >= 8:  # balk: t >= 8
            follow_up(key, "balked")
            return
        with packers.request() as unit:
            # SimPy's request | timeout: the patience is 4 seconds
            result = yield unit | env.timeout(4)
            if unit not in result:
                follow_up(key, "reneged")
                return
            yield env.timeout(
                sampler.sample(env.registry.get_config("Shop.service.packing"))
            )
            env.publish_event(
                key, {"event_type": "order_packed"}, task="pack", instance=number - 1
            )

    # --8<-- [end:tasks]

    def orders():
        for number in range(1, 13):
            yield env.timeout(1)
            env.process(pack(number))

    env.process(orders())


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
