"""Each bet is settled after the match, and a winner is paid the stake times the odds.

The Low-level form of bets_are_settled_and_winners_paid.yaml, with the same records,
built on `DynamicRealtimeEnvironment`. A bet is placed with a stake and odds. One to two
minutes later it is settled: 45 percent win and are paid the stake times the odds, and
the rest lose and are paid nothing.

Needs: nothing
Run it with: python examples/gallery/bets_are_settled_and_winners_paid_low_level.py
"""

import itertools
import logging
from datetime import datetime

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

SEED = 42
UNTIL = 1200
STAKES = {5: 50, 10: 30, 50: 15, 100: 5}


def weighted(rng, options):
    """One key of `options`, chosen by its weight, as the `weighted` generator does."""
    point = rng.random() * sum(options.values())
    running = 0.0
    for value, weight in options.items():
        running += weight
        if point < running:
            return value
    return value


def build(env):
    """Starts the bet source on `env`."""
    sampler = Sampler(seed=SEED)
    stake = sampler.generator("tasks.place_bet.payload.stake")
    odds = sampler.generator("tasks.place_bet.payload.odds")
    wins = sampler.generator("tasks.settle_bet.vars.won")
    until_settled = sampler.generator("tasks.settle_bet.service_time")
    gap = DistributionConfig(dist="exponential", rate=0.2)
    gap.stream = "arrival.bets"
    bet_ids = itertools.count(1)

    def bet(key, instance):
        placed = {
            "bet_id": next(bet_ids),
            "stake": weighted(stake, STAKES),
            "odds": round(1.5 + (4 - 1.5) * odds.random(), 2),
        }
        env.publish_event(key, placed, task="place_bet", instance=instance)
        env.publish_event(
            key, {"status": "started"}, task="settle_bet", instance=instance
        )
        yield env.timeout(60 + (120 - 60) * until_settled.random())
        # The blueprint's `won` var is first read when the settled record is made
        won = bool(wins.random() < 0.45)
        payout = round(placed["stake"] * placed["odds"], 2) if won else 0
        env.publish_event(
            key,
            {
                "bet_id": placed["bet_id"],
                "result": "won" if won else "lost",
                "payout": payout,
            },
            task="settle_bet",
            instance=instance,
        )

    def bets():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(bet(f"bets-{number}", number - 1))

    env.process(bets())


def run():
    """Runs the model for 20 simulation minutes."""
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=datetime(2026, 1, 1))
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
