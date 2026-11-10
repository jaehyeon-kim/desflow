"""Messages and reviews with a known intent or sentiment, low-level API.

The low-level version of `labelled_text.yaml`: plain SimPy on a `DynamicRealtimeEnvironment`,
which publishes each record with `publish_event`. Each draw is made with
`Sampler.generator(<name>)`, from the random sequence the blueprint's generator at that
place uses, so the draws are the blueprint's.

Needs: nothing
Run it with: python examples/gallery/labelled_text_low_level.py
"""

import itertools
import logging
from datetime import datetime, timezone

from desflow import (
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
)

UNTIL = 600
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
INTENTS = {"order_status": 4, "refund": 2, "cancel": 1, "hello": 3}
SENTIMENTS = {"positive": 6, "neutral": 2, "negative": 2}
PRODUCTS = ["lamp", "notebook", "backpack", "headphones"]
# The texts of a message, by intent. A text with {n} takes an order number
MESSAGES = {
    "order_status": [
        ("Where is my order {n}?", 2),
        ("Has my parcel been sent yet?", 1),
    ],
    "refund": [
        ("I want my money back for this order.", 2),
        ("Please refund order {n}.", 1),
    ],
    "cancel": [
        ("Cancel my order, I changed my mind.", 1),
        ("Please stop my subscription.", 1),
    ],
    "hello": [("Hi, I have a question.", 1), ("Hello, is anyone there?", 1)],
}
# The texts of a review, by sentiment. A text takes the product
REVIEWS = {
    "positive": [
        ("The {product} is great and arrived early.", 1),
        ("I love this {product}, I would buy it again.", 1),
    ],
    "neutral": [("The {product} is fine, nothing special.", 1)],
    "negative": [
        ("The {product} broke after a week.", 1),
        ("I would not recommend this {product}.", 1),
    ],
}


def position(rng, weights):
    """The position of one weight, chosen by its size, as the `weighted` generator does."""
    point = rng.random() * sum(weights)
    running = 0.0
    for index, weight in enumerate(weights):
        running += weight
        if point < running:
            return index
    return len(weights) - 1


def build(env: DynamicRealtimeEnvironment) -> None:
    """Starts the arrivals on `env`."""
    sampler = Sampler(seed=42)
    intent = sampler.generator("tasks.message.vars.intent")
    sentiment = sampler.generator("tasks.review.vars.sentiment")
    product = sampler.generator("tasks.review.vars.product")
    counts = {"message": 0, "review": 0}

    def message_text(label):
        """The text of a message: a template chosen by weight, with an order number."""
        place = f"tasks.message.payload.text.cases.{label}"
        templates = MESSAGES[label]
        chosen = position(sampler.generator(place), [w for _, w in templates])
        template = templates[chosen][0]
        number = sampler.generator(f"{place}.choices.{chosen}.value.names.n")
        return template.format(n=int(number.integers(1000, 10000)))

    def review_text(label, name):
        """The text of a review: a template chosen by weight, for the product."""
        templates = REVIEWS[label]
        weights = [w for _, w in templates]
        place = f"tasks.review.payload.text.cases.{label}"
        chosen = 0 if len(weights) == 1 else position(sampler.generator(place), weights)
        return templates[chosen][0].format(product=name)

    def message():
        counts["message"] += 1
        label = list(INTENTS)[position(intent, list(INTENTS.values()))]
        return {
            "message_id": counts["message"],
            "intent": label,
            "text": message_text(label),
        }

    def review():
        counts["review"] += 1
        label = list(SENTIMENTS)[position(sentiment, list(SENTIMENTS.values()))]
        name = PRODUCTS[int(product.integers(len(PRODUCTS)))]
        return {
            "review_id": counts["review"],
            "sentiment": label,
            "text": review_text(label, name),
        }

    def arrival(name, task, make, rate):
        gap = DistributionConfig(dist="exponential", rate=rate)
        gap.stream = f"arrival.{name}"

        def source():
            for number in itertools.count(1):
                yield env.timeout(sampler.sample(gap))
                env.publish_event(
                    f"{name}-{number}", make(), task=task, instance=number - 1
                )

        env.process(source())

    arrival("messages", "message", message, 0.5)
    arrival("reviews", "review", review, 0.2)


def run() -> None:
    """Runs the model unpaced into the console."""
    env = DynamicRealtimeEnvironment(factor=0, **ENVIRONMENT)
    env.setup_egress([ConsoleEgress()])
    build(env)
    try:
        env.run(until=UNTIL)
    finally:
        env.teardown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
