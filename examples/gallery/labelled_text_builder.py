"""Messages and reviews with a known intent or sentiment, builder API.

The builder version of `labelled_text.yaml`: a label is drawn first by weight, and the
text is written from a template for that label.

Needs: nothing
Run it with: python examples/gallery/labelled_text_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 600


def _number(low: int, high: int) -> dict:
    return {"gen": "integer", "bounds": [low, high]}


def build() -> SimulationContext:
    """Builds the support desk and the shop."""
    app = SimulationContext(
        sim_id="Text",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("messages", "exponential", rate=0.5, spawn="message")
    app.add_arrival("reviews", "exponential", rate=0.2, spawn="review")

    def says(template: str, **names: dict) -> dict:
        return {"gen": "format", "template": template, "names": names}

    message_payload = Template(
        {
            "message_id": {"gen": "sequence", "start": 1},
            "intent": {"gen": "var", "name": "intent"},
            "text": {
                "gen": "switch",
                "on": {"gen": "var", "name": "intent"},
                "cases": {
                    "order_status": {
                        "gen": "weighted",
                        "choices": [
                            {
                                "value": says(
                                    "Where is my order {n}?", n=_number(1000, 9999)
                                ),
                                "weight": 2,
                            },
                            {"value": "Has my parcel been sent yet?", "weight": 1},
                        ],
                    },
                    "refund": {
                        "gen": "weighted",
                        "choices": [
                            {
                                "value": "I want my money back for this order.",
                                "weight": 2,
                            },
                            {
                                "value": says(
                                    "Please refund order {n}.", n=_number(1000, 9999)
                                ),
                                "weight": 1,
                            },
                        ],
                    },
                    "cancel": {
                        "gen": "weighted",
                        "choices": [
                            {
                                "value": "Cancel my order, I changed my mind.",
                                "weight": 1,
                            },
                            {"value": "Please stop my subscription.", "weight": 1},
                        ],
                    },
                    "hello": {
                        "gen": "weighted",
                        "choices": [
                            {"value": "Hi, I have a question.", "weight": 1},
                            {"value": "Hello, is anyone there?", "weight": 1},
                        ],
                    },
                },
            },
        },
        name="tasks.message",
        vars={
            "intent": {
                "gen": "weighted",
                "options": {"order_status": 4, "refund": 2, "cancel": 1, "hello": 3},
            }
        },
    )
    review_payload = Template(
        {
            "review_id": {"gen": "sequence", "start": 1},
            "sentiment": {"gen": "var", "name": "sentiment"},
            "text": {
                "gen": "switch",
                "on": {"gen": "var", "name": "sentiment"},
                "cases": {
                    "positive": {
                        "gen": "weighted",
                        "choices": [
                            {
                                "value": says(
                                    "The {product} is great and arrived early."
                                ),
                                "weight": 1,
                            },
                            {
                                "value": says(
                                    "I love this {product}, I would buy it again."
                                ),
                                "weight": 1,
                            },
                        ],
                    },
                    "neutral": says("The {product} is fine, nothing special."),
                    "negative": {
                        "gen": "weighted",
                        "choices": [
                            {
                                "value": says("The {product} broke after a week."),
                                "weight": 1,
                            },
                            {
                                "value": says("I would not recommend this {product}."),
                                "weight": 1,
                            },
                        ],
                    },
                },
            },
        },
        name="tasks.review",
        vars={
            "sentiment": {
                "gen": "weighted",
                "options": {"positive": 6, "neutral": 2, "negative": 2},
            },
            "product": {
                "gen": "one_of",
                "choices": ["lamp", "notebook", "backpack", "headphones"],
            },
        },
    )

    @app.task(name="message")
    def message(task_id):
        """Gives one message with its intent."""
        return message_payload

    @app.task(name="review")
    def review(task_id):
        """Gives one review with its sentiment."""
        return review_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)
