"""The ranking function of the Call Python from YAML page.

It reads the LinUCB model of each product from Valkey, scores the products for one
visitor and returns the best three. Every form of the shop calls it. It needs the redis
package (`pip install "desflow[redis]"`) and the Valkey of `odctl up valkey`.
"""

import json
import math
import os

import redis

PRODUCTS = ["p1", "p2", "p3", "p4", "p5"]
FEATURES = 3
ALPHA = 1.0

# The Valkey of `odctl up valkey`; VALKEY_URL points the function at another one
CLIENT = redis.Redis.from_url(
    os.getenv("VALKEY_URL", "redis://user:password@127.0.0.1:6379/0"),
    decode_responses=True,
)


# --8<-- [start:rank]
def top_products(context, x):
    """The three products with the highest LinUCB score for the features x."""
    scores = {}
    for pid, stored in zip(PRODUCTS, CLIENT.mget([f"linucb:{p}" for p in PRODUCTS])):
        if stored:
            model = json.loads(stored)
        else:  # A product with no model yet gets the identity matrix and a zero vector
            identity = [
                [float(i == j) for j in range(FEATURES)] for i in range(FEATURES)
            ]
            model = {"A_inv": identity, "b": [0.0] * FEATURES}
        ax = [sum(a * v for a, v in zip(row, x)) for row in model["A_inv"]]
        expected = sum(a * b for a, b in zip(ax, model["b"]))
        uncertainty = math.sqrt(sum(a * v for a, v in zip(ax, x)))
        scores[pid] = expected + ALPHA * uncertainty
    return sorted(scores, key=scores.get, reverse=True)[:3]


# --8<-- [end:rank]
