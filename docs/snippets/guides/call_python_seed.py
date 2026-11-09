"""Writes the models the Call Python from YAML page ranks with into Valkey.

Three products have a model and two have none yet. Run it once before the shop with
`python call_python_seed.py`, beside call_python_ranking.py.
"""

import json

from call_python_ranking import CLIENT

# --8<-- [start:models]
MODELS = {
    "p1": {"A_inv": [[0.5, 0, 0], [0, 0.5, 0], [0, 0, 0.5]], "b": [2.0, 0.0, 0.0]},
    "p2": {"A_inv": [[0.5, 0, 0], [0, 0.5, 0], [0, 0, 0.5]], "b": [0.0, 2.0, 0.0]},
    "p3": {"A_inv": [[0.25, 0, 0], [0, 0.25, 0], [0, 0, 0.25]], "b": [0.0, 0.0, 3.0]},
}


def seed(client) -> None:
    """Stores the models of p1, p2 and p3; p4 and p5 keep none."""
    for pid, model in MODELS.items():
        client.set(f"linucb:{pid}", json.dumps(model))


# --8<-- [end:models]


if __name__ == "__main__":
    seed(CLIENT)
