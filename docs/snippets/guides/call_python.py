"""The Call Python from YAML guide, built with the builder API.

A visit every 0.1 seconds, each shown the three products with the highest score from the
models in Valkey. call_python.yaml and call_python_low_level.py are the same shop in YAML
and the low-level API. Needs call_python_ranking.py beside it. Run it with:
python call_python.py
"""

import logging

from call_python_ranking import top_products

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template


# --8<-- [start:shop]
def build_shop() -> SimulationContext:
    """The shop: each visit calls the ranking function with the visitor's features."""
    app = SimulationContext(sim_id="Recommender", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress(format="json"))
    app.add_arrival("visits", dist="constant", value=0.1, spawn="recommend")
    x = {"gen": "var", "name": "x"}
    visit = Template(
        {
            "features": x,
            # top_products(context, x=...) gives the three products
            "shown": {"gen": "call", "function": top_products, "kwargs": {"x": x}},
        },
        name="tasks.recommend",
        # The visitor's three features, each between 0 and 1
        vars={
            "x": {
                "gen": "repeat",
                "times": 3,
                "value": {"gen": "uniform", "low": 0, "high": 1, "decimals": 3},
            }
        },
    )

    @app.task(name="recommend")
    def recommend(task_id):
        """Gives one visit record."""
        return visit

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build_shop().run(until=1)
# --8<-- [end:shop]
