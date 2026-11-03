"""ST-E05: A periodic event that five consumers count.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="T", factor=0, random_seed=42)
    app.add_variable("counter", 0)
    app.add_signal("periodic", once=False)
    app.add_initial_arrival("period", size, spawn="tick", attributes={"n": 0})
    app.add_initial_arrival("consumers", 5, spawn="consume")

    @app.task(
        name="tick",
        service_time=1,
        signal="periodic",
        set={"n": generator({"gen": "expr", "expr": "attr.n + 1"})},
        next={"when": [{"condition": "attr.n < 3", "task": "tick"}]},
    )
    def tick(task_id):
        return {"event_type": "tick"}

    @app.task(
        name="consume",
        wait={"signal": "periodic"},
        on=[
            {
                "path": "T.variables.counter",
                "value": generator(
                    {"gen": "expr", "expr": "registry['T.variables.counter'] + 1"}
                ),
            }
        ],
        next="consume",
    )
    def consume(task_id):
        return {"event_type": "seen"}

    return app


def run(app: SimulationContext):
    """Runs the model with the `run` section of `model.yaml`."""
    return app.run(
        segments=[
            {"stop": {"signal": "periodic"}},
            {"stop": {"signal": "periodic"}},
            {"stop": {"signal": "periodic"}},
            {},
        ]
    )
