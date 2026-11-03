"""EX-3: Machine shop with breakdowns and a repairman.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import generator


def build(size: int = 10) -> SimulationContext:
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.add_resource("repairman", current_cap=1, queue="priority", preempt=True)
    app.add_variable("target", -1)
    app.add_variable("parts_made", {})
    app.add_service("repair_time", dist="constant", value=30)
    app.add_initial_arrival(
        "machines",
        size,
        key=lambda n: "Machine {}".format(n - 1),
        attributes={"id": generator({"gen": "sequence"}), "parts_made": 0, "broken": 0},
        spawn="start_machine",
    )
    app.add_initial_arrival("others", 1, spawn="other_job")

    @app.task(
        name="start_machine",
        spawn={"task": "break_loop", "copy_attributes": True},
        next="part",
    )
    def start_machine(task_id):
        return {"event_type": "machine_started"}

    @app.task(
        name="part",
        service_time=generator(
            {"gen": "expr", "expr": "8 + n % 5", "names": {"n": {"gen": "sequence"}}}
        ),
        on_interrupt="resume",
        on_interrupt_run="begin_repair",
        set={"parts_made": generator({"gen": "expr", "expr": "attr.parts_made + 1"})},
        on=[
            {
                "path": "Shop.variables.parts_made",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Shop.variables.parts_made'], process.key, get_key(registry['Shop.variables.parts_made'], process.key, 0) + 1)",
                    }
                ),
            }
        ],
        next="part",
    )
    def part(task_id):
        return {"event_type": "part_made"}

    @app.task(name="begin_repair", set={"broken": 1}, next="repair")
    def begin_repair(task_id):
        return {"event_type": "broken"}

    @app.task(
        name="repair",
        resource_id="repairman",
        priority=1,
        service_id="repair_time",
        set={"broken": 0},
    )
    def repair(task_id):
        return {"event_type": "repaired"}

    @app.task(
        name="break_loop",
        wait={
            "timeout": generator(
                {
                    "gen": "expr",
                    "expr": "250 + n % 7 * 10",
                    "names": {"n": {"gen": "sequence"}},
                }
            )
        },
        on=[
            {
                "path": "Shop.variables.target",
                "value": generator({"gen": "expr", "expr": "attr.id"}),
            },
            {
                "interrupt": {
                    "task": "part",
                    "where": "attr.id == registry['Shop.variables.target'] and attr.broken == 0",
                }
            },
        ],
        next="break_loop",
    )
    def break_loop(task_id):
        return {"event_type": "tick"}

    @app.task(
        name="other_job",
        resource_id="repairman",
        priority=2,
        service_time=30,
        on_interrupt="resume",
        next="other_job",
    )
    def other_job(task_id):
        return {"event_type": "other_done"}

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=3000)
