"""Machine Shop: SimPy's machine_shop.py example, builder API.

A shop has ten machines, each making parts without end; a part's time is normal with
standard deviation 2, never below 0. Each machine breaks down after an exponential
time, unless it is already broken. A broken machine waits for the one repairman, at
priority 1, for its repair, and then finishes its part. Between repairs the repairman
does other jobs at priority 2, which a repair preempts and which resume afterwards.

SimPy's example counts minutes, and here each of its minutes is one second. So a part
takes about 10 seconds, a machine breaks down about every 300 seconds, a repair and
another job each take 30 seconds, and the run ends after 40320 seconds, SimPy's four
weeks. Each value is drawn from desflow's random sequence for its place in the model,
so the numbers differ from those SimPy prints.

The breakdowns are written as tasks: each machine's `break_loop` waits for the next
failure and interrupts the machine's `part`, whose `on_interrupt_run` starts the
repair. `MachineShop.variables.parts_made` counts the parts of each machine.

Needs no containers. Runs unpaced and ends after 40320 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import generator


def build(size: int = 10) -> SimulationContext:
    """Builds the shop, with `size` machines."""
    app = SimulationContext(sim_id="MachineShop", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
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
        """Starts the machine and its failures."""
        return {"event_type": "machine_started"}

    @app.task(
        name="part",
        service_time=generator(
            {
                "gen": "sample",
                "dist": "truncated_normal",
                "mean": 10,
                "std": 2,
                "low": 0,
            }
        ),
        on_interrupt="resume",
        on_interrupt_run="begin_repair",
        set={"parts_made": generator({"gen": "expr", "expr": "attr.parts_made + 1"})},
        on=[
            {
                "path": "MachineShop.variables.parts_made",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['MachineShop.variables.parts_made'], process.key, get_key(registry['MachineShop.variables.parts_made'], process.key, 0) + 1)",
                    }
                ),
            }
        ],
        next="part",
    )
    def part(task_id):
        """Makes a part."""
        return {"event_type": "part_made"}

    @app.task(name="begin_repair", set={"broken": 1}, next="repair")
    def begin_repair(task_id):
        """Marks the machine broken."""
        return {"event_type": "broken"}

    @app.task(
        name="repair",
        resource_id="repairman",
        priority=1,
        service_id="repair_time",
        set={"broken": 0},
    )
    def repair(task_id):
        """Repairs the machine."""
        return {"event_type": "repaired"}

    # The machine's failures: one wait for each, then an interrupt of its part
    @app.task(
        name="break_loop",
        wait={
            "timeout": generator(
                {"gen": "sample", "dist": "exponential", "rate": 0.0033333333333333335}
            )
        },
        on=[
            {
                "path": "MachineShop.variables.target",
                "value": generator({"gen": "expr", "expr": "attr.id"}),
            },
            {
                "interrupt": {
                    "task": "part",
                    "where": "attr.id == registry['MachineShop.variables.target'] and attr.broken == 0",
                }
            },
        ],
        next="break_loop",
    )
    def break_loop(task_id):
        """Breaks the machine down."""
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
        """Does one of the repairman's other jobs."""
        return {"event_type": "other_done"}

    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs 40320 seconds, SimPy's four weeks counted in minutes."""
    model.run(until=4 * 7 * 24 * 60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()
