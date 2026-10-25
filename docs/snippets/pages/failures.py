"""The Failures page: a failure nobody handles, raised to the caller under `stop`.

A task that fails with no `on_fail` raises `TaskFailed` from `run()`, and a signal
that a failed task fails ends a run that stops on it with `SignalFailed`.
"""

from desflow import SignalFailed, SimulationContext, TaskFailed


def shop() -> SimulationContext:
    """Orders every 5 seconds; packing fails after the 12-second cut-off, and the
    closing task fails the closing signal."""
    app = (
        SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
        .add_resource("packers", current_cap=1, max_cap=1)
        .add_service("packing", dist="constant", value=2)
        .add_arrival("orders", dist="constant", value=5, spawn="pack")
        .add_arrival("closer", dist="constant", value=30, spawn="close")
        .add_signal("closing", once=True)
    )

    @app.task(
        service_id="packing",
        resource_id="packers",
        fail="t > 12",
        fail_reason="after the cut-off",
    )
    def pack(task_id):
        return {"event_type": "packed"}

    @app.task(signal="closing", fail="True", fail_reason="the till did not balance")
    def close(task_id):
        return {"event_type": "closed"}

    return app


# --8<-- [start:task-failed]
# Under stop, the first failure that no on_fail handles reaches the caller of run()
try:
    shop().run(until=60, on_unhandled_failure="stop")
except TaskFailed as failure:
    print(f"{failure.task} failed: {failure.reason}")
# --8<-- [end:task-failed]

# --8<-- [start:signal-failed]
# A run that stops on a signal raises the reason the signal failed with
try:
    shop().run(until=60, stop={"signal": "closing"})
except SignalFailed as failure:
    print(f"{failure.signal} failed: {failure.reason}")
# --8<-- [end:signal-failed]
