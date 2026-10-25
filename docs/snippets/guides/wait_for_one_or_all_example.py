"""Wait for one or all: a shop where orders wait for a restock or a time limit.

Each order waits for the next restock or 30 seconds, whichever comes first, and is
packed after a restock or refunded at the limit. A restock waits for both its
delivery and a stock count, then fires the signal the orders wait for. The script
runs 10 minutes at factor 0 and prints how many orders were packed and refunded, and
the times of the restocks: python wait_for_one_or_all_example.py.
"""

from desflow import SimulationContext

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_arrival("orders", dist="exponential", rate=0.1, spawn="order")
    .add_arrival("deliveries", dist="constant", value=60, spawn="restock")
    .add_service("counting", dist="constant", value=20)
    .add_signal("restocked")
    .with_stats("basic")
)


# Each order waits for the next restock or 30 seconds, whichever comes first
@app.task(
    wait={"any": {"stock": {"signal": "restocked"}, "limit": {"timeout": 30}}},
    next={
        "when": [{"condition": "wait.stock.done", "task": "pack"}],
        "default": "refund",
    },
)
def order(task_id):
    return {"event_type": "waited"}


@app.task()
def pack(task_id):
    return {"event_type": "packed"}


@app.task()
def refund(task_id):
    return {"event_type": "refunded"}


restocked_at = []  # the time of each restock


# A restock waits for its delivery, 10 seconds, and a stock count, a child flow of
# 20 seconds; when both are done it finishes and fires restocked
@app.task(
    wait={"all": {"delivery": {"timeout": 10}, "count": {"task": "count_stock"}}},
    signal="restocked",
)
def restock(task_id):
    restocked_at.append(app.env.now)
    return {"event_type": "restocked"}


@app.task(service_id="counting")
def count_stock(task_id):
    return {"event_type": "counted"}


def outcome() -> dict:
    """Runs 10 minutes and returns the orders packed and refunded, and the times of
    the restocks."""
    app.run(until="10 min")
    readings = app.env.stats.readings()
    counts = {
        task: readings[f"Shop.stats.tasks.{task}.finished"]
        for task in ("pack", "refund")
    }
    return {**counts, "restocked_at": restocked_at}


if __name__ == "__main__":
    print(outcome())
