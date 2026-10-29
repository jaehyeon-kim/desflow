"""Python for the Wait for One or All blueprints: the order's payload, which reads
its wait, as the builder's task function does."""


def branches(task_id, context, wait):
    """The order's event: which branches were done when its wait ended."""
    # `wait` holds each branch's done, ok and value
    return {
        "event_type": "resumed",
        "done": {name: branch["done"] for name, branch in wait.items()},
    }
