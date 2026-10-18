"""A process that publishes the model's state when the run starts, for C6 checks.

Started by every API (a YAML `processes` entry, the builder's `add_process`), it
publishes the packers' capacity, the stock's level and the parcels' items as
telemetry at the initial time, before the first arrival.
"""


def snapshot(context):
    """Publishes the initial state as telemetry named `initial.<name>`."""
    context.publish("initial.packers", context.get_resource("packers").capacity)
    context.publish("initial.stock", context.get_container("stock").level)
    context.publish("initial.parcels", list(context.get_store("parcels").items))
    yield context.env.timeout(0)
