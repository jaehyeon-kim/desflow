"""GA-8: An arrival rate changes while the run continues, from a Kafka message.

The original is the test asserts of the section of yaml-only.md Part 4: `check` states
them and the test runs it on the records of the YAML form. Its last assert runs the
model with the ingress replaced by a stand-in provider that puts the decoded message on
the ingress queue, at `factor: 0.001` to `until: 10 min`, so the case runs against the
wall clock and is paced.
"""

from pathlib import Path

from desflow import KafkaIngress, SimulationContext
from desflow.connectors.ingress.base import BaseIngress, parse_change

HERE = Path(__file__).parent
PACED = True
PATH = "Shop.arrival.orders.rate"


class _MessageIngress(BaseIngress):
    """The stand-in for the Kafka ingress: it puts the message of the section, as the
    ingress puts a decoded message."""

    async def run(self, ingress_queue):
        ingress_queue.put(parse_change({"path": PATH, "value": 0.05}))


def check(records):
    """The file builds with one `KafkaIngress` on `sim-config`, and the path of the
    message, `Shop.arrival.orders.rate`, is a registry path that holds a float. With the
    ingress replaced by the stand-in, a run at `factor: 0.001` with `until: 10 min` ends
    with 0.05 at that path."""
    app = SimulationContext.from_yaml(HERE / "model.yaml")
    assert [type(p) for p in app._ingress_providers] == [KafkaIngress]
    assert app._ingress_providers[0].topic == "sim-config"
    rate = app.compile_parameters().arrival["orders"].rate
    assert isinstance(rate, float)
    assert any(r["stream_type"] == "event" for r in records)

    app._ingress_providers = [_MessageIngress()]
    app.factor = 0.001  # real time for the ingress thread to deliver
    app.run(until=600)
    assert app.env.registry.get_config("Shop.arrival.orders").rate == 0.05
