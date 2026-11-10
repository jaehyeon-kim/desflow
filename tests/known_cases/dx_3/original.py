"""DX-3: A payload that draws the delivery.

The original is the snippet's own Python: `original.yaml` is the model with
`payload: !python shop_logic.choose_delivery`, as docs/snippets/yaml/tasks.yaml names it. The
test runs it, and the YAML form has its events: the same events at the same times, with
the same fields and the same values apart from `delivery`. The program draws `delivery`
from the shared generator and the YAML form from its own stream, so `compare` states what
holds for it: each is `standard` or `express` in both runs, and the YAML form's share of
`express` over its 1,199 records is within `BOUND` of the snippet's chance of 0.1.
"""

PYTHON_FORM = "tests/known_cases/dx_3/original.yaml"
LOGIC = "docs/snippets/yaml"  # the folder of shop_logic.py
EXPRESS = 0.1  # the chance that `choose_delivery` gives express
# The standard error of a share of 0.1 over 1,199 draws is (0.1 * 0.9 / 1199) ** 0.5,
# about 0.0087, so 0.03 is about 3.5 standard errors
BOUND = 0.03


def _events(records):
    return [
        (r["sim_ts"], r["key"], r["task"], r["instance"], r["value"])
        for r in records
        if r["stream_type"] == "event" and isinstance(r["value"], dict)
    ]


def compare(snippet_records, yaml_records):
    """The same events with the same values, and a delivery drawn as the snippet draws
    it."""
    want, got = _events(snippet_records), _events(yaml_records)
    assert len(got) == len(want), (len(got), len(want))
    for a, b in zip(got, want):
        assert a[:4] == b[:4], (a, b)
        assert {k: v for k, v in a[4].items() if k != "delivery"} == {
            k: v for k, v in b[4].items() if k != "delivery"
        }, (a, b)
        assert ("delivery" in a[4]) == ("delivery" in b[4]), (a, b)
    deliveries = {}
    for name, events in (("snippet", want), ("yaml", got)):
        deliveries[name] = [e[4]["delivery"] for e in events if "delivery" in e[4]]
        assert set(deliveries[name]) == {"standard", "express"}, name
    assert len(deliveries["yaml"]) == 1199
    share = deliveries["yaml"].count("express") / len(deliveries["yaml"])
    assert abs(share - EXPRESS) <= BOUND, share
