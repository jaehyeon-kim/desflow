"""Batch 11 `wait: {each}` against the benchtop projects' rules.

Each test runs the smallest blueprint form of a benchtop row (the BT rows of the
known-cases list) that makes its rows with a wait on `each`, and states the rule of the
row. The projects are read only. Rows compared here: BT-AQ6 and BT-AQ7 (air-quality:
seven forecasts and a reading each hour), BT-LD9 and BT-LD10 (live-dashboard: an order of
one to four different products, one row for each item) and
BT-WL4 (web-and-logs: one request for each serving service). web-and-logs is planned
and has no folder yet, so its test states the rule of the row.
"""

import textwrap
from collections import Counter, defaultdict
from typing import Any, Dict, List

from tests.conformance.harness import run_yaml_text

HEAD = (
    "simulation: {sim_id: B, factor: 0, random_seed: %d, "
    'logical_start_time: "2026-01-01T00:00:00+00:00", timezone: UTC}\n'
)


def _rows(outcome: Any, task: str) -> List[Dict[str, Any]]:
    assert outcome.error is None, repr(outcome.error)
    return [
        r["value"]
        for r in outcome.records
        if r["stream_type"] == "event"
        and r["task"] == task
        and isinstance(r["value"], dict)
        and "status" not in r["value"]
    ]


def test_each_hour_has_seven_forecasts_and_a_reading(tmp_path):
    """BT-AQ6 and BT-AQ7: an hour makes one reading and a forecast of each lead from 1
    to 7 days, eight rows, as the tables `observations` and `weather_forecasts` hold
    them."""
    hours = 24
    text = HEAD % 5 + textwrap.dedent(
        f"""\
        arrivals: {{h: {{dist: constant, value: 3600, spawn: hour, max_events: {hours}, first_at: 0}}}}
        tasks:
          hour:
            payload: {{kind: reading, hour: {{gen: expr, expr: "int(t // 3600)"}}}}
            next: forecasts
          forecasts:
            wait: {{each: "range(1, 8)", branch: {{task: forecast, args: {{lead: {{gen: expr, expr: each.value}}}}}}}}
            payload: {{kind: issued}}
          forecast: {{payload: {{kind: forecast, lead: {{gen: expr, expr: arg.lead}}}}}}
        run: {{until: {hours * 3600}}}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    readings = _rows(outcome, "hour")
    forecasts = _rows(outcome, "forecast")
    assert len(readings) == hours
    assert len(forecasts) == 7 * hours
    leads = Counter(f["lead"] for f in forecasts)
    assert leads == {lead: hours for lead in range(1, 8)}
    assert len(readings) + len(forecasts) == 8 * hours


def _orders(tmp_path, seed, orders):
    from sales.simulation.catalogue import ITEM_COUNTS

    cuts, running = [], 0.0
    for count, weight in ITEM_COUNTS.items():
        running += weight
        cuts.append((count, round(running, 10)))
    expr = " else ".join(
        [f"{count} if u < {cut}" for count, cut in cuts[:-1]] + [str(cuts[-1][0])]
    )
    text = HEAD % seed + textwrap.dedent(
        f"""\
        arrivals:
          products: {{initial: 8, spawn: product_row}}
          orders: {{dist: constant, value: 1, first_at: 1, max_events: {orders}, spawn: order}}
        tasks:
          product_row:
            payload: {{id: {{gen: sequence, start: 100}}}}
          order:
            vars:
              oid: {{gen: uuid}}
              n: {{gen: expr, expr: "{expr}", names: {{u: {{gen: uniform, low: 0, high: 1}}}}}}
            set:
              order_id: {{gen: var, name: oid}}
              n: {{gen: var, name: n}}
              products: {{gen: repeat, times: {{gen: var, name: n}}, value: {{gen: lookup, task: product_row, path: id, distinct: true}}}}
            payload: {{kind: order, id: {{gen: var, name: oid}}, num_of_item: {{gen: var, name: n}}}}
            next: items
          items:
            wait: {{each: "range(attr.n)", branch: {{task: order_item, args: {{i: {{gen: expr, expr: each.value}}}}}}}}
            payload: {{kind: items_written}}
          order_item:
            payload: {{kind: item, order_id: {{gen: attr, name: order_id}}, product_id: {{gen: expr, expr: "attr.products[arg.i]"}}}}
        run: {{until: {orders + 5}}}
        """
    )
    return run_yaml_text(tmp_path, text, {})


def test_an_order_has_one_row_for_each_item_and_each_item_is_a_different_product(
    tmp_path, project
):
    """BT-LD9 and BT-LD10: an order holds one to four items by the project's
    `ITEM_COUNTS`, there is one `order_items` row for each item, and the products of one
    order are different, as `rng.choice(..., replace=False)` of `basket` gives."""
    project("live-dashboard")
    from sales.simulation.catalogue import ITEM_COUNTS

    orders = 800
    outcome = _orders(tmp_path, 9, orders)
    placed = {o["id"]: o["num_of_item"] for o in _rows(outcome, "order")}
    rows = defaultdict(list)
    for item in _rows(outcome, "order_item"):
        rows[item["order_id"]].append(item["product_id"])
    assert len(placed) == orders
    assert set(placed.values()) <= set(ITEM_COUNTS)
    for order_id, count in placed.items():
        assert len(rows[order_id]) == count
        assert len(set(rows[order_id])) == count
    seen = Counter(placed.values())
    for count, weight in ITEM_COUNTS.items():
        assert abs(seen[count] / orders - weight) < 0.06, (count, seen)


def test_a_request_is_made_for_each_serving_service(tmp_path):
    """BT-WL4: a visit makes one request for each element of a drawn number of
    services, so a visit of `n` services has `n` requests, with the indexes 0 to n - 1."""
    visits = 300
    text = HEAD % 3 + textwrap.dedent(
        f"""\
        arrivals: {{v: {{dist: constant, value: 1, spawn: visit, max_events: {visits}, first_at: 0}}}}
        tasks:
          visit:
            wait: {{each: {{gen: integer, bounds: [1, 3]}}, branch: {{task: one_request, args: {{i: {{gen: expr, expr: each.index}}}}}}}}
            payload: {{kind: visit, n: {{gen: expr, expr: "len(wait.each.values)"}}}}
          one_request: {{payload: {{kind: request, i: {{gen: expr, expr: arg.i}}}}}}
        run: {{until: {visits + 5}}}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    visits_made = _rows(outcome, "visit")
    requests = [r["i"] for r in _rows(outcome, "one_request")]
    assert len(visits_made) == visits
    assert Counter(v["n"] for v in visits_made).keys() == {1, 2, 3}
    assert len(requests) == sum(v["n"] for v in visits_made)
    assert Counter(requests)[0] == visits


def test_an_order_no_picker_takes_in_time_is_cancelled_with_each_of_its_items(tmp_path):
    """BT-LD12: an order that waits longer than its patience for a picker is
    `Cancelled`, and each of its items is written again as `Cancelled`. The tables are
    left out of the document, because they need Postgres; a record is the row it would
    write."""
    text = HEAD % 4 + textwrap.dedent(
        """\
        resources: {pickers: {current_cap: 1}}
        arrivals:
          busy: {initial: 1, spawn: hold}
          orders: {dist: constant, value: 1, first_at: 1, max_events: 3, spawn: order, attributes: {n: {gen: sequence, start: 1}}}
        tasks:
          hold: {resource: pickers, service_time: 2.5, payload: {kind: hold}}
          order:
            payload: {kind: order, state: Processing}
            next: pick
          pick:
            resource: pickers
            service_time: 1
            patience: 1
            on_renege: cancelled
            payload: {kind: order, state: Shipped}
          cancelled:
            payload: {kind: order, state: Cancelled}
            next: items_cancelled
          items_cancelled:
            wait: {each: "range(attr.n)", branch: {task: order_item, args: {state: Cancelled}}}
            payload: {kind: items_cancelled}
          order_item: {payload: {kind: item, state: {gen: expr, expr: arg.state}}}
        run: {until: 30}
        """
    )
    outcome = run_yaml_text(tmp_path, text, {})
    cancelled = [r for r in _rows(outcome, "cancelled")]
    items = _rows(outcome, "order_item")
    assert len(cancelled) >= 1
    assert all(i["state"] == "Cancelled" for i in items)
    keys = {
        r["key"]
        for r in outcome.records
        if r["stream_type"] == "event" and r["task"] == "cancelled"
    }
    expected = sum(int(k.split("-")[1]) for k in keys)
    assert len(items) == expected
