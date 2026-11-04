"""BT-WL1 to BT-WL12: web-and-logs.

The generator is planned in `planning/benchtop/plan-series-web-and-logs.md`, Part 1, and
has no project folder, so there is no Python function to run. `check` states the rules
of that plan and the test runs it on the records of the YAML form:

- the variables hold the tenants, a tenant's domains, a domain's services, a service's
  paths, the requests per hit of a tenant and the error rate of a service;
- each domain has its own visit arrival, and a visit publishes a page hit to
  `web_events`;
- for each service of the domain that serves the hit's path, a visit makes as many
  requests as its tenant's ratio, each to `logs` with the visit's fields;
- the status of a request follows its service's live error rate, and its level follows
  the status;
- a visit at `/checkout` may go on to `/confirmation`;
- a planned fault changes the registry and publishes one `ground_truth` row with its
  type, start, end, tenants, domains and services;
- headless bots publish hits with a label;
- the three mappings are written to PostgreSQL once at the start.

The document's world is smaller than the plan's: two tenants, each with one or two
domains and two or three services, where the plan gives five tenants with two to four
domains and three to five services. The plan gives no names, rates or fault times. The
values below are the document's chosen values, and `check` compares the records with
them, so a change of one of them in the document fails.
"""

import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from desflow import KafkaEgress, PostgresEgress, SimulationContext
from desflow.blueprint.loader import read_document
from desflow.connectors.egress.kafka import ConfluentAvroSerializer

from tests.known_cases import harness

HERE = Path(__file__).parent
VARIABLES = {"FACTOR": "0"}
UNTIL = 700
# The document's chosen world: the plan's mappings, ratios and error rates.
DOMAINS = {"acme": ["acme.com", "shop.acme.com"], "globex": ["globex.io"]}
SERVICES = {
    "acme.com": ["acme-api", "acme-web", "acme-pay"],
    "shop.acme.com": ["acme-web", "acme-pay"],
    "globex.io": ["globex-api", "globex-web"],
}
PATHS = {
    "acme-api": ["/"],
    "acme-web": ["/"],
    "acme-pay": ["/checkout", "/confirmation"],
    "globex-api": ["/", "/checkout", "/confirmation"],
    "globex-web": ["/"],
}
RATIO = {"acme": 2, "globex": 1}
ERROR_RATE = {
    "acme-api": 0.02,
    "acme-web": 0.005,
    "acme-pay": 0.01,
    "globex-api": 0.02,
    "globex-web": 0.005,
}
VISITS = {"acme.com": 1, "shop.acme.com": 0.5, "globex.io": 0.5}  # visits a second
# The document's chosen fault: the checkout service of acme fails from 600 s.
FAULT = {
    "event_type": "ground_truth",
    "type": "service_fault",
    "start": 600,
    "end": UNTIL,
    "tenants": ["acme"],
    "domains": ["acme.com", "shop.acme.com"],
    "services": ["acme-pay"],
}
FAULT_ERROR_RATE = 0.4
BOTS = 0.2  # bot hits a second


def check(records: list[dict[str, Any]]) -> None:
    """The rules of the plan, on the YAML form's records.

    Args:
        records: The records of the YAML form.

    Raises:
        AssertionError: At the first rule the records break.
    """
    events = [
        r for r in records if r["stream_type"] == "event" and r["value"] is not None
    ]
    by_task = defaultdict(list)
    for record in events:
        if "status" not in record["value"] or "event_type" in record["value"]:
            by_task[record["task"]].append(record)
    _world()
    # BT-WL10: the three mappings are written at the start, once
    rows = by_task["mapping_row"]
    assert all(r["sim_ts"] == 0.0 for r in rows)
    tables = defaultdict(dict)
    for r in rows:
        tables[r["value"]["__table__"]][r["value"]["name"]] = r["value"]["items"]
    assert tables == {
        "tenant_domains": DOMAINS,
        "domain_services": SERVICES,
        "service_paths": PATHS,
    }
    assert len(rows) == len(DOMAINS) + len(SERVICES) + len(PATHS)
    # BT-WL2 and BT-WL3: one visit arrival for each domain; a hit at `/` or `/checkout`
    hits = by_task["hit"]
    for r in hits:
        value = r["value"]
        assert value["__topic__"] == "web_events" and value["path"] in (
            "/",
            "/checkout",
        )
        assert value["domain"] in DOMAINS[value["tenant"]]
        assert r["key"].startswith(value["domain"].replace(".", "_") + "-")
    count = Counter(r["value"]["domain"] for r in hits)
    for domain, rate in VISITS.items():
        assert 0.8 * rate * UNTIL <= count[domain] <= 1.2 * rate * UNTIL, domain
    # BT-WL3: a confirmation follows only a checkout, and not every checkout
    shown = {r["key"]: r["value"] for r in hits}
    confirmed = [r["key"] for r in by_task["confirmation"]]
    checkouts = {k for k, v in shown.items() if v["path"] == "/checkout"}
    assert confirmed and set(confirmed) < checkouts
    # BT-WL4 and BT-WL7: for each hit, the tenant's ratio of requests to each service of
    # the domain that serves its path, with the visit's fields
    pages = [(r["key"], r["value"]) for r in hits + by_task["confirmation"]]
    asked = defaultdict(Counter)
    for r in by_task["request"]:
        value = r["value"]
        assert value["__topic__"] == "logs"
        visit = shown[r["key"]]
        assert (value["tenant"], value["domain"]) == (visit["tenant"], visit["domain"])
        asked[(r["key"], value["path"])][value["service"]] += 1
    for key, page in pages:
        serving = [s for s in SERVICES[page["domain"]] if page["path"] in PATHS[s]]
        want = {s: RATIO[page["tenant"]] for s in serving}
        assert asked[(key, page["path"])] == want, (
            key,
            page,
            asked[(key, page["path"])],
        )
    assert pages and set(asked) == {(key, page["path"]) for key, page in pages}
    # BT-WL5 and BT-WL6: the status follows the service's live error rate, within two
    # standard errors, before the fault and after it; the level follows the status
    for service, rate in ERROR_RATE.items():
        for when, want in ((False, rate), (True, _rate_after(service, rate))):
            rows = [
                r["value"]["status"]
                for r in by_task["request"]
                if r["value"]["service"] == service
                and (r["sim_ts"] >= FAULT["start"]) == when
            ]
            found = sum(code == 500 for code in rows) / len(rows)
            two_errors = 2 * (want * (1 - want) / len(rows)) ** 0.5
            assert abs(found - want) <= two_errors, (service, when, found, want)
    statuses = {r["value"]["status"] for r in by_task["request"]}
    assert statuses == {200, 500}
    for r in by_task["request"]:
        assert r["value"]["level"] == {200: "INFO", 500: "ERROR"}[r["value"]["status"]]
    # BT-WL9: bots publish hits with a label, about one every five seconds
    bots = by_task["bot_hit"]
    assert all(r["value"]["label"] == "bot" for r in bots)
    assert all(r["value"]["__topic__"] == "web_events" for r in bots)
    assert 0.8 * BOTS * UNTIL <= len(bots) <= 1.2 * BOTS * UNTIL
    # BT-WL11: the events go to two topics, which the ground truth's topic joins
    topics = {r["value"]["__topic__"] for r in events if "__topic__" in r["value"]}
    assert topics == {"web_events", "logs"}
    _sinks_and_faults()


def _rate_after(service: str, rate: float) -> float:
    """The error rate of a service once the fault has started."""
    return FAULT_ERROR_RATE if service in FAULT["services"] else rate


def _world() -> None:
    """BT-WL1: the document's variables are the world, and each tenant has one service
    that serves `/checkout`."""
    variables = read_document(HERE / "model.yaml", variables=VARIABLES)[0]["variables"]
    assert variables["tenants"] == list(DOMAINS)
    assert variables["domains"] == DOMAINS
    assert variables["services"] == SERVICES
    assert variables["paths"] == PATHS
    assert variables["ratio"] == RATIO
    assert variables["error_rate"] == ERROR_RATE
    for tenant, domains in DOMAINS.items():
        own = {s for d in domains for s in SERVICES[d]}
        assert len([s for s in own if "/checkout" in PATHS[s]]) == 1, tenant


def _sinks_and_faults() -> None:
    """BT-WL8, BT-WL10, BT-WL11 and BT-WL12: the fault's ground truth row and its change
    of the registry, and the sinks as the file builds them."""
    with tempfile.TemporaryDirectory() as folder:
        run = harness.run_yaml("BT-WL1", Path(folder))
    assert run.error is None
    (label,) = run.labels
    assert label["sim_ts"] == FAULT["start"] and label["label"] == FAULT
    document = read_document(HERE / "model.yaml", variables=VARIABLES)[0]
    (step,) = [s for s in document["scenario"] if "path" in s]
    assert step["at"] == FAULT["start"]
    assert step["path"] == "Web.variables.error_rate"
    assert step["value"] == {s: _rate_after(s, r) for s, r in ERROR_RATE.items()}
    app = SimulationContext.from_yaml(HERE / "model.yaml", variables=VARIABLES)
    events, truth, *tables = app._egress_providers
    assert isinstance(events, KafkaEgress) and isinstance(truth, KafkaEgress)
    assert app._egress_labels == [False, True, False, False, False]
    assert truth.event_topic == "ground_truth"
    assert isinstance(events.default_serializer, ConfluentAvroSerializer)  # WL12
    assert all(isinstance(sink, PostgresEgress) for sink in tables)  # WL10
    assert [sink.table_name for sink in tables] == [
        "tenant_domains",
        "domain_services",
        "service_paths",
    ]
