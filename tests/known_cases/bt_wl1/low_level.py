"""BT-WL1 to BT-WL12: web-and-logs, visits of each domain, their requests and the
reference data.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. A gap is drawn with `Sampler`, from the stream of its
arrival, and the drawn fields are rendered by a `Template` named for the task, so they
draw from the random sequences the document's generators at those places draw from.
"""

import copy
from datetime import datetime, timezone
from typing import Any

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
    SimParameter,
)
from desflow.values import Template, ValueRuntime

from tests.known_cases.bt_support import Context, Entity
from tests.known_cases.bt_wl1.builder import ARRIVALS, SCENARIO, VARIABLES

UNTIL = 700


def _params() -> SimParameter:
    return SimParameter(
        sim_id="Web",
        arrival={
            name: DistributionConfig(dist=spec["dist"], rate=spec["rate"])
            for name, spec in ARRIVALS.items()
        },
        variables=copy.deepcopy(VARIABLES),
    )


def __getattr__(name: str) -> Any:
    """`PARAMS`, new for each run: the scenario's step changes `error_rate` in the
    registry, which would change the mapping a second run starts with."""
    if name == "PARAMS":
        return _params()
    raise AttributeError(name)


ENVIRONMENT = {
    "logical_start_time": datetime(2026, 1, 1, tzinfo=timezone.utc),
    "timezone": "UTC",
}
ONCE = {
    "path": {"gen": "weighted", "options": {"/": 5, "/checkout": 1}},
    "goes_on": {"gen": "boolean", "p": 0.5},
}


def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
    sampler = Sampler(seed=42)
    context = Context(ValueRuntime(42, env, sampler, None))
    variables = env.registry

    def value(name: str) -> Any:
        return variables.get(f"Web.variables.{name}").value

    # the drawn fields: the page of a hit, whether a checkout goes on, and the status
    # of a request, which reads its service's live error rate
    hit = Template(
        {
            "path": {"gen": "var", "name": "path"},
            "goes_on": {"gen": "var", "name": "goes_on"},
        },
        name="tasks.hit",
        vars_once=ONCE,
        check=False,
    )
    request = Template(
        {
            "status": {"gen": "var", "name": "code"},
            "level": {
                "gen": "param",
                "path": "Web.variables.level_of",
                "field": {"gen": "expr", "expr": "str(code)"},
            },
        },
        name="tasks.request",
        vars={
            "err": {
                "gen": "boolean",
                "p": {
                    "gen": "param",
                    "path": "Web.variables.error_rate",
                    "field": {"gen": "attr", "name": "service"},
                },
            },
            "code": {"gen": "expr", "expr": "500 if err else 200"},
        },
        check=False,
    )

    def status(key: str, task: str, state: str) -> None:
        env.publish_event(key, {"status": state}, task=task)

    def requests(key: str, visit: dict[str, Any]) -> None:
        """A request of each service of the domain that serves the page, `ratio` times."""
        serving = [
            s
            for s in value("services")[visit["domain"]]
            if visit["path"] in value("paths")[s]
        ]
        status(key, "requests", "queued")
        for _ in serving:
            status(key, "calls", "queued")
        for service in serving:
            for _ in range(value("ratio")[visit["tenant"]]):
                drawn = request.render(context, Entity(key, service=service))
                env.publish_event(
                    key,
                    {
                        "event_type": "request",
                        **visit,
                        "service": service,
                        **drawn,
                        "__topic__": "logs",
                    },
                    task="request",
                )
        for _ in serving:
            status(key, "calls", "started")
        status(key, "requests", "started")

    def visit(key: str, tenant: str, domain: str) -> None:
        drawn = hit.render(context, Entity(key))
        page = {"tenant": tenant, "domain": domain, "path": drawn["path"]}
        env.publish_event(
            key, {"event_type": "hit", **page, "__topic__": "web_events"}, task="hit"
        )
        requests(key, page)
        if page["path"] == "/checkout" and drawn["goes_on"]:
            page = {**page, "path": "/confirmation"}
            env.publish_event(
                key,
                {"event_type": "hit", **page, "__topic__": "web_events"},
                task="confirmation",
            )
            requests(key, page)

    def arrivals(name: str) -> Any:
        config = env.registry.get_config(f"Web.arrival.{name}")
        config.stream = f"arrival.{name}"
        number = 1
        while True:
            yield env.timeout(sampler.sample(config))
            key = f"{name}-{number}"
            if name == "bots":
                env.publish_event(
                    key,
                    {"event_type": "hit", "label": "bot", "__topic__": "web_events"},
                    task="bot_hit",
                )
            else:
                attributes = ARRIVALS[name]["attributes"]
                visit(key, attributes["tenant"], attributes["domain"])
            number += 1

    def references() -> Any:
        """The three mappings, a row for each key, once for each entity at the start.
        The entities go through the three tables together, each table in turn: the
        rows of one table follow the start of the table before it."""
        keys = [f"reference-{number}" for number in range(1, size + 1)]
        mappings = (
            ("domains", "tenant_domains"),
            ("services", "domain_services"),
            ("paths", "service_paths"),
        )

        def rows(key: str, variable: str, table: str) -> None:
            for name, items in value(variable).items():
                env.publish_event(
                    key,
                    {"__table__": table, "name": name, "items": items},
                    task="mapping_row",
                )

        for key in keys:
            status(key, mappings[0][1], "queued")
        for key in keys:
            rows(key, *mappings[0])
        for before, (variable, table) in zip(mappings, mappings[1:]):
            for key in keys:
                status(key, before[1], "started")
                status(key, table, "queued")
                rows(key, variable, table)
        for key in keys:
            status(key, mappings[-1][1], "started")
        yield env.timeout(0)

    def scenario() -> Any:
        step, record = SCENARIO
        yield env.timeout(step["at"])
        env.registry.update(step["path"], copy.deepcopy(step["value"]))
        env.publish_label(record["record"])

    env.process(references())
    env.process(scenario())
    for name in ARRIVALS:
        env.process(arrivals(name))
