"""C5 conditions on the items a store holds: `count(store, ...)`, `items(store, ...)`
and `in`.

Plain Python over the store's `items` sets the answer: SimPy has no such condition,
so the reference reads the items the way a user process would, on every change. A
watcher or a route in the low-level API, the builder and YAML must agree with it.
"""

import simpy

from desflow import CapacityConfig, DynamicStore, SimParameter
from desflow.core.context import SimulationContext
from tests.conformance.harness import run_builder, run_low_level, run_yaml_text, times

WATCH_UP = 0.5
WATCH_DOWN = 3.5
UNTIL = 8


# ------------------------------------------------------------ count


def _reference_count():
    """North parcels arrive at 1 and 3, a south one at 2; a get for a north parcel
    at 5. A log of the north count at every change gives the two resume times."""
    env = simpy.Environment()
    log = []

    class Logged(simpy.FilterStore):
        def _trigger_put(self, get_event):
            super()._trigger_put(get_event)
            self._note()

        def _trigger_get(self, put_event):
            super()._trigger_get(put_event)
            self._note()

        def _note(self):
            log.append(
                (self._env.now, sum(1 for i in self.items if i["area"] == "north"))
            )

    store = Logged(env)

    def put(at, area):
        yield env.timeout(at)
        yield store.put({"area": area})

    def get(at):
        yield env.timeout(at)
        yield store.get(lambda item: item["area"] == "north")

    for at, area in ((1, "north"), (2, "south"), (3, "north")):
        env.process(put(at, area))
    env.process(get(5))
    env.run(until=UNTIL)
    up = next(t for t, n in log if t >= WATCH_UP and n >= 2)
    down = next(t for t, n in log if t >= WATCH_DOWN and n < 2)
    return up, down


def _low_level_count():
    def start(env):
        store = DynamicStore(env, "C5", "parcels")

        def north():
            return sum(1 for item in store.items if item["area"] == "north")

        def watcher(name, at, holds):
            yield env.timeout(at)
            done = env.event()

            def check():
                if holds(north()) and not done.triggered:
                    done.succeed()
                    env.unwatch_changes(check)

            env.watch_changes(check)
            check()
            yield done
            env.publish_event(f"{name}-0", {"event": "resumed"}, task=name, instance=0)

        def put(at, area):
            yield env.timeout(at)
            yield store.put({"area": area})

        def get(at):
            yield env.timeout(at)
            yield store.get(lambda item: item["area"] == "north")

        env.process(watcher("up", WATCH_UP, lambda n: n >= 2))
        env.process(watcher("down", WATCH_DOWN, lambda n: n < 2))
        for at, area in ((1, "north"), (2, "south"), (3, "north")):
            env.process(put(at, area))
        env.process(get(5))

    params = SimParameter(sim_id="C5", stores={"parcels": CapacityConfig()})
    return run_low_level(params, start, UNTIL)[0]


def _builder_count():
    app = SimulationContext("C5", factor=0.0)
    app.add_store("parcels")

    def parcel(name, area):
        @app.task(name=name, put={"store": "parcels"})
        def put_parcel(task_id):
            return {"area": area}

        return put_parcel

    north, south = parcel("north_parcel", "north"), parcel("south_parcel", "south")

    @app.task(
        name="ship_north",
        get={"store": "parcels", "filter": 'item.area == "north"'},
    )
    def ship_north(task_id):
        return {"event": "shipped"}

    @app.task(
        name="up", wait={"condition": 'count(parcels, item.area == "north") >= 2'}
    )
    def up(task_id):
        return {"event": "resumed"}

    @app.task(
        name="down", wait={"condition": 'count(parcels, item.area == "north") < 2'}
    )
    def down(task_id):
        return {"event": "resumed"}

    def script(context):
        plan = [
            (WATCH_UP, up),
            (1, north),
            (2, south),
            (3, north),
            (WATCH_DOWN, down),
            (5, ship_north),
        ]
        for at, task in plan:
            yield context.env.timeout(at - context.env.now)
            context.spawn(task(0))

    app.add_process(script)
    return run_builder(app, UNTIL)


COUNT_YAML = """
simulation: {sim_id: C5, factor: 0}
stores:
  parcels: {}
arrivals:
  ups: {replay: {path: ups.csv}, spawn: up}
  downs: {replay: {path: downs.csv}, spawn: down}
  norths: {replay: {path: norths.csv}, spawn: north_parcel}
  souths: {replay: {path: souths.csv}, spawn: south_parcel}
  ships: {replay: {path: ships.csv}, spawn: ship_north}
tasks:
  north_parcel:
    put: {store: parcels}
    payload: {area: north}
  south_parcel:
    put: {store: parcels}
    payload: {area: south}
  ship_north:
    get: {store: parcels, filter: 'item.area == "north"'}
    payload: {event: shipped}
  up:
    wait: {condition: 'count(parcels, item.area == "north") >= 2'}
    payload: {event: resumed}
  down:
    wait: {condition: 'count(parcels, item.area == "north") < 2'}
    payload: {event: resumed}
run: {until: 8}
"""


def test_c5_count_store_items(tmp_path):
    """North parcels arrive at 1 and 3, a south one at 2, and a get for a north parcel
    at 5. A watcher on `count(parcels, item.area == "north") >= 2` resumes at 3, and
    one on `< 2` that starts at 3.5 resumes at 5, when the get lowers the count."""
    assert _reference_count() == (3, 5)
    outcome = run_yaml_text(
        tmp_path,
        COUNT_YAML,
        {
            "ups": [WATCH_UP],
            "downs": [WATCH_DOWN],
            "norths": [1, 3],
            "souths": [2],
            "ships": [5],
        },
    )
    assert outcome.error is None
    for records in (_low_level_count(), _builder_count(), outcome.records):
        assert times(records, "up") == [3.0]
        assert times(records, "down") == [5.0]


# ------------------------------------------------------------ in / not in

SHELF = [{"sku": "A"}, {"sku": "B"}]


def _reference_membership(skus, shelf):
    return [("known" if sku in {i["sku"] for i in shelf} else "new") for sku in skus]


def _builder_membership(skus, shelf, priority=False):
    app = SimulationContext("C5", factor=0.0)
    app.add_store("shelf", priority=priority, items=shelf)

    @app.task(name="known")
    def known(task_id):
        return {"event": "known"}

    @app.task(name="new")
    def new(task_id):
        return {"event": "new"}

    routes = {
        "when": [
            {"condition": "payload.sku in items(shelf, item.sku)", "task": "known"},
            {
                "condition": "payload.sku not in items(shelf, item.sku)",
                "task": "new",
            },
        ]
    }
    spawned = []
    for index, sku in enumerate(skus):

        @app.task(name=f"receive{index}", next=routes)
        def receive(task_id, sku=sku):
            return {"sku": sku}

        spawned.append(receive)

    def script(context):
        for index, task in enumerate(spawned):
            yield context.env.timeout(1)
            context.spawn(task(index))

    app.add_process(script)
    return run_builder(app, UNTIL)


def _yaml_membership(tmp_path, skus, shelf, priority=False):
    receives = "".join(
        f"""  receive{i}:
    payload: {{sku: {sku}}}
    next:
      when:
        - {{condition: "payload.sku in items(shelf, item.sku)", task: known}}
        - {{condition: "payload.sku not in items(shelf, item.sku)", task: new}}
"""
        for i, sku in enumerate(skus)
    )
    arrivals = "".join(
        f"  a{i}: {{replay: {{path: a{i}.csv}}, spawn: receive{i}}}\n"
        for i in range(len(skus))
    )
    items = ", ".join(
        f"[{i}, {{sku: {item['sku']}}}]" if priority else f"{{sku: {item['sku']}}}"
        for i, item in enumerate(shelf)
    )
    text = f"""
simulation: {{sim_id: C5, factor: 0}}
stores:
  shelf: {{priority: {str(priority).lower()}, items: [{items}]}}
arrivals:
{arrivals}tasks:
  known: {{payload: {{event: known}}}}
  new: {{payload: {{event: new}}}}
{receives}run: {{until: {UNTIL}}}
"""
    outcome = run_yaml_text(
        tmp_path, text, {f"a{i}": [i + 1] for i in range(len(skus))}
    )
    assert outcome.error is None
    return outcome.records


def _routed(records):
    """The route each receipt took, in time order."""
    found = []
    for record in records:
        if record["stream_type"] == "event" and record["task"] in ("known", "new"):
            found.append((record["sim_ts"], record["task"]))
    return sorted(set(found))


def test_c5_item_in_store(tmp_path):
    """`payload.sku in items(shelf, item.sku)` routes a receipt for A and B to `known`,
    and `not in` routes C and an empty shelf's sku to `new`."""
    skus = ["A", "C", "B"]
    expected = [
        (float(i + 1), kind)
        for i, kind in enumerate(_reference_membership(skus, SHELF))
    ]
    assert expected == [(1.0, "known"), (2.0, "new"), (3.0, "known")]
    assert _routed(_builder_membership(skus, SHELF)) == expected
    assert _routed(_yaml_membership(tmp_path, skus, SHELF)) == expected


def test_c5_item_in_priority_store(tmp_path):
    """A priority store's items are unwrapped: `items(shelf, item.sku)` reads the sku
    of the payload, not of the `(priority, item)` pair."""
    skus = ["A", "C", "B"]
    expected = [(1.0, "known"), (2.0, "new"), (3.0, "known")]
    pairs = [(i, item) for i, item in enumerate(SHELF)]
    builder = _builder_membership(skus, pairs, priority=True)
    assert _routed(builder) == expected
    assert _routed(_yaml_membership(tmp_path, skus, SHELF, priority=True)) == expected


def test_c5_item_in_empty_store(tmp_path):
    """An empty store holds no sku, so every receipt is `new` and `count` is 0."""
    skus = ["A", "B"]
    expected = [(1.0, "new"), (2.0, "new")]
    assert _reference_membership(skus, []) == ["new", "new"]
    assert _routed(_builder_membership(skus, [])) == expected
    assert _routed(_yaml_membership(tmp_path, skus, [])) == expected
