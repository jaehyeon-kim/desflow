"""Conditions and amounts: what the whitelist allows, what it refuses, and the names."""

from types import SimpleNamespace

import pytest

from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.core.expressions import Expression, Scope
from desflow.models.params import DistributionConfig, SimParameter


@pytest.fixture
def scope():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            arrival={"orders": DistributionConfig(dist="exponential", rate=2.0)},
        )
    )
    return Scope(
        payload={"total": 120.0, "channel": "web"},
        attributes={"priority": 1},
        levels={"stock": SimpleNamespace(level=0)},
        registry=env.registry,
        now=30.0,
    )


@pytest.mark.parametrize(
    "text,expected",
    [
        ("payload.total > 100", True),
        ("payload.total * 2 - 40 == 200", True),
        ("payload.total // 7 % 5 / 2", 1.0),
        ("-payload.total < 0 and not attr.priority == 2", True),
        ("level.stock == 0 or payload.total > 1000", True),
        ('payload.channel == "web"', True),
        ('payload.channel != "store"', True),
        ('registry["Shop.arrival.orders.rate"] >= 2', True),
        ("t < 60", True),
        ("1 < attr.priority + 1 <= 2", True),
        ("attr.priority if t > 10 else 5", 1),
        ("True and payload.total > 100.5", True),
    ],
)
def test_allowed_forms_evaluate(scope, text, expected):
    assert Expression(text).evaluate(scope) == expected


@pytest.mark.parametrize(
    "text,fragment",
    [
        ("__import__('os')", "Call"),
        ("payload.total.real", "attribute that is not"),
        ("os.system", "attribute that is not"),
        ("payload.__class__", "'__class__'"),
        ("total > 100", "the name 'total'"),
        ("payload['total'] > 1", "an index other than"),
        ("registry[0]", "an index other than"),
        ("payload.total ** 2", "Pow"),
        ("attr.flags & 1", "BitAnd"),
        ("~attr.flags", "Invert"),
        ('payload.channel in "web"', "in or not in with anything but items("),
        ("attr.x is None", "Is"),
        ('payload.channel < "x"', "a string outside"),
        ('"web"', "a string outside"),
        ("[1, 2]", "List"),
        ("{'a': 1}", "Dict"),
        ("(1, 2)", "Tuple"),
        ("lambda: 1", "Lambda"),
        ("[x for x in t]", "ListComp"),
        ("f'{t}'", "JoinedStr"),
        ("(y := 1)", "NamedExpr"),
        ("None", "the constant None"),
        ("payload.total >", "not a valid expression"),
        ("wait.restock", "wait.<name>.done, .ok, .value"),
        ("wait.restock.value.qty.unit", "or .value.<field>"),
        ("signal.restocked.done", "signal.<name>.count, .ok, .value"),
    ],
)
def test_forms_outside_the_whitelist_are_refused(text, fragment):
    with pytest.raises(ValueError, match="an expression cannot|not a valid") as error:
        Expression(text)
    assert fragment in str(error.value)


def test_references_name_each_source():
    expression = Expression(
        "payload.total > 1 and attr.vip == 1 and level.stock > 0 "
        'and users.machine < 2 and queue.machine >= 1 and registry["Shop.arrival.orders.rate"] > 0 and item.size == 2 '
        "and wait.restock.value.qty > 0 and signal.restocked.count > 0 "
        "and putters.shelf > 0 and getters.shelf == 0 "
        'and count(shelf, item.area == "north") >= 1 and payload.sku in items(shelf, item.sku)'
    )
    assert expression.references == {
        "payload": {"total", "sku"},
        "attr": {"vip"},
        "level": {"stock"},
        "users": {"machine"},
        "queue": {"machine"},
        "putters": {"shelf"},
        "getters": {"shelf"},
        "count": {"shelf"},
        "items": {"shelf"},
        "store_item": {"area", "sku"},
        "item": {"size"},
        "wait": {"restock"},
        "signal": {"restocked"},
        "process": set(),
        "registry": {"Shop.arrival.orders.rate"},
    }


@pytest.mark.parametrize(
    "text,fragment",
    [
        ("payload.missing > 1", "no field 'missing'"),
        ("attr.missing > 1", "no attribute 'missing'"),
        ("level.shelf > 1", "no store or container is called 'shelf'"),
        ("item.size > 1", "the item has no field 'size'"),
        ("wait.restock.done", "no wait branch 'restock'"),
        ("signal.restocked.count > 0", "no signal is called 'restocked'"),
        ("payload.total / (attr.priority - 1)", "division by zero"),
    ],
)
def test_a_missing_name_fails_with_the_expression(scope, text, fragment):
    with pytest.raises(ValueError, match="cannot evaluate '") as error:
        Expression(text).evaluate(scope)
    assert fragment in str(error.value)


class _Holder:
    """A store as the scope sees it: items, a priority flag and the two queue counts."""

    def __init__(self, items, priority=False, putters=0, getters=0):
        self.items = items
        self.priority = priority
        self.level = len(items)
        self.putters_waiting = putters
        self.getters_waiting = getters


@pytest.mark.parametrize(
    "text,expected",
    [
        ("putters.shelf", 2),
        ("getters.shelf", 1),
        ("count(shelf)", 3),
        ('count(shelf, item.area == "north")', 2),
        ('count(shelf, item.area == "west")', 0),
        ("items(shelf, item.sku)", ["a", "b", "c"]),
        ('"b" in items(shelf, item.sku)', True),
        ('"z" in items(shelf, item.sku)', False),
        ('"z" not in items(shelf, item.sku)', True),
        ("count(shelf, item.n > payload.total)", 1),
    ],
)
def test_store_names_and_functions(text, expected):
    items = [
        {"sku": "a", "area": "north", "n": 1},
        {"sku": "b", "area": "south", "n": 5},
        {"sku": "c", "area": "north", "n": 2},
    ]
    scope = Scope(
        payload={"total": 3}, levels={"shelf": _Holder(items, putters=2, getters=1)}
    )
    assert Expression(text).evaluate(scope) == expected


def test_a_priority_store_is_read_without_its_priority_wrapper():
    from simpy import PriorityItem

    held = [PriorityItem((1, 0), {"sku": "a"}), PriorityItem((2, 1), {"sku": "b"})]
    scope = Scope(levels={"shelf": _Holder(held, priority=True)})
    assert Expression("items(shelf, item.sku)").evaluate(scope) == ["a", "b"]
    assert Expression("count(shelf)").evaluate(scope) == 2


def test_an_empty_store_counts_zero():
    scope = Scope(levels={"shelf": _Holder([])})
    assert Expression("count(shelf)").evaluate(scope) == 0
    assert Expression("count(shelf, item.area == 'x')").evaluate(scope) == 0
    assert Expression('"a" in items(shelf, item.sku)').evaluate(scope) is False


@pytest.mark.parametrize(
    "text,fragment",
    [
        ("sum(shelf)", "a Call other than count(<store>) or items(<store>)"),
        ("count(shelf.x)", "of anything but a bare store name"),
        ("count(level.shelf)", "of anything but a bare store name"),
        ("count()", "count() with other arguments"),
        ("count(shelf, item.a, 1)", "count() with other arguments"),
        ("count(shelf, key=1)", "count() with other arguments"),
        ("items(shelf, 1)", "items(<store>, ...) with anything but item.<field>"),
        (
            "items(shelf, item.a + 1)",
            "items(<store>, ...) with anything but item.<field>",
        ),
        ("count(shelf, count(shelf) > 1)", "count() inside count() or items()"),
        ("1 in items(shelf) == True", "in or not in with anything but items("),
        ("payload.x in payload.y", "in or not in with anything but items("),
        ("payload.x in count(shelf)", "in or not in with anything but items("),
        ('count(shelf, item.a == "x") + "y"', "a string outside"),
    ],
)
def test_count_and_items_take_a_bare_store_name_only(text, fragment):
    with pytest.raises(ValueError, match="an expression cannot") as error:
        Expression(text)
    assert fragment in str(error.value)


def test_a_container_or_a_missing_store_fails_with_the_expression():
    container = type(
        "C", (), {"level": 1, "putters_waiting": 0, "getters_waiting": 0}
    )()
    scope = Scope(levels={"tank": container})
    with pytest.raises(ValueError, match="is a container"):
        Expression("count(tank)").evaluate(scope)
    with pytest.raises(ValueError, match="no store is called 'shelf'"):
        Expression("count(shelf)").evaluate(scope)
    with pytest.raises(ValueError, match="no store or container is called 'shelf'"):
        Expression("putters.shelf").evaluate(scope)


def test_item_names_inside_the_functions_are_kept_apart_from_item():
    expression = Expression('count(shelf, item.area == "north") > 0 and item.size > 1')
    assert expression.references["store_item"] == {"area"}
    assert expression.references["item"] == {"size"}
