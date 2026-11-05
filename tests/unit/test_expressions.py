"""Conditions and amounts: what the whitelist allows, what it refuses, and the names."""

import re
from types import SimpleNamespace

import pytest

from desflow.core.environment import DynamicRealtimeEnvironment
from desflow.blueprint.models import check_var_names
from desflow.core.expressions import ANY_NAME, RESERVED_NAMES, Expression, Scope
from desflow.values.models import Expr
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


def test_an_attribute_written_as_an_index_is_the_same_read(scope):
    """`attr["class"]` reads an attribute whose name Python refuses as a word, and
    gives the same value and the same reference as `attr.priority`."""
    scope.attributes["class"] = 3
    scope.attributes["my key"] = 4
    expression = Expression('attr["class"] * 2 + attr["my key"] + attr["priority"]')
    assert expression.evaluate(scope) == 11
    assert expression.references["attr"] == {"class", "my key", "priority"}
    assert Expression('has(attr["class"])').evaluate(scope) is True
    assert Expression('[attr["class"]][0]').evaluate(scope) == 3
    with pytest.raises(ValueError, match="the entity has no attribute 'nope'"):
        Expression('attr["nope"]').evaluate(scope)


@pytest.mark.parametrize("text", ["attr[0]", "attr[payload.total]", 'payload["total"]'])
def test_only_a_text_index_on_attr_is_a_read(text):
    with pytest.raises(ValueError, match="'attr'|'payload'"):
        Expression(text)


@pytest.mark.parametrize(
    "text,fragment",
    [
        ("__import__('os')", "Call"),
        ("os.system", "attribute that is not"),
        ("payload.__class__", "'__class__'"),
        ("total > 100", "the name 'total'"),
        ("registry[0]", "an index other than"),
        ("payload.total @ 2", "MatMult"),
        ("attr.flags & 1", "BitAnd"),
        ("~attr.flags", "Invert"),
        ("attr.x is attr.y", "is or is not with anything but None"),
        ('payload.channel < "x"', "a string outside"),
        ('"web"', "a string outside"),
        ("{**attr.d}", "a ** inside a mapping"),
        ("(1, 2)", "Tuple"),
        ("lambda: 1", "Lambda"),
        ("[x for x in t for y in t]", "more than one clause"),
        ("[t for t in attr.l]", "the loop name 't'"),
        ("attr.l[0:1:2:3]", "not a valid expression"),
        ("users.machine[1:]", "an index on users.machine, which is a number"),
        ("level.stock.x", "a level read is a number"),
        ("f'{t}'", "JoinedStr"),
        ("(y := 1)", "NamedExpr"),
        ("payload.total >", "not a valid expression"),
        ("wait.restock", "wait.<name>.done, .ok, .value"),
        ("wait.restock.done.x", "or .value.<field>"),
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
        "and handle.slot.alive "
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
        "handle": {"slot"},
        "process": set(),
        "registry": {"Shop.arrival.orders.rate"},
        "name": set(),
        "has": set(),
        "arg": set(),
        "cause": set(),
        "upstream": set(),
        "held": set(),
        "waiting": set(),
        "putting": set(),
        "getting": set(),
        "running": set(),
        "members": set(),
        "each": set(),
        "member": set(),
        "flag": set(),
        "loop": set(),
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


def test_division_and_floor_division_follow_python(scope):
    """The evaluator uses Python arithmetic: `/` gives a float and `//` an integer."""
    assert Expression("7 / 2").evaluate(scope) == 3.5
    assert type(Expression("7 / 2").evaluate(scope)) is float
    assert Expression("7 // 2").evaluate(scope) == 3
    assert type(Expression("7 // 2").evaluate(scope)) is int
    # `//` rounds down, and keeps a float when one operand is a float
    assert Expression("7.5 // 2").evaluate(scope) == 3.0
    assert type(Expression("7.5 // 2").evaluate(scope)) is float


@pytest.mark.parametrize(
    "text",
    ["attr.missing > 0 or True", "attr.missing > 0 and False", "not attr.missing or 1"],
)
def test_and_and_or_do_not_absorb_an_error(scope, text):
    """An operand that cannot be evaluated fails the expression even when a later
    operand would decide the result: `and` and `or` do not absorb an error."""
    with pytest.raises(ValueError, match="cannot evaluate '") as error:
        Expression(text).evaluate(scope)
    assert "no attribute 'missing'" in str(error.value)


def test_and_and_or_stop_at_the_first_operand_that_decides(scope):
    """As in Python, an operand after one that decides the result is not evaluated."""
    assert Expression("False and attr.missing > 0").evaluate(scope) is False
    assert Expression("True or attr.missing > 0").evaluate(scope) is True


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
        ("total(shelf)", "a Call other than count(<store>), items(<store>)"),
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
        ('count(shelf, item.a == "x") < "y"', "a string outside"),
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


def _record_scope(record):
    return Scope(
        payload={}, attributes={}, levels={}, registry=None, now=0.0, record=record
    )


def test_record_names_read_the_record_and_none_when_it_lacks_them():
    """In an egress `when`, `record.value.<field>` is None when the value lacks the
    field or is not a mapping, so a comparison with it is false and not an error."""
    expression = Expression("record.value.event_type == 'packed'", records=True)
    assert expression.record_names == {"value.event_type"}
    assert expression.evaluate(_record_scope({"value": {"event_type": "packed"}}))
    assert not expression.evaluate(_record_scope({"value": {"other": 1}}))
    assert not expression.evaluate(_record_scope({"value": 3.5}))
    assert not expression.evaluate(_record_scope({"stream_type": "telemetry"}))
    key = Expression("record.key == 'o-1' and record.task == 'pack'", records=True)
    assert key.evaluate(_record_scope({"key": "o-1", "task": "pack"}))
    assert not key.evaluate(_record_scope({"key": "o-1"}))


@pytest.mark.parametrize(
    "text,fragment",
    [
        ("record.sim_ts > 1", "'record.sim_ts'"),
        ("record > 1", "the name 'record' alone"),
        ("t > 3", "the name 't'"),
        ("attr.total > 1", "attr"),
        ("record.value.a.b == 1", "record"),
    ],
)
def test_record_expressions_refuse_other_names(text, fragment):
    with pytest.raises(ValueError, match=fragment):
        Expression(text, records=True)


def test_record_is_refused_outside_an_egress_when():
    with pytest.raises(ValueError, match="only an egress when reads"):
        Expression("record.key == 'a'")


@pytest.mark.parametrize(
    "text,expected",
    [
        ("2 ** 10", 1024),
        ("2 ** -1", 0.5),
        ("payload.total ** 2", 14400.0),
        ("-2 ** 2", -4),
        ("abs(-3)", 3),
        ("min(attr.priority, 4, 9)", 1),
        ("max(1, 2.5)", 2.5),
        ("round(2.567, 1)", 2.6),
        ("round(2.5)", 2),
        ("floor(2.7) + ceil(2.1)", 5),
        ("sqrt(16)", 4.0),
        ("exp(0)", 1.0),
        ("log(8, 2)", 3.0),
        ("log10(1000)", 3.0),
        ("sin(0) + cos(0) + tan(0)", 1.0),
        ("has(attr.priority)", True),
        ("has(attr.missing)", False),
        ("attr.priority if has(attr.priority) else 0", 1),
    ],
)
def test_powers_math_calls_and_has(scope, text, expected):
    assert Expression(text).evaluate(scope) == pytest.approx(expected)


def test_bare_names_read_the_scope_names(scope):
    scope.names = {"base": 3, "rate": 0.5}
    expression = Expression("base * rate + 1", names=("base", "rate"))
    assert expression.evaluate(scope) == 2.5
    assert expression.references["name"] == {"base", "rate"}
    assert Expression("base", names=ANY_NAME).references["name"] == {"base"}


@pytest.mark.parametrize(
    "text,fragment",
    [
        ("2 ** 10001", "too large"),
        ("9 ** 9 ** 9", "too large"),
        ("(10**5000) ** 5000", "too large"),
        ("(10**200) ** 9000", "too large"),
        ("sqrt()", "sqrt() with other arguments"),
        ("min()", "min() with other arguments"),
        ("round(1, 2, 3)", "round() with other arguments"),
        ("abs(x=1)", "abs() with other arguments"),
        ("has(payload.x)", "has() with anything but one attr.<name>"),
        ("has(attr.a, attr.b)", "has() with anything but one attr.<name>"),
        ("has(attr.__x)", "has() with anything but one attr.<name>"),
    ],
)
def test_powers_calls_and_has_are_refused_or_fail_with_the_expression(
    scope, text, fragment
):
    with pytest.raises(ValueError) as error:
        Expression(text).evaluate(scope)
    assert fragment in str(error.value)


def test_a_math_error_fails_with_the_expression(scope):
    with pytest.raises(ValueError, match="cannot evaluate 'sqrt\\(-1\\)'"):
        Expression("sqrt(-1)").evaluate(scope)


# Batch 6: names, lists, local values.


def _request(entity, rank=0, time=None, since=None, key=None):
    return SimpleNamespace(
        entity_key=entity, priority=rank, time=time, usage_since=since, key=key
    )


def _lists_scope(**extra):
    return Scope(
        resources={
            "m": SimpleNamespace(
                users=[_request("a-1", 1, 0.0, 0.0)],
                queue=[_request("a-2", 2, 1.5), _request("a-3", 3, 2.5)],
            )
        },
        levels={
            "box": SimpleNamespace(
                level=0,
                putters_waiting=1,
                put_queue=[SimpleNamespace(entity_key="p-1", amount=3)],
                get_queue=[SimpleNamespace(entity_key="g-1", amount=2)],
            ),
            "shelf": SimpleNamespace(
                level=0,
                putters_waiting=0,
                items=[],
                put_queue=[SimpleNamespace(entity_key="p-2", item={"id": 4})],
                get_queue=[],
            ),
        },
        **extra,
    )


@pytest.mark.parametrize(
    "text,expected",
    [
        ("process.key", "a-1"),
        ("process.flow_name", "first"),
        ("arg.n + 1", 4),
        ("arg.k", "z"),
        ("cause.by", "high-1"),
        ("cause.left", 7),
        ("upstream.status", "failed"),
        ("item", {"id": 9}),
        ("item_priority", 2),
        ("done_count", 1),
        ("peek()", 6.5),
        ("[h.entity for h in held(m)]", ["a-1"]),
        ("held(m)[0].since", 0.0),
        ("[w.rank for w in waiting(m)]", [2, 3]),
        ("[w.time for w in waiting(m)]", [1.5, 2.5]),
        ("len(waiting(m))", 2),
        ("[p.amount for p in putting(box)]", [3]),
        ("[g.amount for g in getting(box)]", [2]),
        ("putting(shelf)[0].item.id", 4),
        ("running(work)", 2),
        ("running(work, process.instance == 1)", 1),
        ("signal.go.processed", True),
    ],
)
def test_batch_6_names_read_what_the_run_holds(text, expected):
    work = [
        Scope(process={"instance": 1}),
        Scope(process={"instance": 2}),
    ]
    scope = _lists_scope(
        entity="a-1",
        process={"name": "second", "flow_name": "first", "key": "a-1"},
        args={"n": 3, "k": "z"},
        cause={"by": "high-1", "left": 7},
        upstream={"status": "failed"},
        item={"id": 9},
        item_priority=2,
        done_count=1,
        peek=lambda: 6.5,
        instances=lambda task: work,
        signals={"go": SimpleNamespace(count=1, ok=True, value=None, processed=True)},
    )
    assert Expression(text).evaluate(scope) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        ("[1, 2, 3]", [1, 2, 3]),
        ("{'a': 1, 'b': t}", {"a": 1, "b": 30.0}),
        ("[x * 2 for x in [1, 2, 3] if x > 1]", [4, 6]),
        ("[1, 2, 3][1]", 2),
        ("[1, 2, 3][::-1]", [3, 2, 1]),
        ("[1, 2, 3][-1]", 3),
        ("{'a': {'b': 5}}['a']['b']", 5),
        ("sum([1, 2, 3])", 6),
        ("sorted([3, 1, 2])", [1, 2, 3]),
        ("max([4, 9])", 9),
        ("min([4, 9])", 4),
        ("any([False, True])", True),
        ("all([])", True),
        ("sum(range(4))", 6),
        ("int(3.7) + float(2)", 5.0),
        ("str(3) + 'x'", "3x"),
        ("bool([])", False),
        ("2 in [1, 2]", True),
        ("'a' not in {'b': 1}", True),
        ("attr.d is None", True),
        ("attr.priority is not None", True),
        ("get_key({'a': 1}, 'a', 0)", 1),
        ("get_key({'a': 1}, 'z', 0)", 0),
        ("set_key({'a': 1}, 'b', 2)", {"a": 1, "b": 2}),
        ("drop_key({'a': 1, 'b': 2}, 'a')", {"b": 2}),
        ("payload.items_list[0].id", 5),
    ],
)
def test_batch_6_list_and_mapping_forms(text, expected):
    scope = Scope(
        now=30.0,
        attributes={"priority": 1, "d": None},
        payload={"items_list": [{"id": 5}]},
    )
    assert Expression(text).evaluate(scope) == expected


def test_a_set_key_does_not_change_its_argument():
    scope = Scope(attributes={"d": {"a": 1}})
    assert Expression("set_key(attr.d, 'b', 2)").evaluate(scope) == {"a": 1, "b": 2}
    assert scope.attributes["d"] == {"a": 1}


def test_a_comprehension_loop_name_is_local_to_it():
    scope = Scope(names={"x": 7})
    assert Expression("[y for y in [1, 2]]", names={"x"}).evaluate(scope) == [1, 2]
    assert Expression("[x + 1 for y in [1]]", names={"x"}).evaluate(scope) == [8]
    with pytest.raises(ValueError, match="the loop name 'x'"):
        Expression("[x for x in [1, 2]]", names={"x"})


@pytest.mark.parametrize("name", ["sumprod", "prod"])
def test_the_list_function_names_are_reserved_for_a_later_release(name):
    """`sumprod` and `prod` are the list functions planned after 1.0. A var, a loop
    name and a generator name may not take them, so adding the functions refuses no
    1.x blueprint."""
    assert name in RESERVED_NAMES
    with pytest.raises(ValueError, match=f"'{name}' is a name an expression already"):
        check_var_names([name], "vars")
    with pytest.raises(ValueError, match=f"the loop name '{name}'"):
        Expression(f"[{name} for {name} in [1, 2]]")
    with pytest.raises(ValueError, match=f"'{name}' cannot be a name here"):
        Expr(expr="1", names={name: 1})


@pytest.mark.parametrize(
    "text,fragment",
    [
        ("range(2000000)", "range"),
        ("[1, 2][5]", "list index out of range"),
        ("sum(5)", "sum"),
        ("held(nothing)", "no resource is called 'nothing'"),
        ("putting(nothing)", "no store or container is called 'nothing'"),
        ("running(work)", "running() needs a running simulation"),
        ("peek()", "peek() needs a running simulation"),
        ("done_count", "done_count is read in the when of a wait"),
        ("item_priority", "no get from a priority store has given an item"),
    ],
)
def test_batch_6_forms_that_fail_name_the_expression(text, fragment):
    scope = Scope()
    with pytest.raises(Exception) as caught:
        Expression(text).evaluate(scope)
    assert fragment in str(caught.value)
    assert text in str(caught.value) or fragment in str(caught.value)


@pytest.mark.parametrize(
    "text,key,name",
    [
        ("arg.n > 1", "arg", "n"),
        ("cause.by == 'x'", "cause", "by"),
        ("upstream.status", "upstream", "status"),
        ("len(held(m))", "held", "m"),
        ("len(waiting(m))", "waiting", "m"),
        ("len(putting(box))", "putting", "box"),
        ("len(getting(box))", "getting", "box"),
        ("running(work)", "running", "work"),
        ("done_count > 1", "flag", "done_count"),
        ("item_priority", "flag", "item_priority"),
        ("item", "flag", "item"),
    ],
)
def test_batch_6_references_name_what_they_read(text, key, name):
    assert name in Expression(text, names={"x"}).references[key]


def test_numpy_numbers_work_where_a_number_is_read():
    import numpy as np

    scope = Scope(attributes={"n": np.int64(3), "f": np.float64(2.0)})
    assert Expression("sum(range(attr.n))").evaluate(scope) == 3
    assert Expression("[10, 20, 30, 40][attr.n]").evaluate(scope) == 40
    assert Expression("len([0] * attr.n)").evaluate(scope) == 3
    assert Expression("max(attr.n, 2)").evaluate(scope) == 3


@pytest.mark.parametrize("text", ["[0] * 10000000000", "3000000 * [0, 1]"])
def test_a_list_repeated_into_too_many_items_is_refused(text):
    with pytest.raises(Exception, match="more than 1,000,000 items"):
        Expression(text).evaluate(Scope())


@pytest.mark.parametrize(
    "text",
    [
        "('%20000000' + 'd') % 1",
        "attr.k * 5000000",
        "sum([[0] * 2000] * 2000, [])",
        "len([[y for y in range(2000)] for x in range(2000)])",
        "len([y for y in range(900000)] + [y for y in range(900000)])",
        "len([0] * 600000 + [0] * 600000)",
    ],
)
def test_work_that_grows_without_bound_is_refused(text):
    """An expression makes at most 1,000,000 items in one reading, and text
    formatting, a tuple repeated, and a list as the start of `sum` are refused."""
    scope = Scope(attributes={"k": (1, 2, 3)})
    with pytest.raises(ValueError):
        Expression(text).evaluate(scope)


def test_a_memory_error_is_a_named_failure(monkeypatch):
    import ast

    from desflow.core import expressions

    def fail(*_):
        raise MemoryError

    monkeypatch.setitem(expressions._ARITHMETIC, ast.Mod, fail)
    with pytest.raises(ValueError, match="cannot evaluate 't % 2'.*memory"):
        Expression("t % 2").evaluate(Scope())


def test_the_budget_of_one_reading_does_not_carry_to_the_next():
    expression = Expression("len(range(600000))")
    assert expression.evaluate(Scope()) == 600000
    assert expression.evaluate(Scope()) == 600000


def test_items_takes_a_chain_of_any_depth():
    holder = SimpleNamespace(
        level=0,
        putters_waiting=0,
        items=[{"a": {"b": 1}}, {"a": {"b": 2}}],
    )
    scope = Scope(levels={"s": holder})
    assert Expression("items(s, item.a.b)").evaluate(scope) == [1, 2]
    assert Expression("count(s, item.a.b > 1)").evaluate(scope) == 1


def test_a_loop_name_reaches_the_condition_of_running():
    work = [Scope(attributes={"id": 1}), Scope(attributes={"id": 2})]
    scope = Scope(instances=lambda task: work)
    assert Expression(
        "[running(t, attr.id == x) for x in [1, 2, 3]]", names=ANY_NAME
    ).evaluate(scope) == [1, 1, 0]


def test_the_loop_names_are_listed_in_the_references():
    assert Expression("[v for v in [1]]").references["loop"] == {"v"}


# ------------------------------------------------------------- Batch 11: family names


@pytest.mark.parametrize(
    "text,source,name",
    [
        ("users.desk[attr.k]", "users", "desk"),
        ("queue.desk[attr.k + 1]", "queue", "desk"),
        ("level.c[0]", "level", "c"),
        ("putters.c[attr.k]", "putters", "c"),
        ("getters.c[attr.k]", "getters", "c"),
        ("count(q[attr.k])", "count", "q"),
        ("items(q[attr.k])", "items", "q"),
        ("held(desk[attr.k])", "held", "desk"),
        ("waiting(desk[attr.k])", "waiting", "desk"),
        ("signal.reply[attr.k].count", "signal", "reply"),
    ],
)
def test_a_family_member_read_records_the_family_and_the_index(text, source, name):
    """Y1: each index name parses, and the expression records that the family is read by
    an index and not as a plain name."""
    read = Expression(text, names=ANY_NAME)
    assert (source, name) in read.indexed
    assert (source, name) not in read.plain


@pytest.mark.parametrize(
    "text,source,name",
    [
        ("users.desk", "users", "desk"),
        ("queue.desk + 1", "queue", "desk"),
        ("level.c", "level", "c"),
    ],
)
def test_a_family_read_with_no_index_is_recorded_as_plain(text, source, name):
    """Y1: a read of a name with no index is recorded, so the load can refuse it when
    the name is a family."""
    read = Expression(text, names=ANY_NAME)
    assert (source, name) in read.plain
    assert (source, name) not in read.indexed


def test_members_lists_the_keys_in_creation_order_and_reads_nothing_else():
    """Y1: `members(desk)` is the keys of the family made so far, in order."""
    from desflow.core.families import Family

    family = Family("desk", "resource")
    family.keys.extend([2, "a.b", 0])
    scope = Scope(families={"desk": family})
    assert Expression("members(desk)").evaluate(scope) == [2, "a.b", 0]
    assert Expression("len(members(desk))").evaluate(scope) == 3
    assert Expression("members(desk)").references["members"] == {"desk"}


def test_a_member_that_does_not_exist_gives_what_a_new_member_gives():
    """Y1: a read of a key not yet made gives 0 for a count and makes no member."""
    from desflow.core.families import Family

    family = Family("desk", "resource")
    scope = Scope(families={"desk": family})
    assert Expression("users.desk[7]", names=ANY_NAME).evaluate(scope) == 0
    assert Expression("queue.desk['x']", names=ANY_NAME).evaluate(scope) == 0
    assert family.keys == []


@pytest.mark.parametrize("key", [True, False, 3])
def test_a_key_is_an_integer_or_text_and_a_boolean_is_one_or_zero(key):
    """Y1: `true` is the member 1 and `false` the member 0, because a `dict` holds
    `True` and `1` as one key."""
    from desflow.core.families import member_name

    assert member_name("desk", key) == f"desk[{int(key)!r}]"
    assert member_name("desk", "3") == "desk['3']"
    assert member_name("desk", "a.b") == "desk['a.b']"


@pytest.mark.parametrize("key", [1.5, None, (1, 2)])
def test_a_key_that_is_not_an_integer_or_text_is_refused(key):
    from desflow.core.families import member_name

    with pytest.raises(TypeError, match=re.escape("an integer or text")):
        member_name("desk", key)


def test_a_numpy_integer_is_a_key():
    import numpy as np

    from desflow.core.families import member_name

    assert member_name("desk", np.int64(4)) == "desk[4]"


def test_a_registry_path_with_a_bracket_is_one_part():
    """Y1: a dot inside a bracket does not split a registry path."""
    from desflow.core.families import member_path, split_path

    path = "S.resources.desk['a.b'].current_cap"
    assert split_path(path) == ["S", "resources", "desk['a.b']", "current_cap"]
    assert split_path("S.resources.desk[3].current_cap")[2] == "desk[3]"
    assert split_path("S.variables.n") == ["S", "variables", "n"]

    class _Family:
        kind = "resource"

    assert member_path(path, {"desk": _Family()}) == ("desk", "current_cap")
    assert member_path(path, {"other": _Family()}) is None


@pytest.mark.parametrize(
    "text,message",
    [
        ("member.users", "only the lowest of a reference"),
        ("each.index", None),
    ],
)
def test_member_and_each_names_parse_only_where_they_are_read(text, message):
    """Y2 and Y3: `member.*` is read in a `lowest` and `each.*` in a branch of an `each`
    wait, so a plain expression refuses `member.*` and records `each.*`."""
    if message is None:
        assert Expression(text, names=ANY_NAME).references["each"] == {"index"}
        return
    with pytest.raises(ValueError, match=re.escape(message)):
        Expression(text, names=ANY_NAME)


def test_a_lowest_reads_the_member_names():
    read = Expression("member.queue + member.users + member.key", member=True)
    assert read.references["member"] == {"queue", "users", "key"}
    with pytest.raises(ValueError, match=re.escape("member.nothing")):
        Expression("member.nothing", member=True)
