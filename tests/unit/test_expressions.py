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
        ('payload.channel in "web"', "In"),
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
    ],
)
def test_forms_outside_the_whitelist_are_refused(text, fragment):
    with pytest.raises(ValueError, match="an expression cannot|not a valid") as error:
        Expression(text)
    assert fragment in str(error.value)


def test_references_name_each_source():
    expression = Expression(
        "payload.total > 1 and attr.vip == 1 and level.stock > 0 "
        'and registry["Shop.arrival.orders.rate"] > 0'
    )
    assert expression.references == {
        "payload": {"total"},
        "attr": {"vip"},
        "level": {"stock"},
        "registry": {"Shop.arrival.orders.rate"},
    }


@pytest.mark.parametrize(
    "text,fragment",
    [
        ("payload.missing > 1", "no field 'missing'"),
        ("attr.missing > 1", "no attribute 'missing'"),
        ("level.shelf > 1", "no store or container is called 'shelf'"),
        ("payload.total / (attr.priority - 1)", "division by zero"),
    ],
)
def test_a_missing_name_fails_with_the_expression(scope, text, fragment):
    with pytest.raises(ValueError, match="cannot evaluate '") as error:
        Expression(text).evaluate(scope)
    assert fragment in str(error.value)
