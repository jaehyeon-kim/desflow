"""A numeric argument of the Kafka admin connector and the registry clients is checked.

Each argument is given 0, -1, 2.5, NaN and `True`, and must be refused with its name
where the value is not a whole number in the argument's range, and accepted where it is.
"""

import pytest

from desflow.connectors._schema_registry.confluent_client import (
    ConfluentRegistryClient,
)
from desflow.connectors.admin.kafka import KafkaAdminConnector

NAN = float("nan")


def _admin(value):
    return KafkaAdminConnector("x", max_tasks=value)


def _confluent(value):
    return ConfluentRegistryClient("http://registry", max_retries=value)


# The builder of each connector, the argument's name, and the values it refuses
ARGUMENTS = [
    (_admin, "max_tasks", [0, -1, 2.5, NAN, True]),
    (_confluent, "max_retries", [-1, 2.5, NAN, True]),
]


@pytest.mark.parametrize(
    "build,name,value",
    [(b, n, v) for b, n, values in ARGUMENTS for v in values],
    ids=lambda x: x.__name__ if callable(x) else str(x),
)
def test_a_bad_numeric_argument_is_refused_by_name(build, name, value):
    with pytest.raises(ValueError, match=name):
        build(value)


@pytest.mark.parametrize(
    "build,value",
    [(_admin, 1), (_confluent, 0), (_confluent, 3)],
    ids=["admin-1", "confluent-0", "confluent-3"],
)
def test_a_good_numeric_argument_is_accepted(build, value):
    build(value)
