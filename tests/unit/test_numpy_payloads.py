"""A payload that holds a NumPy value is written as its Python value."""

import numpy as np
import pytest

from desflow.core.environment import _jsonable


@pytest.mark.parametrize(
    "value, expected",
    [
        (np.float64(1.5), 1.5),
        (np.int64(3), 3),
        (np.float32(1.5), 1.5),
        (np.bool_(True), True),
        (np.array([1, 2]), [1, 2]),
    ],
    ids=["float64", "int64", "float32", "bool", "array"],
)
def test_a_numpy_value_is_written_as_its_python_value(value, expected):
    assert _jsonable({"x": value}) == {"x": expected}


def test_a_numpy_nan_is_written_as_null():
    assert _jsonable({"x": np.float32("nan")}) == {"x": None}


@pytest.mark.parametrize("text", ["NaN", "Infinity", "-Infinity", "sNaN"])
def test_a_decimal_nan_or_infinity_is_written_as_null(text):
    """`to_jsonable_python` writes a Decimal as text, so the text "NaN" would reach the
    record; a float NaN is already null."""
    from decimal import Decimal

    assert _jsonable({"x": Decimal(text)}) == {"x": None}
    assert _jsonable({"x": [Decimal(text), 1]}) == {"x": [None, 1]}
    if text != "sNaN":  # a signaling NaN cannot be in a set
        assert _jsonable({"x": {Decimal(text)}}) == {"x": [None]}
    assert _jsonable({"x": Decimal("1.5")}) == {"x": "1.5"}
