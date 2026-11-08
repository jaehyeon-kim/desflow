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
