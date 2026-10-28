"""The default of an argument that the caller did not give."""

from typing import Any


class _Unset:
    """The type of `UNSET`. Its one instance is never equal to a value a caller gives."""

    def __repr__(self) -> str:
        return "<not given>"


# Typed as Any so that it fits the type of each argument it is the default of.
UNSET: Any = _Unset()
