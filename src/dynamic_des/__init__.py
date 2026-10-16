"""The import name before the rename: `dynamic_des` loads `desflow`.

The package dynamic-des was renamed desflow in 0.20.0. This module keeps code written
for it running: `import dynamic_des` warns once and gives the names of `desflow`, and
`import dynamic_des.<module>` gives the module `desflow.<module>` itself, so a class
imported under either name is the same class. It is removed in a later release, as
the Changelog says.
"""

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys
import warnings
from types import ModuleType
from typing import Any, Optional, Sequence

# FutureWarning, not DeprecationWarning: Python hides a DeprecationWarning unless it
# is raised in __main__, so code that imports this module from its own package would
# never see it. A FutureWarning is shown by default wherever the import is.
warnings.warn(
    "dynamic_des is renamed desflow and will be removed in a later release. "
    "Import desflow instead.",
    FutureWarning,
    stacklevel=2,
)

import desflow  # noqa: E402
from desflow import *  # noqa: E402, F403
from desflow import __all__, __version__  # noqa: E402, F401

_OLD = __name__
_NEW = desflow.__name__


class _AliasLoader(importlib.abc.Loader):
    """Load `dynamic_des.<module>` as the module `desflow.<module>` itself."""

    def __init__(self, target: str) -> None:
        self._target = target
        self._spec: Optional[importlib.machinery.ModuleSpec] = None

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> ModuleType:
        """Import the desflow module and return it in place of a new module."""
        module = importlib.import_module(self._target)
        self._spec = module.__spec__
        return module

    def exec_module(self, module: ModuleType) -> None:
        """Give the desflow module back its own spec.

        The import system sets `__spec__` to the alias spec after `create_module`,
        and the module must keep the spec of its real name.
        """
        module.__spec__ = self._spec


class _AliasFinder(importlib.abc.MetaPathFinder):
    """Find `dynamic_des.<module>` imports and send them to `desflow.<module>`."""

    # Marks the finder across reloads, where the class is a new object.
    _dynamic_des_alias = True

    def find_spec(
        self,
        fullname: str,
        path: Optional[Sequence[str]] = None,
        target: Optional[ModuleType] = None,
    ) -> Optional[importlib.machinery.ModuleSpec]:
        """Return an alias spec for a submodule of `dynamic_des`, otherwise None."""
        if not fullname.startswith(_OLD + "."):
            return None
        new_name = _NEW + fullname[len(_OLD) :]
        if importlib.util.find_spec(new_name) is None:
            return None
        return importlib.machinery.ModuleSpec(fullname, _AliasLoader(new_name))


if not any(getattr(finder, "_dynamic_des_alias", False) for finder in sys.meta_path):
    sys.meta_path.insert(0, _AliasFinder())


def __getattr__(name: str) -> Any:
    """Give any other attribute of `desflow`, such as a submodule already imported."""
    return getattr(desflow, name)
