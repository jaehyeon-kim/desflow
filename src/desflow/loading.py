"""Loads a simulation from a folder and the name of its entry file.

`desflow run` loads every blueprint through `load_context`, and a builder Python
file loads the same way, so a command that takes either kind of file has one path.
"""

import atexit
import functools
import importlib.machinery
import importlib.util
import itertools
import shutil
import sys
import tempfile
from pathlib import Path
from typing import List, Mapping, Optional, Union

from desflow.blueprint.build import Preview, preview_builder
from desflow.blueprint.loader import entry_modules
from desflow.builder.context import SimulationContext, parts_of
from desflow.utils import is_builder_file

# Each Python entry is imported under a new name, so two loads never share a module
_entry_numbers = itertools.count()


def load_context(
    folder: Union[str, Path],
    entry: str,
    preview: Optional[Preview] = None,
    variables: Optional[Mapping[str, str]] = None,
) -> SimulationContext:
    """Builds the simulation that `entry` in `folder` defines.

    An entry ending in `.py` is a builder file: it is imported under a new module
    name each call, with `folder` first on `sys.path` during the import, so its
    imports resolve from its folder, not from a file of another folder loaded
    earlier, and its `if __name__ == "__main__":` block does not run. Its
    `DESFLOW_APP` names the context to use; without it, the file must define exactly
    one `SimulationContext` at module level. Any other entry is a YAML blueprint,
    built by `SimulationContext.from_yaml`.

    Args:
        folder: A local folder, or a URI that `pyarrow.fs.FileSystem.from_uri`
            reads, such as `s3://bucket/sims/shop` or `file:///srv/shop`. A URI
            folder is copied to a temporary local folder first, which is removed
            when the process ends, because `!python` references and Python imports
            resolve from the entry's folder.
        entry: The entry file's name inside `folder`.
        preview: The preview `desflow run --sample` and `--stdout` make. A
            builder file is imported first, and its sinks are then replaced as a
            blueprint's are (`preview_builder`).
        variables: Values for `${NAME}`, used before the environment, as `desflow
            run --var NAME=VALUE` gives them, for a blueprint only.

    Returns:
        SimulationContext: The built context, not yet run.

    Raises:
        NoContextError: If a Python entry defines no context. It is a
            `ValueError`.
        EntryNotFoundError: If `folder` is a URI and `entry` is not a file in it. It
            is a `ValueError`.
        ValueError: If a Python entry defines several contexts with no `DESFLOW_APP`,
            or `DESFLOW_APP` names no context; or variables are given with a Python
            entry.
        ImportError: If `folder` is a URI and pyarrow is not installed.
        BlueprintError: If a blueprint is invalid.
    """
    local = _local_folder(folder)
    path = local / entry
    if isinstance(folder, str) and "://" in folder and not path.is_file():
        raise EntryNotFoundError(f"no file {folder}/{entry}")
    if not is_builder_file(path):
        return SimulationContext.from_yaml(path, preview, variables)
    if variables:
        raise ValueError(
            f"variables fill ${{NAME}} in a YAML blueprint, and {entry} is a Python "
            f"file, which reads its own settings"
        )
    context = _python_context(path)
    if preview is not None:
        preview_builder(context, preview)
    # analyze(verify=True) builds a new copy from the local file
    parts_of(context).defaults.source = functools.partial(load_context, local, entry)
    return context


def _local_folder(folder: Union[str, Path]) -> Path:
    """The folder itself, or a local copy of a URI folder."""
    if not (isinstance(folder, str) and "://" in folder):
        return Path(folder)
    try:
        from pyarrow import fs
    except ImportError:
        raise ImportError(
            f"loading from {folder} needs pyarrow; install it with "
            'pip install "desflow[parquet]"'
        ) from None
    source, root = fs.FileSystem.from_uri(folder)
    local = Path(tempfile.mkdtemp(prefix="desflow-"))
    atexit.register(shutil.rmtree, local, ignore_errors=True)
    fs.copy_files(
        root,
        str(local),
        source_filesystem=source,
        destination_filesystem=fs.LocalFileSystem(),
    )
    return local


class EntryNotFoundError(ValueError):
    """The entry of a URI folder is not a file in it."""


class NoContextError(ValueError):
    """A Python file that defines no module-level `SimulationContext`, such as a
    script that builds `DynamicRealtimeEnvironment` itself."""


def _python_context(path: Path) -> SimulationContext:
    """Imports a builder file and returns the context it defines."""
    name = f"desflow_entry_{next(_entry_numbers)}"
    # The loader is given, because importlib knows only `.py` in lower case
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_file_location(name, path, loader=loader)
    if spec is None or spec.loader is None:
        raise ValueError(f"{path} cannot be imported as a Python file")
    module = importlib.util.module_from_spec(spec)
    # Registered before it runs, as an import does, so its classes can find it
    sys.modules[name] = module
    folder = str(path.parent)
    sys.path.insert(0, folder)
    try:
        with entry_modules(path.parent):
            spec.loader.exec_module(module)
    finally:
        sys.path.remove(folder)

    chosen = getattr(module, "DESFLOW_APP", None)
    if chosen is not None:
        context = getattr(module, chosen, None)
        if not isinstance(context, SimulationContext):
            raise ValueError(
                f"{path} sets DESFLOW_APP = {chosen!r}, which is not a "
                "SimulationContext defined in the file"
            )
        return context
    found: List[str] = []
    contexts: List[SimulationContext] = []
    for attribute, value in vars(module).items():
        if isinstance(value, SimulationContext):
            found.append(attribute)
            if all(value is not seen for seen in contexts):
                contexts.append(value)
    if len(contexts) == 1:
        return contexts[0]
    if not contexts:
        raise NoContextError(
            f"{path} defines no SimulationContext at module level. A builder file "
            "defines one, such as app = SimulationContext(...); a script that "
            "builds DynamicRealtimeEnvironment directly cannot be loaded"
        )
    raise ValueError(
        f"{path} defines several SimulationContext objects ({', '.join(found)}); "
        'name the one to use with DESFLOW_APP = "<name>"'
    )
