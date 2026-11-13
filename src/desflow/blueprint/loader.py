"""Reads a blueprint file and remembers the line every key came from.

`${VAR}` and `${VAR:-default}` in a string are replaced with the environment
variable, so one file can serve several environments. The default applies when the
variable is unset or empty, as in a shell. `$${` writes a literal `${`. An unquoted
value is then read as YAML reads it, so `port: ${PG_PORT:-5432}` gives the number
5432. A quoted value stays a string. A value given by name to `read_document`, as
`desflow run --var NAME=VALUE` does, is used before the environment.

`!python module.attribute` imports the module and returns the attribute, so a
blueprint can reference a process, a router or any other Python object. Loading a
blueprint therefore runs the imports it names. `!include path` reads a YAML mapping
from another file, relative to the file that names it, such as the settings
`desflow fit` writes; `!include {file: path, spawn: pack}` adds keys to it. Every
other node is constructed by `yaml.SafeLoader`, as `yaml.safe_load` would.
"""

import contextlib
import functools
import importlib
import os
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import yaml

Location = Tuple[Union[str, int], ...]


class BlueprintError(ValueError):
    """A blueprint that cannot be built. The message names the file and line."""


class BlueprintLoader(yaml.SafeLoader):
    """SafeLoader for blueprint files, with `${VAR}` substitution and two extra tags.

    The `!python` and `!include` tags and the substitution are registered on this
    subclass only, so `yaml.safe_load` elsewhere in the process is unaffected.
    """


_VARIABLE = re.compile(r"\$\$\{|\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def substitute(text: str, overrides: Optional[Mapping[str, str]] = None) -> str:
    """Replaces `${VAR}` and `${VAR:-default}` with the variable's value.

    A variable in `overrides` takes its value from there, and any other from the
    environment. `${VAR:-default}` uses the default when the value is unset or empty.
    `$${` is a literal `${`.

    Args:
        text: The text of a blueprint.
        overrides: Values that are used before the environment, by name.

    Returns:
        str: The text with every reference replaced.

    Raises:
        ValueError: If a variable without a default is unset.
    """

    def replace(match: "re.Match[str]") -> str:
        if match.group(0) == "$${":
            return "${"
        name, default = match.group(1), match.group(2)
        if overrides is not None and name in overrides:
            value: Optional[str] = overrides[name]
        else:
            value = os.environ.get(name)
        if value:
            return value
        if default is not None:
            return default
        if value is not None:
            return value
        raise ValueError(
            f"environment variable {name} is not set. Set it, or give a default "
            f"with ${{{name}:-default}}"
        )

    return _VARIABLE.sub(replace, text)


def _substitute_at(loader: yaml.SafeLoader, node: yaml.Node, text: str) -> str:
    """`substitute`, with a failure reported at the line of `node`."""
    try:
        return substitute(text, getattr(loader, "variables", None))
    except ValueError as exc:
        raise BlueprintError(
            f"{loader.name}:{node.start_mark.line + 1}: {exc}"
        ) from None


def _construct_str(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> Any:
    text = loader.construct_scalar(node)
    if "${" not in text:
        return text
    text = _substitute_at(loader, node, text)
    # An unquoted value is typed after substitution, as if the text had been written
    # in the file. A quoted value is a string, as the quotes say.
    if node.style is not None:
        return text
    tag = loader.resolve(yaml.ScalarNode, text, (True, False))
    if tag == "tag:yaml.org,2002:str":
        return text
    return loader.construct_object(
        yaml.ScalarNode(tag, text, node.start_mark, node.end_mark)
    )


# The modules each entry file's folder gave the process, by folder, and the folders
# whose entry files are loading now, the outermost first. `_before_any_entry` holds the
# modules the process had when the first entry file began loading, which no folder owns.
_folder_modules: Dict[Path, Dict[str, ModuleType]] = {}
_loading_folders: List[Path] = []
_before_any_entry: Dict[str, ModuleType] = {}
_INSTALLED_PARTS = ("site-packages", "dist-packages")
_OWN_PACKAGES = ("desflow", "desflow_entry_")


def _owner(name: str, module: ModuleType) -> Optional[Path]:
    """The entry folder whose file `module` is: the most specific folder that holds
    its top-level module or package directly. None for a module of the process before
    the first entry file, a desflow module, an installed package, a module reached
    through another `sys.path` entry that lies under a folder (an editable install,
    the standard library), and a module that is not a file."""
    if _before_any_entry.get(name) is module or name.startswith(_OWN_PACKAGES):
        return None
    origin = getattr(getattr(module, "__spec__", None), "origin", None)
    if not origin or not os.path.isabs(origin):
        return None
    parts = _resolved_parts(origin)
    if any(part in _INSTALLED_PARTS for part in parts):
        return None
    top = name.partition(".")[0]
    # The deepest folder first, each looked up in the entry folders, so the cost
    # does not grow with the number of entry folders a long process has loaded
    for depth in range(len(parts) - 1, 0, -1):
        if parts[depth].partition(".")[0] == top:
            folder = Path(*parts[:depth])
            if folder in _folder_modules:
                return folder
    return None


@functools.lru_cache(maxsize=None)
def _resolved_parts(origin: str) -> Tuple[str, ...]:
    """The parts of a module file's resolved path, worked out once per file."""
    return Path(origin).resolve().parts


def _remember_modules() -> None:
    """Records each module of `sys.modules` under the entry folder that holds its
    file, whenever it was imported, so a module a run imported after its load is kept
    for the next load of its folder."""
    for name, module in list(sys.modules.items()):
        owner = _owner(name, module)
        if owner is not None:
            _folder_modules[owner].setdefault(name, module)


def _holds(folder: Path, top: str) -> bool:
    """Whether `folder` has its own module or package called `top`, directly in it."""
    return (folder / f"{top}.py").is_file() or (folder / top).is_dir()


def _swap_in_modules(folder: Path) -> None:
    """Takes out of `sys.modules` each module of another entry folder whose name
    `folder` also has a file or package for, and puts the modules of `folder` back, so
    `import helpers` finds the `helpers.py` of `folder`. A module whose name `folder`
    does not have stays, as Python keeps it."""
    _remember_modules()
    for name, module in list(sys.modules.items()):
        owner = _owner(name, module)
        if (
            owner is not None
            and owner != folder
            and _holds(folder, name.partition(".")[0])
        ):
            del sys.modules[name]
    for name, module in _folder_modules.get(folder, {}).items():
        current = sys.modules.get(name)
        if current is None or _before_any_entry.get(name) is not current:
            sys.modules[name] = module


@contextlib.contextmanager
def entry_modules(folder: Path) -> Iterator[None]:
    """Makes the Python modules an entry file imports the ones of its own folder.

    Python keeps one module for each name for the whole process. Two files in
    different folders that each import `helpers` would share the first
    `helpers.py`. Inside this block, the modules of the other entry folders are out
    of `sys.modules` when `folder` has a file or a package of the same name, and the
    modules of `folder` that an earlier load or run imported are back in it. A module
    is the folder's when the first part of its name is a file or a package directly
    in the folder, so one that a run imported later, or that a file imported before
    it loaded another file from inside its own import, is swapped out too. A module
    whose name `folder` does not have is never taken out. An installed package, a package reached through
    another `sys.path` entry that lies under the folder (an editable install, the
    standard library), and a module the process had before the first entry file are
    never touched.

    Args:
        folder: The folder of the blueprint or builder file being loaded.

    Yields:
        None: Once the modules are set.
    """
    folder = folder.resolve()
    if not _before_any_entry and not _folder_modules:
        _before_any_entry.update(sys.modules)
    _folder_modules.setdefault(folder, {})
    _loading_folders.append(folder)
    _swap_in_modules(folder)
    try:
        yield
    finally:
        _loading_folders.pop()
        if _loading_folders:
            # A file that loads another file from inside its own import goes on
            # with its own folder's modules
            _swap_in_modules(_loading_folders[-1])
        else:
            _remember_modules()


def resolve(reference: str) -> Any:
    """Imports the longest module prefix of `reference` and returns the rest.

    `pkg.module.func` imports `pkg.module` and returns its `func`. `module.Class.attr`
    falls back to importing `module` and reading `Class.attr`.

    Args:
        reference: A dotted name, such as `shop_logic.pack_payload`.

    Returns:
        Any: The object the name refers to.

    Raises:
        ValueError: If no prefix is a module, or the attribute is missing.
    """
    parts = reference.split(".")
    if len(parts) < 2 or not all(part.isidentifier() for part in parts):
        raise ValueError("expected a dotted path such as module.attribute")

    for end in range(len(parts) - 1, 0, -1):
        module_name = ".".join(parts[:end])
        try:
            target: Any = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            # Only a missing prefix means "try a shorter one". A module that exists
            # but fails on one of its own imports is reported as it is.
            if exc.name and (
                module_name == exc.name or module_name.startswith(exc.name + ".")
            ):
                continue
            raise ValueError(f"importing '{module_name}' failed: {exc}") from None
        except Exception as exc:
            raise ValueError(
                f"importing '{module_name}' failed: {type(exc).__name__}: {exc}"
            ) from None

        for attribute in parts[end:]:
            if not hasattr(target, attribute):
                raise ValueError(
                    f"'{module_name}' has no attribute '{'.'.join(parts[end:])}'"
                )
            target = getattr(target, attribute)
        return target

    raise ValueError(
        f"no module named '{parts[0]}'. Modules are found on sys.path, which "
        f"includes the folder of the YAML file"
    )


def _construct_python(loader: yaml.SafeLoader, node: yaml.Node) -> Any:
    line = node.start_mark.line + 1
    if not isinstance(node, yaml.ScalarNode):
        raise BlueprintError(
            f"{loader.name}:{line}: !python takes a dotted path such as "
            f"module.attribute"
        )
    reference = _substitute_at(loader, node, str(loader.construct_scalar(node)))
    try:
        return resolve(str(reference))
    except ValueError as exc:
        raise BlueprintError(f"{loader.name}:{line}: !python {reference}: {exc}")


def _construct_include(loader: yaml.SafeLoader, node: yaml.Node) -> Any:
    """`!include path`, or `!include {file: path, ...}` with keys added to the
    file's mapping, such as the `spawn` of an arrival whose distribution was fitted."""
    line = node.start_mark.line + 1
    extra: Dict[Any, Any] = {}
    if isinstance(node, yaml.MappingNode):
        extra = dict(loader.construct_mapping(node, deep=True))
        name = extra.pop("file", None)
        if not isinstance(name, str):
            raise BlueprintError(
                f"{loader.name}:{line}: !include takes a file path, or a mapping "
                f"with file and the keys to add"
            )
    elif isinstance(node, yaml.ScalarNode):
        name = _substitute_at(loader, node, str(loader.construct_scalar(node)))
    else:
        raise BlueprintError(
            f"{loader.name}:{line}: !include takes a file path such as "
            f"fitted/packing.yaml"
        )
    path = Path(loader.name).parent / name
    if not path.is_file():
        raise BlueprintError(f"{loader.name}:{line}: !include {name}: no such file")
    including = getattr(loader, "including", ())
    if path.resolve() in including:
        raise BlueprintError(
            f"{loader.name}:{line}: !include {name}: includes itself; include "
            f"another file"
        )
    # Read as a document of its own, so a YAML error in it names its own file and
    # line, and a file that is not a mapping is refused there
    data, _ = read_document(
        path,
        including=including,
        at=getattr(loader, "includes", {}).get(node),
        variables=getattr(loader, "variables", None),
    )
    return {**data, **extra}


def _construct_timestamp(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> Any:
    """A YAML date or datetime, with a value that is no date, such as `2026-13-01`,
    reported at the line of `node`."""
    try:
        return yaml.SafeLoader.construct_yaml_timestamp(loader, node)
    except ValueError as exc:
        raise BlueprintError(
            f"{loader.name}:{node.start_mark.line + 1}: '{node.value}' is not a date "
            f"or time: {exc}"
        ) from None


BlueprintLoader.add_constructor("!python", _construct_python)
BlueprintLoader.add_constructor("!include", _construct_include)
BlueprintLoader.add_constructor("tag:yaml.org,2002:str", _construct_str)
BlueprintLoader.add_constructor("tag:yaml.org,2002:timestamp", _construct_timestamp)


class SourceMap:
    """Maps a location in the document, such as `("tasks", "mill", "service")`, to a line.

    Validation errors and cross-reference errors carry a location rather than a line,
    so this is what turns them into `file:line` messages a reader can act on.
    """

    def __init__(self, path: Path, lines: Dict[Location, int]):
        self.path = path
        self._lines = lines

    def line(self, location: Iterable[Union[str, int]]) -> int:
        """Returns the line of the deepest part of `location` the document has.

        Args:
            location: The keys and list positions from the document's root, such as
                `("tasks", "pack", "service")`.

        Returns:
            int: The line, counting from 1; 1 when no part of the location is found.
        """
        location = tuple(location)
        for end in range(len(location), -1, -1):
            if location[:end] in self._lines:
                return self._lines[location[:end]]
        return 1

    def has(self, location: Iterable[Union[str, int]]) -> bool:
        """Whether the document has a key or list item at exactly `location`.

        Args:
            location: The keys and list indexes from the document's top.
        """
        return tuple(location) in self._lines

    def error(
        self, location: Iterable[Union[str, int]], message: str
    ) -> BlueprintError:
        """Builds a `BlueprintError` pointing at `location`.

        Args:
            location: The keys and list positions from the document's root.
            message: What is wrong, and how to fix it.

        Returns:
            BlueprintError: An error whose text starts with the file and line.
        """
        return BlueprintError(f"{self.path}:{self.line(location)}: {message}")


def _value_read(
    key_node: yaml.ScalarNode,
    name: Any,
    key_of: Optional[Callable[[yaml.Node], Any]],
) -> Any:
    """The value YAML reads from a key, so `yes` and `true` are the same key. A key
    that cannot be read is compared by its text, and the load reports it."""
    if key_of is None:
        return name
    try:
        read = key_of(key_node)
        hash(read)
    except (BlueprintError, yaml.YAMLError, ValueError, TypeError):
        return name
    return read


def _written_twice(
    first: Tuple[str, Any],
    second: Tuple[str, Any],
    first_line: int,
    second_line: int,
) -> str:
    """The message for two keys of one mapping that read as the same value. Each key
    is its text with the value YAML reads it as."""
    (first_text, first_read), (second_text, second_read) = first, second
    where = f"at lines {first_line} and {second_line}; keep one"
    if first_text == second_text:
        return f"the key '{second_text}' is written twice in this mapping, {where}"
    if type(first_read) is type(second_read):
        return (
            f"the keys '{first_text}' and '{second_text}' both read as "
            f"{second_read!r} in this mapping, {where}"
        )
    return (
        f"the keys '{first_text}' and '{second_text}' read as {first_read!r} and "
        f"{second_read!r}, which a mapping holds as one key, {where}"
    )


def _record_lines(
    node: yaml.Node,
    location: Location,
    lines: Dict[Location, int],
    key_of: Optional[Callable[[yaml.Node], Any]] = None,
):
    """Walks the node tree and records the line of every key and list item.

    A key written with `${VAR}` is recorded under the name it has after substitution,
    which `key_of` gives, as the built mapping holds it."""
    lines.setdefault(location, node.start_mark.line + 1)
    if isinstance(node, yaml.MappingNode):
        seen: Dict[Any, Tuple[str, Any, int]] = {}
        for key_node, value_node in node.value:
            name = key_node.value
            if key_of is not None and "${" in str(name):
                try:
                    name = key_of(key_node)
                except BlueprintError:
                    pass  # the load reports it
            if isinstance(key_node, yaml.ScalarNode) and name != "<<":
                # Two keys that read as one value keep only the last value, with no
                # sign that the first was dropped
                read = _value_read(key_node, name, key_of)
                line = key_node.start_mark.line + 1
                if read in seen:
                    first, first_read, first_line = seen[read]
                    raise yaml.constructor.ConstructorError(
                        None,
                        None,
                        _written_twice(
                            (first, first_read),
                            (str(name), read),
                            first_line,
                            line,
                        ),
                        key_node.start_mark,
                    )
                seen[read] = (str(name), read, line)
            child = location + (name,)
            # A key's own line is where a reader looks, even when its value is a
            # block starting on the next line.
            lines[child] = key_node.start_mark.line + 1
            _record_lines(value_node, child, lines, key_of)
    elif isinstance(node, yaml.SequenceNode):
        for index, item in enumerate(node.value):
            _record_lines(item, location + (index,), lines, key_of)


_BOOL_TAG = "tag:yaml.org,2002:bool"
_STR_TAG = "tag:yaml.org,2002:str"


def _keep_on_keys(
    root: yaml.Node, includes: Dict[yaml.Node, str], at: Optional[str] = None
) -> None:
    """Keeps the key `on` a string in task, signal and experiment check mappings, and in
    the `listen` mapping of a task.

    YAML 1.1 reads the bare word `on` as the boolean True, so a task's or a signal's
    `on:` actions and an experiment check's `on` were refused with "Keys should be
    strings". A key spelled `On` or `ON` is renamed `on`. Only those mappings
    change; every other mapping, such as a payload or attributes, keeps today's
    reading.

    An `!include` node that stands for a task, a signal or a check, or for the
    `tasks` or `signals` section, is recorded in `includes`, so the file it reads is
    treated the same.

    Args:
        root: The document's root node, before it is constructed.
        includes: Filled with each such `!include` node: `item` for one task, signal
            or check, `section` for a section of them.
        at: What this document is when an `!include` read it: `item` for one task,
            signal or check, `section` for a section of them, None for a whole
            document.
    """

    def retag(node: yaml.Node) -> None:
        if isinstance(node, yaml.MappingNode):
            for key, value in node.value:
                if (
                    isinstance(key, yaml.ScalarNode)
                    and key.tag == _BOOL_TAG
                    and key.value.lower() == "on"
                ):
                    key.tag = _STR_TAG
                    key.value = "on"
                if (
                    isinstance(key, yaml.ScalarNode)
                    and key.value == "listen"
                    and isinstance(value, yaml.MappingNode)
                ):
                    # The `on` of a task's `listen` is an `on:` list as well
                    retag(value)

    def item(node: yaml.Node) -> None:
        retag(node)
        if node.tag == "!include":
            includes[node] = "item"

    def section(node: yaml.Node) -> None:
        if node.tag == "!include":
            includes[node] = "section"
        elif isinstance(node, yaml.MappingNode):
            for _, member in node.value:
                item(member)

    if at == "item":
        item(root)
        return
    if at == "section":
        section(root)
        return
    if not isinstance(root, yaml.MappingNode):
        return
    for key, value in root.value:
        name = key.value if isinstance(key, yaml.ScalarNode) else None
        if name in ("tasks", "signals"):
            section(value)
        if name in ("tasks", "signals", "checks") and isinstance(
            value, yaml.SequenceNode
        ):
            for member in value.value:
                item(member)


# The words YAML 1.1 reads as booleans that a case or an option key takes as text
_BOOLEAN_WORDS = ("yes", "no", "on", "off")


def _keep_generator_keys(node: yaml.Node) -> None:
    """Keeps the keys a generator reads as text from becoming booleans.

    YAML 1.1 reads the words `yes`, `no`, `on` and `off` as booleans, so the `on` of a
    `{gen: switch, on: ...}` mapping would be refused as a key that is not a string, a
    case `yes:` would never match the text `yes`, and an option `on:` of `weighted`
    would be written as `True`. In a mapping whose `gen` is `switch`, the key `on` is
    the string `on` and every key of `cases` that is one of those four words is a
    string written as it is in the file. In a mapping whose `gen` is `weighted`, the same holds
    for the keys of `options`. A key `true` or `false`, and every other mapping, keep
    today's reading.
    """
    if isinstance(node, yaml.SequenceNode):
        for member in node.value:
            _keep_generator_keys(member)
    elif isinstance(node, yaml.MappingNode):
        kind = next(
            (
                value.value
                for key, value in node.value
                if isinstance(key, yaml.ScalarNode)
                and key.value == "gen"
                and isinstance(value, yaml.ScalarNode)
            ),
            None,
        )
        keyed = {"switch": "cases", "weighted": "options"}.get(kind or "")
        for key, value in node.value:
            if not isinstance(key, yaml.ScalarNode):
                continue
            if kind == "switch" and key.tag == _BOOL_TAG and key.value.lower() == "on":
                key.tag = _STR_TAG
                key.value = "on"
            if key.value == keyed and isinstance(value, yaml.MappingNode):
                for case, _ in value.value:
                    if (
                        isinstance(case, yaml.ScalarNode)
                        and case.tag == _BOOL_TAG
                        and case.value.lower() in _BOOLEAN_WORDS
                    ):
                        case.tag = _STR_TAG
            _keep_generator_keys(value)


def read_document(
    path: Union[str, Path],
    including: Tuple[Path, ...] = (),
    at: Optional[str] = None,
    variables: Optional[Mapping[str, str]] = None,
) -> Tuple[Dict[str, Any], SourceMap]:
    """Parses a blueprint file into a mapping and its source map.

    The folder of the file is moved to the front of `sys.path` before `!python`
    references are resolved, and stays there so later imports find the same modules.
    A file that no `!include` led to resolves them against its own folder's Python
    modules, not those of a file loaded earlier from another folder
    (`entry_modules`).

    Args:
        path: The file.
        including: The files whose `!include` led here, so a file that includes
            itself, directly or through another, is refused.
        at: What the file is when an `!include` read it: `item` for one task, signal
            or check, `section` for the `tasks` or `signals` section, so its `on`
            keys stay strings; None for a whole document.
        variables: Values for `${NAME}` that are used before the environment, as
            `desflow run --var NAME=VALUE` gives them. Files that `!include` reads
            see them too.

    Raises:
        BlueprintError: If the file is not valid YAML, is not a mapping, or names a
            Python object that cannot be imported.
    """
    path = Path(path)
    # `!python` references resolve against the folder of the file first, so a
    # blueprint and the module beside it work from any working directory.
    folder = str(path.resolve().parent)
    if including:
        if folder not in sys.path:
            sys.path.insert(0, folder)
        return _parse_document(path, including, at, variables)
    if folder in sys.path:
        sys.path.remove(folder)
    sys.path.insert(0, folder)
    with entry_modules(Path(folder)):
        return _parse_document(path, including, at, variables)


def _parse_document(
    path: Path,
    including: Tuple[Path, ...],
    at: Optional[str],
    variables: Optional[Mapping[str, str]],
) -> Tuple[Dict[str, Any], SourceMap]:
    """Parses `path` once its folder is on `sys.path`. `read_document` explains the
    arguments."""
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        line = exc.object[: exc.start].count(b"\n") + 1
        raise BlueprintError(
            f"{path}:{line}: the file is not UTF-8 text ({exc.reason} at byte "
            f"{exc.start}); save it as UTF-8"
        ) from None
    loader = BlueprintLoader(text)
    loader.name = str(path)
    # The files being read, so an !include of one of them is refused, not followed
    loader.including = (*including, path.resolve())  # type: ignore[attr-defined]
    loader.variables = variables  # type: ignore[attr-defined]
    lines: Dict[Location, int] = {}
    try:
        node = loader.get_single_node()
        if node is None:
            raise BlueprintError(f"{path}:1: the file is empty")
        includes: Dict[yaml.Node, str] = {}
        _keep_on_keys(node, includes, at)
        _keep_generator_keys(node)
        loader.includes = includes  # type: ignore[attr-defined]
        _record_lines(node, (), lines, loader.construct_object)
        data = loader.construct_document(node)
    except yaml.MarkedYAMLError as exc:
        mark = exc.problem_mark or exc.context_mark
        line = mark.line + 1 if mark else 1
        raise BlueprintError(f"{path}:{line}: {exc.problem or exc}") from None
    finally:
        loader.dispose()

    source = SourceMap(path, lines)
    if not isinstance(data, dict):
        raise source.error(
            (),
            "an included file must hold a mapping"
            if including
            else "the top level must be a mapping of sections",
        )
    return data, source
