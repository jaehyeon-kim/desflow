"""Write the public API snapshot that tests/unit/test_public_api.py compares with.

The public API is every name a user can reach from the docs or the package: the names
in `desflow.__all__`, every `:::` path on the API page (`docs/api.md`), and every name
a script under `examples/` or `docs/snippets/` imports from desflow. For each one the
snapshot holds its signature, and for a class the signature of each public member that
a desflow class defines. A name that is another path to the same object is one line
that names the object's own path. The `desflow` command is listed after the objects:
each command with its exit codes, and each argument and option.

The snapshot is the same on every Python the CI matrix runs, because annotations and
defaults are written by this file, not by `repr`, which changes between versions.

Usage: uv run --all-extras python scripts/public_api.py
"""

import ast
import importlib
import inspect
import re
import sys
import textwrap
import types
import typing
from pathlib import Path
from typing import Any, Dict, Iterator, List

ROOT = Path(__file__).resolve().parents[1]
API_PAGE = ROOT / "docs" / "api.md"
SNAPSHOT = ROOT / "tests" / "unit" / "public_api.txt"
HEADER = "# Written by scripts/public_api.py. Do not edit by hand."
CLI_HEADER = "[cli]"

_ADDRESS = re.compile(r" at 0x[0-9a-fA-F]+")
_PATH = re.compile(r"[A-Za-z_][\w.]*")
_SHORT_MODULES = ("builtins", "typing")


def scripts() -> Iterator[Path]:
    """Every Python file under examples/ and docs/snippets/."""
    for folder in ("examples", "docs/snippets"):
        yield from sorted(
            p for p in (ROOT / folder).rglob("*.py") if "__pycache__" not in p.parts
        )


def _imported_names(path: Path) -> Iterator[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and (node.module == "desflow" or node.module.startswith("desflow."))
        ):
            for alias in node.names:
                yield f"{node.module}.{alias.name}"


def names() -> List[str]:
    """Every public path, from `__all__`, the API page and the scripts."""
    import desflow

    found = {f"desflow.{name}" for name in desflow.__all__}
    for line in API_PAGE.read_text(encoding="utf-8").splitlines():
        if line.startswith("::: "):
            found.add(line[4:].strip())
    for path in scripts():
        found.update(_imported_names(path))
    return sorted(found)


def resolve(path: str) -> Any:
    """The object a dotted path names: the longest importable module, then attributes."""
    parts = path.split(".")
    for cut in range(len(parts), 0, -1):
        try:
            obj = importlib.import_module(".".join(parts[:cut]))
        except ImportError:
            continue
        for part in parts[cut:]:
            obj = getattr(obj, part)
        return obj
    raise ImportError(path)


def _public_module(cls: type) -> str:
    """The module a class is written under: its own, unless a segment of that path is
    private (`pathlib._local` on Python 3.13), then the nearest parent that exports the
    same object under its name (`pathlib`)."""
    parts = cls.__module__.split(".")
    if not any(part.startswith("_") for part in parts):
        return cls.__module__
    for cut in range(len(parts) - 1, 0, -1):
        name = ".".join(parts[:cut])
        if any(part.startswith("_") for part in parts[:cut]):
            continue
        try:
            parent = importlib.import_module(name)
        except ImportError:
            continue
        if getattr(parent, cls.__qualname__, None) is cls:
            return name
    return cls.__module__


def _annotation(value: Any) -> str:
    if value is inspect.Parameter.empty:
        return ""
    if value is None or value is type(None):
        return "None"
    if value is Ellipsis:
        return "..."
    if isinstance(value, str):
        return value
    if isinstance(value, typing.ForwardRef):
        return value.__forward_arg__
    if isinstance(value, typing.TypeVar):
        return value.__name__
    if isinstance(value, list):
        return "[" + ", ".join(_annotation(v) for v in value) + "]"
    origin = typing.get_origin(value)
    args = typing.get_args(value)
    if origin is typing.Union or origin is types.UnionType:
        return " | ".join(_annotation(a) for a in args)
    if origin is typing.Literal:
        return "Literal[" + ", ".join(repr(a) for a in args) + "]"
    if origin is typing.Annotated:
        # Python 3.14 writes `typing.Annotated` for the origin, 3.11 writes `Annotated`.
        return "Annotated[" + ", ".join(_annotation(a) for a in args) + "]"
    if origin is not None:
        return f"{_annotation(origin)}[{', '.join(_annotation(a) for a in args)}]"
    if value is typing.Any:
        return "Any"
    if isinstance(value, type):
        if value.__module__ in _SHORT_MODULES:
            return value.__qualname__
        return f"{_public_module(value)}.{value.__qualname__}"
    return _ADDRESS.sub("", repr(value))


def _default(value: Any) -> str:
    return _ADDRESS.sub("", repr(value))


def _parameter(param: inspect.Parameter) -> str:
    text = param.name
    if param.kind is inspect.Parameter.VAR_POSITIONAL:
        text = "*" + text
    elif param.kind is inspect.Parameter.VAR_KEYWORD:
        text = "**" + text
    annotation = _annotation(param.annotation)
    if annotation:
        text += f": {annotation}"
    if param.default is not inspect.Parameter.empty:
        text += (
            f" = {_default(param.default)}"
            if annotation
            else f"={_default(param.default)}"
        )
    return text


def signature(obj: Any, drop_first: bool = False) -> str:
    """`(params) -> return`, with `/` and `*` markers placed as `inspect` places them."""
    try:
        sig = inspect.signature(obj)
    except (TypeError, ValueError):
        return "(...)"
    params = list(sig.parameters.values())
    if drop_first and params:
        params = params[1:]
    parts: List[str] = []
    positional_only = False
    keyword_marker = True
    for param in params:
        kind = param.kind
        if kind is inspect.Parameter.POSITIONAL_ONLY:
            positional_only = True
        elif positional_only:
            parts.append("/")
            positional_only = False
        if kind is inspect.Parameter.VAR_POSITIONAL:
            keyword_marker = False
        elif kind is inspect.Parameter.KEYWORD_ONLY and keyword_marker:
            parts.append("*")
            keyword_marker = False
        parts.append(_parameter(param))
    if positional_only:
        parts.append("/")
    text = f"({', '.join(parts)})"
    returns = _annotation(sig.return_annotation)
    return f"{text} -> {returns}" if returns else text


def _own_path(obj: Any) -> str:
    module = getattr(obj, "__module__", None)
    qualname = getattr(obj, "__qualname__", None)
    if module and qualname and (inspect.isclass(obj) or inspect.isroutine(obj)):
        return f"{module}.{qualname}"
    return ""


def _members(cls: type, path: str) -> Iterator[str]:
    seen = set()
    for klass in cls.__mro__:
        if not klass.__module__.startswith("desflow"):
            continue
        for name, value in vars(klass).items():
            if name.startswith("_") or name in seen:
                continue
            seen.add(name)
            where = f"{path}.{name}"
            if isinstance(value, property):
                yield f"{where}: property"
            elif isinstance(value, staticmethod):
                yield where + signature(value.__func__)
            elif isinstance(value, classmethod):
                yield where + signature(value.__func__, drop_first=True)
            elif inspect.isfunction(value):
                yield where + signature(value, drop_first=True)
            elif inspect.isclass(value):
                yield f"{where}: class"
            else:
                yield f"{where}: {type(value).__qualname__}"


def object_lines(paths: List[str]) -> List[str]:
    """One line per object and per public member, sorted."""
    lines = set()
    for path in paths:
        obj = resolve(path)
        own = _own_path(obj)
        if own and own != path:
            lines.add(f"{path} = {own}")
            path = own
        if inspect.isclass(obj):
            lines.add(path + signature(obj))
            lines.update(_members(obj, path))
        elif inspect.isroutine(obj):
            lines.add(path + signature(obj))
        elif inspect.ismodule(obj):
            lines.add(f"{path}: module")
        else:
            lines.add(f"{path}: {type(obj).__qualname__}")
    return sorted(lines)


def _exit_codes(function: Any) -> List[int]:
    """0, and every code the command raises: `typer.Exit` or a usage error.

    The raise statements are read from the command's function and, once each, from
    every function of the same module that it calls, directly or through another.
    """
    codes = {0}
    pending = [function]
    seen = set()
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        tree = ast.parse(textwrap.dedent(inspect.getsource(current)))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                called = current.__globals__.get(node.func.id)
                if (
                    inspect.isfunction(called)
                    and called.__module__ == function.__module__
                ):
                    pending.append(called)
            if not (isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call)):
                continue
            name = ast.unparse(node.exc.func)
            if name == "typer.BadParameter":
                codes.add(2)
            elif name == "typer.Exit":
                given = node.exc.args[:1] + [
                    k.value for k in node.exc.keywords if k.arg == "code"
                ]
                codes.add(given[0].value if given else 0)  # type: ignore[attr-defined]
    return sorted(codes)


def _callbacks(app: Any, prefix: str) -> Dict[str, Any]:
    found = {}
    if app.registered_callback is not None:
        found[prefix] = app.registered_callback.callback
    for command in app.registered_commands:
        found[f"{prefix} {command.name}"] = command.callback
    for group in app.registered_groups:
        found.update(_callbacks(group.typer_instance, f"{prefix} {group.name}"))
    return found


def _params(command: Any, path: str) -> Iterator[str]:
    for param in command.params:
        if param.param_type_name == "argument":
            name = param.human_readable_name
            kind = "argument"
        else:
            name = "/".join(param.opts + param.secondary_opts)
            kind = "option"
        required = "required" if param.required else "optional"
        yield (
            f"{path} {name} :: {kind}, {param.type.name}, "
            f"default {_default(param.default)}, {required}"
        )


def cli_lines() -> List[str]:
    """Each command path with its exit codes, then each of its arguments and options."""
    from typer.main import get_command

    from desflow.blueprint.cli import app

    callbacks = _callbacks(app, "desflow")
    lines = []

    def walk(command: Any, path: str) -> None:
        function = callbacks.get(path)
        codes = _exit_codes(function) if function is not None else [0]
        lines.append(f"{path} :: exit codes {', '.join(map(str, codes))}")
        lines.extend(_params(command, path))
        for name, child in sorted(getattr(command, "commands", {}).items()):
            walk(child, f"{path} {name}")

    walk(get_command(app), "desflow")
    return lines


def snapshot() -> str:
    lines = [HEADER, *object_lines(names()), CLI_HEADER, *cli_lines()]
    return "\n".join(lines) + "\n"


def key(line: str) -> str:
    """What a line describes: the text before ` :: ` for a command, else the path."""
    if " :: " in line:
        return line.split(" :: ")[0]
    match = _PATH.match(line)
    return match.group(0) if match else line


def main() -> None:
    SNAPSHOT.write_text(snapshot(), encoding="utf-8")
    print(f"Wrote {SNAPSHOT.relative_to(ROOT)}", file=sys.stderr)


if __name__ == "__main__":
    main()
