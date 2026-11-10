"""The `desflow` command line is frozen: 1.x may only widen it.

1.x may only widen the command line: the declared attributes of every 1.0 command and
parameter are kept as a file, every difference from them must be a listed widening, and
every 1.0 invocation parses to the same values. The Command Line pages document this
surface, and the Compatibility Policy states the promise.

`frozen/cli.1.0.json` holds two things, both made once from the app after the removals
and never regenerated, because the next release is compared with 1.0.

"description" holds the declared attributes of every command, group, parameter and
parameter type: the arguments that Click documents for their constructors, as the
literal tuples below list them, read from the Click API reference for the Click that
Typer 0.27.3 carries (8.3.1) and from that copy's source where the reference is silent.
Each is recorded as Click stores it, with the module and qualified name of its class.
Nothing outside the tuples is recorded, so a Typer or Click release that changes only
internals changes nothing here. `_compare` walks the 1.0 description and the current
one side by side. A difference passes only when it is one of the widenings in
`_widening` and `_compare_parameters`: a new command, a new option that is not
required and has no callback, prompt, environment variable or computed default, a new
argument that is not required, on a command with no subcommands, after every argument
of 1.0 and after no argument that takes any number of values, a new name for an option,
`required` made false, a range limit that widens, a path check that loosens, a new
choice, and a change of an attribute that only shows help. Every other difference fails
with the path and both values.

When Click or Typer renames or moves something that the tuples name, the comparison
fails with the path and says what to do: check the release notes of Click and Typer,
confirm that every command line 1.0 accepts is still accepted, and record the change in
`TYPER_CHANGES` with the release and the reason. The file is never regenerated. A
desflow callback that is renamed is recorded there too.

"commands" holds, for each command and group, a list of argument lists and, for each,
what Click parses it to: the parameter values by name, the exit code of `--help` and
`--version`, or the kind of error when 1.0 refuses the list. The test replays every
list, in a temporary folder with the files the lists name, without running a command,
with each parent parsed as `desflow` parses it. A list that 1.0 accepts must parse to
the same values, with the same types, so 3 and 3.0 differ. A list that 1.0 refused
may still be refused or may now be accepted. The replay shows what the description
cannot: how the parser reads a command line. A parent is given only the name of the
next command, so an argument added to a group is caught by the description, not the
replay.

The refusal of a missing required parameter depends on the locked Typer, which every
job uses.

What the guard does not cover: a check inside a command or a callback, such as
`parse_until`, which `run` and `explain` call on `--until`, is outside it. The guard
compares what Click parses, not what the command then accepts. That check is covered by
the command's own tests and by the review of each change.

`NARROWING_CASES`, the attribute cases and the small app change a copy of the
description, or a small app, in each way that would narrow the command line, and each
must fail. `WIDENING_CASES` and the small app change it in each way that widens, and
each must pass. A test fails when a declared attribute has no case, so a kind of
difference that the comparison misses gets a case with the fix.
"""

import contextlib
import copy
import io
import json
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional, Tuple

import pytest
import typer
from typer.main import get_command

from desflow.cli import app

FROZEN_FILE = Path(__file__).parent / "frozen" / "cli.1.0.json"

FROZEN_MESSAGE = (
    "The `desflow` command line is frozen from 1.0. 1.x may only widen the command "
    "line: the declared attributes of every 1.0 command and parameter are kept as a "
    "file, every difference from them must be a listed widening, and every 1.0 "
    "invocation parses to the same values. Both are kept in "
    "tests/unit/frozen/cli.1.0.json. A command, option, alias or choice that is new "
    "needs no change to the file. Command Line documents the surface and "
    "Compatibility Policy states the promise."
)

UPKEEP_MESSAGE = (
    "Typer {version} is installed. If desflow did not change this, a Click or Typer "
    "release did. Check the release notes of Click and Typer for the change. Confirm "
    "that every command line 1.0 accepts is still accepted: the replay in this test "
    "shows it. Then record the change in TYPER_CHANGES in "
    "tests/unit/test_cli_surface.py, with the release and the reason. Never "
    "regenerate tests/unit/frozen/cli.1.0.json."
)

# Changes that a Click or Typer release made to what the description names, or that
# desflow made on purpose (a renamed callback), each checked by hand as the message
# above says. Each key is one of:
#   "class:<old module>.<qualified name>"      new value: "<new module>.<qualified name>"
#   "attr:<owner>.<name>" or "attr:<path>.<name>"  new value: the new attribute name,
#       or "" when the attribute is gone; <owner> is command, param or type
#   "value:<path>.<name>"                       new value: the new value as JSON
# Each value is (new, release, reason). A path reads as in a failure, for example
# "desflow run.callback".
TYPER_CHANGES: Dict[str, Tuple[str, str, str]] = {}

# The names of the files and the folder that the argument lists name.
FILES = ("file_1.txt", "file_2.txt", "file_3.txt")
FOLDER = "folder_1"
MISSING = "missing_1"


def _make_workspace(folder: Path) -> None:
    for name in FILES:
        (folder / name).write_text("x\n")
    (folder / FOLDER).mkdir()


def _jsonable(value: Any, folder: Optional[Path]) -> Any:
    if isinstance(value, (list, tuple)):
        return [_jsonable(item, folder) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item, folder) for key, item in value.items()}
    if isinstance(value, Enum):
        return _jsonable(value.value, folder)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    text = str(value)
    if folder is not None:
        for prefix in {str(folder), str(folder.resolve())}:
            text = text.replace(prefix, "<folder>")
    return text


def _parse(
    root: Any, path: List[str], args: List[str], folder: Optional[Path]
) -> Dict[str, Any]:
    """What Click parses the argument list to, without running the command."""
    command: Any = root
    parent = None
    try:
        for position, name in enumerate(path[1:]):
            parent = command.make_context(
                path[position], [name], parent=parent, resilient_parsing=False
            )
            command = command.get_command(parent, name)
        sink = io.StringIO()
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            context = command.make_context(
                path[-1], list(args), parent=parent, resilient_parsing=False
            )
        context.close()
    except Exception as error:
        if type(error).__name__ == "Exit":
            return {"exit": getattr(error, "exit_code", 0)}
        return {"error": type(error).__name__}
    return {
        "values": {
            name: _jsonable(value, folder) for name, value in context.params.items()
        }
    }


def _walk(command: Any, path: List[str]) -> List[Tuple[List[str], Any]]:
    nodes = [(path, command)]
    for name, child in getattr(command, "commands", {}).items():
        nodes.extend(_walk(child, [*path, name]))
    return nodes


# The argument lists of one command, made from its parameters.


def _kind(param_type: Any) -> str:
    return type(param_type).__name__.removeprefix("Typer")


def _numbers(param_type: Any) -> List[str]:
    """A typical value, each limit, and the value just inside and just outside it."""
    low, high = param_type.min, param_type.max
    is_float = "Float" in _kind(param_type)
    step = 0.001 if is_float else 1
    if low is not None and high is not None:
        values = [(low + high) / 2 if is_float else (low + high) // 2]
    elif low is not None:
        values = [low + 10]
    elif high is not None:
        values = [high - 10]
    else:
        values = [3]
    for limit, direction in ((low, 1), (high, -1)):
        if limit is not None:
            values += [limit - direction * step, limit, limit + direction * step]
    return [repr(round(v, 6)) if is_float else str(int(v)) for v in values]


def _samples(param: Any) -> List[List[str]]:
    """The values to try for one parameter, each as the tokens that give it."""
    param_type, kind = param.type, _kind(param.type)
    if getattr(param_type, "choices", None) is not None:
        values = [str(choice) for choice in param_type.choices]
    elif hasattr(param_type, "min") and hasattr(param_type, "max"):
        values = _numbers(param_type)
    elif hasattr(param_type, "exists"):
        values = [MISSING, FILES[0], FOLDER]
    elif kind.startswith("Int"):
        values = ["3", "-3"]
    elif kind.startswith("Float"):
        values = ["0.5", "-0.5"]
    else:
        values = ["text"]
    return [[value] * max(param.nargs, 1) for value in values]


def _valid(param: Any) -> List[str]:
    """Tokens that give the parameter a value that 1.0 accepts."""
    if hasattr(param.type, "exists") and not param.type.file_okay:
        return [FOLDER]
    if hasattr(param.type, "exists"):
        return [FILES[0]]
    return _samples(param)[0]


def _three(param: Any) -> List[str]:
    """Three values for a variadic argument."""
    if hasattr(param.type, "exists"):
        return list(FILES)
    if hasattr(param.type, "min") or _kind(param.type)[:3] in ("Int", "Flo"):
        return _valid(param) * 3
    return ["a", "b", "c"]


def _is_argument(param: Any) -> bool:
    return param.param_type_name == "argument"


def _is_flag(param: Any) -> bool:
    return bool(getattr(param, "is_flag", False)) and not hasattr(param.type, "choices")


def _required(
    arguments: List[Any],
    options: List[Any],
    skip: Any = None,
    give: Optional[Tuple[Any, List[str]]] = None,
) -> List[str]:
    """Tokens for the required arguments and options, one of them replaced."""
    tokens: List[str] = []
    for argument in arguments:
        if give is not None and argument is give[0]:
            tokens += give[1]
        elif argument.required and argument is not skip:
            tokens += _valid(argument)
    for option in options:
        if option.required and option is not skip:
            tokens += [option.opts[0], *_valid(option)]
    return tokens


def _argument_lists(command: Any) -> List[List[str]]:
    arguments: List[Any] = []
    options: List[Any] = []
    for param in command.params:
        if param.hidden:
            continue
        (arguments if _is_argument(param) else options).append(param)
    lists: List[List[str]] = [[], ["--help"], _required(arguments, options)]
    every_argument: List[str] = []
    for argument in arguments:
        every_argument += _valid(argument)
    lists.append(every_argument)
    for argument in arguments:
        values = _samples(argument)
        if argument.nargs < 0:
            values.append(_three(argument))
        for value in values:
            lists.append(_required(arguments, options, give=(argument, value)))
    for option in options:
        base = _required(arguments, options, skip=option)
        for name in [*option.opts, *option.secondary_opts]:
            if _is_flag(option):
                lists.append([*base, name])
                continue
            for value in _samples(option):
                lists.append([*base, name, *value])
            if option.multiple:
                lists.append([*base, name, *_valid(option), name, *_valid(option)])
    unique: List[List[str]] = []
    for item in lists:
        if item not in unique:
            unique.append(item)
    return unique


def _capture(root: Any, folder: Optional[Path]) -> Dict[str, Any]:
    """The invocations of every command and what Click parses each to."""
    commands: Dict[str, Any] = {}
    for path, command in _walk(root, ["desflow"]):
        commands[" ".join(path)] = [
            {"args": args, "result": _parse(root, path, args, folder)}
            for args in _argument_lists(command)
        ]
    return {"commands": commands}


def _same(left: Any, right: Any) -> bool:
    """Equal, with the same types all the way down, so 3 and 3.0 differ."""
    if type(left) is not type(right):
        return False
    if isinstance(left, list):
        return len(left) == len(right) and all(map(_same, left, right))
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(
            _same(value, right[key]) for key, value in left.items()
        )
    return bool(left == right)


def _differences(
    frozen: Dict[str, Any], root: Any, folder: Optional[Path]
) -> List[str]:
    """Every invocation that parsed in 1.0 and now parses differently."""
    nodes = {" ".join(path): path for path, _ in _walk(root, ["desflow"])}
    problems: List[str] = []
    for key, entries in frozen["commands"].items():
        if key not in nodes:
            problems.append(f"{key}: the command no longer exists")
            continue
        for entry in entries:
            then = entry["result"]
            if "error" in then:
                continue
            now = _parse(root, nodes[key], entry["args"], folder)
            if "values" in then and "values" in now:
                same = all(
                    name in now["values"] and _same(now["values"][name], value)
                    for name, value in then["values"].items()
                )
            else:
                same = _same(then, now)
            if not same:
                problems.append(
                    f"{key} {' '.join(entry['args'])}\n"
                    f"  1.0: {json.dumps(then)}\n  now: {json.dumps(now)}"
                )
    return problems


def _report(problems: List[str]) -> str:
    return FROZEN_MESSAGE + "\n" + "\n".join(problems)


def _frozen() -> Dict[str, Any]:
    return json.loads(FROZEN_FILE.read_text())


# The description: every attribute of every command, parameter and parameter type.


def _plain(value: Any) -> Any:
    """A value as JSON holds it; what is not plain is its qualified name."""
    if isinstance(value, Enum):
        return {"enum": f"{type(value).__qualname__}.{value.name}"}
    if value is None:
        return None
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return float(value)
    if isinstance(value, str):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((_plain(item) for item in value), key=repr)
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, Path):
        return {"path": str(value)}
    name = getattr(value, "__qualname__", type(value).__qualname__)
    return {"object": name}


# The declared attributes: the arguments that Click documents for each constructor,
# recorded under the name Click stores them by. Source: the Click 8.3.1 API reference
# (the Click that Typer 0.27.3 carries, as typer/_click/__init__.py says), and the
# source of that copy (typer/_click/core.py, typer/_click/types.py, typer/core.py,
# typer/models.py, typer/_types.py) where the reference is silent about the stored
# name. An argument that the copy does not store is not listed.
COMMAND = (  # click.Command(name, context_settings, callback, params, help, epilog, ...)
    "name",
    "context_settings",
    "callback",
    "help",
    "epilog",
    "short_help",
    "options_metavar",
    "add_help_option",
    "no_args_is_help",
    "hidden",
    "deprecated",
)
GROUP = (  # click.Group: Command and commands, invoke_without_command, ...
    *COMMAND,
    "invoke_without_command",
    "subcommand_metavar",
    "_result_callback",  # the `result_callback` argument, stored with a leading _
)
PARAMETER = (  # click.Parameter(param_decls, type, required, default, callback, ...)
    "name",  # from param_decls
    "opts",  # from param_decls
    "secondary_opts",  # from param_decls
    "required",
    "default",
    "callback",
    "nargs",
    "multiple",
    "metavar",
    "expose_value",
    "is_eager",
    "envvar",
    "_custom_shell_complete",  # the `shell_complete` argument, stored with a leading _
)
ARGUMENT = PARAMETER  # click.Argument(param_decls, required, **attrs)
OPTION = (  # click.Option: Parameter and prompt, confirmation_prompt, ... show_envvar
    *PARAMETER,
    "show_default",
    "prompt",
    "confirmation_prompt",
    "prompt_required",
    "hide_input",
    "is_flag",
    "count",
    "allow_from_autoenv",
    "help",
    "hidden",
    "show_choices",
    "show_envvar",
)
INT_RANGE = ("min", "max", "min_open", "max_open", "clamp")  # click.IntRange
FLOAT_RANGE = INT_RANGE  # click.FloatRange
PATH = (  # click.Path(exists, file_okay, dir_okay, writable, readable, ...)
    "name",
    "exists",
    "file_okay",
    "dir_okay",
    "writable",
    "readable",
    "resolve_path",
    "allow_dash",
    "type",  # the `path_type` argument
)
CHOICE = ("choices", "case_sensitive")  # click.Choice(choices, case_sensitive)
# click.BOOL, click.STRING, click.INT and click.FLOAT take no argument.
TYPES = {
    "IntRange": INT_RANGE,
    "FloatRange": FLOAT_RANGE,
    "TyperPath": PATH,
    "TyperChoice": CHOICE,
}


def _class_path(obj: Any) -> str:
    return f"{type(obj).__module__}.{type(obj).__qualname__}"


def _declared(obj: Any, names: Tuple[str, ...]) -> Dict[str, Any]:
    stored = vars(obj)
    return {name: _plain(stored[name]) for name in names if name in stored}


def _describe_param(param: Any) -> Dict[str, Any]:
    names = ARGUMENT if param.param_type_name == "argument" else OPTION
    return {
        "class": _class_path(param),
        "attrs": _declared(param, names),
        "type": {
            "class": _class_path(param.type),
            "attrs": _declared(
                param.type, TYPES.get(type(param.type).__qualname__, ())
            ),
        },
    }


def _describe(command: Any) -> Dict[str, Any]:
    """The declared attributes of the command and its parameters, and of each
    subcommand."""
    subcommands = getattr(command, "commands", None)
    return {
        "class": _class_path(command),
        "attrs": _declared(command, COMMAND if subcommands is None else GROUP),
        "params": {param.name: _describe_param(param) for param in command.params},
        "commands": {
            name: _describe(child) for name, child in (subcommands or {}).items()
        },
    }


# The attributes that only show help.
HELP_ATTRIBUTES = frozenset(
    {
        "help",
        "short_help",
        "epilog",
        "metavar",
        "options_metavar",
        "subcommand_metavar",
        "show_default",
        "show_choices",
        "show_envvar",
        "deprecated",
        "hidden",
    }
)
HELP = {
    "command": HELP_ATTRIBUTES,
    "param": HELP_ATTRIBUTES,
    "type": frozenset({"name"}),
}
TRUE_TO_FALSE = frozenset({"min_open", "max_open", "exists", "readable", "writable"})
FALSE_TO_TRUE = frozenset({"clamp", "file_okay", "dir_okay", "allow_dash"})


def _number(value: Any) -> bool:
    return type(value) in (int, float)


def _widening(owner: str, key: str, then: Any, now: Any) -> bool:
    """Whether a changed attribute is one of the listed widenings."""
    if key in HELP[owner]:
        return True  # it only shows help
    if owner == "param" and key == "required":
        return then is True and now is False  # a parameter the caller may leave out
    if owner == "param" and key in ("opts", "secondary_opts"):
        # a new name, and every name 1.0 had stays in its list
        return (
            isinstance(then, list)
            and isinstance(now, list)
            and all(name in now for name in then)
        )
    if owner != "type":
        return False
    if key == "min":  # the lower limit falls or goes
        return _number(then) and (now is None or (_number(now) and now <= then))
    if key == "max":  # the upper limit rises or goes
        return _number(then) and (now is None or (_number(now) and now >= then))
    if key in TRUE_TO_FALSE:  # a limit or a check that is lifted
        return then is True and now is False
    if key in FALSE_TO_TRUE:  # something that is now allowed
        return then is False and now is True
    if key == "choices":  # a new choice, and every 1.0 choice stays
        return (
            isinstance(then, list)
            and isinstance(now, list)
            and all(choice in now for choice in then)
        )
    return False


def _show(value: Any) -> str:
    return json.dumps(value)


def _upkeep(what: str) -> str:
    return f"{what}\n  {UPKEEP_MESSAGE.format(version=typer.__version__)}"


def _recorded(kind: str, where: str, owner: str, name: str) -> Optional[Tuple]:
    """The TYPER_CHANGES entry for an attribute, by its path or by its owner."""
    for key in (f"{kind}:{where}.{name}", f"{kind}:{owner}.{name}"):
        if key in TYPER_CHANGES:
            return TYPER_CHANGES[key]
    return None


def _compare_class(then: str, now: str, where: str) -> List[str]:
    if then == now:
        return []
    entry = TYPER_CHANGES.get(f"class:{then}")
    if entry is not None and entry[0] == now:
        return []
    if then.rsplit(".", 1)[-1] == now.rsplit(".", 1)[-1]:
        return [_upkeep(f"{where}: class 1.0 {then}, now {now}: the module changed")]
    return [f"{where}: class 1.0 {then}, now {now}"]


def _compare_attributes(
    owner: str, then: Dict[str, Any], now: Dict[str, Any], where: str
) -> List[str]:
    problems = []
    renamed = set()
    for key, value in then.items():
        if key in now:
            continue
        entry = _recorded("attr", where, owner, key)
        if entry is None:
            problems.append(
                _upkeep(f"{where}.{key}: attribute is gone, 1.0 had {_show(value)}")
            )
        elif entry[0] and not (entry[0] in now and _same(value, now[entry[0]])):
            problems.append(
                f"{where}.{key}: TYPER_CHANGES says it is now {entry[0]}, which does "
                f"not hold {_show(value)}"
            )
        else:
            renamed.add(entry[0])
    for key, value in now.items():
        if key not in then and key not in renamed:
            problems.append(
                _upkeep(f"{where}.{key}: attribute is new, it is {_show(value)}")
            )
    for key, value in then.items():
        if key in now and not _same(value, now[key]):
            entry = _recorded("value", where, owner, key)
            if entry is not None and entry[0] == _show(now[key]):
                continue
            if not _widening(owner, key, value, now[key]):
                problems.append(
                    f"{where}.{key}: 1.0 {_show(value)}, now {_show(now[key])}"
                )
    return problems


def _is_argument_node(node: Dict[str, Any]) -> bool:
    return node["class"].endswith("Argument")


def _refusing(node: Dict[str, Any]) -> Optional[str]:
    """The attribute through which a new parameter could refuse a command line that
    1.0 accepts, if it has one: Click runs the callback, the prompt, the environment
    variable and a default that is a function on every parse."""
    attrs = node["attrs"]
    if attrs.get("required") is not False:
        return "required"
    for key in ("callback", "prompt", "envvar"):
        if attrs.get(key) not in (None, False):
            return key
    default = attrs.get("default")
    if isinstance(default, dict) and "object" in default:
        return "default"
    return None


def _compare_parameters(
    then: Dict[str, Any], now: Dict[str, Any], where: str, has_commands: bool
) -> List[str]:
    problems = []
    for name, param in then.items():
        here = f"{where}:{name}"
        if name not in now:
            problems.append(f"{here}: the parameter no longer exists")
            continue
        for part, then_part, now_part in (
            ("", param, now[name]),
            (".type", param["type"], now[name]["type"]),
        ):
            problems += _compare_class(
                then_part["class"], now_part["class"], here + part
            )
            owner = "type" if part else "param"
            problems += _compare_attributes(
                owner, then_part["attrs"], now_part["attrs"], f"{here}{part}"
            )
    kept_then = [name for name in then if name in now]
    if kept_then != [name for name in now if name in then]:
        problems.append(f"{where}: the order of the parameters changed")
    names = list(now)
    last_old_argument = max(
        (i for i, n in enumerate(names) if n in then and _is_argument_node(now[n])),
        default=-1,
    )
    variadic = any(
        _is_argument_node(node) and node["attrs"].get("nargs") == -1
        for node in then.values()
    )
    for position, name in enumerate(names):
        node = now[name]
        if name not in then:
            refusing = _refusing(node)
            if refusing == "required":
                problems.append(f"{where}:{name}: a new parameter that is required")
            elif refusing is not None:
                problems.append(
                    f"{where}:{name}: a new parameter that sets {refusing} can refuse a "
                    "command line that 1.0 accepts"
                )
            elif _is_argument_node(node):
                if has_commands:
                    problems.append(
                        f"{where}:{name}: a new argument on a group takes the name "
                        "of its command"
                    )
                elif variadic or position < last_old_argument:
                    problems.append(
                        f"{where}:{name}: a new argument that does not come last, or "
                        "that follows an argument taking any number of values"
                    )
        if _is_argument_node(node) and node["attrs"].get("nargs") == -1:
            variadic = True
    return problems


def _compare(then: Dict[str, Any], now: Dict[str, Any], where: str = "desflow"):
    """Every difference between the 1.0 description and the current one that is
    not a listed widening, each with its path and both values."""
    problems = _compare_class(then["class"], now["class"], where)
    problems += _compare_attributes("command", then["attrs"], now["attrs"], where)
    has_commands = bool(then["commands"] or now["commands"])
    problems += _compare_parameters(then["params"], now["params"], where, has_commands)
    for name, child in then["commands"].items():
        if name not in now["commands"]:
            problems.append(f"{where} {name}: the command no longer exists")
        else:
            problems += _compare(child, now["commands"][name], f"{where} {name}")
    return problems


def _walk_description(desc: Dict[str, Any], where: str = "desflow"):
    """Each node's attributes as (owner, path, attributes), in a fixed order."""
    yield "command", where, desc["attrs"]
    for name, param in desc["params"].items():
        yield "param", f"{where}:{name}", param["attrs"]
        yield "type", f"{where}:{name}.type", param["type"]["attrs"]
    for name, child in desc["commands"].items():
        yield from _walk_description(child, f"{where} {name}")


def _pairs(desc: Dict[str, Any]) -> set:
    return {
        (owner, key) for owner, _, attrs in _walk_description(desc) for key in attrs
    }


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    _make_workspace(tmp_path)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_every_1_0_invocation_still_parses_the_same(workspace):
    problems = _differences(_frozen(), get_command(app), workspace)
    assert not problems, _report(problems)


# A small app, changed in one way at a time.

REQUIRED: Any = ...


def _small_app(
    *,
    low: int = 1,
    high: int = 9,
    exists: bool = False,
    out_exists: bool = False,
    flag: str = "--drain/--no-drain",
    names: Tuple[str, ...] = ("--name", "--label"),
    seed_type: Any = int,
    modes: Tuple[str, ...] = ("fast", "slow"),
    level_required: bool = False,
    target_required: bool = True,
    with_option: bool = True,
    root_required: bool = False,
    after_variadic: bool = False,
    help_names: Optional[List[str]] = None,
    run_help: bool = True,
    new_command: bool = False,
    new_option: bool = False,
    new_alias: bool = False,
    new_choice: bool = False,
    new_argument: bool = False,
) -> typer.Typer:
    settings = {"help_option_names": help_names} if help_names is not None else None
    small = typer.Typer(
        no_args_is_help=True, add_completion=False, context_settings=settings
    )
    members = {name: name for name in (*modes, *(["extra"] * new_choice))}
    mode_type = Enum("Mode", members, type=str)  # type: ignore[misc]
    all_names = (*names, *(["--tag"] * new_alias))
    run_settings: Optional[Dict[str, Any]] = (
        None if run_help else {"help_option_names": []}
    )

    if root_required:

        @small.callback()
        def _root_with_profile(
            profile: Annotated[str, typer.Option("--profile")] = REQUIRED,
        ) -> None:
            """Small app."""

    else:

        @small.callback()
        def _root() -> None:
            """Small app."""

    @small.command(context_settings=run_settings)
    def run(
        source: Annotated[Path, typer.Argument(exists=exists)],
        count: Annotated[
            Optional[int], typer.Option("--count", min=low, max=high)
        ] = None,
        seed: Annotated[Optional[seed_type], typer.Option("--seed")] = None,
        drain: Annotated[bool, typer.Option(flag)] = False,
        name: Annotated[Optional[str], typer.Option(*all_names)] = None,
        mode: Annotated[Optional[mode_type], typer.Option("--mode")] = None,
        out: Annotated[Optional[Path], typer.Option("--out", exists=out_exists)] = None,
        level: Annotated[float, typer.Option("--level")] = (
            REQUIRED if level_required else 0.5
        ),
        target: Annotated[Optional[float], typer.Option("--target")] = (
            REQUIRED if target_required else None
        ),
    ) -> None:
        """Run."""

    if new_argument:

        @small.command("show")
        def show(
            item: Annotated[str, typer.Argument()],
            label: Annotated[Optional[str], typer.Argument()] = None,
        ) -> None:
            """Show."""

    else:

        @small.command("show")
        def show(item: Annotated[str, typer.Argument()]) -> None:
            """Show."""

    if after_variadic:

        @small.command("check")
        def check(
            files: Annotated[List[Path], typer.Argument()],
            last: Annotated[Optional[str], typer.Argument()] = None,
        ) -> None:
            """Check."""

    else:

        @small.command("check")
        def check(files: Annotated[List[Path], typer.Argument()]) -> None:
            """Check."""

    if with_option and new_option:

        @small.command("tune")
        def tune(
            speed: Annotated[Optional[int], typer.Option("--speed")] = None,
            pace: Annotated[Optional[int], typer.Option("--pace")] = None,
        ) -> None:
            """Tune."""

    elif with_option:

        @small.command("tune")
        def tune(
            speed: Annotated[Optional[int], typer.Option("--speed")] = None,
        ) -> None:
            """Tune."""

    else:

        @small.command("tune")
        def tune() -> None:
            """Tune."""

    if new_command:

        @small.command("brand-new")
        def brand_new() -> None:
            """New."""

    return small


# Each change that would narrow the command line, which the replay must fail.
NARROWINGS: Dict[str, Dict[str, Any]] = {
    "a range limit raised": {"low": 3},
    "a range limit lowered at the top": {"high": 8},
    "exists=True added to an argument": {"exists": True},
    "exists=True added to an option": {"out_exists": True},
    "a boolean pair swapped": {"flag": "--no-drain/--drain"},
    "an argument added after a variadic one": {"after_variadic": True},
    "--help renamed": {"help_names": ["-h"]},
    "--help turned off": {"run_help": False},
    "an option removed": {"with_option": False},
    "an alias removed": {"names": ("--name",)},
    "a choice removed": {"modes": ("fast",)},
    "an option made required": {"level_required": True},
    "a type changed from int to text": {"seed_type": str},
    "a type changed from int to float": {"seed_type": float},
    "a required option added to the root": {"root_required": True},
}

# Each change that widens it, which the replay must accept.
WIDENINGS: Dict[str, Dict[str, Any]] = {
    "a new command": {"new_command": True},
    "a new option": {"new_option": True},
    "a new alias": {"new_alias": True},
    "a new choice": {"new_choice": True},
    "an option made optional": {"target_required": False},
    "a new optional argument after a plain one": {"new_argument": True},
    "a range limit lowered": {"low": 0, "high": 10},
}


def _small_differences(workspace: Path, **changes: Any) -> List[str]:
    frozen = _capture(get_command(_small_app()), workspace)
    return _differences(frozen, get_command(_small_app(**changes)), workspace)


def _small_description_differences(**changes: Any) -> List[str]:
    then = _describe(get_command(_small_app()))
    return _compare(then, _describe(get_command(_small_app(**changes))))


def test_the_small_app_replays_with_no_change(workspace):
    assert _small_differences(workspace) == []
    assert _small_description_differences() == []


@pytest.mark.parametrize("name", NARROWINGS)
def test_each_narrowing_fails(workspace, name):
    assert _small_differences(workspace, **NARROWINGS[name]), name


@pytest.mark.parametrize("name", NARROWINGS)
def test_each_narrowing_fails_the_description(name):
    assert _small_description_differences(**NARROWINGS[name]), name


@pytest.mark.parametrize("name", WIDENINGS)
def test_each_widening_passes(workspace, name):
    assert _small_differences(workspace, **WIDENINGS[name]) == [], name
    assert _small_description_differences(**WIDENINGS[name]) == [], name


def test_a_removed_command_fails_by_name(workspace):
    frozen = _capture(get_command(_small_app()), workspace)
    frozen["commands"]["desflow gone"] = [{"args": [], "result": {"values": {}}}]
    problems = _differences(frozen, get_command(_small_app()), workspace)
    assert problems == ["desflow gone: the command no longer exists"]


def test_a_difference_names_the_command_the_arguments_and_both_results(workspace):
    problems = _small_differences(workspace, seed_type=str)
    assert any(
        "desflow run" in problem
        and "--seed 3" in problem
        and '"seed": 3' in problem
        and '"seed": "3"' in problem
        for problem in problems
    ), problems
    message = _report(problems)
    assert "Command Line" in message and "Compatibility Policy" in message
    assert "may only widen the command line" in message


# The description: the real app, and one change to a copy of the 1.0 description for
# each attribute it holds and each rule of `_widening`.


def _description() -> Dict[str, Any]:
    return _frozen()["description"]


def test_the_current_app_has_the_1_0_description():
    problems = _compare(_description(), _describe(get_command(app)))
    assert not problems, _report(problems)


# A declared attribute that is neither a rule nor help only. Changing it fails.
OTHER_ATTRIBUTES = {
    "command": (
        "_result_callback",
        "add_help_option",
        "callback",
        "context_settings",
        "invoke_without_command",
        "name",
        "no_args_is_help",
    ),
    "param": (
        "_custom_shell_complete",
        "allow_from_autoenv",
        "callback",
        "confirmation_prompt",
        "count",
        "default",
        "envvar",
        "expose_value",
        "hide_input",
        "is_eager",
        "is_flag",
        "multiple",
        "name",
        "nargs",
        "prompt",
        "prompt_required",
    ),
    "type": ("case_sensitive", "resolve_path", "type"),
}
# The attributes that a rule of `_widening` handles, each with cases below.
RULE_ATTRIBUTES = {
    "command": (),
    "param": ("required", "opts", "secondary_opts"),
    "type": (
        "min",
        "max",
        "min_open",
        "max_open",
        "clamp",
        "exists",
        "readable",
        "writable",
        "file_okay",
        "dir_okay",
        "allow_dash",
        "choices",
    ),
}


def _change(value: Any) -> Any:
    """A different value of the same kind, which no rule of `_widening` lists."""
    if value is None:
        return {"object": "added_callback"}
    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1
    if isinstance(value, float):
        return value + 1.0
    if isinstance(value, str):
        return value + "x"
    if isinstance(value, list):
        return [*value, "x"]
    if isinstance(value, dict):
        return {**value, "x": 1}
    raise TypeError(f"no change is defined for {value!r}")


def _nodes(desc: Dict[str, Any]) -> List[Tuple[str, str, Dict[str, Any]]]:
    return list(_walk_description(desc))


def _has(key: str) -> Any:
    return lambda attrs: key in attrs


def _every(attrs: Dict[str, Any]) -> bool:
    return True


def _rule(owner: str, wanted: Any, then_set: Dict[str, Any], now_set: Dict[str, Any]):
    """A case: both copies get `then_set` on the first node that `wanted` picks,
    and the current copy gets `now_set` on top."""

    def case(then: Dict[str, Any], now: Dict[str, Any]) -> None:
        index = next(
            i
            for i, (kind, _, attrs) in enumerate(_nodes(then))
            if kind == owner and wanted(attrs)
        )
        _nodes(then)[index][2].update(then_set)
        _nodes(now)[index][2].update(then_set)
        _nodes(now)[index][2].update(now_set)

    return case


def _arguments(node: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [param for param in node["params"].values() if _is_argument_node(param)]


def _variadic(node: Dict[str, Any]) -> bool:
    return any(a["attrs"]["nargs"] == -1 for a in _arguments(node))


def _plain_arguments(node: Dict[str, Any]) -> bool:
    arguments = _arguments(node)
    return bool(arguments) and not _variadic(node)


def _group(node: Dict[str, Any]) -> bool:
    return bool(node["commands"])


def _node_for(now: Dict[str, Any], kind: str) -> Dict[str, Any]:
    """The root, the first group, or the first command that has the kind of arguments."""
    if kind == "root":
        return now
    commands = list(now["commands"].values())
    if kind == "group":
        return next(command for command in commands if _group(command))
    fits = {"variadic": _variadic, "plain": _plain_arguments, "any": lambda n: True}
    return next(command for command in commands if fits[kind](command))


def _new(kind: str, required: bool, position: str = "last", **attrs: Any) -> Any:
    """A case that adds a parameter to the first command that `kind` fits. It has no
    callback, prompt, environment variable or default unless `attrs` sets one."""

    def case(then: Dict[str, Any], now: Dict[str, Any]) -> None:
        node = _node_for(now, kind)
        argument = position != "option"
        param = {
            "class": f"typer.core.{'TyperArgument' if argument else 'TyperOption'}",
            "attrs": {
                "name": "added",
                "required": required,
                "nargs": 1,
                "callback": None,
                "prompt": None,
                "envvar": None,
                "default": None,
                **attrs,
            },
            "type": {"class": "typer._click.types.StringParamType", "attrs": {}},
        }
        if position == "first":
            node["params"] = {"added": param, **node["params"]}
        else:
            node["params"]["added"] = param

    return case


def _new_command(then: Dict[str, Any], now: Dict[str, Any]) -> None:
    now["commands"]["brand-new"] = copy.deepcopy(now["commands"]["run"])
    now["commands"]["brand-new"]["params"]["file"]["attrs"]["required"] = True


def _command_gone(then: Dict[str, Any], now: Dict[str, Any]) -> None:
    del now["commands"]["schema"]


def _parameter_gone(then: Dict[str, Any], now: Dict[str, Any]) -> None:
    del now["commands"]["run"]["params"]["until"]


def _order_changed(then: Dict[str, Any], now: Dict[str, Any]) -> None:
    params = now["commands"]["run"]["params"]
    now["commands"]["run"]["params"] = dict(reversed(list(params.items())))


def _class_changed(owner: str, module_only: bool = False) -> Any:
    def case(then: Dict[str, Any], now: Dict[str, Any]) -> None:
        run = now["commands"]["run"]
        node = {
            "command": run,
            "param": run["params"]["until"],
            "type": run["params"]["until"]["type"],
        }[owner]
        module, _, name = node["class"].rpartition(".")
        node["class"] = f"other.{name}" if module_only else f"{module}.Other{name}"

    return case


def _attribute_added(owner: str) -> Any:
    def case(then: Dict[str, Any], now: Dict[str, Any]) -> None:
        for kind, _, attrs in _nodes(now):
            if kind == owner:
                attrs["a_new_attribute"] = None
                return

    return case


def _attribute_removed(owner: str) -> Any:
    def case(then: Dict[str, Any], now: Dict[str, Any]) -> None:
        for kind, _, attrs in _nodes(now):
            if kind == owner and attrs:
                del attrs[next(iter(attrs))]
                return

    return case


def _typed(then_value: Any, now_value: Any) -> Any:
    return _rule(
        "param", _has("default"), {"default": then_value}, {"default": now_value}
    )


# Each change that narrows the command line, applied to a copy of the 1.0 description.
NARROWING_CASES: Dict[str, Any] = {
    "a required option added to the root": _new("root", True, "option"),
    "a required option added to a group": _new("group", True, "option"),
    "a required option added to a command": _new("any", True, "option"),
    "a required argument added": _new("plain", True),
    "an argument added after one that takes any number of values": _new(
        "variadic", False
    ),
    "an optional argument added to the root": _new("root", False),
    "an optional argument added to a group": _new("group", False),
    "a new optional option with a callback": _new(
        "any", False, "option", callback={"object": "f"}
    ),
    "a new optional option with a prompt": _new("any", False, "option", prompt="Added"),
    "a new optional option with an envvar": _new(
        "any", False, "option", envvar="DESFLOW_ADDED"
    ),
    "a new optional option with a default that is a function": _new(
        "any", False, "option", default={"object": "f"}
    ),
    "an argument added before an argument of 1.0": _new("plain", False, "first"),
    "a command removed": _command_gone,
    "a parameter removed": _parameter_gone,
    "the order of the parameters changed": _order_changed,
    "the class of a command changed": _class_changed("command"),
    "the class of a parameter changed": _class_changed("param"),
    "the class of a type changed": _class_changed("type"),
    "a class of the same name from another module, a command": _class_changed(
        "command", True
    ),
    "a class of the same name from another module, a parameter": _class_changed(
        "param", True
    ),
    "a class of the same name from another module, a type": _class_changed(
        "type", True
    ),
    "an attribute added to a command": _attribute_added("command"),
    "an attribute added to a parameter": _attribute_added("param"),
    "an attribute added to a type": _attribute_added("type"),
    "an attribute removed from a command": _attribute_removed("command"),
    "an attribute removed from a parameter": _attribute_removed("param"),
    "an attribute removed from a type": _attribute_removed("type"),
    "a callback added": _rule(
        "command", _has("callback"), {"callback": None}, {"callback": {"object": "f"}}
    ),
    "a default changed from 3 to 3.0": _typed(3, 3.0),
    "a default changed from True to 1": _typed(True, 1),
    "an option made required": _rule(
        "param", _has("required"), {"required": False}, {"required": True}
    ),
    "a name of an option removed": _rule(
        "param", _has("opts"), {"opts": ["--a", "--b"]}, {"opts": ["--a"]}
    ),
    "a name of an option replaced": _rule(
        "param", _has("opts"), {"opts": ["--a"]}, {"opts": ["--b"]}
    ),
    "a name of a boolean pair removed": _rule(
        "param", _has("opts"), {"secondary_opts": ["--no-a"]}, {"secondary_opts": []}
    ),
    "a boolean pair swapped": _rule(
        "param",
        _has("opts"),
        {"opts": ["--a"], "secondary_opts": ["--no-a"]},
        {"opts": ["--no-a"], "secondary_opts": ["--a"]},
    ),
    "a lower limit raised": _rule("type", _has("min"), {"min": 2}, {"min": 3}),
    "a lower limit added": _rule("type", _has("min"), {"min": None}, {"min": 0}),
    "an upper limit lowered": _rule("type", _has("max"), {"max": 9}, {"max": 8}),
    "an upper limit added": _rule("type", _has("max"), {"max": None}, {"max": 50}),
    "a lower limit made open": _rule(
        "type", _has("min_open"), {"min_open": False}, {"min_open": True}
    ),
    "an upper limit made open": _rule(
        "type", _has("max_open"), {"max_open": False}, {"max_open": True}
    ),
    "clamp turned off": _rule("type", _has("clamp"), {"clamp": True}, {"clamp": False}),
    "exists=True added": _rule(
        "type", _has("exists"), {"exists": False}, {"exists": True}
    ),
    "readable=True added": _rule(
        "type", _has("readable"), {"readable": False}, {"readable": True}
    ),
    "writable=True added": _rule(
        "type", _has("writable"), {"writable": False}, {"writable": True}
    ),
    "file_okay turned off": _rule(
        "type", _has("file_okay"), {"file_okay": True}, {"file_okay": False}
    ),
    "dir_okay turned off": _rule(
        "type", _has("dir_okay"), {"dir_okay": True}, {"dir_okay": False}
    ),
    "allow_dash turned off": _rule(
        "type", _has("allow_dash"), {"allow_dash": True}, {"allow_dash": False}
    ),
    "a choice removed": _rule(
        "type", _every, {"choices": ["a", "b"]}, {"choices": ["a"]}
    ),
    "a choice replaced": _rule(
        "type", _every, {"choices": ["a", "b"]}, {"choices": ["a", "c"]}
    ),
}

# Each change that widens it, applied the same way.
WIDENING_CASES: Dict[str, Any] = {
    "a new command": _new_command,
    "a new option on the root": _new("root", False, "option"),
    "a new option on a group": _new("group", False, "option"),
    "a new option on a command": _new("any", False, "option"),
    "a new argument after the arguments of a command": _new("plain", False),
    "a parameter made optional": _rule(
        "param", _has("required"), {"required": True}, {"required": False}
    ),
    "a new name for an option": _rule(
        "param", _has("opts"), {"opts": ["--a"]}, {"opts": ["--a", "-a"]}
    ),
    "a new name for a boolean pair": _rule(
        "param",
        _has("opts"),
        {"secondary_opts": ["--no-a"]},
        {"secondary_opts": ["--no-a", "--off"]},
    ),
    "a lower limit lowered": _rule("type", _has("min"), {"min": 2}, {"min": 1}),
    "a lower limit removed": _rule("type", _has("min"), {"min": 2}, {"min": None}),
    "an upper limit raised": _rule("type", _has("max"), {"max": 9}, {"max": 10}),
    "an upper limit removed": _rule("type", _has("max"), {"max": 9}, {"max": None}),
    "a lower limit made closed": _rule(
        "type", _has("min_open"), {"min_open": True}, {"min_open": False}
    ),
    "an upper limit made closed": _rule(
        "type", _has("max_open"), {"max_open": True}, {"max_open": False}
    ),
    "clamp turned on": _rule("type", _has("clamp"), {"clamp": False}, {"clamp": True}),
    "exists=True removed": _rule(
        "type", _has("exists"), {"exists": True}, {"exists": False}
    ),
    "readable=True removed": _rule(
        "type", _has("readable"), {"readable": True}, {"readable": False}
    ),
    "writable=True removed": _rule(
        "type", _has("writable"), {"writable": True}, {"writable": False}
    ),
    "file_okay turned on": _rule(
        "type", _has("file_okay"), {"file_okay": False}, {"file_okay": True}
    ),
    "dir_okay turned on": _rule(
        "type", _has("dir_okay"), {"dir_okay": False}, {"dir_okay": True}
    ),
    "allow_dash turned on": _rule(
        "type", _has("allow_dash"), {"allow_dash": False}, {"allow_dash": True}
    ),
    "a new choice": _rule("type", _every, {"choices": ["a"]}, {"choices": ["a", "b"]}),
}


def _case_copies() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    then = copy.deepcopy(_description())
    return then, copy.deepcopy(then)


@pytest.mark.parametrize("name", NARROWING_CASES)
def test_each_narrowing_of_the_description_fails(name):
    then, now = _case_copies()
    NARROWING_CASES[name](then, now)
    problems = _compare(then, now)
    assert problems, name


@pytest.mark.parametrize("name", WIDENING_CASES)
def test_each_widening_of_the_description_passes(name):
    then, now = _case_copies()
    WIDENING_CASES[name](then, now)
    assert _compare(then, now) == [], name


def _pair_ids(table: Dict[str, Tuple[str, ...]]) -> List[Tuple[str, str]]:
    held = _pairs(_frozen()["description"])
    return [
        (owner, key)
        for owner, keys in table.items()
        for key in keys
        if (owner, key) in held
    ]


@pytest.mark.parametrize(("owner", "key"), _pair_ids(OTHER_ATTRIBUTES))
def test_changing_any_other_attribute_fails(owner, key):
    then, now = _case_copies()
    attrs = next(a for kind, _, a in _nodes(now) if kind == owner and key in a)
    attrs[key] = _change(attrs[key])
    problems = _compare(then, now)
    assert problems and f".{key}: 1.0" in problems[0], (owner, key)


@pytest.mark.parametrize(
    ("owner", "key"), _pair_ids({k: tuple(sorted(v)) for k, v in HELP.items()})
)
def test_changing_an_attribute_that_only_shows_help_passes(owner, key):
    then, now = _case_copies()
    attrs = next(a for kind, _, a in _nodes(now) if kind == owner and key in a)
    attrs[key] = _change(attrs[key])
    assert _compare(then, now) == [], (owner, key)


def test_every_declared_attribute_has_a_case():
    declared = {
        *(("command", key) for key in GROUP),
        *(("param", key) for key in OPTION),
        *(("type", key) for names in (*TYPES.values(), PATH) for key in names),
    }
    cased = {
        (owner, key)
        for table in (
            OTHER_ATTRIBUTES,
            RULE_ATTRIBUTES,
            {k: tuple(v) for k, v in HELP.items()},
        )
        for owner, keys in table.items()
        for key in keys
    }
    missing = sorted((declared | _pairs(_description())) - cased)
    assert not missing, (
        f"These declared attributes have no mutation case: {missing}. Add each to "
        "OTHER_ATTRIBUTES, or to a rule of `_widening` with a narrowing and a "
        "widening case, or to HELP."
    )


def test_a_failure_names_the_path_and_both_values():
    then, now = _case_copies()
    _rule("type", _has("min"), {"min": 2}, {"min": 3})(then, now)
    message = _report(_compare(then, now))
    assert "desflow run:replications.type.min: 1.0 2, now 3" in message
    assert "1.x may only widen the command line" in message
    assert "every difference from them must be a listed widening" in message
    assert "Command Line" in message and "Compatibility Policy" in message


# Upkeep: a Click or Typer release that renames or moves a declared attribute or class.


def _gone(then: Dict[str, Any], now: Dict[str, Any]) -> None:
    del now["commands"]["run"]["params"]["until"]["attrs"]["is_eager"]


def _renamed(then: Dict[str, Any], now: Dict[str, Any]) -> None:
    attrs = now["commands"]["run"]["params"]["until"]["attrs"]
    attrs["is_eager_now"] = attrs.pop("is_eager")


def _moved(then: Dict[str, Any], now: Dict[str, Any]) -> None:
    node = now["commands"]["run"]["params"]["until"]["type"]
    node["class"] = "click.types." + node["class"].rpartition(".")[2]


def _callback_renamed(then: Dict[str, Any], now: Dict[str, Any]) -> None:
    now["commands"]["run"]["attrs"]["callback"] = {"object": "_renamed"}


UPKEEP_CASES = {
    "an attribute is gone": (_gone, "desflow run:until.is_eager: attribute is gone"),
    "an attribute is renamed": (
        _renamed,
        "desflow run:until.is_eager: attribute is gone",
    ),
    "a class moves to another module": (_moved, "the module changed"),
}


@pytest.mark.parametrize("name", UPKEEP_CASES)
def test_a_release_change_fails_with_what_to_do(name):
    mutate, expected = UPKEEP_CASES[name]
    then, now = _case_copies()
    mutate(then, now)
    message = "\n".join(_compare(then, now))
    assert expected in message, name
    assert f"Typer {typer.__version__} is installed" in message
    assert "release notes of Click and Typer" in message
    assert "the replay in this test shows it" in message
    assert "TYPER_CHANGES" in message and "Never regenerate" in message


UPKEEP_ENTRIES = {
    "an attribute is gone": (
        _gone,
        {"attr:param.is_eager": ("", "Typer 9", "the attribute is gone")},
    ),
    "an attribute is renamed": (
        _renamed,
        {"attr:desflow run:until.is_eager": ("is_eager_now", "Typer 9", "renamed")},
    ),
    "a class moves to another module": (
        _moved,
        {
            "class:typer._click.types.StringParamType": (
                "click.types.StringParamType",
                "Typer 9",
                "moved back to Click",
            )
        },
    ),
    "a desflow callback is renamed": (
        _callback_renamed,
        {
            "value:desflow run.callback": (
                '{"object": "_renamed"}',
                "desflow",
                "renamed",
            )
        },
    ),
}


@pytest.mark.parametrize("name", UPKEEP_ENTRIES)
def test_a_recorded_change_passes(name, monkeypatch):
    mutate, entries = UPKEEP_ENTRIES[name]
    then, now = _case_copies()
    mutate(then, now)
    assert _compare(then, now) != [], name
    for key, entry in entries.items():
        monkeypatch.setitem(TYPER_CHANGES, key, entry)
    assert _compare(then, now) == [], name


def test_a_recorded_rename_must_hold_the_old_value(monkeypatch):
    then, now = _case_copies()
    _renamed(then, now)
    now["commands"]["run"]["params"]["until"]["attrs"]["is_eager_now"] = "other"
    monkeypatch.setitem(
        TYPER_CHANGES, "attr:param.is_eager", ("is_eager_now", "Typer 9", "renamed")
    )
    assert any("TYPER_CHANGES says" in p for p in _compare(then, now))
