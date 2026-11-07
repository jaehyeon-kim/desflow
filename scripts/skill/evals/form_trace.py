"""A pytest plugin that records which forms of the model each test ran.

`grade.py` runs the agent's tests once with `-p form_trace`, with this folder on
`PYTHONPATH`, so that nothing is added to the run folder. A test ran a form when, during its
setup or its call, during the setup of a fixture whose value it got, or during the import of
its module or of a `conftest.py`:

- Python code of `builder.py` or `low_level.py` ran (a `sys.monitoring` hook on the start of
  each function whose code comes from one of these files; `sys.setprofile` on Python 3.11);
- `model.yaml` was opened (the `open` audit event);
- a process was started whose arguments name `model.yaml`, `builder.py` or `low_level.py`,
  run `-m builder` or `-m low_level`, or give `-c` code that imports `builder` or `low_level`
  (the `subprocess.Popen`, `os.system`, `os.posix_spawn`, `os.exec` and `os.spawn` audit
  events).

A fixture with a wider scope than the test runs once, during the setup of the first test that
needs it; the forms it ran are kept with its value, so each later test that gets the same
value counts them as well. The forms run while a test module is imported count for every test
of that module, and the forms run while a `conftest.py` is imported count for every test. A
process started by `multiprocessing` raises no audit event, so a form run in one is not seen.
At the end of the session the plugin writes a JSON object, from each test id to the sorted
list of forms it ran, to the path in `FORM_TRACE_OUT`.
"""

import json
import os
import re
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

import pytest

FILES = {"model.yaml": "yaml", "builder.py": "builder", "low_level.py": "low_level"}
_PYTHON_FORMS = {"builder.py": "builder", "low_level.py": "low_level"}
# `-m builder`, and code given with `-c` that imports a Python form
_MODULE_RUN = re.compile(r"(?:^|\s)-m\s*(builder|low_level)\b")
_IMPORT = re.compile(
    r"\b(?:import|from)\s+(builder|low_level)\b"
    r"|\b(?:import_module|run_module|__import__)\(\s*[\"'](builder|low_level)[\"']"
)
# The key of the forms run while a `conftest.py` is imported, which count for every test
_EVERY_TEST = ""
_PROCESS_EVENTS = {
    "subprocess.Popen",
    "os.system",
    "os.posix_spawn",
    "os.exec",
    "os.spawn",
}


class _Trace:
    """The forms run in each open phase, and what each test and fixture value ran."""

    def __init__(self, root: Path):
        self.root = root
        self.open_sets: List[Set[str]] = []
        # The collection phases, innermost last: a form counts for the innermost only
        self.collecting: List[Set[str]] = []
        self.tests: Dict[str, Set[str]] = {}
        self.fixtures: Dict[Tuple[str, int, str], Set[str]] = {}
        self.imports: Dict[str, Set[str]] = {}
        # Keeps each fixture value alive, so that its id is not reused
        self.kept: List[Any] = []
        self.lock = threading.Lock()
        self.files: Dict[str, Optional[str]] = {}
        self.started = False

    def active(self) -> bool:
        """True while a test, a fixture or a collection phase is open."""
        return bool(self.open_sets or self.collecting)

    def ran(self, form: str) -> None:
        """Adds a form to every test phase that is open, and to the innermost collection."""
        with self.lock:
            for forms in self.open_sets:
                forms.add(form)
            if self.collecting:
                self.collecting[-1].add(form)

    def close(self, forms: Set[str]) -> None:
        """Closes one phase; sets are compared by identity, as two phases may hold the same forms."""
        with self.lock:
            for index, open_set in enumerate(self.open_sets):
                if open_set is forms:
                    del self.open_sets[index]
                    return

    def python_form(self, filename: str) -> Optional[str]:
        """Returns the form whose file a code object comes from, or None."""
        if filename not in self.files:
            path = Path(filename)
            if not path.is_absolute():
                path = Path.cwd() / path
            name = path.name if path.parent.resolve() == self.root else ""
            self.files[filename] = _PYTHON_FORMS.get(name)
        return self.files[filename]


def _texts(value: Any) -> Iterator[str]:
    """Yields the text of each argument of an audit event, through lists and tuples."""
    if isinstance(value, (list, tuple)):
        for item in value:
            yield from _texts(item)
    elif isinstance(value, bytes):
        yield value.decode("utf-8", "replace")
    elif isinstance(value, (str, os.PathLike)):
        yield os.fsdecode(value)


def _named_forms(texts: Iterator[str]) -> Set[str]:
    """Returns the forms a process runs: by a file name, `-m`, or `-c` code that imports one."""
    parts = list(texts)
    joined = " ".join(parts)
    found = set()
    for name, form in FILES.items():
        if name in joined:
            found.add(form)
    for match in _MODULE_RUN.finditer(joined):
        found.add(match.group(1))
    if "-c" in parts or re.search(r"(?:^|\s)-c\s", joined):
        for match in _IMPORT.finditer(joined):
            found.add(match.group(1) or match.group(2))
    return found


def _audit(trace: _Trace, event: str, args: tuple) -> None:
    """Records a form from an `open` or a process audit event."""
    if not trace.active():
        return
    if event == "open" and args and isinstance(args[0], (str, bytes, os.PathLike)):
        if Path(os.fsdecode(args[0])).name == "model.yaml":
            trace.ran("yaml")
    elif event in _PROCESS_EVENTS:
        for form in _named_forms(_texts(args)):
            trace.ran(form)


def _watch_python(trace: _Trace) -> None:
    """Records a Python form each time a function of its file starts."""
    monitoring = getattr(sys, "monitoring", None)
    if monitoring is None:

        def profile(frame: Any, event: str, arg: Any) -> None:
            if event == "call":
                form = trace.python_form(frame.f_code.co_filename)
                if form and trace.active():
                    trace.ran(form)

        sys.setprofile(profile)
        threading.setprofile(profile)
        return
    tool = None
    for ident in range(6):
        if monitoring.get_tool(ident) is None:
            tool = ident  # the last free id: the debugger, coverage and profiler take the low ids
    if tool is None:
        return
    monitoring.use_tool_id(tool, "form_trace")

    def on_start(code: Any, offset: int) -> Any:
        form = trace.python_form(code.co_filename)
        if form is None:
            # Why: code of other files never counts, so its hook is turned off for good
            return monitoring.DISABLE
        if trace.active():
            trace.ran(form)
        return None

    monitoring.register_callback(tool, monitoring.events.PY_START, on_start)
    monitoring.set_events(tool, monitoring.events.PY_START)


_TRACE = _Trace(Path.cwd().resolve())


def _start() -> None:
    """Starts the audit hook and the hook on Python code, once."""
    if not _TRACE.started:
        _TRACE.started = True
        sys.addaudithook(lambda event, args: _audit(_TRACE, event, args))
        _watch_python(_TRACE)


def _collected(key: str) -> Iterator[None]:
    """Records the forms one collection step runs, under `key`."""
    forms: Set[str] = set()
    _TRACE.collecting.append(forms)
    try:
        return (yield)
    finally:
        for index, open_set in enumerate(_TRACE.collecting):
            if open_set is forms:
                del _TRACE.collecting[index]
                break
        _TRACE.imports.setdefault(key, set()).update(forms)


@pytest.hookimpl(wrapper=True)
def pytest_load_initial_conftests(early_config: pytest.Config) -> Iterator[None]:
    """Starts the hooks, and records the forms run while the first `conftest.py` loads.

    Args:
        early_config: The pytest configuration.

    Returns:
        The result of the wrapped hook.
    """
    _start()
    return (yield from _collected(_EVERY_TEST))


def pytest_configure(config: pytest.Config) -> None:
    """Starts the hooks, when the plugin was loaded too late for the first conftest.

    Args:
        config: The pytest configuration.
    """
    _start()


@pytest.hookimpl(wrapper=True)
def pytest_make_collect_report(collector: pytest.Collector) -> Iterator[None]:
    """Records the forms run while a test module, or a `conftest.py` of a folder, is imported.

    Args:
        collector: The node being collected: a module, a folder or the session.

    Returns:
        The result of the wrapped hook.
    """
    key = collector.nodeid if isinstance(collector, pytest.Module) else _EVERY_TEST
    return (yield from _collected(key))


def _fixture_key(name: str, value: Any, param: Any) -> Tuple[str, int, str]:
    """Returns the key of one fixture value: its name, its object and its parameter."""
    return name, id(value), repr(param)


@pytest.hookimpl(wrapper=True)
def pytest_fixture_setup(fixturedef: Any, request: Any) -> Iterator[None]:
    """Keeps the forms a fixture ran with its value.

    Args:
        fixturedef: The fixture's definition.
        request: The fixture request, which holds the parameter of a parametrized fixture.

    Returns:
        The fixture's value.
    """
    forms: Set[str] = set()
    _TRACE.open_sets.append(forms)
    try:
        value = yield
    finally:
        _TRACE.close(forms)
    key = _fixture_key(fixturedef.argname, value, getattr(request, "param", None))
    _TRACE.fixtures.setdefault(key, set()).update(forms)
    _TRACE.kept.append(value)
    return value


def _fixture_forms(item: pytest.Item) -> Set[str]:
    """Returns the forms of every fixture value a test got."""
    params = getattr(getattr(item, "callspec", None), "params", {})
    found: Set[str] = set()
    for name in getattr(item, "fixturenames", []):
        funcargs = getattr(item, "funcargs", {})
        if name in funcargs:
            key = _fixture_key(name, funcargs[name], params.get(name))
            found |= _TRACE.fixtures.get(key, set())
    return found


def _module_forms(item: pytest.Item) -> Set[str]:
    """Returns the forms run while the test's module and every `conftest.py` were imported."""
    module = item.getparent(pytest.Module)
    found = set(_TRACE.imports.get(_EVERY_TEST, set()))
    if module is not None:
        found |= _TRACE.imports.get(module.nodeid, set())
    return found


def _phase(item: pytest.Item) -> Iterator[None]:
    """Records the forms one phase of a test runs."""
    forms = _TRACE.tests.setdefault(item.nodeid, set())
    _TRACE.open_sets.append(forms)
    try:
        return (yield)
    finally:
        _TRACE.close(forms)
        forms |= _fixture_forms(item) | _module_forms(item)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_setup(item: pytest.Item) -> Iterator[None]:
    """Records the forms of a test's setup.

    Args:
        item: The test.

    Returns:
        The result of the wrapped hook.
    """
    return (yield from _phase(item))


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item: pytest.Item) -> Iterator[None]:
    """Records the forms of a test's call.

    Args:
        item: The test.

    Returns:
        The result of the wrapped hook.
    """
    return (yield from _phase(item))


def pytest_sessionfinish(session: pytest.Session) -> None:
    """Writes the forms of each test to `FORM_TRACE_OUT`.

    Args:
        session: The pytest session.
    """
    out = os.environ.get("FORM_TRACE_OUT")
    if out:
        record = {ident: sorted(forms) for ident, forms in _TRACE.tests.items()}
        Path(out).write_text(json.dumps(record, sort_keys=True), "utf-8")
