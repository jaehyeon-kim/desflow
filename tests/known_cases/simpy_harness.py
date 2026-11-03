"""The small harness of the plain SimPy programs in the `original.py` files.

A program builds its model on a SimPy environment from `new`, logs what happens with
the `L` that `new` returns, starts its entities with `arrival` and ends with `go`.
`arrival` starts `initial` entities at once, in order, and then one every `gap`, so a
program has the entity keys `<name>-1`, `<name>-2` that a blueprint's arrival gives.
The test sets `MODE` to run a program at a size: the arrivals are then collected and
started when `go` runs, and the first with `initial` takes the size.
"""

from collections.abc import Callable
from typing import Any

import time

import simpy
import simpy.rt

WALL = [0.0]
ERRT: list[float | None] = [None]  # simulated time of the error that ended the run
VARS: dict[str, Any] = {}  # final values of the variables a program reads
PROCS: dict[str, Any] = {}  # entity key -> its process, filled by `arrival`
TELE: list[tuple[Any, ...]] = []
TRACE: list[tuple[Any, ...]] = []
PENDING: list[Any] = []
MODE: dict[str, Any] = {"on": False, "n": None, "rev": False}


def S(status: str, **fields: Any) -> dict[str, Any]:
    """A record value that carries a status."""
    return dict(status=status, **fields)


def new(t0: float = 0, rt: float | None = None) -> tuple[Any, list[Any], Callable]:
    """A SimPy environment (a real-time one when `rt` is a factor), its log and the
    function that adds a record to the log: (time, key, task, value)."""
    ERRT[0] = None
    VARS.clear()
    TELE.clear()
    TRACE.clear()
    PENDING.clear()
    PROCS.clear()
    env: Any = (
        simpy.rt.RealtimeEnvironment(t0, factor=rt, strict=True)
        if rt
        else simpy.Environment(t0)
    )
    log: list[Any] = []

    def L(key: Any, task: str, payload: Any) -> None:
        log.append((round(env.now, 6), key, task, payload))

    return env, log, L


def flush() -> None:
    """Starts the collected arrivals, the first with `initial` at the size."""
    if not MODE["on"]:
        return
    items = list(reversed(PENDING)) if MODE["rev"] else list(PENDING)
    PENDING.clear()
    sized = False
    for item in items:
        if item[0] == "call":
            item[1]()  # a process that `start` collected
            continue
        item = list(item)
        if MODE["n"] is not None and not sized and item[3]:
            item[3] = MODE["n"]
            sized = True
        _arrival(*item)


def start(env: Any, function: Callable[[], Any]) -> None:
    """Starts the process `function()` makes; a sized run starts it with the arrivals."""
    if MODE["on"]:
        PENDING.append(("call", function))
        return
    function()


def go(env: Any, log: list[Any], until: Any = None, expect_error: bool = False) -> Any:
    """Runs the environment; an error it ends with is a result of the program."""
    flush()
    error = None
    began = time.perf_counter()
    try:
        env.run(until=until)
    except TimeoutError:
        raise  # the test's own limit on a case that does not end
    except BaseException as exc:  # the program's own error is a result
        error = f"{type(exc).__name__}: {str(exc)[:300]}"
        ERRT[0] = float(env.now)
    WALL[0] = time.perf_counter() - began
    return log, error


def arrival(
    env: Any,
    name: str,
    make: Callable[[str], Any],
    initial: int = 0,
    gap: Any = None,
    max_events: int | None = None,
    first: float | None = None,
    keyfn: Callable[[], str] | None = None,
    gate: Callable[[], bool] | None = None,
) -> None:
    """Starts `initial` entities at once, then one every `gap`; `make(key)` returns the
    entity's process. A sized run collects the call and starts it in `go`."""
    if MODE["on"]:
        PENDING.append((env, name, make, initial, gap, max_events, first, keyfn, gate))
        return
    _arrival(env, name, make, initial, gap, max_events, first, keyfn, gate)


def _arrival(
    env: Any,
    name: str,
    make: Callable[[str], Any],
    initial: int = 0,
    gap: Any = None,
    max_events: int | None = None,
    first: float | None = None,
    keyfn: Callable[[], str] | None = None,
    gate: Callable[[], bool] | None = None,
) -> None:
    count = [0]

    def key() -> str:
        count[0] += 1
        return f"{name}-{count[0]}" if keyfn is None else keyfn()

    def begin(label: str) -> None:
        if gate is not None and not gate():
            return  # an arrival before its lookups have a row is dropped
        PROCS[label] = env.process(make(label))

    for _ in range(initial):
        begin(key())
    if gap is not None:

        def source() -> Any:
            made = 0
            if first is not None and first > 0:
                yield env.timeout(first)
            while max_events is None or made < max_events:
                if not (first is not None and made == 0):
                    yield env.timeout(gap() if callable(gap) else gap)
                made += 1
                begin(key())

        env.process(source())


def set_capacity(resource: Any, n: int) -> None:
    """SimPy has no `set_capacity`; this is the SimPy 3 idiom: change the capacity and
    wake the queue."""
    resource._capacity = n
    resource._trigger_put(None)


def watch_resource(env: Any, resource: Any) -> None:
    """The monitoring guide's patch: record (now, count, queue length) after each
    request and release call."""
    for name in ("request", "release"):
        original = getattr(resource, name)

        def wrapped(*args: Any, _original: Any = original, **kwargs: Any) -> Any:
            event = _original(*args, **kwargs)
            TELE.append((float(env.now), len(resource.users), len(resource.queue)))
            return event

        setattr(resource, name, wrapped)


def trace_steps(env: Any) -> None:
    """The monitoring guide's patched step: record each event before its callbacks run."""
    original = env.step

    def step() -> Any:
        if env._queue:
            time, priority, _id, event = env._queue[0]
            TRACE.append((float(time), priority, type(event).__name__))
        return original()

    env.step = step
