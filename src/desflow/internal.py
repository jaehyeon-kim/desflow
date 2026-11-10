"""Events desflow creates for its own work, marked when they are created.

The environment keeps SimPy's one queue, so a model event and one of desflow's own
(a flush timer, a statistics reading, a capacity watcher) are ordered together and
`run()` processes both. The public `step()` and `peek()` skip the second kind, as
`DynamicRealtimeEnvironment` sets out, so each event carries `_desflow_internal`.

The mark is set when the event is made, because many of these events are scheduled
later, from a model event's callbacks, outside any code of desflow's own: a store's
get is granted while a put is processed, and the run's end is scheduled by a
callback. A marked class is therefore the only reliable place for the mark.

A plain `simpy.Environment` ignores the marks, so a resource, store or container
built on one works as before.
"""

from typing import Any, Optional

from simpy import Event, Process, Store, Timeout
from simpy.core import BoundClass
from simpy.resources.store import StoreGet, StorePut


def is_internal(event: Any) -> bool:
    """Whether `event` is one of desflow's own, made through an internal class.

    Args:
        event: Any event in the queue.

    Returns:
        bool: True when the public `step()` and `peek()` do not count it.
    """
    return getattr(event, "_desflow_internal", False)


class InternalEvent(Event):
    """An event for desflow's own use, such as the end of a run."""

    _desflow_internal = True


class InternalTimeout(Timeout):
    """A timeout for desflow's own use, such as the end of a warm-up."""

    _desflow_internal = True


class InternalProcess(Process):
    """A process for desflow's own use, such as a flush timer.

    Its completion event is internal because the process is itself the event. Its
    `Initialize` event is made and scheduled inside SimPy's constructor, so it is
    marked once that returns, before any event is processed.
    """

    _desflow_internal = True

    def __init__(self, env: Any, generator: Any):
        super().__init__(env, generator)
        self._target._desflow_internal = True  # type: ignore[attr-defined]


class _InternalStorePut(StorePut):
    _desflow_internal = True


class _InternalStoreGet(StoreGet):
    _desflow_internal = True


class InternalStore(Store):
    """A store whose puts and gets are internal, so a registry change that wakes a
    watcher schedules internal events only."""

    put = BoundClass(_InternalStorePut)
    get = BoundClass(_InternalStoreGet)


def internal_process(env: Any, generator: Any) -> Process:
    """Starts `generator` as a process desflow owns, on any environment.

    Args:
        env: A `DynamicRealtimeEnvironment`, or a plain SimPy environment, where the
            process is an ordinary one.
        generator: The process's generator.

    Returns:
        simpy.Process: The started process.
    """
    start: Optional[Any] = getattr(env, "start_internal", None)
    return start(generator) if start is not None else env.process(generator)
