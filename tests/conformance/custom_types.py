"""Store and container types for the C5 custom-type test, each beside the SimPy class
it copies.

A blueprint names the desflow class with `type: !python`. Each rule is a mixin shared
by the SimPy class and the desflow class, so one scenario runs on both and the traces
compare.
"""

from typing import Any

import simpy
from simpy.core import BoundClass
from simpy.resources.store import StorePut

from desflow import ContainerBase, StoreBase
from desflow.resources.store import DynamicStorePut


class LastInFirstOut:
    """A `_do_get` that takes the newest item. It does not call `super()`."""

    def _do_get(self, event: Any) -> None:
        if self.items:  # type: ignore[attr-defined]
            event.succeed(self.items.pop())  # type: ignore[attr-defined]


class SkipBlockedGets:
    """A container `_do_get` that returns True when the amount is not there, so a
    later get that does fit is served around it. It does not call `super()`."""

    def _do_get(self, event: Any) -> bool:
        if self._level >= event.amount:  # type: ignore[attr-defined]
            self._level -= event.amount  # type: ignore[attr-defined]
            event.succeed()
        return True


class FrontList(list):
    """A waiting line whose `append` puts the newest request first."""

    def append(self, item: Any) -> None:
        self.insert(0, item)


class NewestGetFirst:
    """A store that serves the newest waiting get first."""

    GetQueue = FrontList


class WrongPop(list):
    """A waiting line whose `pop` ignores the index, so SimPy's check finds the wrong
    request."""

    def pop(self, index: Any = -1) -> Any:  # type: ignore[override]
        return super().pop(0)


class BrokenQueue:
    GetQueue = WrongPop


class Dock(LastInFirstOut, StoreBase):
    """A store that gives back the newest item."""


class SimPyDock(LastInFirstOut, simpy.Store):
    """The SimPy original of `Dock`."""


class Tank(SkipBlockedGets, ContainerBase):
    """A container that serves a later get around one that does not fit."""


class SimPyTank(SkipBlockedGets, simpy.Container):
    """The SimPy original of `Tank`."""


class Stack(NewestGetFirst, StoreBase):
    """A store whose waiting gets are served newest first."""


class SimPyStack(NewestGetFirst, simpy.Store):
    """The SimPy original of `Stack`."""


class BrokenTank(BrokenQueue, SkipBlockedGets, ContainerBase):
    """A container with a waiting line that breaks SimPy's invariant."""


class SimPyBrokenTank(BrokenQueue, SkipBlockedGets, simpy.Container):
    """The SimPy original of `BrokenTank`."""


class StampedPut(DynamicStorePut):
    """A put that records when it was made."""

    def __init__(self, resource: Any, item: Any):
        super().__init__(resource, item)
        self.stamp = resource._env.now


class SimPyStampedPut(StorePut):
    """The SimPy original of `StampedPut`."""

    def __init__(self, resource: Any, item: Any):
        super().__init__(resource, item)
        self.stamp = resource._env.now


class StampingDock(StoreBase):
    """A store whose puts carry the time they were made."""

    put = BoundClass(StampedPut)


class SimPyStampingDock(simpy.Store):
    """The SimPy original of `StampingDock`."""

    put = BoundClass(SimPyStampedPut)
