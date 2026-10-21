"""Resource types for the Q8 conformance test, each beside the SimPy subclass it copies.

A blueprint names the desflow class with `type: !python`. The SimPy twin of each is
in the test, so one scenario runs on both and the traces compare.
"""

import functools
from typing import Any, List, Optional, Tuple

import simpy
from simpy.core import BoundClass
from simpy.resources.resource import Preempted, Request, SortedQueue

from desflow import ResourceBase
from desflow.resources.resource import DynamicResourceRequest


class MonitoredResource(ResourceBase):
    """`MonitoredResource` of SimPy's monitoring guide: it logs the time and the
    length of the queue at every request and release."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.data: List[Tuple[float, int]] = []

    def request(self, *args: Any, **kwargs: Any) -> Any:
        self.data.append((self._env.now, len(self.queue)))
        return super().request(*args, **kwargs)

    def release(self, *args: Any, **kwargs: Any) -> Any:
        self.data.append((self._env.now, len(self.queue)))
        return super().release(*args, **kwargs)


class SimPyMonitoredResource(simpy.Resource):
    """The SimPy original of `MonitoredResource`."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.data: List[Tuple[float, int]] = []

    def request(self, *args: Any, **kwargs: Any) -> Any:
        self.data.append((self._env.now, len(self.queue)))
        return super().request(*args, **kwargs)

    def release(self, *args: Any, **kwargs: Any) -> Any:
        self.data.append((self._env.now, len(self.queue)))
        return super().release(*args, **kwargs)


class NewestUserLoses(ResourceBase):
    """A preemptive resource that takes the unit of the newest user, not the one with
    the highest key, when the request's priority value is lower than that user's."""

    def _do_put(self, request: DynamicResourceRequest) -> Optional[bool]:
        if len(self.users) >= self.capacity and request.preempt:
            victim = max(self.users, key=lambda user: (user.usage_since, user.key))
            if victim.key[0] > request.key[0]:
                self.users.remove(victim)
                self._changed()
                victim.proc.interrupt(
                    Preempted(
                        by=request.proc, usage_since=victim.usage_since, resource=self
                    )
                )
        return super()._do_put(request)


class SimPyNewestUserLoses(simpy.PreemptiveResource):
    """The SimPy original of `NewestUserLoses`."""

    def _do_put(self, event: Any) -> None:
        if len(self.users) >= self.capacity and event.preempt:
            victim = max(self.users, key=lambda user: (user.usage_since, user.key))
            if victim.key[0] > event.key[0]:
                self.users.remove(victim)
                victim.proc.interrupt(
                    Preempted(
                        by=event.proc, usage_since=victim.usage_since, resource=self
                    )
                )
        return simpy.Resource._do_put(self, event)


class TwoWaiting(ResourceBase):
    """A priority resource whose waiting line holds two requests."""

    PutQueue = staticmethod(functools.partial(SortedQueue, maxlen=2))


class SimPyTwoWaiting(simpy.PriorityResource):
    """The SimPy original of `TwoWaiting`."""

    PutQueue = staticmethod(functools.partial(SortedQueue, maxlen=2))


class StampedRequest(DynamicResourceRequest):
    """A request that remembers the time it was made."""

    def __init__(self, resource: Any, *args: Any, **kwargs: Any):
        self.stamp = resource.env.now
        super().__init__(resource, *args, **kwargs)


class Stamping(ResourceBase):
    """A resource that logs `(stamp, grant time)` of every request it grants."""

    request = BoundClass(StampedRequest)

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.stamps: List[Tuple[float, float]] = []

    def _do_put(self, request: Any) -> None:
        super()._do_put(request)
        if request.triggered:
            self.stamps.append((request.stamp, self._env.now))


class SimPyStampedRequest(Request):
    """The SimPy original of `StampedRequest`."""

    def __init__(self, resource: Any):
        self.stamp = resource._env.now
        super().__init__(resource)


class SimPyStamping(simpy.Resource):
    """The SimPy original of `Stamping`."""

    request = BoundClass(SimPyStampedRequest)

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.stamps: List[Tuple[float, float]] = []

    def _do_put(self, event: Any) -> None:
        super()._do_put(event)
        if event.triggered:
            self.stamps.append((event.stamp, self._env.now))
