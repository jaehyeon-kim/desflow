from __future__ import annotations

import bisect
import itertools
import math
from typing import Any, ClassVar, List, Optional, Tuple, Union

from simpy import Environment, Event
from simpy.core import BoundClass
from simpy.resources.resource import Preempted

from desflow.config import DEFAULT_QUEUE_ORDER, QUEUE_ORDERS
from desflow.resources.base import BaseDynamicResource


class QueueFull(RuntimeError):
    """A request was refused because its resource's waiting line is full.

    It is a `RuntimeError` with SimPy's message, as `SortedQueue(maxlen)` raises.
    """


def _whole(value: float) -> Union[int, float]:
    """A capacity as a whole number; infinity, a resource with no limit, stays."""
    return value if math.isinf(value) else int(value)


class DynamicResourceRequest(Event):
    """
    A request for one unit of a `DynamicResource`, and the event of its grant.

    It succeeds when a unit is granted. Used in a `with` block, leaving the block
    cancels it while it waits and releases the unit once it is granted, as a SimPy
    `Request` does.

    Attributes:
        resource (ResourceBase): The resource it asks.
        priority (float): Its rank under the `priority` queue order, lowest first.
        preempt (bool): Whether it may take a unit from a user of a higher priority
            value, on a resource built with `preempt=True`.
        key (tuple): `(priority, time, not preempt)`, the order SimPy ranks users by.
        time (float): The simulation time the request was made.
        usage_since (Optional[float]): The simulation time of the grant, or None.
        proc: The process that made the request, the one an interrupt would reach.
    """

    def __init__(
        self, resource: ResourceBase, priority: float = 0, preempt: bool = True
    ):
        super().__init__(resource.env)
        self.resource = resource
        self.priority = priority
        self.preempt = preempt
        self.time = resource.env.now
        # SimPy's PreemptiveResource ranks users by this key: a lower priority
        # value, then an earlier request, then one that may preempt
        self.key = (priority, self.time, not preempt)
        self.usage_since: Optional[float] = None
        self.proc = resource.env.active_process
        resource._enqueue(self)

    def cancel(self) -> None:
        """Leaves the queue at once while waiting; does nothing once granted.

        A granted unit is given back with `DynamicResource.release(request)`, as in
        SimPy, where `cancel` never releases a unit either.
        """
        if not self.triggered:
            self.resource._remove(self)

    def __enter__(self) -> DynamicResourceRequest:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.cancel()
        # SimPy releases on every exit except a generator being closed, which only
        # happens when a finished run is garbage collected.
        if exc_type is not GeneratorExit:
            self.resource.release(self)


class DynamicRelease(Event):
    """
    The release of a unit of a `ResourceBase`, as SimPy's `Release` event.

    It is triggered when it is made, because a release is served at once. The
    resource then tries the head of its queue one event later, when this event is
    processed. A subclass of this class, set as `release = BoundClass(MyRelease)` on
    a `ResourceBase` subclass, replaces it.

    Attributes:
        resource (ResourceBase): The resource it gives a unit back to.
        request (Optional[DynamicResourceRequest]): The request it releases. A release
            made with no request holds the oldest user, or None when nobody holds a unit.
        proc: The process that made the release.
    """

    def __init__(
        self,
        resource: ResourceBase,
        request: Optional[DynamicResourceRequest] = None,
    ):
        super().__init__(resource.env)
        self.resource = resource
        if request is None and resource.users:
            request = resource.users[0]
        self.request = request
        self.proc = resource.env.active_process

        resource.get_queue.append(self)
        self.callbacks.append(resource._trigger_put)
        resource._trigger_get(None)


def _default_put_queue(resource_class: Any) -> bool:
    """Whether a resource class keeps the default queue, the one `queue_limit` limits."""
    put_queue = resource_class.PutQueue
    return isinstance(put_queue, type) and issubclass(put_queue, _RequestQueue)


class _RequestQueue(list):
    """The waiting requests of a resource, in service order.

    It is a list, so `len(resource.queue)` is how many wait and `queue[0]` is the
    next to be served. `append` inserts a request by the rank its resource gave it,
    and raises `RuntimeError` when `maxlen` requests wait already, as SimPy's
    `SortedQueue` does.

    Args:
        maxlen: The most requests that can wait, or None for no limit.
    """

    def __init__(self, maxlen: Optional[int] = None) -> None:
        super().__init__()
        self.maxlen = maxlen

    def append(self, request: DynamicResourceRequest) -> None:
        """Inserts a request by its rank; raises `RuntimeError` if the queue is full."""
        if self.maxlen is not None and len(self) >= self.maxlen:
            raise RuntimeError("Cannot append event. Queue is full.")
        bisect.insort(self, request, key=lambda waiting: waiting._rank)

    def remove(self, request: DynamicResourceRequest) -> bool:  # type: ignore[override]
        """Takes a request out; returns False, and does nothing, if it is not here."""
        for index, waiting in enumerate(self):
            if waiting is request:
                del self[index]
                return True
        return False


class ResourceBase(BaseDynamicResource):
    """
    A resource of units that tasks request, with a capacity that follows the registry.

    It is `DynamicResource` built on SimPy's hooks, so a subclass changes it as a
    subclass of SimPy's `Resource` does: set `PutQueue` or `GetQueue`, set `request`
    or `release` to a `BoundClass` of a request or release class, or override
    `_do_put(request)` and `_do_get(release)`. Use a subclass with `type=` in
    `add_resource` or `type: !python module.Class` in a blueprint.

    The constructor is `DynamicResource`'s, because a resource reads its capacity
    from the registry. A subclass that defines `__init__` takes `*args, **kwargs` and
    passes them on.

    Grants follow SimPy's `Resource`: a request is granted when it is made if a unit
    is free and nothing waits ahead of it, and a release grants the head of the
    queue one event later, when its release event is processed; a release that
    gives back nothing tries the head too. Equal-time grants therefore happen in
    the order SimPy gives them.

    Waiting requests are served in the resource's queue order: `fifo` (the
    default) serves the oldest first, `lifo` the newest first, and `priority` by
    the request's key, `(priority, time, not preempt)`, as SimPy's
    `PriorityResource` does: the lowest `priority` value first, then the earliest,
    then one that may preempt. A sequence number breaks the remaining ties, so
    such requests are served first in, first out.

    Attributes:
        env (DynamicRealtimeEnvironment): The active simulation environment.
        sim_id (str): The prefix ID (e.g., 'Shop').
        resource_id (str): The specific resource name (e.g., 'packers').
        queue_order (str): `fifo`, `lifo` or `priority`.
        PutQueue: The type of `put_queue`, `_RequestQueue` by default.
        GetQueue: The type of `get_queue`, `list` by default.
        put_queue: The waiting requests in service order.
        queue: An alias of `put_queue`; `len(queue)` is how many wait.
        get_queue: The releases being served, empty between events.
        users (List[DynamicResourceRequest]): The granted requests, oldest first.
        count: How many users there are; the same as `in_use`.
        request: Makes a `DynamicResourceRequest`; a `BoundClass`.
        release: Makes a `DynamicRelease`; a `BoundClass`.
    """

    PutQueue: ClassVar[Any] = _RequestQueue
    """The type of `put_queue`: `_RequestQueue`, a list ranked by queue order."""
    GetQueue: ClassVar[Any] = list
    """The type of `get_queue`."""
    request = BoundClass(DynamicResourceRequest)
    """Requests one unit: `request(priority=0, preempt=True)` returns a
    `DynamicResourceRequest`, an event that succeeds when a unit is granted. Lower
    `priority` values are served first under `queue="priority"`; `preempt` lets the
    request take a unit from a user of a higher value on a resource built with
    `preempt=True`. Set it to `BoundClass(MyRequest)` to use a subclass."""
    release = BoundClass(DynamicRelease)
    """Gives a granted unit back: `release(request)` returns a `DynamicRelease`,
    triggered at once. Releasing a request twice, or one never granted, gives back
    nothing; the queue is still tried when the release is processed. With no request,
    the oldest unit held is given back. While units are busy above a lowered
    capacity, a release brings the busy count down and grants nothing until it is
    within the capacity. Set it to `BoundClass(MyRelease)` to use a subclass."""

    def __init__(
        self,
        env: Environment,
        sim_id: str,
        resource_id: str,
        queue: str = DEFAULT_QUEUE_ORDER,
        preempt: bool = False,
        queue_limit: Optional[int] = None,
    ):
        """
        Initializes the resource and binds it to the registry.

        Args:
            env (Environment): The SimPy environment (must include the RegistryMixIn).
            sim_id (str): The parent simulation ID.
            resource_id (str): The name of the resource.
            queue (str): The queue order: `fifo`, `lifo` or `priority`. Defaults to
                `fifo`. A request's `priority` counts only under `priority`.
            preempt (bool): When True, a request that finds every unit busy takes
                one from the user SimPy's `PreemptiveResource` would choose: the one
                whose key `(priority, request time, not preempt)` sorts last, if that
                key sorts after the request's. That user's process receives
                `simpy.Interrupt` with a `Preempted` cause. Needs `queue="priority"`.
            queue_limit (Optional[int]): The most requests that may wait, or None for
                no limit. A request made while that many wait raises `QueueFull`, a
                `RuntimeError`, as SimPy's `SortedQueue(maxlen)` does, before it is
                granted or preempts a user. So 0 refuses every request.

        Raises:
            ValueError: If `queue` is not one of the three orders, `preempt` is set
                without `queue="priority"`, or `queue_limit` is negative or is set on a
                class whose `PutQueue` is not the default.
        """
        if queue not in QUEUE_ORDERS:
            raise ValueError(
                f"queue must be one of {', '.join(QUEUE_ORDERS)}, got '{queue}'"
            )
        if preempt and queue != "priority":
            raise ValueError(
                "preempt ranks users by priority, so it needs queue='priority'"
            )
        check_resource_type(type(self), queue_limit)
        self.preempt = preempt
        self.queue_order = queue
        self.queue_limit = queue_limit
        # Counts requests, so equal ranks keep their arrival order
        self._sequence = itertools.count()
        self._env = env
        self.put_queue = (
            self.PutQueue(maxlen=queue_limit)
            if queue_limit is not None
            else self.PutQueue()
        )
        self.queue = self.put_queue
        self.get_queue = self.GetQueue()
        self.users: List[DynamicResourceRequest] = []
        # The down state of a breakdown: no unit is granted while it is down
        self.down = False
        self.failures = 0
        self.has_breakdowns = False
        self._down_since: Optional[float] = None
        self._downtime = 0.0
        # The registry values `_target` last read and its result, so a grant that
        # finds both values unchanged skips the conversion
        self._target_read: Tuple[Any, Any] = (None, None)
        self._target_value: Union[int, float] = 0
        super().__init__(env, sim_id, resource_id, "resources")
        BoundClass.bind_early(self)

        self._capacity = self._target()
        self._register()

    @property
    def capacity(self) -> Union[int, float]:
        """
        The current active capacity of the resource.

        This value reflects the live state from the Registry and may change
        during the simulation.
        """
        return self._capacity

    @property
    def in_use(self) -> int:
        """Units currently held by tasks, including units above a lowered capacity."""
        return len(self.users)

    @property
    def count(self) -> int:
        """Number of users currently using the resource, as SimPy's `Resource.count`;
        the same as `in_use`."""
        return len(self.users)

    @property
    def _owed(self) -> Union[int, float]:
        """Units busy above a lowered capacity, given back as their tasks finish."""
        return max(0, len(self.users) - self._capacity)

    def _rank(self, request: DynamicResourceRequest) -> Tuple[Any, ...]:
        """The sort key of a new request under this resource's queue order."""
        sequence = next(self._sequence)
        if self.preempt or self.queue_order == "priority":
            # SimPy's PriorityResource and PreemptiveResource sort their queue by
            # the request's key, (priority, time, not preempt): among equal
            # priorities and times a request that may preempt goes first
            return (*request.key, sequence)
        if self.queue_order == "lifo":
            return (0, -sequence)
        return (0, sequence)

    def _enqueue(self, request: DynamicResourceRequest) -> None:
        """Queues a new request, then grants the head if a unit is free, as SimPy does."""
        request._rank = self._rank(request)
        try:
            self.put_queue.append(request)
        except RuntimeError as error:
            raise QueueFull(*error.args) from None
        self._trigger_put(None)
        self._changed()

    def _remove(self, request: DynamicResourceRequest) -> None:
        """Takes a waiting request out of the queue, so it is never granted."""
        try:
            removed = self.put_queue.remove(request)
        except ValueError:
            return
        # A list's remove returns None; the default queue returns whether it removed one
        if removed is not False:
            self._changed()

    def _trigger_put(self, get_event: Optional[Event]) -> None:
        """Tries the head of the queue, as SimPy's `_trigger_put`.

        Called when a request is made and when a release is processed. `_do_put`
        returns None, so each call stops after the first request it tries.
        """
        # The registry may hold a capacity its watcher has not applied yet, such as
        # one set earlier in this same step, so a grant always reads it.
        self._capacity = self._target()
        idx = 0
        while idx < len(self.put_queue):
            put_event = self.put_queue[idx]
            proceed = self._do_put(put_event)
            if not put_event.triggered:
                idx += 1
            elif self.put_queue.pop(idx) != put_event:
                raise RuntimeError("Put queue invariant violated")
            else:
                # The granted request has left the queue, so the readings are taken now
                self._changed()
            if not proceed:
                break

    def _trigger_get(self, put_event: Optional[Event]) -> None:
        """Serves the releases in `get_queue`, as SimPy's `_trigger_get`."""
        idx = 0
        while idx < len(self.get_queue):
            get_event = self.get_queue[idx]
            proceed = self._do_get(get_event)
            if not get_event.triggered:
                idx += 1
            elif self.get_queue.pop(idx) != get_event:
                raise RuntimeError("Get queue invariant violated")
            if not proceed:
                break

    def _do_put(self, request: DynamicResourceRequest) -> Optional[bool]:
        """Grants `request` if a unit is free, or one can be taken from a user.

        On a resource built with `preempt=True`, a request that may preempt takes a
        unit from the user SimPy's `PreemptiveResource` would choose.
        """
        if len(self.users) >= self._capacity and not self._preempt_for(request):
            return None
        self.users.append(request)
        request.usage_since = self.env.now
        request.succeed()
        return None

    def _do_get(self, release: DynamicRelease) -> Optional[bool]:
        """Gives the released request's unit back, if it holds one."""
        request = release.request
        if request is not None and request in self.users:
            self.users.remove(request)
            self._changed()
        release.succeed()
        return None

    def _grant_all(self) -> None:
        """Grants waiting requests while a unit is free: a capacity rise or a repair
        frees several at once, and a pass grants one."""
        while True:
            waiting = len(self.put_queue)
            self._trigger_put(None)
            if len(self.put_queue) >= waiting:
                return

    def _preempt_for(self, request: DynamicResourceRequest) -> bool:
        """Takes a unit for `request` from the user SimPy would preempt, if any.

        Returns:
            bool: True when a unit was freed for it.
        """
        if not (self.preempt and request.preempt):
            return False
        if not self.users or len(self.users) > self._capacity:
            # Busy units above a lowered capacity are owed, not preemptible
            return False
        # SimPy 4.1.2 takes `max`, the first of equal keys in grant order; 4.1.1
        # took the last, `sorted(...)[-1]`
        victim = max(self.users, key=lambda user: user.key)
        if not victim.key > request.key:
            return False
        self.users.remove(victim)
        if victim.proc is not None and victim.proc.is_alive:
            victim.proc.interrupt(
                Preempted(
                    by=request.proc, usage_since=victim.usage_since, resource=self
                )
            )
        return True

    def _target(self) -> Union[int, float]:
        """The registry's capacity, bound to [0, max_cap], and 0 while it is down."""
        if self.down:
            return 0
        current, max_cap = self._current_cap_val.value, self._max_cap_val.value
        read = self._target_read
        if current is read[0] and max_cap is read[1]:
            return self._target_value
        self._target_read = (current, max_cap)
        self._target_value = max(0, min(_whole(current), _whole(max_cap)))
        return self._target_value

    @property
    def downtime(self) -> float:
        """Seconds the resource has been down so far, the current repair included."""
        if self._down_since is not None:
            return self._downtime + self.env.now - self._down_since
        return self._downtime

    def fail(self, interrupt_work: bool = True) -> int:
        """
        Puts the resource down: its capacity is 0 until `repair()`.

        `current_cap` in the registry keeps the planned capacity, so a change during
        the repair sets the capacity the resource returns to.

        Args:
            interrupt_work: True interrupts every task being served, with the cause
                `breakdown`; False lets them finish, and their units are not handed
                on until the repair.

        Returns:
            int: How many tasks were interrupted.
        """
        self.down = True
        self.failures += 1
        self._down_since = self.env.now
        self._capacity = 0
        self._changed()
        interrupted = 0
        if interrupt_work:
            for user in list(self.users):
                if user.proc is not None and user.proc.is_alive:
                    user.proc.interrupt("breakdown")
                    interrupted += 1
        return interrupted

    def repair(self) -> None:
        """Ends the down state: the capacity returns to `current_cap` in the registry
        and waiting requests are granted."""
        if self._down_since is not None:
            self._downtime += self.env.now - self._down_since
        self.down, self._down_since = False, None
        self._grant_all()
        self._changed()

    def _handle_capacity_change(self, new_target: float):
        self._capacity = self._target()
        # A rise grants waiting requests at once. A fall stops nothing: busy units
        # above the new capacity are given back as their tasks finish, and no
        # request is granted until the busy count is within it.
        self._grant_all()
        self._changed()


def check_resource_type(
    resource_type: Optional[Any], queue_limit: Optional[int] = None
) -> None:
    """Checks the `type` and `queue_limit` a resource is declared with.

    Args:
        resource_type: A subclass of `ResourceBase`, or None for `DynamicResource`.
        queue_limit: The most requests that may wait, or None.

    Raises:
        ValueError: If `resource_type` is not a subclass of `ResourceBase`,
            `queue_limit` is negative, or `queue_limit` is set on a type whose
            `PutQueue` is not the default.
    """
    if resource_type is not None and not (
        isinstance(resource_type, type) and issubclass(resource_type, ResourceBase)
    ):
        raise ValueError(
            f"type must be a subclass of ResourceBase, got {resource_type!r}"
        )
    if queue_limit is not None:
        if queue_limit < 0:
            raise ValueError(f"queue_limit must be 0 or more, got {queue_limit}")
        if not _default_put_queue(resource_type or ResourceBase):
            raise ValueError(
                "queue_limit needs the default PutQueue; a custom PutQueue sets its "
                "own maxlen"
            )


class DynamicResource(ResourceBase):
    """
    A SimPy Resource with a dynamically adjustable capacity.

    Unlike a standard `simpy.Resource` where capacity is fixed at creation,
    the `DynamicResource` listens to a `SimulationRegistry` path (e.g., 'Shop.resources.packers.current_cap').
    When the registry updates the capacity, the resource adjusts at once, granting
    waiting requests if the capacity rises. It is `ResourceBase` as built, and the
    class to subclass for a resource of your own is `ResourceBase`.
    """
