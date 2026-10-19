from __future__ import annotations

import heapq
import itertools
from typing import Any, List, Optional, Tuple

from simpy import Environment, Event
from simpy.resources.resource import Preempted

from desflow.config import DEFAULT_QUEUE_ORDER, QUEUE_ORDERS
from desflow.resources.base import BaseDynamicResource


class DynamicResourceRequest(Event):
    """
    A request for one unit of a `DynamicResource`, and the event of its grant.

    It succeeds when a unit is granted. Used in a `with` block, leaving the block
    cancels it while it waits and releases the unit once it is granted, as a SimPy
    `Request` does.

    Attributes:
        resource (DynamicResource): The resource it asks.
        priority (float): Its rank under the `priority` queue order, lowest first.
        preempt (bool): Whether it may take a unit from a user of a higher priority
            value, on a resource built with `preempt=True`.
        key (tuple): `(priority, time, not preempt)`, the order SimPy ranks users by.
        usage_since (Optional[float]): The simulation time of the grant, or None.
        proc: The process that made the request, the one an interrupt would reach.
    """

    def __init__(
        self, resource: DynamicResource, priority: float, preempt: bool = True
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


class _RequestQueue:
    """The waiting requests, as a heap ranked by the resource's queue order.

    `items` is the heap itself, so `len(resource.queue.items)` is the number of
    requests waiting, as it is for a SimPy store.
    """

    def __init__(self) -> None:
        self.items: List[Tuple[Tuple[Any, ...], DynamicResourceRequest]] = []

    def push(self, rank: Tuple[Any, ...], request: DynamicResourceRequest) -> None:
        heapq.heappush(self.items, (rank, request))

    def head(self) -> Optional[DynamicResourceRequest]:
        return self.items[0][1] if self.items else None

    def pop(self) -> DynamicResourceRequest:
        return heapq.heappop(self.items)[1]

    def remove(self, request: DynamicResourceRequest) -> bool:
        for index, (_rank, waiting) in enumerate(self.items):
            if waiting is request:
                self.items[index] = self.items[-1]
                self.items.pop()
                heapq.heapify(self.items)
                return True
        return False


class DynamicResource(BaseDynamicResource):
    """
    A SimPy Resource with a dynamically adjustable capacity.

    Unlike a standard `simpy.Resource` where capacity is fixed at creation,
    the `DynamicResource` listens to a `SimulationRegistry` path (e.g., 'Shop.resources.packers.current_cap').
    When the registry updates the capacity, the resource adjusts at once, granting
    waiting requests if the capacity rises.

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
        queue: The waiting requests; `len(queue.items)` is how many wait.
        users (List[DynamicResourceRequest]): The granted requests, oldest first.
    """

    def __init__(
        self,
        env: Environment,
        sim_id: str,
        resource_id: str,
        queue: str = DEFAULT_QUEUE_ORDER,
        preempt: bool = False,
    ):
        """
        Initializes the DynamicResource and binds it to the registry.

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

        Raises:
            ValueError: If `queue` is not one of the three orders, or `preempt` is
                set without `queue="priority"`.
        """
        if queue not in QUEUE_ORDERS:
            raise ValueError(
                f"queue must be one of {', '.join(QUEUE_ORDERS)}, got '{queue}'"
            )
        if preempt and queue != "priority":
            raise ValueError(
                "preempt ranks users by priority, so it needs queue='priority'"
            )
        self.preempt = preempt
        self.queue_order = queue
        # Counts requests, so equal ranks keep their arrival order
        self._sequence = itertools.count()
        self.queue = _RequestQueue()
        self.users: List[DynamicResourceRequest] = []
        # The down state of a breakdown: no unit is granted while it is down
        self.down = False
        self.failures = 0
        self.has_breakdowns = False
        self._down_since: Optional[float] = None
        self._downtime = 0.0
        super().__init__(env, sim_id, resource_id, "resources")

        self._capacity = self._target()
        self._register()

    @property
    def capacity(self) -> int:
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
    def _owed(self) -> int:
        """Units busy above a lowered capacity, given back as their tasks finish."""
        return max(0, len(self.users) - self._capacity)

    def request(
        self, priority: float = 1, preempt: bool = True
    ) -> DynamicResourceRequest:
        """
        Returns a context-manager enabled request.

        Args:
            priority: Lower values are served first when the queue order is
                `priority`; ignored under `fifo` and `lifo`.
            preempt: On a resource built with `preempt=True`, whether this request
                may take a unit from a user of a higher priority value. Ignored
                otherwise. Defaults to True, as in SimPy.

        Returns:
            DynamicResourceRequest: An event that succeeds when a unit is granted.
        """
        return DynamicResourceRequest(self, priority, preempt)

    def release(self, request: Optional[DynamicResourceRequest] = None) -> Event:
        """
        Gives a granted unit back, and tries the head of the queue one event later.

        Releasing a request twice, or one never granted, gives back nothing, as
        SimPy's `Release` does; the queue is still tried when the release event is
        processed, as SimPy tries it after every release. While units are busy above
        a lowered capacity, a release brings the busy count down and grants nothing
        until it is within the capacity.

        Args:
            request: The granted request. None gives back the oldest unit held, the
                form 0.21.0 code without a request uses.

        Returns:
            Event: The release event, already triggered.
        """
        released = self.env.event()
        if request is None and self.users:
            request = self.users[0]
        if request is not None and request in self.users:
            self.users.remove(request)
            self._changed()
        # SimPy tries the head of the queue when any release event is processed,
        # one that gave back no unit too, such as the release of a request that
        # reneged; on a preemptive resource that try can preempt a user
        released.callbacks.append(self._grant_from_release)
        released.succeed()
        return released

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
        self.queue.push(self._rank(request), request)
        self._changed()
        self._grant(limit=1)

    def _remove(self, request: DynamicResourceRequest) -> None:
        """Takes a waiting request out of the queue, so it is never granted."""
        if self.queue.remove(request):
            self._changed()

    def _grant_from_release(self, _event: Event) -> None:
        self._grant(limit=1)

    def _grant(self, limit: Optional[int] = None) -> None:
        """Grants waiting requests, head first, while a unit is free.

        Args:
            limit: The most requests to grant. SimPy grants one per request and one
                per release; a capacity rise grants as many as it frees.
        """
        # The registry may hold a capacity its watcher has not applied yet, such as
        # one set earlier in this same step, so a grant always reads it.
        self._capacity = self._target()
        granted = 0
        while self.queue.items:
            if limit is not None and granted >= limit:
                return
            if len(self.users) >= self._capacity and not self._preempt_for(
                self.queue.head()
            ):
                return
            request = self.queue.pop()
            self.users.append(request)
            request.usage_since = self.env.now
            request.succeed()
            self._changed()
            granted += 1

    def _preempt_for(self, request: Optional[DynamicResourceRequest]) -> bool:
        """Takes a unit for `request` from the user SimPy would preempt, if any.

        Returns:
            bool: True when a unit was freed for it.
        """
        if not (self.preempt and request is not None and request.preempt):
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
        self._changed()
        if victim.proc is not None and victim.proc.is_alive:
            victim.proc.interrupt(
                Preempted(
                    by=request.proc, usage_since=victim.usage_since, resource=self
                )
            )
        return True

    def _target(self) -> int:
        """The registry's capacity, bound to [0, max_cap], and 0 while it is down."""
        if self.down:
            return 0
        max_cap = int(self._max_cap_val.value)
        return max(0, min(int(self._current_cap_val.value), max_cap))

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
        self._grant()
        self._changed()

    def _handle_capacity_change(self, new_target: float):
        self._capacity = self._target()
        # A rise grants waiting requests at once. A fall stops nothing: busy units
        # above the new capacity are given back as their tasks finish, and no
        # request is granted until the busy count is within it.
        self._grant()
        self._changed()
