from __future__ import annotations

from simpy import Container, Environment, Event, PriorityItem, PriorityStore

from dynamic_des.resources.base import BaseDynamicResource


class DynamicResourceRequest(Event):
    """
    A custom Request event for DynamicResource.

    This subclass allows us to attach metadata to the request if needed
    for future features like priority-shifting or logging.
    """

    def __init__(self, resource: DynamicResource, priority: int):
        super().__init__(resource.env)
        self.resource = resource
        self.cancelled = False  # Track if user leaves the queue before getting a token

        # Put this request into the priority queue
        self.resource.queue.put(PriorityItem(priority, self))

        # Wake up the dispatcher
        if not self.resource._request_event.triggered:
            self.resource._request_event.succeed()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.triggered:
            # The context manager exited before the dispatcher granted the token
            # (e.g., due to a timeout). Mark it so the dispatcher ignores it.
            self.cancelled = True
        else:
            # The task actually held the token, so we must release it back to the pool.
            self.resource.release()


class DynamicResource(BaseDynamicResource):
    """
    A SimPy Resource with a dynamically adjustable capacity.

    Unlike a standard `simpy.Resource` where capacity is fixed at creation,
    the `DynamicResource` listens to a `SimulationRegistry` path (e.g., 'Shop.resources.packers.current_cap').
    When the registry updates the capacity, the resource automatically adjusts,
    triggering pending requests if capacity increases.

    Attributes:
        env (DynamicRealtimeEnvironment): The active simulation environment.
        sim_id (str): The prefix ID (e.g., 'Shop').
        resource_id (str): The specific resource name (e.g., 'packers').
    """

    def __init__(self, env: Environment, sim_id: str, resource_id: str):
        """
        Initializes the DynamicResource and binds it to the registry.

        Args:
            env (Environment): The SimPy environment (must include the RegistryMixIn).
            sim_id (str): The parent simulation ID.
            resource_id (str): The name of the resource.
        """
        super().__init__(env, sim_id, resource_id, "resources")

        max_cap = int(self._max_cap_val.value)
        # Prevent initialization out-of-bounds
        self._capacity = max(0, min(int(self._current_cap_val.value), max_cap))

        self.queue = PriorityStore(env)
        self.pool = Container(env, init=self._capacity, capacity=max_cap)
        # Units held by tasks, counted when granted and when released
        self._busy = 0
        # Tokens that busy units still owe after a shrink. Each release while this
        # is above 0 is discarded, so no queued task starts until the busy count
        # is within the new capacity.
        self._owed = 0

        self._request_event = self.env.event()
        self.env.process(self._dispatcher())

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
        return self._busy

    def request(self, priority: int = 1):
        """Returns a context-manager enabled request."""
        return DynamicResourceRequest(self, priority)

    def release(self):
        """Return a token to the pool, or discard it while a shrink is still owed."""
        self._busy -= 1
        return self._return_token()

    def _return_token(self) -> Event:
        if self._owed > 0:
            self._owed -= 1
            discarded = self.env.event()
            discarded.succeed()
            return discarded
        return self.pool.put(1)

    def _dispatcher(self):
        """Bridges PriorityStore and Container without pre-fetching tokens."""
        while True:
            # If no one is waiting, sleep until a request is made. The wake-up event
            # can be left over from a request already served, so check the queue again.
            while not self.queue.items:
                yield self._request_event
                self._request_event = self.env.event()

            # Wait for a physical token to be available
            yield self.pool.get(1)

            # A shrink made while this token was being handed over owes it, so discard it
            if self._owed > 0:
                self._owed -= 1
                continue

            # Pull the highest-priority request currently in the queue. The queue is
            # not empty, so the get succeeds at once; reading it without a yield
            # leaves no step in which a shrink could arrive before the grant.
            ticket = self.queue.get().value

            # If the user cancelled their request (e.g., timed out while waiting),
            # return the token to the pool and move to the next person.
            if getattr(ticket.item, "cancelled", False):
                self._return_token()
                continue

            # Signal the user process
            if not ticket.item.triggered:
                self._busy += 1
                ticket.item.succeed()

    def _handle_capacity_change(self, new_target: float):
        # Safely bind the new target to physical limits [0, max_cap]
        max_cap = int(self._max_cap_val.value)
        if max_cap > self.pool.capacity:
            # Raise the pool's limit first so the tokens added below fit
            self.pool._capacity = max_cap
        new_target = max(0, min(int(new_target), max_cap))

        diff = new_target - self._capacity
        self._capacity = new_target
        if diff > 0:
            # A grow first cancels tokens still owed by an earlier shrink
            cancelled = min(diff, self._owed)
            self._owed -= cancelled
            if diff > cancelled:
                self.pool.put(diff - cancelled)
        elif diff < 0:
            # Idle tokens leave the pool at once. Busy units owe the rest, and their
            # releases are discarded, so running tasks are never stopped.
            amount = -diff
            idle = min(amount, self.pool.level)
            if idle:
                self.pool.get(idle)
            self._owed += amount - idle

        # Lower the pool's limit when max_cap falls. This is safe because the pool
        # now holds at most new_target tokens, and owed releases never reach it.
        self.pool._capacity = max_cap
