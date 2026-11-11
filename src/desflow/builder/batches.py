"""Batches: task instances that wait to be served together by one run of the
resource and the service.

`OpenBatch` is one batch: its members in join order, when it closes, which member
serves it, and how it ends when its members leave. `BatchSeat` is one instance's
place in a batch. `TaskRun` (`builder/task.py`) evaluates the size, the timeout and
`close_when` in the instance's scope, and waits for the batch with its patience and
its interrupt handling; this module keeps the members and the batch open under each
batch name, in the run's `open_batches`.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    from desflow.builder.task import TaskRun


class OpenBatch:
    """The members that wait to be served together, and what they wait for.

    Attributes:
        key: The batch name the batch is open under.
        size: The number of members that closes it.
        since: When its first member joined, read as `batch.age`.
        members: The seats of its members, in join order.
        closed: Whether it has closed and published `started` for its members.
        finished: Whether its resource and service are done.
        dead: Whether every member left before it closed, so it runs nothing.
        leader: The member that runs the resource and service, once it closed.
    """

    def __init__(
        self,
        key: str,
        size: Union[int, float],
        since: float,
        open_batches: Dict[str, "OpenBatch"],
    ):
        self.key = key
        self.size = size
        self.since = since
        self.members: List[BatchSeat] = []
        self.closed = False
        self.finished = False
        self.dead = False
        self.leader: Optional[BatchSeat] = None
        # The run's batches open under each batch name, which this one leaves
        self._open_batches = open_batches

    def close(self) -> None:
        """Closes the batch: publishes `started` for every member in join order, and
        wakes the first member, which runs the resource and service."""
        self.closed = True
        self.leader = self.members[0]
        self._unlist()
        for member in self.members:
            member.started = True
            run = member.run
            run.publish(run.status("started"), lifecycle=True)
            run.env.stats.task_started(run.spec.name, run.task_id)
        # Only the first member runs now; the others wait until it has served them
        self.leader.notify("closed")

    def time_out(self, _event: Any) -> None:
        """Closes the batch when its timeout ends, if it is still open; a batch with
        no member left runs nothing.

        Args:
            _event: The timer event, which SimPy passes to its callbacks.
        """
        if self.closed or self.dead:
            return
        if not self.members:
            self.dead = True
            self._unlist()
            return
        self.close()

    def remove(self, member: BatchSeat) -> None:
        """Takes a member out. When it was the member serving the closed batch, the
        next member serves it; an open batch with no member left is dropped.

        Args:
            member: The seat of the member that leaves.
        """
        if member in self.members:
            self.members.remove(member)
        if self.finished or self.dead:
            return
        if self.closed:
            if self.leader is member and self.members:
                self.leader = self.members[0]
                self.leader.notify("lead")
        elif not self.members:
            self.dead = True
            self._unlist()

    def _unlist(self) -> None:
        """Takes the batch out of the batches open under its name."""
        if self._open_batches.get(self.key) is self:
            del self._open_batches[self.key]


class BatchSeat:
    """One task instance's place in the batches of its task.

    Attributes:
        run: The task instance.
        batch: The batch it joined, or None.
        event: The event it waits on in the batch, or None.
        started: Whether its batch has published its `started`.
        owes_resume: Whether it was interrupted after `started` was published at its
            batch's close, so the next grant is a resume.
        served: Whether another member's run of the resource and service served it.
    """

    __slots__ = ("run", "batch", "event", "started", "owes_resume", "served")

    def __init__(self, run: TaskRun):
        self.run = run
        self.batch: Optional[OpenBatch] = None
        self.event: Any = None
        self.started = False
        self.owes_resume = False
        self.served = False

    def _key(self) -> str:
        """The name of the batch the task joins: its `name`, or the task's name. Tasks
        with the same name share one open batch."""
        batch = self.run.spec.batch
        assert batch is not None
        return batch.name or self.run.spec.name

    def open_batch(self) -> Optional[OpenBatch]:
        """The batch open under the task's batch name, or None.

        Returns:
            Optional[OpenBatch]: The open batch, or None when none is open.
        """
        return self.run.parts.run.open_batches.get(self._key())

    def open(self, size: Union[int, float], limit: Optional[float]) -> OpenBatch:
        """Opens a batch under the task's batch name, with a timer when it has a
        timeout.

        Args:
            size: The number of members that closes it.
            limit: The seconds after which it closes, or None.

        Returns:
            OpenBatch: The batch opened.
        """
        env = self.run.env
        open_batches = self.run.parts.run.open_batches
        batch = OpenBatch(self._key(), size, env.now, open_batches)
        open_batches[batch.key] = batch
        if limit is not None:
            timer = env.timeout(limit)
            timer.callbacks.append(batch.time_out)
        return batch

    def join(self, batch: OpenBatch) -> None:
        """Adds the instance to a batch, and closes the batch when it is now full or
        its `close_when` holds.

        Args:
            batch: The open batch of the task.
        """
        run = self.run
        batch.members.append(self)
        self.batch = batch
        if len(batch.members) >= batch.size:
            batch.close()
            return
        spec = run.spec.batch
        assert spec is not None
        when = spec.close_when_of
        if when is not None:
            scope = replace(
                run.scope(),
                batch={"size": len(batch.members), "age": run.env.now - batch.since},
            )
            if when.evaluate(scope):
                batch.close()

    def ready(self) -> bool:
        """Whether the instance goes on: its batch is done, so it has been served, or
        it closed and this instance serves it.

        Returns:
            bool: True when the instance goes on.
        """
        batch = self.batch
        assert batch is not None
        if batch.finished:
            self.served = True
            return True
        return batch.closed and batch.leader is self

    def closed(self) -> bool:
        """Whether the batch the instance joined has closed.

        Returns:
            bool: True once it closed.
        """
        assert self.batch is not None
        return self.batch.closed

    def wait_event(self, event: Any) -> Any:
        """The event to wait on in the batch: `event` while it has not fired, else a
        new one, which the batch triggers to wake the instance.

        Args:
            event: The event waited on before, or None.

        Returns:
            Any: The event.
        """
        if event is None or event.triggered:
            event = self.event = self.run.env.event()
        return event

    def notify(self, value: str) -> None:
        """Wakes the instance if it waits for the batch.

        Args:
            value: What the batch woke it for: `closed`, `lead` or `done`.
        """
        event, self.event = self.event, None
        if event is not None and not event.triggered:
            event.succeed(value)

    def finish(self):
        """Wakes every member of the batch in join order once its resource and service
        are done, this one included."""
        batch = self.batch
        assert batch is not None
        if batch.finished:
            return
        batch.finished = True
        mine = self.event = self.run.env.event()
        for member in list(batch.members):
            if member is self:
                mine.succeed("done")
            else:
                member.notify("done")
        yield mine
        self.event = None

    def leave(self) -> None:
        """Takes the instance out of its batch. When it was the member serving a
        closed batch, the next member serves it."""
        batch, self.batch = self.batch, None
        self.event = None
        if batch is None:
            return
        batch.remove(self)
