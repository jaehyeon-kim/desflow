"""The SimPy program of V3: a value computed at the moment it is used. A write reads
the state and changes it, an interrupt's cause is read from the state just before the
state changes, and a timeout's value is fixed when the timeout is made."""

from typing import Any, List, Tuple

import simpy

Trace = List[Tuple[Any, ...]]

WRITING, WORKING, CUTTING, TIMING, READING = 0.0, 0.1, 0.2, 3.0, 8.0


def times(n: int) -> dict:
    """The start times of each group of processes: the rows of the replay files."""
    return {
        "writing": [WRITING] * n,
        "working": [WORKING] * n,
        "cutting": [CUTTING],
        "timing": [TIMING + 0.5 * i for i in range(n)],
        "reading": [READING],
    }


def reference(n: int) -> Trace:
    """`n` writers, `n` workers, one cutter that interrupts them, `n` timers and a
    reader, each group starting at its time in `times`."""
    env = simpy.Environment()
    state: dict = {"total": 10, "reason": "r1"}
    trace: Trace = []
    workers: List[simpy.Process] = []

    def starting(at: float, make):
        def start():
            yield env.timeout(at)
            make()

        env.process(start())

    def writer(index: int):
        state["total"] = state["total"] - 3
        trace.append((env.now, f"writing-{index}", "written"))

    def worker(index: int):
        try:
            yield env.timeout(9)
            trace.append((env.now, f"working-{index}", "finished"))
        except simpy.Interrupt as interrupt:
            trace.append((env.now, f"working-{index}", "interrupted", interrupt.cause))

    def cutter():
        yield env.timeout(1)
        trace.append((env.now, "cutting-1", "cut"))
        cause = state["reason"]
        state["reason"] = "changed in the same step"
        for target in workers:
            if target.is_alive:
                target.interrupt(cause)

    def timed(index: int):
        value = yield env.timeout(2, value=env.now * 2)
        trace.append((env.now, f"timing-{index}", "timed", value))

    def report():
        trace.append((env.now, "reading-1", "report", state["total"]))

    def start_worker(index: int):
        workers.append(env.process(worker(index)))

    for index in range(1, n + 1):
        starting(WRITING, lambda index=index: writer(index))
        starting(WORKING, lambda index=index: start_worker(index))
        starting(
            TIMING + 0.5 * (index - 1), lambda index=index: env.process(timed(index))
        )
    starting(CUTTING, lambda: env.process(cutter()))
    starting(READING, report)
    env.run(until=10)
    return trace
