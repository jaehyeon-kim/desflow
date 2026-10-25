"""Throughput benchmark and profile for desflow (issues #5 and #24).

This is a measurement tool, not a test. It runs one workload at ``factor=0`` with a
fixed seed and reports records per second for each configuration, so the effect of
each change in issue #24 is measured on the same events. A configuration marked
"0.21.0" puts back the code that release had, so the gain of each change is the
ratio of two lines of the table.

Why ``factor=0``: at any positive factor the run waits for the wall clock on
purpose, so a throughput number would measure the wait, not the code.

Why a counting sink: the sink must take batches off the real queue without adding
its own cost. ``CountingEgress`` adds ``len(batch)`` to a counter. The console and
the real sinks are measured as configurations of their own.

Usage::

    uv run --all-extras python benchmarks/throughput.py configs  # the table
    uv run --all-extras python benchmarks/throughput.py micro    # costs per call
    uv run --all-extras python benchmarks/throughput.py profile  # where the time goes
    uv run --all-extras python benchmarks/throughput.py sinks    # Parquet, and Kafka
    uv run --all-extras python benchmarks/throughput.py all      # configs, micro, profile

``sinks`` writes Parquet to a temporary folder, and to Kafka at ``KAFKA`` (default
``127.0.0.1:9092``) when it answers, such as the odctl ``kafka-lite`` profile.
Run it with another interpreter to compare them, such as
``uv run --all-extras --python 3.13 python benchmarks/throughput.py configs``; the
header of every table names the interpreter.
"""

from __future__ import annotations

import asyncio
import collections
import gc
import logging
import os
import platform
import queue
import random
import statistics
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from simpy import Environment
from simpy.rt import RealtimeEnvironment

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
)
from desflow.connectors.egress.base import BaseEgress
from desflow.connectors.egress.local import _PREFIXES

# --------------------------------------------------------------------------
# Workload
# --------------------------------------------------------------------------

SEED = 20260910
UNTIL = 5000.0  # simulation seconds
ARRIVAL_RATE = 10.0  # tasks per simulation second
SERVICE_MEAN = 2.0
SERVICE_STD = 0.4
CAPACITY = 25  # offered load 10 * 2.0 / 25 = 80 percent utilisation
REPEATS = int(os.environ.get("REPEATS", "7"))
START = datetime(2026, 1, 1)


class CountingEgress(BaseEgress):
    """Sink that only counts records, so the measurement is not about the sink."""

    def __init__(self) -> None:
        self.records = 0

    async def run(self, egress_queue: "queue.Queue[Any]") -> None:
        while True:
            try:
                self.records += len(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.05)


class CountingConsoleEgress(ConsoleEgress):
    """``ConsoleEgress`` as shipped, with a record counter."""

    def __init__(self) -> None:
        self.records = 0

    async def run(self, egress_queue: "queue.Queue[Any]") -> None:
        # The wrapper has the two methods the sink calls
        await super().run(_CountingQueue(egress_queue, self))  # type: ignore[arg-type]


class OldConsoleEgress(BaseEgress):
    """0.21.0's ``ConsoleEgress``: one log call per record."""

    def __init__(self) -> None:
        self.records = 0

    async def run(self, egress_queue: "queue.Queue[Any]") -> None:
        log = logging.getLogger("desflow.connectors.egress.local")
        while True:
            try:
                batch = egress_queue.get_nowait()
                self.records += len(batch)
                for data in batch:
                    prefix = _PREFIXES.get(data.get("stream_type", "unknown"), "[EVT]")
                    rest = {k: v for k, v in data.items() if k != "stream_type"}
                    log.info(f"{prefix} {rest}")
            except queue.Empty:
                await asyncio.sleep(0.1)


class _CountingQueue:
    """Wraps a queue so a sink's batches are counted on the way out."""

    def __init__(self, inner: "queue.Queue[Any]", sink: Any) -> None:
        self.inner, self.sink = inner, sink

    def get_nowait(self) -> Any:
        batch = self.inner.get_nowait()
        self.sink.records += len(batch)
        return batch

    def qsize(self) -> int:
        return self.inner.qsize()

    def empty(self) -> bool:
        return self.inner.empty()


def _old_timestamp(self: Any, start_time: datetime, sim_now: float) -> str:
    """0.21.0's timestamp: a datetime and isoformat for every record."""
    return (start_time + timedelta(seconds=sim_now)).isoformat(timespec="milliseconds")


def build_params() -> SimParameter:
    return SimParameter(
        sim_id="Shop",
        arrival={"standard": DistributionConfig(dist="exponential", rate=ARRIVAL_RATE)},
        service={
            "packing": DistributionConfig(
                dist="normal", mean=SERVICE_MEAN, std=SERVICE_STD
            )
        },
        resources={"packers": CapacityConfig(current_cap=CAPACITY, max_cap=CAPACITY)},
    )


def attach_workload(env: Any, publish: bool = True) -> None:
    """The arrival, task and telemetry processes, as a low-level model writes them.

    The draws do not depend on the configuration, so every configuration runs the
    same events and publishes the same number of records.
    """
    res = DynamicResource(env, "Shop", "packers")
    sampler = Sampler(seed=SEED)

    def work_task(task_id: int, path_id: str):
        key = f"standard-{task_id + 1}"
        if publish:
            env.publish_event(key, {"path_id": path_id, "status": "queued"})
        with res.request() as req:
            yield req
            cfg = env.registry.get_config(path_id)
            if publish:
                env.publish_event(key, {"path_id": path_id, "status": "started"})
            yield env.timeout(sampler.sample(cfg))
            if publish:
                env.publish_event(key, {"path_id": path_id, "status": "finished"})

    def arrival_process():
        arrival_cfg = env.registry.get_config("Shop.arrival.standard")
        task_id = 0
        while True:
            yield env.timeout(sampler.sample(arrival_cfg))
            env.process(work_task(task_id, "Shop.service.packing"))
            task_id += 1

    def telemetry_monitor():
        while True:
            if publish:
                env.publish_telemetry("Shop.resources.packers.capacity", res.capacity)
                env.publish_telemetry("Shop.resources.packers.in_use", res.in_use)
                env.publish_telemetry(
                    "Shop.resources.packers.queue_length", len(res.queue)
                )
            yield env.timeout(2.0)

    env.process(arrival_process())
    env.process(telemetry_monitor())


# --------------------------------------------------------------------------
# Configurations
# --------------------------------------------------------------------------

_DEVNULL = open(os.devnull, "w")


def configure_logging(level: int) -> None:
    """Send log records to /dev/null with the format the examples use.

    A terminal is slower than /dev/null and its speed depends on the emulator, so
    the console lines measure formatting and one write, and understate a terminal.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(_DEVNULL)
    handler.setFormatter(logging.Formatter("%(levelname)s [%(asctime)s] %(message)s"))
    root.addHandler(handler)
    root.setLevel(level)
    logging.getLogger("desflow").setLevel(level)


def run_config(
    *,
    egress: bool = True,
    publish: bool = True,
    sink: Optional[Callable[[], Any]] = None,
    log_level: int = logging.WARNING,
    realtime_step: bool = False,
    old_timestamp: bool = False,
    lag: Any = "auto",
    freeze: bool = False,
) -> Tuple[float, float, int]:
    """Runs one configuration once: (run seconds, run and teardown seconds, records)."""
    configure_logging(log_level)
    # strict=False: the realtime_step configuration runs SimPy's own step at factor
    # 0, whose strict check raises there, as 0.21.0 never checked it
    env = DynamicRealtimeEnvironment(factor=0, strict=False, logical_start_time=START)
    env.registry.register_sim_parameter(build_params())
    if realtime_step:
        # 0.21.0 stepped through RealtimeEnvironment at every factor
        env.step = RealtimeEnvironment.step.__get__(env)  # type: ignore[method-assign]
    if old_timestamp:
        env._get_iso_timestamp = _old_timestamp.__get__(env)  # type: ignore[method-assign]
    provider: Any = None
    if egress:
        provider = sink() if sink else CountingEgress()
        env.setup_egress([provider], lag_monitor_interval=lag)
    attach_workload(env, publish=publish)
    # PyPy has no gc.freeze
    freeze = freeze and hasattr(gc, "freeze")
    if freeze:
        gc.collect()
        gc.freeze()
    started = time.perf_counter()
    try:
        env.run(until=UNTIL)
        ran = time.perf_counter() - started
    finally:
        env.teardown()
        if freeze:
            gc.unfreeze()
    return ran, time.perf_counter() - started, getattr(provider, "records", 0)


CONFIGS: List[Tuple[str, Dict[str, Any]]] = [
    ("A. core only, nothing published", dict(egress=False, publish=False)),
    ("B. as shipped, counting sink", dict()),
    ("C. B with 0.21.0's real-time step", dict(realtime_step=True)),
    ("D. B with 0.21.0's timestamp", dict(old_timestamp=True)),
    ("E. B with lag every second, 0.21.0's default", dict(lag=1.0)),
    (
        "F. 0.21.0: C, D and E together",
        dict(realtime_step=True, old_timestamp=True, lag=1.0),
    ),
    ("G. B with gc.freeze() after setup", dict(freeze=True)),
    (
        "H. ConsoleEgress, INFO to /dev/null",
        dict(sink=CountingConsoleEgress, log_level=logging.INFO),
    ),
    (
        "I. 0.21.0's ConsoleEgress, a call per record",
        dict(sink=OldConsoleEgress, log_level=logging.INFO),
    ),
]


def cmd_configs() -> Dict[str, Tuple[float, float, int]]:
    print(header())
    print(
        f"\nWorkload: until={UNTIL:.0f} simulation seconds, arrival rate "
        f"{ARRIVAL_RATE}/s, service normal(mean={SERVICE_MEAN}, std={SERVICE_STD}), "
        f"capacity {CAPACITY}, seed {SEED}, median of {REPEATS} runs."
    )
    print(
        "run s covers env.run(); total s adds the teardown, which waits for the "
        "sink to take every batch.\n"
    )
    print(
        f"{'configuration':<46} {'run s':>7} {'total s':>8} {'records':>8}"
        f" {'rec/s run':>10} {'rec/s total':>12}"
    )
    print("-" * 96)
    results: Dict[str, Tuple[float, float, int]] = {}
    for name, kwargs in CONFIGS:
        runs = [run_config(**kwargs) for _ in range(REPEATS)]
        ran = statistics.median(r[0] for r in runs)
        total = statistics.median(r[1] for r in runs)
        records = runs[0][2]
        results[name] = (ran, total, records)
        rate = f"{records / ran:>10.0f}" if records else f"{'':>10}"
        rate_total = f"{records / total:>12.0f}" if records else f"{'':>12}"
        print(f"{name:<46} {ran:>7.3f} {total:>8.3f} {records:>8d} {rate} {rate_total}")
    return results


# --------------------------------------------------------------------------
# Costs per call
# --------------------------------------------------------------------------


def _ns(fn: Callable[[], Any], iterations: int = 200_000) -> float:
    """Nanoseconds per call, best of five."""
    best = float("inf")
    for _ in range(5):
        started = time.perf_counter()
        for _ in range(iterations):
            fn()
        best = min(best, time.perf_counter() - started)
    return best / iterations * 1e9


def cmd_micro() -> None:
    import numpy as np

    print(header())
    print("\nCosts per call, nanoseconds, best of five.\n")
    env = DynamicRealtimeEnvironment(factor=0, logical_start_time=START)
    env.registry.register_sim_parameter(build_params())
    sampler = Sampler(seed=SEED)
    service = env.registry.get_config("Shop.service.packing")
    arrival = env.registry.get_config("Shop.arrival.standard")
    rng = np.random.default_rng(SEED)
    stdlib = random.Random(SEED)
    clock = iter(x * 0.0137 for x in range(10**9))
    cases: List[Tuple[str, Callable[[], Any]]] = [
        ("timestamp, cached text", lambda: env._get_iso_timestamp(START, next(clock))),
        (
            "timestamp, 0.21.0's isoformat",
            lambda: _old_timestamp(env, START, next(clock)),
        ),
        ("sampler.sample, normal service", lambda: sampler.sample(service)),
        ("sampler.sample, exponential arrival", lambda: sampler.sample(arrival)),
        ("numpy Generator.normal, one scalar", lambda: rng.normal(2.0, 0.4)),
        ("random.Random.gauss, one scalar", lambda: stdlib.gauss(2.0, 0.4)),
    ]
    for name, fn in cases:
        print(f"{name:<44} {_ns(fn):>8.1f} ns")

    # One request granted and released, and one timeout, on a plain simpy loop
    for label, body in (
        ("request, grant and release, per task", _request_release),
        ("timeout and resume, per event", _timeouts),
    ):
        print(f"{label:<44} {body():>8.1f} ns")


def _request_release(n: int = 100_000) -> float:
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(build_params())
    res = DynamicResource(env, "Shop", "packers")

    def user():
        for _ in range(n):
            with res.request() as req:
                yield req

    env.process(user())
    started = time.perf_counter()
    env.run()
    return (time.perf_counter() - started) / n * 1e9


def _timeouts(n: int = 200_000) -> float:
    env = Environment()

    def ticker():
        for _ in range(n):
            yield env.timeout(1)

    env.process(ticker())
    started = time.perf_counter()
    env.run()
    return (time.perf_counter() - started) / n * 1e9


# --------------------------------------------------------------------------
# Profile
# --------------------------------------------------------------------------

# Where a sampled stack belongs: the first frame from the innermost outwards whose
# file and function match decides.
_GROUPS: List[Tuple[str, str, str]] = [
    ("publish: timestamp", "environment.py", "_get_iso_timestamp"),
    ("publish: value to JSON", "environment.py", "_jsonable"),
    ("publish: buffer and flush", "environment.py", "_append_record"),
    ("publish: buffer and flush", "environment.py", "_flush_buffer"),
    ("publish: build the record", "environment.py", "publish_event"),
    ("publish: build the record", "environment.py", "publish_telemetry"),
    ("sampling", "sampler.py", ""),
    ("sampling", "distributions.py", ""),
    ("resource request and release", "resource.py", ""),
    ("registry lookups", "registry.py", ""),
    ("user generators", "throughput.py", "work_task"),
    ("user generators", "throughput.py", "arrival_process"),
    ("user generators", "throughput.py", "telemetry_monitor"),
    ("SimPy scheduler", "core.py", ""),
    ("SimPy events and processes", "events.py", ""),
]


def cmd_profile(interval: float = 0.0002, runs: int = 10) -> None:
    """A sampling profile of configuration B, from a thread that reads the main
    thread's stack every ``interval`` seconds.

    py-spy needs root on macOS, so this samples from inside the process with
    ``sys._current_frames``, which is what py-spy does from outside. A sample is
    only taken when the sampling thread holds the GIL, so very short calls can be
    under-counted; read the shares, not the absolute times.
    """
    print(header())
    counts: "collections.Counter[str]" = collections.Counter()
    main = threading.main_thread().ident
    assert main is not None
    done = threading.Event()
    running = threading.Event()

    def sample() -> None:
        while not done.is_set():
            if not running.wait(0.01):
                continue
            frame = sys._current_frames().get(main)
            group = "other"
            while frame is not None:
                code = frame.f_code
                file = code.co_filename.rsplit("/", 1)[-1]
                match = next(
                    (
                        g
                        for g, f, fn in _GROUPS
                        if file == f and (not fn or code.co_name == fn)
                    ),
                    None,
                )
                if match:
                    group = match
                    break
                frame = frame.f_back
            counts[group] += 1
            time.sleep(interval)

    sampler = threading.Thread(target=sample, daemon=True)
    configure_logging(logging.WARNING)
    switch = sys.getswitchinterval()
    sys.setswitchinterval(interval / 4)
    sampler.start()
    elapsed = 0.0
    for _ in range(runs):
        env = DynamicRealtimeEnvironment(factor=0, logical_start_time=START)
        env.registry.register_sim_parameter(build_params())
        env.setup_egress([CountingEgress()])
        attach_workload(env)
        started = time.perf_counter()
        running.set()
        env.run(until=UNTIL)
        running.clear()
        elapsed += time.perf_counter() - started
        env.teardown()
    done.set()
    sampler.join()
    sys.setswitchinterval(switch)
    total = sum(counts.values())
    print(
        f"\nConfiguration B, {runs} runs, {elapsed:.3f} s under the sampler, "
        f"{total} samples of env.run().\n"
    )
    for group, n in counts.most_common():
        print(f"{group:<36} {100 * n / total:>5.1f} %")


# --------------------------------------------------------------------------
# Real sinks
# --------------------------------------------------------------------------


def cmd_sinks() -> None:
    from desflow.connectors.egress.storage import ParquetStorageEgress

    print(header())
    print(f"\nRun and teardown seconds, median of {REPEATS} runs.\n")
    folder = tempfile.mkdtemp(prefix="desflow-bench-")

    def parquet() -> Any:
        return _Counted(ParquetStorageEgress(default_path=folder))

    sinks: List[Tuple[str, Callable[[], Any]]] = [
        ("counting sink", CountingEgress),
        ("Parquet, local disk", parquet),
    ]
    bootstrap = os.environ.get("KAFKA", "127.0.0.1:9092")
    if _kafka_answers(bootstrap):
        from desflow.connectors.egress.kafka import KafkaEgress

        def kafka() -> Any:
            return _Counted(KafkaEgress(bootstrap_servers=bootstrap))

        sinks.append((f"Kafka at {bootstrap}, JSON", kafka))
    else:
        print(f"No Kafka at {bootstrap}; start the odctl kafka-lite profile for it.\n")
    for name, make in sinks:
        runs = [run_config(sink=make) for _ in range(REPEATS)]
        total = statistics.median(r[1] for r in runs)
        records = runs[0][2]
        print(f"{name:<36} {total:>7.3f} s {records / total:>10.0f} records/s")


class _Counted(BaseEgress):
    """Counts the records a real sink is given, and passes each batch on."""

    def __init__(self, inner: BaseEgress) -> None:
        self.inner, self.records = inner, 0

    async def run(self, egress_queue: "queue.Queue[Any]") -> None:
        await self.inner.run(_CountingQueue(egress_queue, self))  # type: ignore[arg-type]


def _kafka_answers(bootstrap: str) -> bool:
    import socket

    host, _, port = bootstrap.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=1):
            return True
    except OSError:
        return False


def header() -> str:
    return (
        f"machine: {platform.machine()}, {platform.platform()}\n"
        f"python: {sys.version.split()[0]} ({sys.implementation.name})"
    )


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    if command in ("configs", "all"):
        cmd_configs()
    if command in ("micro", "all"):
        print()
        cmd_micro()
    if command in ("profile", "all"):
        print()
        cmd_profile()
    if command == "sinks":
        cmd_sinks()
