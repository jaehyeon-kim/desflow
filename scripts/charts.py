"""Draw the charts the docs show, from runs of the tutorial snippets.

Each name in `CHARTS` runs the code of a page, unpaced, and saves a PNG under
docs/assets/charts/ with the same name. The charts are committed, so the docs build
needs no matplotlib; run this after a change to a tutorial snippet or a distribution.
tests/docs/test_docs_diagrams.py checks that every chart is made here and used on a
page.

Usage: python scripts/charts.py [name ...]
"""

import contextlib
import importlib.util
import io
import sys
from pathlib import Path
from typing import Callable, Dict, List, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CHARTS_DIR = ROOT / "docs" / "assets" / "charts"
TUTORIALS = ROOT / "docs" / "snippets" / "tutorials"

# 7.5 inches at 150 dots per inch is 1,125 pixels, within the width the docs show
# without shrinking the text.
WIDTH, HEIGHT, DPI = 7.5, 4.0, 150
BLUE, ORANGE, GREY, GREEN = "#6c8ebf", "#d79b00", "#666666", "#82b366"

plt.rcParams.update(
    {
        "font.size": 12,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.titlesize": 13,
    }
)


def _module(name: str):
    """Imports a tutorial snippet by its file name."""
    sys.path.insert(0, str(TUTORIALS))
    try:
        spec = importlib.util.spec_from_file_location(name, TUTORIALS / f"{name}.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(TUTORIALS))


def _pages_module(name: str):
    """Imports a User Guide snippet by its file name."""
    folder = ROOT / "docs" / "snippets" / "pages"
    spec = importlib.util.spec_from_file_location(name, folder / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _save(fig, name: str) -> None:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(CHARTS_DIR / f"{name}.png", dpi=DPI)
    plt.close(fig)


def queues_wait() -> None:
    """Queues and Capacity: the mean wait of the busy hour for 1 to 4 packers."""
    module = _module("queues_and_capacity")
    packers = [1, 2, 3, 4]
    waits = [module.mean_wait(n) for n in packers]
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    ax.bar([str(n) for n in packers], waits, color=BLUE)
    ax.axhline(10, color=ORANGE, linestyle="--", linewidth=2)
    ax.text(3.45, 12, "10 seconds", color=ORANGE, ha="right")
    ax.set_yscale("log")
    ax.set_ylim(0.1, max(waits) * 4)
    ticks = [0.1, 1, 10, 100, 1000]
    ax.set_yticks(ticks, [f"{t:,g}" for t in ticks])
    ax.minorticks_off()
    for x, wait in enumerate(waits):
        ax.text(x, wait * 1.15, f"{wait:.2f} s", ha="center")
    ax.set_xlabel("packers")
    ax.set_ylabel("mean wait (seconds, log scale)")
    ax.set_title("Mean wait in the busy hour: 1.8 orders a second, 1.5 s of packing")
    _save(fig, "queues-wait")


def _draws(config, n: int = 20000):
    """`n` draws of a distribution, as the run draws them."""
    from desflow import Sampler

    sampler = Sampler(seed=42)
    config.stream = "chart"
    return [sampler.sample(config) for _ in range(n)]


def choosing_inputs() -> None:
    """Choosing Inputs: the four packing candidates and the mean wait of each."""
    module = _module("choosing_inputs")
    low_level = _module("choosing_inputs_low_level")
    services = low_level.params().service
    colours = {
        "constant": GREY,
        "triangular": GREEN,
        "empirical": BLUE,
        "exponential": ORANGE,
    }
    fig, (shapes, waits) = plt.subplots(
        1, 2, figsize=(WIDTH, HEIGHT), gridspec_kw={"width_ratios": [3, 2]}
    )
    bins = [i * 0.25 for i in range(0, 25)]
    for name in ("exponential", "empirical", "triangular"):
        shapes.hist(
            _draws(services[name]),
            bins=bins,
            density=True,
            histtype="step",
            linewidth=2,
            color=colours[name],
            label=name,
        )
    shapes.axvline(1.5, color=GREY, linewidth=2, label="constant")
    shapes.set_xlabel("packing time (seconds)")
    shapes.set_ylabel("share of draws")
    shapes.set_yticks([])
    shapes.legend(frameon=False)
    shapes.set_title("Four packing times, mean 1.5 s")
    names = module.CANDIDATES
    values = [module.mean_wait(name) for name in names]
    waits.barh(names[::-1], values[::-1], color=[colours[n] for n in names[::-1]])
    for y, value in enumerate(values[::-1]):
        waits.text(value + 0.05, y, f"{value:.2f} s", va="center")
    waits.set_xlim(0, max(values) * 1.35)
    waits.set_xlabel("mean wait (seconds)")
    waits.set_title("Mean wait, busy hour")
    _save(fig, "choosing-inputs")


def measuring_queue() -> None:
    """Measuring a Run: the packer queue over five minutes, its time-weighted
    average, and what readings every 10 seconds see."""
    from desflow import DynamicRealtimeEnvironment, DynamicResource, Sampler

    module = _module("measuring_a_run_low_level")
    until = 300
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(module.PARAMS)
    env.setup_stats("Shop", "basic", interval=10)
    trace = [(0.0, 0)]
    changed = env.stats.changed

    def record(category, name):
        changed(category, name)
        trace.append((env.now, len(packers.queue.items)))

    env.stats.changed = record
    packers = DynamicResource(env, "Shop", "packers")
    env.process(module.order_generator(env, Sampler(seed=42), packers))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    average = env.stats.readings()["Shop.stats.resources.packers.queue_length_avg"]

    def at(time):
        return [length for when, length in trace if when <= time][-1]

    samples = [(s, at(s)) for s in range(10, until + 1, 10)]
    sampled = sum(length for _, length in samples) / len(samples)
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    ax.step(
        [w for w, _ in trace],
        [n for _, n in trace],
        where="post",
        color=BLUE,
        linewidth=1,
        label="queue length",
    )
    ax.plot(
        [s for s, _ in samples],
        [n for _, n in samples],
        "o",
        color=GREY,
        label=f"reading every 10 s, mean {sampled:.2f}",
    )
    ax.axhline(
        average,
        color=ORANGE,
        linestyle="--",
        linewidth=2,
        label=f"time-weighted average, {average:.2f}",
    )
    ax.set_xlim(0, until)
    # Room above the highest queue for the legend, so it covers no data
    top = max(n for _, n in trace)
    ax.set_ylim(-0.5, top * 1.5)
    ax.set_yticks(range(0, top + 1, 2))
    ax.set_xlabel("simulation time (seconds)")
    ax.set_ylabel("orders waiting")
    ax.set_title("Orders waiting for a packer, first five minutes")
    ax.legend(frameon=False, loc="upper left")
    _save(fig, "measuring-queue")


def trusting_warmup() -> None:
    """Trusting Results: the start of the busy hour, read every 10 seconds, with
    the 2-minute warm-up marked."""
    from desflow import DynamicRealtimeEnvironment, DynamicResource, Sampler

    module = _module("trusting_results_low_level")
    until = 15 * 60
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(module.PARAMS)
    env.setup_stats("Shop", "typical", interval=10)
    readings = []

    def read():
        while True:
            yield env.timeout(10)
            values = env.stats.readings()
            readings.append(
                (
                    env.now / 60,
                    values["Shop.stats.resources.packers.queue_length"],
                    values.get("Shop.stats.tasks.pack_order.wait_mean", 0.0),
                )
            )

    packers = DynamicResource(env, "Shop", "packers")
    env.process(module.order_generator(env, Sampler(seed=42), packers))
    env.process(read())
    try:
        env.run(until=until + 1)
    finally:
        env.teardown()
    minutes = [m for m, _, _ in readings]
    fig, (queue, wait) = plt.subplots(2, 1, figsize=(WIDTH, 5.0), sharex=True)
    queue.plot(minutes, [q for _, q, _ in readings], "o-", color=BLUE, markersize=3)
    queue.set_ylabel("orders waiting")
    queue.set_title("The busy hour from an empty shop, read every 10 seconds")
    wait.plot(minutes, [w for _, _, w in readings], color=BLUE, linewidth=2)
    wait.set_ylabel("mean wait so far (s)")
    wait.set_xlabel("simulation time (minutes)")
    for ax in (queue, wait):
        ax.axvline(module.WARMUP / 60, color=ORANGE, linestyle="--", linewidth=2)
    wait.text(
        module.WARMUP / 60 + 0.2, 0.55, "end of the 2-minute warm-up", color=ORANGE
    )
    wait.set_xlim(0, until / 60)
    _save(fig, "trusting-warmup")


def stock_level() -> None:
    """Stock: the level of the stock over 30 minutes, for 60-box and 40-box
    deliveries every 100 seconds, with the stock-outs marked."""
    import logging

    module = _module("stock")
    logging.getLogger("desflow").setLevel(logging.WARNING)
    until = 30 * 60
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    for delivery, colour in ((60, BLUE), (40, ORANGE)):
        app = module.shop(delivery)
        points: list = []

        def read(context, points=points):
            stock = context.get_container("stock")
            while True:
                points.append((context.env.now / 60, stock.level))
                yield context.env.timeout(1)

        app.add_process(read)
        app.run(until=until + 1)
        minutes = [m for m, _ in points]
        levels = [level for _, level in points]
        ax.plot(
            minutes,
            levels,
            color=colour,
            linewidth=2,
            label=f"deliveries of {delivery}",
        )
        if delivery == 40:
            # A grey bar under the line wherever the stock is 0
            empty = [-8 if level == 0 else None for _, level in points]
            ax.plot(
                minutes,
                [float("nan") if e is None else e for e in empty],
                color=GREY,
                linewidth=5,
                solid_capstyle="butt",
                label="stock is 0: orders wait",
            )
    ax.set_xlim(0, until / 60)
    ax.set_ylim(-15, None)
    ax.set_xlabel("simulation time (minutes)")
    ax.set_ylabel("boxes on the shelf")
    ax.set_title("Stock level, a delivery every 100 seconds")
    ax.legend(frameon=False, loc="upper left")
    _save(fig, "stock-level")


def priorities_wait() -> None:
    """Priorities: the mean wait of express and regular orders under fifo and
    priority."""
    import logging

    module = _module("priorities")
    logging.getLogger("desflow").setLevel(logging.WARNING)
    waits = {queue: module.mean_waits(queue) for queue in ("fifo", "priority")}
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    width = 0.38
    for offset, task, colour, label in (
        (-width / 2, "pack_express", ORANGE, "express orders"),
        (width / 2, "pack_order", BLUE, "regular orders"),
    ):
        values = [waits[q][task] for q in waits]
        xs = [i + offset for i in range(len(waits))]
        ax.bar(xs, values, width, color=colour, label=label)
        for x, value in zip(xs, values):
            ax.text(x, value + 0.1, f"{value:.2f} s", ha="center")
    ax.set_xticks(range(len(waits)), [f"queue: {q}" for q in waits])
    ax.set_ylabel("mean wait (seconds)")
    ax.set_title("One packer, 0.6 orders a second, a quarter of them express")
    ax.legend(frameon=False, loc="upper left")
    ax.set_ylim(0, max(max(w.values()) for w in waits.values()) * 1.2)
    _save(fig, "priorities-wait")


def patience_waits() -> None:
    """Patience: the wait of every packed order over the hour, with a patience of 30
    seconds and with none."""
    import logging

    from desflow import SimulationContext

    module = _module("patience")
    logging.getLogger("desflow").setLevel(logging.WARNING)
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    for patience, colour, label in (
        (3600, GREY, "no patience"),
        (30, BLUE, "a patience of 30 s"),
    ):
        app: SimulationContext = module.shop(patience)
        queued: Dict[str, float] = {}
        waits: List[Tuple[float, float]] = []

        def watch(record, queued=queued, waits=waits):
            if record["stream_type"] != "event":
                return False
            status = record["value"].get("status")
            if status == "queued":
                queued[record["key"]] = record["sim_ts"]
            elif status == "started":
                start = queued.pop(record["key"])
                waits.append((record["sim_ts"] / 60, record["sim_ts"] - start))
            return False

        app.add_egress(_NullEgress(), when=watch)
        app.run(until="1 h")
        ax.scatter(*zip(*waits), s=4, color=colour, label=label)
    ax.set_xlabel("time packing started (minutes)")
    ax.set_ylabel("wait before packing (seconds)")
    ax.set_title("One packer, 0.7 orders a second")
    ax.legend(frameon=False, loc="upper left", markerscale=3)
    _save(fig, "patience-waits")


class _NullEgress:
    """An egress that drops what it is given; a chart reads records on the way in."""

    async def run(self, egress_queue) -> None:
        import asyncio
        import queue

        try:
            while True:
                try:
                    egress_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


def filtered_outcomes() -> None:
    """Filtered Inventory: filled and cancelled orders of each size over 8 hours."""
    import logging

    module = _module("filtered_inventory")
    logging.getLogger("desflow").setLevel(logging.WARNING)
    counts = module.outcomes()
    sizes = list(module.ORDERS)
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    width = 0.38
    for offset, outcome, colour in (
        (-width / 2, "filled", BLUE),
        (width / 2, "cancelled", ORANGE),
    ):
        values = [counts[(size, outcome)] for size in sizes]
        xs = [i + offset for i in range(len(sizes))]
        ax.bar(xs, values, width, color=colour, label=f"orders {outcome}")
        for x, value in zip(xs, values):
            ax.text(x, value + 15, str(value), ha="center")
    ax.set_xticks(range(len(sizes)), [f"size {size}" for size in sizes])
    ax.set_ylabel("orders in 8 hours")
    ax.set_ylim(0, max(counts.values()) * 1.15)
    ax.set_title("One shelf, each order waiting for its own size")
    ax.legend(frameon=False, loc="upper left")
    _save(fig, "filtered-outcomes")


def courier_shares() -> None:
    """Process Interaction: orders sent by courier and by post, for limits of 5 and
    8 minutes."""
    import logging

    module = _module("process_interaction")
    logging.getLogger("desflow").setLevel(logging.WARNING)
    limits = (300, 480)
    results = [module.outcomes(limit) for limit in limits]
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    labels = [f"limit {limit // 60} min" for limit in limits]
    courier = [r["by_courier"] for r in results]
    post = [r["by_post"] for r in results]
    ax.bar(labels, courier, 0.5, color=BLUE, label="by courier")
    ax.bar(labels, post, 0.5, bottom=courier, color=ORANGE, label="by post")
    for i, (c, p) in enumerate(zip(courier, post)):
        ax.text(i, c / 2, str(c), ha="center", color="white")
        ax.text(i, c + p / 2, str(p), ha="center")
    ax.set_ylabel("orders in 8 hours")
    ax.set_title("A courier every 10 minutes")
    ax.legend(frameon=False, loc="upper left", bbox_to_anchor=(1, 1))
    _save(fig, "courier-shares")


def interruption_policies() -> None:
    """Interruptions: the mean wait of regular orders, and how many were packed,
    under each policy."""
    import logging

    module = _module("interruptions")
    logging.getLogger("desflow").setLevel(logging.WARNING)
    policies = ("resume", "restart", "abandon")
    results = [module.outcomes(policy) for policy in policies]
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    waits = [r["wait_mean"] for r in results]
    ax.bar(policies, waits, 0.5, color=[BLUE, ORANGE, GREY])
    for i, r in enumerate(results):
        ax.text(
            i, waits[i] + 0.5, f"{waits[i]:.2f} s, {r['finished']} packed", ha="center"
        )
    ax.set_ylabel("mean wait of regular orders (s)")
    ax.set_ylim(0, max(waits) * 1.2)
    ax.set_title("Express orders preempt regular ones: one hour")
    _save(fig, "interruption-policies")


def handoff_waits() -> None:
    """Competing Resources: where orders wait, with and without the picker handoff."""
    import logging

    module = _module("competing_resources")
    logging.getLogger("desflow").setLevel(logging.WARNING)
    results = {keep: module.outcomes(keep) for keep in (False, True)}
    labels = ["frees the picker", "keeps the picker"]
    pick = [results[k]["pick_wait"] for k in (False, True)]
    pack = [results[k]["pack_wait"] for k in (False, True)]
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    ax.bar(labels, pick, 0.5, color=BLUE, label="wait for the picker")
    ax.bar(labels, pack, 0.5, bottom=pick, color=ORANGE, label="wait for the bench")
    for i in range(2):
        ax.text(
            i, pick[i] + pack[i] + 0.2, f"{pick[i] + pack[i]:.2f} s in all", ha="center"
        )
    ax.set_ylabel("mean wait of an order (s)")
    ax.set_ylim(0, max(a + b for a, b in zip(pick, pack)) * 1.2)
    ax.set_title("One picker, one bench, an order every 2.5 s")
    ax.legend(frameon=False, loc="upper left")
    _save(fig, "handoff-waits")


def end_counts() -> None:
    """End Conditions: orders finished and left in the shop, with a fixed horizon
    and with a drain."""
    import logging

    module = _module("end_conditions")
    logging.getLogger("desflow").setLevel(logging.WARNING)
    results = [module.outcomes(drain) for drain in (False, True)]
    labels = [
        f"fixed horizon, ends at {results[0]['end']:.0f} s",
        f"drain, ends at {results[1]['end']:.1f} s",
    ]
    finished = [r["finished"] for r in results]
    left = [r["in_system"] for r in results]
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    ax.bar(labels, finished, 0.5, color=BLUE, label="finished")
    ax.bar(labels, left, 0.5, bottom=finished, color=ORANGE, label="still in the shop")
    for i in range(2):
        ax.text(i, finished[i] / 2, str(finished[i]), ha="center", color="white")
        ax.text(i, finished[i] + left[i] + 30, f"{left[i]} left", ha="center")
    ax.set_ylabel("orders that started in the hour")
    ax.set_ylim(0, max(f + x for f, x in zip(finished, left)) * 1.3)
    ax.set_title("One packer, 0.7 orders a second")
    ax.legend(frameon=False, loc="upper center", ncol=2)
    _save(fig, "end-counts")


def rush_waits() -> None:
    """Changing Demand: the wait of every packed order over the hour, with one
    packer through the rush and with a second packer for it."""
    import logging

    from desflow import SimulationContext

    module = _module("changing_demand")
    logging.getLogger("desflow").setLevel(logging.WARNING)
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    ax.axvspan(20, 40, color=GREY, alpha=0.15, label="rush, 0.9 orders a second")
    for packers, colour, label in (
        (1, ORANGE, "one packer in the rush"),
        (2, BLUE, "two packers in the rush"),
    ):
        app: SimulationContext = module.shop(packers)
        queued: Dict[str, float] = {}
        waits: List[Tuple[float, float]] = []

        def watch(record, queued=queued, waits=waits):
            if record["stream_type"] != "event":
                return False
            status = record["value"].get("status")
            if status == "queued":
                queued[record["key"]] = record["sim_ts"]
            elif status == "started":
                start = queued.pop(record["key"])
                waits.append((record["sim_ts"] / 60, record["sim_ts"] - start))
            return False

        app.add_egress(_NullEgress(), when=watch)
        app.run(until="1 h")
        ax.scatter(*zip(*waits), s=4, color=colour, label=label)
    ax.set_xlim(0, 60)
    # Room above the longest wait for the legend, so it covers no data
    ax.set_ylim(-20, 640)
    ax.set_xlabel("time packing started (minutes)")
    ax.set_ylabel("wait before packing (seconds)")
    ax.set_title("1.5 s of packing, 0.4 orders a second outside the rush")
    ax.legend(frameon=False, loc="upper left", markerscale=3)
    _save(fig, "rush-waits")


def replay_waits() -> None:
    """Local Replay: the wait of every packed order over the hour, with the recorded
    orders and with a model of them at the same mean rate."""
    import logging

    from desflow import SimulationContext

    module = _module("local_replay")
    logging.getLogger("desflow").setLevel(logging.ERROR)
    fig, ax = plt.subplots(figsize=(WIDTH, HEIGHT))
    for replay, colour, label in (
        (True, ORANGE, "recorded orders"),
        (False, BLUE, "exponential gaps, same mean rate"),
    ):
        app: SimulationContext = module.shop(replay)
        queued: Dict[str, float] = {}
        waits: List[Tuple[float, float]] = []

        def watch(record, queued=queued, waits=waits):
            if record["stream_type"] != "event":
                return False
            status = record["value"].get("status")
            if status == "queued":
                queued[record["key"]] = record["sim_ts"]
            elif status == "started":
                start = queued.pop(record["key"])
                waits.append((record["sim_ts"] / 60, record["sim_ts"] - start))
            return False

        app.add_egress(_NullEgress(), when=watch)
        app.run(until="1 h")
        ax.scatter(*zip(*waits), s=4, color=colour, label=label)
    ax.set_xlim(0, 60)
    # Room above the longest wait for the legend, so it covers no data
    ax.set_ylim(-15, 450)
    ax.set_xlabel("minutes after 09:00, when packing started")
    ax.set_ylabel("wait before packing (seconds)")
    ax.set_title("One packer, 0.545 orders a second on average")
    ax.legend(frameon=False, loc="upper left", markerscale=3)
    _save(fig, "replay-waits")


def distribution_shapes() -> None:
    """Arrivals and Services: 200,000 draws from each distribution family, with the
    parameters of the examples on the page."""
    from desflow import DistributionConfig, Sampler

    module = _pages_module("arrivals_services")
    examples = [
        ("exponential", module.gap, "rate 0.5"),
        (
            "normal",
            DistributionConfig(dist="normal", mean=1.0, std=1.0),
            "mean 1, std 1",
        ),
        (
            "truncated_normal",
            DistributionConfig(dist="truncated_normal", mean=1.0, std=1.0, low=0.0),
            "mean 1, std 1, low 0",
        ),
        ("lognormal", module.repair, "mean 4, std 3"),
        ("constant", module.inspection, "value 0.5"),
        ("uniform", module.scanning, "low 1, high 2"),
        ("triangular", module.wrapping, "low 1, mode 1.25, high 2.25"),
        ("gamma", module.checking, "shape 3, scale 0.5"),
        (
            "weibull",
            DistributionConfig(dist="weibull", shape=1.5, scale=1.0),
            "shape 1.5, scale 1",
        ),
        ("empirical", module.sealing, "edges 0, 1, 2, 4; counts 10, 30, 5"),
    ]
    # Two columns and five rows, so the text keeps its full size at the page's width
    fig, axes = plt.subplots(5, 2, figsize=(WIDTH, 10.0))
    for ax, (name, config, label) in zip(axes.flat, examples):
        sampler = Sampler(seed=42)
        config.stream = name
        draws = [sampler.sample(config) for _ in range(200_000)]
        mean = sum(draws) / len(draws)
        # The longest 0.5% of draws are left off, so a long tail does not hide the shape
        top = sorted(draws)[int(len(draws) * 0.995)]
        low, high = min(draws), max(top, min(draws) + 1)
        if top == low:  # a constant: one bar, with the axis centred on its value
            low, high = low - 0.5, low + 0.5
        ax.hist(draws, bins=40, range=(low, high), color=BLUE)
        ax.axvline(mean, color=ORANGE, linewidth=1.5)
        ax.set_title(name.replace("_", " "))
        ax.set_xlabel(label)
        ax.set_yticks([])
        for side in ("left",):
            ax.spines[side].set_visible(False)
    fig.suptitle("200,000 draws from each family; the orange line is their mean")
    _save(fig, "distribution-shapes")


def fit_the_inputs() -> None:
    """Fit the Inputs: the 40 packing times against values drawn from the best fit,
    as `desflow fit --plot` draws them."""
    module = _module("fit_the_inputs")
    with contextlib.redirect_stdout(io.StringIO()):
        result = module.fitted()
    result.plot(CHARTS_DIR / "fit-the-inputs.png")


CHARTS: Dict[str, Callable[[], None]] = {
    "distribution-shapes": distribution_shapes,
    "priorities-wait": priorities_wait,
    "patience-waits": patience_waits,
    "filtered-outcomes": filtered_outcomes,
    "courier-shares": courier_shares,
    "interruption-policies": interruption_policies,
    "handoff-waits": handoff_waits,
    "end-counts": end_counts,
    "rush-waits": rush_waits,
    "replay-waits": replay_waits,
    "stock-level": stock_level,
    "trusting-warmup": trusting_warmup,
    "measuring-queue": measuring_queue,
    "queues-wait": queues_wait,
    "choosing-inputs": choosing_inputs,
    "fit-the-inputs": fit_the_inputs,
}


def main(names) -> None:
    for name in names or CHARTS:
        CHARTS[name]()
        print(f"{name} -> docs/assets/charts/{name}.png")


if __name__ == "__main__":
    main(sys.argv[1:])
