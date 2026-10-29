"""The Analyse the Event Log guide, low-level API: the shop written to Parquet.

The low-level version of the shop in analyse_the_event_log.py: one packer, orders
about every 2 seconds, each event written to Parquet with its status. python
analyse_the_event_log_low_level.py writes the files to analyse_output/ and analyses
them with the functions of analyse_the_event_log.py, which must be beside it.
"""

import itertools
import logging
import shutil
from pathlib import Path

from desflow import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    ParquetStorageEgress,
    Sampler,
    SimParameter,
)

logging.basicConfig(level=logging.WARNING)

RUN_SECONDS = 120
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={"orders": DistributionConfig(dist="exponential", rate=0.5)},
    service={"packing": DistributionConfig(dist="constant", value=1.5)},
    resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
)


# --8<-- [start:run]
def pack_order(env, packers, sampler, key, task_id):
    path_id = "Shop.service.packing"
    env.publish_event(
        key, {"path_id": path_id, "status": "queued"}, "pack_order", task_id
    )
    with packers.request() as unit:
        yield unit
        env.publish_event(
            key, {"path_id": path_id, "status": "started"}, "pack_order", task_id
        )
        yield env.timeout(sampler.sample(env.registry.get_config(path_id)))
        # The finished event's value, so every event of a task has a status
        env.publish_event(key, {"status": "finished"}, "pack_order", task_id)


def orders(env, packers, sampler):
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(pack_order(env, packers, sampler, f"orders-{number}", number - 1))


def shop(out: Path) -> DynamicRealtimeEnvironment:
    """One packer, orders about every 2 seconds, events written to Parquet in out."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    # One flat row per event, in files named events_<8 hex digits>.parquet
    env.setup_egress([ParquetStorageEgress(default_path=f"{out}/events.parquet")])
    packers = DynamicResource(env, "Shop", "packers")
    env.process(orders(env, packers, Sampler(seed=42)))
    return env


# --8<-- [end:run]


def run(out: Path) -> DynamicRealtimeEnvironment:
    """Runs the shop for two minutes into an empty folder and returns it."""
    shutil.rmtree(out, ignore_errors=True)  # a run adds files; start empty
    env = shop(out)
    env.run(until=RUN_SECONDS)
    env.teardown()  # writes the last files
    return env


if __name__ == "__main__":
    from analyse_the_event_log import (
        event_log,
        mean_wait,
        read_events,
        throughput_per_minute,
        waits_and_times,
    )

    folder = Path("analyse_output")
    run(folder)
    log = event_log(read_events(folder))
    per_order = waits_and_times(log)
    print(f"{log.num_rows} rows in the log, for {len(per_order)} orders")
    print(f"mean wait {mean_wait(per_order):.3f} s")
    print(f"throughput {throughput_per_minute(log, RUN_SECONDS):.1f} orders a minute")
