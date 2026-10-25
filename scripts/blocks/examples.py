"""The blocks of the Examples pages."""

from typing import Callable, Dict, List

from .capture import event_lines, fields, yaml_records
from .commands import sample


def local_records() -> List[str]:
    """Local Simulation: the first capacity reading, the first order, and the change."""
    lines = yaml_records("examples/yaml/local.yaml")
    capacity = [line for line in lines if "packers.capacity'" in line]
    orders = [line for line in event_lines(lines) if fields(line)["key"] == "orders-1"]
    changed = [line for line in capacity if fields(line)["value"] == 3][:1]
    return sorted(
        capacity[:1] + orders + changed, key=lambda line: fields(line)["sim_ts"]
    )


def _example_sample(name: str, events: int) -> Callable[[], List[str]]:
    """An example page: the first records of `desflow run examples/yaml/<name>.yaml
    --sample <events> --stdout`."""

    def block() -> List[str]:
        return sample(f"examples/yaml/{name}.yaml", events, "--stdout")

    return block


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "kafka-sample": _example_sample("kafka", 5),
    "parquet-sample": _example_sample("parquet", 4),
    "iceberg-sample": _example_sample("iceberg", 4),
    "postgres-sample": _example_sample("postgres", 3),
    "redis-sample": _example_sample("redis", 3),
    "backfill-live-sample": _example_sample("backfill_live", 4),
    "related-records-sample": _example_sample("advanced/postgres_orders", 2),
    "local-records": local_records,
}
