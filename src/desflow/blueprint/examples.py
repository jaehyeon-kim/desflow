"""The examples `desflow examples` lists and downloads.

The files are not shipped in the package. They are downloaded from GitHub at the tag
of the installed release, such as `v0.17.0`, so an example always matches the code it
runs on. A development install has no tag and downloads from `main`.
"""

import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

RAW_URL = "https://raw.githubusercontent.com/jaehyeon-kim/desflow/{ref}/examples/{path}"
APIS = ("yaml", "builder", "low-level")
_FOLDERS = {"yaml": "yaml", "builder": "builder", "low-level": "low-level"}


@dataclass(frozen=True)
class Example:
    """One example: what it shows, what it needs, and its files for each API."""

    name: str
    title: str
    shows: str
    files: Dict[str, List[str]]  # API name to paths under examples/
    profile: Optional[str] = None  # the odctl profile it needs, if any
    extras: str = ""  # the desflow extras it needs
    shared: List[str] = field(default_factory=list)  # files every API version uses
    notes: List[str] = field(default_factory=list)  # printed after the run commands
    until_stopped: bool = False  # runs until Ctrl + C rather than ending on its own
    watch: Optional[str] = None  # a shared script run beside it in a second terminal
    watch_needs: str = ""  # the package that script needs beyond desflow
    # The YAML version's command when it is not `desflow run <file>`
    yaml_command: Optional[str] = None


def _three(yaml: str, python: str, *yaml_helpers: str) -> Dict[str, List[str]]:
    return {
        "yaml": [f"yaml/{yaml}", *(f"yaml/{h}" for h in yaml_helpers)],
        "builder": [f"builder/{python}"],
        "low-level": [f"low-level/{python}"],
    }


EXAMPLES: Dict[str, Example] = {
    e.name: e
    for e in (
        Example(
            "local",
            "Local Simulation",
            "prints events and telemetry, with no container",
            _three("local.yaml", "local_example.py"),
        ),
        Example(
            "kafka",
            "Kafka Digital Twin",
            "takes updates from Kafka and publishes events and telemetry to Kafka",
            _three("kafka.yaml", "kafka_example.py"),
            profile="kafka-lite",
            extras="kafka",
            shared=["kafka_dashboard.py"],
            notes=[
                "Watch and steer a run: cd kafka && python kafka_dashboard.py "
                "(needs nicegui; serves http://localhost:8080)"
            ],
            until_stopped=True,
            watch="kafka_dashboard.py",
            watch_needs="nicegui",
        ),
        Example(
            "parquet",
            "Fast-Forward to Parquet",
            "a week generated unpaced, written to Parquet",
            _three("parquet.yaml", "parquet_example.py"),
            extras="parquet",
        ),
        Example(
            "iceberg",
            "Fast-Forward to Iceberg",
            "a day generated unpaced, appended to an Iceberg table",
            _three("iceberg.yaml", "iceberg_example.py"),
            profile="catalog",
            extras="iceberg",
        ),
        Example(
            "postgres",
            "PostgreSQL",
            "writes orders to PostgreSQL and takes updates from a table",
            _three("postgres.yaml", "postgres_example.py"),
            profile="postgres",
            extras="postgres",
            until_stopped=True,
        ),
        Example(
            "redis",
            "Redis",
            "writes to a Redis Stream and takes updates from Pub/Sub",
            _three("redis.yaml", "redis_example.py"),
            profile="valkey",
            extras="redis",
            until_stopped=True,
        ),
        Example(
            "backfill-live",
            "Parquet History, Kafka Live",
            "backdated history to Parquet, then live to Kafka",
            _three("backfill_live.yaml", "backfill_live_example.py"),
            profile="kafka-lite",
            extras="kafka,parquet",
        ),
        Example(
            "stock",
            "Stock, Parcels and Express Orders",
            "packing takes stock and fills a parcel store; express orders go first",
            _three("stock.yaml", "stock_example.py"),
        ),
        Example(
            "flow",
            "A Flow of Tasks",
            "orders routed through pick, gift wrap, pack and ship, with rework",
            _three("flow.yaml", "flow_example.py"),
        ),
        Example(
            "measuring",
            "Measuring a Run",
            "built-in statistics after a warm-up, and ground truth for a fault",
            _three("measuring.yaml", "measuring_example.py"),
        ),
        Example(
            "patience",
            "Patience",
            "orders leave a queue after their own patience, and none join after the last collection",
            _three("patience.yaml", "patience_example.py"),
        ),
        Example(
            "replications",
            "Replications",
            "one hour of the shop run 20 times, with the mean and interval of each statistic",
            _three("replications.yaml", "replications_example.py"),
            yaml_command="desflow run replications.yaml --replications 20",
        ),
        Example(
            "study",
            "A Local Study",
            "two designs of the shop compared with paired replications, a check, "
            "observed waits and a report",
            {
                "yaml": ["yaml/study.yaml", "yaml/study_shop.yaml"],
                "builder": ["builder/study_example.py"],
                "low-level": ["low-level/study_example.py"],
            },
            shared=["observed_waits.csv"],
            yaml_command="desflow study study.yaml",
        ),
        Example(
            "fitting",
            "Fitted Inputs",
            "the packing time fitted from 40 measured times, then loaded into the shop",
            _three("fitting.yaml", "fitting_example.py", "packing_fit.yaml"),
            extras="fit",
            shared=["packing_times.csv"],
            yaml_command="desflow fit ../packing_times.csv --column packing_seconds "
            "--lower 0 --out packing_fit.yaml && desflow run fitting.yaml",
        ),
        Example(
            "related-records",
            "Generate Related Records",
            "orders and their line items, in two PostgreSQL tables",
            {
                "yaml": [
                    "yaml/advanced/postgres_orders.yaml",
                    "yaml/advanced/postgres_orders_logic.py",
                ],
                "builder": ["builder/postgres_orders_example.py"],
                "low-level": ["low-level/postgres_orders_example.py"],
            },
            profile="postgres",
            extras="postgres",
            until_stopped=True,
        ),
    )
}


def release_ref(version: str) -> str:
    """The git ref an installed version's examples live at: its tag, or `main`."""
    if not version or "dev" in version or version.startswith("0.0.0"):
        return "main"
    return f"v{version}"


def _fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310 (fixed host)
        return response.read()


def download(
    example: Example, apis: List[str], dest: Path, ref: str, force: bool = False
) -> List[Path]:
    """Writes each API's files to `dest/<example>/<api>/`, and shared files beside them.

    Raises:
        FileExistsError: If a file is already there and `force` is not set.
        RuntimeError: If a file cannot be downloaded, naming the URL.
    """
    plan = []
    for api in apis:
        for path in example.files[api]:
            plan.append((path, dest / example.name / api / Path(path).name))
    for path in example.shared:
        plan.append((path, dest / example.name / Path(path).name))

    existing = [str(target) for _, target in plan if target.exists()]
    if existing and not force:
        raise FileExistsError(
            f"already there: {', '.join(existing)}. Use --force to replace them."
        )

    # Everything is downloaded before anything is written, so a failure leaves no
    # half-copied example behind.
    contents = []
    for path, target in plan:
        url = RAW_URL.format(ref=ref, path=path)
        try:
            contents.append((target, _fetch(url)))
        except urllib.error.HTTPError as exc:
            hint = ""
            if ref != "main":  # main has every example, so nothing else to try
                hint = (
                    f" An example added after this release is not at {ref}; "
                    "try --ref main."
                )
            raise RuntimeError(
                f"could not download {url} ({exc.code}).{hint}"
            ) from None
        except urllib.error.URLError as exc:
            raise RuntimeError(f"could not reach {url}: {exc.reason}") from None
    for target, content in contents:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return [target for target, _ in contents]


def run_hint(example: Example, api: str) -> str:
    """How to run one API's version once it is downloaded."""
    folder = f"{example.name}/{api}"
    main_file = Path(example.files[api][0]).name
    command = (
        example.yaml_command or f"desflow run {main_file}"
        if api == "yaml"
        else f"python {main_file}"
    )
    return f"cd {folder} && {command}"
