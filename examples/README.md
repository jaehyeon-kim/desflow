# Examples

Runnable examples, each written three ways: as a YAML blueprint in `yaml/`, with the builder API in `builder/` and with the low-level API in `low-level/`. They live outside the package, so `pip install dynamic-des` does not carry them and the library declares only what it imports.

Outside a clone, `ddes examples get <name>` downloads an example at the release you have installed, each version into a folder of its own; `ddes examples` lists them.

From a clone, run one directly:

```bash
uv run examples/builder/local_example.py
```

## What to install

From a clone, with uv, `--extra` installs what a script needs for that run:

```bash
uv run --extra kafka examples/builder/kafka_example.py
```

With pip, install first and run with `python`:

```bash
pip install "dynamic-des[kafka]"
python examples/builder/kafka_example.py
```

Extras by example: `kafka` for `*/kafka_example.py` and `kafka_dashboard.py`, `postgres` for `*/postgres_example.py`, `redis` for `*/redis_example.py`, `parquet` for `*/parquet_example.py`, `iceberg` for `*/iceberg_example.py`, both `kafka` and `parquet` for `*/backfill_live_example.py`, `postgres` for `*/postgres_orders_example.py`, and none for `*/local_example.py`.

`kafka_dashboard.py` additionally needs `nicegui`, which is not a dependency of the library because nothing in the package imports it. Run it as `uv run --extra kafka --with nicegui examples/kafka_dashboard.py`, which installs nothing permanently, or `pip install nicegui` first.

## What to start

Containers come from [odctl](https://github.com/jaehyeon-kim/odctl), installed once with `uv tool install "odctl>=1.0,<2"`. odctl 1.x keeps its profile names, ports and commands stable, so any 1.x release works.

| Example | Profile | Start |
|---|---|---|
| `*/local_example.py` | none | |
| `*/kafka_example.py` | `kafka-lite` | `odctl up kafka-lite` |
| `*/backfill_live_example.py`, `yaml/backfill_live.yaml` | `kafka-lite` | `odctl up kafka-lite` |
| `*/postgres_orders_example.py`, `yaml/advanced/postgres_orders.yaml` | `postgres` | `odctl up postgres` |
| `kafka_dashboard.py` | `kafka-lite` | `odctl up kafka-lite` |
| `*/postgres_example.py` | `postgres` | `odctl up postgres` |
| `*/redis_example.py` | `valkey` | `odctl up valkey` |
| `*/parquet_example.py` | none, or `storage` with `PARQUET_FILESYSTEM=s3` | `odctl up storage` |
| `*/iceberg_example.py` | `catalog` | `odctl up catalog` |

Stop a profile with `odctl down <profile> --volumes`. `odctl list -d` shows every profile and the ports it publishes.

Kafka and Redis are the two whose profile names are not what you would guess, because odctl ships a one-broker Kafka as `kafka-lite` and uses Valkey rather than Redis.

## Three ways, same simulation

`builder/` uses `SimulationContext`, the builder, where infrastructure is declared by chaining and the simulation logic hangs off decorators.

`low-level/` uses `DynamicRealtimeEnvironment` directly, wiring the registry, resources and connectors by hand. It is the lower-level API the builder is written on.

`yaml/` holds the same simulations as YAML blueprints, described in the next section.

Most pairs run the same simulation, so reading one against the other shows what the builder does for you. The local pair is the exception and the two differ on purpose: `builder/local_example.py` runs `Factory_A` for 60 seconds with no ingress, while `low-level/local_example.py` runs `Line_A` for 30 seconds and uses `LocalIngress` to schedule two capacity changes.

## YAML blueprints

`yaml/` holds the YAML version of every example, run with the `ddes` command:

```bash
uv run ddes run examples/yaml/local.yaml
```

The blueprints in `yaml/` are plain YAML with no Python. `yaml/advanced/postgres_orders.yaml` keeps its order generator in Python, in `postgres_orders_logic.py` beside it, and references it with `!python`. The extras and odctl profiles are the same as for the Python versions, so `kafka.yaml` needs `--extra kafka` and `odctl up kafka-lite`, and `backfill_live.yaml` needs both `kafka` and `parquet`. `tests/unit/test_yaml_examples.py` builds and runs each blueprint and checks the records it produces.
