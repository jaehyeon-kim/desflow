# Examples

Runnable examples of one shop, each written three ways: as a YAML blueprint in `yaml/`, with the builder API in `builder/` and with the low-level API in `low-level/`. They live outside the package, so `pip install desflow` does not carry them and the library declares only what it imports.

Outside a clone, `desflow examples get <name>` downloads an example at the release you have installed, each version into a folder of its own; `desflow examples` lists them.

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
pip install "desflow[kafka]"
python examples/builder/kafka_example.py
```

Extras by example: `kafka` for `*/kafka_example.py` and `kafka_dashboard.py`, `postgres` for `*/postgres_example.py` and `*/postgres_orders_example.py`, `redis` for `*/redis_example.py`, `parquet` for `*/parquet_example.py`, `iceberg` for `*/iceberg_example.py`, both `kafka` and `parquet` for `*/backfill_live_example.py`, and `fit` for `*/fitting_example.py`. Every other example needs no extra. `desflow examples` lists each example with the odctl profile it needs, and `desflow examples get` prints the extra it needs.

`kafka_dashboard.py` additionally needs `nicegui`, which is not a dependency of the library because nothing in the package imports it. Run it as `uv run --extra kafka --with nicegui examples/kafka_dashboard.py`, which installs nothing permanently, or `pip install nicegui` first.

## What to start

Containers come from [odctl](https://github.com/jaehyeon-kim/odctl), installed once with `uv tool install "odctl>=1.0,<2"`. odctl 1.x keeps its profile names, ports and commands stable, so any 1.x release works.

| Example | Profile | Start |
|---|---|---|
| every example not named below | none | |
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

The three versions of each example run the same simulation and publish the same records, so reading one against another shows what the builder and the blueprint do for you. `tests/docs/test_example_twins.py` runs all three and compares their events and telemetry.

## YAML blueprints

`yaml/` holds the YAML version of every example, run with the `desflow` command:

```bash
uv run desflow run examples/yaml/local.yaml
```

The blueprints in `yaml/` are plain YAML with no Python, apart from the two in `yaml/advanced/`. `yaml/advanced/postgres_orders.yaml` keeps its order generator in Python, in `postgres_orders_logic.py` beside it, and `yaml/advanced/fuel_dock.yaml` its lane type in `fuel_dock_types.py`; each references its module with `!python`. The extras and odctl profiles are the same as for the Python versions, so `kafka.yaml` needs `--extra kafka` and `odctl up kafka-lite`, and `backfill_live.yaml` needs both `kafka` and `parquet`. `tests/docs/test_yaml_examples.py` builds and runs each blueprint and checks the records it produces.
