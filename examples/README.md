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

Extras by example: `kafka` for `*/kafka_example.py` and `kafka_dashboard.py`, `postgres` for `*/postgres_example.py` and `*/postgres_orders_example.py`, `redis` for `*/redis_example.py`, `parquet` for `*/parquet_example.py`, `iceberg` for `*/iceberg_example.py`, both `kafka` and `parquet` for `*/backfill_live_example.py`, and `fit` for `*/fitting_example.py`. Every other example in `builder/`, `low-level/` and `yaml/` needs no extra. `desflow examples` lists each example with the odctl profile it needs, and `desflow examples get` prints the extra it needs.

`kafka_dashboard.py` additionally needs `nicegui`, which is not a dependency of the library because nothing in the package imports it. Run it as `uv run --extra kafka --with nicegui examples/kafka_dashboard.py`, which installs nothing permanently, or `pip install nicegui` first.

## What to start

Containers come from [odctl](https://github.com/jaehyeon-kim/odctl), installed once with `uv tool install "odctl>=1.0,<2"`. odctl 1.x keeps its profile names, ports and commands stable, so any 1.x release works.

| Example | Profile | Start |
|---|---|---|
| every example in `builder/`, `low-level/` and `yaml/` not named below | none | |
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

## Gallery

`gallery/` holds examples named after the behaviour they produce, such as `orders_wait_for_busy_packers` and `point_in_time_training_set`. Each is written three times with the same records, as `<name>.yaml`, `<name>_builder.py` and `<name>_low_level.py`, and its first lines say what it needs and give the command that runs it. The `Needs:` line names the extras to install first. Unless its title is about a sink, it writes to the console and ends in seconds at `factor: 0`, so it needs no container. The run line of each form is `desflow run examples/gallery/<name>.yaml`, `python examples/gallery/<name>_builder.py` or `python examples/gallery/<name>_low_level.py`. `tests/docs/test_yaml_examples.py` runs each example, checks the behaviour its title states, and checks that the three versions publish the same records. The behaviour of the three examples that are known cases (GA-1 to GA-3) is checked in `tests/known_cases`.

## SimPy examples

`builder/simpy/`, `low-level/simpy/` and `yaml/simpy/` hold the seven programs that ship with SimPy, written in desflow: `bank_renege`, `carwash`, `gas_station_refuel`, `latency`, `machine_shop`, `movie_renege` and `process_communication`. `process_communication` has a second file, `process_communication_broadcast`, for the broadcast form of the program. Each is run as `desflow run examples/yaml/simpy/<name>.yaml`, `python examples/builder/simpy/<name>_example.py` or `python examples/low-level/simpy/<name>_example.py`. [SimPy Examples](https://jaehyeon.me/desflow/latest/simpy/examples/) puts each beside SimPy's code. The known cases DX-9 to DX-15 in `tests/known_cases` run them.

## Generators

`generators/` holds one example for each kind of generated value, as `<kind>.yaml`, `<kind>_builder.py` and `<kind>_low_level.py`, with the records it writes in `<kind>_output.txt`. [Generators](https://jaehyeon.me/desflow/latest/generators/) shows each beside the kind's keys. `tests/docs/test_generator_examples.py` runs them.
