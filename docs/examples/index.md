# Runnable Examples

Each example is a complete model that you can download with `desflow examples get` and run, as [Get an Example](../user-guide/command-line.md#get-an-example) shows. Each is written three ways: as a YAML blueprint, with the builder API and with the low-level API. Its page shows the versions in tabs.

| Group | What it covers |
| --- | --- |
| [Local Simulation](local.md) | prints events and telemetry, with no container |
| [Kafka Digital Twin](kafka.md) | takes updates from Kafka and publishes events and telemetry to Kafka |
| [Fast-Forward to Storage](parquet.md) | data generated unpaced at `factor=0.0`: a week written to Parquet, and a day appended to an Iceberg table |
| [PostgreSQL](postgres.md) | writes orders to PostgreSQL and takes updates from a table |
| [Redis](redis.md) | writes to a Redis Stream and takes updates from Pub/Sub |
| [Parquet History, Kafka Live](backfill-live.md) | backdated history to Parquet, then live to Kafka |
| [Related Records](advanced-postgres-orders.md) | orders and their line items, in two PostgreSQL tables |

Each example page says how to download and run it, and which odctl profile from [Installation](../getting-started.md#services-for-the-examples) it needs. `desflow examples get` prints the extra an example needs, and `pip install "desflow[all]"` installs every extra, so each example runs once its profile is up.

`desflow examples list` prints every example in the terminal, with the odctl profile each one needs. The list also has the shop examples, such as `stock` and `breakdowns`, which are shown on the page that teaches their feature. `desflow examples get NAME` downloads the files of the release you installed into `NAME/yaml/`, `NAME/builder/` and `NAME/low-level/`. `--api` keeps one version, `--dest` downloads into another folder, `--ref` names another tag or branch, such as `main`, and `--force` replaces files that are already there:

```bash
desflow examples list
desflow examples get local --api yaml --dest work
```

The second command writes `work/local/yaml/local.yaml`.
