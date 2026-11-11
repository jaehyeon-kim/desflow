# How-To

Each page here does one task, step by step, with code that you can run. To see what a part of desflow does and all its options, see the [User Guide](https://jaehyeon.me/desflow/latest/user-guide/index.md). For a complete model to download and run, see the [Gallery](https://jaehyeon.me/desflow/latest/gallery/index.md).

## Modelling

| Task                                                                                                            | What you do                                                                                           |
| --------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| [Vary Arrivals by Time](https://jaehyeon.me/desflow/latest/how-to/modelling/time-varying-arrivals/index.md)     | change the arrival rate and the staffing at set times                                                 |
| [Add Reneging and Balking](https://jaehyeon.me/desflow/latest/how-to/modelling/reneging-and-balking/index.md)   | give a task a time limit on its wait, or a condition that stops it joining                            |
| [Wait for One or All](https://jaehyeon.me/desflow/latest/how-to/modelling/wait-for-one-or-all/index.md)         | wait for the first of several things, for all of them, for a computed number of them, or for a resume |
| [Hand Off to a Resource](https://jaehyeon.me/desflow/latest/how-to/modelling/multi-resource-handoffs/index.md)  | pass an order from one resource to the next without letting go of the first too early                 |
| [Share Work by Weight](https://jaehyeon.me/desflow/latest/how-to/modelling/share-by-weight/index.md)            | share one resource between classes of work by weight                                                  |
| [Model Breakdowns](https://jaehyeon.me/desflow/latest/how-to/modelling/breakdowns/index.md)                     | make a resource fail at random, count only its busy time, and check its downtime                      |
| [Add Resources Mid-run](https://jaehyeon.me/desflow/latest/how-to/modelling/dynamic-topology/index.md)          | add a resource during a run, or give a resource with no units its units at a set time                 |
| [Serve Entities Together](https://jaehyeon.me/desflow/latest/how-to/modelling/serve-entities-together/index.md) | serve several entities with one run of a resource, in a batch                                         |

## Generate Data

| Task                                                                                                                | What you do                                                              |
| ------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| [Send Data to a Sink](https://jaehyeon.me/desflow/latest/how-to/generate-data/send-data-to-a-sink/index.md)         | write generated orders to the console, Kafka, PostgreSQL or a file       |
| [Realistic Fields](https://jaehyeon.me/desflow/latest/how-to/generate-data/realistic-fields/index.md)               | fill records with names, emails and cities, and draw a row of a CSV file |
| [Choices and Numbers](https://jaehyeon.me/desflow/latest/how-to/generate-data/choices-and-numbers/index.md)         | pick values with set odds and draw numbers from a range or a bell curve  |
| [Timestamps and Gaps](https://jaehyeon.me/desflow/latest/how-to/generate-data/timestamps-and-gaps/index.md)         | put times into records and make the gaps between records vary            |
| [Stop After a Count](https://jaehyeon.me/desflow/latest/how-to/generate-data/stop-after-a-count/index.md)           | end a run after a set number of records                                  |
| [Many Entities](https://jaehyeon.me/desflow/latest/how-to/generate-data/many-entities/index.md)                     | start many entities from one arrival, each with its own key and values   |
| [Build on the Last Value](https://jaehyeon.me/desflow/latest/how-to/generate-data/build-on-the-last-value/index.md) | make each value depend on the one before it                              |
| [Skewed Keys](https://jaehyeon.me/desflow/latest/how-to/generate-data/skewed-keys/index.md)                         | make a few keys carry most of the records                                |
| [Change State Over Time](https://jaehyeon.me/desflow/latest/how-to/generate-data/change-state-over-time/index.md)   | write a state machine as tasks                                           |
| [Look Up Other Records](https://jaehyeon.me/desflow/latest/how-to/generate-data/look-up-other-records/index.md)     | join records as they are made                                            |
| [Join Across Sinks](https://jaehyeon.me/desflow/latest/how-to/generate-data/join-across-sinks/index.md)             | write the ids of one kind of record to one sink and use them in another  |
| [Change the Schema](https://jaehyeon.me/desflow/latest/how-to/generate-data/change-the-schema/index.md)             | add, rename and retype a field at known times                            |

## Other Groups

| Group                                                                                                     | What it covers                                                                                                                                                                                                                                                 |
| --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Records and Kafka](https://jaehyeon.me/desflow/latest/how-to/records-and-kafka/complex-routing/index.md) | records routed to Kafka topics, Avro, JSON Schema, Protobuf and Pydantic records, secure Kafka connections, connectors of your own, and datasets of a size you choose                                                                                          |
| [Storage and Tables](https://jaehyeon.me/desflow/latest/how-to/storage-and-tables/cloud-storage/index.md) | Parquet and JSON Lines files on S3, Google Cloud Storage and Azure Blob Storage, and Iceberg tables through REST, SQL and Glue catalogs                                                                                                                        |
| [Analysis](https://jaehyeon.me/desflow/latest/how-to/analysis/fit-a-distribution/index.md)                | distributions fitted to measured times, checks against the queueing formulas, tests for a model, and a run's event log, analysed and animated                                                                                                                  |
| [Tools](https://jaehyeon.me/desflow/latest/how-to/tools/yaml-advanced/index.md)                           | Python logic in a blueprint, completion and checks for blueprints in an editor, and the fastest data generation                                                                                                                                                |
| [Gallery](https://jaehyeon.me/desflow/latest/gallery/index.md)                                            | complete models and data sets to download and run: data sets and event streams, processes with queues, a local simulation, a Kafka digital twin, history written fast to Parquet or Iceberg, PostgreSQL, Redis, history then live records, and related records |
