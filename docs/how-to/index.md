# How-To

Each page here does one task, step by step, with code that you can run. To see what a part of desflow does and all its options, see the [User Guide](../user-guide/index.md). For a complete model to download and run, see the [Gallery](../gallery/index.md).

## Modelling

| Task | What you do |
| --- | --- |
| [Vary Arrivals by Time](modelling/time-varying-arrivals.md) | change the arrival rate and the staffing at set times |
| [Add Reneging and Balking](modelling/reneging-and-balking.md) | give a task a time limit on its wait, or a condition that stops it joining |
| [Wait for One or All](modelling/wait-for-one-or-all.md) | wait for the first of several things, for all of them, for a computed number of them, or for a resume |
| [Hand Off to a Resource](modelling/multi-resource-handoffs.md) | pass an order from one resource to the next without letting go of the first too early |
| [Share Work by Weight](modelling/share-by-weight.md) | share one resource between classes of work by weight |
| [Model Breakdowns](modelling/breakdowns.md) | make a resource fail at random, count only its busy time, and check its downtime |
| [Add Resources Mid-run](modelling/dynamic-topology.md) | add a resource during a run, or give a resource with no units its units at a set time |
| [Switch a Task On Mid-run](modelling/switch-a-task-on.md) | send work to a task declared from the start once a scenario step changes its route's weight |
| [Serve Entities Together](modelling/serve-entities-together.md) | serve several entities with one run of a resource, in a batch |

## Generate Data

| Task | What you do |
| --- | --- |
| [Send Data to a Sink](generate-data/send-data-to-a-sink.md) | write generated orders to the console, Kafka, PostgreSQL or a file |
| [Extend Payloads](generate-data/realistic-fields.md) | fill a payload with names, emails and cities that look real, and keep related fields together with one row of a file |
| [Choices and Numbers](generate-data/choices-and-numbers.md) | pick values with set odds and draw numbers from a range or a bell curve |
| [Timestamps and Gaps](generate-data/timestamps-and-gaps.md) | put times into records and make the gaps between records vary |
| [Stop After a Count](generate-data/stop-after-a-count.md) | end a run after a set number of records |
| [Many Entities](generate-data/many-entities.md) | start many entities from one arrival, each with its own key and values |
| [Build on the Last Value](generate-data/build-on-the-last-value.md) | make each value depend on the one before it |
| [Skewed Keys](generate-data/skewed-keys.md) | make a few keys carry most of the records |
| [Change State Over Time](generate-data/change-state-over-time.md) | write a state machine as tasks |
| [Look Up Other Records](generate-data/look-up-other-records.md) | join records as they are made |
| [Join Across Sinks](generate-data/join-across-sinks.md) | write the ids of one kind of record to one sink and use them in another |
| [Change the Schema](generate-data/change-the-schema.md) | add, rename and retype a field at known times |

## Other Groups

| Group | What it covers |
| --- | --- |
| [Records and Kafka](records-and-kafka/complex-routing.md) | records routed to Kafka topics, Avro, JSON Schema, Protobuf and Pydantic records, secure Kafka connections, connectors of your own, and datasets of a size you choose |
| [Storage and Tables](storage-and-tables/cloud-storage.md) | Parquet and JSON Lines files on S3, Google Cloud Storage and Azure Blob Storage, and Iceberg tables through REST, SQL and Glue catalogs |
| [Analysis](analysis/fit-a-distribution.md) | distributions fitted to measured times, checks against the queueing formulas, tests for a model, and a run's event log, analysed and animated |
| [Tools](tools/yaml-advanced.md) | Python logic in a blueprint, completion and checks for blueprints in an editor, and the fastest data generation |
| [Gallery](../gallery/index.md) | complete models and data sets to download and run: data sets and event streams, processes with queues, a local simulation, a Kafka digital twin, history written fast to Parquet or Iceberg, PostgreSQL, Redis, history then live records, and related records |
