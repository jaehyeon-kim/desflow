# API Reference

## Simulation Context

<!-- prettier-ignore -->
::: desflow.SimulationContext
    options:
      heading_level: 3
      show_root_heading: true

## Task Flow

A task's flow is built from the classes below. The signals and failures here are shared between tasks.

<!-- prettier-ignore -->
::: desflow.Signal
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.SignalFailed
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.TaskFailed
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.NO_RECORD
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.Entity
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.Upstream
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.Router
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.Transfer
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.Expression
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.Scope
    options:
      heading_level: 3
      show_root_heading: true

## Loading a Model

`SimulationContext.from_yaml` is the usual entry point for a blueprint. `from_yaml` calls `build`, which returns the context and the `run` section. `load_context` loads a blueprint or a builder Python file from a folder, local or in object storage, in the way `desflow run` loads a blueprint. `desflow run --sample` and `--stdout` pass a `Preview` to these functions. `preview_builder` changes a context that a builder file defines, so that it runs as such a preview. `check_model` checks a blueprint or a builder file as `desflow check` does and returns a `ModelCheck`.

<!-- prettier-ignore -->
::: desflow.load_context
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.build.build
    options:
      heading_level: 3
      heading: desflow.blueprint.build
      separate_signature: true
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.Preview
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.preview_builder
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.check_model
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.ModelCheck
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.Run
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.RunSegment
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.SegmentEvery
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.Drain
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.BlueprintError
    options:
      heading_level: 3
      show_root_heading: true

## Replications

<!-- prettier-ignore -->
::: desflow.replicate
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.Replications
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.LowLevelModel
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.interval
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.Interval
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.t_quantile
    options:
      heading_level: 3
      show_root_heading: true

## Experiments

<!-- prettier-ignore -->
::: desflow.run_experiment
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.ExperimentSpec
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.Check
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.Compare
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.Observed
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.load_experiment
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.SourceMap
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.ExperimentResult
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.Design
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.RunResult
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.CheckFailed
    options:
      heading_level: 3
      show_root_heading: true

## Input Fitting

<!-- prettier-ignore -->
::: desflow.fit_column
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.FitResult
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.Candidate
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.read_column
    options:
      heading_level: 3
      show_root_heading: true

## Explaining a Model

`SimulationContext.analyze` is the usual entry point, and `desflow explain` prints the same `Explanation`. `build_graph`, `analyze` and `verify` are the steps it takes, and `render_terminal` and `to_json` print the result. The JSON form is experimental through 1.x.

<!-- prettier-ignore -->
::: desflow.explain.Explanation
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.build_graph
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.Graph
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.Node
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.Edge
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.ArrivalResult
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.CapacityRow
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.Comparison
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.Hint
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.HolderResult
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.ResourceResult
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.TaskResult
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.Verification
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.analyze
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.verify.verify
    options:
      heading_level: 3
      heading: desflow.explain.verify
      separate_signature: true
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.render_terminal
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.to_json
    options:
      heading_level: 3
      show_root_heading: true

## Environment

<!-- prettier-ignore -->
::: desflow.DynamicRealtimeEnvironment
    options:
      heading_level: 3
      show_root_heading: true
      # setup_egress and the publish methods come from the egress and ingress
      # mixins, and mkdocstrings skips inherited members unless asked.
      inherited_members: true
      members:
        - setup_ingress
        - setup_egress
        - publish_telemetry
        - publish_event
        - watch_changes
        - unwatch_changes
        - notify_queue_change
        - resume_pacing
        - teardown

## Registry and Parameters

<!-- prettier-ignore -->
::: desflow.SimulationRegistry
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.DynamicValue
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.ParamChange
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.ParamRejected
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.SimParameter
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.CapacityConfig
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.DistributionConfig
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.ContainerConfig
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.BreakdownConfig
    options:
      heading_level: 3

## Data Payloads

<!-- prettier-ignore -->
::: desflow.TelemetryPayload
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.EventPayload
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.LabelPayload
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.models.BaseStreamPayload
    options:
      heading_level: 3

## Utilities

<!-- prettier-ignore -->
::: desflow.Sampler
    options:
      heading_level: 3
      members:
        - sample

<!-- prettier-ignore -->
::: desflow.time_to_seconds
    options:
      heading_level: 3
      show_root_heading: true

## Resources

<!-- prettier-ignore -->
::: desflow.ResourceBase
    options:
      heading_level: 3
      members:
        - request
        - release
        - in_use
        - count
        - capacity

<!-- prettier-ignore -->
::: desflow.DynamicResource
    options:
      heading_level: 3
      members: false

<!-- prettier-ignore -->
::: desflow.ContainerBase
    options:
      heading_level: 3
      members:
        - put
        - get

<!-- prettier-ignore -->
::: desflow.DynamicContainer
    options:
      heading_level: 3
      members:
        - put
        - get
        - level
        - capacity
        - putters_waiting
        - getters_waiting

<!-- prettier-ignore -->
::: desflow.StoreBase
    options:
      heading_level: 3
      members:
        - put
        - get

<!-- prettier-ignore -->
::: desflow.DynamicStore
    options:
      heading_level: 3
      members:
        - put
        - get
        - items
        - capacity
        - putters_waiting
        - getters_waiting

<!-- prettier-ignore -->
::: desflow.resources.DynamicResourceRequest
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.resources.DynamicRelease
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.resources.PreemptRule
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.resources.QueueFull
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.resources.DynamicContainerPut
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.resources.DynamicContainerGet
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.resources.DynamicStorePut
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.resources.DynamicStoreGet
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.resources.DynamicFilterStoreGet
    options:
      heading_level: 3

## Breakdowns, Schedules and Replay

The builder and YAML start these SimPy processes and readers for you. Low-level code starts them itself.

<!-- prettier-ignore -->
::: desflow.run_breakdowns
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.run_scenario
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.read_trace
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.Row
    options:
      heading_level: 3
      show_root_heading: true

## Admin and Infrastructure

<!-- prettier-ignore -->
::: desflow.connectors.admin.kafka.KafkaAdminConnector
    options:
      heading_level: 3
      members:
        - create_topics
        - send_config
        - collect_data
        - get_state
        - get_vitals

## Connectors (Ingress)

<!-- prettier-ignore -->
::: desflow.connectors.ingress.base.BaseIngress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.file.FileIngress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.kafka.KafkaIngress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.local.LocalIngress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.postgres.PostgresIngress
    options:
      heading_level: 3
      members:
        - run

<!-- prettier-ignore -->
::: desflow.connectors.ingress.redis.RedisIngress
    options:
      heading_level: 3

## Deserializers

<!-- prettier-ignore -->
::: desflow.connectors.ingress.kafka.MessageDeserializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.kafka.JsonDeserializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.kafka.ConfluentAvroDeserializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.kafka.GlueAvroDeserializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.kafka.ConfluentJsonSchemaDeserializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.kafka.ConfluentProtobufDeserializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.kafka.GlueJsonSchemaDeserializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.ingress.kafka.GlueProtobufDeserializer
    options:
      heading_level: 3

## Connectors (Egress)

<!-- prettier-ignore -->
::: desflow.connectors.egress.base.BaseEgress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.KafkaEgress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.local.ConsoleEgress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.postgres.PostgresEgress
    options:
      heading_level: 3
      members:
        - run

<!-- prettier-ignore -->
::: desflow.connectors.egress.redis.RedisEgress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.storage.JsonlStorageEgress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.storage.ParquetStorageEgress
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.iceberg.IcebergStorageEgress
    options:
      heading_level: 3

## Serializers

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.MessageSerializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.JsonSerializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.ConfluentAvroSerializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.GlueAvroSerializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.ConfluentJsonSchemaSerializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.ConfluentProtobufSerializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.GlueJsonSchemaSerializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.GlueProtobufSerializer
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.SchemaRegistryError
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.connectors.egress.kafka.SerializationError
    options:
      heading_level: 3

## Generated Values

<!-- prettier-ignore -->
::: desflow.values.Template
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.BoundPayload
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.generator
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.ValueGenerationError
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.ValueRuntime
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.Aggregate
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.Fake
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.HistogramBin
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.Lookup
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.Previous
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.values.Row
    options:
      heading_level: 3
