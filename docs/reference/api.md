# API Reference

## Simulation Context

<!-- prettier-ignore -->
::: desflow.core.context.SimulationContext
    options:
      heading_level: 3
      show_root_heading: true

## Task Flow

A task's flow is built from the classes below. The signals and failures here are shared between tasks.

<!-- prettier-ignore -->
::: desflow.core.signals.Signal
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.core.signals.SignalFailed
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.core.task.TaskFailed
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.models.flow.Entity
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.models.flow.Upstream
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.core.flow.Router
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.core.flow.Transfer
    options:
      heading_level: 3
      show_root_heading: true

## Loading a Model

`SimulationContext.from_yaml` is the usual entry point for a blueprint. `from_yaml` calls `build`, which returns the context and the `run` section. `load_context` loads a blueprint or a builder Python file from a folder, local or in object storage, in the way `desflow run` loads a blueprint. `desflow run --sample` and `--stdout` pass a `Preview` to these functions.

<!-- prettier-ignore -->
::: desflow.loading.load_context
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.build.build
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.build.Preview
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.blueprint.loader.BlueprintError
    options:
      heading_level: 3
      show_root_heading: true

## Replications

<!-- prettier-ignore -->
::: desflow.analysis.runner.replicate
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.runner.Replications
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.models.LowLevelModel
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.intervals.interval
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.intervals.Interval
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.intervals.t_quantile
    options:
      heading_level: 3
      show_root_heading: true

## Experiments

<!-- prettier-ignore -->
::: desflow.analysis.runner.run_experiment
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.spec.ExperimentSpec
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.spec.load_experiment
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.results.ExperimentResult
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.results.CheckFailed
    options:
      heading_level: 3
      show_root_heading: true

## Input Fitting

<!-- prettier-ignore -->
::: desflow.analysis.fit.fit_column
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.fit.FitResult
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.analysis.fit.Candidate
    options:
      heading_level: 3
      show_root_heading: true

## Explaining a Model

`SimulationContext.analyze` is the usual entry point, and `desflow explain` prints the same `Explanation`. `build_graph`, `analyze` and `verify` are the steps it takes, and `render_terminal` and `to_json` print the result. The JSON form is experimental through 1.x.

<!-- prettier-ignore -->
::: desflow.explain.models.Explanation
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.graph.build_graph
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.analysis.analyze
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.verify.verify
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.render.render_terminal
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.explain.render.to_json
    options:
      heading_level: 3
      show_root_heading: true

## Environment

<!-- prettier-ignore -->
::: desflow.core.environment.DynamicRealtimeEnvironment
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
::: desflow.core.registry.SimulationRegistry
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.models.params.SimParameter
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.models.params.CapacityConfig
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.models.params.DistributionConfig
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.models.params.ContainerConfig
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.models.params.BreakdownConfig
    options:
      heading_level: 3

## Data Payloads

<!-- prettier-ignore -->
::: desflow.models.schemas.TelemetryPayload
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.models.schemas.EventPayload
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.models.schemas.LabelPayload
    options:
      heading_level: 3

<!-- prettier-ignore -->
::: desflow.models.schemas.BaseStreamPayload
    options:
      heading_level: 3

## Utilities

<!-- prettier-ignore -->
::: desflow.core.sampler.Sampler
    options:
      heading_level: 3
      members:
        - sample

<!-- prettier-ignore -->
::: desflow.utils.time_to_seconds
    options:
      heading_level: 3
      show_root_heading: true

## Resources

<!-- prettier-ignore -->
::: desflow.resources.resource.ResourceBase
    options:
      heading_level: 3
      members:
        - request
        - release
        - in_use
        - count
        - capacity

<!-- prettier-ignore -->
::: desflow.resources.resource.DynamicResource
    options:
      heading_level: 3
      members: false

<!-- prettier-ignore -->
::: desflow.resources.container.ContainerBase
    options:
      heading_level: 3
      members:
        - put
        - get

<!-- prettier-ignore -->
::: desflow.resources.container.DynamicContainer
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
::: desflow.resources.store.StoreBase
    options:
      heading_level: 3
      members:
        - put
        - get

<!-- prettier-ignore -->
::: desflow.resources.store.DynamicStore
    options:
      heading_level: 3
      members:
        - put
        - get
        - items
        - capacity
        - putters_waiting
        - getters_waiting

## Breakdowns, Schedules and Replay

The builder and YAML start these SimPy processes and readers for you. Low-level code starts them itself.

<!-- prettier-ignore -->
::: desflow.core.breakdowns.run_breakdowns
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.core.scenario.run_scenario
    options:
      heading_level: 3
      show_root_heading: true

<!-- prettier-ignore -->
::: desflow.core.replay.read_trace
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
