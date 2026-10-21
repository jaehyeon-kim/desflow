# API Reference

## Simulation Context

<!-- prettier-ignore -->
::: desflow.core.context.SimulationContext
    options:
      heading_level: 3
      show_root_heading: true

## Task Flow

The classes a task's flow is built from, and the signals and failures tasks share.

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

## YAML Blueprints

`SimulationContext.from_yaml` is the usual entry point. `build` is what it calls, and returns the `run` section as well.

<!-- prettier-ignore -->
::: desflow.blueprint.build.build
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
        - teardown

## Registry & Parameters

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
::: desflow.resources.container.DynamicContainer
    options:
      heading_level: 3
      members:
        - put
        - get
        - level
        - capacity

<!-- prettier-ignore -->
::: desflow.resources.store.DynamicStore
    options:
      heading_level: 3
      members:
        - put
        - get
        - items
        - capacity

## Breakdowns, Schedules and Replay

SimPy processes and readers the builder and YAML use, which low-level code starts itself.

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

## Admin & Infrastructure

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
