// Sends a visitor who opens an old section link to the page the section moved to.
// The redirects plugin works per page and cannot see a hash, so a link such as
// user-guide/resources/#breakdowns would stay on the page that no longer holds it.
// The script acts only when the hash names no element on the page.
(function () {
  var MOVED = {
  "reference/yaml-keys/": {
    "running-a-blueprint": "user-guide/writing-a-model/blueprints/#preview-the-records",
    "include-a-file": "user-guide/writing-a-model/blueprints/#include-a-file",
    "security": "user-guide/writing-a-model/blueprints/#security",
    "resources-stores-and-containers": "reference/yaml-keys/#resources",
    "services-and-arrivals": "reference/yaml-keys/#services",
    "arrivals-and-services": "reference/yaml-keys/#arrivals",
    "ingress-and-egress": "reference/yaml-keys/#ingress",
    "changes-during-a-run": "reference/yaml-keys/#scenario",
    "statistics-and-sinks": "reference/yaml-keys/#stats"
  },
  "learn/concepts/common-mistakes/": {
    "busy-hour-figures": "learn/concepts/output-analysis/#busy-hour-figures"
  },
  "learn/first-model/first-simulation/": {
    "busy-hour-figures": "learn/concepts/output-analysis/#busy-hour-figures"
  },
  "learn/first-model/queues-and-capacity/": {
    "networks-and-bottlenecks": "learn/shop-logic/flow-of-tasks/#networks-and-bottlenecks",
    "tuning": "cli/#check-it-before-a-long-run"
  },
  "user-guide/resources/": {
    "task-priority": "user-guide/resources/priority-and-preemption/#task-priority",
    "preemption": "user-guide/resources/priority-and-preemption/#preemption",
    "several-resources": "user-guide/resources/several-resources/#several-resources",
    "breakdowns": "user-guide/resources/priority-and-preemption/#breakdowns",
    "custom-resource-types": "user-guide/resources/custom-resource-types/#custom-resource-types",
    "example-interruptions": "user-guide/resources/priority-and-preemption/#example-interruptions",
    "example-competing-resources": "user-guide/resources/several-resources/#example-competing-resources"
  },
  "gallery/simulation-models/backfill-live/": {
    "why-one-run-rather-than-two": "user-guide/time-and-pacing/clocks-and-pacing/#go-live",
    "how-the-three-settings-fit-together": "user-guide/time-and-pacing/clocks-and-pacing/#go-live",
    "two-details-to-get-right": "gallery/simulation-models/backfill-live/#get-two-details-right",
    "what-the-run-logs": "gallery/simulation-models/backfill-live/#check-the-result",
    "low-level-api": "user-guide/time-and-pacing/clocks-and-pacing/#go-live"
  },
  "user-guide/tasks-and-processes/": {
    "a-process": "user-guide/tasks-and-processes/processes/#a-process",
    "processes-and-the-low-level-api": "user-guide/tasks-and-processes/processes/#how-it-works",
    "wait-signals-and-child-flows": "user-guide/tasks-and-processes/signals-and-waiting/#wait-signals-and-child-flows",
    "one-shot-signals": "user-guide/tasks-and-processes/signals-and-waiting/#one-shot-signals",
    "start-a-flow-without-waiting": "user-guide/tasks-and-processes/signals-and-waiting/#start-a-flow-without-waiting",
    "be-told-when-a-flow-ends": "user-guide/tasks-and-processes/signals-and-waiting/#be-told-when-a-flow-ends",
    "actions-when-an-event-happens": "user-guide/tasks-and-processes/signals-and-waiting/#actions-when-an-event-happens",
    "failures": "user-guide/tasks-and-processes/failures/#failures",
    "example-process-interaction": "user-guide/tasks-and-processes/processes/#example-process-interaction",
    "example-process-events": "user-guide/tasks-and-processes/signals-and-waiting/#example-process-events"
  },
  "user-guide/time-and-pacing/": {
    "logical-start-time": "user-guide/time-and-pacing/clocks-and-pacing/#logical-start-time",
    "time-zone": "user-guide/time-and-pacing/clocks-and-pacing/#time-zone",
    "factor": "user-guide/time-and-pacing/clocks-and-pacing/#factor",
    "go-live": "user-guide/time-and-pacing/clocks-and-pacing/#go-live",
    "lag": "user-guide/time-and-pacing/clocks-and-pacing/#lag",
    "strict-pacing": "user-guide/time-and-pacing/clocks-and-pacing/#strict-pacing",
    "warm-up": "user-guide/time-and-pacing/run-length/#warm-up",
    "initial-time": "user-guide/time-and-pacing/run-length/#initial-time",
    "run-length": "user-guide/time-and-pacing/run-length/#run-length_1",
    "stop-early": "user-guide/time-and-pacing/run-length/#stop-early",
    "drain": "user-guide/time-and-pacing/run-length/#drain",
    "step-and-peek": "user-guide/time-and-pacing/run-length/#step-and-peek",
    "run-in-segments": "user-guide/time-and-pacing/run-length/#run-in-segments",
    "example-end-conditions": "user-guide/time-and-pacing/run-length/#example-end-conditions"
  },
  "user-guide/results/experiments/": {
    "compare-designs": "user-guide/results/experiment-results/#compare-designs",
    "check-results": "user-guide/results/experiment-results/#check-results",
    "compare-with-observed-values": "user-guide/results/experiment-results/#compare-with-observed-values",
    "files": "user-guide/results/experiment-results/#files",
    "read-the-report": "user-guide/results/experiment-results/#read-the-report"
  },
  "user-guide/model-parts/task-flow/": {
    "entity-key": "user-guide/model-parts/task-flow/#record-key-and-fields"
  },
  "user-guide/model-parts/arrivals-and-services/": {
    "exponential": "reference/distributions/#exponential",
    "normal": "reference/distributions/#normal",
    "truncated-normal": "reference/distributions/#truncated_normal",
    "lognormal": "reference/distributions/#lognormal",
    "constant": "reference/distributions/#constant",
    "uniform": "reference/distributions/#uniform",
    "triangular": "reference/distributions/#triangular",
    "gamma": "reference/distributions/#gamma",
    "weibull": "reference/distributions/#weibull",
    "empirical": "reference/distributions/#empirical"
  },
  "reference/api/": {
    "desflow.core.context.SimulationContext": "reference/api/#desflow.SimulationContext",
    "desflow.core.signals.Signal": "reference/api/#desflow.Signal",
    "desflow.core.signals.SignalFailed": "reference/api/#desflow.SignalFailed",
    "desflow.core.task.TaskFailed": "reference/api/#desflow.TaskFailed",
    "desflow.core.task.NO_RECORD": "reference/api/#desflow.NO_RECORD",
    "desflow.models.flow.Entity": "reference/api/#desflow.Entity",
    "desflow.models.flow.Upstream": "reference/api/#desflow.Upstream",
    "desflow.core.flow.Router": "reference/api/#desflow.Router",
    "desflow.core.flow.Transfer": "reference/api/#desflow.Transfer",
    "desflow.loading.load_context": "reference/api/#desflow.load_context",
    "desflow.blueprint.build.Preview": "reference/api/#desflow.blueprint.Preview",
    "desflow.blueprint.loader.BlueprintError": "reference/api/#desflow.blueprint.BlueprintError",
    "desflow.analysis.runner.replicate": "reference/api/#desflow.replicate",
    "desflow.analysis.runner.Replications": "reference/api/#desflow.Replications",
    "desflow.analysis.models.LowLevelModel": "reference/api/#desflow.LowLevelModel",
    "desflow.analysis.intervals.interval": "reference/api/#desflow.analysis.interval",
    "desflow.analysis.intervals.Interval": "reference/api/#desflow.analysis.Interval",
    "desflow.analysis.intervals.t_quantile": "reference/api/#desflow.analysis.t_quantile",
    "desflow.analysis.runner.run_experiment": "reference/api/#desflow.run_experiment",
    "desflow.analysis.spec.ExperimentSpec": "reference/api/#desflow.ExperimentSpec",
    "desflow.analysis.spec.load_experiment": "reference/api/#desflow.analysis.load_experiment",
    "desflow.analysis.results.ExperimentResult": "reference/api/#desflow.ExperimentResult",
    "desflow.analysis.results.CheckFailed": "reference/api/#desflow.CheckFailed",
    "desflow.analysis.fit.fit_column": "reference/api/#desflow.fit_column",
    "desflow.analysis.fit.FitResult": "reference/api/#desflow.FitResult",
    "desflow.analysis.fit.Candidate": "reference/api/#desflow.analysis.Candidate",
    "desflow.explain.models.Explanation": "reference/api/#desflow.explain.Explanation",
    "desflow.explain.graph.build_graph": "reference/api/#desflow.explain.build_graph",
    "desflow.explain.analysis.analyze": "reference/api/#desflow.explain.analyze",
    "desflow.explain.render.render_terminal": "reference/api/#desflow.explain.render_terminal",
    "desflow.explain.render.to_json": "reference/api/#desflow.explain.to_json",
    "desflow.core.environment.DynamicRealtimeEnvironment": "reference/api/#desflow.DynamicRealtimeEnvironment",
    "desflow.core.registry.SimulationRegistry": "reference/api/#desflow.SimulationRegistry",
    "desflow.core.registry.ParamChange": "reference/api/#desflow.ParamChange",
    "desflow.core.registry.ParamRejected": "reference/api/#desflow.ParamRejected",
    "desflow.models.params.SimParameter": "reference/api/#desflow.SimParameter",
    "desflow.models.params.CapacityConfig": "reference/api/#desflow.CapacityConfig",
    "desflow.models.params.DistributionConfig": "reference/api/#desflow.DistributionConfig",
    "desflow.models.params.ContainerConfig": "reference/api/#desflow.ContainerConfig",
    "desflow.models.params.BreakdownConfig": "reference/api/#desflow.BreakdownConfig",
    "desflow.models.schemas.TelemetryPayload": "reference/api/#desflow.TelemetryPayload",
    "desflow.models.schemas.EventPayload": "reference/api/#desflow.EventPayload",
    "desflow.models.schemas.LabelPayload": "reference/api/#desflow.LabelPayload",
    "desflow.models.schemas.BaseStreamPayload": "reference/api/#desflow.models.BaseStreamPayload",
    "desflow.core.sampler.Sampler": "reference/api/#desflow.Sampler",
    "desflow.utils.time_to_seconds": "reference/api/#desflow.time_to_seconds",
    "desflow.resources.resource.ResourceBase": "reference/api/#desflow.ResourceBase",
    "desflow.resources.resource.DynamicResource": "reference/api/#desflow.DynamicResource",
    "desflow.resources.container.ContainerBase": "reference/api/#desflow.ContainerBase",
    "desflow.resources.container.DynamicContainer": "reference/api/#desflow.DynamicContainer",
    "desflow.resources.store.StoreBase": "reference/api/#desflow.StoreBase",
    "desflow.resources.store.DynamicStore": "reference/api/#desflow.DynamicStore",
    "desflow.core.breakdowns.run_breakdowns": "reference/api/#desflow.run_breakdowns",
    "desflow.core.scenario.run_scenario": "reference/api/#desflow.run_scenario",
    "desflow.core.replay.read_trace": "reference/api/#desflow.read_trace"
  }
};
  function base() {
    try {
      var config = JSON.parse(document.getElementById("__config").textContent);
      return new URL(config.base + "/", window.location.href).pathname;
    } catch (error) {
      return "/";
    }
  }
  var path = window.location.pathname;
  var root = base();
  if (path.indexOf(root) !== 0 || !window.location.hash) {
    return;
  }
  var page = path.slice(root.length).replace(/index\.html$/, "");
  var anchor = decodeURIComponent(window.location.hash.slice(1));
  var anchors = MOVED[page];
  if (!anchors || document.getElementById(anchor)) {
    return;
  }
  var target = anchors[anchor];
  if (!target) {
    // A member anchor (`<old id>.<member>`) follows the entry it belongs to: the
    // longest old id that the hash starts with, followed by a dot.
    var best = "";
    Object.keys(anchors).forEach(function (key) {
      if (anchor.indexOf(key + ".") === 0 && key.length > best.length) {
        best = key;
      }
    });
    if (best) {
      var moved = anchors[best];
      target = moved + anchor.slice(best.length);
    }
  }
  if (target) {
    window.location.replace(root + target);
  }
})();
