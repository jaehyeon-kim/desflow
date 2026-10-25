// Sends a visitor who opens an old section link to the page the section moved to.
// The redirects plugin works per page and cannot see a hash, so a link such as
// user-guide/resources/#breakdowns would stay on the page that no longer holds it.
// The script acts only when the hash names no element on the page.
(function () {
  var MOVED = {
  "user-guide/resources/": {
    "task-priority": "user-guide/resources/priority-and-preemption/#task-priority",
    "preemption": "user-guide/resources/priority-and-preemption/#preemption",
    "several-resources": "user-guide/resources/several-resources/#several-resources",
    "breakdowns": "user-guide/resources/priority-and-preemption/#breakdowns",
    "custom-resource-types": "user-guide/resources/custom-resource-types/#custom-resource-types",
    "example-interruptions": "user-guide/resources/priority-and-preemption/#example-interruptions",
    "example-competing-resources": "user-guide/resources/several-resources/#example-competing-resources"
  },
  "user-guide/tasks-and-processes/": {
    "a-process": "user-guide/tasks-and-processes/processes/#a-process",
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
  "user-guide/experiments/": {
    "compare-designs": "user-guide/experiments/experiment-results/#compare-designs",
    "check-results": "user-guide/experiments/experiment-results/#check-results",
    "compare-with-observed-values": "user-guide/experiments/experiment-results/#compare-with-observed-values",
    "files": "user-guide/experiments/experiment-results/#files",
    "read-the-report": "user-guide/experiments/experiment-results/#read-the-report"
  },
  "user-guide/arrivals-and-services/": {
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
  var target = MOVED[page] && MOVED[page][anchor];
  if (target && !document.getElementById(anchor)) {
    window.location.replace(root + target);
  }
})();
