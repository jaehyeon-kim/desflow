# Why desflow

This page says why desflow exists, and how it is built, including how much of it AI agents write and how that work is checked.

---

## Why It Exists

Most test data generators write random rows. desflow writes the events of a working process: orders queue for packers, packing slows down at busy times, and a backlog builds when arrivals outpace capacity. The records have the cause and effect of a real system, because they come from a [SimPy](https://simpy.readthedocs.io/) simulation. [SimPy and desflow](../des/simpy-and-desflow.md) shows each SimPy construct beside its desflow form.

The parameters of the simulation can change while it runs, from Kafka, Redis, PostgreSQL or a timed scenario, and the pipeline downstream sees the effect. The records go to Kafka, databases and files, and one run can write a week of history at full speed and then keep streaming in real time. The [Roadmap](roadmap.md) lists what comes before 1.0.

---

## How It Is Built

AI agents write much of the code and the documentation of desflow. The checks below are there so that you can see what is verified instead of guessing.

The owner of the project sets the design, the plans and every decision, and approves each commit and each release. An agent never decides what desflow does.

---

## How a Change Is Checked

A writer agent makes a change. A second, fresh reviewer agent then checks every claim of the change against its source and every number against a run or the test that asserts it. The findings are fixed, and the rounds repeat until a review finds nothing serious. An audit runs before each release.

---

## What the Tests Hold

Three sets of tests stay in the repository, and each runs on a push to `main` and on every pull request to `main`:

- **SimPy behaviour.** Every SimPy feature marked native on the [SimPy Compatibility](../reference/simpy-compatibility.md) page is compared with plain SimPy in the low-level, builder and YAML forms. The page lists each feature's status: native and checked, or a difference kept by design. The tests are in [`tests/conformance/`](https://github.com/jaehyeon-kim/desflow/tree/main/tests/conformance), and the [`simpy-range` job](https://github.com/jaehyeon-kim/desflow/blob/main/.github/workflows/pipeline.yml) runs them at the lowest and the newest SimPy that desflow allows.
- **Documentation.** Every Python and YAML block on a page is a copy of a file that a test runs, except the hot rolling adapter and its blueprint, and every answer a page states is asserted by its test. The tests are in [`tests/docs/`](https://github.com/jaehyeon-kim/desflow/tree/main/tests/docs).
- **Records.** Every example and scenario runs on the last release and on the new commit, and the records are compared. A difference fails the [`regression` job](https://github.com/jaehyeon-kim/desflow/blob/main/.github/workflows/pipeline.yml) unless the change is listed as intended.
