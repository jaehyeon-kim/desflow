# Evaluation

How the desflow skill was measured: 20 modelling tasks, run by three models with and without the skill, and the mistakes that each run made.

The skill came from the mistakes that the writers of the 1.0 documentation made when they wrote desflow models and tests. The review rounds of that work found 17 kinds of mistake. A change to the skill is kept only when a measurement shows it helps, so the evaluation was written before the first change.

---

## Twenty Tasks

The evaluation has 20 tasks. Each is a known case of the [Known Cases](../reference/known-cases/index.md) catalogue, chosen to cover its 14 topics and the kinds of mistake with the most findings. A task gives an assistant a request in words, such as a SimPy program to port or a set of checks to meet. It asks for the model in the YAML, Builder and Low-level forms, a pytest file and a final reply. The examples that ship with `desflow examples` are left out, because an assistant could download the answer.

Each run starts in an empty folder inside a sandbox that denies reads of the home folder and the repository and blocks the network. The folder holds a Python with desflow and pytest. A script grades the folder after the run: it runs each form, compares its records with the list of records that the task names, applies three changes to the model and runs its tests under each, and counts the mistakes of each kind.

---

## What Was Counted

The script counts these kinds of mistake:

| Kind | The mistake |
| --- | --- |
| K1 | A fixed draw or constant removes the random behaviour the request is about |
| K2 | A form leaves out a behaviour the request names |
| K4 | The three forms give different records, often seen only at another seed |
| K6 | Builder or Low-level code uses a name that desflow does not document |
| K7 | A run end that differs from the request's, or a time with no unit |
| K9 | A test that cannot fail on the change it is meant to catch |
| K11 | A Python file that does not run as its reader runs it |
| K12 | Tests that depend on order or leave process state changed |
| K13 | Low-level draws written by hand, which desflow does not seed |

Seven kinds need a reader and were not measured in the results below: a header, docstring or field that says what the run does not do (K3), a test that claims to compare more than it does (K5), a wall-clock assert or paced tests run in parallel (K8), expected values taken from the model under test (K10), a constant with no reason (K14), a cause claimed without a run (K15) and a SimPy program ported with SimPy's behaviour assumed (K17).

---

## Results

Before the change, Sonnet ran each task once with no skill and once with the previous skill. After the change, Sonnet ran every task three times, and Haiku and Opus ran every task once. That makes 100 runs with the skill as it is now.

| Skill | Model | Runs | Runs with a counted mistake |
| --- | --- | --- | --- |
| None | Sonnet | 20 | 10 |
| Previous skill | Sonnet | 20 | 5, all K6 |
| Current skill | Sonnet | 60 | 0 |
| Current skill | Haiku | 20 | 0 |
| Current skill | Opus | 20 | 0 |

Every run with the current skill gave a YAML file that ran and gave the records of the task, and no Python form differed from it. The grades of the first two rows did not check K4 and K13, so they count fewer kinds than the others, and their K6 was judged against the current list of documented names.

What the result does not show: the kinds that need a reader were not measured, and each task is one request in words, so the runs show that the current skill removes the counted mistakes on these 20 tasks and not that an assistant makes none on a task of its own.

---

## Sources

The tasks, the grading script and the rules of the rubric are in `scripts/skill/evals/` of the repository. They are not part of the skill folder.
