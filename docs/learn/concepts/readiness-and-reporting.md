# Readiness and Reporting

A model is ready when someone who was not part of the study can see what it answers, how far to trust the answer, and how to repeat it. This page explains what that takes, with the shop's study in [The Complete Model](../studies/complete-model.md) as the example. It points to three published checklists written for simulation studies, and shows which parts desflow does for you and which parts only you can write.

---

## What You Will Learn

- why readiness is a statement about a decision, not about a model;
- what the three levels of the Model Readiness Checklist ask for;
- what a report needs, by the STRESS-DES checklist;
- what to share so that others can run the model, by the STARS framework;
- which items desflow records for you, and which only you can write.

---

## Ready for What

### In the Tutorial

The shop model is ready to say how many packers the busy hour needs: its inputs came from data, its checks passed, its results carry an interval, and a reader can repeat the runs. It is not ready to say anything about a holiday week, whatever its code looks like.

### Why It Matters

A model is built and checked for one question. The checks of [Checking and Validating](checking-and-validating.md) show it fits that question, with the inputs and the range of conditions it was checked under. Outside them, nothing has been checked.

### How to Use It

Write down the question the model is ready for, as [A Simulation Study](a-simulation-study.md#ask-one-question) does, and state the limits of its use. An experiment file keeps them with the runs: The Complete Model lists "One busy hour only; the rest of the day is not modelled" among its limits, and `report.md` prints them.

### What the Result Means

A reader who wants to use the model for another question can see at once that it was not built for it. That question needs its own checks before the model answers it.

---

## Model Readiness Checklist

### In the Tutorial

The shop's study meets most of the first level below. Its replications and warm-up come from [Trusting Results](../studies/trusting-results.md), its seeds are fixed, its core logic is checked in [Checking a Model](../studies/checking-a-model.md), and it is compared with values observed in the shop. The review of the process with the people who know it is the walk-through of [Check the Assumptions](a-simulation-study.md#check-the-assumptions), which no tool does.

### Why It Matters

The open book [HSMA: the little book of DES](https://des.hsma.co.uk/) has a chapter, [Model Readiness Checklist](https://des.hsma.co.uk/model_checklist.html), that sorts what a model needs into three levels, by the size of the decision:

- **Bronze** is the level for small to medium decisions within one organisation. It asks for views of the model running, checks of the core logic, enough [replications](output-analysis.md#replications) and a [warm-up](output-analysis.md#one-run-starts-empty), a fixed seed, a comparison with the real system, a review of the process with the people who know it, and documentation.
- **Silver** is the level for larger or costlier decisions, or for models used across several areas. It adds a formal reporting checklist such as STRESS-DES, documented code, a code review, automated tests, an open licence, version control and a sensitivity analysis.
- **Gold** is the level for models that will be reused, adapted or published. It adds:
    - a documentation site and a changelog;
    - identifiers for the authors and an archive of the code;
    - an online environment to run the model in, parallel runs, a web app and an animation;
    - formal methods for the warm-up and the number of replications;
    - tests against historical data.

### How to Use It

Choose the level from the decision, then go through its items. Some items cost nothing in desflow, and some need you:

| Item | In desflow |
|---|---|
| A fixed seed and independent streams | `random_seed` fixes a run, and `replicate` makes one integer seed for each replication from one seed and records them, so the same call gives the same results and any replication can be repeated alone. See [Replications](../../user-guide/results/replications.md) |
| Enough replications, with an interval | `replicate` gives the mean and the [half-width of the 95% interval](output-analysis.md#interval-and-half-width). [How many](output-analysis.md#how-many-replications) is your decision |
| The designs and the inputs on record | An experiment records the compiled parameters of each design and writes its summary, its comparisons, its checks and a `report.md`. See [Experiments](../../user-guide/results/experiments.md) |
| A comparison with the real system | An experiment can read an observed file and compare it with the model. See [Experiment Results](../../user-guide/results/experiment-results.md). Which data to use is your decision |
| A view of the model running | Every event and reading is a record, which you can read or send to a sink. See [Egress, Batching and Delivery](../../user-guide/live-runs-and-connectors/egress.md) |
| Core logic checked | Yours: [Checking and Validating](checking-and-validating.md) shows the cases to work by hand |
| Assumptions, data sources and limits | Yours: they go in the experiment file and the report, and no tool decides them |

### What the Result Means

A level is met when every item of it is done, not most. An item left out goes into the report as a limit, so that the reader knows which check the model has not had.

---

## Reporting a Study

### In the Tutorial

The Complete Model's `report.md` gives the question, the model and its versions, the assumptions, each input with its fitted parameters and where it came from, the run settings, the counts, the results, the comparisons, the checks and the observed comparison, then the limits and any input with no source. Its Comparisons section answers the question with each difference and its interval: a fourth packer cuts the packing wait by 1.77 seconds, with a half-width of 0.16 seconds.

### Why It Matters

A reader acts on the report alone. The reporting checklist STRESS-DES lists what that reader will ask. It has 20 items in six groups: objectives, logic, data, experimentation, implementation and code access. It is by Monks, Currie, Onggo, Robinson, Kunc and Taylor, and the book gives it in its chapter [STRESS-DES Reporting](https://des.hsma.co.uk/stress_des.html).

### How to Use It

Answer each group in one short paragraph of the report, in this order, after the answer and its interval:

- **Objectives:** the question and the measures, as in [Ask One Question](a-simulation-study.md#ask-one-question).
- **Logic:** the conceptual model, with its assumptions and what it leaves out.
- **Data:** where each input came from, and how it was fitted or chosen.
- **Experimentation:** the run length, the warm-up, the number of replications and the designs compared.
- **Implementation:** the software and its version, and the random number streams.
- **Code access:** where the code, the data and the seeds are kept.

### What the Result Means

`report.md` answers the objectives, the data and the experimentation from the experiment file, and gives the software versions. The logic beyond the listed assumptions, how the model was checked, and where the code is kept are yours to add. [Common Mistakes](common-mistakes.md) lists what to look for before you send the report.

---

## Sharing the Model

### In the Tutorial

Each tutorial page ends with Full Files: every file its runs need, so a reader can repeat them. desflow itself is under the MIT licence and installs from PyPI with a stated version.

### Why It Matters

The framework STARS, by Monks, Harper and Mustafee, says what to share so that others can run and reuse a model. The book gives it in [The STARS Framework](https://des.hsma.co.uk/stars.html). A model that only its author can run cannot be checked by anyone else.

### How to Use It

STARS separates essential parts from optional ones:

- **Essential:** an open licence; a list of the software the model needs, with its versions; free and open software; a minimum of documentation; an identifier for each author; a citation file; a code repository; and an archive that gives the code a permanent identifier.
- **Optional:** fuller documentation, a hosted documentation site, an online environment to run the model in, an interface, and a hosted web app.

Keep the blueprint or the Python files, the experiment file and the observed data in one repository, with the desflow version they ran on.

### What the Result Means

A model shared with its essential parts can be run again by a reader, with the same seeds and the same results. That is the strongest evidence a report can give.

---

## Key Takeaways

- Ready is a statement about one decision: write down the question and the limits of the model's use.
- Choose the readiness level from the size of the decision, and meet every item of it.
- desflow records the seeds, the intervals, the designs, the inputs and the comparisons. The assumptions, the checks and the limits are yours to write.
- Report the answer first, with its interval, then the six groups of STRESS-DES.
- Share the model with an open licence, its software versions and its files.

---

## Sources

- The open book [HSMA: the little book of DES](https://des.hsma.co.uk/), its chapters [Model Readiness Checklist](https://des.hsma.co.uk/model_checklist.html), [STRESS-DES Reporting](https://des.hsma.co.uk/stress_des.html) and [The STARS Framework](https://des.hsma.co.uk/stars.html). The chapters are under the CC BY-NC-SA licence, so none of their text is copied here. We read the three and link them, and the lists above are our own summaries.
- Monks, Currie, Onggo, Robinson, Kunc and Taylor, "Strengthening the reporting of empirical simulation studies: Introducing the STRESS guidelines" ([Journal of Simulation, 2019](https://doi.org/10.1080/17477778.2018.1442155)).
- Monks, Harper and Mustafee, "Towards sharing tools and artefacts for reusable simulations in healthcare" ([Journal of Simulation, 2024](https://doi.org/10.1080/17477778.2024.2347882)).
