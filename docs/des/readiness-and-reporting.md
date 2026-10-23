# Readiness and Reporting

A model is ready when someone who was not part of the study can see what it answers, how far to trust the answer, and how to repeat it. This page lists what that takes. It points to three published checklists written for simulation studies, and shows which parts desflow does for you and which parts only you can write. We read the three and link them. The chapters of the open book [HSMA: the little book of DES](https://des.hsma.co.uk/) are under the CC BY-NC-SA licence, so none of their text is copied here, and the lists below are our own summaries.

---

## Ready for What

Ready is a statement about a decision, not about a model. The shop model is ready to say how many packers the busy hour needs when its inputs came from data, its checks passed, its results carry an interval, and a reader can repeat the runs. It is not ready to say anything about a holiday week, whatever its code looks like. Write down the question the model is ready for, as [A Simulation Study](a-simulation-study.md) does, and state the limits of its use.

---

## Model Readiness Checklist

The open book [HSMA: the little book of DES](https://des.hsma.co.uk/) has a chapter, [Model Readiness Checklist](https://des.hsma.co.uk/model_checklist.html), that sorts what a model needs into three levels.

- **Bronze** is the level for small to medium decisions within one organisation. It asks for views of the model running, checks of the core logic, enough [replications](output-analysis.md#replications) and a [warm-up](output-analysis.md#one-run-starts-empty), a fixed seed, a comparison with the real system, a review of the process with the people who know it, and documentation.
- **Silver** is the level for larger or costlier decisions, or for models used across several areas. It adds a formal reporting checklist such as STRESS-DES, documented code, a code review, automated tests, an open licence, version control and a sensitivity analysis.
- **Gold** is the level for models that will be reused, adapted or published. It adds:
    - a documentation site and a changelog;
    - identifiers for the authors and an archive of the code;
    - an online environment to run the model in, parallel runs, a web app and an animation;
    - formal methods for the warm-up and the number of replications;
    - tests against historical data.

[Checking and Validating](checking-and-validating.md) explains the checks of the Bronze level, and [Output Analysis](output-analysis.md) explains the replications and the warm-up.

---

## Reporting a Study

The reporting checklist STRESS-DES has 20 items, in six groups: objectives, logic, data, experimentation, implementation and code access. It is by Monks, Currie, Onggo, Robinson, Kunc and Taylor, "Strengthening the reporting of empirical simulation studies: Introducing the STRESS guidelines" ([Journal of Simulation, 2019](https://doi.org/10.1080/17477778.2018.1442155)). The book gives it in its chapter [STRESS-DES Reporting](https://des.hsma.co.uk/stress_des.html). Use it as a list of questions a reader will ask. Each group is answered for the shop by one short paragraph of the report, in this order:

- **Objectives:** the question and the measures, as in [Ask One Question](a-simulation-study.md#ask-one-question).
- **Logic:** the conceptual model, with its assumptions and what it leaves out.
- **Data:** where each input came from, and how it was fitted or chosen.
- **Experimentation:** the run length, the warm-up, the number of replications and the designs compared.
- **Implementation:** the software and its version, and the random number streams.
- **Code access:** where the code and the data are kept.

---

## Sharing the Model

The framework STARS, by Monks, Harper and Mustafee, "Towards sharing tools and artefacts for reusable simulations in healthcare" ([Journal of Simulation, 2024](https://doi.org/10.1080/17477778.2024.2347882)), says what to share so that others can run and reuse a model. The book gives it in [The STARS Framework](https://des.hsma.co.uk/stars.html). It separates essential parts from optional ones.

- **Essential:** an open licence; a list of the software the model needs, with its versions; free and open software; a minimum of documentation; an identifier for each author; a citation file; a code repository; and an archive that gives the code a permanent identifier.
- **Optional:** fuller documentation, a hosted documentation site, an online environment to run the model in, an interface, and a hosted web app.

---

## What desflow Does for You

Some readiness items cost nothing in desflow, and some need you. Look at the table for which is which.

| Item | In desflow |
|---|---|
| A fixed seed and independent streams | `random_seed` fixes a run, and `replicate` makes one integer seed for each replication from one seed and records them, so the same call gives the same results and any replication can be repeated alone. See [Replications](../user-guide/replications.md) |
| Enough replications, with an interval | `replicate` gives the mean and the [half-width of the 95% interval](output-analysis.md#interval-and-half-width). [How many](output-analysis.md#how-many-replications) is your decision |
| The designs and the inputs on record | An experiment records the compiled parameters of each design and writes its summary, its comparisons, its checks and a `report.md`. See [Experiments](../user-guide/experiments/index.md) |
| A comparison with the real system | An experiment can read an observed file and compare it with the model. See [Experiment Results](../user-guide/experiments/experiment-results.md). Which data to use is your decision |
| A view of the model running | Every event and reading is a record, which you can read or send to a sink. See [Egress, Batching and Delivery](../user-guide/egress.md) |
| Core logic checked | Yours: [Checking and Validating](checking-and-validating.md) shows the cases to work by hand |
| Assumptions, data sources and limits | Yours: they go in the report, and no tool writes them |

---

## A Report for the Shop

A report for the busy-hour question is short. It opens with the answer and its interval. Then it gives:

- the question;
- the model and what it leaves out;
- the inputs and where each came from;
- the run length, the warm-up and the number of replications;
- how the model was checked;
- the limits of its use;
- where the code and the seeds are.

[Common Mistakes](common-mistakes.md) lists what to look for before you send it.
