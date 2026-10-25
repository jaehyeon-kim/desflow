# A Simulation Study

Building a model is one step of a simulation study, and often not the largest. This page walks through the whole study, from the question to the report, with the packing shop as the example. Each step says what to produce and names the page that explains it.

The steps follow Law, "How to Conduct a Successful Simulation Study" (Winter Simulation Conference, 2003), which gives a seven-step approach. We read the paper in full. The wording and the shop example are our own. Law notes that programming the model is only 25 to 50 percent of the work of a sound study, despite the common view of simulation as a programming exercise.

______________________________________________________________________

## Steps in Order

Look at the diagram for the order of the steps. Two steps are checks: [Check the Assumptions](#check-the-assumptions) checks the conceptual model, and [Build and Check the Model](#build-and-check-the-model) checks the programmed model. When either check fails, Law's figure sends you back to [Ask One Question](#ask-one-question) and [Conceptual Model and Data](#conceptual-model-and-data), because the problem or the conceptual model may need to change.

______________________________________________________________________

## Ask One Question

Start by writing down the question and the measures that will answer it. A study with no well-defined objective cannot say when it is finished, and Law lists this first among the pitfalls. Without specific questions the right level of model detail cannot be set either. For the shop the question is: **how many packers keep the mean wait under 10 seconds in the busy hour?** The measures are the mean wait of an order and how busy the packers are. The question also fixes the period (the busy hour), the system (the packing area, not the whole warehouse) and who decides (the shop manager).

[Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md) answers this question: 3 packers. [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/index.md) measures the mean wait of an order with 3 packers: 2.32 ± 0.20 seconds, the mean of 40 [replications](https://jaehyeon.me/desflow/latest/des/output-analysis/#replications) with the half-width of its [95% interval](https://jaehyeon.me/desflow/latest/des/output-analysis/#interval-and-half-width). Those runs use the empirical packing times and a 2-minute [warm-up](https://jaehyeon.me/desflow/latest/des/output-analysis/#one-run-starts-empty), and [other pages run the hour in other ways](https://jaehyeon.me/desflow/latest/des/common-mistakes/#busy-hour-figures).

______________________________________________________________________

## Conceptual Model and Data

Before writing code, collect what is known about the system and write the **conceptual model**: what the model includes, the assumptions it makes and the data it needs. Law calls this the assumptions document. For the shop it holds the following:

- Orders arrive one at a time, at a rate that does not change during the hour.
- A packer takes the next order from one queue, in order, and packs it.
- Packing takes 1.5 seconds on average. The first model uses a constant 1.5 seconds, and [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/index.md) replaces it with the times measured at the bench, which the runs above use. Pickers, stock and delivery are outside the model.

Each input is an estimate until it is measured. For the shop these are the arrival rate and the packing time. Take the data from the real system where it exists, choose a distribution for each input, and test how much the choice matters. [Choosing Inputs](https://jaehyeon.me/desflow/latest/tutorials/choosing-inputs/index.md) compares the candidate distributions, and [Fitting the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/index.md) fits them to measured times. A model with an input nobody has measured is still usable, as long as the study tests how far the answer depends on it, as [Parameter Sweeps](https://jaehyeon.me/desflow/latest/tutorials/parameter-sweeps/index.md) does.

The question decides how much detail goes in. Law warns against building one model element for each element of the real system: more detail costs run time, can miss the deadline and can hide the factors that matter. Add detail only when it can change the decision. [Building Blocks](https://jaehyeon.me/desflow/latest/user-guide/building-blocks/index.md) lists the parts a model can have.

### AIC and the KS Test

Fitting a distribution to measured times gives a candidate from each family, such as a gamma or a lognormal, and two scores say how well each candidate matches the data. For the shop's 40 packing times, [Fitting the Inputs](https://jaehyeon.me/desflow/latest/tutorials/fit-the-inputs/#fit-the-packing-times) gives the triangular an AIC of 56.68 and the gamma 56.79.

- **AIC**, Akaike's information criterion, scores how likely the data is under the fitted family, with a penalty for each parameter the fit chose: 2k − 2 × the log-likelihood, where k is the number of parameters. Lower is better. Only the difference between two families' AIC on the same data means anything, and by a common guideline a difference below 2 does not tell two fits apart.
- **KS**, the Kolmogorov-Smirnov statistic, is the largest gap between the share of the data below a value and the share the family puts below it. Its **p-value** is the chance of a gap at least that large if the data came from the family. A small p-value, such as below 0.05, is a reason to reject the family. When the family was fitted to the same data, the p-value is approximate and tends to be too high.

A good score does not settle the choice. Run the model with each close candidate and check whether the answer changes, as Fitting the Inputs does. The sources are the Wikipedia articles on the [Akaike information criterion](https://en.wikipedia.org/wiki/Akaike_information_criterion) and the [Kolmogorov-Smirnov test](https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test).

______________________________________________________________________

## Check the Assumptions

Walk the people who know the system through the assumptions document. Law calls this a structured walk-through. It finds wrong assumptions before code depends on them, and it makes the results more credible to the people who will act on them. Ask the shop manager whether arrivals really are steady in the busy hour, and whether a packer ever does anything else.

______________________________________________________________________

## Build and Check the Model

Write the model, then check it in the two ways [Checking and Validating](https://jaehyeon.me/desflow/latest/des/checking-and-validating/index.md) describes. **Verify** that the program does what the conceptual model says: read the trace, work a case by hand, and compare the statistics with each other. **Validate** that the model is a good enough picture of the shop: compare its results with the real shop's where there is data, and test its sensitivity to the inputs.

The shop's model is built in [First Simulation](https://jaehyeon.me/desflow/latest/tutorials/first-simulation/index.md) and extended in the tutorials that follow it. [Queueing Basics](https://jaehyeon.me/desflow/latest/des/queueing-basics/index.md) gives the formulas that check the queue.

______________________________________________________________________

## Run the Experiments

An experiment is a set of runs designed to answer the question. Decide the run length, the warm-up and the number of replications, then run each design and compute an interval for each measure. [Output Analysis](https://jaehyeon.me/desflow/latest/des/output-analysis/index.md) explains these choices, [Trusting Results](https://jaehyeon.me/desflow/latest/tutorials/trusting-results/index.md) makes them for the shop, and [Comparing Designs](https://jaehyeon.me/desflow/latest/tutorials/comparing-designs/index.md) compares three packers with four. Over 20 replications of each design with the same seeds, a fourth packer cuts the busy hour's mean wait by 2.21 ± 0.31 seconds (the mean difference and the half-width of its 95% interval).

______________________________________________________________________

## Report the Result

State the answer to the question first, then the evidence for it. Say what the model includes and leaves out, what the inputs were and where they came from, how the model was checked, and how precise each result is. A reader who was not part of the study should be able to repeat it. [Readiness and Reporting](https://jaehyeon.me/desflow/latest/des/readiness-and-reporting/index.md) lists what to include, and [Common Mistakes](https://jaehyeon.me/desflow/latest/des/common-mistakes/index.md) lists the errors that make a study wrong.

A study often repeats. A result that surprises is a reason to check the model again, and a changed question is a reason to change it. The steps are an order of work, not a one-way path.
