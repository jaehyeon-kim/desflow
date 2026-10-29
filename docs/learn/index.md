# Learn

Each page here teaches discrete-event simulation (DES), and how desflow does it, to a reader new to both. What Is DES? explains the idea with no maths. The tutorial then builds one model of a packing shop through a whole study, so read its pages in order. The Simulation Concepts pages explain the theory behind the tutorial's steps. If you already know SimPy, see [SimPy and desflow](../simpy/index.md). To look up one part of desflow, see the [User Guide](../user-guide/index.md). To do one task step by step, see [How-To](../how-to/index.md).

| Group | What it covers |
| --- | --- |
| [What Is DES?](what-is-des.md) | what a discrete-event simulation is, and how it plays out a system one event at a time, with no maths |
| [Tutorial: A First Model](first-model/first-simulation.md) | a first model of the shop: how many packers it needs, random times and seeds, what a run measures and how a run ends |
| [Tutorial: Inputs](inputs/choosing-inputs.md) | packing times chosen and fitted from measured data, demand that changes through the day, and a recorded morning replayed |
| [Tutorial: Shop Logic](shop-logic/stock.md) | stock, priorities, a flow of tasks, waits for signals, patience, interruptions, competing resources and filtered inventory |
| [Tutorial: Studies](studies/trusting-results.md) | how many runs to trust, comparing designs, parameter sweeps, checking a model, and a complete study from data to recommendation |
| [Tutorial: Running Live](running-live/live-changes.md) | parameters changed while the model runs, and records streamed out to files |
| [Queueing Basics](concepts/queueing-basics.md) | the results of queueing theory that a study uses: utilisation, Little's law, the effect of variation and the bottleneck |
| [A Simulation Study](concepts/a-simulation-study.md) | every step of a study, from the question to the report |
| [Checking and Validating](concepts/checking-and-validating.md) | verification, whether the program does what the model on paper says, and validation, whether the model is close enough to the real system |
| [Output Analysis](concepts/output-analysis.md) | how runs become an answer with a stated precision: replications, warm-up, intervals and comparing designs |
| [Readiness and Reporting](concepts/readiness-and-reporting.md) | what a model needs before someone else can trust and repeat it, with the published checklists |
| [Common Mistakes](concepts/common-mistakes.md) | eight mistakes that give wrong results, in the order they happen in a study |
