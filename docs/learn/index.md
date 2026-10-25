# Learn

Each page here teaches discrete-event simulation (DES), and how desflow does it, to a reader new to both. The pages from What Is DES? to Coming from SimPy explain the ideas. The tutorial then builds one model of a packing shop through a whole study, so read its pages in order. To look up one part of desflow, see the [User Guide](../user-guide/index.md). To do one task step by step, see [How-To](../guides/index.md).

| Group | What it covers |
| --- | --- |
| [What Is DES?](../des/how-it-works.md) | what a discrete-event simulation is, and how it plays out a system one event at a time, with no maths |
| [Queueing Basics](../des/queueing-basics.md) | the results of queueing theory that a study uses: utilisation, Little's law, the effect of variation and the bottleneck |
| [A Simulation Study](../des/a-simulation-study.md) | every step of a study, from the question to the report |
| [Checking and Validating](../des/checking-and-validating.md) | verification, whether the program does what the model on paper says, and validation, whether the model is close enough to the real system |
| [Output Analysis](../des/output-analysis.md) | how runs become an answer with a stated precision: replications, warm-up, intervals and comparing designs |
| [Readiness and Reporting](../des/readiness-and-reporting.md) | what a model needs before someone else can trust and repeat it, with the published checklists |
| [Common Mistakes](../des/common-mistakes.md) | eight mistakes that give wrong results, in the order they happen in a study |
| [SimPy and desflow](../des/simpy-and-desflow.md) | the SimPy constructs desflow is built on, and what desflow adds to them |
| [Coming from SimPy](../des/coming-from-simpy.md) | where each part of a plain SimPy model goes in desflow |
| [Tutorial: A First Model](../tutorials/first-simulation.md) | a first model of the shop: how many packers it needs, random times and seeds, what a run measures and how a run ends |
| [Tutorial: Inputs](../tutorials/choosing-inputs.md) | packing times chosen and fitted from measured data, demand that changes through the day, and a recorded morning replayed |
| [Tutorial: Shop Logic](../tutorials/stock.md) | stock, priorities, a flow of tasks, waits for signals, patience, interruptions, competing resources and filtered inventory |
| [Tutorial: Studies](../tutorials/trusting-results.md) | how many runs to trust, comparing designs, parameter sweeps, checking a model, and a complete study from data to recommendation |
| [Tutorial: Running Live](../tutorials/live-changes.md) | parameters changed while the model runs, and records streamed out to files |
