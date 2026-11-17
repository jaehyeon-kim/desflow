# Why desflow

I like building demo apps, and every demo app needs data. I could not find enough public APIs that suited my projects, and the fake data scripts I wrote did not work well: each app needed its own, and none of them behaved like a real system.

Then I came across discrete-event simulation (DES). A DES models the process itself: what arrives, which resources serve it and how long each step takes. The data follows from that model, so it has cause and effect, as a real system does. For example, a queue grows because orders arrive faster than packers can handle them. A few numbers, such as an arrival rate, a capacity and a service time, are enough for queues, waits and backlogs that change over time. A simulation also runs much faster than real time, and the same seed gives the same data every time.

[SimPy](https://simpy.readthedocs.io/) is the standard DES library for Python, but it could not do two things I needed: change the parameters of a simulation while it runs, and write the events to external systems. So I started this project, under the name dynamic-des, to add both.

Later I found that a SimPy model needs a lot of code around it: the environment, the resources, the generators and the record of what happened, all written by hand. I had used the builder pattern in PySpark, so I applied it here and added a builder in Python. Not everyone is a Python programmer, though, so I also added a YAML format, an idea I took from Apache Beam YAML.

Next I looked for ways to manage and schedule simulations from a server, an idea inspired by Airflow. That is when I changed the name to desflow. I will build the server while I learn from [Architecture Patterns with Python](https://www.cosmicpython.com/), a book I have wanted to work through.

More features came along the way, and desflow now has feature parity with SimPy. The [Roadmap](https://jaehyeon.me/desflow/latest/about/roadmap/index.md) shows what comes next.

When I learned DES, what I needed was spread across textbooks, lecture notes and papers. So I added a [Learn](https://jaehyeon.me/desflow/latest/des/how-it-works/index.md) section that brings it together in one place and teaches DES itself, not only desflow, with code you can run. It is for anyone who is new to DES, whether or not they use desflow. The [`desflow` command](https://jaehyeon.me/desflow/latest/user-guide/command-line/index.md) supports this learning too: the Learn pages use its `explain` command to check a model against queueing theory.

If you already model with SimPy, desflow keeps what you know: a desflow model is SimPy underneath, with the additions above. [Coming from SimPy](https://jaehyeon.me/desflow/latest/des/coming-from-simpy/index.md) shows where each part of a SimPy model goes.

I use desflow in my own projects, such as the demos in [benchtop](https://github.com/jaehyeon-kim/benchtop). It is useful in many kinds of project. A few examples:

- **Simulation studies.** A question such as how many packers a shop needs takes many runs of a model. In SimPy you write the replications, the statistics and the confidence intervals yourself. desflow collects the statistics, runs the [replications](https://jaehyeon.me/desflow/latest/user-guide/replications/index.md) and compares [designs](https://jaehyeon.me/desflow/latest/user-guide/experiments/index.md) from one command, and `desflow explain` names the bottleneck before anything runs. See [Queues and Capacity](https://jaehyeon.me/desflow/latest/tutorials/queues-and-capacity/index.md) for the full worked example.
- **Data engineering.** Batch and streaming pipelines need data that moves like a real system. desflow writes it to Kafka, Redis, PostgreSQL, Parquet and JSONL files, and Iceberg tables, as history at full speed and then live.
- **ML and MLOps.** A model that predicts delays or load needs training data in which queues build and clear for a reason. When a parameter changes during a run, the data shifts at a known time, which is what a drift monitor has to catch.
- **AI.** An anomaly detector or an AI agent needs a known answer to be scored against. A seeded run can plant a fault and record what it planted as label records. An AI coding assistant can write and check blueprints with the [desflow skill](https://jaehyeon.me/desflow/latest/guides/use-an-ai-assistant/index.md), and after 1.0 the simulation server will let AI agents build, run and change simulations through tools.
