# Build a Model

Ask an assistant for a desflow model, then check it against your request, check its files before a long run and test it, with the rules and the script of the desflow skill.

[Agent Skill](index.md) shows how to install the skill. With the skill loaded, an assistant that is asked for a desflow model reads its `SKILL.md` first. This page follows the order of that file.

---

## Before You Start

- Install the skill as [Agent Skill](index.md) shows, and install desflow, so that the assistant can run the commands of [Command Line](../cli/index.md).
- [YAML Blueprints](../user-guide/writing-a-model/blueprints.md) describes what the assistant writes.

---

## Write the Model

Ask for a model in your own words, such as "write a desflow blueprint for a shop with two packers and a lunch rush, and run it". The skill tells the assistant to pick a way to write the model. A YAML blueprint is the first choice, because it is checked as a whole before anything runs and an error names the file and line. The Builder API is for a model that needs a payload function or a process in Python. The Low-level API is for a model that needs SimPy itself.

The assistant then downloads the nearest example with `desflow examples list` and `desflow examples get NAME --api yaml`, edits it, and runs it with `desflow run FILE`. [Get an Example](../cli/index.md#get-an-example) shows the commands.

### Draft from a Description

When you describe a process in words and have no model yet, the skill tells the assistant to follow these steps. You can follow the same steps by hand:

1. Start from the nearest example: `desflow examples list`, then `desflow examples get <name> --api yaml`.
2. Build in stages, and run the file after each stage: the arrivals, then the tasks and their order, then the resources, then the service times, then the working hours.
3. Ask for the numbers the description does not give, such as the spread of each time, the share of each branch, the working hours and who does each task, before assuming them.
4. Mark every assumed number with a comment that says `assumed:`. The assistant lists the assumed numbers first in its reply, because you must confirm them.
5. Check the load by hand when `desflow explain` gives no estimate, as it does for a model with scenario steps or a task that routes on a variable. The work for one case, times the arrival rate, must fit inside the capacity of the working hours.
6. Close an arrival outside working hours with a first task that routes on a variable the scenario sets. A scenario step that sets an exponential rate to 0 is refused when the file loads.
7. Stop after three failed attempts at the same error, and read the file and the error yourself.

The repair desk is the worked example these steps were written from. The description is: "A laptop repair desk takes in about 30 laptops a day between 09:00 and 17:00. A technician diagnoses each one in about 20 minutes. Most need a part, which arrives in 1 to 3 days. Repair takes about an hour, then a test. About 1 in 10 fails the test and goes back to repair. There are 3 technicians."

The description does not give the spread of each time, the share that needs a part, how long a test takes, who tests, or the technicians' hours. The blueprint assumes them, and each assumption is marked in a comment. The resources, services and arrivals are below, and the whole file is in [Full Files](#full-files):

=== "YAML"

    ```yaml title="docs/snippets/yaml/repair_desk.yaml:assumptions"
    resources:
      # said: 3 technicians; assumed: they work 09:00 to 17:00
      technicians: {current_cap: 0, max_cap: 3}

    services:
      # said: about 20 minutes; assumed: the spread
      diagnosing: {dist: lognormal, mean: 20 min, std: 10 min}
      waiting_for_part: {dist: uniform, low: 1 day, high: 3 days}  # said: 1 to 3 days
      # said: about an hour; assumed: the spread
      repairing: {dist: lognormal, mean: 1 hour, std: 30 min}
      # assumed: not described
      testing: {dist: triangular, low: 10 min, mode: 15 min, high: 30 min}

    arrivals:
      # said: about 30 a day between 09:00 and 17:00, one every 16 minutes on average while
      # open. Arrivals run all day at that rate, and check_open drops those that come while
      # the desk is closed.
      laptops: {dist: exponential, rate: 0.0010417, spawn: check_open}
    ```

=== "Builder"

    The skill drafts a blueprint, so the drafted model is the YAML tab. [Builder API](../user-guide/writing-a-model/builder-api.md) shows the same model parts in Python.

=== "Low-level"

    The skill drafts a blueprint, so the drafted model is the YAML tab. [Low-level API](../user-guide/writing-a-model/low-level-api.md) shows the same model parts in SimPy code.

The arrivals run all day at the open rate, because a rate of 0 is refused, and `check_open` ends the laptops that arrive while the desk is closed.

The load check follows from the means and the shares. A laptop needs about 107 technician-minutes: 20 to diagnose, 66.7 to repair (60 minutes, done 1 / 0.9 times on average because of the retests) and 20.4 to test. 30 laptops a day need about 53.5 technician-hours. 3 technicians for 8 hours give 24 hours, so the described desk cannot keep up. The assistant reports this before you read the records, and asks whether the numbers or the technicians are wrong.

---

## Check the Model Against the Request

A model can run and still answer another question than yours. Before the assistant reports, the skill tells it to copy this checklist into its reply and tick each line as it is done:

```text
- [ ] List every behaviour the request names, one line each.
- [ ] For each, name the key or the code that makes it, and the record that shows it.
- [ ] Run python <skill folder>/scripts/check_model.py <files>; fix every line it prints; run it again.
- [ ] Find each listed record in the output of desflow run <file> --sample 50.
- [ ] Read each header, docstring and comment against that output.
```

`scripts/check_model.py` is in the skill's folder. The assistant runs it and does not read it. It takes one, two or three forms of the model, in any order:

```bash
python ~/.claude/skills/desflow/scripts/check_model.py model.yaml builder.py low_level.py
```

It runs each Python file as `python FILE`, unpaced, with its sinks replaced by a JSON console, and it reports:

- a file that `desflow check` refuses;
- an import from desflow that the skill's references do not document;
- a draw with no seed, such as Python's `random`, which makes two runs at one seed differ;
- a random generator the model seeds itself, other than one it gives to a `Sampler`;
- the first record that differs between the forms at the model's seed and the two after it;
- a failure of `desflow test` on the YAML form.

Each line it prints names the file and line and what to write instead. The script does not look for private attributes such as `app._sinks`, so read for those yourself. Four rules complete the check:

- A header says only what a run shows. "Adding two agents ends the hang-ups" is wrong when every caller in the run still hangs up. The header must match `--sample`.
- A field that has the same value in every record shows the reader nothing. A `count` over the last 10 records with no `where` is 10 in every report.
- Every constant that is not the request's has a comment with its reason: a run end, a `history`, a tolerance.
- Before the assistant writes that desflow lacks a form, it searches `references/api.md` and `references/blueprint-keys.md` for it, and it gives the cause of a difference only after a run shows it.

---

## Check the Files Before a Long Run

`desflow check` builds each file you give it and does not run it. It takes a blueprint and a Builder Python file. A Low-level file is refused with "defines no SimulationContext at module level", so the script above is the check for a Low-level file.

```bash
desflow check model.yaml builder.py
```

`desflow run --sample N` prints the first N events of a blueprint or a Builder file as JSON lines, unpaced, with no ingress and no real sink, so a model that writes to Kafka or a database needs no service:

```bash
desflow run builder.py --sample 10
```

`desflow check` gives two more messages that matter to a model an assistant wrote. It warns about an arrival with no `max_events` in a file with no `run.until`, `run.stop` or `run.segments`, because `desflow run` then runs until it is stopped. The exit code is still 0:

```text
model.yaml:3: arrivals.orders: the run has no end: this arrival has no max_events, and run has no until or stop, so desflow run runs until it is stopped. Give run.until, or pass --until to desflow run
```

It refuses a `tests` entry that has no seed, in a file with no `simulation.random_seed`, and exits with 1. `desflow test` gives the same text:

```text
Error: model.yaml:8: test 'five_orders' needs a seed: give `seed` in the test or simulation.random_seed in the file
```

A Builder or Low-level file runs with `python FILE`. A `ConsoleEgress` in its default `log` format writes at INFO, so a script that does not set up logging prints no records. The run then gives one warning on stderr that names the line of the script, and the records are the same:

```text
nolog.py:75: UserWarning: ConsoleEgress writes each batch with logging at INFO, which is not enabled, so it prints nothing. Use ConsoleEgress(format="json"), or call logging.basicConfig(level=logging.INFO) before the run.
```

---

## Test the Model

The skill tells the assistant to test the statistics with `desflow test` and the records with pytest, and to read `references/testing.md` before it writes a test. That file gives six rules, each with a wrong and a right example:

- Keep the draws that the request is about, and run at a seed. A test that replaces `weights: {a: 3, b: 1}` with a single branch checks nothing about the odds.
- Compare whole record streams between the forms, not one field. Test each form through the entry point its reader runs.
- Make no assert on the wall clock in an unpaced test. A paced test runs alone and allows for the 15.6 ms step of the Windows clock before Python 3.13.
- Make the test fail on the change it is about. Change the rate, the weight or the capacity by a small amount, and the test must fail. A run that publishes no record passes every "for each record" assert, so assert the record count too.
- Take each expected value from the request, from a hand calculation or from `desflow explain`, never from the model under test. `desflow test --fix` writes what the run gave, so use it only after a deliberate change.
- Let each test stand alone. It must pass alone, in reverse order and under `pytest -n 4`.

The file ends with a pytest template that runs the model with the `desflow` command at three seeds. [Test a Model](../how-to/analysis/test-a-model.md) shows how to write such tests by hand.

---

## Check the Result

Check first that the assistant has the skill, as [Agent Skill](index.md#check-the-install) shows. An assistant can still write a model that runs and answers the wrong question. Read the blueprint before you trust its records, and check that `desflow run` ends without an error. Save the repair desk from [Full Files](#full-files) as `repair_desk.yaml` and run it. The run ends with the teardown lines and no error:

```bash
desflow run repair_desk.yaml
```

```text
[INFO] desflow.core.context: Building SimulationContext for 'Repairs'...
[INFO] desflow.core.context: Simulation engine started.
[INFO] desflow.core.environment: Environment teardown initiated.
[INFO] desflow.core.environment: Tearing down egress connectors, flushing final events...
[INFO] desflow.core.environment: Tearing down ingress connectors...
[INFO] desflow.core.environment: Environment teardown complete.
```

Each line also starts with the wall-clock time. The first task routes on a variable and the model has scenario steps, so the formulas of `desflow explain` give no estimate and the load check above is by hand, as the steps say:

```bash
desflow explain repair_desk.yaml | grep Bottleneck
```

```text
Bottleneck: none can be named: no resource has a utilisation the formulas cover; technicians: task diagnose has no known rate: the split after check_open depends on the run; run --verify to measure it
```

The skill asks the assistant to run a study rather than one run, so ask for the [95% interval](../learn/concepts/output-analysis.md#interval-and-half-width), and read [Trusting Results](../learn/studies/trusting-results.md) for what a number from a simulation supports.

---

## Full Files

??? example "Full files"

    === "YAML"

        ```yaml title="docs/snippets/yaml/repair_desk.yaml"
        # Drafted from a description of a laptop repair desk. Each value says where it came
        # from: "said" means the description gives it, and "assumed" means the user has not
        # confirmed it.
        # Run it with: desflow run repair_desk.yaml
        simulation: {sim_id: Repairs, factor: 0, random_seed: 42}

        variables:
          desk_open: 0  # 1 from 09:00 to 17:00, set by the scenario

        resources:
          # said: 3 technicians; assumed: they work 09:00 to 17:00
          technicians: {current_cap: 0, max_cap: 3}

        services:
          # said: about 20 minutes; assumed: the spread
          diagnosing: {dist: lognormal, mean: 20 min, std: 10 min}
          waiting_for_part: {dist: uniform, low: 1 day, high: 3 days}  # said: 1 to 3 days
          # said: about an hour; assumed: the spread
          repairing: {dist: lognormal, mean: 1 hour, std: 30 min}
          # assumed: not described
          testing: {dist: triangular, low: 10 min, mode: 15 min, high: 30 min}

        arrivals:
          # said: about 30 a day between 09:00 and 17:00, one every 16 minutes on average while
          # open. Arrivals run all day at that rate, and check_open drops those that come while
          # the desk is closed.
          laptops: {dist: exponential, rate: 0.0010417, spawn: check_open}

        tasks:
          check_open:
            payload: {event_type: checked_open}
            next:
              when:
                - {condition: 'registry["Repairs.variables.desk_open"] == 1', task: diagnose}
          diagnose:
            payload: {event_type: diagnosed}
            service: diagnosing
            resource: technicians
            next:
              weights: {order_part: 8, repair: 2}  # said: most need a part; assumed: 8 in 10
          order_part:
            payload: {event_type: part_arrived}
            # a timed step: no resource is held while the part is on its way
            service: waiting_for_part
            next: repair
          repair:
            payload: {event_type: repaired}
            service: repairing
            resource: technicians
            next: test
          test:
            payload: {event_type: tested}
            service: testing
            resource: technicians  # assumed: a technician tests
            next:
              weights: {done: 9, repair: 1}  # said: about 1 in 10 fails and goes back to repair
          done:
            payload: {event_type: returned}

        scenario:  # the desk opens at 09:00 and closes at 17:00 on the first day
          - {at: 32400, path: Repairs.variables.desk_open, value: 1}
          - {at: 32400, path: Repairs.resources.technicians.current_cap, value: 3}
          - {at: 61200, path: Repairs.variables.desk_open, value: 0}
          - {at: 61200, path: Repairs.resources.technicians.current_cap, value: 0}

        run: {until: 1 day}
        ```

    === "Builder"

        The skill drafts a blueprint, so the drafted model is the YAML tab. [Builder API](../user-guide/writing-a-model/builder-api.md) shows the same model parts in Python.

    === "Low-level"

        The skill drafts a blueprint, so the drafted model is the YAML tab. [Low-level API](../user-guide/writing-a-model/low-level-api.md) shows the same model parts in SimPy code.

---

## Sources

The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification). The checklist, the script and the rules are in `skills/desflow/` of the repository.
