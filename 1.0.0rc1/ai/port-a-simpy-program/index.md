# Port a SimPy Program

Ask for a port when you have a SimPy program and want its records streamed, its parameters changed during a run, or its study run with desflow's tools. An assistant with the skill writes a desflow model that does what the program does, and compares the two outputs.

______________________________________________________________________

## Ask

```text
Port shop.py, a SimPy program, to desflow.
Keep its behaviour, and compare its mean wait with the desflow model's.
```

______________________________________________________________________

## What the Assistant Does

The skill gives the assistant a table of the places where desflow behaves differently from SimPy, each with what to write in the port. It checks each row against the program before it writes the model. Some of the rows:

- A program that counts time in minutes has each time converted to its unit, such as `mean: 4 min`, because a bare number is seconds.
- A signal fires once by default. One that the program fires more than once gets `once: false`.
- A `put` with no `yield` in the program becomes a `put` with `wait: false`, which goes on at once.
- A failure no task handles stops the run. A program that goes on after a child process fails gets `on_unhandled_failure: continue`.

It picks the way to write the model as for any model: YAML when the program is resources, arrivals and tasks, and the Low-level API when it needs SimPy itself. Then it runs both and compares them: the records with `desflow run <file> --sample`, or the statistics over replications, within their intervals.

______________________________________________________________________

## Example

[Coming from SimPy](https://jaehyeon.me/desflow/latest/simpy/coming-from-simpy/index.md) ports the shop, written as a plain SimPy program in the four classes most tutorials teach. Both versions run 10 times for an hour, and their mean waits agree within their intervals. [SimPy Examples](https://jaehyeon.me/desflow/latest/simpy/examples/index.md) ports the seven examples that ship with SimPy, each in YAML, the Builder and the Low-level API, and compares each with the program's output.

______________________________________________________________________

## Check the Reply

Check that the reply names each row of the table that applies to the program, and what the port writes for it. Then check the comparison: the same records at the same times, or statistics whose intervals overlap. [SimPy Compatibility](https://jaehyeon.me/desflow/latest/simpy/compatibility/index.md) lists every SimPy feature with its form in desflow.
