# Port from SimPy

Port a SimPy program to desflow with an assistant that has the desflow skill, and check that the port does what the program does.

The skill's file `references/simpy.md` lists where desflow behaves differently from SimPy. An assistant that ports a program reads it, so each row below becomes a decision in the port and not a surprise in the records. [Coming from SimPy](../simpy/coming-from-simpy.md) maps each SimPy construct to its desflow form by hand, and [SimPy Examples](../simpy/examples/index.md) ports the seven programs that ship with SimPy.

---

## Ask for the Port

Give the assistant the SimPy program and ask for the model in the form you want: a YAML blueprint first, then the Builder or Low-level form if the program needs Python. The skill tells it to keep the program's behaviour, so the model has the same arrivals, resources, waits and branches, and publishes the records that show them. [Build a Model](build-a-model.md) shows the checks that follow, which apply to a port in the same way.

---

## Where desflow Differs

Check each row against the program before writing the model, then compare the program's output with `desflow run FILE --sample N`:

| SimPy | desflow | Write |
|---|---|---|
| A program makes a new event after each firing, so a process can wait for the next one. | A signal fires once, and a wait on a signal that has fired returns at once. | `once: false` on a signal that fires more than once. |
| `env.run(until="3.141592")` converts the text to a number. | A time in text needs a unit, so `run.until: "3.141592"` is refused. | The number, `until: 3.141592`, or the text with a unit, `until: 3.14 s`. |
| The capacity of an unbounded container or store is `inf`. | The registry reads it as `None`, and a record holds `null`. | `.inf` in YAML to set no limit; compare a read capacity with `None`. |
| `subscribe_at(event)` interrupts the process with the cause `(event, value)`, a tuple. | The cause's `value` is a mapping with the keys `event` and `value`: the event subscribed to (`null` for a signal) and its value. | `cause.value.value` to read the event's value. |
| A child process that ends by an interrupt it does not catch fails, and the parent decides what follows. | A failure no task handles stops the run. | `run: {on_unhandled_failure: continue}` when the program goes on after it. |
| `out_pipe.put(msg)` with no `yield` puts the item and goes on at once. | A `put` waits until the item is in, then publishes. | `wait: false` in the `put`, which goes on at once as the program does. |
| A program has no reading of the wall clock. | A paced run publishes `system.simulation.lag_seconds`, which depends on the wall clock. | Leave those readings out of a comparison. |
| `env.run()` with no `until` runs until no event is left. | A model with no `run.until` does the same. | Nothing, or `run.until` when the program passes one. |
| A program often counts its time in minutes. | A bare number is seconds. | Each time converted to its unit, such as `mean: 4 min`, or one sentence in the header that says one SimPy minute is one second here. |
| A program prints `env.now` as it is, so `5` for a whole-number time. | `sim_ts` is a float in each record, so `5.0`. | Compare the numbers, not the printed text. |
| An event that has a value but has not yet been processed is `triggered`, not `processed`. | A signal has its value from the moment it fires. | `signal.<name>.count > 0` to ask whether it has fired. |

A time in a blueprint takes a unit, so the assistant writes `mean: 4 min` and not `mean: 240`. The Low-level `DistributionConfig` takes seconds only, so the assistant gives the unit in a comment. Random numbers differ from SimPy's output even at one seed, because desflow draws each value from its own sequence, named by the place where it is used. Compare the shape of the results, such as the mean wait over several runs, and not single numbers.

---

## Check the Port

Run `scripts/check_model.py` on the forms, as [Build a Model](build-a-model.md#check-the-model-against-the-request) shows, and compare the port's records with the program's output at the points the program prints. [Compatibility](../simpy/compatibility.md) lists the SimPy constructs that desflow supports.
