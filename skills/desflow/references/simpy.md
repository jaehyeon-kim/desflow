# Porting a SimPy program

Where desflow behaves differently from SimPy, and what to write in a port so that the desflow model does what the SimPy program does. Check each row against the program before writing the model, then compare the program's output with `desflow run <file> --sample N`.

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
