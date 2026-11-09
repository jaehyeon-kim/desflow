# Event Latency

A sender puts a message on a cable every 5 seconds, the cable delivers it 10 seconds later, and a receiver takes each message as it arrives. This is SimPy's Event Latency example, written in desflow.

---

## Before You Start

SimPy's example keeps the delay of a cable outside the sender and the receiver. The sender puts a message on the cable every 5 time units. The cable holds each message for 10 time units and then puts it into a store. The receiver takes each message from the store. The run lasts 100 time units. SimPy counts no unit, and desflow reads each unit as one second.

Nothing in this example is random, so the times are the same as SimPy's.

The example needs only desflow, which each Quick Start in [Run It](#run-it) installs, and no other service. `desflow examples get simpy-latency --api yaml` downloads it, as [Get an Example](../../cli/index.md#get-an-example) shows. [SimPy Examples](index.md) says how these pages read SimPy's times and numbers. SimPy's own page for the example is [on its documentation site](https://simpy.readthedocs.io/en/4.1.2/examples/latency.html).

---

## Steps

### Cable Delays Messages

SimPy's `Cable` class starts a process for each message. The process waits for the delay and then puts the message into a store. The sender and the receiver never see the delay.

```python title="docs/snippets/simpy/latency.py:cable-delays"
class Cable:
    """This class represents the propagation through a cable."""

    def __init__(self, env, delay):
        self.env = env
        self.delay = delay
        self.store = simpy.Store(env)

    def latency(self, value):
        yield self.env.timeout(self.delay)
        self.store.put(value)

    def put(self, value):
        self.env.process(self.latency(value))

    def get(self):
        return self.store.get()
```

=== "YAML"

    A store named `cable` holds the delivered messages. The `latency` task is the cable's process: it waits `service_time: 10` seconds and then puts its message, `arg.msg`, into the store.

    ```yaml title="examples/yaml/simpy/latency.yaml:cable-store"
    stores:
      cable: {}
    ```

    ```yaml title="examples/yaml/simpy/latency.yaml:cable-delays"
    latency:
      service_time: 10
      put:
        store: cable
        item: {gen: expr, expr: "arg.msg"}
      payload: {event_type: delivered}
    ```

=== "Builder"

    `add_store` declares the store, and a `latency` task with `service_time` and `put` is the cable's process.

    ```python title="examples/builder/simpy/latency_example.py:cable-store"
    app.add_store("cable")
    ```

    ```python title="examples/builder/simpy/latency_example.py:cable-delays"
    @app.task(
        name="latency",
        service_time=10,
        put={"store": "cable", "item": generator({"gen": "expr", "expr": "arg.msg"})},
    )
    def latency(task_id):
        """Delivers the message into the cable's store after the delay."""
        return {"event_type": "delivered"}
    ```

=== "Low-level"

    The low-level version keeps SimPy's `Cable` class. Its `latency` method also publishes the task's records with `publish_event`.

    ```python title="examples/low-level/simpy/latency_example.py:cable-delays"
    class Cable:
        """A cable that delivers each value `delay` seconds after it was put in."""

        def __init__(self, env: DynamicRealtimeEnvironment, delay: float):
            self.env = env
            self.delay = delay
            self.store = simpy.Store(env)
            self.numbers = itertools.count(1)
            self.instances = itertools.count()

        def latency(self, name, value):
            number = next(self.instances)

            def publish(record):
                self.env.publish_event(name, record, task="latency", instance=number)

            publish({"status": "started"})
            yield self.env.timeout(self.delay)
            done = self.store.put(value)
            done.callbacks.append(lambda _: publish({"event_type": "delivered"}))

        def put(self, value):
            """Sends `value`, which travels on its own."""
            name = f"latency-{next(self.numbers)}"
            self.env.process(self.latency(name, value))

        def get(self):
            """The next value delivered."""
            return self.store.get()
    ```

### Sender Sends

SimPy's `sender` waits 5 time units, then puts a message on the cable, and repeats.

```python title="docs/snippets/simpy/latency.py:sender-sends"
def sender(env, cable):
    """A process which randomly generates messages."""
    while True:
        # wait for next transmission
        yield env.timeout(5)
        cable.put(f'Sender sent this at {env.now}')
```

=== "YAML"

    An arrival with a constant gap of 5 seconds starts one `send` task for each message. The task starts a `latency` task with the message text as its argument and goes on at once, so the sender never waits for the cable.

    ```yaml title="examples/yaml/simpy/latency.yaml:senders-arrive"
    sender: {dist: constant, value: 5, spawn: send}
    ```

    ```yaml title="examples/yaml/simpy/latency.yaml:sender-sends"
    send:
      payload: {event_type: sent}
      # Each message travels on its own, so the sender goes on at once
      spawn:
        task: latency
        args:
          msg:
            gen: format
            template: "Sender sent this at {now:g}"
            names:
              now: {gen: expr, expr: t}
    ```

=== "Builder"

    `add_arrival` starts one `send` task every 5 seconds, and the task's `spawn` starts the `latency` task with the message as its argument.

    ```python title="examples/builder/simpy/latency_example.py:senders-arrive"
    app.add_arrival("sender", "constant", value=5, spawn="send")
    ```

    ```python title="examples/builder/simpy/latency_example.py:sender-sends"
    # Each message travels on its own, so the sender goes on at once
    @app.task(
        name="send",
        spawn={
            "task": "latency",
            "args": {
                "msg": generator(
                    {
                        "gen": "format",
                        "template": "Sender sent this at {now:g}",
                        "names": {"now": {"gen": "expr", "expr": "t"}},
                    }
                )
            },
        },
    )
    def send(task_id):
        """Sends a message."""
        return {"event_type": "sent"}
    ```

=== "Low-level"

    The sender is SimPy's loop. It also publishes a `sent` record for each message.

    ```python title="examples/low-level/simpy/latency_example.py:sender-sends"
    def sender():
        for number in itertools.count(1):
            yield env.timeout(5)
            env.publish_event(
                f"sender-{number}",
                {"event_type": "sent"},
                task="send",
                instance=number - 1,
            )
            cable.put(f"Sender sent this at {env.now:g}")
    ```

### Receiver Gets

SimPy's `receiver` takes a message from the cable and prints it, and repeats.

```python title="docs/snippets/simpy/latency.py:receiver-gets"
def receiver(env, cable):
    """A process which consumes messages."""
    while True:
        # Get event for message pipe
        msg = yield cable.get()
        print(f'Received this at {env.now} while {msg}')
```

=== "YAML"

    One `receive` task starts at time 0. It takes the next message from the store, publishes it as `received`, and starts again with `next: receive`.

    ```yaml title="examples/yaml/simpy/latency.yaml:receiver-arrives"
    receiver: {initial: 1, spawn: receive}
    ```

    ```yaml title="examples/yaml/simpy/latency.yaml:receiver-gets"
    receive:
      get: {store: cable}
      payload:
        event_type: received
        msg: {gen: expr, expr: item}
      next: receive
    ```

=== "Builder"

    `add_initial_arrival` starts one `receive` task at time 0. The task takes from the store and starts again with `next`.

    ```python title="examples/builder/simpy/latency_example.py:receiver-arrives"
    app.add_initial_arrival("receiver", size, spawn="receive")
    ```

    ```python title="examples/builder/simpy/latency_example.py:receiver-gets"
    receive_payload = Template(
        {"event_type": "received", "msg": {"gen": "expr", "expr": "item"}},
        name="receive",
    )

    @app.task(name="receive", get={"store": "cable"}, next="receive")
    def receive(task_id):
        """Takes the next message."""
        return receive_payload
    ```

=== "Low-level"

    The receiver is SimPy's loop around `cable.get()`. The last block starts the sender and the receiver.

    ```python title="examples/low-level/simpy/latency_example.py:receiver-gets"
    def receiver(name):
        def publish(number, record):
            env.publish_event(name, record, task="receive", instance=number)

        while True:
            number = next(gets)  # each message is taken by a new instance
            publish(number, {"status": "queued"})
            msg = yield cable.get()
            publish(number, {"status": "started"})
            publish(number, {"event_type": "received", "msg": msg})
    ```

    ```python title="examples/low-level/simpy/latency_example.py:start-processes"
    env.process(sender())
    for number in range(1, size + 1):
        env.process(receiver(f"receiver-{number}"))
    ```

### Run It

=== "YAML"

    `desflow run` loads the blueprint and runs it unpaced, because it sets `factor: 0`. Each record is printed to the terminal.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get simpy-latency --api yaml
        cd simpy-latency/yaml

        # 3. Run it
        desflow run latency.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-latency --api yaml
        cd simpy-latency/yaml

        # 3. Run it
        desflow run latency.yaml
        ```

=== "Builder"

    The script builds the model with the builder API, runs it unpaced, and prints each record to the terminal.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get simpy-latency --api builder
        cd simpy-latency/builder

        # 3. Run it
        uv run --no-project --with desflow latency_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-latency --api builder
        cd simpy-latency/builder

        # 3. Run it
        python latency_example.py
        ```

=== "Low-level"

    The script wires the model by hand on `DynamicRealtimeEnvironment`, runs it unpaced, and prints each record to the terminal.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get simpy-latency --api low-level
        cd simpy-latency/low-level

        # 3. Run it
        uv run --no-project --with desflow latency_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-latency --api low-level
        cd simpy-latency/low-level

        # 3. Run it
        python latency_example.py
        ```

---

## Check the Result

The first message is sent at 5 seconds and the receiver gets it at 15 seconds, as the record `Sender sent this at 5`. By 100 seconds the sender has sent 19 messages and the receiver has received 17. The last one received arrives at 95 seconds and reads `Sender sent this at 85`. The two newest messages are still on the cable when the run ends.

SimPy prints one line for each message received, and its file is shown below. The times in it are the same as the times here.

```text title="docs/snippets/simpy/latency.out"
Event Latency
Received this at 15 while Sender sent this at 5
Received this at 20 while Sender sent this at 10
Received this at 25 while Sender sent this at 15
Received this at 30 while Sender sent this at 20
Received this at 35 while Sender sent this at 25
Received this at 40 while Sender sent this at 30
Received this at 45 while Sender sent this at 35
Received this at 50 while Sender sent this at 40
Received this at 55 while Sender sent this at 45
Received this at 60 while Sender sent this at 50
Received this at 65 while Sender sent this at 55
Received this at 70 while Sender sent this at 60
Received this at 75 while Sender sent this at 65
Received this at 80 while Sender sent this at 70
Received this at 85 while Sender sent this at 75
Received this at 90 while Sender sent this at 80
Received this at 95 while Sender sent this at 85
```

The three versions in desflow print the same records, which [a test](https://github.com/jaehyeon-kim/desflow/blob/main/tests/docs/test_example_twins.py) checks by running each version unpaced. Each record has `sim_ts`, the simulation time, and a key for the entity it belongs to. The statistics and the parameters that a live run can change are in the same stream, which SimPy's file does not have.

---

## Full Files

??? example "Full files"

    === "YAML"

        The blueprint is `latency.yaml`.

        ```yaml title="examples/yaml/simpy/latency.yaml"
        # Event Latency: SimPy's latency.py example, in YAML.
        #
        # The YAML version of latency_example.py, which --api builder downloads. A sender
        # sends a message every 5 seconds over a cable. The cable delivers each message into a
        # store 10 seconds later, and a receiver takes each message as it arrives.
        #
        # Needs no containers. Runs unpaced and ends after 100 simulation seconds.
        # Run it from its folder with: desflow run latency.yaml

        simulation: {sim_id: Cable, factor: 0, random_seed: 42}

        egress:
          - {type: Console}

        stores:
          cable: {}

        arrivals:
          sender: {dist: constant, value: 5, spawn: send}
          receiver: {initial: 1, spawn: receive}

        tasks:
          send:
            payload: {event_type: sent}
            # Each message travels on its own, so the sender goes on at once
            spawn:
              task: latency
              args:
                msg:
                  gen: format
                  template: "Sender sent this at {now:g}"
                  names:
                    now: {gen: expr, expr: t}
          latency:
            service_time: 10
            put:
              store: cable
              item: {gen: expr, expr: "arg.msg"}
            payload: {event_type: delivered}
          receive:
            get: {store: cable}
            payload:
              event_type: received
              msg: {gen: expr, expr: item}
            next: receive

        run: {until: 100}
        ```

    === "Builder"

        The script builds the model with the builder API.

        ```python title="examples/builder/simpy/latency_example.py"
        """Event Latency: SimPy's latency.py example, builder API.

        A sender sends a message every 5 seconds over a cable. The cable delivers each message
        into a store 10 seconds later, and a receiver takes each message as it arrives.

        Needs no containers. Runs unpaced and ends after 100 simulation seconds.
        """

        import logging

        from desflow import ConsoleEgress, SimulationContext
        from desflow.values import Template, generator


        def build(size: int = 1) -> SimulationContext:
            """Builds the cable, with `size` receivers."""
            app = SimulationContext(sim_id="Cable", factor=0, random_seed=42)
            app.add_egress(ConsoleEgress())
            app.add_store("cable")
            app.add_arrival("sender", "constant", value=5, spawn="send")
            app.add_initial_arrival("receiver", size, spawn="receive")

            # Each message travels on its own, so the sender goes on at once
            @app.task(
                name="send",
                spawn={
                    "task": "latency",
                    "args": {
                        "msg": generator(
                            {
                                "gen": "format",
                                "template": "Sender sent this at {now:g}",
                                "names": {"now": {"gen": "expr", "expr": "t"}},
                            }
                        )
                    },
                },
            )
            def send(task_id):
                """Sends a message."""
                return {"event_type": "sent"}


            @app.task(
                name="latency",
                service_time=10,
                put={"store": "cable", "item": generator({"gen": "expr", "expr": "arg.msg"})},
            )
            def latency(task_id):
                """Delivers the message into the cable's store after the delay."""
                return {"event_type": "delivered"}


            receive_payload = Template(
                {"event_type": "received", "msg": {"gen": "expr", "expr": "item"}},
                name="receive",
            )

            @app.task(name="receive", get={"store": "cable"}, next="receive")
            def receive(task_id):
                """Takes the next message."""
                return receive_payload


            return app


        app = build()


        def run(model: SimulationContext = app) -> None:
            """Runs 100 simulation seconds."""
            model.run(until=100)


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            run()
        ```

    === "Low-level"

        The script wires the model by hand on `DynamicRealtimeEnvironment`, with SimPy's own processes.

        ```python title="examples/low-level/simpy/latency_example.py"
        """Event Latency: SimPy's latency.py example, low-level API.

        The low-level version of `builder/simpy/latency_example.py`: SimPy's own `Cable`,
        sender and receiver on a `DynamicRealtimeEnvironment`. They publish with
        `publish_event` the records that the YAML and builder versions publish: each task's
        `queued` and `started`, its own events, and the number of the task's instance. A
        sender sends a message every 5 seconds over a cable. The cable delivers each message
        into a store 10 seconds later, and a receiver takes each message as it arrives.

        Needs no containers. Runs unpaced and ends after 100 simulation seconds.
        """

        import itertools
        import logging

        import simpy

        from desflow import ConsoleEgress, DynamicRealtimeEnvironment

        UNTIL = 100


        class Cable:
            """A cable that delivers each value `delay` seconds after it was put in."""

            def __init__(self, env: DynamicRealtimeEnvironment, delay: float):
                self.env = env
                self.delay = delay
                self.store = simpy.Store(env)
                self.numbers = itertools.count(1)
                self.instances = itertools.count()

            def latency(self, name, value):
                number = next(self.instances)

                def publish(record):
                    self.env.publish_event(name, record, task="latency", instance=number)

                publish({"status": "started"})
                yield self.env.timeout(self.delay)
                done = self.store.put(value)
                done.callbacks.append(lambda _: publish({"event_type": "delivered"}))

            def put(self, value):
                """Sends `value`, which travels on its own."""
                name = f"latency-{next(self.numbers)}"
                self.env.process(self.latency(name, value))

            def get(self):
                """The next value delivered."""
                return self.store.get()


        def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
            """Starts the sender and `size` receivers on `env`."""
            cable = Cable(env, 10)
            gets = itertools.count()

            def sender():
                for number in itertools.count(1):
                    yield env.timeout(5)
                    env.publish_event(
                        f"sender-{number}",
                        {"event_type": "sent"},
                        task="send",
                        instance=number - 1,
                    )
                    cable.put(f"Sender sent this at {env.now:g}")


            def receiver(name):
                def publish(number, record):
                    env.publish_event(name, record, task="receive", instance=number)

                while True:
                    number = next(gets)  # each message is taken by a new instance
                    publish(number, {"status": "queued"})
                    msg = yield cable.get()
                    publish(number, {"status": "started"})
                    publish(number, {"event_type": "received", "msg": msg})


            env.process(sender())
            for number in range(1, size + 1):
                env.process(receiver(f"receiver-{number}"))


        def run() -> None:
            """Runs 100 simulation seconds."""
            env = DynamicRealtimeEnvironment(factor=0)
            env.setup_egress([ConsoleEgress()])
            build(env)
            try:
                env.run(until=UNTIL)
            finally:
                env.teardown()


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            run()
        ```

    === "SimPy"

        This is SimPy 4.1.2's `latency.py`, with SimPy's licence notice added at the top and comment lines that mark the pieces shown above. Run with Python, it prints the output shown under Check the Result.

        ```python title="docs/snippets/simpy/latency.py"
        # SimPy 4.1.2, docs/examples/code/latency.py, under SimPy's licence (LICENSE.rst):
        #
        # The MIT License (MIT)
        #
        # Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke (also see AUTHORS.txt)
        #
        # Permission is hereby granted, free of charge, to any person obtaining a copy of
        # this software and associated documentation files (the "Software"), to deal in
        # the Software without restriction, including without limitation the rights to
        # use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
        # the Software, and to permit persons to whom the Software is furnished to do so,
        # subject to the following conditions:
        #
        # The above copyright notice and this permission notice shall be included in all
        # copies or substantial portions of the Software.
        #
        # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
        # FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
        # COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
        # IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
        # CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

        """
        Event Latency example

        Covers:

        - Resources: Store

        Scenario:
          This example shows how to separate the time delay of events between
          processes from the processes themselves.

        When Useful:
          When modeling physical things such as cables, RF propagation, etc.  it
          better encapsulation to keep this propagation mechanism outside of the
          sending and receiving processes.

          Can also be used to interconnect processes sending messages

        Example by:
          Keith Smith

        """

        import simpy

        SIM_DURATION = 100


        class Cable:
            """This class represents the propagation through a cable."""

            def __init__(self, env, delay):
                self.env = env
                self.delay = delay
                self.store = simpy.Store(env)

            def latency(self, value):
                yield self.env.timeout(self.delay)
                self.store.put(value)

            def put(self, value):
                self.env.process(self.latency(value))

            def get(self):
                return self.store.get()


        def sender(env, cable):
            """A process which randomly generates messages."""
            while True:
                # wait for next transmission
                yield env.timeout(5)
                cable.put(f'Sender sent this at {env.now}')


        def receiver(env, cable):
            """A process which consumes messages."""
            while True:
                # Get event for message pipe
                msg = yield cable.get()
                print(f'Received this at {env.now} while {msg}')


        # Setup and start the simulation
        print('Event Latency')
        env = simpy.Environment()

        cable = Cable(env, 10)
        env.process(sender(env, cable))
        env.process(receiver(env, cable))

        env.run(until=SIM_DURATION)
        ```

---

## Sources

- [SimPy 4.1.2, Event Latency](https://simpy.readthedocs.io/en/4.1.2/examples/latency.html): the original code and its output.
- [SimPy's licence](https://gitlab.com/team-simpy/simpy/-/blob/master/LICENSE.rst): MIT, Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke. The SimPy file above carries this notice.
- [`examples/` in the desflow repository](https://github.com/jaehyeon-kim/desflow/tree/main/examples): the three versions in desflow, under `simpy/` in each folder.
