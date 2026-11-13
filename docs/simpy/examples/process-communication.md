# Process Communication

A generator puts messages into a pipe and a consumer takes them, first from one pipe and then from two. This is SimPy's Process Communication example, written in desflow.

---

## Before You Start

SimPy's example has a generator that puts a message into a store every 6 to 10 time units. The message holds the time it was sent. A consumer takes a message and then works on it for 4 to 8 time units, so it takes some messages late. The run lasts 100 time units. SimPy runs the example twice: once with one consumer (one to one), and once with two consumers that each have a pipe of their own, which the generator fills with a broadcast pipe (one to many). SimPy counts no unit, and desflow reads each unit as one second.

The example has two blueprints, `process_communication.yaml` for one to one and `process_communication_broadcast.yaml` for one to many, and a builder and a low-level file for each. `desflow examples get` downloads both.

The example needs only desflow, which each Quick Start in [Run It](#run-it) installs, and no other service. `desflow examples get simpy-process-communication --api yaml` downloads it, as [Get an Example](../../cli/index.md#get-an-example) shows. [SimPy Examples](index.md) says how these pages read SimPy's times and numbers. SimPy's own page for the example is [on its documentation site](https://simpy.readthedocs.io/en/4.1.2/examples/process_communication.html).

---

## Steps

### Generator Sends

SimPy's `message_generator` waits 6 to 10 time units and puts a message with the time it was sent into the pipe, without waiting for the put.

```python title="docs/snippets/simpy/process_communication.py:generator-sends"
def message_generator(name, env, out_pipe):
    """A process which randomly generates messages."""
    while True:
        # wait for next transmission
        yield env.timeout(random.randint(6, 10))

        # messages are time stamped to later check if the consumer was
        # late getting them.  Note, using event.triggered to do this may
        # result in failure due to FIFO nature of simulation yields.
        # (i.e. if at the same env.now, message_generator puts a message
        # in the pipe first and then message_consumer gets from pipe,
        # the event.triggered will be True in the other order it will be
        # False
        msg = (env.now, f'{name} says hello at {env.now}')
        out_pipe.put(msg)
```

=== "YAML"

    `pipe` is a store. The arrival `generator` starts one `speak` task after each gap of 6 to 10 seconds. The task puts an item with the time it was sent, `sent`, and a text into the store. `wait: false` means the task does not wait for the put to finish, as in SimPy.

    ```yaml title="examples/yaml/simpy/process_communication.yaml:pipe"
    stores:
      pipe: {}
    ```

    ```yaml title="examples/yaml/simpy/process_communication.yaml:generator-arrives"
    generator:
      gap: {gen: integer, bounds: [6, 10]}
      spawn: speak
    ```

    ```yaml title="examples/yaml/simpy/process_communication.yaml:generator-sends"
    speak:
      payload: {event_type: said}
      # The generator does not wait for its put, as SimPy's `out_pipe.put(msg)` does not
      put:
        store: pipe
        wait: false
        item:
          sent: {gen: expr, expr: t}
          text:
            gen: format
            template: "Generator A says hello at {now:g}"
            names:
              now: {gen: expr, expr: t}
    ```

=== "Builder"

    `add_store` and `add_arrival` declare the pipe and the generator, and the `speak` task takes `put` with the same item.

    ```python title="examples/builder/simpy/process_communication_example.py:pipe"
    app.add_store("pipe")
    ```

    ```python title="examples/builder/simpy/process_communication_example.py:generator-arrives"
    app.add_arrival(
        "generator",
        gap=generator({"gen": "integer", "bounds": [6, 10]}),
        spawn="speak",
    )
    ```

    ```python title="examples/builder/simpy/process_communication_example.py:generator-sends"
    @app.task(
        name="speak",
        put={
            "store": "pipe",
            "wait": False,
            "item": {
                "sent": generator({"gen": "expr", "expr": "t"}),
                "text": generator(
                    {
                        "gen": "format",
                        "template": "Generator A says hello at {now:g}",
                        "names": {"now": {"gen": "expr", "expr": "t"}},
                    }
                ),
            },
        },
    )
    def speak(task_id):
        """Puts a message into the pipe."""
        return {"event_type": "said"}
    ```

=== "Low-level"

    `message_generator` is SimPy's function. It draws each gap from desflow's `Sampler` and publishes a `said` record.

    ```python title="examples/low-level/simpy/process_communication_example.py:pipe-and-draws"
    sampler = Sampler(seed=42)
    gaps = sampler.generator("arrivals.generator.gap")
    busy_times = sampler.generator("tasks.busy.service_time")
    pipe = simpy.Store(env)
    numbers = {task: itertools.count() for task in ("listen", "busy")}
    ```

    ```python title="examples/low-level/simpy/process_communication_example.py:generator-sends"
    def message_generator(name, out_pipe):
        for number in itertools.count(1):
            yield env.timeout(int(gaps.integers(6, 11)))
            msg = (env.now, f"{name} says hello at {env.now:g}")
            out_pipe.put(msg)  # not waited for, as in SimPy's example
            env.publish_event(
                f"generator-{number}",
                {"event_type": "said"},
                task="speak",
                instance=number - 1,
            )
    ```

### Consumer Gets

SimPy's `message_consumer` takes a message from the pipe, prints it, and works on it for 4 to 8 time units. It is late for a message that was sent before it asked for it.

```python title="docs/snippets/simpy/process_communication.py:consumer-gets"
def message_consumer(name, env, in_pipe):
    """A process which consumes messages."""
    while True:
        # Get event for message pipe
        msg = yield in_pipe.get()

        if msg[0] < env.now:
            # if message was already put into pipe, then
            # message_consumer was late getting to it. Depending on what
            # is being modeled this, may, or may not have some
            # significance
            print(
                f'LATE Getting Message: at time {env.now}: '
                f'{name} received message: {msg[1]}'
            )

        else:
            # message_consumer is synchronized with message_generator
            print(f'at time {env.now}: {name} received message: {msg[1]}.')

        # Process does some other work, which may result in missing messages
        yield env.timeout(random.randint(4, 8))
```

=== "YAML"

    The arrival `consumer` starts one `listen` task at time 0. `listen` takes the next item from the store and publishes `received`, with `late` true when the message waited in the store before the task took it, `item.sent < t`. `busy` works for 4 to 8 seconds and goes back to `listen`.

    ```yaml title="examples/yaml/simpy/process_communication.yaml:consumer-arrives"
    consumer: {initial: 1, spawn: listen}
    ```

    ```yaml title="examples/yaml/simpy/process_communication.yaml:consumer-gets"
    listen:
      get: {store: pipe}
      payload:
        event_type: received
        late: {gen: expr, expr: "item.sent < t"}
        text: {gen: expr, expr: "item.text"}
      next: busy
    busy:
      service_time: {gen: integer, bounds: [4, 8]}
      payload: {event_type: handled}
      next: listen
    ```

=== "Builder"

    `add_initial_arrival` starts the consumer, and the tasks `listen` and `busy` take `get`, `next` and `service_time`.

    ```python title="examples/builder/simpy/process_communication_example.py:consumer-arrives"
    app.add_initial_arrival("consumer", size, spawn="listen")
    ```

    ```python title="examples/builder/simpy/process_communication_example.py:consumer-gets"
    listen_payload = Template(
        {
            "event_type": "received",
            "late": {"gen": "expr", "expr": "item.sent < t"},
            "text": {"gen": "expr", "expr": "item.text"},
        },
        name="listen",
    )

    @app.task(name="listen", get={"store": "pipe"}, next="busy")
    def listen(task_id):
        """Takes the next message."""
        return listen_payload

    @app.task(
        name="busy",
        service_time=generator({"gen": "integer", "bounds": [4, 8]}),
        next="listen",
    )
    def busy(task_id):
        """Works on the message."""
        return {"event_type": "handled"}
    ```

=== "Low-level"

    `message_consumer` is SimPy's function. The last block starts the generator and the consumer.

    ```python title="examples/low-level/simpy/process_communication_example.py:consumer-gets"
    def message_consumer(key, in_pipe):
        def publish(task, number, value):
            env.publish_event(key, value, task=task, instance=number)

        while True:
            listen = next(numbers["listen"])
            publish("listen", listen, {"status": "queued"})
            msg = yield in_pipe.get()
            publish("listen", listen, {"status": "started"})
            # The message is late when it waited in the pipe
            received = {
                "event_type": "received",
                "late": msg[0] < env.now,
                "text": msg[1],
            }
            publish("listen", listen, received)
            busy = next(numbers["busy"])
            publish("busy", busy, {"status": "started"})
            yield env.timeout(int(busy_times.integers(4, 9)))
            publish("busy", busy, {"event_type": "handled"})
    ```

    ```python title="examples/low-level/simpy/process_communication_example.py:start-processes"
    env.process(message_generator("Generator A", pipe))
    for number in range(1, size + 1):
        env.process(message_consumer(f"consumer-{number}", pipe))
    ```

### Broadcast Pipe

SimPy's `BroadcastPipe` keeps one store for each consumer and puts each message into all of them.

```python title="docs/snippets/simpy/process_communication.py:broadcast-pipe"
class BroadcastPipe:
    """A Broadcast pipe that allows one process to send messages to many.

    This construct is useful when message consumers are running at
    different rates than message generators and provides an event
    buffering to the consuming processes.

    The parameters are used to create a new
    :class:`~simpy.resources.store.Store` instance each time
    :meth:`get_output_conn()` is called.

    """

    def __init__(self, env, capacity=simpy.core.Infinity):
        self.env = env
        self.capacity = capacity
        self.pipes = []

    def put(self, value):
        """Broadcast a *value* to all receivers."""
        if not self.pipes:
            raise RuntimeError('There are no output pipes.')
        events = [store.put(value) for store in self.pipes]
        return self.env.all_of(events)  # Condition event for all "events"

    def get_output_conn(self):
        """Get a new output connection for this broadcast pipe.

        The return value is a :class:`~simpy.resources.store.Store`.

        """
        pipe = simpy.Store(self.env, capacity=self.capacity)
        self.pipes.append(pipe)
        return pipe
```

=== "YAML"

    In `process_communication_broadcast.yaml`, `pipe` is a store family with two members. The `speak` task makes the message and goes to `send`, which opens one branch for each member of `pipe` and puts the message into it. Each consumer has an attribute `idx` and takes from the member with that number.

    ```yaml title="examples/yaml/simpy/process_communication_broadcast.yaml:broadcast-pipe"
    stores:
      pipe: {family: true, members: 2}
    ```

    ```yaml title="examples/yaml/simpy/process_communication_broadcast.yaml:consumers-arrive"
    consumer_a:
      initial: 1
      spawn: listen
      attributes: {idx: 0}
    consumer_b:
      initial: 1
      spawn: listen
      attributes: {idx: 1}
    ```

    ```yaml title="examples/yaml/simpy/process_communication_broadcast.yaml:broadcast-sends"
    speak:
      payload:
        event_type: said
        sent: {gen: expr, expr: t}
        text:
          gen: format
          template: "Generator A says hello at {now:g}"
          names:
            now: {gen: expr, expr: t}
      next: send
    send:
      open:
        sends:
          each: "members(pipe)"
          branch:
            put:
              store: {of: pipe, key: "each.value"}
      payload: {event_type: sent}
    ```

    ```yaml title="examples/yaml/simpy/process_communication_broadcast.yaml:consumer-gets"
    listen:
      get:
        store: {of: pipe, key: "attr.idx"}
      payload:
        event_type: received
        late: {gen: expr, expr: "item.sent < t"}
        text: {gen: expr, expr: "item.text"}
      next: busy
    busy:
      service_time: {gen: integer, bounds: [4, 8]}
      payload: {event_type: handled}
      next: listen
    ```

=== "Builder"

    `add_store` takes `family=True` and `members=2`, and the `send` task opens one branch for each member.

    ```python title="examples/builder/simpy/process_communication_broadcast_example.py:broadcast-pipe"
    app.add_store("pipe", family=True, members=2)
    ```

    ```python title="examples/builder/simpy/process_communication_broadcast_example.py:consumers-arrive"
    app.add_initial_arrival("consumer_a", size, spawn="listen", attributes={"idx": 0})
    app.add_initial_arrival("consumer_b", 1, spawn="listen", attributes={"idx": 1})
    ```

    ```python title="examples/builder/simpy/process_communication_broadcast_example.py:broadcast-sends"
    speak_payload = Template(
        {
            "event_type": "said",
            "sent": {"gen": "expr", "expr": "t"},
            "text": {
                "gen": "format",
                "template": "Generator A says hello at {now:g}",
                "names": {"now": {"gen": "expr", "expr": "t"}},
            },
        },
        name="speak",
    )

    # The message is the payload of `speak`, which each put of `send` takes as its item
    @app.task(name="speak", next="send")
    def speak(task_id):
        """Makes a message."""
        return speak_payload

    @app.task(
        name="send",
        open={
            "sends": {
                "each": "members(pipe)",
                "branch": {"put": {"store": {"of": "pipe", "key": "each.value"}}},
            }
        },
    )
    def send(task_id):
        """Puts the message into every pipe."""
        return {"event_type": "sent"}
    ```

    ```python title="examples/builder/simpy/process_communication_broadcast_example.py:consumer-gets"
    listen_payload = Template(
        {
            "event_type": "received",
            "late": {"gen": "expr", "expr": "item.sent < t"},
            "text": {"gen": "expr", "expr": "item.text"},
        },
        name="listen",
    )

    @app.task(
        name="listen", get={"store": {"of": "pipe", "key": "attr.idx"}}, next="busy"
    )
    def listen(task_id):
        """Takes the next message of its own pipe."""
        return listen_payload

    @app.task(
        name="busy",
        service_time=generator({"gen": "integer", "bounds": [4, 8]}),
        next="listen",
    )
    def busy(task_id):
        """Works on the message."""
        return {"event_type": "handled"}
    ```

=== "Low-level"

    `BroadcastPipe` is SimPy's class, with one store for each consumer. The generator and the consumer are SimPy's functions again.

    ```python title="examples/low-level/simpy/process_communication_broadcast_example.py:broadcast-pipe"
    class BroadcastPipe:
        """A pipe that puts each message into the store of every consumer."""

        def __init__(self, env: DynamicRealtimeEnvironment):
            self.env = env
            self.pipes: list = []

        def put(self, value):
            """Puts `value` into every output store."""
            return self.env.all_of([pipe.put(value) for pipe in self.pipes])

        def get_output_conn(self):
            """A new store that receives every message from now on."""
            pipe = simpy.Store(self.env)
            self.pipes.append(pipe)
            return pipe
    ```

    ```python title="examples/low-level/simpy/process_communication_broadcast_example.py:broadcast-sends"
    def message_generator(name, out_pipe):
        for number in itertools.count(1):
            yield env.timeout(int(gaps.integers(6, 11)))
            key = f"generator-{number}"
            msg = {
                "event_type": "said",
                "sent": env.now,
                "text": f"{name} says hello at {env.now:g}",
            }
            out_pipe.put(msg)  # not waited for, as in SimPy's example
            env.publish_event(key, msg, task="speak", instance=number - 1)
            env.publish_event(
                key, {"event_type": "sent"}, task="send", instance=number - 1
            )
    ```

    ```python title="examples/low-level/simpy/process_communication_broadcast_example.py:consumer-gets"
    def message_consumer(key, in_pipe):
        def publish(task, number, value):
            env.publish_event(key, value, task=task, instance=number)

        while True:
            listen = next(numbers["listen"])
            publish("listen", listen, {"status": "queued"})
            msg = yield in_pipe.get()
            publish("listen", listen, {"status": "started"})
            # The message is late when it waited in the pipe
            received = {
                "event_type": "received",
                "late": msg["sent"] < env.now,
                "text": msg["text"],
            }
            publish("listen", listen, received)
            busy = next(numbers["busy"])
            publish("busy", busy, {"status": "started"})
            yield env.timeout(int(busy_times.integers(4, 9)))
            publish("busy", busy, {"event_type": "handled"})
    ```

    ```python title="examples/low-level/simpy/process_communication_broadcast_example.py:start-processes"
    env.process(message_generator("Generator A", bc_pipe))
    for number in range(1, size + 1):
        env.process(message_consumer(f"consumer_a-{number}", pipes[0]))
    env.process(message_consumer("consumer_b-1", pipes[1]))
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
        desflow examples get simpy-process-communication --api yaml
        cd simpy-process-communication/yaml

        # 3. Run it
        desflow run process_communication.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-process-communication --api yaml
        cd simpy-process-communication/yaml

        # 3. Run it
        desflow run process_communication.yaml
        ```

    The one to many run is the second blueprint:

    ```bash
    desflow run process_communication_broadcast.yaml
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
        desflow examples get simpy-process-communication --api builder
        cd simpy-process-communication/builder

        # 3. Run it
        uv run --no-project --with desflow process_communication_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-process-communication --api builder
        cd simpy-process-communication/builder

        # 3. Run it
        python process_communication_example.py
        ```

    The one to many run is the second file:

    ```bash
    uv run --no-project --with desflow process_communication_broadcast_example.py
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
        desflow examples get simpy-process-communication --api low-level
        cd simpy-process-communication/low-level

        # 3. Run it
        uv run --no-project --with desflow process_communication_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-process-communication --api low-level
        cd simpy-process-communication/low-level

        # 3. Run it
        python process_communication_example.py
        ```

    The one to many run is the second file:

    ```bash
    uv run --no-project --with desflow process_communication_broadcast_example.py
    ```

---

## Check the Result

In the one to one run the generator sends 13 messages in the 100 seconds, and the consumer receives 12. The first message is sent at 8 seconds and received at once. The consumer finishes 11 of them. No message is late: the consumer is free each time a message is sent, so it takes each one at once. The 13th message is sent at 99 seconds, while the consumer is still working on the 12th, so it is not taken before the run ends.

In the one to many run the generator also sends 13 messages, and each consumer receives 12. No receipt is late. SimPy prints one line for each message taken, and its output for the one to one run is shown below. desflow draws each random value from its own stream, named by the place where the value is used, such as `Pipes.service.patience`. A seed of 42 therefore gives different numbers from SimPy's `random.seed(42)`. The system is the same. The figures are not.

```text title="docs/snippets/simpy/process_communication.out"
Process communication

One-to-one pipe communication

at time 6: Consumer A received message: Generator A says hello at 6.
at time 12: Consumer A received message: Generator A says hello at 12.
at time 19: Consumer A received message: Generator A says hello at 19.
at time 26: Consumer A received message: Generator A says hello at 26.
at time 36: Consumer A received message: Generator A says hello at 36.
at time 46: Consumer A received message: Generator A says hello at 46.
at time 52: Consumer A received message: Generator A says hello at 52.
at time 58: Consumer A received message: Generator A says hello at 58.
LATE Getting Message: at time 66: Consumer A received message: Generator A says hello at 65
at time 75: Consumer A received message: Generator A says hello at 75.
at time 85: Consumer A received message: Generator A says hello at 85.
at time 95: Consumer A received message: Generator A says hello at 95.

One-to-many pipe communication

at time 10: Consumer A received message: Generator A says hello at 10.
at time 10: Consumer B received message: Generator A says hello at 10.
at time 18: Consumer A received message: Generator A says hello at 18.
at time 18: Consumer B received message: Generator A says hello at 18.
at time 27: Consumer A received message: Generator A says hello at 27.
at time 27: Consumer B received message: Generator A says hello at 27.
at time 34: Consumer A received message: Generator A says hello at 34.
at time 34: Consumer B received message: Generator A says hello at 34.
at time 40: Consumer A received message: Generator A says hello at 40.
LATE Getting Message: at time 41: Consumer B received message: Generator A says hello at 40
at time 46: Consumer A received message: Generator A says hello at 46.
LATE Getting Message: at time 47: Consumer B received message: Generator A says hello at 46
at time 56: Consumer A received message: Generator A says hello at 56.
at time 56: Consumer B received message: Generator A says hello at 56.
at time 65: Consumer A received message: Generator A says hello at 65.
at time 65: Consumer B received message: Generator A says hello at 65.
at time 74: Consumer A received message: Generator A says hello at 74.
at time 74: Consumer B received message: Generator A says hello at 74.
at time 82: Consumer A received message: Generator A says hello at 82.
at time 82: Consumer B received message: Generator A says hello at 82.
at time 92: Consumer A received message: Generator A says hello at 92.
at time 92: Consumer B received message: Generator A says hello at 92.
at time 98: Consumer B received message: Generator A says hello at 98.
at time 98: Consumer A received message: Generator A says hello at 98.
```

The three versions in desflow print the same records, which [a test](https://github.com/jaehyeon-kim/desflow/blob/main/tests/docs/test_example_twins.py) checks by running each version unpaced. Each record has `sim_ts`, the simulation time, and a key for the entity it belongs to. The statistics and the parameters that a live run can change are in the same stream, which SimPy's file does not have.

---

## Full Files

??? example "Full files"

    === "YAML"

        The blueprint is `process_communication.yaml`. The second blueprint, `process_communication_broadcast.yaml`, is the one to many run.

        ```yaml title="examples/yaml/simpy/process_communication.yaml"
        # Process Communication: SimPy's process_communication.py example, one to one, in YAML.
        #
        # The YAML version of process_communication_example.py, which --api builder downloads.
        # A generator puts a message into a store every 6 to 10 seconds, stamped with the time
        # it was sent. A consumer takes each message and then works on it for 4 to 8 seconds,
        # so it is late for some messages: `late` is true when the message waited in the store.
        # process_communication_broadcast.yaml sends each message to two consumers.
        #
        # Each value is drawn from desflow's random sequence for its place in the file, so the
        # numbers differ from those SimPy prints.
        #
        # Needs no containers. Runs unpaced and ends after 100 simulation seconds.
        # Run it from its folder with: desflow run process_communication.yaml

        simulation: {sim_id: Pipes, factor: 0, random_seed: 42}

        egress:
          - {type: Console}

        stores:
          pipe: {}

        arrivals:
          generator:
            gap: {gen: integer, bounds: [6, 10]}
            spawn: speak
          consumer: {initial: 1, spawn: listen}

        tasks:
          speak:
            payload: {event_type: said}
            # The generator does not wait for its put, as SimPy's `out_pipe.put(msg)` does not
            put:
              store: pipe
              wait: false
              item:
                sent: {gen: expr, expr: t}
                text:
                  gen: format
                  template: "Generator A says hello at {now:g}"
                  names:
                    now: {gen: expr, expr: t}
          listen:
            get: {store: pipe}
            payload:
              event_type: received
              late: {gen: expr, expr: "item.sent < t"}
              text: {gen: expr, expr: "item.text"}
            next: busy
          busy:
            service_time: {gen: integer, bounds: [4, 8]}
            payload: {event_type: handled}
            next: listen

        run: {until: 100}
        ```

        ```yaml title="examples/yaml/simpy/process_communication_broadcast.yaml"
        # Process Communication: SimPy's process_communication.py example, broadcast, in YAML.
        #
        # The YAML version of process_communication_broadcast_example.py, which --api builder
        # downloads. A generator makes a message every 6 to 10 seconds, stamped with the time it
        # was sent, and puts a copy into each of two pipes without waiting. Two consumers each
        # take the messages of their own pipe and work on each for 4 to 8 seconds, so they are
        # late for some messages: `late` is true when the message waited in its pipe.
        #
        # Each value is drawn from desflow's random sequence for its place in the file, so the
        # numbers differ from those SimPy prints.
        #
        # Needs no containers. Runs unpaced and ends after 100 simulation seconds.
        # Run it from its folder with: desflow run process_communication_broadcast.yaml

        simulation: {sim_id: Pipes, factor: 0, random_seed: 42}

        egress:
          - {type: Console}

        stores:
          pipe: {family: true, members: 2}

        arrivals:
          generator:
            gap: {gen: integer, bounds: [6, 10]}
            spawn: speak
          consumer_a:
            initial: 1
            spawn: listen
            attributes: {idx: 0}
          consumer_b:
            initial: 1
            spawn: listen
            attributes: {idx: 1}

        tasks:
          # The message is the payload of `speak`, which each put of `send` takes as its item
          speak:
            payload:
              event_type: said
              sent: {gen: expr, expr: t}
              text:
                gen: format
                template: "Generator A says hello at {now:g}"
                names:
                  now: {gen: expr, expr: t}
            next: send
          send:
            open:
              sends:
                each: "members(pipe)"
                branch:
                  put:
                    store: {of: pipe, key: "each.value"}
            payload: {event_type: sent}
          listen:
            get:
              store: {of: pipe, key: "attr.idx"}
            payload:
              event_type: received
              late: {gen: expr, expr: "item.sent < t"}
              text: {gen: expr, expr: "item.text"}
            next: busy
          busy:
            service_time: {gen: integer, bounds: [4, 8]}
            payload: {event_type: handled}
            next: listen

        run: {until: 100}
        ```

    === "Builder"

        The script builds the model with the builder API.

        ```python title="examples/builder/simpy/process_communication_example.py"
        """Process Communication: SimPy's process_communication.py example, one to one, builder API.

        A generator puts a message into a store every 6 to 10 seconds, stamped with the time
        it was sent. A consumer takes each message and then works on it for 4 to 8 seconds, so
        it is late for some messages: `late` is true when the message waited in the store.
        `process_communication_broadcast_example.py` sends each message to two consumers.

        Each value is drawn from desflow's random sequence for its place in the model, so the
        numbers differ from those SimPy prints.

        Needs no containers. Runs unpaced and ends after 100 simulation seconds.
        """

        import logging

        from desflow import ConsoleEgress, SimulationContext
        from desflow.values import Template, generator


        def build(size: int = 1) -> SimulationContext:
            """Builds the pipe, with `size` consumers."""
            app = SimulationContext(sim_id="Pipes", factor=0, random_seed=42)
            app.add_egress(ConsoleEgress())
            app.add_store("pipe")
            app.add_arrival(
                "generator",
                gap=generator({"gen": "integer", "bounds": [6, 10]}),
                spawn="speak",
            )
            app.add_initial_arrival("consumer", size, spawn="listen")

            # The generator does not wait for its put, as SimPy's `out_pipe.put(msg)` does not
            @app.task(
                name="speak",
                put={
                    "store": "pipe",
                    "wait": False,
                    "item": {
                        "sent": generator({"gen": "expr", "expr": "t"}),
                        "text": generator(
                            {
                                "gen": "format",
                                "template": "Generator A says hello at {now:g}",
                                "names": {"now": {"gen": "expr", "expr": "t"}},
                            }
                        ),
                    },
                },
            )
            def speak(task_id):
                """Puts a message into the pipe."""
                return {"event_type": "said"}


            listen_payload = Template(
                {
                    "event_type": "received",
                    "late": {"gen": "expr", "expr": "item.sent < t"},
                    "text": {"gen": "expr", "expr": "item.text"},
                },
                name="listen",
            )

            @app.task(name="listen", get={"store": "pipe"}, next="busy")
            def listen(task_id):
                """Takes the next message."""
                return listen_payload

            @app.task(
                name="busy",
                service_time=generator({"gen": "integer", "bounds": [4, 8]}),
                next="listen",
            )
            def busy(task_id):
                """Works on the message."""
                return {"event_type": "handled"}


            return app


        app = build()


        def run(model: SimulationContext = app) -> None:
            """Runs 100 simulation seconds."""
            model.run(until=100)


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            run()
        ```

        ```python title="examples/builder/simpy/process_communication_broadcast_example.py"
        """Process Communication: SimPy's process_communication.py example, broadcast, builder API.

        A generator makes a message every 6 to 10 seconds, stamped with the time it was sent,
        and puts a copy into each of two pipes without waiting. Two consumers each take the
        messages of their own pipe and work on each for 4 to 8 seconds, so they are late for
        some messages: `late` is true when the message waited in its pipe.

        Each value is drawn from desflow's random sequence for its place in the model, so the
        numbers differ from those SimPy prints.

        Needs no containers. Runs unpaced and ends after 100 simulation seconds.
        """

        import logging

        from desflow import ConsoleEgress, SimulationContext
        from desflow.values import Template, generator


        def build(size: int = 1) -> SimulationContext:
            """Builds the two pipes, with `size` consumers on the first."""
            app = SimulationContext(sim_id="Pipes", factor=0, random_seed=42)
            app.add_egress(ConsoleEgress())
            app.add_store("pipe", family=True, members=2)
            app.add_arrival(
                "generator",
                gap=generator({"gen": "integer", "bounds": [6, 10]}),
                spawn="speak",
            )
            app.add_initial_arrival("consumer_a", size, spawn="listen", attributes={"idx": 0})
            app.add_initial_arrival("consumer_b", 1, spawn="listen", attributes={"idx": 1})
            speak_payload = Template(
                {
                    "event_type": "said",
                    "sent": {"gen": "expr", "expr": "t"},
                    "text": {
                        "gen": "format",
                        "template": "Generator A says hello at {now:g}",
                        "names": {"now": {"gen": "expr", "expr": "t"}},
                    },
                },
                name="speak",
            )

            # The message is the payload of `speak`, which each put of `send` takes as its item
            @app.task(name="speak", next="send")
            def speak(task_id):
                """Makes a message."""
                return speak_payload

            @app.task(
                name="send",
                open={
                    "sends": {
                        "each": "members(pipe)",
                        "branch": {"put": {"store": {"of": "pipe", "key": "each.value"}}},
                    }
                },
            )
            def send(task_id):
                """Puts the message into every pipe."""
                return {"event_type": "sent"}


            listen_payload = Template(
                {
                    "event_type": "received",
                    "late": {"gen": "expr", "expr": "item.sent < t"},
                    "text": {"gen": "expr", "expr": "item.text"},
                },
                name="listen",
            )

            @app.task(
                name="listen", get={"store": {"of": "pipe", "key": "attr.idx"}}, next="busy"
            )
            def listen(task_id):
                """Takes the next message of its own pipe."""
                return listen_payload

            @app.task(
                name="busy",
                service_time=generator({"gen": "integer", "bounds": [4, 8]}),
                next="listen",
            )
            def busy(task_id):
                """Works on the message."""
                return {"event_type": "handled"}


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

        ```python title="examples/low-level/simpy/process_communication_example.py"
        """Process Communication: SimPy's process_communication.py example, one to one, low-level API.

        The low-level version of `builder/simpy/process_communication_example.py`: SimPy's own
        `message_generator` and `message_consumer` on a `DynamicRealtimeEnvironment`. They
        publish with `publish_event` the records that the YAML and builder versions publish:
        each task's `queued` and `started`, its own events, and the number of the task's
        instance. A generator puts a message into a store every 6 to 10 seconds, stamped with
        the time it was sent. A consumer takes each message and then works on it for 4 to 8
        seconds, so it is late for some messages.

        Each value is drawn from desflow's `Sampler`, from the random sequence the YAML and
        builder versions use for the same place, so the three versions publish the same
        records.

        Needs no containers. Runs unpaced and ends after 100 simulation seconds.
        """

        import itertools
        import logging

        import simpy

        from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

        UNTIL = 100


        def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
            """Starts the generator and `size` consumers on `env`."""
            sampler = Sampler(seed=42)
            gaps = sampler.generator("arrivals.generator.gap")
            busy_times = sampler.generator("tasks.busy.service_time")
            pipe = simpy.Store(env)
            numbers = {task: itertools.count() for task in ("listen", "busy")}

            def message_generator(name, out_pipe):
                for number in itertools.count(1):
                    yield env.timeout(int(gaps.integers(6, 11)))
                    msg = (env.now, f"{name} says hello at {env.now:g}")
                    out_pipe.put(msg)  # not waited for, as in SimPy's example
                    env.publish_event(
                        f"generator-{number}",
                        {"event_type": "said"},
                        task="speak",
                        instance=number - 1,
                    )


            def message_consumer(key, in_pipe):
                def publish(task, number, value):
                    env.publish_event(key, value, task=task, instance=number)

                while True:
                    listen = next(numbers["listen"])
                    publish("listen", listen, {"status": "queued"})
                    msg = yield in_pipe.get()
                    publish("listen", listen, {"status": "started"})
                    # The message is late when it waited in the pipe
                    received = {
                        "event_type": "received",
                        "late": msg[0] < env.now,
                        "text": msg[1],
                    }
                    publish("listen", listen, received)
                    busy = next(numbers["busy"])
                    publish("busy", busy, {"status": "started"})
                    yield env.timeout(int(busy_times.integers(4, 9)))
                    publish("busy", busy, {"event_type": "handled"})


            env.process(message_generator("Generator A", pipe))
            for number in range(1, size + 1):
                env.process(message_consumer(f"consumer-{number}", pipe))


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

        ```python title="examples/low-level/simpy/process_communication_broadcast_example.py"
        """Process Communication: SimPy's process_communication.py example, broadcast, low-level API.

        The low-level version of `builder/simpy/process_communication_broadcast_example.py`:
        SimPy's own `BroadcastPipe`, `message_generator` and `message_consumer` on a
        `DynamicRealtimeEnvironment`. They publish with `publish_event` the records that the
        YAML and builder versions publish: each task's `queued` and `started`, its own events,
        and the number of the task's instance. A generator makes a message every 6 to 10
        seconds, stamped with the time it was sent, and puts a copy into each of two pipes
        without waiting. Two consumers each take the messages of their own pipe and work on each
        for 4 to 8 seconds.

        Each value is drawn from desflow's `Sampler`, from the random sequence the YAML and
        builder versions use for the same place, so the three versions publish the same
        records.

        Needs no containers. Runs unpaced and ends after 100 simulation seconds.
        """

        import itertools
        import logging

        import simpy

        from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

        UNTIL = 100


        class BroadcastPipe:
            """A pipe that puts each message into the store of every consumer."""

            def __init__(self, env: DynamicRealtimeEnvironment):
                self.env = env
                self.pipes: list = []

            def put(self, value):
                """Puts `value` into every output store."""
                return self.env.all_of([pipe.put(value) for pipe in self.pipes])

            def get_output_conn(self):
                """A new store that receives every message from now on."""
                pipe = simpy.Store(self.env)
                self.pipes.append(pipe)
                return pipe


        def build(env: DynamicRealtimeEnvironment, size: int = 1) -> None:
            """Starts the generator, `size` consumers on the first pipe and one on the second."""
            sampler = Sampler(seed=42)
            gaps = sampler.generator("arrivals.generator.gap")
            busy_times = sampler.generator("tasks.busy.service_time")
            bc_pipe = BroadcastPipe(env)
            pipes = [bc_pipe.get_output_conn(), bc_pipe.get_output_conn()]
            numbers = {task: itertools.count() for task in ("listen", "busy")}

            def message_generator(name, out_pipe):
                for number in itertools.count(1):
                    yield env.timeout(int(gaps.integers(6, 11)))
                    key = f"generator-{number}"
                    msg = {
                        "event_type": "said",
                        "sent": env.now,
                        "text": f"{name} says hello at {env.now:g}",
                    }
                    out_pipe.put(msg)  # not waited for, as in SimPy's example
                    env.publish_event(key, msg, task="speak", instance=number - 1)
                    env.publish_event(
                        key, {"event_type": "sent"}, task="send", instance=number - 1
                    )


            def message_consumer(key, in_pipe):
                def publish(task, number, value):
                    env.publish_event(key, value, task=task, instance=number)

                while True:
                    listen = next(numbers["listen"])
                    publish("listen", listen, {"status": "queued"})
                    msg = yield in_pipe.get()
                    publish("listen", listen, {"status": "started"})
                    # The message is late when it waited in the pipe
                    received = {
                        "event_type": "received",
                        "late": msg["sent"] < env.now,
                        "text": msg["text"],
                    }
                    publish("listen", listen, received)
                    busy = next(numbers["busy"])
                    publish("busy", busy, {"status": "started"})
                    yield env.timeout(int(busy_times.integers(4, 9)))
                    publish("busy", busy, {"event_type": "handled"})


            env.process(message_generator("Generator A", bc_pipe))
            for number in range(1, size + 1):
                env.process(message_consumer(f"consumer_a-{number}", pipes[0]))
            env.process(message_consumer("consumer_b-1", pipes[1]))


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

        This is SimPy 4.1.2's `process_communication.py`, with SimPy's licence notice added at the top and comment lines that mark the pieces shown above. Run with Python, it prints the output shown under Check the Result.

        ```python title="docs/snippets/simpy/process_communication.py"
        # SimPy 4.1.2, docs/examples/code/process_communication.py, under SimPy's licence (LICENSE.rst):
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
        Process communication example

        Covers:

        - Resources: Store

        Scenario:
          This example shows how to interconnect simulation model elements
          together using :class:`~simpy.resources.store.Store` for one-to-one,
          and many-to-one asynchronous processes. For one-to-many a simple
          BroadCastPipe class is constructed from Store.

        When Useful:
          When a consumer process does not always wait on a generating process
          and these processes run asynchronously. This example shows how to
          create a buffer and also tell is the consumer process was late
          yielding to the event from a generating process.

          This is also useful when some information needs to be broadcast to
          many receiving processes

          Finally, using pipes can simplify how processes are interconnected to
          each other in a simulation model.

        Example By:
          Keith Smith

        """

        import random

        import simpy
        import simpy.core

        RANDOM_SEED = 42
        SIM_TIME = 100


        class BroadcastPipe:
            """A Broadcast pipe that allows one process to send messages to many.

            This construct is useful when message consumers are running at
            different rates than message generators and provides an event
            buffering to the consuming processes.

            The parameters are used to create a new
            :class:`~simpy.resources.store.Store` instance each time
            :meth:`get_output_conn()` is called.

            """

            def __init__(self, env, capacity=simpy.core.Infinity):
                self.env = env
                self.capacity = capacity
                self.pipes = []

            def put(self, value):
                """Broadcast a *value* to all receivers."""
                if not self.pipes:
                    raise RuntimeError('There are no output pipes.')
                events = [store.put(value) for store in self.pipes]
                return self.env.all_of(events)  # Condition event for all "events"

            def get_output_conn(self):
                """Get a new output connection for this broadcast pipe.

                The return value is a :class:`~simpy.resources.store.Store`.

                """
                pipe = simpy.Store(self.env, capacity=self.capacity)
                self.pipes.append(pipe)
                return pipe


        def message_generator(name, env, out_pipe):
            """A process which randomly generates messages."""
            while True:
                # wait for next transmission
                yield env.timeout(random.randint(6, 10))

                # messages are time stamped to later check if the consumer was
                # late getting them.  Note, using event.triggered to do this may
                # result in failure due to FIFO nature of simulation yields.
                # (i.e. if at the same env.now, message_generator puts a message
                # in the pipe first and then message_consumer gets from pipe,
                # the event.triggered will be True in the other order it will be
                # False
                msg = (env.now, f'{name} says hello at {env.now}')
                out_pipe.put(msg)


        def message_consumer(name, env, in_pipe):
            """A process which consumes messages."""
            while True:
                # Get event for message pipe
                msg = yield in_pipe.get()

                if msg[0] < env.now:
                    # if message was already put into pipe, then
                    # message_consumer was late getting to it. Depending on what
                    # is being modeled this, may, or may not have some
                    # significance
                    print(
                        f'LATE Getting Message: at time {env.now}: '
                        f'{name} received message: {msg[1]}'
                    )

                else:
                    # message_consumer is synchronized with message_generator
                    print(f'at time {env.now}: {name} received message: {msg[1]}.')

                # Process does some other work, which may result in missing messages
                yield env.timeout(random.randint(4, 8))


        # Setup and start the simulation
        print('Process communication')
        random.seed(RANDOM_SEED)
        env = simpy.Environment()

        # For one-to-one or many-to-one type pipes, use Store
        pipe = simpy.Store(env)
        env.process(message_generator('Generator A', env, pipe))
        env.process(message_consumer('Consumer A', env, pipe))

        print('\nOne-to-one pipe communication\n')
        env.run(until=SIM_TIME)

        # For one-to many use BroadcastPipe
        # (Note: could also be used for one-to-one,many-to-one or many-to-many)
        env = simpy.Environment()
        bc_pipe = BroadcastPipe(env)

        env.process(message_generator('Generator A', env, bc_pipe))
        env.process(message_consumer('Consumer A', env, bc_pipe.get_output_conn()))
        env.process(message_consumer('Consumer B', env, bc_pipe.get_output_conn()))

        print('\nOne-to-many pipe communication\n')
        env.run(until=SIM_TIME)
        ```

---

## Sources

- [SimPy 4.1.2, Process Communication](https://simpy.readthedocs.io/en/4.1.2/examples/process_communication.html): the original code and its output.
- [SimPy's licence](https://gitlab.com/team-simpy/simpy/-/blob/master/LICENSE.rst): MIT, Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke. The SimPy file above carries this notice.
- [`examples/` in the desflow repository](https://github.com/jaehyeon-kim/desflow/tree/main/examples): the three versions in desflow, under `simpy/` in each folder.
