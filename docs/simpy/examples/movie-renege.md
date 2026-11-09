# Movie Renege

A ticket counter sells tickets for three films, and a film that sells out sends its queue away. This is SimPy's Movie Renege example, written in desflow.

---

## Before You Start

SimPy's theatre has one ticket counter and three films with 50 tickets each. A moviegoer arrives about every 0.5 minutes (exponential gaps), picks a film and 1 to 6 tickets, and joins the queue only while the film has tickets. At the counter, a moviegoer who asks for more tickets than are left argues for 0.5 minutes and leaves. Otherwise the sale takes 1 minute. When fewer than 2 tickets are left, the film is sold out and everyone still queueing for it leaves (reneges). The run lasts 120 minutes. desflow reads each of SimPy's minutes as one second.

The example needs only desflow, which each Quick Start in [Run It](#run-it) installs, and no other service. `desflow examples get simpy-movie-renege --api yaml` downloads it, as [Get an Example](../../cli/index.md#get-an-example) shows. [SimPy Examples](index.md) says how these pages read SimPy's times and numbers. SimPy's own page for the example is [on its documentation site](https://simpy.readthedocs.io/en/4.1.2/examples/movie_renege.html).

---

## Steps

### Theater

SimPy's `theater` holds the counter, the tickets left for each film, the time each film sold out and the number of moviegoers who left each queue.

```python title="docs/snippets/simpy/movie_renege.py:theater"
class Theater(NamedTuple):
    counter: simpy.Resource
    movies: List[str]
    available: Dict[str, int]
    sold_out: Dict[str, simpy.Event]
    when_sold_out: Dict[str, Optional[float]]
    num_renegers: Dict[str, int]
```

=== "YAML"

    `available`, `when_sold_out` and `num_renegers` are variables that hold one number for each film. `counter` is a resource with one unit. `sold_out` is a signal family with one member for each film, and `once: true` makes a member fire one time.

    ```yaml title="examples/yaml/simpy/movie_renege.yaml:theater"
    variables:
      available: {Python Unchained: 50, Kill Process: 50, Pulp Implementation: 50}
      when_sold_out: {}
      num_renegers: {Python Unchained: 0, Kill Process: 0, Pulp Implementation: 0}

    resources:
      counter: {current_cap: 1}

    signals:
      sold_out: {family: true, once: true}
    ```

=== "Builder"

    `add_resource`, `add_variable` and `add_signal` declare the same parts.

    ```python title="examples/builder/simpy/movie_renege_example.py:theater"
    app.add_resource("counter", current_cap=1)
    app.add_variable("available", {movie: 50 for movie in MOVIES})
    app.add_variable("when_sold_out", {})
    app.add_variable("num_renegers", {movie: 0 for movie in MOVIES})
    app.add_signal("sold_out", family=True, once=True)
    ```

=== "Low-level"

    The low-level version has SimPy's `simpy.Resource` for the counter, a dictionary for the tickets left and one `env.event()` for each film.

    ```python title="examples/low-level/simpy/movie_renege_example.py:theater"
    counter = simpy.Resource(env, capacity=1)
    available = {movie: TICKETS for movie in MOVIES}
    sold_out = {movie: env.event() for movie in MOVIES}
    numbers = {task: itertools.count() for task in TASKS}
    ```

### Moviegoers Arrive

SimPy's `customer_arrivals` starts a moviegoer after each exponential gap, with a random film and a number of tickets.

```python title="docs/snippets/simpy/movie_renege.py:moviegoers-arrive"
def customer_arrivals(env, theater):
    """Create new *moviegoers* until the sim time reaches 120."""
    while True:
        yield env.timeout(random.expovariate(1 / 0.5))

        movie = random.choice(theater.movies)
        num_tickets = random.randint(1, 6)
        if theater.available[movie]:
            env.process(moviegoer(env, movie, num_tickets, theater))
```

=== "YAML"

    The arrival `goers` has exponential gaps at the rate 2 a second. Each moviegoer has two attributes: `movie`, one of the three films, and `tickets`, an integer from 1 to 6.

    ```yaml title="examples/yaml/simpy/movie_renege.yaml:moviegoers-arrive"
    arrivals:
      goers:
        dist: exponential
        rate: 2
        spawn: moviegoer
        attributes:
          movie:
            gen: one_of
            choices: [Python Unchained, Kill Process, Pulp Implementation]
          tickets: {gen: integer, bounds: [1, 6]}
    ```

=== "Builder"

    `add_arrival` takes the same `attributes`, written as generators.

    ```python title="examples/builder/simpy/movie_renege_example.py:moviegoers-arrive"
    app.add_arrival(
        "goers",
        "exponential",
        rate=2,
        attributes={
            "movie": generator({"gen": "one_of", "choices": MOVIES}),
            "tickets": generator({"gen": "integer", "bounds": [1, 6]}),
        },
        spawn="moviegoer",
    )
    ```

=== "Low-level"

    `customer_arrivals` is SimPy's function. It draws the gap, the film and the number of tickets from desflow's `Sampler`.

    ```python title="examples/low-level/simpy/movie_renege_example.py:moviegoers-arrive"
    def customer_arrivals():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            movie = MOVIES[int(movie_choices.integers(len(MOVIES)))]
            num_tickets = int(ticket_counts.integers(1, 7))
            env.process(moviegoer(f"goers-{number}", movie, num_tickets))

    env.process(customer_arrivals())
    ```

### Moviegoer Waits or Leaves

SimPy's `moviegoer` requests the counter. It waits for either its turn or the film to sell out. If the film sells out first, the moviegoer leaves.

```python title="docs/snippets/simpy/movie_renege.py:moviegoer-buys-or-leaves"
def moviegoer(env, movie, num_tickets, theater):
    """A moviegoer tries to by a number of tickets (*num_tickets*) for
    a certain *movie* in a *theater*.

    If the movie becomes sold out, she leaves the theater. If she gets
    to the counter, she tries to buy a number of tickets. If not enough
    tickets are left, she argues with the teller and leaves.

    If at most one ticket is left after the moviegoer bought her
    tickets, the *sold out* event for this movie is triggered causing
    all remaining moviegoers to leave.

    """
    with theater.counter.request() as my_turn:
        # Wait until it's our turn or until the movie is sold out
        result = yield my_turn | theater.sold_out[movie]

        # Check if it's our turn or if movie is sold out
        if my_turn not in result:
            theater.num_renegers[movie] += 1
            return

        # Check if enough tickets left.
        if theater.available[movie] < num_tickets:
            # Moviegoer leaves after some discussion
            yield env.timeout(0.5)
            return

        # Buy tickets
        theater.available[movie] -= num_tickets
        if theater.available[movie] < SELLOUT_THRESHOLD:
            # Trigger the "sold out" event for the movie
            theater.sold_out[movie].succeed()
            theater.when_sold_out[movie] = env.now
            theater.available[movie] = 0
        yield env.timeout(1)
```

=== "YAML"

    `balk` stops a moviegoer from joining when the film has no tickets left. `open` places a request for the counter and calls it `slot`, and `wait: any` waits for the turn or for the `sold_out` signal of the moviegoer's film, whichever comes first. If the turn came first the moviegoer goes to `serve`. Otherwise it goes to `leave`, which cancels the request and counts a moviegoer who reneged.

    ```yaml title="examples/yaml/simpy/movie_renege.yaml:moviegoer-waits"
    moviegoer:
      balk: "registry['Movie.variables.available'][attr.movie] == 0"
      open:
        slot: {request: counter}
      wait:
        any:
          turn: {handle: slot}
          out:
            signal: {of: sold_out, key: "attr.movie"}
      payload: {event_type: at_counter}
      next:
        when:
          - {condition: "wait.turn.done", task: serve}
        default: leave
    leave:
      cancel: slot
      release: {handle: slot}
      payload:
        event_type: reneged
        movie: {gen: expr, expr: attr.movie}
      on:
        - path: Movie.variables.num_renegers
          value:
            gen: expr
            expr: "set_key(registry['Movie.variables.num_renegers'], attr.movie,
              get_key(registry['Movie.variables.num_renegers'], attr.movie, 0) + 1)"
    ```

=== "Builder"

    The same two tasks, with `open`, `wait` and `next` as in the YAML.

    ```python title="examples/builder/simpy/movie_renege_example.py:moviegoer-waits"
    @app.task(
        name="moviegoer",
        balk="registry['Movie.variables.available'][attr.movie] == 0",
        open={"slot": {"request": "counter"}},
        wait={
            "any": {
                "turn": {"handle": "slot"},
                "out": {"signal": {"of": "sold_out", "key": "attr.movie"}},
            }
        },
        next={
            "when": [{"condition": "wait.turn.done", "task": "serve"}],
            "default": "leave",
        },
    )
    def moviegoer(task_id):
        """Waits for the counter or for the movie to sell out."""
        return {"event_type": "at_counter"}

    leave_payload = Template(
        {"event_type": "reneged", "movie": {"gen": "expr", "expr": "attr.movie"}},
        name="leave",
    )

    @app.task(
        name="leave",
        cancel="slot",
        release={"handle": "slot"},
        on=[
            {
                "path": "Movie.variables.num_renegers",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Movie.variables.num_renegers'], attr.movie, get_key(registry['Movie.variables.num_renegers'], attr.movie, 0) + 1)",
                    }
                ),
            }
        ],
    )
    def leave(task_id):
        """Leaves the queue: the movie sold out."""
        return leave_payload
    ```

=== "Low-level"

    `moviegoer` is SimPy's function, with `turn | sold_out[movie]`. This block shows its first part, up to the moviegoer who leaves.

    ```python title="examples/low-level/simpy/movie_renege_example.py:moviegoer-waits"
    def moviegoer(name, movie, num_tickets):
        number = next(numbers["moviegoer"])

        def publish(task, value, instance=None):
            """Publishes `value` as `instance` of `task`, or as a new instance."""
            step = next(numbers[task]) if instance is None else instance
            env.publish_event(name, value, task=task, instance=step)
            return step

        if available[movie] == 0:  # sold out: does not join the queue
            publish("moviegoer", {"status": "balked"}, number)
            return
        # The task's wait and the counter's request it opens each publish queued
        publish("moviegoer", {"status": "queued"}, number)
        publish("moviegoer", {"status": "queued"}, number)
        turn = counter.request()
        result = yield turn | sold_out[movie]
        if turn in result:  # the request publishes started as it is granted
            publish("moviegoer", {"status": "started"}, number)
        publish("moviegoer", {"status": "started"}, number)
        publish("moviegoer", {"event_type": "at_counter"}, number)
        if turn not in result:  # the movie sold out while it queued
            turn.cancel()
            if turn.triggered:
                counter.release(turn)
            publish("leave", {"event_type": "reneged", "movie": movie})
            return
    ```

### Counter Sells or Argues

At the counter SimPy's `moviegoer` checks the tickets left. A moviegoer who asks for too many argues and leaves. Otherwise the tickets are bought, and the film is sold out when fewer than 2 are left.

=== "YAML"

    `serve` goes to `argue` when the film has fewer tickets left than the moviegoer asks for. `argue` takes 0.5 seconds and releases the counter. Otherwise `buy` takes the tickets from `available`, and `check` goes to `sold` when fewer than 2 are left.

    ```yaml title="examples/yaml/simpy/movie_renege.yaml:counter-sells"
    serve:
      payload: {event_type: served}
      next:
        when:
          - condition: "registry['Movie.variables.available'][attr.movie] < attr.tickets"
            task: argue
        default: buy
    argue:
      service_time: 0.5
      payload: {event_type: argued}
      release: {handle: slot}
    buy:
      payload: {event_type: bought}
      on:
        - path: Movie.variables.available
          value:
            gen: expr
            expr: "set_key(registry['Movie.variables.available'], attr.movie,
              registry['Movie.variables.available'][attr.movie] - attr.tickets)"
      next: check
    check:
      payload: {event_type: checked}
      next:
        when:
          - condition: "registry['Movie.variables.available'][attr.movie] < 2"
            task: sold
        default: finish
    ```

=== "Builder"

    The tasks `serve`, `argue`, `buy` and `check`, with the same rules.

    ```python title="examples/builder/simpy/movie_renege_example.py:counter-sells"
    @app.task(
        name="serve",
        next={
            "when": [
                {
                    "condition": "registry['Movie.variables.available'][attr.movie] < attr.tickets",
                    "task": "argue",
                }
            ],
            "default": "buy",
        },
    )
    def serve(task_id):
        """Reaches the counter."""
        return {"event_type": "served"}

    @app.task(name="argue", service_time=0.5, release={"handle": "slot"})
    def argue(task_id):
        """Asks for more tickets than are left."""
        return {"event_type": "argued"}

    @app.task(
        name="buy",
        on=[
            {
                "path": "Movie.variables.available",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Movie.variables.available'], attr.movie, registry['Movie.variables.available'][attr.movie] - attr.tickets)",
                    }
                ),
            }
        ],
        next="check",
    )
    def buy(task_id):
        """Buys the tickets."""
        return {"event_type": "bought"}

    @app.task(
        name="check",
        next={
            "when": [
                {
                    "condition": "registry['Movie.variables.available'][attr.movie] < 2",
                    "task": "sold",
                }
            ],
            "default": "finish",
        },
    )
    def check(task_id):
        """Checks how many tickets are left."""
        return {"event_type": "checked"}
    ```

=== "Low-level"

    The middle of the same `moviegoer` function.

    ```python title="examples/low-level/simpy/movie_renege_example.py:counter-sells"
    publish("serve", {"event_type": "served"})
    if available[movie] < num_tickets:
        arguing = publish("argue", {"status": "started"})
        yield env.timeout(0.5)
        publish("argue", {"event_type": "argued"}, arguing)
        counter.release(turn)
        return
    publish("buy", {"event_type": "bought"})
    available[movie] -= num_tickets
    publish("check", {"event_type": "checked"})
    ```

### Film Sells Out

When a film is sold out, SimPy's `moviegoer` fires the film's event, which sends the queue away, and records the time. The sale takes 1 minute and the counter is released.

=== "YAML"

    `sold` fires the `sold_out` signal of the film, which ends the wait of everyone queueing for it. It also records the time and sets the tickets left to 0, so later moviegoers balk. `finish` takes 1 second and releases the counter.

    ```yaml title="examples/yaml/simpy/movie_renege.yaml:film-sells-out"
    sold:
      payload:
        event_type: sold_out
        movie: {gen: expr, expr: attr.movie}
      on:
        - fire:
            signal: {of: sold_out, key: "attr.movie"}
        - path: Movie.variables.when_sold_out
          value:
            gen: expr
            expr: "set_key(registry['Movie.variables.when_sold_out'], attr.movie, t)"
        - path: Movie.variables.available
          value:
            gen: expr
            expr: "set_key(registry['Movie.variables.available'], attr.movie, 0)"
      next: finish
    finish:
      service_time: 1
      payload: {event_type: done}
      release: {handle: slot}
    ```

=== "Builder"

    The tasks `sold` and `finish`, with the same rules.

    ```python title="examples/builder/simpy/movie_renege_example.py:film-sells-out"
    sold_payload = Template(
        {"event_type": "sold_out", "movie": {"gen": "expr", "expr": "attr.movie"}},
        name="sold",
    )

    @app.task(
        name="sold",
        on=[
            {"fire": {"signal": {"of": "sold_out", "key": "attr.movie"}}},
            {
                "path": "Movie.variables.when_sold_out",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Movie.variables.when_sold_out'], attr.movie, t)",
                    }
                ),
            },
            {
                "path": "Movie.variables.available",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Movie.variables.available'], attr.movie, 0)",
                    }
                ),
            },
        ],
        next="finish",
    )
    def sold(task_id):
        """Sells the movie out, which sends its queue away."""
        return sold_payload

    @app.task(name="finish", service_time=1, release={"handle": "slot"})
    def finish(task_id):
        """Finishes the sale."""
        return {"event_type": "done"}
    ```

=== "Low-level"

    The end of the same `moviegoer` function.

    ```python title="examples/low-level/simpy/movie_renege_example.py:film-sells-out"
    if available[movie] < 2:
        publish("sold", {"event_type": "sold_out", "movie": movie})
        sold_out[movie].succeed()
        available[movie] = 0
    finishing = publish("finish", {"status": "started"})
    yield env.timeout(1)
    publish("finish", {"event_type": "done"}, finishing)
    counter.release(turn)
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
        desflow examples get simpy-movie-renege --api yaml
        cd simpy-movie-renege/yaml

        # 3. Run it
        desflow run movie_renege.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-movie-renege --api yaml
        cd simpy-movie-renege/yaml

        # 3. Run it
        desflow run movie_renege.yaml
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
        desflow examples get simpy-movie-renege --api builder
        cd simpy-movie-renege/builder

        # 3. Run it
        uv run --no-project --with desflow movie_renege_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-movie-renege --api builder
        cd simpy-movie-renege/builder

        # 3. Run it
        python movie_renege_example.py
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
        desflow examples get simpy-movie-renege --api low-level
        cd simpy-movie-renege/low-level

        # 3. Run it
        uv run --no-project --with desflow movie_renege_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-movie-renege --api low-level
        cd simpy-movie-renege/low-level

        # 3. Run it
        python movie_renege_example.py
        ```

---

## Check the Result

`Kill Process` sells out at 36.662 seconds, `Python Unchained` at 39.662 seconds and `Pulp Implementation` at 43.662 seconds. 17 moviegoers leave the queue for `Python Unchained`, 8 for `Kill Process` and 8 for `Pulp Implementation`. 43 sales go through, and 154 moviegoers find their film sold out and do not join a queue.

SimPy's output, below, has the films selling out at 38, 43 and 28 minutes, with 16, 5 and 5 moviegoers leaving. SimPy prints no count of moviegoers who find a film sold out. Here each one has a `balked` record. desflow draws each random value from its own stream, named by the place where the value is used, such as `Movie.service.patience`. A seed of 42 therefore gives different numbers from SimPy's `random.seed(42)`. The system is the same. The figures are not.

```text title="docs/snippets/simpy/movie_renege.out"
Movie renege
Movie "Python Unchained" sold out 38.0 minutes after ticket counter opening.
  Number of people leaving queue when film sold out: 16
Movie "Kill Process" sold out 43.0 minutes after ticket counter opening.
  Number of people leaving queue when film sold out: 5
Movie "Pulp Implementation" sold out 28.0 minutes after ticket counter opening.
  Number of people leaving queue when film sold out: 5
```

The three versions in desflow print the same records, which [a test](https://github.com/jaehyeon-kim/desflow/blob/main/tests/docs/test_example_twins.py) checks by running each version unpaced. Each record has `sim_ts`, the simulation time, and a key for the entity it belongs to. The statistics and the parameters that a live run can change are in the same stream, which SimPy's file does not have.

---

## Full Files

??? example "Full files"

    === "YAML"

        The blueprint is `movie_renege.yaml`.

        ```yaml title="examples/yaml/simpy/movie_renege.yaml"
        # Movie Renege: SimPy's movie_renege.py example, in YAML.
        #
        # The YAML version of movie_renege_example.py, which --api builder downloads. A movie
        # theatre has one ticket counter and three movies with 50 tickets each. A moviegoer
        # arrives at exponential gaps, picks a movie and 1 to 6 tickets, and does not join the
        # queue once the movie is sold out. At the counter, a moviegoer who asks for more
        # tickets than are left argues and leaves; otherwise the sale goes ahead. When fewer
        # than 2 tickets are left the movie is sold out, and everyone still queueing for it
        # leaves (reneged).
        #
        # SimPy's example counts minutes, and here each of its minutes is one second. So a
        # moviegoer arrives about every 0.5 seconds, an argument takes 0.5 seconds, a sale 1
        # second, and the run ends after 120 seconds, SimPy's 120 minutes. Each value is drawn
        # from desflow's random sequence for its place in the file, so the numbers differ from
        # those SimPy prints.
        #
        # The counter is a request opened by the moviegoer and held across the tasks that
        # follow, and each movie's sold-out event is one member of the signal family
        # `sold_out`.
        #
        # Needs no containers. Runs unpaced and ends after 120 simulation seconds.
        # Run it from its folder with: desflow run movie_renege.yaml

        simulation: {sim_id: Movie, factor: 0, random_seed: 42}

        egress:
          - {type: Console}

        variables:
          available: {Python Unchained: 50, Kill Process: 50, Pulp Implementation: 50}
          when_sold_out: {}
          num_renegers: {Python Unchained: 0, Kill Process: 0, Pulp Implementation: 0}

        resources:
          counter: {current_cap: 1}

        signals:
          sold_out: {family: true, once: true}

        arrivals:
          goers:
            dist: exponential
            rate: 2
            spawn: moviegoer
            attributes:
              movie:
                gen: one_of
                choices: [Python Unchained, Kill Process, Pulp Implementation]
              tickets: {gen: integer, bounds: [1, 6]}

        tasks:
          moviegoer:
            balk: "registry['Movie.variables.available'][attr.movie] == 0"
            open:
              slot: {request: counter}
            wait:
              any:
                turn: {handle: slot}
                out:
                  signal: {of: sold_out, key: "attr.movie"}
            payload: {event_type: at_counter}
            next:
              when:
                - {condition: "wait.turn.done", task: serve}
              default: leave
          leave:
            cancel: slot
            release: {handle: slot}
            payload:
              event_type: reneged
              movie: {gen: expr, expr: attr.movie}
            on:
              - path: Movie.variables.num_renegers
                value:
                  gen: expr
                  expr: "set_key(registry['Movie.variables.num_renegers'], attr.movie,
                    get_key(registry['Movie.variables.num_renegers'], attr.movie, 0) + 1)"
          serve:
            payload: {event_type: served}
            next:
              when:
                - condition: "registry['Movie.variables.available'][attr.movie] < attr.tickets"
                  task: argue
              default: buy
          argue:
            service_time: 0.5
            payload: {event_type: argued}
            release: {handle: slot}
          buy:
            payload: {event_type: bought}
            on:
              - path: Movie.variables.available
                value:
                  gen: expr
                  expr: "set_key(registry['Movie.variables.available'], attr.movie,
                    registry['Movie.variables.available'][attr.movie] - attr.tickets)"
            next: check
          check:
            payload: {event_type: checked}
            next:
              when:
                - condition: "registry['Movie.variables.available'][attr.movie] < 2"
                  task: sold
              default: finish
          sold:
            payload:
              event_type: sold_out
              movie: {gen: expr, expr: attr.movie}
            on:
              - fire:
                  signal: {of: sold_out, key: "attr.movie"}
              - path: Movie.variables.when_sold_out
                value:
                  gen: expr
                  expr: "set_key(registry['Movie.variables.when_sold_out'], attr.movie, t)"
              - path: Movie.variables.available
                value:
                  gen: expr
                  expr: "set_key(registry['Movie.variables.available'], attr.movie, 0)"
            next: finish
          finish:
            service_time: 1
            payload: {event_type: done}
            release: {handle: slot}

        run: {until: 120}
        ```

    === "Builder"

        The script builds the model with the builder API.

        ```python title="examples/builder/simpy/movie_renege_example.py"
        """Movie Renege: SimPy's movie_renege.py example, builder API.

        A movie theatre has one ticket counter and three movies with 50 tickets each. A
        moviegoer arrives at exponential gaps, picks a movie and 1 to 6 tickets, and does not
        join the queue once the movie is sold out. At the counter, a moviegoer who asks for
        more tickets than are left argues and leaves; otherwise the sale goes ahead. When fewer
        than 2 tickets are left the movie is sold out, and everyone still queueing for it
        leaves (reneged).

        SimPy's example counts minutes, and here each of its minutes is one second. So a
        moviegoer arrives about every 0.5 seconds, an argument takes 0.5 seconds, a sale 1
        second, and the run ends after 120 seconds, SimPy's 120 minutes. Each value is drawn
        from desflow's random sequence for its place in the model, so the numbers differ from
        those SimPy prints.

        The counter is a request opened by the moviegoer and held across the tasks that
        follow, and each movie's sold-out event is one member of the signal family `sold_out`.

        Needs no containers. Runs unpaced and ends after 120 simulation seconds.
        """

        import logging

        from desflow import ConsoleEgress, SimulationContext
        from desflow.values import Template, generator

        MOVIES = ["Python Unchained", "Kill Process", "Pulp Implementation"]


        def build() -> SimulationContext:
            """Builds the theatre."""
            app = SimulationContext(sim_id="Movie", factor=0, random_seed=42)
            app.add_egress(ConsoleEgress())
            app.add_resource("counter", current_cap=1)
            app.add_variable("available", {movie: 50 for movie in MOVIES})
            app.add_variable("when_sold_out", {})
            app.add_variable("num_renegers", {movie: 0 for movie in MOVIES})
            app.add_signal("sold_out", family=True, once=True)
            app.add_arrival(
                "goers",
                "exponential",
                rate=2,
                attributes={
                    "movie": generator({"gen": "one_of", "choices": MOVIES}),
                    "tickets": generator({"gen": "integer", "bounds": [1, 6]}),
                },
                spawn="moviegoer",
            )

            @app.task(
                name="moviegoer",
                balk="registry['Movie.variables.available'][attr.movie] == 0",
                open={"slot": {"request": "counter"}},
                wait={
                    "any": {
                        "turn": {"handle": "slot"},
                        "out": {"signal": {"of": "sold_out", "key": "attr.movie"}},
                    }
                },
                next={
                    "when": [{"condition": "wait.turn.done", "task": "serve"}],
                    "default": "leave",
                },
            )
            def moviegoer(task_id):
                """Waits for the counter or for the movie to sell out."""
                return {"event_type": "at_counter"}

            leave_payload = Template(
                {"event_type": "reneged", "movie": {"gen": "expr", "expr": "attr.movie"}},
                name="leave",
            )

            @app.task(
                name="leave",
                cancel="slot",
                release={"handle": "slot"},
                on=[
                    {
                        "path": "Movie.variables.num_renegers",
                        "value": generator(
                            {
                                "gen": "expr",
                                "expr": "set_key(registry['Movie.variables.num_renegers'], attr.movie, get_key(registry['Movie.variables.num_renegers'], attr.movie, 0) + 1)",
                            }
                        ),
                    }
                ],
            )
            def leave(task_id):
                """Leaves the queue: the movie sold out."""
                return leave_payload


            @app.task(
                name="serve",
                next={
                    "when": [
                        {
                            "condition": "registry['Movie.variables.available'][attr.movie] < attr.tickets",
                            "task": "argue",
                        }
                    ],
                    "default": "buy",
                },
            )
            def serve(task_id):
                """Reaches the counter."""
                return {"event_type": "served"}

            @app.task(name="argue", service_time=0.5, release={"handle": "slot"})
            def argue(task_id):
                """Asks for more tickets than are left."""
                return {"event_type": "argued"}

            @app.task(
                name="buy",
                on=[
                    {
                        "path": "Movie.variables.available",
                        "value": generator(
                            {
                                "gen": "expr",
                                "expr": "set_key(registry['Movie.variables.available'], attr.movie, registry['Movie.variables.available'][attr.movie] - attr.tickets)",
                            }
                        ),
                    }
                ],
                next="check",
            )
            def buy(task_id):
                """Buys the tickets."""
                return {"event_type": "bought"}

            @app.task(
                name="check",
                next={
                    "when": [
                        {
                            "condition": "registry['Movie.variables.available'][attr.movie] < 2",
                            "task": "sold",
                        }
                    ],
                    "default": "finish",
                },
            )
            def check(task_id):
                """Checks how many tickets are left."""
                return {"event_type": "checked"}


            sold_payload = Template(
                {"event_type": "sold_out", "movie": {"gen": "expr", "expr": "attr.movie"}},
                name="sold",
            )

            @app.task(
                name="sold",
                on=[
                    {"fire": {"signal": {"of": "sold_out", "key": "attr.movie"}}},
                    {
                        "path": "Movie.variables.when_sold_out",
                        "value": generator(
                            {
                                "gen": "expr",
                                "expr": "set_key(registry['Movie.variables.when_sold_out'], attr.movie, t)",
                            }
                        ),
                    },
                    {
                        "path": "Movie.variables.available",
                        "value": generator(
                            {
                                "gen": "expr",
                                "expr": "set_key(registry['Movie.variables.available'], attr.movie, 0)",
                            }
                        ),
                    },
                ],
                next="finish",
            )
            def sold(task_id):
                """Sells the movie out, which sends its queue away."""
                return sold_payload

            @app.task(name="finish", service_time=1, release={"handle": "slot"})
            def finish(task_id):
                """Finishes the sale."""
                return {"event_type": "done"}


            return app


        app = build()


        def run(model: SimulationContext = app) -> None:
            """Runs 120 simulation seconds."""
            model.run(until=120)


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            run()
        ```

    === "Low-level"

        The script wires the model by hand on `DynamicRealtimeEnvironment`, with SimPy's own processes.

        ```python title="examples/low-level/simpy/movie_renege_example.py"
        """Movie Renege: SimPy's movie_renege.py example, low-level API.

        The low-level version of `builder/simpy/movie_renege_example.py`: SimPy's own
        `moviegoer` and `customer_arrivals` on a `DynamicRealtimeEnvironment`. They publish
        with `publish_event` the records that the YAML and builder versions publish: each
        task's `queued`, `started` and `balked`, its own events, and the number of the task's
        instance. A movie theatre has one ticket counter and three movies with 50 tickets
        each. A moviegoer picks a movie and 1 to 6 tickets, and does not join the queue once
        the movie is sold out. At the counter, a moviegoer who asks for more tickets than are
        left argues and leaves; otherwise the sale goes ahead. When fewer than 2 tickets are
        left the movie is sold out, and everyone still queueing for it leaves.

        SimPy's example counts minutes, and here each of its minutes is one second. So a
        moviegoer arrives about every 0.5 seconds, an argument takes 0.5 seconds, a sale 1
        second, and the run ends after 120 seconds, SimPy's 120 minutes. Each value is drawn
        from desflow's `Sampler`, from the random sequence the YAML and builder versions use
        for the same place, so the three versions publish the same records.

        Needs no containers. Runs unpaced and ends after 120 simulation seconds.
        """

        import itertools
        import logging

        import simpy

        from desflow import (
            ConsoleEgress,
            DistributionConfig,
            DynamicRealtimeEnvironment,
            Sampler,
        )

        MOVIES = ["Python Unchained", "Kill Process", "Pulp Implementation"]
        TICKETS = 50
        UNTIL = 120
        TASKS = ("moviegoer", "leave", "serve", "argue", "buy", "check", "sold", "finish")


        def build(env: DynamicRealtimeEnvironment) -> None:
            """Starts the moviegoers' arrivals on `env`."""
            sampler = Sampler(seed=42)
            gap = DistributionConfig(dist="exponential", rate=2)
            gap.stream = "arrival.goers"  # the sequence the blueprint's arrival draws from
            movie_choices = sampler.generator("arrivals.goers.attributes.movie")
            ticket_counts = sampler.generator("arrivals.goers.attributes.tickets")
            counter = simpy.Resource(env, capacity=1)
            available = {movie: TICKETS for movie in MOVIES}
            sold_out = {movie: env.event() for movie in MOVIES}
            numbers = {task: itertools.count() for task in TASKS}

            def moviegoer(name, movie, num_tickets):
                number = next(numbers["moviegoer"])

                def publish(task, value, instance=None):
                    """Publishes `value` as `instance` of `task`, or as a new instance."""
                    step = next(numbers[task]) if instance is None else instance
                    env.publish_event(name, value, task=task, instance=step)
                    return step

                if available[movie] == 0:  # sold out: does not join the queue
                    publish("moviegoer", {"status": "balked"}, number)
                    return
                # The task's wait and the counter's request it opens each publish queued
                publish("moviegoer", {"status": "queued"}, number)
                publish("moviegoer", {"status": "queued"}, number)
                turn = counter.request()
                result = yield turn | sold_out[movie]
                if turn in result:  # the request publishes started as it is granted
                    publish("moviegoer", {"status": "started"}, number)
                publish("moviegoer", {"status": "started"}, number)
                publish("moviegoer", {"event_type": "at_counter"}, number)
                if turn not in result:  # the movie sold out while it queued
                    turn.cancel()
                    if turn.triggered:
                        counter.release(turn)
                    publish("leave", {"event_type": "reneged", "movie": movie})
                    return
                publish("serve", {"event_type": "served"})
                if available[movie] < num_tickets:
                    arguing = publish("argue", {"status": "started"})
                    yield env.timeout(0.5)
                    publish("argue", {"event_type": "argued"}, arguing)
                    counter.release(turn)
                    return
                publish("buy", {"event_type": "bought"})
                available[movie] -= num_tickets
                publish("check", {"event_type": "checked"})
                if available[movie] < 2:
                    publish("sold", {"event_type": "sold_out", "movie": movie})
                    sold_out[movie].succeed()
                    available[movie] = 0
                finishing = publish("finish", {"status": "started"})
                yield env.timeout(1)
                publish("finish", {"event_type": "done"}, finishing)
                counter.release(turn)

            def customer_arrivals():
                for number in itertools.count(1):
                    yield env.timeout(sampler.sample(gap))
                    movie = MOVIES[int(movie_choices.integers(len(MOVIES)))]
                    num_tickets = int(ticket_counts.integers(1, 7))
                    env.process(moviegoer(f"goers-{number}", movie, num_tickets))

            env.process(customer_arrivals())


        def run() -> None:
            """Runs 120 simulation seconds."""
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

        This is SimPy 4.1.2's `movie_renege.py`, with SimPy's licence notice added at the top and comment lines that mark the pieces shown above. Run with Python, it prints the output shown under Check the Result.

        ```python title="docs/snippets/simpy/movie_renege.py"
        # SimPy 4.1.2, docs/examples/code/movie_renege.py, under SimPy's licence (LICENSE.rst):
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
        Movie renege example

        Covers:

        - Resources: Resource
        - Condition events
        - Shared events

        Scenario:
          A movie theatre has one ticket counter selling tickets for three
          movies (next show only). When a movie is sold out, all people waiting
          to buy tickets for that movie renege (leave queue).

        """

        from __future__ import annotations

        import random
        from typing import Dict, List, NamedTuple, Optional

        import simpy

        RANDOM_SEED = 42
        TICKETS = 50  # Number of tickets per movie
        SELLOUT_THRESHOLD = 2  # Fewer tickets than this is a sellout
        SIM_TIME = 120  # Simulate until


        def moviegoer(env, movie, num_tickets, theater):
            """A moviegoer tries to by a number of tickets (*num_tickets*) for
            a certain *movie* in a *theater*.

            If the movie becomes sold out, she leaves the theater. If she gets
            to the counter, she tries to buy a number of tickets. If not enough
            tickets are left, she argues with the teller and leaves.

            If at most one ticket is left after the moviegoer bought her
            tickets, the *sold out* event for this movie is triggered causing
            all remaining moviegoers to leave.

            """
            with theater.counter.request() as my_turn:
                # Wait until it's our turn or until the movie is sold out
                result = yield my_turn | theater.sold_out[movie]

                # Check if it's our turn or if movie is sold out
                if my_turn not in result:
                    theater.num_renegers[movie] += 1
                    return

                # Check if enough tickets left.
                if theater.available[movie] < num_tickets:
                    # Moviegoer leaves after some discussion
                    yield env.timeout(0.5)
                    return

                # Buy tickets
                theater.available[movie] -= num_tickets
                if theater.available[movie] < SELLOUT_THRESHOLD:
                    # Trigger the "sold out" event for the movie
                    theater.sold_out[movie].succeed()
                    theater.when_sold_out[movie] = env.now
                    theater.available[movie] = 0
                yield env.timeout(1)


        def customer_arrivals(env, theater):
            """Create new *moviegoers* until the sim time reaches 120."""
            while True:
                yield env.timeout(random.expovariate(1 / 0.5))

                movie = random.choice(theater.movies)
                num_tickets = random.randint(1, 6)
                if theater.available[movie]:
                    env.process(moviegoer(env, movie, num_tickets, theater))


        class Theater(NamedTuple):
            counter: simpy.Resource
            movies: List[str]
            available: Dict[str, int]
            sold_out: Dict[str, simpy.Event]
            when_sold_out: Dict[str, Optional[float]]
            num_renegers: Dict[str, int]


        # Setup and start the simulation
        print('Movie renege')
        random.seed(RANDOM_SEED)
        env = simpy.Environment()

        # Create movie theater
        movies = ['Python Unchained', 'Kill Process', 'Pulp Implementation']
        theater = Theater(
            counter=simpy.Resource(env, capacity=1),
            movies=movies,
            available=dict.fromkeys(movies, TICKETS),
            sold_out={movie: env.event() for movie in movies},
            when_sold_out=dict.fromkeys(movies),
            num_renegers=dict.fromkeys(movies, 0),
        )

        # Start process and run
        env.process(customer_arrivals(env, theater))
        env.run(until=SIM_TIME)

        # Analysis/results
        for movie in movies:
            if theater.sold_out[movie]:
                sellout_time = theater.when_sold_out[movie]
                num_renegers = theater.num_renegers[movie]
                print(
                    f'Movie "{movie}" sold out {sellout_time:.1f} minutes '
                    f'after ticket counter opening.'
                )
                print(f'  Number of people leaving queue when film sold out: {num_renegers}')
        ```

---

## Sources

- [SimPy 4.1.2, Movie Renege](https://simpy.readthedocs.io/en/4.1.2/examples/movie_renege.html): the original code and its output.
- [SimPy's licence](https://gitlab.com/team-simpy/simpy/-/blob/master/LICENSE.rst): MIT, Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke. The SimPy file above carries this notice.
- [`examples/` in the desflow repository](https://github.com/jaehyeon-kim/desflow/tree/main/examples): the three versions in desflow, under `simpy/` in each folder.
