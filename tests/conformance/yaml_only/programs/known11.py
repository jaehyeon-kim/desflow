"""The SimPy programs of the Batch 11 known cases (families, references, `wait: {each}`,
failed and defused events, event priority and `listen`), one for each document in
`tests/conformance/yaml_only/known11/`. Each is the
program of the prototype's comparison, written from the original SimPy test or example
or from the design's program, and returns (log, error): `log` holds (time, key, task,
payload) as the blueprint's records do. The harness (`new`, `arrival`, `go`) is the one
of `known8`."""

import simpy

from tests.conformance.yaml_only.programs.known8 import (  # noqa: F401
    MODE,
    PENDING,
    PROCS,
    _arrival,
    arrival,
    go,
    new,
)


def S(status, **extra):
    return dict(status=status, **extra)


def _number(key):
    """The number an entity key ends in, from 1."""
    return int(key.rsplit("-", 1)[1])


def DS_Y1():
    # research/yaml-only/programs/Y1.py: at_start(3) (four jobs of 2 seconds over three
    # resources of one unit, in turn) and reply (a reply event for each entity whose key
    # travels in a store item; the server fires the reply of an item after one second)
    env, log, L = new()
    desks = [simpy.Resource(env, capacity=1) for _ in range(3)]
    q = simpy.Store(env)
    replies = {}

    def event(i):
        return replies.setdefault(i, env.event())

    def job(k):
        i = _number(k) - 1
        with desks[i % 3].request() as request:
            yield request
            yield env.timeout(2)
        L(k, "job", {"event_type": "got", "i": i})

    def client(i):
        def run(k):
            yield q.put(i)
            L(k, "client", {"event_type": "put", "i": i})
            if i == 1:
                yield env.timeout(5)
            L(k, "waits", {"event_type": "waits", "i": i, "at": env.now})
            value = yield event(i)
            L(k, "hear", {"event_type": "reply", "i": i, "v": value, "at": env.now})

        return run

    def server(k):
        while True:
            item = yield q.get()
            yield env.timeout(1)
            L(
                k,
                "server_fire",
                {"event_type": "server_fired", "i": item, "at": env.now},
            )
            if not event(item).triggered:
                event(item).succeed(item * 10)

    arrival(env, "jobs", job, initial=4)
    arrival(env, "c0", client(0), gap=1, max_events=1, first=0)
    arrival(env, "c1", client(1), gap=1, max_events=1, first=0)
    arrival(env, "srv", server, gap=1, max_events=1, first=0)
    return go(env, log, 20)


def DS_Y2():
    # research/yaml-only/programs/Y2.py: pick(shortest) (the member with the fewest in
    # line and in use, the first of equal ones), pick(i % 2) (a member of a list by an
    # index) and same_step() (two entities at one instant read the state in turn)
    env, log, L = new()
    desk = [simpy.Resource(env, 1) for _ in range(3)]
    named = [simpy.Resource(env, 1) for _ in range(3)]
    pair = [simpy.Resource(env, 1) for _ in range(2)]

    def pick(rs):
        return min(range(len(rs)), key=lambda n: len(rs[n].queue) + rs[n].count)

    def shortest(k):
        i = _number(k) - 1
        with desk[pick(desk)].request() as request:
            yield request
            yield env.timeout(1 + i)
        L(k, "shortest", {"event_type": "done", "i": i})

    def by_index(k):
        i = _number(k) - 1
        with named[i % 2].request() as request:
            yield request
            yield env.timeout(1 + i)
        L(k, "by_index", {"event_type": "done", "i": i})

    def same_step(k):
        with pair[pick(pair)].request() as request:
            yield request
            yield env.timeout(1)
        L(k, "same_step", {"event_type": "done"})

    arrival(env, "a", shortest, gap=0.5, max_events=5, first=0)
    arrival(env, "b", by_index, gap=0.5, max_events=5, first=0)
    arrival(env, "d", same_step, initial=2)
    return go(env, log)


def DS_Y3():
    # research/yaml-only/programs/Y3.py: a wait on a computed number of children and
    # their values (all), a wait for the first of a list of timeouts (any), and a put to
    # every member of a family (each, the pipes of process_communication.py)
    env, log, L = new()
    pipes = [simpy.Store(env) for _ in range(3)]

    def child(i):
        yield env.timeout(i + 1)
        return {"event_type": "worker", "v": i * 10}

    def fan(k):
        children = []
        for i in range(3):
            children.append(env.process(child(i)))

        def logged(i, process):
            value = yield process
            L(k, "worker", value)

        for i, process in enumerate(children):
            env.process(logged(i, process))
        results = yield env.all_of(children)
        L(
            k,
            "fan",
            {"event_type": "all_done", "values": [results[c] for c in children]},
        )
        yield env.any_of([env.timeout(d) for d in [4, 1, 2]])
        L(k, "first", {"event_type": "first_done"})
        yield env.all_of([p.put(5) for p in pipes])
        L(k, "broadcast", {"event_type": "sent"})

    arrival(env, "a", fan, initial=1)
    return go(env, log)


def ST_U07():
    # tests/test_util.py test_all_of: all_of over ten timeouts of 0 to 9 seconds
    env, log, L = new()

    def pem(k):
        events = [env.timeout(d, value=d) for d in range(10)]
        yield env.all_of(events)
        L(k, "pem", {"event_type": "all_done", "at": env.now})

    arrival(env, "a", pem, initial=1)
    return go(env, log)


def ST_U08():
    # tests/test_util.py test_all_of_generator: the same all_of over a generator, with
    # the values 0 to 9 in order
    env, log, L = new()

    def pem(k):
        events = (env.timeout(d, value=d) for d in range(10))
        results = yield env.all_of(events)
        L(
            k,
            "pem",
            {
                "event_type": "all_done",
                "at": env.now,
                "values": list(results.values()),
            },
        )

    arrival(env, "a", pem, initial=1)
    return go(env, log)


def ST_U09():
    # tests/test_util.py test_wait_for_all_with_errors: all_of of timeout(1), a process
    # that fails at 2 with 'crashing' and timeout(3); the parent catches the failure
    env, log, L = new()

    def child_with_error(k):
        yield env.timeout(2)
        L(k, "child_with_error", {"status": "failed", "reason": "crashing"})
        raise ValueError("crashing")

    def pem(k):
        child = env.process(child_with_error(k))
        try:
            yield env.all_of([env.timeout(1, value=1), child, env.timeout(3, value=3)])
        except ValueError as error:
            L(k, "pem", {"status": "failed", "reason": str(error)})
            L(
                k,
                "caught",
                {"event_type": "caught", "reason": str(error), "at": env.now},
            )

    arrival(env, "a", pem, initial=1)
    return go(env, log)


def P64():
    # patterns/h_signals.py H1: customers 0 and 1 wait, the clerk wakes customer 1 at 2
    env, log, L = new()
    events = {}
    keys = iter(str(n) for n in range(1000))

    def customer(k):
        yield events.setdefault(k, env.event())
        L(k, "sleep", {"woken": 1})

    def wake_one(k):
        events.setdefault("1", env.event()).succeed()
        L(k, "wake_one", {})
        yield env.timeout(0)

    arrival(env, "c", customer, initial=2, keyfn=lambda: next(keys))
    arrival(env, "k", wake_one, gap=2, max_events=1)
    return go(env, log, 10)


def CV_074():
    # research/yaml-only/programs/Y3.py requests(3): three resources held for 3, 2 and 1
    # seconds; at 0.5 a process requests all three and waits for the first
    env, log, L = new()
    desks = [simpy.Resource(env, capacity=1) for _ in range(3)]

    def hold(index, seconds):
        def run(k):
            with desks[index].request() as request:
                yield request
                yield env.timeout(seconds)
            L(k, f"hold{index}", {"event_type": f"held{index}"})

        return run

    def pause(k):
        yield env.timeout(0.5)
        L(k, "pause", {"event_type": "paused"})
        requests = [d.request() for d in desks]
        yield env.any_of(requests)
        L(k, "any_desk", {"event_type": "got_one", "at": env.now})
        for index, request in enumerate(requests):
            if request.triggered:
                desks[index].release(request)
            else:
                request.cancel()

    arrival(env, "h0", hold(0, 3), initial=1)
    arrival(env, "h1", hold(1, 2), initial=1)
    arrival(env, "h2", hold(2, 1), initial=1)
    arrival(env, "p", pause, initial=1)
    return go(env, log)


def CV_083():
    # research/yaml-only/programs/X1.py subclass(): a get takes min(level, amount) once
    # the level is at least 1, and gets are served in the order they were made
    env, log, L = new()
    state = {"level": 0, "tickets": 0, "turn": 0}
    changed = env.event()

    def poke():
        nonlocal changed
        old, changed = changed, env.event()
        old.succeed()

    def ticket(amount):
        def run(k):
            i = state["tickets"]
            state["tickets"] += 1
            L(k, "ticket", {"event_type": "ticket"})
            while not (state["level"] >= 1 and state["turn"] == i):
                yield changed
            taken = min(state["level"], amount)
            L(k, "get_it", {"event_type": "got", "taken": taken})
            state["level"] -= taken
            state["turn"] += 1
            poke()

        return run

    def put_it(amount):
        def run(k):
            state["level"] += amount
            L(k, "put_it", {"event_type": "put"})
            poke()
            yield env.timeout(0)

        return run

    arrival(env, "g0", ticket(3), initial=1)
    arrival(env, "g1", ticket(1), initial=1)
    arrival(env, "g2", ticket(4), initial=1)
    arrival(env, "p1", put_it(2), gap=1, max_events=1)
    arrival(env, "p2", put_it(5), gap=2, max_events=1)
    return go(env, log)


def EX_7b():
    # docs/examples/code/process_communication.py (broadcast): the generator puts a
    # message to every output pipe without waiting; each consumer reads its own pipe,
    # handles it for 6 seconds and reads again. The gap (8) and the handling time (6) are
    # the fixed values the document writes in place of the draws.
    env, log, L = new()
    pipes = [simpy.Store(env) for _ in range(2)]

    def speak(k):
        message = {
            "event_type": "said",
            "sent": env.now,
            "text": "Generator A says hello at %g" % env.now,
        }
        for pipe in pipes:
            pipe.put(message)
        L(k, "speak", message)
        L(k, "send", {"event_type": "sent"})
        yield env.timeout(0)

    def listen(index):
        def run(k):
            while True:
                item = yield pipes[index].get()
                L(
                    k,
                    "listen",
                    {
                        "event_type": "received",
                        "late": item["sent"] < env.now,
                        "text": item["text"],
                    },
                )
                yield env.timeout(6)
                L(k, "busy", {"event_type": "handled"})

        return run

    arrival(env, "generator", speak, gap=8)
    arrival(env, "consumer_a", listen(0), initial=1)
    arrival(env, "consumer_b", listen(1), initial=1)
    return go(env, log, 100)


def EX_4():
    # docs/examples/code/movie_renege.py in the form of the document: one counter, three
    # movies with 50 tickets each, and a sold-out event for each movie that makes the
    # moviegoers waiting for the counter leave. The movie of arrival n and its tickets
    # are the fixed values the document writes in place of the draws.
    env, log, L = new()
    movies = ["Python Unchained", "Kill Process", "Pulp Implementation"]
    counter = simpy.Resource(env, capacity=1)
    available = {m: 50 for m in movies}
    sold_out = {m: env.event() for m in movies}

    def moviegoer(k):
        n = _number(k) - 1
        movie = movies[(n * 7) % 3 if n % 5 else 0]
        tickets = 1 + (n * 5) % 6
        if available[movie] == 0:
            L(k, "moviegoer", {"status": "balked"})
            return
        turn = counter.request()
        result = yield turn | sold_out[movie]
        L(k, "moviegoer", {"event_type": "at_counter"})
        if turn not in result:
            turn.cancel()
            if turn.triggered:
                counter.release(turn)
            L(k, "leave", {"event_type": "reneged"})
            return
        L(k, "serve", {"event_type": "served"})
        if available[movie] < tickets:
            yield env.timeout(0.5)
            L(k, "argue", {"event_type": "argued"})
            counter.release(turn)
            return
        L(k, "buy", {"event_type": "bought"})
        available[movie] -= tickets
        L(k, "check", {"event_type": "checked"})
        if available[movie] < 2:
            L(k, "sold", {"event_type": "sold_out"})
            sold_out[movie].succeed()
            available[movie] = 0
        yield env.timeout(1)
        L(k, "finish", {"event_type": "done"})
        counter.release(turn)

    arrival(env, "goers", moviegoer, gap=0.5)
    return go(env, log, 120)


def GD_V2():
    # events.rst:111-126: succeed(value), fail(exc) and trigger(event). One process
    # waits on `a`, one catches the failure of `b`, and the driver acts on all three
    # events at 1
    env, log, L = new()
    a, b, c = env.event(), env.event(), env.event()

    def ok_waiter(k):
        value = yield a
        L(k, "ok_waiter", {"event_type": "ok", "v": value})

    def fail_waiter(k):
        try:
            yield b
            L(k, "fail_waiter", {"event_type": "fine"})
        except ValueError as error:
            L(k, "fail_waiter", S("failed", reason=str(error)))
            L(k, "caught", {"event_type": "caught", "why": str(error)})

    def driver(k):
        yield env.timeout(1)
        L(k, "driver", {"event_type": "drive"})
        a.succeed(7)
        b.fail(ValueError("oops"))
        c.trigger(a)

    arrival(env, "x", ok_waiter, initial=1)
    arrival(env, "y", fail_waiter, initial=1)
    arrival(env, "z", driver, initial=1)
    return go(env, log)


def ST_C06():
    # tests/test_condition.py test_nested_cond_with_error, test_cond_with_error and
    # test_cond_with_nested_error, one after the other: a child process fails inside an
    # all_of, an any_of and a nested condition, and the parent catches the failure
    env, log, L = new()

    def explode(k, stage, delay, text):
        yield env.timeout(delay)
        L(k, f"explode_{stage}", S("failed", reason=text))
        raise ValueError(text)

    def parent(k):
        for stage, (delay, text, how) in enumerate(
            [
                (1, "Onoes!", "all"),
                (0, "Onoes, failed after 0!", "any"),
                (0, "Onoes, failed after 0!", "nested"),
            ],
            1,
        ):
            child = env.process(explode(k, stage, delay, text))
            if how == "all":
                condition = child & env.timeout(1)
            elif how == "any":
                condition = child | env.timeout(1)
            else:
                condition = (child & env.timeout(1)) | env.timeout(1)
            try:
                yield condition
                L(k, f"stage_{stage}", {"event_type": "unreachable"})
            except ValueError as error:
                L(k, f"stage_{stage}", S("failed", reason=str(error)))
                L(k, f"caught_{stage}", {"event_type": "caught", "reason": str(error)})

    arrival(env, "a", parent, initial=1)
    return go(env, log)


def ST_C09():
    # tests/test_condition.py test_cond_with_uncaught_error: timeout(1) | process(explode(2));
    # the condition fires at 1, the child fails at 2 and nobody handles it, so run()
    # raises
    env, log, L = new()

    def explode(k):
        yield env.timeout(2)
        L(k, "explode", S("failed", reason="Onoes, failed after 2!"))
        raise ValueError("Onoes, failed after 2!")

    def pem(k):
        yield env.timeout(1) | env.process(explode(k))
        L(k, "pem", {"event_type": "done", "at": env.now})

    arrival(env, "a", pem, initial=1)
    return go(env, log)


def ST_U03():
    # tests/test_util.py test_subscribe_terminated_proc: subscribe_at on a process that has
    # ended raises RuntimeError, which the test catches, so nothing is recorded
    from simpy.util import subscribe_at

    env, log, L = new()

    def child(k):
        yield env.timeout(1)
        L(k, "child", {"event_type": "child_done"})

    def parent(k):
        yield env.timeout(2)
        L(k, "parent", {"event_type": "parent"})
        try:
            subscribe_at(PROCS["c-1"])
            raise AssertionError("subscribe_at of an ended process must raise")
        except RuntimeError:
            pass

    arrival(env, "c", child, initial=1)
    arrival(env, "p", parent, initial=1)
    return go(env, log)


def ST_V02():
    # tests/test_event.py test_fail: the parent starts a child that waits for an event and
    # fails the event at 5 with ValueError('ohai'); the child catches it. A second parent
    # fails the one event again, which stops the run in SimPy and in desflow. The child
    # of the parent that starts n-th is `child-n`
    env, log, L = new()
    ev = env.event()

    def child(k):
        try:
            yield ev
            L(k, "child", {"event_type": "fine"})
        except ValueError as error:
            L(k, "child", S("failed", reason=str(error)))
            L(k, "caught", {"event_type": "caught", "reason": str(error)})

    made = [0]

    def parent(k):
        yield env.timeout(5)
        L(k, "parent", {"event_type": "parent"})
        made[0] += 1
        env.process(child(f"child-{made[0]}"))
        ev.fail(ValueError("ohai"))

    arrival(env, "p", parent, initial=1)
    return go(env, log)


# Programs whose arrivals are written through a helper, so their source has no `initial=`
SIZED = ("ST_X03", "ST_X04", "ST_X05")


def _x_case(how):
    # tests/test_exceptions.py: a failed event that nobody handles (ST-X03), whose callback
    # defuses it (ST-X04), or that a process handles (ST-X05). The rows take a size. In
    # ST-X03 and ST-X04 more entities fail the one event again, which stops the run in
    # SimPy and in desflow. In ST-X05 the size adds waiters, which all handle the one failure
    env, log, L = new()
    ev = env.event()

    def fail_it(k):
        L(k, "fail_it", {"event_type": "fail"})
        ev.fail(ValueError(""))
        return
        yield  # pragma: no cover

    def pem(k):
        try:
            yield ev
            L(k, "pem", {"event_type": "should_not_finish"})
        except ValueError as error:
            L(k, "pem", S("failed", reason=str(error)))
            L(k, "caught", {"event_type": "caught"})

    if how == "defused":
        ev.callbacks.append(lambda event: setattr(event, "defused", True))
    if how == "handled":
        arrival(env, "w", pem, initial=1)
    arrival(env, "a" if how != "handled" else "f", fail_it, initial=1)
    return go(env, log, 1)


def ST_X03():
    return _x_case("unhandled")


def ST_X04():
    return _x_case("defused")


def ST_X05():
    return _x_case("handled")


def P78():
    # patterns REPORT.md P78, simmer untrap: a callback added to the event of a door
    # that rings three times at 2, 4 and 6 (a new event after each ring). Each watcher
    # adds one callback to the event of the first ring and, as `untrap` does, a flag says
    # whether it still listens. One watcher clears the flag at once, the other does not.
    env, log, L = new()
    door = [env.event()]
    heard = [0]
    flags = {}

    def watcher(stops):
        def run(k):
            flags[k] = 1

            def callback(event, k=k):
                if flags[k] == 1:
                    heard[0] += 1

            door[0].callbacks.append(callback)
            L(
                k,
                "watch" if stops else "keep_listening",
                {"event_type": "watch" if stops else "keep_listening"},
            )
            if stops:
                flags[k] = 0
            return
            yield  # pragma: no cover

        return run

    def ring(k):
        L(k, "ring", {"event_type": "ring"})
        event, door[0] = door[0], env.event()
        event.succeed()
        return
        yield  # pragma: no cover

    def report(k):
        L(k, "report", {"event_type": "report", "heard": heard[0]})
        return
        yield  # pragma: no cover

    arrival(env, "w2", watcher(False), initial=1)
    arrival(env, "w", watcher(True), initial=1)
    arrival(env, "d", ring, gap=2, max_events=3)
    arrival(env, "r", report, gap=7, max_events=1)
    return go(env, log)


def DS_M3():
    # research/yaml-only/programs/M3.py: a process waits on an event and handles its failure;
    # a second process fails the event at 1 with the reason "event"
    env, log, L = new()
    ev = env.event()

    def t(k):
        try:
            yield ev
            L(k, "t", {"event_type": "fine"})
        except ValueError as error:
            L(k, "t", S("failed", reason=str(error)))
            L(k, "handle", {"event_type": "handled", "reason": str(error)})

    def breaker(k):
        L(k, "breaker", {"event_type": "break"})
        ev.fail(ValueError("event"))
        return
        yield  # pragma: no cover

    arrival(env, "a", t, initial=1)
    arrival(env, "b", breaker, gap=1, max_events=1)
    return go(env, log)


def DS_Y9():
    # research/yaml-only/programs/Y9.py: a waiter of an event failed from a flow (waiter),
    # a failure defused with the failer going on (defused), and a reason computed from the
    # entity and raised and caught in the process (child_raises, in one process)
    env, log, L = new()
    ev, dv = env.event(), env.event()

    def w(k):
        try:
            value = yield ev
            L(k, "w", {"event_type": "value", "v": value})
        except ValueError as error:
            L(k, "w", S("failed", reason=error.args[0]))
            L(
                k,
                "caught",
                {"event_type": "caught", "why": error.args[0], "at": env.now},
            )

    def f(k):
        yield env.timeout(1)
        ev.fail(ValueError(f"bad {7}"))

    def d(k):
        yield env.timeout(1)
        L(k, "d", {"event_type": "failer_goes_on"})
        dv.fail(ValueError("x"))
        dv.defused = True
        yield env.timeout(1)
        L(k, "d_end", {"event_type": "failer_ended"})

    def c(k):
        yield env.timeout(1)
        try:
            raise KeyError(f"missing {3}")
        except KeyError as error:
            L(k, "c", S("failed", reason=error.args[0]))
            L(
                k,
                "c_caught",
                {"event_type": "caught", "why": error.args[0], "at": env.now},
            )

    arrival(env, "w", w, initial=1)
    arrival(env, "f", f, initial=1)
    arrival(env, "d", d, initial=1)
    arrival(env, "c", c, initial=1)
    return go(env, log)


def DS_Y10():
    # research/yaml-only/programs/Y10.py run(2): an event scheduled at 1 with priority 2
    # runs after every normal event at 1, including the ones those events make at 1
    env, log, L = new()
    marked = env.event()

    def later(name):
        def run(k):
            yield env.timeout(1)
            L(k, "later", {"event_type": "later", "name": name})
            yield env.timeout(0)
            L(k, "again", {"event_type": "again", "name": name})

        return run

    def mark(k):
        marked._ok, marked._value = True, "m"
        env.schedule(marked, priority=2, delay=1)
        return
        yield  # pragma: no cover

    def see(k):
        yield marked
        L(k, "see", {"event_type": "marked"})

    arrival(env, "a", later("a"), initial=1)
    arrival(env, "m", mark, initial=1)
    arrival(env, "b", later("b"), initial=1)
    arrival(env, "w", see, initial=1)
    return go(env, log)


def DS_Y11():
    # research/yaml-only/programs/Y11.py: order() (a first waiter, a callback attached at
    # 1, a late waiter at 1.5, the event fired at 2 with 5) and conditional(2) and (0)
    env, log, L = new()
    ev, d2, d0 = env.event(), env.event(), env.event()
    shared = {"cb": 0, "n2": 0, "n0": 0}

    def first_waiter(k):
        value = yield ev
        L(
            k,
            "first_waiter",
            {
                "event_type": "first_waiter",
                "value": value,
                "cb": shared["cb"],
                "now": env.now,
            },
        )

    def attach(k):
        yield env.timeout(1)
        ev.callbacks.append(lambda e: shared.__setitem__("cb", e.value))

    def fire(k):
        yield env.timeout(2)
        ev.succeed(5)

    def late(k):
        yield env.timeout(1.5)
        value = yield ev
        L(
            k,
            "late_wait",
            {
                "event_type": "late_waiter",
                "value": value,
                "cb": shared["cb"],
                "now": env.now,
            },
        )

    def cond2(k):
        d2.callbacks.append(lambda e: shared.__setitem__("n2", shared["n2"] + e.value))
        yield env.timeout(1)
        d2.succeed(3)
        yield env.timeout(1)
        L(k, "cond2_end", {"event_type": "n", "n": shared["n2"]})

    def cond0(k):
        yield env.timeout(1)
        d0.succeed(3)
        yield env.timeout(1)
        L(k, "cond0_end", {"event_type": "n", "n": shared["n0"]})

    for name, run in [
        ("fw", first_waiter),
        ("at", attach),
        ("fi", fire),
        ("lw", late),
        ("p2", cond2),
        ("p0", cond0),
    ]:
        arrival(env, name, run, initial=1)
    return go(env, log)


def CV_014():
    # Event.callbacks on a Timeout: the callback is added to the timeout before the process
    # that later waits on it, so it has run when that process resumes
    env, log, L = new()
    seen = [0]

    def begin(k):
        t = env.timeout(2, "v")
        t.callbacks.append(lambda e: seen.__setitem__(0, 1))
        L(k, "begin", {"event_type": "begin"})
        value = yield t
        L(k, "later", {"event_type": "later", "value": value, "seen": seen[0]})

    arrival(env, "a", begin, initial=1)
    return go(env, log)


def CV_112():
    # `except` by exception type: a step raises the exception named by its attribute and
    # two handlers catch KeyError and ValueError. The kinds are the document's fixed ones
    env, log, L = new()
    kinds = ["KeyError", "ValueError", "ValueError"]
    classes = {"KeyError": KeyError, "ValueError": ValueError}
    count = [0]

    def step(k):
        kind = kinds[count[0] % 3]
        count[0] += 1
        try:
            raise classes[kind](kind)
        except KeyError as error:
            L(k, "step", S("failed", reason=error.args[0]))
            L(k, "handle", {"event_type": "handled"})
            L(k, "key_handler", {"event_type": "key_error"})
        except ValueError as error:
            L(k, "step", S("failed", reason=error.args[0]))
            L(k, "handle", {"event_type": "handled"})
            L(k, "other_handler", {"event_type": "other"})
        return
        yield  # pragma: no cover

    arrival(env, "a", step, initial=3)
    return go(env, log)
