"""ST-E05: A periodic event that five consumers count. The plain SimPy program: from the SimPy test's program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import VARS, arrival, flush, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():  # tests/test_environment.py test_run_all_until_callbacks
    env, log, L = new()

    class System:
        def __init__(self, env):
            self.env = env
            self.counter = 0
            self.periodic_event = env.event()

        def periodic(self, k):
            for _ in range(3):
                yield self.env.timeout(1)
                L(k, "tick", {"event_type": "tick"})
                event, self.periodic_event = self.periodic_event, self.env.event()
                event.succeed(self.counter)

        def consumer(self, k):
            while True:
                yield self.periodic_event
                self.counter += 1
                L(k, "consume", {"event_type": "seen"})

    system = System(env)
    arrival(env, "period", system.periodic, initial=1)
    arrival(env, "consumers", system.consumer, initial=5)
    flush()
    err = None
    try:
        period_counter = env.run(until=system.periodic_event)
        assert env.now == 1 and period_counter == 0 and system.counter == 0
        period_counter = env.run(until=system.periodic_event)
        assert env.now == 2 and period_counter == 5 and system.counter == 5
        period_counter = env.run(until=system.periodic_event)
        assert env.now == 3 and period_counter == 10 and system.counter == 10
        env.run()
        assert env.now == 3 and system.counter == 15
    except BaseException as e:
        err = f"{type(e).__name__}: {str(e)[:120]}"
    VARS["counter"] = system.counter
    return log, err
