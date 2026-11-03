"""GD-V3: A shared event made again after each firing. The plain SimPy program: from the SimPy program."""

from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # events.rst, class School: a shared event class_ends that the bell re-creates after each firing; pupils wait twice
    env, log, L = new()

    class School:
        def __init__(self, env):
            self.env = env
            self.class_ends = env.event()

        def bell(self):
            for i in range(2):
                yield self.env.timeout(45)
                L(f"bell-{i + 1}", "ring", {"event_type": "bell"})
                self.class_ends.succeed()
                self.class_ends = self.env.event()

        def pupil(self, k):
            for i in range(2):
                yield self.class_ends  # the guide prints ' \o/' before this yield
                L(
                    k,
                    ["first_class", "second_class"][i],
                    {"event_type": ["class_over", "class_over_again"][i]},
                )

    school = School(env)
    arrival(env, "pupils", school.pupil, initial=3)
    env.process(school.bell())
    return go(env, log)
