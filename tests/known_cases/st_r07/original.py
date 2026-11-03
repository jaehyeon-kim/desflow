"""ST-R07: An exception inside the with block releases the unit. The plain SimPy program: from the SimPy test's program."""

import simpy
import simpy.rt
from tests.known_cases import simpy_harness
from tests.known_cases.simpy_harness import MODE, arrival, go, new

HARNESS = simpy_harness  # the module that holds the harness state the test sets


def program():
    # test_resource_cm_exception: the ValueError is raised and caught inside the with block (pytest.raises), so nothing fails
    env, log, L = new()
    olog = []

    def process(env, resource, raise_, k, task):
        with resource.request() as req:
            yield req
            yield env.timeout(1)
            olog.append(env.now)
            if raise_:
                try:
                    raise ValueError("Foo")
                except ValueError as e:
                    assert "Foo" in str(e)
            L(k, task, {"event_type": "logged"})

    resource = simpy.Resource(env, 1)
    arrival(
        env, "raising", lambda k: process(env, resource, True, k, "raising"), initial=1
    )
    arrival(
        env, "plain", lambda k: process(env, resource, False, k, "plain"), initial=1
    )
    r = go(env, log)
    if not MODE["rev"]:
        assert olog == [1, 2]
    return r
