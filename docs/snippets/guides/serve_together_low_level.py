"""The Serve Entities Together guide, low-level API: seven orders arrive 1 second apart,
and an oven bakes them in batches of 3, or sooner when 2.5 seconds pass.

SimPy has no batch, so a batch is a list of members and an event that closes it.
`batch_records()` returns the (time, key, status) of each `started` and `baked` record,
for a batch that closes by size and timeout, or by an expression when `close_when` is
true.
"""

from simpy import Resource

from desflow import DynamicRealtimeEnvironment

SIZE = 3  # members that close a batch
TIMEOUT = 2.5  # seconds after the first member that close it
SERVICE = 4  # seconds the oven takes for a whole batch


def batch_records(close_when: bool = False) -> list:
    env = DynamicRealtimeEnvironment(factor=0)
    oven = Resource(env, capacity=1)
    seen = []
    open_batch: list = []  # the batch that still takes members: none, or one

    # --8<-- [start:batch]
    def closes_by_size(batch):
        return len(batch["members"]) == SIZE

    def close(batch):
        if not batch["closed"].triggered:
            batch["closed"].succeed()
            open_batch.clear()

    def timer(batch):
        yield env.timeout(TIMEOUT)  # 2.5 seconds after the first member
        close(batch)

    def member(key, batch):
        batch["members"].append(key)
        if closes(batch):  # tested each time a member joins
            close(batch)
        yield batch["closed"]
        seen.append((env.now, key, "started"))  # every member, at the close
        if key == batch["members"][0]:
            # The first member runs the oven once for the whole batch
            with oven.request() as request:
                yield request
                yield env.timeout(SERVICE)
            batch["served"].succeed()
        yield batch["served"]
        seen.append((env.now, key, "baked"))  # every member goes on by itself

    # --8<-- [end:batch]

    # --8<-- [start:close]
    def closes_by_expression(batch):
        """batch.size >= 2 and batch.age >= 2, or 5 members."""
        size = len(batch["members"])
        age = env.now - batch["since"]  # seconds since the first member joined
        return size == 5 or (size >= 2 and age >= 2)

    # --8<-- [end:close]

    closes = closes_by_expression if close_when else closes_by_size

    def orders():
        for number in range(1, 8):
            yield env.timeout(1)
            if not open_batch:
                batch = {
                    "members": [],
                    "since": env.now,
                    "closed": env.event(),
                    "served": env.event(),
                }
                open_batch.append(batch)
                if not close_when:
                    env.process(timer(batch))
            env.process(member(f"orders-{number}", open_batch[0]))

    env.process(orders())
    env.run(until=30)
    env.teardown()
    return sorted(seen)


if __name__ == "__main__":
    for row in batch_records():
        print("check:", row)
