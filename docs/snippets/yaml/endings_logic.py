"""Python for task_endings.yaml: an environment of your own."""

from desflow import DynamicRealtimeEnvironment


class CountingEnvironment(DynamicRealtimeEnvironment):
    """An environment that counts the events the run processes."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.steps = 0

    def step(self):
        self.steps += 1
        super().step()
