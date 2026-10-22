"""The store type the fuel dock example uses: a lane where the newest truck goes first."""

from desflow import StoreBase


class Lane(StoreBase):
    """A store that gives back the newest item, like trucks parked nose to tail."""

    def _do_get(self, event):
        if self.items:
            event.succeed(self.items.pop())
