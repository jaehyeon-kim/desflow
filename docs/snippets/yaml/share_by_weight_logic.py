"""Python for share_by_weight.yaml: a packer that shares its time between classes by
weight, as the Share by Weight guide builds it."""

import bisect
import itertools
from fractions import Fraction

from desflow import ResourceBase

# --8<-- [start:weights]
# Tier 1 is served three times for every once of tier 2
WEIGHTS = {1: 3, 2: 1}
# --8<-- [end:weights]


# --8<-- [start:queue]
class FinishTagQueue(list):
    """Waiting requests in the order of their finish tags, smallest first.

    A request's tag is where its class would finish it if every class had its share:
    the later of the queue's clock and the class's last tag, plus 1 divided by the
    class's weight. Equal tags leave in the order the requests arrived.
    """

    def __init__(self):
        super().__init__()
        self.clock = Fraction(0)  # the tag of the request granted last
        self.last = {}  # the last tag given to each class
        self.arrived = itertools.count()

    def append(self, request):
        tier = request.priority  # set by `priority: attr.tier`, not by _rank
        start = max(self.clock, self.last.get(tier, Fraction(0)))
        tag = start + Fraction(1, WEIGHTS[tier])
        self.last[tier] = tag
        request.tag = (tag, next(self.arrived))
        bisect.insort(self, request, key=lambda waiting: waiting.tag)

    def pop(self, index=-1):
        request = super().pop(index)
        self.clock = request.tag[0]  # a grant moves the clock to its tag
        return request


# --8<-- [end:queue]


# --8<-- [start:resource]
class WeightedPackers(ResourceBase):
    """A resource that grants its units by weight and keeps the tier of each grant."""

    PutQueue = FinishTagQueue

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.granted = []

    def _do_put(self, request):
        super()._do_put(request)
        if request.triggered:  # the request was granted just now
            self.granted.append(request.priority)


# --8<-- [end:resource]
