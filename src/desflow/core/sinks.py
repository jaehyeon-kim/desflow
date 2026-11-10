"""The sinks and sources a model's records go to and come from.

`Sinks` holds what a model declares: one `Sink` record per egress provider, the
ingress providers, the batching defaults of `with_batching` and the statistics of
`with_stats`. The run's environment takes the sinks in `setup_egress`, whose
arguments are lists matched by position, so `Sinks` makes those lists in one place.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from desflow.config import DEFAULT_STATS_INTERVAL, DEFAULT_STATS_LEVEL

if TYPE_CHECKING:
    from desflow.core.environment import DynamicRealtimeEnvironment


@dataclass(frozen=True)
class Sink:
    """One egress provider and its settings.

    Attributes:
        provider: The connector, which subclasses `BaseEgress`.
        predicate: Takes one record and returns True to send it to the provider; None
            sends every record.
        batch_size: Records the provider buffers before a flush; None uses the
            default of `with_batching`.
        flush_interval: Simulation seconds the provider waits before a flush; None
            uses the default.
        label: Whether it is a labels sink, which takes the ground-truth records
            only.
        trace: Whether it is a trace sink, which takes the event trace only.
        envelope: Whether it writes each record whole; False writes each event's
            value alone.
    """

    provider: Any
    predicate: Optional[Callable[[Dict[str, Any]], bool]] = None
    batch_size: Optional[int] = None
    flush_interval: Optional[float] = None
    label: bool = False
    trace: bool = False
    envelope: bool = True


class Sinks:
    """A model's egress and ingress providers, their batching and its statistics.

    Attributes:
        egress: One `Sink` per egress provider, in the order they were added.
        ingress: The ingress providers.
        batch_size: The default batch size of a sink.
        flush_interval: The default flush interval of a sink.
        max_queued_batches: How many batches may wait for the egress threads.
        drain_stall_seconds: How long teardown waits once the queues stop shrinking.
        stats_level: The level of the built-in statistics.
        stats_interval: How often the statistics are published, in seconds.
        stats_on_change: Whether the statistics are published at each request and
            release.
        stats_trace: Whether the event trace is on.
    """

    def __init__(self) -> None:
        self.egress: List[Sink] = []
        self.ingress: List[Any] = []
        self.batch_size = 500
        self.flush_interval = 1.0
        self.max_queued_batches = 2000
        self.drain_stall_seconds = 30.0
        self.stats_level = DEFAULT_STATS_LEVEL
        self.stats_interval = DEFAULT_STATS_INTERVAL
        self.stats_on_change = False
        self.stats_trace = False

    @property
    def traces(self) -> bool:
        """Whether a trace sink was added."""
        return any(sink.trace for sink in self.egress)

    def setup_egress(
        self,
        env: "DynamicRealtimeEnvironment",
        warmup: Optional[float],
        lag_monitor_interval: Union[float, str, None],
    ) -> None:
        """Starts the run's egress with every sink, through `setup_egress`.

        Args:
            env: The run's environment.
            warmup: The context's `warmup`.
            lag_monitor_interval: How often the lag is published, or "auto".
        """
        env.setup_egress(
            [sink.provider for sink in self.egress],
            batch_size=self.batch_size,
            flush_interval=self.flush_interval,
            max_queued_batches=self.max_queued_batches,
            drain_stall_seconds=self.drain_stall_seconds,
            predicates=[sink.predicate for sink in self.egress],
            batch_sizes=[sink.batch_size for sink in self.egress],
            flush_intervals=[sink.flush_interval for sink in self.egress],
            labels=[sink.label for sink in self.egress],
            traces=[sink.trace for sink in self.egress],
            envelopes=[sink.envelope for sink in self.egress],
            warmup=warmup,
            lag_monitor_interval=lag_monitor_interval,
        )
