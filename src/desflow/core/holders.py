"""The resources, stores, containers and breakdowns of a model.

`Holders` holds what a model declares for them. Each run's build makes a `HolderRun`,
which builds the run's resources, stores and containers from the declarations, keeps
them by name, and starts the breakdowns.
"""

import copy
import dataclasses
import logging
import math
import numbers
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

from desflow.core.breakdowns import run_breakdowns
from desflow.core.expressions import Expression, Scope
from desflow.core.families import SECTIONS
from desflow.models.params import BreakdownConfig, CapacityConfig, ResourceConfig
from desflow.resources.container import ContainerBase, DynamicContainer
from desflow.resources.resource import (
    DynamicResource,
    PreemptRule,
    ResourceBase,
    _default_put_queue,
)
from desflow.resources.store import DynamicStore, StoreBase
from desflow.values.models import has_generator

if TYPE_CHECKING:
    from desflow.core.environment import DynamicRealtimeEnvironment
    from desflow.core.families import Family
    from desflow.core.sampler import Sampler
    from desflow.values.runtime import ValueRuntime

logger = logging.getLogger("desflow.core.context")


def _within_max(
    kind: str,
    name: str,
    current_cap: Optional[Union[int, float]],
    max_cap: Optional[Union[int, float]],
) -> Optional[Union[int, float]]:
    """A `current_cap` clamped to `[0, max_cap]`, with a warning when it changes, as
    the registry clamps a live update; the run and `analyze()` then use the same
    capacity."""
    if current_cap is None or max_cap is None:
        return current_cap
    clamped = type(current_cap)(min(max(current_cap, 0), max_cap))
    if clamped != current_cap:
        logger.warning(
            f"current_cap {current_cap} of {kind} '{name}' is outside "
            f"[0, max_cap {max_cap}]. Using {clamped}."
        )
    return clamped


def _run_copy(config: Any) -> Any:
    """A run's copy of one declared config object, which is what the registry writes
    into: a copy of the object, and for a breakdown a copy of its two distributions as
    well. Every other field of a config is a number, text or a list the registry
    never writes. A run, the load and start checks and a family's new member each
    register what this gives, so none of them writes into the declarations.
    """
    twin = copy.copy(config)
    if isinstance(config, BreakdownConfig):
        twin.time_to_failure = copy.copy(config.time_to_failure)
        twin.repair = copy.copy(config.repair)
    return twin


def _whole_or_unlimited(value: Any) -> bool:
    """Whether a store capacity is a whole number or positive infinity."""
    if isinstance(value, (str, bytes)):
        return False
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return number == math.inf or (math.isfinite(number) and number.is_integer())


def _limit_path(sim_id: str, resource: str) -> str:
    """The registry path of a resource's `queue_limit`.

    Args:
        sim_id: The context's `sim_id`, which the path starts with.
        resource: The resource's name.
    """
    return f"{sim_id}.resources.{resource}.queue_limit"


class Holders:
    """What a model declares for its resources, stores, containers and breakdowns.

    Attributes:
        resources_config: Each resource's capacity.
        resource_queues: Each resource's queue order.
        resource_preempt: Whether each resource preempts.
        resource_queue_limits: Each resource's `queue_limit`, or None.
        resource_rank: A resource's `queue` rank: the expression and `ties`.
        resource_rule: A resource's `preempt` rule: the `when` and `victim`
            expressions.
        holder_ranks: A store's or container's `get_queue` and `put_queue` ranks, by
            (name, "get" or "put").
        resource_types: The `ResourceBase` subclass each resource is built as, or None.
        breakdowns_config: Each resource's breakdowns.
        containers_config: Each container's capacity and level.
        stores_config: Each store's capacity.
        store_priority: Whether each store is a priority store.
        store_types: The `StoreBase` subclass each store is built as, or None.
        container_types: The `ContainerBase` subclass each container is built as, or
            None.
        store_items: The items each store starts with.
    """

    def __init__(self) -> None:
        self.resources_config: Dict[str, CapacityConfig] = {}
        self.resource_queues: Dict[str, str] = {}
        self.resource_preempt: Dict[str, bool] = {}
        self.resource_queue_limits: Dict[str, Optional[int]] = {}
        self.resource_rank: Dict[str, Tuple[Expression, str]] = {}
        self.resource_rule: Dict[
            str, Tuple[Optional[Expression], Optional[Expression]]
        ] = {}
        self.holder_ranks: Dict[Tuple[str, str], Tuple[Expression, str]] = {}
        self.resource_types: Dict[str, Optional[Type[ResourceBase]]] = {}
        self.breakdowns_config: Dict[str, BreakdownConfig] = {}
        self.containers_config: Dict[str, CapacityConfig] = {}
        self.stores_config: Dict[str, CapacityConfig] = {}
        self.store_priority: Dict[str, bool] = {}
        self.store_types: Dict[str, Optional[Type[StoreBase]]] = {}
        self.container_types: Dict[str, Optional[Type[ContainerBase]]] = {}
        self.store_items: Dict[str, List[Any]] = {}

    def declare_resource(
        self,
        name: str,
        current_cap: Any,
        max_cap: Any,
        queue: str,
        preempt: bool,
        queue_limit: Optional[int],
        rank: Optional[Tuple[Expression, str]],
        rule: Optional[Tuple[Optional[Expression], Optional[Expression]]],
        type: Optional[Type[ResourceBase]],
    ) -> None:
        """Keeps a resource's declaration; a second declaration of a name replaces
        the first. The capacity is checked after the queue settings are kept.

        Args:
            name: The resource's name.
            current_cap: Its starting capacity, or None for 1.
            max_cap: Its largest capacity, or None for no limit.
            queue: Its queue order.
            preempt: Whether it preempts.
            queue_limit: Its `queue_limit`, or None.
            rank: Its `queue` rank, the expression and `ties`, or None.
            rule: Its `preempt` rule, the `when` and `victim` expressions, or None.
            type: The `ResourceBase` subclass it is built as, or None.
        """
        self.resource_preempt[name] = preempt
        if rank is None:
            self.resource_rank.pop(name, None)
        else:
            self.resource_rank[name] = rank
        if rule is None:
            self.resource_rule.pop(name, None)
        else:
            self.resource_rule[name] = rule
        self.resources_config[name] = ResourceConfig(
            current_cap=_within_max("resource", name, current_cap, max_cap),
            max_cap=max_cap,
            queue_limit=queue_limit,
            limit_path=_default_put_queue(type or ResourceBase),
        )
        self.resource_queues[name] = queue
        self.resource_queue_limits[name] = queue_limit
        self.resource_types[name] = type

    def declare_container(
        self,
        name: str,
        config: CapacityConfig,
        type: Optional[Type[ContainerBase]],
    ) -> None:
        """Keeps a container's declaration; a second declaration of a name replaces
        the first.

        Args:
            name: The container's name.
            config: Its capacity and level.
            type: The `ContainerBase` subclass it is built as, or None.
        """
        self.container_types[name] = type
        self.containers_config[name] = config

    def declare_store(
        self,
        name: str,
        config: CapacityConfig,
        items: List[Any],
        priority: bool,
        type: Optional[Type[StoreBase]],
    ) -> None:
        """Keeps a store's declaration; a second declaration of a name replaces the
        first.

        Args:
            name: The store's name.
            config: Its capacity.
            items: The items it starts with.
            priority: Whether it is a priority store.
            type: The `StoreBase` subclass it is built as, or None.
        """
        self.store_items[name] = items
        self.stores_config[name] = config
        self.store_priority[name] = priority
        self.store_types[name] = type

    def new_run(
        self,
        env: "DynamicRealtimeEnvironment",
        values: "ValueRuntime",
        sampler: "Sampler",
        sim_id: str,
        rule_scope: Callable[..., Scope],
    ) -> "HolderRun":
        """The holders of a new run, none built yet.

        Args:
            env: The run's environment.
            values: The run's values, which make a store's starting items.
            sampler: The run's sampler, which the breakdowns draw from.
            sim_id: The context's `sim_id`, under which the holders register.
            rule_scope: What a resource's preempt rule reads, given its `request` and
                `holder`.
        """
        return HolderRun(self, env, values, sampler, sim_id, rule_scope)

    def ends_early(
        self, sim_id: str, resources: Iterable[str], written: Set[str]
    ) -> bool:
        """Whether a request for one of `resources` can end before it is granted: the
        resource has a `queue_limit`, or the model writes its `queue_limit` path.

        Args:
            sim_id: The context's `sim_id`, which the path starts with.
            resources: The resources a task requests.
            written: The registry paths the model writes.
        """
        return any(
            self.resource_queue_limits.get(resource) is not None
            or _limit_path(sim_id, resource) in written
            for resource in resources
        )

    def ties(self, name: str) -> Dict[str, Any]:
        """The `get_ties` and `put_ties` a store or container is built with.

        Args:
            name: The store's or container's name, or the family it is a member of.
        """
        return {
            f"{direction}_ties": self.holder_ranks[(name, direction)][1]
            for direction in ("get", "put")
            if (name, direction) in self.holder_ranks
        }

    def rank(self, holder: Any, source: Optional[str] = None) -> None:
        """Gives a built store or container the rank expressions its tasks read;
        `source` is the family a member is made from.

        Args:
            holder: The built store or container.
            source: The family it is a member of; None for its own name.
        """
        for direction in ("get", "put"):
            rule = self.holder_ranks.get((source or holder.obj_id, direction))
            if rule is not None:
                setattr(holder, f"{direction}_rank_of", rule[0])


@dataclass
class HolderRun:
    """The resources, stores and containers of one run, by name. Each run's build
    makes a new one, which builds them from the declarations.

    Attributes:
        holders: The declarations.
        env: The run's environment.
        values: The run's values, which make a store's starting items.
        sampler: The run's sampler, which the breakdowns draw from.
        sim_id: The context's `sim_id`.
        rule_scope: What a resource's preempt rule reads, given its `request` and
            `holder`.
        resources: The run's resources.
        stores: The run's stores.
        containers: The run's containers.
    """

    holders: Holders
    env: "DynamicRealtimeEnvironment"
    values: "ValueRuntime"
    sampler: "Sampler"
    sim_id: str
    rule_scope: Callable[..., Scope]
    resources: Dict[str, ResourceBase] = dataclasses.field(default_factory=dict)
    stores: Dict[str, DynamicStore] = dataclasses.field(default_factory=dict)
    containers: Dict[str, DynamicContainer] = dataclasses.field(default_factory=dict)

    def starting_items(self, store_id: str) -> Optional[List[Any]]:
        """The items a store starts with; a generator in `items` is evaluated now,
        once for each item.

        Args:
            store_id: The store's name.
        """
        items = self.holders.store_items.get(store_id)
        if not items or not has_generator(items):
            return items
        made = self.values.evaluate(items, self.values.scope_of())
        if self.holders.store_priority.get(store_id) and not all(
            isinstance(item, (tuple, list)) and len(item) == 2 for item in made
        ):
            raise ValueError(
                f"priority store '{store_id}' starts with (priority, item) pairs; "
                f"a generator in its items gave {made!r}"
            )
        return made

    def preemption(
        self,
        resource: str,
        when: Optional[Expression],
        victim: Optional[Expression],
    ) -> PreemptRule:
        """The `PreemptRule` of a resource's `when` and `victim` expressions, which
        read the run and the request and holder they are given.

        Args:
            resource: The resource's name.
            when: The `when` expression; None for none.
            victim: The `victim` expression; None for none.
        """

        def read(expression: Expression, request: Any, holder: Any) -> Any:
            scope = self.rule_scope(
                request=_rule_row(request), holder=_rule_row(holder)
            )
            try:
                return expression.evaluate(scope)
            except ValueError as error:
                raise ValueError(
                    f"the preempt rule of resource '{resource}': {error}"
                ) from None

        return PreemptRule(
            when=None if when is None else lambda r, h: bool(read(when, r, h)),
            victim=None if victim is None else lambda r, h: read(victim, r, h),
        )

    def new_resource(self, res_id: str, source: Optional[str] = None) -> ResourceBase:
        """Builds a resource and registers it by name.

        Args:
            res_id: The name it is built and registered under.
            source: The entry whose settings it takes, when it is a member of a
                family; `res_id` otherwise.

        Returns:
            ResourceBase: The resource.
        """
        origin = source or res_id
        resource_type = self.holders.resource_types.get(origin) or DynamicResource
        rank = self.holders.resource_rank.get(origin)
        rule = self.holders.resource_rule.get(origin)
        options: Dict[str, Any] = {}
        if self.holders.resource_queue_limits.get(origin) is not None:
            options["queue_limit"] = self.holders.resource_queue_limits[origin]
        if rank is not None and rank[1] != "fifo":
            options["ties"] = rank[1]
        resource = resource_type(
            self.env,
            self.sim_id,
            res_id,
            queue=self.holders.resource_queues[origin],
            preempt=(
                self.preemption(res_id, *rule)
                if rule is not None
                else self.holders.resource_preempt.get(origin, False)
            ),
            **options,
        )
        if rank is not None:
            setattr(resource, "rank_of", rank[0])
        self.resources[res_id] = resource
        return resource

    def new_store(self, store_id: str, source: Optional[str] = None) -> DynamicStore:
        """Builds a store and registers it by name; `source` is the family it is a
        member of.

        Args:
            store_id: The name it is built and registered under.
            source: The entry whose settings it takes, when it is a member of a family;
                `store_id` otherwise.
        """
        origin = source or store_id
        store = DynamicStore(
            self.env,
            self.sim_id,
            store_id,
            priority=self.holders.store_priority.get(origin, False),
            items=self.starting_items(origin),
            type=self.holders.store_types.get(origin),
            **self.holders.ties(origin),
        )
        self.stores[store_id] = store
        self.holders.rank(store, origin)
        return store

    def new_container(
        self, container_id: str, source: Optional[str] = None
    ) -> DynamicContainer:
        """Builds a container and registers it by name; `source` is the family it is
        a member of.

        Args:
            container_id: The name it is built and registered under.
            source: The entry whose settings it takes, when it is a member of a family;
                `container_id` otherwise.
        """
        origin = source or container_id
        container = DynamicContainer(
            self.env,
            self.sim_id,
            container_id,
            type=self.holders.container_types.get(origin),
            **self.holders.ties(origin),
        )
        self.containers[container_id] = container
        self.holders.rank(container, origin)
        return container

    def start_breakdown(self, res_id: str, breakdown: BreakdownConfig) -> None:
        """Starts the process that breaks the resource down and repairs it.

        Args:
            res_id: The resource's name.
            breakdown: The resource's breakdown settings.
        """
        assert self.env is not None and self.sampler is not None
        self.env.process(
            run_breakdowns(
                self.env,
                self.resources[res_id],
                self.sampler,
                work=breakdown.work,
                clock=breakdown.clock,
            )
        )

    def make_member(self, family: "Family", name: str, scope: Scope) -> None:
        """Makes the member `name` of a resource, store or container family, with
        the family's settings and the capacities its expressions give in `scope`.

        Args:
            family: The family.
            name: The member's name.
            scope: What the family's capacity expressions read.
        """
        base = family.name
        config = _run_copy(
            {
                "resource": self.holders.resources_config,
                "store": self.holders.stores_config,
                "container": self.holders.containers_config,
            }[family.kind][base]
        )
        for field, expression in family.expressions.items():
            found = expression.evaluate(scope)
            where = f"{SECTIONS[family.kind]}.{base}.{field} for {name}"
            if isinstance(found, bool) or not isinstance(found, numbers.Real):
                raise TypeError(f"{where} gave {found!r}, which is not a number")
            if (
                field != "level"
                and family.kind != "container"
                and not _whole_or_unlimited(found)
            ):
                raise ValueError(f"{where} gave {found!r}, not a whole number")
            setattr(config, field, found)
        if config.current_cap is not None and config.max_cap is not None:
            config.current_cap = _within_max(
                family.kind, name, config.current_cap, config.max_cap
            )
        registry = self.env.registry
        if family.kind == "resource":
            registry.register_resource(self.sim_id, name, config)
            self.new_resource(name, base)
            breakdown = self.holders.breakdowns_config.get(base)
            if breakdown is not None:
                found_breakdown = _run_copy(breakdown)
                registry.register_breakdown(self.sim_id, name, found_breakdown)
                self.start_breakdown(name, found_breakdown)
        elif family.kind == "store":
            registry.register_store(self.sim_id, name, config)
            self.new_store(name, base)
        else:
            registry.register_container(self.sim_id, name, config)
            self.new_container(name, base)

    def levels(self) -> Dict[str, Any]:
        """Stores and containers by name, which `level.<name>` reads."""
        return {**self.stores, **self.containers}


def _rule_row(request: Any) -> Dict[str, Any]:
    """What a preempt rule reads of a request or a holder: its fields."""
    return {
        "rank": request.priority,
        "time": request.time,
        "preempt": request.preempt,
        "key": request.key,
        "since": request.usage_since,
        "attr": request.attributes,
    }
