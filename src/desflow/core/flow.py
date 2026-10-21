"""Routing between tasks, and what a task takes from or adds to a store or container.

`Router` decides the next task when one finishes, from the same rules in all three
APIs. `Transfer` describes a task's `get` or `put`. The builder and YAML tasks use
both; low-level code uses `Router` directly and calls `get` and `put` itself.
"""

from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple, Union

import numpy as np

from desflow.core.expressions import Expression, Scope
from desflow.models.flow import Upstream, payload_of

NextSpec = Union[None, str, Mapping[str, Any], Callable[[Upstream], Optional[str]]]
Amount = Union[float, str, Callable[[Any, Dict[str, Any]], float]]


def make_scope(
    upstream: Optional[Upstream],
    attributes: Optional[Mapping[str, Any]] = None,
    env: Any = None,
    levels: Optional[Mapping[str, Any]] = None,
    item: Any = None,
    waits: Optional[Mapping[str, Mapping[str, Any]]] = None,
    signals: Optional[Mapping[str, Any]] = None,
    process: Optional[Mapping[str, Any]] = None,
    resources: Optional[Mapping[str, Any]] = None,
) -> Scope:
    """
    Builds what an expression's names read.

    Args:
        upstream: What the previous task handed on; its payload is `payload`.
        attributes: The entity's attributes, read as `attr`. Defaults to the
            upstream's.
        env: The environment, for `registry[...]` and `t`.
        levels: Stores and containers by name, for `level`.
        item: The item a store get took, for `item`.
        waits: The task's wait branches, for `wait`.
        signals: Signals by name, for `signal`.
        process: The running task's `name`, `instance` and `target`, for `process`.
        resources: Resources by name, for `users` and `queue`.

    Returns:
        Scope: The scope.
    """
    if attributes is None:
        attributes = {} if upstream is None else upstream.attributes
    return Scope(
        payload=payload_of(upstream),
        attributes=attributes,
        levels=levels or {},
        resources=resources or {},
        registry=None if env is None else env.registry,
        now=0.0 if env is None else env.now,
        item=item,
        waits=waits or {},
        signals=signals or {},
        process=process or {},
    )


class Router:
    """
    Chooses the task that follows a finished one.

    The rules take one of these forms, in every API:

    - a task name: always that task;
    - `{"when": [{"condition": "...", "task": "..."}, ...], "default": "..."}`: the
      first rule whose condition holds wins, and `default`, if given, catches the
      rest; with no default and no rule holding, the flow ends;
    - `{"weights": {"ship": 9, "rework": 1}}`: a task drawn with these weights from
      the routing task's own random sequence, so seeded runs repeat;
    - a function taking the `Upstream` and returning a task name or None;
    - None: the flow ends.

    A route back to an earlier task (rework) is allowed.

    Attributes:
        tasks (Optional[Set[str]]): Every task the rules can name, so a loader can
            check they exist; None for a function.
        rng (Optional[np.random.Generator]): The generator weights draw from. The
            builder sets it when the run starts.
    """

    def __init__(self, rules: NextSpec, rng: Optional[np.random.Generator] = None):
        """
        Checks the rules and parses their conditions.

        Args:
            rules: The rules, in one of the forms above.
            rng: The generator weights draw from, such as
                `sampler.generator("route.<task>")`. Needed only for weights.

        Raises:
            ValueError: If the rules are malformed or a condition is refused.
        """
        self._function: Optional[Callable[[Upstream], Optional[str]]] = None
        self._when: List[Tuple[Expression, str]] = []
        self._default: Optional[str] = None
        self._names: List[str] = []
        self._cumulative: Optional[np.ndarray] = None
        self.rng = rng
        self.tasks: Optional[Set[str]] = set()

        if rules is None:
            return
        if isinstance(rules, str):
            self._default = rules
            self.tasks = {rules}
            return
        if callable(rules):
            self._function = rules
            self.tasks = None
            return
        if not isinstance(rules, Mapping):
            raise ValueError(
                f"next must be a task name, rules or a function, got {rules!r}"
            )
        unknown = set(rules) - {"when", "default", "weights"}
        if unknown:
            raise ValueError(f"next does not take {', '.join(sorted(unknown))}")
        if "weights" in rules:
            if "when" in rules or "default" in rules:
                raise ValueError(
                    "next takes weights, or when rules with a default, not both"
                )
            self._set_weights(rules["weights"])
            return
        for rule in rules.get("when") or []:
            if not isinstance(rule, Mapping) or set(rule) != {"condition", "task"}:
                raise ValueError(
                    f"each when rule needs a condition and a task, got {rule!r}"
                )
            self._when.append((Expression(rule["condition"]), str(rule["task"])))
        self._default = rules.get("default")
        if not self._when and self._default is None:
            raise ValueError("next needs when rules, a default or weights")
        self.tasks = {task for _, task in self._when} | (
            {self._default} if self._default is not None else set()
        )

    @property
    def weighted(self) -> bool:
        """True when the rules are weights, which draw from `rng`."""
        return self._cumulative is not None

    @property
    def conditions(self) -> List[Expression]:
        """The parsed conditions, in rule order."""
        return [condition for condition, _ in self._when]

    def choose(
        self,
        upstream: Upstream,
        env: Any = None,
        levels: Optional[Mapping[str, Any]] = None,
        item: Any = None,
        scope: Optional[Scope] = None,
    ) -> Optional[str]:
        """
        Returns the next task's name, or None when the flow ends.

        Args:
            upstream: What the finished task hands on.
            env: The environment, for `registry[...]` and `t` in conditions.
            levels: Stores and containers by name, for `level.<name>`.
            item: The item the finished task's store get took, for `item.<field>`.
            scope: What the conditions read, when the caller has built it; it
                replaces the one built from the arguments above.

        Returns:
            Optional[str]: The next task, or None.

        Raises:
            ValueError: If a condition reads a name that is missing.
        """
        if self._function is not None:
            return self._function(upstream)
        if self._cumulative is not None:
            if self.rng is None:
                raise ValueError("weights need a random generator to draw from")
            index = int(np.searchsorted(self._cumulative, self.rng.random(), "right"))
            return self._names[min(index, len(self._names) - 1)]
        if self._when:
            if scope is None:
                scope = make_scope(upstream, env=env, levels=levels, item=item)
            for condition, task in self._when:
                if condition.evaluate(scope):
                    return task
        return self._default

    def _set_weights(self, weights: Any) -> None:
        if not isinstance(weights, Mapping) or not weights:
            raise ValueError("weights must map task names to numbers")
        values = []
        for name, weight in weights.items():
            if isinstance(weight, bool) or not isinstance(weight, (int, float)):
                raise ValueError(f"the weight of '{name}' must be a number")
            if not weight >= 0:
                raise ValueError(f"the weight of '{name}' must be 0 or more")
            values.append(float(weight))
        total = sum(values)
        if not total > 0:
            raise ValueError("weights must add up to more than 0")
        self._names = [str(name) for name in weights]
        self._cumulative = np.cumsum(values) / total
        self.tasks = set(self._names)


class Transfer:
    """
    What a task takes from, or adds to, a store or a container.

    A container transfer moves an amount: a number, an expression such as
    `payload.quantity`, or in Python a function of the task id and the upstream
    payload. A store transfer moves one item: a `get` takes the next item, and a
    `put` adds the task's finished payload.

    Attributes:
        container (Optional[str]): The container's name.
        store (Optional[str]): The store's name.
        amount: The amount, for a container.
    """

    def __init__(
        self,
        container: Optional[str] = None,
        store: Optional[str] = None,
        amount: Amount = 1.0,
        priority: Amount = 0.0,
        filter: Union[str, Callable[[Any], bool], None] = None,
    ):
        """
        Checks the transfer.

        Args:
            container: The container's name.
            store: The store's name, instead of a container.
            amount: For a container, a number, an expression or a function.
            priority: For a put into a priority store, a number, an expression
                or a function, lowest served first. Ignored by a `fifo` store.
            filter: For a store get, which item it takes: a condition such as
                `"item.size == attr.size"`, where `item.<field>` reads the item
                offered, or a function of the item returning True. The get takes
                the oldest item that matches and waits while none does.

        Raises:
            ValueError: If not exactly one of container and store is given, an
                amount is given for a store, a priority is given for a
                container, or the amount or priority is malformed.
        """
        if (container is None) == (store is None):
            raise ValueError("a get or put names one container or one store")
        if store is not None and amount != 1.0:
            raise ValueError("a store moves one item, so it takes no amount")
        if container is not None and priority != 0.0:
            raise ValueError("priority orders a store's items, so it needs a store")
        if container is not None and filter is not None:
            raise ValueError("a filter picks a store's item, so it needs a store")
        self.container = container
        self.store = store
        self.amount = amount
        self.priority = priority
        self._expression: Optional[Expression] = None
        if isinstance(amount, str):
            self._expression = Expression(amount)
        elif not callable(amount) and (
            isinstance(amount, bool) or not isinstance(amount, (int, float))
        ):
            raise ValueError(
                f"amount must be a number or an expression, got {amount!r}"
            )
        self.filter = filter
        self._filter_expression: Optional[Expression] = None
        if isinstance(filter, str):
            self._filter_expression = Expression(filter)
        elif filter is not None and not callable(filter):
            raise ValueError(
                f"filter must be a condition or a function, got {filter!r}"
            )
        self._priority_expression: Optional[Expression] = None
        if isinstance(priority, str):
            self._priority_expression = Expression(priority)
        elif not callable(priority) and (
            isinstance(priority, bool) or not isinstance(priority, (int, float))
        ):
            raise ValueError(
                f"priority must be a number or an expression, got {priority!r}"
            )

    @classmethod
    def of(cls, spec: Union["Transfer", Mapping[str, Any]]) -> "Transfer":
        """
        Returns a transfer from a mapping such as `{"container": "stock", "amount": 2}`.

        Args:
            spec: A mapping with `container` or `store` and an optional `amount`, or
                a `Transfer`.

        Returns:
            Transfer: The transfer.

        Raises:
            ValueError: If the mapping has other keys or is malformed.
        """
        if isinstance(spec, Transfer):
            return spec
        if not isinstance(spec, Mapping):
            raise ValueError(f"a get or put is a mapping, got {spec!r}")
        unknown = set(spec) - {"container", "store", "amount", "priority", "filter"}
        if unknown:
            raise ValueError(f"a get or put does not take {', '.join(sorted(unknown))}")
        return cls(**spec)

    @property
    def target(self) -> str:
        """The name of the store or container."""
        return self.container if self.container is not None else self.store  # type: ignore[return-value]

    def amount_for(
        self, task_id: Any, upstream: Optional[Upstream], scope: Scope
    ) -> float:
        """
        Returns the amount for one task instance.

        Args:
            task_id: The instance id.
            upstream: What the previous task handed on.
            scope: What an expression's names read.

        Returns:
            float: The amount.
        """
        if self._expression is not None:
            return float(self._expression.evaluate(scope))
        if callable(self.amount):
            return float(self.amount(task_id, payload_of(upstream)))
        return float(self.amount)

    @property
    def expressions(self) -> List[Expression]:
        """Every expression the transfer holds, so a loader can check its names."""
        return [
            expression
            for expression in (
                self._expression,
                self._priority_expression,
                self._filter_expression,
            )
            if expression is not None
        ]

    def filter_for(self, scope: Scope) -> Optional[Callable[[Any], bool]]:
        """
        Returns the function a store get offers each item to, or None for any item.

        Args:
            scope: What the condition's other names read, taken when the get is made;
                `item` is set to each item offered.

        Returns:
            Optional[Callable[[Any], bool]]: True for an item the get may take.
        """
        if self._filter_expression is not None:
            condition = self._filter_expression

            def matches(item: Any) -> bool:
                scope.item = item
                return bool(condition.evaluate(scope))

            return matches
        return None if isinstance(self.filter, str) else self.filter

    def priority_for(
        self, task_id: Any, upstream: Optional[Upstream], scope: Scope
    ) -> float:
        """
        Returns the priority for one put into a priority store, lowest first.

        Args:
            task_id: The instance id.
            upstream: What the previous task handed on.
            scope: What an expression's names read.

        Returns:
            float: The priority.
        """
        if self._priority_expression is not None:
            return float(self._priority_expression.evaluate(scope))
        if callable(self.priority):
            return float(self.priority(task_id, payload_of(upstream)))
        return float(self.priority)


# The kinds of branch a task's `wait` takes
WAIT_KINDS = (
    "signal",
    "task",
    "timeout",
    "condition",
    "request",
    "get",
    "put",
    "running",
)

# Kinds a later SimPy release may add, refused until then with a clear message
# rather than the one-branch or length error. The check reads the mapping's keys.
NOT_YET_KINDS: Tuple[str, ...] = ()

# Group kinds: a branch that holds named child branches.
GROUP_KINDS = ("any", "all", "at_least")


def _refuse_not_yet(spec: Any) -> None:
    """Refuses a wait kind a later batch adds, before the length checks."""
    if isinstance(spec, Mapping):
        for kind in spec:
            if kind in NOT_YET_KINDS:
                raise ValueError(
                    f"wait kind '{kind}' comes with desflow 0.23.0 and is not built yet"
                )


class WaitNode:
    """One node of a parsed wait: a leaf, with a kind and a target, or a group, with
    a mode (`any`, `all` or `at_least`), named child nodes, and a count for
    `at_least`."""

    __slots__ = ("name", "kind", "target", "children", "count")

    def __init__(
        self,
        name: Optional[str],
        kind: str,
        target: Any = None,
        children: Optional[List["WaitNode"]] = None,
        count: int = 0,
    ):
        self.name = name
        self.kind = kind
        self.target = target
        self.children: List[WaitNode] = children if children is not None else []
        self.count = count

    @property
    def is_group(self) -> bool:
        """True for a group node (`any`, `all` or `at_least`), False for a leaf."""
        return self.kind in GROUP_KINDS


class Wait:
    """
    What a task waits for before it queues for its resource: one branch, or any,
    all or at least some of several, nested to any depth.

    The forms, in every API:

    - one branch: `{"signal": "restocked"}`, `{"task": "inspect"}`,
      `{"timeout": 30}` or `{"condition": "level.stock >= 5"}`, named after the
      signal or task, or `timeout` or `condition`;
    - `{"any": {name: branch, ...}}`: done when the first branch is, as SimPy's
      `AnyOf`;
    - `{"all": {name: branch, ...}}`: done when every branch is, as SimPy's `AllOf`;
    - `{"at_least": n, "of": {name: branch, ...}}`: done when `n` branches are;
    - a branch in a group may itself be a group, so waits nest.

    A `task` branch starts that task for the same entity and waits for its flow to
    end; its value is the last task's finished payload. A `timeout` takes seconds or
    a duration such as `"5 min"`. An empty `any` or `all`, or an empty `of`, succeeds
    at once with an empty value, as SimPy's `any_of([])` does.

    Attributes:
        mode (str): The top node's mode, `any`, `all` or `at_least`.
        root (WaitNode): The top of the tree.
        branches (Dict[str, WaitNode]): Every named node, by name, leaves and groups,
            so `wait.<name>` reads any node in the tree.
    """

    def __init__(self, spec: Mapping[str, Any]):
        """
        Checks the wait and parses its conditions into a tree.

        Args:
            spec: The wait, in one of the forms above.

        Raises:
            ValueError: If the form, a branch or a condition is malformed, or a
                branch name is used more than once.
        """
        _refuse_not_yet(spec)
        if not isinstance(spec, Mapping) or not spec:
            raise ValueError(
                f"wait takes one branch, or any or all of named branches, got {spec!r}"
            )
        self.branches: Dict[str, WaitNode] = {}
        self.root = self._node(spec, None)
        self.mode = self.root.kind if self.root.is_group else "any"

    def _node(self, spec: Any, name: Optional[str]) -> WaitNode:
        """Parses one branch spec, a leaf or a group, under branch name `name` (None
        at the top of a single-branch wait), and registers every named node."""
        if not isinstance(spec, Mapping):
            raise ValueError(f"a wait branch is a mapping, got {spec!r}")
        _refuse_not_yet(spec)
        keys = set(spec)
        if "at_least" in keys:
            node = self._at_least(spec, name)
        elif "any" in keys or "all" in keys:
            if len(spec) != 1:
                raise ValueError(
                    "wait takes one branch, or any or all of named branches, "
                    f"got {spec!r}"
                )
            kind = "any" if "any" in keys else "all"
            node = WaitNode(name, kind, children=self._children(kind, spec[kind]))
        else:
            kind, target = _wait_branch(spec)
            if name is not None:
                node_name: Optional[str] = name
            elif kind in ("signal", "task", "request"):
                node_name = target
            elif kind == "running":
                node_name = target["task"]
            else:
                node_name = kind
            node = WaitNode(node_name, kind, target=target)
        if node.name is not None:
            if node.name in self.branches:
                raise ValueError(
                    f"wait branch name '{node.name}' is used more than once"
                )
            self.branches[node.name] = node
        return node

    def _children(self, kind: str, members: Any) -> List[WaitNode]:
        """Parses the named child branches of a group."""
        if not isinstance(members, Mapping):
            raise ValueError(f"wait.{kind} takes a mapping of named branches")
        return [self._node(branch, str(name)) for name, branch in members.items()]

    def _at_least(self, spec: Mapping[str, Any], name: Optional[str]) -> WaitNode:
        """Parses an `{at_least: n, of: {...}}` group."""
        extra = set(spec) - {"at_least", "of"}
        if extra:
            raise ValueError(
                f"at_least takes at_least and of, not {', '.join(sorted(extra))}"
            )
        count = spec["at_least"]
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            raise ValueError(f"at_least is a whole number of 1 or more, got {count!r}")
        children = self._children("at_least", spec.get("of", {}))
        if children and count > len(children):
            raise ValueError(
                f"at_least {count} is above the {len(children)} branches it waits for"
            )
        return WaitNode(name, "at_least", children=children, count=count)

    def targets(self, kind: str) -> Set[str]:
        """Returns the signals or the tasks the wait's leaves name.

        Args:
            kind: `signal`, `task` or `running`. `task` also returns the tasks a
                `running` branch names, so a load check finds them all.

        Returns:
            Set[str]: The names the leaves of that kind name.
        """
        running = {
            n.target["task"] for n in self.branches.values() if n.kind == "running"
        }
        if kind == "running":
            return running
        found = {n.target for n in self.branches.values() if n.kind == kind}
        return found | running if kind == "task" else found

    @property
    def conditions(self) -> List[Expression]:
        """The parsed conditions of the `condition` leaves."""
        return [n.target for n in self.branches.values() if n.kind == "condition"]


def _wait_branch(branch: Any) -> Tuple[str, Any]:
    """One branch of a wait, as its kind and its checked target."""
    from desflow.utils import suggestion, time_to_seconds

    _refuse_not_yet(branch)
    if not isinstance(branch, Mapping) or len(branch) != 1:
        raise ValueError(
            f"a wait branch is one of {', '.join(WAIT_KINDS)}, got {branch!r}"
        )
    ((kind, target),) = branch.items()
    if kind not in WAIT_KINDS:
        raise ValueError(
            f"a wait branch is one of {', '.join(WAIT_KINDS)}, got '{kind}'"
            + suggestion(kind, WAIT_KINDS)
        )
    if kind == "timeout":
        delay, value = target, None
        if isinstance(target, Mapping):
            extra = set(target) - {"after", "value"}
            if extra:
                raise ValueError(
                    f"a wait timeout takes after and value, not "
                    f"{', '.join(sorted(extra))}"
                )
            if "after" not in target:
                raise ValueError("a wait timeout mapping needs after")
            delay, value = target["after"], target.get("value")
        seconds = time_to_seconds(delay) if isinstance(delay, str) else delay
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise ValueError(f"a wait timeout is seconds or a duration, got {target!r}")
        if seconds < 0:
            raise ValueError(f"a wait timeout must be 0 or more, got {target!r}")
        return kind, (float(seconds), value)
    if kind == "condition":
        return kind, Expression(target)
    if kind in ("get", "put"):
        try:
            return kind, Transfer.of(target)
        except ValueError as exc:
            raise ValueError(f"wait {kind} branch: {exc}") from None
    if kind == "running":
        if not isinstance(target, Mapping):
            raise ValueError(
                f"a wait running branch takes {{task: name, where: ...}}, got "
                f"{target!r}"
            )
        extra = set(target) - {"task", "where"}
        if extra:
            raise ValueError(
                f"a wait running branch takes task and where, not "
                f"{', '.join(sorted(extra))}"
            )
        running_task = target.get("task")
        if not isinstance(running_task, str):
            raise ValueError(
                f"a wait running branch names a task, got {running_task!r}"
            )
        where = target.get("where")
        return kind, {
            "task": running_task,
            "where": Expression(where) if where is not None else None,
        }
    if not isinstance(target, str):
        raise ValueError(f"a wait {kind} branch names a {kind}, got {target!r}")
    return kind, target
