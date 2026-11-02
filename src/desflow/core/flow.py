"""Routing between tasks, and what a task takes from or adds to a store or container.

`Router` decides the next task when one finishes, from the same rules in all three
APIs. `Transfer` describes a task's `get` or `put`. The builder and YAML tasks use
both; low-level code uses `Router` directly and calls `get` and `put` itself.
"""

import inspect
import numbers
import re
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple, Union

import numpy as np

from desflow.core.expressions import ANY_NAME, Expression, Scope, boolean_word
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
    names: Optional[Mapping[str, Any]] = None,
    entity: Optional[str] = None,
    args: Optional[Mapping[str, Any]] = None,
    cause: Optional[Mapping[str, Any]] = None,
    upstream_payload: Optional[Mapping[str, Any]] = None,
    item_priority: Any = None,
    peek: Optional[Callable[[], Any]] = None,
    instances: Optional[Callable[[str], List[Scope]]] = None,
    stream: Any = None,
    handles: Optional[Callable[[str], Any]] = None,
    request: Optional[Mapping[str, Any]] = None,
    holder: Optional[Mapping[str, Any]] = None,
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
        names: The task's vars, for the bare names of an expression.
        entity: The key of the entity the task runs for, for `gen: entity`.
        args: The process's local names, for `arg`.
        cause: The cause of the interrupt that led here, for `cause`.
        upstream_payload: The payload of the task that routed here, for `upstream`.
        item_priority: The priority of the item a priority store gave.
        peek: Gives the time of the next scheduled event, for `peek()`.
        instances: Gives the scopes of the running instances of a task, for
            `running()`.
        stream: The stream `previous` follows for the entity.
        handles: Gives the opened handle of a name, for `handle`.
        request: The request a preempt rule tests, for `request`.
        holder: The user whose unit it would take, for `holder`.

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
        names=names or {},
        entity=entity,
        args=args,
        cause=cause,
        upstream=upstream_payload,
        item_priority=item_priority,
        peek=peek,
        instances=instances,
        stream=stream,
        handles=handles,
        request=request,
        holder=holder,
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
        # Weights with a text in them: each weight is a number or an `Expression`,
        # read when a task finishes
        self._weights: Optional[List[Union[int, float, Expression]]] = None
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
            self._when.append(
                (Expression(rule["condition"], names=ANY_NAME), str(rule["task"]))
            )
        self._default = rules.get("default")
        if not self._when and self._default is None:
            raise ValueError("next needs when rules, a default or weights")
        self.tasks = {task for _, task in self._when} | (
            {self._default} if self._default is not None else set()
        )

    @property
    def weighted(self) -> bool:
        """True when the rules are weights, which draw from `rng`."""
        return self._cumulative is not None or self._weights is not None

    @property
    def computed(self) -> bool:
        """True when a weight is an expression, so the shares are known only when a
        task finishes."""
        return self._weights is not None

    @property
    def conditions(self) -> List[Expression]:
        """The parsed conditions, in rule order."""
        weights = self._weights or []
        return [
            *(condition for condition, _ in self._when),
            *(w for w in weights if isinstance(w, Expression)),
        ]

    @property
    def probabilities(self) -> Optional[Dict[str, float]]:
        """The share of finished instances each next task receives, where the rules
        fix it: 1.0 for a task name, the normalised weights for weights, and an
        empty mapping when the flow ends. None for when rules or a function, whose
        split depends on the run."""
        if self._function is not None or self._when or self._weights is not None:
            return None
        if self._cumulative is not None:
            shares = np.diff(self._cumulative, prepend=0.0)
            return {name: float(share) for name, share in zip(self._names, shares)}
        return {} if self._default is None else {self._default: 1.0}

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
        if self._weights is not None:
            if scope is None:
                scope = make_scope(upstream, env=env, levels=levels, item=item)
            cumulative = _share_of(self._names, self._weights, scope)
        else:
            cumulative = self._cumulative
        if cumulative is not None:
            if self.rng is None:
                raise ValueError("weights need a random generator to draw from")
            index = int(np.searchsorted(cumulative, self.rng.random(), "right"))
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
        self._names = [str(name) for name in weights]
        self.tasks = set(self._names)
        if any(isinstance(weight, str) for weight in weights.values()):
            # Text is an expression, read when a task finishes; the numbers are
            # checked now, and a result is checked when it is read
            self._weights = [
                Expression(weight)
                if isinstance(weight, str)
                else _checked_weight(name, weight)
                for name, weight in weights.items()
            ]
            return
        self._cumulative = _cumulative(self._names, list(weights.values()))


def _checked_weight(name: Any, weight: Any) -> Union[int, float]:
    """A weight that is a number of 0 or more.

    Raises:
        ValueError: If it is not a number, or is below 0.
    """
    if isinstance(weight, bool) or not isinstance(weight, numbers.Real):
        raise ValueError(f"the weight of '{name}' must be a number")
    if not weight >= 0:
        raise ValueError(f"the weight of '{name}' must be 0 or more")
    return weight


def _cumulative(names: List[str], weights: List[Any]) -> np.ndarray:
    """The running shares of weights: each 0 or more, adding up to more than 0."""
    values = [
        float(_checked_weight(name, weight)) for name, weight in zip(names, weights)
    ]
    total = sum(values)
    if not total > 0:
        raise ValueError("weights must add up to more than 0")
    return np.cumsum(values) / total


def _share_of(
    names: List[str], weights: List[Union[int, float, Expression]], scope: Scope
) -> np.ndarray:
    """The running shares of weights some of which are expressions, each read once
    in `scope`, in the order written."""
    return _cumulative(
        names,
        [w.evaluate(scope) if isinstance(w, Expression) else w for w in weights],
    )


def _names_entity(function: Callable[..., Any]) -> bool:
    """Whether a store filter takes the entity by name.

    True when the function has more than one parameter and one after the first is
    called `entity` and can be passed by keyword. The first parameter is the item.
    A function with one parameter, or one whose first parameter is `entity`, is
    called with the item alone, so a filter written before this rule keeps its call.
    """
    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):
        return False
    names = list(parameters)
    if len(names) < 2 or names[0] == "entity":
        return False
    named = parameters.get("entity")
    return named is not None and named.kind in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    )


class Transfer:
    """
    What a task takes from, or adds to, a store or a container.

    A container transfer moves an amount: a number, an expression such as
    `payload.quantity`, or in Python a function of the task id and the upstream
    payload. A store transfer moves one item: a `get` takes the next item, and a
    `put` adds the task's finished payload.

    A put with `wait=False` is made and not waited for: the task goes on at once, as
    a SimPy process that calls `store.put(item)` without `yield`. The put stays in the
    queue, completes in first in, first out order whenever there is room, and is
    never cancelled by an interrupt, a patience or an abandon. It publishes no
    record, a drain does not wait for it, and one still queued when the run ends is
    dropped.

    Attributes:
        container (Optional[str]): The container's name.
        store (Optional[str]): The store's name.
        amount: The amount, for a container.
        wait (bool): False for a put the task does not wait for.
    """

    def __init__(
        self,
        container: Optional[str] = None,
        store: Optional[str] = None,
        amount: Amount = 1.0,
        priority: Any = 0.0,
        filter: Union[str, Callable[[Any], bool], None] = None,
        wait: bool = True,
        item: Any = None,
    ):
        """
        Checks the transfer.

        Args:
            container: The container's name.
            store: The store's name, instead of a container.
            amount: For a container, a number, an expression or a function.
            priority: For a put into a priority store, any value SimPy can order: a
                number, a string, a tuple, or an expression or function that
                returns one, lowest served first. A string is an expression. It is
                kept as given, so `"10"` from a function compares as text and
                priorities of types that cannot be compared raise `TypeError` at
                the put. Ignored by a `fifo` store.
            filter: For a store get, which item it takes: a condition such as
                `"item.size == attr.size"`, where `item.<field>` reads the item
                offered, or a function of the item returning True. A function with
                more than one parameter, one of them after the first named `entity`,
                also receives the entity that makes the get, by that name: `lambda
                item, entity: ...`. A function with one parameter is called with the
                item, whatever the parameter is named. The get takes the oldest item that matches
                and waits while none does.
            wait: False for a put the task does not wait for (see above). Defaults
                to True.
            item: For a store put, the item to put: a constant, text, or a `{gen:
                ...}` generator read when the put is made. None puts the task's
                finished payload, as a SimPy `put(x)` of the task's own value does.

        Raises:
            ValueError: If not exactly one of container and store is given, an
                amount is given for a store, a priority is given for a
                container, the amount is malformed, the priority is an expression
                that is malformed, or `wait` is not a bool.
        """
        if not isinstance(wait, bool):
            raise ValueError(f"wait must be true or false, got {wait!r}")
        self.wait = wait
        if (container is None) == (store is None):
            raise ValueError("a get or put names one container or one store")
        if store is not None and amount != 1.0:
            raise ValueError("a store moves one item, so it takes no amount")
        if container is not None and priority != 0.0:
            raise ValueError("priority orders a store's items, so it needs a store")
        if container is not None and filter is not None:
            raise ValueError("a filter picks a store's item, so it needs a store")
        if item is not None and store is None:
            raise ValueError("item is the value a store put adds, so it needs a store")
        self.container = container
        self.store = store
        self.amount = amount
        self.priority = priority
        self.item = item
        self._expression: Optional[Expression] = None
        if isinstance(amount, str):
            self._expression = Expression(amount, names=ANY_NAME)
        elif not callable(amount) and (
            isinstance(amount, bool) or not isinstance(amount, (int, float))
        ):
            raise ValueError(
                f"amount must be a number or an expression, got {amount!r}"
            )
        self.filter = filter
        self._filter_expression: Optional[Expression] = None
        self._filter_takes_entity = False
        if isinstance(filter, str):
            self._filter_expression = Expression(filter)
        elif filter is not None and not callable(filter):
            raise ValueError(
                f"filter must be a condition or a function, got {filter!r}"
            )
        elif filter is not None:
            self._filter_takes_entity = _names_entity(filter)
        self._priority_expression: Optional[Expression] = None
        if isinstance(priority, str):
            self._priority_expression = Expression(priority, names=ANY_NAME)

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
        unknown = set(spec) - {
            "container",
            "store",
            "amount",
            "priority",
            "filter",
            "wait",
            "item",
        }
        if unknown:
            raise ValueError(f"a get or put does not take {', '.join(sorted(unknown))}")
        return cls(**spec)

    @property
    def target(self) -> str:
        """The name of the store or container."""
        return self.container if self.container is not None else self.store  # type: ignore[return-value]

    def amount_for(
        self, task_id: Any, upstream: Optional[Upstream], scope: Scope
    ) -> Union[int, float]:
        """
        Returns the amount for one task instance.

        Args:
            task_id: The instance id.
            upstream: What the previous task handed on.
            scope: What an expression's names read.

        Returns:
            Union[int, float]: The amount, as written when it is a number.
        """
        if self._expression is not None:
            return as_given(self._expression.evaluate(scope))
        if callable(self.amount):
            return as_given(self.amount(task_id, payload_of(upstream)))
        return as_given(self.amount)

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

    def filter_for(
        self, scope: Scope, entity: Any = None
    ) -> Optional[Callable[[Any], bool]]:
        """
        Returns the function a store get offers each item to, or None for any item.

        Args:
            scope: What the condition's other names read, taken when the get is made;
                `item` is set to each item offered.
            entity: The entity that makes the get, passed by name to a function that
                names a parameter `entity`.

        Returns:
            Optional[Callable[[Any], bool]]: True for an item the get may take.
        """
        if self._filter_expression is not None:
            condition = self._filter_expression

            def matches(item: Any) -> bool:
                scope.item = item
                return bool(condition.evaluate(scope))

            return matches
        if self._filter_takes_entity:
            function = self.filter

            def matches_entity(item: Any) -> bool:
                return bool(function(item, entity=entity))  # type: ignore[misc,call-arg,operator]

            return matches_entity
        return None if isinstance(self.filter, str) else self.filter

    def priority_for(
        self, task_id: Any, upstream: Optional[Upstream], scope: Scope
    ) -> Any:
        """
        Returns the priority for one put into a priority store, lowest first.

        Args:
            task_id: The instance id.
            upstream: What the previous task handed on.
            scope: What an expression's names read.

        Returns:
            Any: The priority as it was given or computed, not changed to a number.
        """
        if self._priority_expression is not None:
            return self._priority_expression.evaluate(scope)
        if callable(self.priority):
            return self.priority(task_id, payload_of(upstream))
        return self.priority


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
    "handle",
)

# Group kinds: a branch that holds named child branches.
GROUP_KINDS = ("any", "all", "at_least", "when")


class WaitNode:
    """One node of a parsed wait: a leaf, with a kind and a target, or a group, with
    a mode (`any`, `all`, `at_least` or `when`), named child nodes, a count for
    `at_least` and a predicate for `when`."""

    __slots__ = (
        "name",
        "kind",
        "target",
        "children",
        "count",
        "args",
        "predicate",
        "options",
    )

    def __init__(
        self,
        name: Optional[str],
        kind: str,
        target: Any = None,
        children: Optional[List["WaitNode"]] = None,
        count: int = 0,
        args: Optional[Mapping[str, Any]] = None,
        predicate: Optional[Expression] = None,
    ):
        self.name = name
        self.kind = kind
        self.target = target
        self.children: List[WaitNode] = children if children is not None else []
        self.count = count
        # The arguments a `task` leaf passes to its child flow, and the condition of
        # a `when` group
        self.args = args
        self.predicate = predicate
        # The priority and preempt flag of a `request` leaf written as a mapping
        self.options: Optional["RequestOptions"] = None

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
    - `{"when": "done_count == 1 and t < 5", "of": {name: branch, ...}}`: done when the
      expression holds, as SimPy's `Condition(env, evaluate, events)` does. It is
      read each time a branch's event is processed, and once at the start for a
      branch already processed. It reads `wait.<branch>.done`, `.ok` and `.value` of
      the group's branches and `done_count`. A branch that fails fails the wait; an
      empty `of` succeeds at once;
    - `{"task": "child", "args": {...}}`: a `task` branch whose child flow reads the
      argument values as `arg.<name>`, read in the parent's scope when the child
      starts;
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
        keys = set(spec)
        if "when" in keys:
            node = self._when(spec, name)
        elif "at_least" in keys:
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
            args = None
            if "task" in keys and "args" in keys:
                if len(spec) != 2 or not isinstance(spec["args"], Mapping):
                    raise ValueError(
                        "a wait task branch takes task and args, a mapping of "
                        f"values, got {spec!r}"
                    )
                args = spec["args"]
                spec = {"task": spec["task"]}
            options = None
            if isinstance(spec.get("request"), Mapping):
                options = RequestOptions(spec["request"])
                options.mapping = True
                spec = {"request": options.resource}
            kind, target = _wait_branch(spec)
            if name is not None:
                node_name: Optional[str] = name
            elif kind in ("signal", "task", "request", "handle"):
                node_name = target
            elif kind == "running":
                node_name = target["task"]
            else:
                node_name = kind
            node = WaitNode(node_name, kind, target=target, args=args)
            node.options = options
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

    def _when(self, spec: Mapping[str, Any], name: Optional[str]) -> WaitNode:
        """Parses a `{when: <expression>, of: {...}}` group: done when the condition,
        read each time a branch's event is processed, holds."""
        extra = set(spec) - {"when", "of"}
        if extra:
            raise ValueError(f"when takes when and of, not {', '.join(sorted(extra))}")
        text = spec["when"]
        if not isinstance(text, str):
            raise ValueError(f"when is an expression, got {text!r}")
        children = self._children("when", spec.get("of", {}))
        return WaitNode(
            name, "when", children=children, predicate=Expression(text, names=None)
        )

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

    def requests(self) -> List[str]:
        """Returns the resources the wait's `request` leaves name, in the order the
        wait lists them, each once."""
        found: List[str] = []
        for node in self.branches.values():
            if node.kind == "request" and node.target not in found:
                found.append(node.target)
        return found

    def plain_requests(self) -> List[str]:
        """Returns the resources the wait's `request` leaves written as a name,
        in the order the wait lists them, each once. A leaf written as a mapping
        takes no `priority` or `preempt` of the task, so it is not counted."""
        found: List[str] = []
        for node in self.branches.values():
            if (
                node.kind == "request"
                and node.options is None
                and node.target not in found
            ):
                found.append(node.target)
        return found

    def requests_together(self) -> List[Tuple[str, str]]:
        """Returns the pairs of resources the wait can hold one of while it waits for
        the other: two `request` leaves under different branches of an `all`, or of
        an `at_least` that needs two or more; and, under an `any` or an `at_least` of
        one, a request a group branch can hold before that branch is won, with each
        request of the other branches. A single request leaf of an `any` ends the
        `any` when it is granted, so it holds nothing while the `any` waits."""
        pairs: List[Tuple[str, str]] = []

        def pair(first: List[str], later: List[str]) -> None:
            for a in first:
                for b in later:
                    if a != b and {(a, b), (b, a)}.isdisjoint(pairs):
                        pairs.append((a, b))

        def walk(node: WaitNode) -> Tuple[List[str], List[str]]:
            """The node's requests, and those it can hold before it is done."""
            if not node.is_group:
                return ([node.target] if node.kind == "request" else []), []
            found = [walk(child) for child in node.children]
            names = [name for names, _ in found for name in names]
            if node.kind == "all" or (node.kind == "at_least" and node.count > 1):
                for i, (first, _) in enumerate(found):
                    for later, _ in found[i + 1 :]:
                        pair(first, later)
                return names, names
            for i, (_, held) in enumerate(found):
                for j, (others, _) in enumerate(found):
                    if i != j:
                        pair(held, others)
            return names, [name for _, held in found for name in held]

        walk(self.root)
        return pairs

    @property
    def conditions(self) -> List[Expression]:
        """The parsed conditions of the `condition` leaves."""
        return [n.target for n in self.branches.values() if n.kind == "condition"]

    @property
    def predicates(self) -> List[Expression]:
        """The parsed conditions of the `when` groups, read as branches are processed."""
        return [n.predicate for n in self.branches.values() if n.predicate is not None]

    @property
    def request_expressions(self) -> List[Expression]:
        """The parsed expressions of the `priority` and `preempt` of the `request`
        leaves written as mappings, read when the request is made."""
        return [
            e
            for n in self.branches.values()
            if n.options is not None
            for e in n.options.expressions
        ]

    @property
    def time_expressions(self) -> List[Expression]:
        """The parsed expressions of the `timeout` leaves, read when the wait starts."""
        return [
            n.target[0]
            for n in self.branches.values()
            if n.kind == "timeout" and isinstance(n.target[0], Expression)
        ]


def as_given(value: Any) -> Union[int, float]:
    """
    Returns a number as it was written, so an integer amount stays an integer.

    Args:
        value: A number, or other text or value that `float` can read.

    Returns:
        Union[int, float]: The number itself, or `float(value)` for anything else.

    Raises:
        ValueError: If `value` is not a number and `float` cannot read it.
    """
    if isinstance(value, (int, float)):
        return value
    return float(value)


# Text that is only a number, so a name such as `inf` stays an expression
_PLAIN_NUMBER = re.compile(r"[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?")


def _timeout_delay(delay: Any, written: Any) -> Any:
    """The delay of a wait `timeout`, checked: a number of seconds as it is, a
    duration in text as seconds, other text as an `Expression`, and a generator as
    it is. The last two are read when the wait starts.

    Args:
        delay: The delay as written.
        written: The whole `timeout` value, for the message.

    Raises:
        ValueError: If the delay is none of these, or is a negative number.
    """
    from desflow.utils import time_to_seconds
    from desflow.values.models import _Gen

    if isinstance(delay, _Gen):
        return delay
    if isinstance(delay, str):
        try:
            seconds = time_to_seconds(delay)
        except ValueError:
            seconds = None
        if seconds is None:
            if _PLAIN_NUMBER.fullmatch(delay.strip()):
                seconds = float(delay)  # a number in text, such as '-1'
        if seconds is not None:
            return _non_negative(seconds, written)
        try:
            return Expression(delay)
        except ValueError as exc:
            raise ValueError(
                f"a wait timeout is seconds, a duration such as '5 min' or an "
                f"expression, got {written!r}: {exc}"
            ) from None
    if isinstance(delay, bool) or not isinstance(delay, numbers.Real):
        raise ValueError(
            f"a wait timeout is seconds, a duration or an expression, got {written!r}"
        )
    return _non_negative(delay, written)


def _non_negative(seconds: Any, written: Any) -> Any:
    if seconds < 0:
        raise ValueError(f"a wait timeout must be 0 or more, got {written!r}")
    return seconds


def _wait_branch(branch: Any) -> Tuple[str, Any]:
    """One branch of a wait, as its kind and its checked target."""
    from desflow.utils import suggestion

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
        return kind, (_timeout_delay(delay, target), value)
    if kind == "condition":
        return kind, Expression(target)
    if kind in ("get", "put"):
        try:
            transfer = Transfer.of(target)
        except ValueError as exc:
            raise ValueError(f"wait {kind} branch: {exc}") from None
        if not transfer.wait:
            raise ValueError(
                f"wait {kind} branch: wait: false is for a task's own {kind}; a wait "
                f"branch is what the task waits for"
            )
        return kind, transfer
    if kind == "running":
        if not isinstance(target, Mapping):
            raise ValueError(
                f"a wait running branch takes {{task: name, where: ...}}, got "
                f"{target!r}"
            )
        extra = set(target) - {"task", "where", "whole_flow"}
        if extra:
            raise ValueError(
                f"a wait running branch takes task, where and whole_flow, not "
                f"{', '.join(sorted(extra))}"
            )
        whole = target.get("whole_flow", False)
        if not isinstance(whole, bool):
            raise ValueError(
                f"whole_flow of a wait running branch is true or false, got {whole!r}"
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
            "whole_flow": whole,
        }
    if not isinstance(target, str):
        raise ValueError(f"a wait {kind} branch names a {kind}, got {target!r}")
    return kind, target


# The kinds of branch an `open` makes, and those that a group of an `open` may hold
OPEN_LEAVES = ("timeout", "request", "get", "put", "task", "signal")
OPEN_GROUP_LEAVES = ("timeout", "task", "signal", "handle")


class Open:
    """
    What a task's `open` makes at its step: named events, each made in the order
    written and not waited for, that later keys of the task, the tasks after it and
    its child flows reach by name, as a SimPy process keeps `ev = env.timeout(5)` or
    `q = m.request()` in a local name.

    A name takes one branch of a `wait`: `timeout`, `request`, `get`, `put`, `task` or
    `signal`, or `any` or `all` of named `timeout`, `task`, `signal` and `handle`
    branches. A request, get or put in a group is refused, because each of those is
    already a handle of its own: open each under its own name.

    Attributes:
        wait (Wait): The branches, parsed as one `all` group.
        nodes (Dict[str, WaitNode]): The node of each name.
    """

    def __init__(self, spec: Mapping[str, Any]):
        """
        Checks the branches and parses them.

        Args:
            spec: A mapping of names to wait branches.

        Raises:
            ValueError: If the form is malformed, a name is not an identifier, or a
                branch is of a kind `open` does not take.
        """
        if not isinstance(spec, Mapping) or not spec:
            raise ValueError(
                f"open takes a mapping of names to wait branches, got {spec!r}"
            )
        for name in spec:
            if not isinstance(name, str) or not name.isidentifier():
                raise ValueError(
                    f"an open name is read as handle.<name>, so it is letters, "
                    f"digits and _, not starting with a digit, got {name!r}"
                )
        self.wait = Wait({"all": dict(spec)})
        self.nodes: Dict[str, WaitNode] = {n.name: n for n in self.wait.root.children}  # type: ignore[misc]
        for name, node in self.nodes.items():
            self._check(name, node, in_group=False)

    def _check(self, name: str, node: WaitNode, in_group: bool) -> None:
        """Refuses a branch kind `open` does not make, in a group or at the top."""
        if node.is_group:
            if node.kind not in ("any", "all"):
                raise ValueError(
                    f"open '{name}' is an {node.kind} group; open takes any and all"
                )
            for child in node.children:
                self._check(str(child.name), child, in_group=True)
            return
        allowed = OPEN_GROUP_LEAVES if in_group else OPEN_LEAVES
        if node.kind not in allowed:
            what = (
                "a request, get or put branch is opened under its own name, not "
                "inside a group"
                if in_group and node.kind in ("request", "get", "put")
                else "open takes " + ", ".join(allowed)
            )
            raise ValueError(f"open '{name}' has a {node.kind} branch: {what}")

    def requests(self) -> List[str]:
        """The resources the open `request` branches name, in order, each once."""
        return self.wait.requests()

    def plain_requests(self) -> List[str]:
        """The resources the open `request` branches written as a name, in
        order, each once."""
        return self.wait.plain_requests()

    def kind_of(self, name: str) -> str:
        """What the name opens.

        Args:
            name: An opened name.

        Returns:
            str: `timeout`, `request`, `get`, `put`, `task`, `signal`, or `any` or
            `all` for a group.
        """
        return self.nodes[name].kind

    def targets(self, kind: str) -> Set[str]:
        """The tasks, signals or handles the open branches name.

        Args:
            kind: `task`, `signal` or `handle`.

        Returns:
            Set[str]: The names the branches of that kind name.
        """
        return self.wait.targets(kind)

    @property
    def expressions(self) -> List[Expression]:
        """The parsed expressions of the open `timeout` branches and of the `priority`
        and `preempt` of the open `request` branches."""
        return [*self.wait.time_expressions, *self.wait.request_expressions]


class RequestOptions:
    """
    The `resource`, `priority` and `preempt` of one request: a `request` branch written
    as a mapping, and the base of `Seize`.

    Attributes:
        resource (str): The resource.
        priority: A number, the `Expression` of a text, or a function of the task id
            and the entity's attributes; None takes the task's `priority`.
        preempt: A bool, or the `Expression` of a text; None takes the task's.
        mapping (bool): True for a `request` branch written as a mapping. It then
            takes SimPy's defaults for what it leaves out (priority 0, preempt true),
            and not the task's values.
    """

    mapping: bool = False

    def __init__(self, spec: Union[str, Mapping[str, Any]], what: str = "request"):
        """
        Checks the form.

        Args:
            spec: A resource name, or `{resource, priority, preempt}`.
            what: The key's name, for the messages.

        Raises:
            ValueError: If the form is malformed or a text is not an expression.
        """
        if isinstance(spec, str):
            spec = {"resource": spec}
        options = self._OPTIONS
        if not isinstance(spec, Mapping):
            raise ValueError(
                f"{what} is a resource name or {{resource, {', '.join(options)}}}, "
                f"got {spec!r}"
            )
        extra = set(spec) - {"resource", *options}
        if extra or not isinstance(spec.get("resource"), str):
            raise ValueError(
                f"{what} takes a resource and, optionally, "
                f"{', '.join(options[:-1])} and {options[-1]}, got {dict(spec)!r}"
            )
        self.resource: str = spec["resource"]
        priority = spec.get("priority")
        if isinstance(priority, bool) or not (
            priority is None
            or callable(priority)
            or isinstance(priority, (str, numbers.Real))
        ):
            raise ValueError(
                f"{what} priority is a number or an expression, got {priority!r}"
            )
        self.priority: Any = (
            Expression(priority) if isinstance(priority, str) else priority
        )
        preempt = boolean_word(spec.get("preempt"))
        if not (preempt is None or isinstance(preempt, (bool, str))):
            raise ValueError(
                f"{what} preempt is true, false or an expression, got {preempt!r}"
            )
        self.preempt: Any = Expression(preempt) if isinstance(preempt, str) else preempt

    _OPTIONS: Tuple[str, ...] = ("priority", "preempt")

    @property
    def expressions(self) -> List[Expression]:
        """The parsed expressions of the priority and the preempt flag."""
        return [e for e in (self.priority, self.preempt) if isinstance(e, Expression)]


class Seize(RequestOptions):
    """
    One `seize`: a request for a unit of a resource that the flow keeps until a
    `release`, as `q = m.request(); yield q` does.

    Attributes:
        resource (str): The resource.
        priority: A number, the `Expression` of a text, or a function of the task id
            and the entity's attributes; None takes the task's `priority`.
        preempt: A bool, or the `Expression` of a text; None takes the task's.
        patience: The patience as written, or None. A task has one patience, so
            `TaskSpec` takes this when the task gives none.
    """

    _OPTIONS = ("priority", "preempt", "patience")

    def __init__(self, spec: Union[str, Mapping[str, Any]]):
        """
        Checks the form.

        Args:
            spec: A resource name, or `{resource, priority, preempt, patience}`.

        Raises:
            ValueError: If the form is malformed or a text is not an expression.
        """
        super().__init__(spec, "seize")
        self.patience: Any = None if isinstance(spec, str) else spec.get("patience")


class Release:
    """
    One `release`: a unit the flow gives back at the end of the task.

    Three forms: `<resource>` frees the oldest unit of that resource the flow
    holds from a `seize` or an `open`, and does nothing when it holds none;
    `{handle: <name>}` frees the unit of that opened request; `{resource, task,
    where}` frees the units of that resource that instances of `task` hold, however
    they asked (`resource`, `seize`, `open`, or a `request` in a wait) and whichever
    flow they are in, narrowed by `where` in the instance's scope.

    Attributes:
        resource (Optional[str]): The resource of the first and third forms.
        handle (Optional[str]): The name of the second form.
        task (Optional[str]): The task of the third form.
        where (Optional[Expression]): The condition of the third form.
    """

    def __init__(self, spec: Union[str, Mapping[str, Any]]):
        """
        Checks the form.

        Args:
            spec: A resource name, `{handle: name}` or `{resource, task, where}`.

        Raises:
            ValueError: If the form is malformed.
        """
        self.resource: Optional[str] = None
        self.handle: Optional[str] = None
        self.task: Optional[str] = None
        self.where: Optional[Expression] = None
        if isinstance(spec, str):
            self.resource = spec
            return
        if not isinstance(spec, Mapping):
            raise ValueError(
                f"release is a resource name, {{handle: name}} or {{resource, task, "
                f"where}}, got {spec!r}"
            )
        keys = set(spec)
        if keys == {"handle"} and isinstance(spec["handle"], str):
            self.handle = spec["handle"]
        elif (
            {"resource", "task"} <= keys
            and keys <= {"resource", "task", "where"}
            and isinstance(spec["resource"], str)
            and isinstance(spec["task"], str)
        ):
            self.resource, self.task = spec["resource"], spec["task"]
            if spec.get("where") is not None:
                self.where = Expression(spec["where"])
        else:
            raise ValueError(
                f"release is a resource name, {{handle: name}} or {{resource, task, "
                f"where}}, got {dict(spec)!r}"
            )

    @property
    def expressions(self) -> List[Expression]:
        """The parsed `where` condition, if any."""
        return [] if self.where is None else [self.where]
