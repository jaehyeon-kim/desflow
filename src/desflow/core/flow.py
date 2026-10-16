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
) -> Scope:
    """
    Builds what an expression's names read.

    Args:
        upstream: What the previous task handed on; its payload is `payload`.
        attributes: The entity's attributes, read as `attr`. Defaults to the
            upstream's.
        env: The environment, for `registry[...]` and `t`.
        levels: Stores and containers by name, for `level`.

    Returns:
        Scope: The scope.
    """
    if attributes is None:
        attributes = {} if upstream is None else upstream.attributes
    return Scope(
        payload=payload_of(upstream),
        attributes=attributes,
        levels=levels or {},
        registry=None if env is None else env.registry,
        now=0.0 if env is None else env.now,
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
    ) -> Optional[str]:
        """
        Returns the next task's name, or None when the flow ends.

        Args:
            upstream: What the finished task hands on.
            env: The environment, for `registry[...]` and `t` in conditions.
            levels: Stores and containers by name, for `level.<name>`.

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
            scope = make_scope(upstream, env=env, levels=levels)
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
    ):
        """
        Checks the transfer.

        Args:
            container: The container's name.
            store: The store's name, instead of a container.
            amount: For a container, a number, an expression or a function.

        Raises:
            ValueError: If not exactly one of container and store is given, an
                amount is given for a store, or the amount is malformed.
        """
        if (container is None) == (store is None):
            raise ValueError("a get or put names one container or one store")
        if store is not None and amount != 1.0:
            raise ValueError("a store moves one item, so it takes no amount")
        self.container = container
        self.store = store
        self.amount = amount
        self._expression: Optional[Expression] = None
        if isinstance(amount, str):
            self._expression = Expression(amount)
        elif not callable(amount) and (
            isinstance(amount, bool) or not isinstance(amount, (int, float))
        ):
            raise ValueError(
                f"amount must be a number or an expression, got {amount!r}"
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
        unknown = set(spec) - {"container", "store", "amount"}
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
