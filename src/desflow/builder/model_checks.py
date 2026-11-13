"""The checks of a model's declarations, made before each run.

`ModelChecks` is the piece of `SimulationContext` that checks what the declarations
name: the tasks, signals, arrivals, families, resources, stores, containers,
readings and scenario steps. It holds those declarations and nothing of a run, so a
check cannot read or change a run. `SimulationContext._check_tasks` calls it from
`start()` and `analyze()`. Most of the functions these checks call are in
`task_checks`, which the blueprint loader calls too.
"""

import copy
import logging
import numbers
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)

import simpy

from desflow.builder.arrivals import Arrivals
from desflow.core.expressions import ANY_NAME, Expression, member_name
from desflow.core.families import Families, Reference, member_path, split_path
from desflow.builder.holders import Holders
from desflow.core.registry import ParamRejected, SimulationRegistry
from desflow.builder.runs import EndSets
from desflow.builder.scenario import walk_steps
from desflow.builder.signals import Signals, timing_problem
from desflow.builder.task import TaskSpec
from desflow.builder.task_checks import (
    Holding,
    bare_name_problem,
    batch_mismatch,
    batch_mismatch_text,
    builder_value,
    check_acquisition_order,
    families_made_at_first_use,
    first_family_problem,
    flow_sets,
    generator_expressions,
    handle_problem,
    rule_expressions,
    scope_problem,
    whole_flow_problems,
)
from desflow.builder.tasks import Tasks
from desflow.models.params import DistributionConfig
from desflow.utils import suggestion
from desflow.values.check import (
    generators_in,
    lookup_cycle,
    task_value_places,
    task_value_problems,
)
from desflow.values.models import Gen

# Moved from builder/context.py, so it keeps the logger name a user's setup reads
logger = logging.getLogger("desflow.builder.context")


# The kinds of name an expression reads, in the order each check reads them
_RESOURCE_REFERENCES = ("users", "queue", "held", "waiting")
_HOLDER_REFERENCES = ("level", "putters", "getters", "putting", "getting")
_RULE_REFERENCES = (
    *_RESOURCE_REFERENCES,
    *_HOLDER_REFERENCES,
    "count",
    "items",
    "signal",
    "running",
    "registry",
)
_TASK_REFERENCES = (
    *_HOLDER_REFERENCES,
    "running",
    "count",
    "items",
    *_RESOURCE_REFERENCES,
    "registry",
)


def not_added(name: str, added: Iterable[str], how: str) -> str:
    """The end of a message for a name nothing added: the closest, or how to add it.

    It begins with `which`, so the text before it ends in a space or a comma and a
    space; `test_refusal_texts.py` checks each join.

    Args:
        name: The name the model uses.
        added: The names added, from which the closest is suggested.
        how: The call that adds it, such as `@app.task()`.

    Returns:
        str: The end of the message.
    """
    return "which is not added" + (suggestion(name, added) or f"; add it with {how}")


def _unmade_member(
    path: str,
    families: Families,
    holders: Holders,
    sim_id: str,
    tasks: Tasks,
    signals: Signals,
) -> Optional[str]:
    """The text that says why a family never makes the member `path` names, or None
    when the path is not a member path of a resource, store or container family, or
    the family can make the member. The arguments are those of `names_a_member`."""
    found = member_path(path, families.by_name, sim_id)
    if found is None:
        return None
    family = families.by_name[found[0]]
    if family.kind not in ("resource", "store", "container"):
        return None
    if family.name in families_made_at_first_use(tasks.specs, signals.on):
        return None
    declared = family.members
    count = 0 if declared is None else declared
    if not isinstance(count, numbers.Integral):
        return None
    count = int(count)
    member = split_path(path[len(sim_id) + 1 :])[1]
    if member in {member_name(family.name, key) for key in range(count)}:
        return None
    if declared is None:
        made = "the family has no `members`"
    elif count == 0:
        made = "`members: 0` makes none"
    elif count == 1:
        made = f"`members: 1` makes only {member_name(family.name, 0)}"
    else:
        made = (
            f"`members: {count}` makes {member_name(family.name, 0)} to "
            f"{member_name(family.name, count - 1)}"
        )
    return (
        f"'{member}' is a member the family '{family.name}' never makes: {made}, "
        f"and no task or `on:` action picks a member by `key`"
    )


def unmade_member_note(
    path: str,
    families: Families,
    holders: Holders,
    sim_id: str,
    tasks: Tasks,
    signals: Signals,
) -> str:
    """The end of a refusal of a registry path that names a member its family never
    makes, so the reader sees why a path of the right form is refused.

    Args:
        path: The registry path.
        families: The declared families.
        holders: The declared resources, stores and containers.
        sim_id: The model's `sim_id`.
        tasks: The model's tasks.
        signals: The model's signals.

    Returns:
        str: A sentence that starts with a full stop, or "" when the path names no
            such member.
    """
    why = _unmade_member(path, families, holders, sim_id, tasks, signals)
    return "" if why is None else f". {why}"


def names_a_member(
    path: str,
    families: Families,
    holders: Holders,
    sim_id: str,
    tasks: Tasks,
    signals: Signals,
) -> bool:
    """Whether a registry path names an attribute a member of a family has, such as
    `S.resources.desk[3].current_cap`, of a member the family can have.

    The member is made when the run starts or at its first use, so the scratch
    registry does not hold it. The attributes a member has are the paths the registry
    gives an ordinary resource, store or container of the family's settings, found by
    registering one under the member's name; a signal has none. A path that names a
    member with another attribute, such as `desk[3].curent_cap`, is no registry path.
    So is a path of a member the family can never have: no task or `on:` action
    names the family with a `key`, which would make a member at its first use, and the
    key is not one of 0 to `members` - 1 (none, when the family has no `members`). A
    `members` that is read when the run starts is not known here, so such a family
    accepts every key.

    Args:
        path: The registry path.
        families: The declared families.
        holders: The declared resources, stores and containers.
        sim_id: The model's `sim_id`.
        tasks: The model's tasks, which name a family with a `key`.
        signals: The model's signals, whose `on:` actions do the same.

    Returns:
        bool: True when the path names an attribute of a member of a family.
    """
    found = member_path(path, families.by_name, sim_id)
    if found is None:
        return False
    family = families.by_name[found[0]]
    configs = {
        "resource": holders.resources_config,
        "store": holders.stores_config,
        "container": holders.containers_config,
    }.get(family.kind)
    if configs is None or family.name not in configs:
        return False
    member = split_path(path[len(sim_id) + 1 :])[1]
    if _unmade_member(path, families, holders, sim_id, tasks, signals) is not None:
        return False
    registry = SimulationRegistry(simpy.Environment())
    register = {
        "resource": registry.register_resource,
        "store": registry.register_store,
        "container": registry.register_container,
    }[family.kind]
    try:
        register(sim_id, member, copy.copy(configs[family.name]))
    except ValueError:
        # The run refuses the family's own settings, with its own message
        return True
    try:
        registry.get(path)
    except KeyError:
        return False
    return True


def generator_reads(spec: TaskSpec) -> List[Tuple[str, Expression]]:
    """The expressions inside the generators of a task's value places (an `expr`
    generator's text, a `when` condition and an `omit_when`), each with the key of the
    place. Text that does not parse is left to the check of the value.

    Args:
        spec: The task.

    Returns:
        List[Tuple[str, Expression]]: The key of the place and the parsed expression.
    """
    found: List[Tuple[str, Expression]] = []
    for key, _where, _after, place in task_value_places(
        lambda key: builder_value(spec, key)
    ):
        for _path, generator in generators_in(place, (Gen,)):
            for text in generator_expressions(generator):
                try:
                    found.append((key, Expression(text, names=ANY_NAME)))
                except ValueError:
                    continue
    return found


def _interrupt_where(step: Mapping[str, Any]) -> Optional[Expression]:
    """The `where` of a scenario step's interrupt as an expression, or None when the
    step has none. `check_step` has refused text that does not parse."""
    action = step.get("interrupt")
    where = action.get("where") if action is not None else None
    return Expression(where, names=ANY_NAME) if isinstance(where, str) else where


def written_paths(
    scenario_steps: List[Dict[str, Any]], tasks: Tasks, signals: Signals
) -> Set[str]:
    """The registry paths a model writes: its scenario steps and the `path` actions
    of its tasks' and signals' `on:` lists.

    Args:
        scenario_steps: The model's scenario steps.
        tasks: The model's tasks.
        signals: The model's signals.

    Returns:
        Set[str]: The paths.
    """
    paths = {step["path"] for step in scenario_steps if step.get("path")}
    actions = [
        action
        for spec in tasks.specs.values()
        for action in [*spec.on, *(spec.listen.on if spec.listen else [])]
    ] + [action for found in signals.on.values() for action in found]
    return paths | {a["path"] for a in actions if isinstance(a.get("path"), str)}


def _timeout_values(wait: Any) -> Dict[Tuple[Any, ...], Any]:
    """The delay and the `value` of every timeout in a wait, the branch an `each`
    group makes included, by where each is written.

    Args:
        wait: The task's parsed `Wait`, or None.

    Returns:
        Dict[Tuple[Any, ...], Any]: The values the check reads before the vars have a
            value, by path.
    """
    found: Dict[Tuple[Any, ...], Any] = {}

    def walk(node: Any, prefix: Tuple[Any, ...], named: bool = True) -> None:
        if node.kind == "timeout":
            where = prefix + ((node.name,) if named else ())
            found[where + ("timeout",)] = node.target[0]
            found[where] = node.target[1]
        for child in node.children:
            walk(child, prefix)
        if node.template is not None:
            walk(node.template, ("wait", node.name, "branch"), named=False)

    if wait is not None:
        walk(wait.root, ("wait",))
    return found


def _holding(spec: TaskSpec) -> Holding:
    """What a task does with the units a flow holds, for the order check: the resource
    it seizes, the resource a `release` names, and the tasks that can follow it."""
    release = spec.release
    return Holding(
        None if spec.seize is None else spec.seize.resource,
        release.resource if release is not None and release.task is None else None,
        {
            *(spec.router.tasks or ()),
            *(
                target
                for target in (
                    spec.on_fail,
                    spec.on_renege,
                    spec.on_balk,
                    spec.on_interrupt_next,
                )
                if target is not None
            ),
        },
    )


class ModelChecks:
    """The checks of a model's declarations, made before the clock or any connector
    starts.

    It holds the declaration pieces of one context, shared with it, and reads them
    when a check runs, so a declaration added after it is made is checked too.
    `check` takes what a check needs of the context: its `sim_id`, the scratch
    registry and the warnings already logged.

    Args:
        tasks: The model's tasks.
        families: The model's families.
        holders: The model's resources, stores, containers and breakdowns.
        signals: The model's signals.
        arrivals: The model's arrivals.
        readings: The readings of `add_readings`.
        scenario_steps: The scenario's steps.
        services: The services' distributions, by name.
    """

    def __init__(
        self,
        tasks: Tasks,
        families: Families,
        holders: Holders,
        signals: Signals,
        arrivals: Arrivals,
        readings: List[Dict[str, Expression]],
        scenario_steps: List[Dict[str, Any]],
        services: Dict[str, DistributionConfig],
    ) -> None:
        self._tasks = tasks
        self._families = families
        self._holders = holders
        self._signals = signals
        self._arrivals = arrivals
        self._readings = readings
        self._scenario_steps = scenario_steps
        self._services_config = services

    def end_sets(self) -> EndSets:
        """Checks the `spawn`, `subscribe` and `on:` names, then gives the tasks whose
        instances need an end event, so a task knows it at build time.

        Returns:
            EndSets: The tasks the run tracks, copies and ends, and those a
                `release: {task}` names.

        Raises:
            ValueError: Naming the first `spawn`, `subscribe`, `activate`,
                `deactivate` or `on:` name that is not declared, a spawned task
                with an arrival's name, or a fire that cannot be made.
        """
        arrivals = set(self._arrivals.entities)

        def fire_sources(actions: Iterable[Mapping[str, Any]]) -> Iterable[str]:
            for action in actions:
                source = (
                    action.get("fire", {}).get("from") if "fire" in action else None
                )
                if source is not None:
                    yield source

        def check_spawn(task: str, owner: str, spawned: str) -> None:
            if spawned not in self._tasks.specs:
                raise ValueError(
                    f"{owner} spawns task '{spawned}', "
                    + not_added(spawned, self._tasks.specs, "@app.task()")
                )
            if spawned in arrivals:
                raise ValueError(
                    f"task '{spawned}', which {owner} spawns, has the name of arrival "
                    f"'{spawned}'; rename the task or the arrival, because their flow "
                    f"statistics and entity numbers would merge"
                )

        def check_switches(names: Any, owner: str) -> None:
            for arrival in [names] if isinstance(names, str) else list(names or []):
                if arrival not in self._arrivals.spawns:
                    raise ValueError(
                        f"{owner} switches '{arrival}', which is not an arrival with "
                        f"a spawn" + suggestion(arrival, self._arrivals.spawns)
                    )

        def check_action(name: str, action: Mapping[str, Any], owner: str) -> None:
            for key in ("activate", "deactivate"):
                if key in action:
                    check_switches(action[key], owner)
            if "spawn" in action:
                check_spawn(name, owner, action["spawn"]["task"])
            if "interrupt" in action:
                target = action["interrupt"]["task"]
                if target not in self._tasks.specs:
                    raise ValueError(
                        f"the on: action of {owner} interrupts task '{target}', "
                        + not_added(target, self._tasks.specs, "@app.task()")
                    )
            if "resume" in action:
                target = action["resume"]["task"]
                if target not in self._tasks.specs:
                    raise ValueError(
                        f"the on: action of {owner} resumes task '{target}', "
                        + not_added(target, self._tasks.specs, "@app.task()")
                    )
            if "fire" in action:
                signal = action["fire"]["signal"]
                if signal not in self._signals.entries:
                    raise ValueError(
                        f"the on: action of {owner} fires signal '{signal}', "
                        + not_added(
                            signal, self._signals.entries, f"add_signal('{signal}')"
                        )
                    )
                fire = action["fire"]
                problem = timing_problem(
                    fire.get("urgent", False),
                    fire.get("after"),
                    fire.get("priority"),
                    self._signals.declaration(signal).once,
                    "`once=True`",
                )
                if problem is not None:
                    raise ValueError(problem)
                if fire["from"] is not None:
                    check_from(fire["from"], owner)

        def check_from(source: str, owner: str) -> None:
            is_signal = source in self._signals.entries
            is_task = source in self._tasks.specs
            if is_signal and is_task:
                raise ValueError(
                    f"the on: action of {owner} fires from '{source}', which is both "
                    f"a signal and a task; rename one"
                )
            if not is_signal and not is_task:
                raise ValueError(
                    f"the on: action of {owner} fires from '{source}', which is "
                    f"neither a signal nor a task"
                )

        for name, spec in self._tasks.specs.items():
            owner = f"task '{name}'"
            if spec.subscribe is not None:
                for target in sorted(spec.subscribe.targets("task")):
                    if target not in self._tasks.specs:
                        raise ValueError(
                            f"task '{name}' subscribes to task '{target}', "
                            + not_added(target, self._tasks.specs, "@app.task()")
                        )
                for target in sorted(spec.subscribe.targets("signal")):
                    if target not in self._signals.entries:
                        raise ValueError(
                            f"task '{name}' subscribes to signal '{target}', "
                            + not_added(
                                target, self._signals.entries, f"add_signal('{target}')"
                            )
                        )
            if spec.spawn is not None:
                check_spawn(name, owner, spec.spawn["task"])
            check_switches(spec.activate, owner)
            check_switches(spec.deactivate, owner)
            listened = [] if spec.listen is None else spec.listen.on
            for action in [*spec.on, *listened]:
                check_action(name, action, owner)

        for name, actions in self._signals.on.items():
            owner = f"signal '{name}'"
            for action in actions:
                check_action(name, action, owner)

        # The sets, worked out once every name is checked; nothing below raises
        tracked: Set[str] = set()
        copied: Set[str] = set()
        has_on = set()
        for name, spec in self._tasks.specs.items():
            if spec.wait is not None:
                tracked |= spec.wait.targets("running")
            if spec.subscribe is not None:
                tracked |= spec.subscribe.targets("running")
            if spec.on:
                has_on.add(name)
            listened = [] if spec.listen is None else spec.listen.on
            for source in fire_sources([*spec.on, *listened]):
                if source in self._tasks.specs:
                    copied.add(source)
        for actions in self._signals.on.values():
            for source in fire_sources(actions):
                if source in self._tasks.specs:
                    copied.add(source)
        return EndSets(
            tracked=frozenset(tracked),
            copied=frozenset(copied),
            ended=frozenset(tracked | copied | has_on),
            whole_flow=frozenset(
                node.target["task"]
                for spec in self._tasks.specs.values()
                for node in (spec.wait.branches.values() if spec.wait else ())
                if node.kind == "running" and node.target["whole_flow"]
            ),
            held=frozenset(
                spec.release.task
                for spec in self._tasks.specs.values()
                if spec.release is not None and spec.release.task is not None
            ),
        )

    def check(
        self,
        sim_id: str,
        overrides: Optional[Mapping[str, Any]],
        scratch: Callable[[], SimulationRegistry],
        order_warned: Set[str],
    ) -> None:
        """Checks what each task names, before the clock or any connector starts.

        Args:
            sim_id: The model's `sim_id`, the first part of each registry path.
            overrides: The values `start()` applies before the clock starts. The
                scenario is checked against them, as the run will apply it.
            scratch: Builds a new registry with the paths a run registers. It is
                a function, so the registry is built only when a check looks a
                path up, and a refusal of the model's parameters comes at that
                check.
            order_warned: The warnings already logged, so each is logged once
                across runs; a warning logged here is added to it.

        Raises:
            ValueError: Naming the first declaration that is wrong.
        """
        self._check_batches()
        self._check_lookups()
        self._check_families()
        self._check_scenario_steps(sim_id, overrides, scratch)
        self._check_on_paths(sim_id, scratch)
        self._check_spawns_and_breakdowns()
        self._check_resources_held_once()
        self._check_handles()
        self._warn_acquisition_order(order_warned)
        holders = {**self._holders.stores_config, **self._holders.containers_config}
        registry: Optional[SimulationRegistry] = None
        written = written_paths(self._scenario_steps, self._tasks, self._signals)
        for place, expression, _location in rule_expressions(
            self._holders, self._readings, self._signals.on
        ):
            self._check_rule_names(place, expression, holders, sim_id, scratch)
        for place, expression, interrupted in self._run_reads():
            self._check_rule_names(
                place, expression, holders, sim_id, scratch, interrupted
            )
        routes = self._routes()
        self._warn_whole_flows(routes, order_warned)
        after_get_tasks, cause_targets = flow_sets(
            routes,
            [
                name
                for name, spec in self._tasks.specs.items()
                if spec.get is not None and spec.get.store is not None and spec.get.wait
            ],
            [
                target
                for spec in self._tasks.specs.values()
                for target in (spec.on_interrupt_next, spec.on_interrupt_run)
                if target
            ],
        )
        for name, spec in self._tasks.specs.items():
            self._check_task_holding(name, spec)
            self._check_task_signals(name, spec)
            self._check_task_expressions(name, spec, after_get_tasks, cause_targets)
            if registry is None:
                # The paths the run will register, built by the same code
                registry = scratch()
            self._check_task_values(name, spec, registry, written, sim_id)
            self._check_task_reads(name, spec, holders, registry, sim_id)

    def _check_lookups(self) -> None:
        """Checks that every task a `lookup` or `aggregate` names is a task, and that
        no tasks look each other up, because none of them could start. A task that
        `note_values` names must be a task too."""
        for target in sorted(self._tasks.row_targets):
            if target not in self._tasks.specs:
                raise ValueError(
                    f"a lookup or aggregate names task '{target}', "
                    + not_added(target, self._tasks.specs, "@app.task()")
                )
        for task in sorted(self._tasks.gates):
            if task not in self._tasks.specs:
                raise ValueError(
                    f"note_values names task '{task}', "
                    + not_added(task, self._tasks.specs, "@app.task()")
                )
        cycle = lookup_cycle(self._tasks.gates)
        if cycle:
            raise ValueError(
                "tasks look each other up, so none could start a flow before the "
                "other had a row: " + " -> ".join(cycle)
            )

    def _check_batches(self) -> None:
        """Refuses two tasks that share a batch name but differ in what the batch needs
        one answer for.

        Raises:
            ValueError: Naming both tasks and the key that differs.
        """
        found = batch_mismatch(self._tasks.specs)
        if found is not None:
            raise ValueError(batch_mismatch_text(*found))

    def _check_families(self) -> None:
        """Checks that every place naming a family uses a reference, a reference's `of`
        is a family of the right kind, a list names declared entries, and no entry is
        named as a member.

        Raises:
            ValueError: Naming the task and the key.
        """
        found = first_family_problem(
            self._tasks.specs,
            self._families.by_name,
            self._holders,
            self._signals,
            self._readings,
        )
        if found is None:
            return
        section, name = found.holder
        if found.kind == "entry":
            raise ValueError(f"{section}.{name} {found.message}")
        if section == "rule":
            raise ValueError(f"{name} {found.message}")
        label = f"signals.{name}" if section == "signals" else f"task '{name}'"
        raise ValueError(f"{label}: {found.key} {found.message}")

    def _check_scenario_steps(
        self,
        sim_id: str,
        overrides: Optional[Mapping[str, Any]],
        scratch: Callable[[], SimulationRegistry],
    ) -> None:
        """Checks each scenario step against the registry the run will build, after
        the overrides `start()` applies."""
        if self._scenario_steps:
            known = scratch()
            for path, value in (overrides or {}).items():
                try:
                    known.change(path, value)
                except ParamRejected:
                    # A refused override is reported by the run's own
                    # set_exactly, with its own message; change logs nothing here
                    pass
            # In the order run_scenario applies them, so a value that is valid only
            # after an earlier step loads as it runs
            steps = self._scenario_steps
            for index, problem, detail in walk_steps(
                [(step["at"], step.get("path"), step.get("value")) for step in steps],
                known,
                lambda path: names_a_member(
                    str(path),
                    self._families,
                    self._holders,
                    sim_id,
                    self._tasks,
                    self._signals,
                ),
            ):
                step = steps[index]
                if problem == "path":
                    raise ValueError(
                        f"scenario step at {step['at']:g} s sets '{step['path']}', "
                        f"which is not a registry path"
                        + suggestion(step["path"], known.paths())
                        + self._unmade_note(str(step["path"]), sim_id)
                    )
                if problem == "value":
                    raise ValueError(
                        f"scenario step at {step['at']:g} s sets "
                        f"'{step['path']}': {detail}"
                    )
                action = step.get("interrupt")
                if (
                    action is not None
                    and action["task"] is not None
                    and action["task"] not in self._tasks.specs
                ):
                    raise ValueError(
                        f"scenario step at {step['at']:g} s interrupts task "
                        f"'{action['task']}', "
                        + not_added(action["task"], self._tasks.specs, "@app.task()")
                    )

    def _check_on_paths(
        self, sim_id: str, scratch: Callable[[], SimulationRegistry]
    ) -> None:
        """Checks that the `path` of every `on:` action is a registry path, as a
        scenario step's is, because an action on a path the registry lacks ends the
        run with `KeyError` when it first runs.

        Raises:
            ValueError: Naming the task or signal and the path.
        """
        lists = [
            (f"the on: action of task '{name}'", spec.on)
            for name, spec in self._tasks.specs.items()
        ]
        lists += [
            (f"the listen on: action of task '{name}'", spec.listen.on)
            for name, spec in self._tasks.specs.items()
            if spec.listen is not None
        ]
        lists += [
            (f"the on: action of signal '{name}'", found)
            for name, found in self._signals.on.items()
        ]
        registry: Optional[SimulationRegistry] = None
        for place, actions in lists:
            for action in actions:
                path = action.get("path")
                if not isinstance(path, str) or names_a_member(
                    path,
                    self._families,
                    self._holders,
                    sim_id,
                    self._tasks,
                    self._signals,
                ):
                    continue
                if registry is None:
                    registry = scratch()
                try:
                    registry.get(path)
                except KeyError:
                    raise ValueError(
                        f"{place} sets '{path}', which is not a "
                        f"registry path"
                        + suggestion(path, registry.paths())
                        + self._unmade_note(path, sim_id)
                    ) from None

    def _check_spawns_and_breakdowns(self) -> None:
        """Checks that the task each arrival spawns, and the resource each breakdown
        names, are added."""
        for arrival, task in self._arrivals.spawns.items():
            if task not in self._tasks.specs:
                raise ValueError(
                    f"arrival '{arrival}' spawns task '{task}', "
                    + not_added(task, self._tasks.specs, "@app.task()")
                )
        for resource in self._holders.breakdowns_config:
            if resource not in self._holders.resources_config:
                raise ValueError(
                    f"a breakdown names resource '{resource}', "
                    + not_added(
                        resource, self._holders.resources_config, "add_resource()"
                    )
                )

    def _check_resources_held_once(self) -> None:
        """Checks that no task both requests a resource in its wait and lists it in
        `resource`."""
        for name, spec in self._tasks.specs.items():
            for resource in spec.wait.requests() if spec.wait else ():
                if resource in spec.resources:
                    raise ValueError(
                        f"task '{name}' holds each resource once, but resource "
                        f"'{resource}' is both requested in the wait and listed in "
                        f"resource; list it in only one"
                    )

    def _check_handles(self) -> None:
        """Checks that each handle a task uses is opened by a task, as the use needs."""
        opens: Dict[str, List[Tuple[str, str]]] = {}
        for name, spec in self._tasks.specs.items():
            if spec.open is not None:
                for handle in spec.open.nodes:
                    opens.setdefault(handle, []).append(
                        (name, spec.open.opened_as(handle))
                    )
        uses: List[Tuple[str, str, str, str]] = []
        for name, spec in self._tasks.specs.items():
            if spec.wait is not None:
                uses += [
                    (name, "wait", h, "any")
                    for h in sorted(spec.wait.targets("handle"))
                ]
            if spec.open is not None:
                uses += [
                    (name, "open", h, "any")
                    for h in sorted(spec.open.targets("handle"))
                ]
            if spec.wait is not None:
                uses += [
                    (name, "wait", h, "element")
                    for h in sorted(spec.wait.indexed_handles())
                ]
            if spec.open is not None:
                uses += [
                    (name, "open", h, "element")
                    for h in sorted(spec.open.indexed_handles())
                ]
            if spec.cancel is not None:
                uses.append((name, "cancel", spec.cancel, "line"))
                if spec.cancel_index is not None:
                    uses.append((name, "cancel", spec.cancel, "element"))
            if spec.release is not None and spec.release.handle is not None:
                uses.append((name, "release", spec.release.handle, "unit"))
                if spec.release.index is not None:
                    uses.append((name, "release", spec.release.handle, "element"))
            if spec.listen is not None and spec.listen.handle is not None:
                uses.append((name, "listen", spec.listen.handle, "any"))
                if spec.listen.index is not None:
                    uses.append((name, "listen", spec.listen.handle, "element"))
            if spec.interrupt is not None and spec.interrupt.get("handle") is not None:
                uses.append((name, "interrupt", spec.interrupt["handle"], "child"))
                if spec.interrupt.get("index") is not None:
                    uses.append(
                        (name, "interrupt", spec.interrupt["handle"], "element")
                    )
            for key, expression, _place in spec.expressions:
                uses += [
                    (name, key, h, "any")
                    for h in sorted(expression.references["handle"])
                ]
            for key, read in generator_reads(spec):
                uses += [
                    (name, key, h, "any") for h in sorted(read.references["handle"])
                ]
        unmatched = handle_problem(opens, uses)
        if unmatched is not None:
            holder, key, message = unmatched
            raise ValueError(f"task '{holder}': {key} {message}")

    def _warn_acquisition_order(self, order_warned: Set[str]) -> None:
        """Warns, once each, of two tasks that take the same resources in different
        orders."""
        for message in check_acquisition_order(
            {name: list(spec.resources) for name, spec in self._tasks.specs.items()},
            {
                name: (spec.keep, spec.router.tasks or set())
                for name, spec in self._tasks.specs.items()
            },
            {name: spec.wait for name, spec in self._tasks.specs.items()},
            {name: _holding(spec) for name, spec in self._tasks.specs.items()},
        ):
            if message not in order_warned:
                order_warned.add(message)
                logger.warning(message)

    def _routes(self) -> Dict[str, Set[str]]:
        """The tasks each task can go on to: its `next` and its `on_*` routes."""
        return {
            name: {
                *(spec.router.tasks or ()),
                *(
                    target
                    for target in (
                        spec.on_fail,
                        spec.on_renege,
                        spec.on_balk,
                        spec.on_interrupt_next,
                    )
                    if target is not None
                ),
            }
            for name, spec in self._tasks.specs.items()
        }

    def _warn_whole_flows(
        self, routes: Mapping[str, Set[str]], order_warned: Set[str]
    ) -> None:
        """Warns, once each, of a whole-flow wait that its own flow cannot end."""
        waited_handles = {
            handle
            for spec in self._tasks.specs.values()
            if spec.wait is not None
            for handle in sorted(spec.wait.targets("handle"))
        }
        for message in whole_flow_problems(
            {
                # a child flow a task waits for ends before the task does, and so
                # does one it opens when a task waits for its handle
                name: {
                    *routes[name],
                    *([spec.on_interrupt_run] if spec.on_interrupt_run else ()),
                    *(sorted(spec.wait.targets("task")) if spec.wait else ()),
                    *(
                        node.target
                        for handle, node in (
                            spec.open.nodes if spec.open else {}
                        ).items()
                        if node.kind == "task" and handle in waited_handles
                    ),
                }
                for name, spec in self._tasks.specs.items()
            },
            {
                name: [
                    node.target["task"]
                    for node in (spec.wait.branches.values() if spec.wait else ())
                    if node.kind == "running" and node.target.get("whole_flow")
                ]
                for name, spec in self._tasks.specs.items()
            },
        ):
            if message not in order_warned:
                order_warned.add(message)
                logger.warning(message)

    def _check_task_holding(self, name: str, spec: TaskSpec) -> None:
        """Checks the resources, stores, containers and tasks a task holds, waits for
        and goes on to."""
        holds = [
            *spec.resources,
            *([spec.seize.resource] if spec.seize is not None else []),
            *(spec.open.requests() if spec.open is not None else []),
            *(
                [spec.release.resource]
                if spec.release is not None and spec.release.resource is not None
                else []
            ),
        ]
        for resource in holds:
            if resource not in self._holders.resources_config:
                raise ValueError(
                    f"task '{name}' uses resource '{resource}', "
                    + not_added(
                        resource, self._holders.resources_config, "add_resource()"
                    )
                )
        if spec.service_id is not None and spec.service_id not in self._services_config:
            raise ValueError(
                f"task '{name}' uses service '{spec.service_id}', "
                + not_added(spec.service_id, self._services_config, "add_service()")
            )
        for resource in (
            (
                *(spec.wait.requests() if spec.wait else ()),
                *spec.resources,
                *([spec.seize.resource] if spec.seize is not None else []),
                *(spec.open.requests() if spec.open is not None else []),
            )
            if spec.priority is not None
            else ()
        ):
            if (
                resource in self._holders.resources_config
                and self._holders.resource_queues.get(resource) != "priority"
            ):
                raise ValueError(
                    f"task '{name}' has a priority, but resource '{resource}' "
                    f"does not queue by priority; add it with queue='priority'"
                )
            if resource in self._holders.resource_rank:
                raise ValueError(
                    f"task '{name}' has a priority, but resource '{resource}' "
                    f"ranks its queue by an expression, which is every "
                    f"request's priority; remove the priority"
                )
        if (
            spec.seize is not None
            and spec.seize.priority is not None
            and spec.seize.resource in self._holders.resources_config
            and self._holders.resource_queues.get(spec.seize.resource) != "priority"
        ):
            raise ValueError(
                f"task '{name}' has a priority on its seize, but resource "
                f"'{spec.seize.resource}' does not queue by priority; add it "
                f"with queue='priority'"
            )
        if (
            spec.seize is not None
            and spec.seize.priority is not None
            and spec.seize.resource in self._holders.resource_rank
        ):
            raise ValueError(
                f"task '{name}' has a priority on its seize, but resource "
                f"'{spec.seize.resource}' ranks its queue by an expression, which "
                f"is every request's priority; remove the priority"
            )
        for place, group in (
            ("wait", spec.wait),
            ("open", spec.open.wait if spec.open is not None else None),
        ):
            for node in group.all_nodes if group else ():
                if (
                    node.options is None
                    or node.options.priority is None
                    or node.target not in self._holders.resources_config
                ):
                    continue
                if self._holders.resource_queues.get(node.target) != "priority":
                    raise ValueError(
                        f"task '{name}' has a priority on its request in its "
                        f"{place}, but resource '{node.target}' does not queue by "
                        f"priority; add it with queue='priority'"
                    )
                if node.target in self._holders.resource_rank:
                    raise ValueError(
                        f"task '{name}' has a priority on its request in its "
                        f"{place}, but resource '{node.target}' ranks its queue "
                        f"by an expression, which is every request's priority; "
                        f"remove the priority"
                    )
        for transfer in (spec.get, spec.put):
            if transfer is None:
                continue
            kind = "store" if transfer.store is not None else "container"
            config = (
                self._holders.stores_config
                if kind == "store"
                else self._holders.containers_config
            )
            if transfer.target not in config:
                raise ValueError(
                    f"task '{name}' uses {kind} '{transfer.target}', which is not added"
                )
            for store in (
                transfer.target.names
                if isinstance(transfer.target, Reference)
                else (transfer.target,)
            ):
                if transfer.filter is not None and self._holders.store_priority.get(
                    store
                ):
                    raise ValueError(
                        f"task '{name}' filters priority store '{store}'; "
                        f"SimPy has no store that both ranks and filters its items"
                    )
            if (
                transfer.filter is not None
                and kind == "store"
                and (store_type := self._holders.store_types.get(transfer.target))
                is not None
                and not issubclass(store_type, simpy.FilterStore)
            ):
                raise ValueError(
                    f"task '{name}' filters store '{transfer.target}', whose "
                    f"type is not a simpy.FilterStore; subclass FilterStore to "
                    f"filter its gets"
                )
        for place, group in (
            ("wait", spec.wait),
            ("open", spec.open.wait if spec.open is not None else None),
        ):
            for node in group.all_nodes if group else ():
                if (
                    node.kind == "request"
                    and node.target not in self._holders.resources_config
                ):
                    raise ValueError(
                        f"task '{name}' requests resource '{node.target}' in "
                        f"its {place}, which is not added"
                    )
                if node.kind in ("get", "put"):
                    kind = "store" if node.target.store is not None else "container"
                    config = (
                        self._holders.stores_config
                        if kind == "store"
                        else self._holders.containers_config
                    )
                    if node.target.target not in config:
                        raise ValueError(
                            f"task '{name}' {node.kind}s from {kind} "
                            f"'{node.target.target}' in its {place}, which is "
                            "not added"
                        )
        for target in spec.router.tasks or ():
            if target not in self._tasks.specs:
                raise ValueError(
                    f"task '{name}' routes to '{target}', which is not a task"
                )
        for target in (
            spec.on_fail,
            spec.on_renege,
            spec.on_balk,
            spec.on_interrupt_next,
            spec.on_interrupt_run,
            spec.resume["task"] if spec.resume else None,
            spec.interrupt["task"] if spec.interrupt else None,
            *(sorted(spec.wait.targets("task")) if spec.wait else ()),
            *(sorted(spec.open.targets("task")) if spec.open is not None else ()),
            spec.release.task if spec.release is not None else None,
        ):
            if target is not None and target not in self._tasks.specs:
                raise ValueError(
                    f"task '{name}' names task '{target}', "
                    + not_added(target, self._tasks.specs, "@app.task()")
                )

    def _check_task_signals(self, name: str, spec: TaskSpec) -> None:
        """Checks that each signal a task waits for, opens, fires, listens to or
        reads is added."""
        signals = set(spec.wait.targets("signal")) if spec.wait else set()
        if spec.open is not None:
            signals |= spec.open.targets("signal")
        if spec.signal is not None:
            signals.add(spec.signal)
        if spec.listen is not None and spec.listen.signal is not None:
            signals.add(spec.listen.signal)
        expressions = [found.expression for found in spec.expressions]
        expressions += [read for _key, read in generator_reads(spec)]
        for expression in expressions:
            signals |= expression.references["signal"]
        for signal in sorted(signals):
            if signal not in self._signals.entries:
                raise ValueError(
                    f"task '{name}' uses signal '{signal}', which is not added; "
                    f"add it with add_signal('{signal}')"
                )

    def _check_task_expressions(
        self,
        name: str,
        spec: TaskSpec,
        after_get_tasks: Set[str],
        cause_targets: Set[str],
    ) -> None:
        """Checks what each expression of a task may read where it is written."""
        branches = set(spec.wait.branches) if spec.wait else set()
        for key, expression, _place in spec.expressions:
            problem = scope_problem(
                expression,
                own_item=bool(
                    spec.get is not None
                    and spec.get.store is not None
                    and (
                        key == "get"
                        or (
                            spec.get.wait
                            and key in ("put", "next", "fail", "service_time")
                        )
                    )
                ),
                later_item=name in after_get_tasks,
                cause_ok=name in cause_targets,
                in_when=key == "wait_when",
                has_each=spec.has_each,
                each_here=key in ("wait", "open", "subscribe"),
            )
            if problem:
                raise ValueError(f"task '{name}' reads '{expression.text}': {problem}")
            unknown = expression.references["wait"] - branches
            # balk and patience are read before the wait starts
            early = key in (
                "wait",
                "batch",
                "balk",
                "patience",
                "open",
                "seize",
                "preempt",
            )
            if unknown or (expression.references["wait"] and early):
                branch = sorted(unknown or expression.references["wait"])[0]
                raise ValueError(
                    f"task '{name}' reads wait.{branch} in '{expression.text}', "
                    f"which is not a branch of its wait read after it"
                )
            # item.<field> reads what the task's store get took, so it means
            # nothing before the get, and nothing without one
            takes_item = spec.get is not None and spec.get.store is not None
            # A get that is not waited for gives the task no item, so only its
            # own filter reads one
            after_get = key == "get" or (
                takes_item
                and spec.get is not None
                and spec.get.wait
                and key in ("put", "next", "fail", "service_time")
            )
            if (
                expression.references["item"]
                and not (takes_item and after_get)
                and name not in after_get_tasks
            ):
                raise ValueError(
                    f"task '{name}' reads item.<field> in '{expression.text}', "
                    f"but it has no store get before it"
                )
        batch_parts = (
            {}
            if spec.batch is None
            else {
                id(spec.batch.size_of): "size",
                id(spec.batch.timeout_of): "timeout",
                id(spec.batch.close_when_of): "close_when",
            }
        )
        for key, expression, _place in spec.expressions:
            place = (
                f"batch {batch_parts[id(expression)]}"
                if key == "batch"
                else "wait"
                if key.startswith("wait_")
                else key
            )
            problem = bare_name_problem(
                expression,
                place,
                key,
                spec.var_names,
                {*spec.vars_once, *spec.vars},
            )
            if problem:
                raise ValueError(f"task '{name}' {problem}")

    def _check_task_values(
        self,
        name: str,
        spec: TaskSpec,
        registry: SimulationRegistry,
        written: Set[str],
        sim_id: str,
    ) -> None:
        """Checks the generated values of a task against the registry the run will
        build and the paths a run writes."""
        problems = task_value_problems(
            vars_once=spec.vars_once,
            vars=spec.vars,
            on_start=spec.on_start,
            finish_vars=spec.finish_vars,
            others={
                "set": spec.attribute_changes,
                "interrupt": spec.interrupt or {},
                "on": spec.on,
                "fail_reason": spec.fail_reason,
                "listen": [] if spec.listen is None else spec.listen.on,
            },
            wait_values={
                **_timeout_values(spec.wait),
                ("patience",): spec.patience,
                ("key",): spec.key,
                ("batch", "timeout"): (
                    None if spec.batch is None else spec.batch.timeout
                ),
            },
            service_time=spec.service_time,
            services=set(self._services_config),
            base_dir=Path.cwd(),
            registry=registry,
            early_end=(
                spec.patience is not None
                or spec.balk is not None
                or spec.wait is not None
                or spec.on_interrupt == "abandon"
                or self._holders.ends_early(
                    sim_id,
                    (
                        *spec.resources,
                        *([spec.seize.resource] if spec.seize is not None else []),
                        *(spec.open.requests() if spec.open is not None else []),
                    ),
                    written,
                )
            ),
        )
        if problems:
            where, message = problems[0]
            raise ValueError(
                f"task '{name}' {'.'.join(str(p) for p in where)}: {message}"
            )

    def _check_task_reads(
        self,
        name: str,
        spec: TaskSpec,
        holders: Mapping[str, Any],
        registry: SimulationRegistry,
        sim_id: str,
    ) -> None:
        """Checks that each name a task's expressions read is declared, those in its
        generators too."""
        expressions = [found.expression for found in spec.expressions]
        expressions += [read for _key, read in generator_reads(spec)]
        for expression in expressions:
            for kind, ref in self._unknown_references(
                expression, _TASK_REFERENCES, holders, lambda: registry, sim_id
            ):
                if kind in ("putting", "getting"):
                    raise ValueError(
                        f"task '{name}' reads {kind}({ref}), but no store "
                        f"or container is called '{ref}'"
                    )
                if kind in _HOLDER_REFERENCES:
                    raise ValueError(
                        f"task '{name}' reads {kind}.{ref}, but no "
                        f"store or container is called '{ref}'"
                    )
                if kind == "running":
                    raise ValueError(
                        f"task '{name}' reads running({ref}), but no task is "
                        f"called '{ref}'"
                        + (
                            suggestion(ref, self._tasks.specs)
                            or "; add it with @app.task()"
                        )
                    )
                if kind in ("count", "items"):
                    raise ValueError(
                        f"task '{name}' reads {kind}({ref}), but no store "
                        f"is called '{ref}'"
                    )
                if kind in _RESOURCE_REFERENCES:
                    raise ValueError(
                        f"task '{name}' reads {kind}.{ref}, but no "
                        f"resource is called '{ref}'"
                        + (
                            suggestion(ref, self._holders.resources_config)
                            or "; add it with add_resource()"
                        )
                    )
                raise ValueError(
                    f"task '{name}' reads registry[\"{ref}\"], which is "
                    f"not a registry path" + self._unmade_note(ref, sim_id)
                )

    def _unmade_note(self, path: str, sim_id: str) -> str:
        """The end of a refusal of `path`: why its family never makes the member it
        names, or "" when it names no such member."""
        return unmade_member_note(
            path,
            self._families,
            self._holders,
            sim_id,
            self._tasks,
            self._signals,
        )

    def _run_reads(self) -> Iterator[Tuple[str, Expression, Optional[str]]]:
        """Yields each expression the run places of an arrival and the values of a
        scenario step hold, with the place for a message and the task whose scope it
        is read in: an `Expression` given as the value, the text of each `expr`,
        `when` or `omit_when` of a generator in it, and the `where` of a scenario
        step's interrupt, as text or an `Expression`. Only that `where` has a task,
        the one it interrupts, because `interrupt` reads it in each instance's scope.
        Text that does not parse is left to the check of the value."""
        places: List[Tuple[str, Any]] = []
        for name, plan in self._arrivals.plans.items():
            where = f"arrival '{name}'"
            places += [
                (f"{where} {key}", getattr(plan, key))
                for key in ("gap", "count", "stagger", "max_events", "group", "key")
            ]
            places += [
                (f"{where} vars.{var}", value) for var, value in plan.vars.items()
            ]
        for name, (attributes, _prefix) in self._arrivals.entities.items():
            places.append((f"arrival '{name}' attributes", attributes))
        for step in self._scenario_steps:
            where = f"scenario step at {step['at']:g} s"
            places += [(f"{where} {key}", step.get(key)) for key in ("value", "record")]
            places.append((f"{where} interrupt", step.get("interrupt")))
            condition = _interrupt_where(step)
            if condition is not None:
                yield f"{where} interrupt where", condition, step["interrupt"]["task"]
        for place, value in places:
            if isinstance(value, Expression):
                yield place, value, None
            for _path, generator in generators_in(value, (Gen,)):
                for text in generator_expressions(generator):
                    try:
                        yield place, Expression(text, names=ANY_NAME), None
                    except ValueError:
                        continue

    def _check_rule_names(
        self,
        place: str,
        expression: Expression,
        holders: Mapping[str, Any],
        sim_id: str,
        scratch: Callable[[], SimulationRegistry],
        interrupted: Optional[str] = None,
    ) -> None:
        """Checks that the resources, stores, containers, signals and registry paths a
        queue rank or a preempt rule reads exist, and the `wait.<branch>` names.

        Args:
            interrupted: The task a scenario step's interrupt `where` is read for, in
                the scope of each of its instances, so it reads a branch of that
                task's wait; None elsewhere, where no wait is in scope.

        Raises:
            ValueError: Naming the place and the name, for the first that does not.
        """
        if interrupted is not None:
            spec = self._tasks.specs[interrupted]
            branches = set(spec.wait.branches) if spec.wait else set()
            for branch in sorted(expression.references["wait"] - branches):
                raise ValueError(
                    f"{place} reads wait.{branch}, which is not a branch of the wait "
                    f"of task '{interrupted}'" + suggestion(branch, branches)
                )
        for branch in sorted(
            expression.references["wait"] if interrupted is None else ()
        ):
            raise ValueError(
                f"{place} reads wait.{branch}, where no wait is in scope; a signal's "
                f"on entries run with no wait in scope"
            )
        for kind, name in self._unknown_references(
            expression, _RULE_REFERENCES, holders, scratch, sim_id
        ):
            if kind in _RESOURCE_REFERENCES:
                raise ValueError(
                    f"{place} reads {kind}.{name}, but no resource is called "
                    f"'{name}'" + suggestion(name, self._holders.resources_config)
                )
            if kind in _HOLDER_REFERENCES:
                raise ValueError(
                    f"{place} reads {kind}.{name}, but no store or container "
                    f"is called '{name}'" + suggestion(name, holders)
                )
            if kind in ("count", "items"):
                raise ValueError(
                    f"{place} reads count or items of '{name}', but no store is "
                    f"called '{name}'" + suggestion(name, self._holders.stores_config)
                )
            if kind == "signal":
                raise ValueError(
                    f"{place} reads signal.{name}, but no signal is called '{name}'"
                )
            if kind == "running":
                raise ValueError(
                    f"{place} reads running({name}), but no task is called '{name}'"
                )
            raise ValueError(
                f'{place} reads registry["{name}"], which is not a registry path'
                + self._unmade_note(name, sim_id)
            )

    def _unknown_references(
        self,
        expression: Expression,
        kinds: Tuple[str, ...],
        holders: Mapping[str, Any],
        registry: Callable[[], SimulationRegistry],
        sim_id: str,
    ) -> Iterator[Tuple[str, str]]:
        """Yields `(kind, name)` for each name an expression reads that the model does
        not declare, in the order of `kinds` and then of the expression's references,
        so each caller words its own message.

        Args:
            expression: The expression.
            kinds: The kinds of reference to check, in the order they are checked.
            holders: The stores and containers by name.
            registry: Gives the scratch registry a `registry[...]` path is looked up
                in; it is called for each path that names no member of a family.
            sim_id: The model's `sim_id`, the first part of each registry path.
        """
        for kind in kinds:
            for name in sorted(expression.references[kind]):
                if kind in _RESOURCE_REFERENCES:
                    known = name in self._holders.resources_config
                elif kind in _HOLDER_REFERENCES:
                    known = name in holders
                elif kind in ("count", "items"):
                    known = name in self._holders.stores_config
                elif kind == "signal":
                    known = name in self._signals.entries
                elif kind == "running":
                    known = name in self._tasks.specs
                elif names_a_member(
                    name,
                    self._families,
                    self._holders,
                    sim_id,
                    self._tasks,
                    self._signals,
                ):
                    known = True
                else:
                    try:
                        registry().get(name)
                        known = True
                    except KeyError:
                        known = False
                if not known:
                    yield kind, name
