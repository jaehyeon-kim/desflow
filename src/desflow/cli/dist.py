"""`desflow dist`: lists the distribution families, and converts between a family's
parameters and its mean and standard deviation."""

from typing import Annotated, Any, Dict, Optional

import typer

# The options that take a family's parameters, in the order the help lists them
_OPTIONS = ("rate", "mean", "std", "low", "value", "high", "mode", "shape", "scale")


def register(app: typer.Typer) -> None:
    """Adds the `dist` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("dist")(_dist)


def _option(help: str) -> Any:
    return typer.Option(metavar="X", help=help)


def _dist(
    family: Annotated[
        Optional[str],
        typer.Argument(
            metavar="FAMILY",
            help="A family, such as gamma. Leave it out to list every family.",
        ),
    ] = None,
    rate: Annotated[Optional[float], _option("Events per second.")] = None,
    mean: Annotated[
        Optional[float],
        _option("The mean, a parameter or, with --std, the mean to convert from."),
    ] = None,
    std: Annotated[
        Optional[float],
        _option("The std, a parameter or, with --mean, the std to convert from."),
    ] = None,
    low: Annotated[Optional[float], _option("The lowest value.")] = None,
    value: Annotated[Optional[float], _option("A constant's time.")] = None,
    high: Annotated[Optional[float], _option("The highest value.")] = None,
    mode: Annotated[Optional[float], _option("The most likely value.")] = None,
    shape: Annotated[Optional[float], _option("The shape.")] = None,
    scale: Annotated[Optional[float], _option("The scale.")] = None,
) -> None:
    """List the distribution families, or convert parameters to a mean and std and back.

    Exits 1 for a family with no formula from a mean and std.
    """
    from desflow.core.distributions import DISTRIBUTIONS, parameters_from_moments
    from desflow.utils import suggestion

    if family is None:
        _list_families(DISTRIBUTIONS)
        return
    chosen = DISTRIBUTIONS.get(family)
    if chosen is None:
        raise typer.BadParameter(
            f"no family called '{family}'"
            + (
                suggestion(family, DISTRIBUTIONS)
                or f"; use one of {', '.join(DISTRIBUTIONS)}"
            )
        )
    given = {
        name: number
        for name, number in zip(
            _OPTIONS, (rate, mean, std, low, value, high, mode, shape, scale)
        )
        if number is not None
    }
    if not given:
        typer.echo(_family_text(chosen))
        return
    converting = "mean" not in chosen.parameters and ({"mean", "std"} & set(given))
    if converting:
        if chosen.from_moments is None:
            typer.echo(f"Error: {family}: {chosen.no_formula}", err=True)
            raise typer.Exit(code=1)
        others = sorted(set(given) - {"mean", "std"})
        if others or len(given) != 2:
            raise typer.BadParameter(
                f"{family} converts from --mean and --std together, with no other "
                f"option; give its parameters ({', '.join(chosen.parameters)}) instead"
            )
        try:
            given = parameters_from_moments(chosen, given["mean"], given["std"])
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from None
    try:
        typer.echo(_values_text(chosen, given))
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from None


def _list_families(families: Dict[str, Any]) -> None:
    from rich.console import Console
    from rich.table import Table

    table = Table("Family", "Parameters", "Summary")
    for name, family in families.items():
        table.add_row(name, ", ".join(family.parameters), family.summary)
    Console().print(table)
    typer.echo("Show one with: desflow dist <family>")


def _family_text(family: Any) -> str:
    """A family's parameters, rules and syntax, before any value is given."""
    defaults = ", ".join(f"{k} {v:g}" for k, v in family.defaults.items()) or "none"
    names = family.parameters
    lines = [
        f"{family.name}: {family.summary}",
        f"parameters: {', '.join(names)} (defaults: {defaults})",
        f"rules: {family.rules}",
    ]
    if family.from_moments is not None and "mean" not in names:
        lines.append(
            f"from a mean and std: desflow dist {family.name} --mean M --std S"
        )
    elif family.no_formula:
        lines.append(f"from a mean and std: {family.no_formula}")
    options = " ".join(f"--{n} X" for n in names if n in _OPTIONS)
    if options:
        lines.append(f"mean and std: desflow dist {family.name} {options}")
    keys = ", ".join(f"{n}: ..." for n in names)
    arguments = ", ".join(f"{n}=..." for n in names)
    lines += [
        f"YAML: {{dist: {family.name}, {keys}}}",
        f'Python: dist="{family.name}", {arguments}',
    ]
    return "\n".join(lines)


def _values_text(family: Any, parameters: Dict[str, float]) -> str:
    """The parameters, mean, std and variability of one distribution, and its syntax.

    Raises `ValueError` when a parameter breaks the family's rules.
    """
    from desflow.config import MIN_DELAY
    from desflow.core.distributions import clamped_normal_moments, variability
    from desflow.models.params import DistributionConfig

    config = DistributionConfig(dist=family.name, **parameters)  # type: ignore[arg-type]
    shown = {name: getattr(config, name) for name in family.parameters}
    mean, variance = family.mean(config), family.variance(config)
    moments = f"mean {mean:.6g}, std {variance**0.5:.6g}"
    if family.name == "normal":
        # The variability is that of the draws the sampler gives
        # A normal config always has both, which its validation requires
        assert config.mean is not None and config.std is not None
        mean, variance = clamped_normal_moments(config.mean, config.std)
        moments += (
            f"; draws below {MIN_DELAY:.5f} s are raised to it, which gives "
            f"mean {mean:.6g}, std {variance**0.5:.6g}"
        )
    std = variance**0.5
    fixed = family.fixed(config)
    draw = f"{fixed:.6g}"
    if fixed < MIN_DELAY:
        # The sampler raises every draw below the minimum delay to it
        draw += f", raised to the minimum delay {MIN_DELAY:.5f} s"
    ratio = f" (std / mean {std / mean:.3g})" if mean > 0 else ""
    lines = [
        "parameters: " + ", ".join(f"{k} {v:.6g}" for k, v in shown.items()),
        moments,
        f"variability: {variability(mean, std)}{ratio}",
        f"with no random generator, every draw is {draw}",
    ]
    yaml = ", ".join(f"{name}: {number:.6g}" for name, number in shown.items())
    python = ", ".join(f"{name}={number:.6g}" for name, number in shown.items())
    lines += [
        f"YAML: {{dist: {family.name}, {yaml}}}",
        f'Python: dist="{family.name}", {python}',
    ]
    return "\n".join(lines)
