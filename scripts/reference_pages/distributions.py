"""The Distributions page, and its sections the skill also uses."""

from typing import Dict, List

# One example per family, in the units a blueprint uses. The code does not know a
# typical value, so these are written here; every number below them is computed.
EXAMPLES: Dict[str, Dict[str, object]] = {
    "exponential": {"rate": 0.5},
    "normal": {"mean": 3.0, "std": 0.5},
    "lognormal": {"mean": 4.0, "std": 3.0},
    "truncated_normal": {"mean": 1.0, "std": 1.0, "low": 0.0},
    "constant": {"value": 1.5},
    "uniform": {"low": 1.0, "high": 2.0},
    "triangular": {"low": 1.0, "mode": 1.25, "high": 2.25},
    "gamma": {"shape": 9.0, "scale": 0.1667},
    "weibull": {"shape": 1.5, "scale": 3600.0},
    "empirical": {"values": [1.2, 1.5, 1.5, 2.1, 3.0]},
}


def _number(value: float) -> str:
    return f"{value:.4g}"


def distributions_page() -> str:
    """The Distributions reference page, as Markdown."""
    from desflow.core.distributions import TIME_LIST_PARAMETERS, TIME_PARAMETERS

    lines: List[str] = [
        "# Distributions",
        "",
        "Every distribution family desflow can draw from, generated from the registry of "
        "distributions in the code. [Arrivals and Services](../user-guide/arrivals-and-services.md) "
        "explains how a draw works, and how each family is seeded and changed live.",
        "",
        "Each family lists its parameters, the rules they must follow, and the registry "
        "path of each parameter that can change during a run. The mean, variance and "
        "value with no random generator are computed by the family's own functions for "
        "the example given. In a blueprint, a time-valued parameter "
        f"({', '.join(f'`{p}`' for p in TIME_PARAMETERS + TIME_LIST_PARAMETERS)}) also takes "
        "a duration such as `5 min`. The builder and the low-level API take these "
        'parameters in seconds only, so `add_service("packing", dist="constant", '
        'value=120)`, not `value="2 min"`; a task\'s `patience` takes a duration in '
        "the builder as well.",
        "",
    ]
    lines += distribution_sections()
    return "\n".join(lines) + "\n"


def distribution_sections() -> List[str]:
    """The family table and one section per family, as lines of Markdown.

    The Distributions page and the skill's `references/distributions.md` both use
    these lines.
    """
    from desflow.core.distributions import DISTRIBUTIONS
    from desflow.models.params import DistributionConfig

    lines: List[str] = [
        "| Family | Parameters | Live paths |",
        "|---|---|---|",
    ]
    for name, family in DISTRIBUTIONS.items():
        live = ", ".join(f"`.{p}`" for p in family.live) or "none"
        params = ", ".join(f"`{p}`" for p in family.parameters)
        lines.append(f"| [`{name}`](#{name}) | {params} | {live} |")
    for name, family in DISTRIBUTIONS.items():
        example = EXAMPLES[name]
        config = DistributionConfig(dist=name, **example)  # type: ignore[arg-type]
        yaml_line = ", ".join(
            f"{key}: {value}" for key, value in {"dist": name, **example}.items()
        ).replace("'", "")
        defaults = ", ".join(f"`{k}` {v:g}" for k, v in family.defaults.items())
        lines += [
            "",
            f"## {name}",
            "",
            family.summary,
            "",
            "| | |",
            "|---|---|",
            f"| `dist` | `{name}` |",
            f"| Parameters | {', '.join(f'`{p}`' for p in family.parameters)} |",
            f"| Defaults | {defaults or 'none'} |",
            f"| Rules | {family.rules} |",
            "| Registry paths | "
            + (
                ", ".join(f"`<sim_id>.service.<name>.{p}`" for p in family.live)
                + " (and `.arrival.` for an arrival)"
                if family.live
                else "none: the lists are taken at load"
            )
            + " |",
            f"| Uses a random number | {'yes' if family.random else 'no'} |",
            "",
            f"For the example `{{{yaml_line}}}`: mean {_number(family.mean(config))}, "
            f"variance {_number(family.variance(config))}, and with no random "
            f"generator every draw is {_number(family.fixed(config))}.",
        ]
    return lines
