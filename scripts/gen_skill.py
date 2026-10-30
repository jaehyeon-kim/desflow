"""Generate the desflow skill in `skills/desflow/` for AI coding assistants.

The skill follows the Agent Skills specification (https://agentskills.io/specification):
a folder with a `SKILL.md` and files it links to. The instructions are written by hand
in `scripts/skill/SKILL.template.md`. Every table and reference file is generated from
the code and from the functions that write the Reference pages, so the skill and the
documentation cannot disagree.

Run it after a change to the template, the blueprint models, the distributions, the
connectors or the `local` example:

    uv run --all-extras python scripts/gen_skill.py

`tests/docs/test_skill.py` fails when the committed folder differs from this output.
The skill carries the desflow version from `pyproject.toml` and no date, so a release
changes only the lines that hold the version.
"""

import importlib.util
import json
import tomllib
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parents[1]
SKILL = ROOT / "skills" / "desflow"
TEMPLATE = ROOT / "scripts" / "skill" / "SKILL.template.md"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gen_pages = _load("gen_pages")

# One line for each top-level key of a blueprint. `sections()` fails when a key of the
# schema is missing here, or one here is not in the schema.
SECTIONS: Dict[str, str] = {
    "simulation": "Required. The simulation id, pacing factor, random seed, time zone, warm-up and clock.",
    "resources": "Named resources with a capacity, a queue order, preemption and breakdowns.",
    "containers": "Named containers that hold a continuous amount.",
    "stores": "Named stores that hold items.",
    "services": "Named distributions that give how long a task step takes.",
    "arrivals": "When entities arrive: a distribution or a replay of recorded rows, and the task each one starts.",
    "variables": "Values that expressions and tasks read and that a run can change.",
    "signals": "Named events that tasks fire and wait for.",
    "tasks": "What an entity does: wait, request a resource, take or put an amount, pass the time, then go to the next task.",
    "telemetry": "Custom readings published at a fixed interval.",
    "processes": "Python functions that run beside the tasks, named with `!python`.",
    "scenario": "Changes to a parameter at set times in the run, and records of what was true then.",
    "ingress": "Where parameter changes come in from: Local, File, Kafka, Postgres or Redis.",
    "egress": "Where records go out to: Console, Kafka, Parquet, Jsonl, Iceberg, Postgres or Redis.",
    "batching": "How records are grouped before they are sent.",
    "stats": "Which built-in statistics are published, and how often.",
    "run": "How long the run lasts, whether to drain at the end, and functions to call before it starts.",
}


def version() -> str:
    """The desflow version in `pyproject.toml`."""
    with (ROOT / "pyproject.toml").open("rb") as file:
        return str(tomllib.load(file)["project"]["version"])


def sections() -> str:
    """The table of blueprint sections, one row for each top-level key."""
    from desflow.blueprint.schema import schema_text

    keys = list(json.loads(schema_text())["properties"])
    assert set(keys) == set(SECTIONS), (
        f"SECTIONS in scripts/gen_skill.py differs from the schema: missing "
        f"{sorted(set(keys) - set(SECTIONS))}, unknown {sorted(set(SECTIONS) - set(keys))}"
    )
    rows = ["| Section | What it holds |", "|---|---|"]
    rows += [f"| `{key}` | {SECTIONS[key]} |" for key in keys]
    return "\n".join(rows)


def template() -> str:
    """`SKILL.md`: the template with the version and the generated tables."""
    text = TEMPLATE.read_text(encoding="utf-8")
    text = text.replace("{{version}}", version())
    # Links are written `](skill:references/...)` in the template, which sits in another
    # folder than the skill; the `skill:` is dropped here so they resolve in the skill.
    text = text.replace("](skill:", "](")
    return text.replace("<!-- gen:sections -->", sections())


def blueprint_keys() -> str:
    """`references/blueprint-keys.md`: the body of the YAML Keys page."""
    lines = [
        "# Blueprint keys",
        "",
        "Every key a desflow blueprint takes, from the JSON Schema in "
        "[../assets/blueprint.schema.json](../assets/blueprint.schema.json). Where a "
        "value is a number, a whole number, `true` or `false`, or one of a fixed set "
        "of names, a `${VAR}` or `${VAR:-default}` reference is also accepted, and "
        "a Duration is a number and a unit such as `5 min`.",
        "",
    ]
    return "\n".join(lines + gen_pages.yaml_keys_body(2)).rstrip("\n") + "\n"


def distributions() -> str:
    """`references/distributions.md`: the families, as on the Distributions page."""
    from desflow.core.distributions import TIME_LIST_PARAMETERS, TIME_PARAMETERS

    times = ", ".join(f"`{p}`" for p in TIME_PARAMETERS + TIME_LIST_PARAMETERS)
    lines = [
        "# Distributions",
        "",
        "Every distribution family a `services` entry or an arrival can draw from. "
        f"In a blueprint, a time-valued parameter ({times}) also takes a duration such "
        "as `5 min`. `rate` is a number per second. The builder and the low-level API "
        "take seconds only.",
        "",
    ]
    return "\n".join(lines + gen_pages.distribution_sections()) + "\n"


def connectors() -> str:
    """`references/connectors.md`: each short name, its class and its extra.

    The serializer table is written here by hand, because the registry and the extra
    of each class are not in the code; `test_skill.py` checks that it names every
    Confluent and Glue serializer of `desflow.connectors.egress.kafka` and every
    Confluent and Glue deserializer of `desflow.connectors.ingress.kafka`.
    """
    from desflow.blueprint.build import EGRESS_TYPES, INGRESS_TYPES

    lines = [
        "# Connectors",
        "",
        "The short names a connector `type` takes in `ingress` and `egress`. A name is "
        "written with its capital letter. `type` also takes a `!python` reference to "
        "your own class. A connector that needs an extra is installed with "
        '`pip install "desflow[<extra>]"`.',
    ]
    for title, table in (("Ingress", INGRESS_TYPES), ("Egress", EGRESS_TYPES)):
        lines += [
            "",
            f"## {title}",
            "",
            "| Short name | Class | Extra |",
            "|---|---|---|",
        ]
        for name, (module, cls, extra) in table.items():
            lines.append(
                f"| `{name}` | `{module}.{cls}` | {f'`{extra}`' if extra else 'none'} |"
            )
    from desflow.connectors.egress.storage import _FILESYSTEM_TYPES

    lines += [
        "",
        "## Storage",
        "",
        "A `Parquet` or `Jsonl` egress writes through a `filesystem` mapping. Its `type` "
        "is one of the PyArrow classes below, and its other keys are the arguments of "
        "that class.",
        "",
        "| `filesystem` type | PyArrow class |",
        "|---|---|",
    ]
    lines += [f"| `{kind}` | `{cls}` |" for kind, cls in _FILESYSTEM_TYPES.items()]
    lines += [
        "",
        "An `Iceberg` egress writes through a `catalog` mapping of PyIceberg properties. "
        "Its `type` is `rest` for an Iceberg REST catalog, `sql` for a SQL catalog on "
        "SQLite or PostgreSQL, or `glue` for AWS Glue, and the `iceberg` extra installs "
        "all three.",
    ]
    lines += [
        "",
        "## Serializers and Deserializers",
        "",
        "The serializer of a `KafkaEgress` topic and the deserializer of a `KafkaIngress` "
        "topic, given in Python. Each needs its own extra.",
        "",
        "| Format | Schema registry | Extra | Serializer | Deserializer |",
        "|---|---|---|---|---|",
    ]
    for fmt, registry, extra, stem in (
        ("Avro", "Confluent Schema Registry", "confluent", "ConfluentAvro"),
        ("Avro", "AWS Glue Schema Registry", "glue", "GlueAvro"),
        (
            "JSON Schema",
            "Confluent Schema Registry",
            "confluent",
            "ConfluentJsonSchema",
        ),
        ("JSON Schema", "AWS Glue Schema Registry", "glue", "GlueJsonSchema"),
        ("Protobuf", "Confluent Schema Registry", "confluent", "ConfluentProtobuf"),
        ("Protobuf", "AWS Glue Schema Registry", "glue", "GlueProtobuf"),
    ):
        lines.append(
            f"| {fmt} | {registry} | `{extra}` "
            f"| `desflow.connectors.egress.kafka.{stem}Serializer` "
            f"| `desflow.connectors.ingress.kafka.{stem}Deserializer` |"
        )
    lines += [
        "",
        "The Confluent classes take `auth`, `verify`, `cert`, `timeout`, `headers`, "
        "`proxy` and `max_retries` for a secured registry. Each step of a request to "
        "the registry times out after 10 seconds by default; `timeout=None` waits "
        "without a limit.",
        "",
        "The JSON Schema serializers take the schema as the JSON string `schema_str`. "
        "The Protobuf classes take `message_type`, the class that `protoc` generated. "
        "`GlueProtobufSerializer` also takes `schema_str`, the text of the `.proto` "
        "file. A deserializer returns a 64 bit integer field of a Protobuf message as "
        "a string.",
    ]
    return "\n".join(lines) + "\n"


def cli() -> str:
    """`references/cli.md`: the help of every command, as the CLI pages show it."""
    from typer.main import get_command

    from desflow.cli import app

    lines = [
        "# Commands",
        "",
        "The help of every `desflow` command, generated from the command's code.",
    ]
    for path, text in gen_pages.commands(get_command(app), "desflow"):
        lines += ["", f"## `{path}`", "", "```text", text.rstrip("\n"), "```"]
    return "\n".join(lines) + "\n"


def api() -> str:
    """`references/api.md`: every name `desflow` exports, with its summary."""
    import desflow

    lines = [
        "# Python API",
        "",
        "Every name `from desflow import ...` gives, with the first line of its "
        "docstring. https://jaehyeon.me/desflow/latest/reference/api/ has each one in full.",
        "",
        "| Name | What it is |",
        "|---|---|",
    ]
    for name in sorted(desflow.__all__):
        doc = (getattr(desflow, name).__doc__ or "").strip().split("\n\n")[0]
        summary = " ".join(doc.split()).replace("|", "\\|")
        lines.append(f"| `{name}` | {summary} |")
    return "\n".join(lines) + "\n"


def apis() -> str:
    """`references/apis.md`: the `local` example in the three ways to write it."""
    from desflow.examples import EXAMPLES

    example = EXAMPLES["local"]
    lines = [
        "# One model in three ways",
        "",
        "The `local` example is a shop that packs orders. The three files give the same "
        "records. Download them with `desflow examples get local`.",
    ]
    for api, label, language in (
        ("yaml", "YAML blueprint", "yaml"),
        ("builder", "Builder API", "python"),
        ("low-level", "Low-level API", "python"),
    ):
        path = example.files[api][0]
        source = (ROOT / "examples" / path).read_text(encoding="utf-8").rstrip("\n")
        lines += [
            "",
            f"## {label}",
            "",
            f"`examples/{path}`",
            "",
            f"```{language}",
            source,
            "```",
        ]
    return "\n".join(lines) + "\n"


def files() -> Dict[str, str]:
    """Every file of the skill, by its path under `skills/desflow/`."""
    from desflow.blueprint.schema import schema_text

    return {
        "SKILL.md": template(),
        "references/blueprint-keys.md": blueprint_keys(),
        "references/distributions.md": distributions(),
        "references/connectors.md": connectors(),
        "references/apis.md": apis(),
        "references/cli.md": cli(),
        "references/api.md": api(),
        "assets/blueprint.schema.json": schema_text(),
    }


def main() -> None:
    """Writes the skill folder, removing files it no longer generates."""
    wanted = files()
    for path in sorted(SKILL.rglob("*")) if SKILL.exists() else []:
        if path.is_file() and path.relative_to(SKILL).as_posix() not in wanted:
            path.unlink()
    for name, text in wanted.items():
        target = SKILL / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    print(f"Wrote {len(wanted)} files to {SKILL.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
