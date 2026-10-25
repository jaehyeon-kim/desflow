"""What the feature coverage tests read: the features, from the code, and the text of
the pages, the skill and the README they check.

The features are read from the code, so a new one fails the tests until its docs
exist:

- commands and their options, from the Typer app;
- the public API, from `desflow.__all__`;
- blueprint keys, each with the model that declares it, from the blueprint JSON
  Schema (blueprint_keys.py reads them);
- connectors, from the blueprint's connector registry;
- extras, from `pyproject.toml`;
- distribution families, from `DISTRIBUTIONS`.

A tested example is one of these, on a Learn, How-To, User Guide or Examples page
(test_feature_coverage_api.py checks a type the reader only receives through
`_RECEIVED`):

- a code block labelled with a file under `docs/snippets/` or `examples/`, which
  test_docs_match_examples.py keeps equal to the file and the snippet and example
  tests run; a blueprint key counts only where its block sets it on its model;
- for a command and its options, a command line on the Command Line page, which
  test_docs_command_line.py runs.

The reference of a command, a blueprint key and a distribution family is generated
from the code by scripts/gen_pages.py, so the tests check the generated text. The
skill is checked in the files scripts/gen_skill.py writes.
"""

import importlib.util
import re
import tomllib
from pathlib import Path

import desflow

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
SKILL = ROOT / "skills" / "desflow"
README = (ROOT / "README.md").read_text(encoding="utf-8")
# The Learn, How-To, User Guide and Examples pages
PAGES = sorted(
    page
    for folder in ("des", "tutorials", "guides", "user-guide", "examples")
    for page in (DOCS / folder).rglob("*.md")
)
LABELLED = re.compile(
    r'^( *)```(\w+) title="(?:docs/snippets|examples)/[^"]*"[^\n]*\n(.*?)\n\1```$',
    re.S | re.M,
)


def _labelled(*languages):
    """The code of every labelled block of these languages on the pages."""
    return "\n".join(
        code
        for page in PAGES
        for _, language, code in LABELLED.findall(page.read_text(encoding="utf-8"))
        if language in languages
    )


YAML_CODE = _labelled("yaml")
PYTHON_CODE = _labelled("python")
SKILL_TEXT = "\n".join(
    path.read_text(encoding="utf-8") for path in sorted(SKILL.rglob("*.md"))
)


def gen_pages():
    spec = importlib.util.spec_from_file_location(
        "gen_pages", ROOT / "scripts" / "gen_pages.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _commands():
    """Each command path, such as `desflow examples get`, with its option names."""
    from typer.main import get_command

    from desflow.cli import app

    def walk(command, path):
        names = sorted(
            name
            for parameter in command.params
            # The off form of a flag, such as --no-force, is the same option
            for name in parameter.opts
            if name.startswith("--") and name != "--help"
        )
        yield path, names
        for name, child in getattr(command, "commands", {}).items():
            yield from walk(child, f"{path} {name}")

    return dict(walk(get_command(app), "desflow"))


COMMANDS = _commands()


API = sorted(desflow.__all__)


def connectors():
    from desflow.blueprint.build import EGRESS_TYPES, INGRESS_TYPES

    return [("ingress", name) for name in INGRESS_TYPES] + [
        ("egress", name) for name in EGRESS_TYPES
    ]


def extras():
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return sorted(pyproject["project"]["optional-dependencies"])


def families():
    from desflow.core.distributions import DISTRIBUTIONS

    return sorted(DISTRIBUTIONS)
