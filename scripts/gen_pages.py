"""Generate the Reference pages that the code already knows.

mkdocs-gen-files runs this file during every docs build (the `gen-files` plugin in
mkdocs.yml), so the pages are built from the code and cannot disagree with it. The
functions return Markdown, so tests/docs/test_gen_pages.py checks them without a
build. They are in `scripts/reference_pages/`, one module for each page or group of
pages:

- Distributions: every family in `desflow.core.distributions.DISTRIBUTIONS`, with its
  parameters, rules and registry paths, and the mean, variance and value with no
  random generator of one example, computed by the family's own functions.
- Statistics: every name each level publishes, found by running the statistics on a
  model with one resource, store, container, task and flow.
- YAML Keys: every key a blueprint takes, from the JSON Schema that ships with desflow
  (`desflow schema`), which is also published as `schema/blueprint.schema.json`.
- SimPy Compatibility: every operation in `tests/conformance/register.yaml`, with its
  form in each API, the conformance tests that compare it with SimPy, and its page.
- CLI: one page per command and sub-command of `desflow`, each its help as Click
  prints it, and an index page for `desflow` itself.
"""

import sys
from pathlib import Path

# The page modules are a package beside this file; mkdocs-gen-files and the tests run
# this file by its path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from reference_pages.cli import cli_pages, commands  # noqa: E402
from reference_pages.compatibility import (  # noqa: E402
    GROUPS,
    compatibility_page,
    register,
    simpy_version,
)
from reference_pages.distributions import (  # noqa: E402
    EXAMPLES,
    distribution_sections,
    distributions_page,
)
from reference_pages.statistics import statistic_names, statistics_page  # noqa: E402
from reference_pages.yaml_keys import (  # noqa: E402
    paragraphs,
    yaml_keys_body,
    yaml_keys_page,
)

# What the tests and scripts/gen_skill.py read from this file
__all__ = [
    "EXAMPLES",
    "GROUPS",
    "cli_pages",
    "commands",
    "compatibility_page",
    "distribution_sections",
    "distributions_page",
    "paragraphs",
    "register",
    "simpy_version",
    "statistic_names",
    "statistics_page",
    "write",
    "yaml_keys_body",
    "yaml_keys_page",
]


def write() -> None:
    """Writes the generated pages into the docs build."""
    import mkdocs_gen_files

    from desflow.blueprint.schema import schema_text

    for path, page in (
        ("reference/distributions.md", distributions_page()),
        ("reference/statistics.md", statistics_page()),
        ("reference/yaml-keys.md", yaml_keys_page()),
        ("reference/simpy-compatibility.md", compatibility_page()),
        *cli_pages().items(),
    ):
        with mkdocs_gen_files.open(path, "w") as file:
            file.write(page)
        mkdocs_gen_files.set_edit_path(path, "scripts/gen_pages.py")
    # The schema file as the package holds it, so the URL a modeline names serves the
    # same text as `desflow schema`
    with mkdocs_gen_files.open("schema/blueprint.schema.json", "w") as file:
        file.write(schema_text())


# mkdocs-gen-files runs this file with runpy, under this name
if __name__ == "<run_path>":
    write()
