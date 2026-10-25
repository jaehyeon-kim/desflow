"""The desflow skill in `skills/desflow/` follows the code and the Agent Skills
specification (https://agentskills.io/specification).

`scripts/gen_skill.py` writes the folder. These tests fail when the committed folder
differs from its output, when it leaves out a section, a distribution or a connector,
when its frontmatter breaks the specification, or when it stops teaching a study.
"""

import importlib.util
import re
from pathlib import Path

import yaml

from desflow.blueprint.build import EGRESS_TYPES, INGRESS_TYPES
from desflow.core.distributions import DISTRIBUTIONS

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "gen_skill", _ROOT / "scripts" / "gen_skill.py"
)
assert _spec is not None and _spec.loader is not None
gen_skill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gen_skill)

SKILL = _ROOT / "skills" / "desflow"


def _frontmatter(text: str) -> dict:
    match = re.match(r"---\n(.*?)\n---\n", text, re.S)
    assert match, "SKILL.md must start with YAML frontmatter"
    return yaml.safe_load(match.group(1))


def test_committed_skill_is_current():
    for name, text in gen_skill.files().items():
        assert (SKILL / name).read_text(encoding="utf-8") == text, (
            f"skills/desflow/{name} is out of date; run "
            "`uv run --all-extras python scripts/gen_skill.py`"
        )
    committed = {
        p.relative_to(SKILL).as_posix() for p in SKILL.rglob("*") if p.is_file()
    }
    assert committed == set(gen_skill.files())


def test_skill_names_every_section_distribution_and_connector():
    skill = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    for key in gen_skill.SECTIONS:
        assert f"| `{key}` |" in skill
    distributions = (SKILL / "references" / "distributions.md").read_text(
        encoding="utf-8"
    )
    for name in DISTRIBUTIONS:
        assert f"\n## {name}\n" in distributions
    connectors = (SKILL / "references" / "connectors.md").read_text(encoding="utf-8")
    for name in {*INGRESS_TYPES, *EGRESS_TYPES}:
        assert f"| `{name}` |" in connectors


def test_skill_names_every_avro_serializer():
    """The skill's Avro table is hand-kept, so a new serializer class fails here."""
    import desflow.connectors.egress.kafka as kafka

    text = (SKILL / "references" / "connectors.md").read_text(encoding="utf-8")
    for name in dir(kafka):
        if name.endswith("AvroSerializer"):
            assert f"`desflow.connectors.egress.kafka.{name}`" in text, name


def test_skill_frontmatter():
    text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    meta = _frontmatter(text)
    name = meta["name"]
    assert name == SKILL.name
    assert 1 <= len(name) <= 64
    assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", name)
    assert 1 <= len(meta["description"]) <= 1024
    assert meta["license"] == "MIT"
    assert 1 <= len(meta["compatibility"]) <= 500
    assert meta["metadata"] == {"desflow-version": gen_skill.version()}
    assert gen_skill.version() in meta["compatibility"]
    assert len(text.splitlines()) < 500


def test_skill_teaches_a_study():
    skill = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    assert "## Run a study, not a single run" in skill
    study = skill.split("## Run a study, not a single run")[1].split("\n## ")[0].lower()
    for word in ("seed", "warm", "replications", "confidence interval"):
        assert word in study


def test_skill_reference_links_exist():
    for page in SKILL.rglob("*.md"):
        for target in re.findall(
            r"\]\(([^)#\s]+)(?:#[^)]*)?\)", page.read_text(encoding="utf-8")
        ):
            if "://" in target:
                continue
            assert (page.parent / target).resolve().exists(), f"{page}: {target}"
