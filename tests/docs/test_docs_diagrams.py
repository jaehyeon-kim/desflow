"""Every diagram source has its exported image, and every exported image is used.

The sources are draw.io files under docs/assets/diagrams/, exported by
scripts/diagrams.py to a PNG or a GIF beside them. Files whose name starts with an
underscore hold the shared shapes and are not exported.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DIAGRAMS = ROOT / "docs" / "assets" / "diagrams"
IMAGE_SUFFIXES = (".png", ".gif")
# The docs content column is about 700 pixels wide. An image wider than this is shrunk
# so far that its 14-point labels become too small to read: draw it narrower, usually
# top to bottom.
MAX_WIDTH = 1200


def _sources():
    return sorted(p for p in DIAGRAMS.glob("*.drawio") if not p.name.startswith("_"))


def _images():
    return sorted(p for p in DIAGRAMS.iterdir() if p.suffix in IMAGE_SUFFIXES)


def test_every_diagram_has_its_image():
    missing = [
        p.name
        for p in _sources()
        if not any(p.with_suffix(s).exists() for s in IMAGE_SUFFIXES)
    ]
    assert not missing, f"Run scripts/diagrams.py to export: {missing}"


def test_every_image_has_a_source():
    orphans = [p.name for p in _images() if not p.with_suffix(".drawio").exists()]
    assert not orphans, f"These images have no draw.io source: {orphans}"


def test_every_image_is_used_on_a_page():
    pages = "\n".join(p.read_text() for p in (ROOT / "docs").rglob("*.md"))
    unused = [p.name for p in _images() if f"diagrams/{p.name}" not in pages]
    assert not unused, f"No page uses these diagrams: {unused}"


def test_every_image_is_narrow_enough_to_read():
    import struct

    def width(path):
        data = path.read_bytes()[:30]
        if data[:8] == b"\x89PNG\r\n\x1a\n":
            return struct.unpack(">I", data[16:20])[0]
        return struct.unpack("<H", data[6:8])[0]  # GIF logical screen width

    wide = [f"{p.name} ({width(p)} px)" for p in _images() if width(p) > MAX_WIDTH]
    assert not wide, f"Draw these narrower, at most {MAX_WIDTH} px: {wide}"


CHARTS = ROOT / "docs" / "assets" / "charts"


def test_every_chart_is_drawn_by_the_script_and_used_on_a_page():
    """Charts are PNGs, and one Plotly HTML page, that scripts/charts.py draws."""
    import ast

    tree = ast.parse((ROOT / "scripts" / "charts.py").read_text(encoding="utf-8"))
    names = {
        key.value
        for node in ast.walk(tree)
        if isinstance(node, ast.AnnAssign)
        and getattr(node.target, "id", "") == "CHARTS"
        for key in node.value.keys  # type: ignore[union-attr]
    }
    found = [*CHARTS.glob("*.png"), *CHARTS.glob("*.html")]
    stems = [p.stem for p in found]
    assert len(stems) == len(set(stems)), "A chart name has two files"
    files = {p.stem: p.name for p in found}
    images = set(files)
    assert names and images == names, (
        "Run scripts/charts.py, and delete charts it no longer draws"
    )
    pages = "\n".join(p.read_text() for p in (ROOT / "docs").rglob("*.md"))
    unused = [name for name in sorted(images) if f"charts/{files[name]}" not in pages]
    assert not unused, f"No page uses these charts: {unused}"


def test_every_tape_is_recorded_by_the_script_and_its_gif_used_on_a_page():
    """vhs tapes under docs/assets/tapes/ that scripts/diagrams.py records."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "diagrams", ROOT / "scripts" / "diagrams.py"
    )
    assert spec is not None and spec.loader is not None
    diagrams = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(diagrams)
    tapes = {p.stem for p in (ROOT / "docs" / "assets" / "tapes").glob("*.tape")}
    assert tapes == set(diagrams.TAPES), (
        "List every tape in TAPES in scripts/diagrams.py"
    )
    pages = "\n".join(p.read_text() for p in (ROOT / "docs").rglob("*.md"))
    missing = [str(gif) for gif in diagrams.TAPES.values() if not gif.exists()]
    unused = [
        gif.name for gif in diagrams.TAPES.values() if f"charts/{gif.name}" not in pages
    ]
    assert not missing, f"Record these with scripts/diagrams.py: {missing}"
    assert not unused, f"No page uses these GIFs: {unused}"
