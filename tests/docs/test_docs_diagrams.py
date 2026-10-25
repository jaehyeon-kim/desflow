"""Every diagram source has its exported image, and every exported image is used.

The sources are draw.io files under docs/assets/diagrams/, exported by
scripts/diagrams.py to a PNG or a GIF beside them. Files whose name starts with an
underscore hold the shared shapes and are not exported.

Every GIF under docs/ has a still beside it: a PNG of the same name, its last frame,
for a reader or a format that does not play a GIF. No page needs to use a still, so
the checks that every image is used on a page leave the stills out.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DIAGRAMS = ROOT / "docs" / "assets" / "diagrams"
IMAGE_SUFFIXES = (".png", ".gif")
# The docs content column is about 700 pixels wide. An image wider than this is shrunk
# so far that its 14-point labels become too small to read: draw it narrower, usually
# top to bottom.
MAX_WIDTH = 1200
# A GIF stays under a few hundred KB, so a page with one still loads quickly
MAX_GIF_BYTES = 300_000


def _sources():
    return sorted(p for p in DIAGRAMS.glob("*.drawio") if not p.name.startswith("_"))


def _images():
    return sorted(p for p in DIAGRAMS.iterdir() if p.suffix in IMAGE_SUFFIXES)


def _is_still(path):
    """Whether `path` is the still PNG of a GIF beside it."""
    return path.suffix == ".png" and path.with_suffix(".gif").exists()


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
    unused = [
        p.name
        for p in _images()
        if not _is_still(p) and f"diagrams/{p.name}" not in pages
    ]
    assert not unused, f"No page uses these diagrams: {unused}"


def test_every_gif_has_its_still_and_is_small():
    gifs = sorted((ROOT / "docs").rglob("*.gif"))
    no_still = [str(p) for p in gifs if not p.with_suffix(".png").exists()]
    large = [
        f"{p} ({p.stat().st_size} bytes)"
        for p in gifs
        if p.stat().st_size > MAX_GIF_BYTES
    ]
    assert gifs
    assert not no_still, f"Run scripts/diagrams.py to write their stills: {no_still}"
    assert not large, f"Make these smaller than {MAX_GIF_BYTES} bytes: {large}"


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
    found = [
        p for p in [*CHARTS.glob("*.png"), *CHARTS.glob("*.html")] if not _is_still(p)
    ]
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


def test_every_tape_is_recorded_by_the_script_and_its_gif_used_on_command_line():
    """vhs tapes under docs/assets/tapes/ that scripts/diagrams.py records.

    A recording of a command shows the YAML way only, so it goes on Command Line,
    the page about the command, and on no page that teaches all three ways."""
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
    command_line = ROOT / "docs" / "user-guide" / "command-line.md"
    missing = [str(gif) for gif in diagrams.TAPES.values() if not gif.exists()]
    unused = [
        gif.name
        for gif in diagrams.TAPES.values()
        if f"charts/{gif.name}" not in command_line.read_text()
    ]
    elsewhere = [
        f"{page.relative_to(ROOT)}: {gif.name}"
        for page in (ROOT / "docs").rglob("*.md")
        if page != command_line
        for gif in diagrams.TAPES.values()
        if f"charts/{gif.name}" in page.read_text()
    ]
    assert not missing, f"Record these with scripts/diagrams.py: {missing}"
    assert not unused, f"Command Line does not use these GIFs: {unused}"
    assert not elsewhere, (
        f"Recordings of a command go on Command Line only: {elsewhere}"
    )
