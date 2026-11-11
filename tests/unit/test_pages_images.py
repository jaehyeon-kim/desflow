"""scripts/pages_images.py stores the images of the older docs versions once.

An image that two older versions hold is stored once in shared-images/ and their pages
link to that copy, whatever the case of its extension and whether its link is
percent-encoded. The latest version keeps its images. An image stays in place when any
page of the site links to it by a root path, or when a file of its version still names
it after the rewrite.
"""

import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "pages_images.py"
_spec = importlib.util.spec_from_file_location("pages_images", SCRIPT)
assert _spec is not None and _spec.loader is not None
pages_images = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pages_images)

PAGE = (
    '<img src="../assets/chart.png"><img src="../assets/My%20Plot.PNG">'
    '<link href="/desflow/{v}/assets/pinned.png">'
)
IMAGES = {
    "chart.png": b"same chart",
    "My Plot.PNG": b"same plot",
    "pinned.png": b"pinned",
    "linked-from-latest.png": b"linked from latest",
    "built-by-script.png": b"built by script",
}


def _site(tmp_path):
    versions = [("2.0", ["latest"]), ("1.1", []), ("1.0", [])]
    (tmp_path / "versions.json").write_text(
        json.dumps([{"version": v, "title": v, "aliases": a} for v, a in versions])
    )
    for version, _ in versions:
        (tmp_path / version / "guide").mkdir(parents=True)
        (tmp_path / version / "guide" / "index.html").write_text(PAGE.format(v=version))
        (tmp_path / version / "assets").mkdir()
        for name, content in IMAGES.items():
            (tmp_path / version / "assets" / name).write_bytes(content)
    # The latest version links into 1.0 by a root path, and 1.1 builds a link in a
    # script, which the rewrite cannot read
    (tmp_path / "2.0" / "other.html").write_text(
        '<img src="/desflow/1.0/assets/linked-from-latest.png">'
    )
    (tmp_path / "1.1" / "assets" / "app.js").write_text(
        'img.src = base + "built-by-script.png";'
    )
    return tmp_path


def test_older_versions_share_one_copy_and_latest_keeps_its_own(tmp_path):
    site = _site(tmp_path)

    kept = pages_images.rewrite_site(site)

    shared = {p.read_bytes(): p.name for p in (site / "shared-images").iterdir()}
    for version in ("1.1", "1.0"):
        page = (site / version / "guide" / "index.html").read_text()
        assert f'src="../../shared-images/{shared[b"same chart"]}"' in page
        assert f'src="../../shared-images/{shared[b"same plot"]}"' in page
        assert not (site / version / "assets" / "chart.png").exists()
        assert not (site / version / "assets" / "My Plot.PNG").exists()
        assert (site / version / "assets" / "pinned.png").exists()
    assert (site / "2.0" / "guide" / "index.html").read_text() == PAGE.format(v="2.0")
    assert all((site / "2.0" / "assets" / name).exists() for name in IMAGES)
    # Pinned from outside its version, and named by a script of its version
    assert (site / "1.0" / "assets" / "linked-from-latest.png").exists()
    assert not (site / "1.1" / "assets" / "linked-from-latest.png").exists()
    assert kept == [site / "1.1" / "assets" / "built-by-script.png"]
    assert (site / "1.1" / "assets" / "built-by-script.png").exists()
