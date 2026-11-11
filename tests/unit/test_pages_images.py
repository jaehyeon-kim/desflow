"""scripts/pages_images.py stores the images of the older docs versions once.

An image that two older versions hold is stored once in shared-images/ and their pages
link to that copy. The latest version keeps its images, and an image that a page links
to by a root path stays in place.
"""

import importlib.util
import json
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "pages_images.py"
_spec = importlib.util.spec_from_file_location("pages_images", SCRIPT)
assert _spec is not None and _spec.loader is not None
pages_images = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pages_images)

PAGE = '<img src="../assets/chart.png"><link href="/desflow/{v}/assets/pinned.png">'


def _site(tmp_path):
    versions = [("2.0", ["latest"]), ("1.1", []), ("1.0", [])]
    (tmp_path / "versions.json").write_text(
        json.dumps([{"version": v, "title": v, "aliases": a} for v, a in versions])
    )
    for version, _ in versions:
        (tmp_path / version / "guide").mkdir(parents=True)
        (tmp_path / version / "guide" / "index.html").write_text(PAGE.format(v=version))
        (tmp_path / version / "assets").mkdir()
        (tmp_path / version / "assets" / "chart.png").write_bytes(b"same chart")
        (tmp_path / version / "assets" / "pinned.png").write_bytes(b"pinned")
    return tmp_path


def test_older_versions_share_one_copy_and_latest_keeps_its_own(tmp_path):
    site = _site(tmp_path)

    assert pages_images.rewrite_site(site) == []

    shared = list((site / "shared-images").iterdir())
    assert [p.read_bytes() for p in shared] == [b"same chart"]
    for version in ("1.1", "1.0"):
        page = (site / version / "guide" / "index.html").read_text()
        assert f'src="../../shared-images/{shared[0].name}"' in page
        assert not (site / version / "assets" / "chart.png").exists()
        assert (site / version / "assets" / "pinned.png").exists()
    assert (site / "2.0" / "guide" / "index.html").read_text() == PAGE.format(v="2.0")
    assert (site / "2.0" / "assets" / "chart.png").exists()
