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
from urllib.parse import unquote

import pytest

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


# Each page links to an image of version 1.0 and must still show it after the rewrite
STILL_LINKED = [
    ("1.0/index.html", "img/my chart(1),a.png", "a space, parentheses and a comma"),
    (
        "1.1/index.html",
        "../1.0/img/x.png",
        "a relative link from another older version",
    ),
    ("2.0/index.html", "../1.0/img/x.png", "a relative link from latest"),
    ("1.0/INDEX.HTML", "img/x.png", "a page suffix in capitals"),
]


@pytest.mark.parametrize(
    "page, link, name", STILL_LINKED, ids=[case[2] for case in STILL_LINKED]
)
def test_a_link_still_reaches_its_image_after_the_rewrite(tmp_path, page, link, name):
    versions = [("2.0", ["latest"]), ("1.1", []), ("1.0", [])]
    (tmp_path / "versions.json").write_text(
        json.dumps([{"version": v, "title": v, "aliases": a} for v, a in versions])
    )
    image = tmp_path / "1.0" / "img" / unquote(link.split("/")[-1])
    image.parent.mkdir(parents=True)
    image.write_bytes(b"the image")
    (tmp_path / page).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / page).write_text(f'<img src="{link}">')

    pages_images.rewrite_site(tmp_path)

    written = (tmp_path / page).read_text().split('src="')[1].split('"')[0]
    assert (tmp_path / page).parent.joinpath(unquote(written)).resolve().exists()


def test_a_text_run_that_names_two_images_keeps_both(tmp_path):
    versions = [("2.0", ["latest"]), ("1.0", [])]
    (tmp_path / "versions.json").write_text(
        json.dumps([{"version": v, "title": v, "aliases": a} for v, a in versions])
    )
    for name in ("a.png", "b.png", "c.png"):
        (tmp_path / "1.0" / "img").mkdir(parents=True, exist_ok=True)
        (tmp_path / "1.0" / "img" / name).write_bytes(name.encode())
    (tmp_path / "1.0" / "index.html").write_text("<code>a.png b.png</code>")

    pages_images.rewrite_site(tmp_path)

    kept = {path.name for path in (tmp_path / "1.0" / "img").iterdir()}
    assert kept == {"a.png", "b.png"}


def test_an_image_a_template_literal_names_is_kept(tmp_path):
    """A backtick, and the brace that ends a `${...}` in front of a name, separate the
    name from the text before it, so the check before deleting finds both images."""
    versions = [("2.0", ["latest"]), ("1.0", [])]
    (tmp_path / "versions.json").write_text(
        json.dumps([{"version": v, "title": v, "aliases": a} for v, a in versions])
    )
    (tmp_path / "1.0" / "img").mkdir(parents=True)
    for name in ("plot.png", "chart.png"):
        (tmp_path / "1.0" / "img" / name).write_bytes(name.encode())
    (tmp_path / "1.0" / "app.js").write_text(
        "a.src = `plot.png`; b.src = `${base}chart.png`;"
    )

    pages_images.rewrite_site(tmp_path)

    kept = {path.name for path in (tmp_path / "1.0" / "img").iterdir()}
    assert kept == {"plot.png", "chart.png"}
