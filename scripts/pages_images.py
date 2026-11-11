"""Stores each image of the older docs versions once, in a folder they share.

mike gives every version of the docs its own folder, so an image that did not change
between versions is published once per version. GitHub Pages packs the site with
`tar --dereference`, so a symlink would be published as a full copy too. This script
moves each image of every version except the one aliased `latest` to
`shared-images/<hash>.<ext>` at the root of the site, named by a hash of its content,
and rewrites the links to it in that version's pages. An unchanged image keeps its
hash, so all versions link to one file; a changed image gets a new hash, so each
version still shows its own.

The `latest` version keeps its images in place, because pages outside it (the 404 page
at the root) link to `/desflow/latest/...`. It is rewritten when the next version
replaces it as `latest`.

    python scripts/pages_images.py <gh-pages worktree> [--compress]

With `--compress`, each PNG it stores is shrunk with pngquant (`--quality 70-95`),
which must be installed. The `latest` version keeps its images as they are.

An image that a page links to by an absolute or root path stays in place. The script
exits with 1 when a link in a rewritten version points at an image it moved.
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

SHARED = "shared-images"
IMAGE_SUFFIXES = (".png", ".gif", ".jpg", ".jpeg", ".webp")
# Pages, the Markdown copies of pages, and llms-full.txt link to images
PAGE_SUFFIXES = (".html", ".md", ".txt", ".css")
# Measured on the 1.0.0rc2 charts and diagrams: 20.7 MB to 6.8 MB
PNGQUANT_OPTIONS = ("--quality", "70-95", "--skip-if-larger")
# pngquant exits with 98 when the result would be larger and 99 when the quality
# would fall below 70; the file is then left as it was
PNGQUANT_KEPT = (0, 98, 99)
# A link in an attribute, a srcset, a CSS url() or a Markdown image
_LINK = re.compile(
    r"(?<=[\"'(\s=,])([^\"'()\s<>,]{1,300}\.(?:png|gif|jpe?g|webp))(?=[?#\"')\s,])"
)


def _is_relative(link):
    return "://" not in link and not link.startswith(("/", "data:"))


def _resolve(page, link):
    return Path(os.path.normpath(page.parent / link))


def _versions_to_rewrite(site):
    versions = json.loads((site / "versions.json").read_text())
    return [v["version"] for v in versions if "latest" not in v["aliases"]]


def _pages(folder):
    return [p for p in folder.rglob("*") if p.suffix in PAGE_SUFFIXES and p.is_file()]


def _images(folder):
    return {
        p
        for p in folder.rglob("*")
        if p.suffix.lower() in IMAGE_SUFFIXES and p.is_file() and not p.is_symlink()
    }


def _pinned(folder, version, pages):
    """Returns the images some page links to by an absolute or root path."""
    pinned = set()
    marker = f"/{version}/"
    for page in pages:
        for link in _LINK.findall(page.read_text(errors="ignore")):
            if not _is_relative(link) and marker in link:
                pinned.add(folder / link.split(marker, 1)[1])
    return pinned


def _compress(png):
    """Shrinks a PNG in place with pngquant, keeping it when pngquant cannot shrink it."""
    result = subprocess.run(
        ["pngquant", *PNGQUANT_OPTIONS, "--force", "--output", str(png), str(png)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode not in PNGQUANT_KEPT:
        raise RuntimeError(f"pngquant failed on {png}: {result.stderr.strip()}")


def _store(site, image, compress):
    """Copies an image into the shared folder under its hash and returns the copy.

    The hash is of the image as the version holds it, so the same image from two
    versions gets one copy whether or not that copy was compressed.
    """
    digest = hashlib.sha256(image.read_bytes()).hexdigest()[:16]
    shared = site / SHARED / f"{digest}{image.suffix.lower()}"
    if not shared.exists():
        shared.parent.mkdir(exist_ok=True)
        shutil.copyfile(image, shared)
        if compress and shared.suffix == ".png":
            _compress(shared)
    return shared


def _rewrite_page(page, moved):
    text = page.read_text(errors="ignore")

    def _swap(match):
        link = match.group(1)
        target = _resolve(page, link) if _is_relative(link) else None
        if target not in moved:
            return link
        return os.path.relpath(moved[target], page.parent)

    new = _LINK.sub(_swap, text)
    if new != text:
        page.write_text(new)


def _dangling(pages, moved):
    """Returns the links that still point at a moved image."""
    found = []
    for page in pages:
        for link in _LINK.findall(page.read_text(errors="ignore")):
            if _is_relative(link) and _resolve(page, link) in moved:
                found.append(f"{page}: {link}")
    return found


def rewrite_version(site, version, compress=False):
    """Moves one version's images to the shared folder and rewrites its links.

    Args:
        site: The root of the gh-pages worktree.
        version: The version folder to rewrite.
        compress: Whether to shrink each PNG it stores with pngquant.

    Returns:
        The links that still point at a moved image, empty when the rewrite is complete.
    """
    folder = site / version
    pages = _pages(folder)
    images = _images(folder) - _pinned(folder, version, pages)
    moved = {image: _store(site, image, compress) for image in sorted(images)}
    for page in pages:
        _rewrite_page(page, moved)
    dangling = _dangling(pages, moved)
    if not dangling:
        for image in moved:
            image.unlink()
    return dangling


def rewrite_site(site, compress=False):
    """Rewrites every version except the one aliased `latest`.

    Args:
        site: The root of the gh-pages worktree.
        compress: Whether to shrink each PNG it stores with pngquant.

    Returns:
        The links that still point at a moved image, empty when the rewrite is complete.
    """
    dangling = []
    for version in _versions_to_rewrite(site):
        dangling += rewrite_version(site, version, compress)
    return dangling


def _main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("site", type=Path, help="the gh-pages worktree")
    parser.add_argument(
        "--compress", action="store_true", help="shrink each stored PNG with pngquant"
    )
    args = parser.parse_args()
    dangling = rewrite_site(args.site, args.compress)
    for line in dangling:
        print(f"link to a moved image: {line}", file=sys.stderr)
    return 1 if dangling else 0


if __name__ == "__main__":
    sys.exit(_main())
