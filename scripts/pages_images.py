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

An image stays in place when any page of the site links to it by an absolute or root
path, or when a page of its version still names it after the rewrite, such as a link
the rewrite could not read. Links are matched without regard to case, and a
percent-encoded link is decoded before it is resolved.
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
from urllib.parse import unquote

SHARED = "shared-images"
IMAGE_SUFFIXES = (".png", ".gif", ".jpg", ".jpeg", ".webp")
# Pages, the Markdown copies of pages, and llms-full.txt link to images
PAGE_SUFFIXES = (".html", ".md", ".txt", ".css")
# Files searched for an image's name before the image is deleted
TEXT_SUFFIXES = (*PAGE_SUFFIXES, ".json", ".js", ".xml", ".map")
# Measured on the 1.0.0rc2 charts and diagrams: 20.7 MB to 6.8 MB
PNGQUANT_OPTIONS = ("--quality", "70-95", "--skip-if-larger")
# pngquant exits with 98 when the result would be larger and 99 when the quality
# would fall below 70; the file is then left as it was
PNGQUANT_KEPT = (0, 98, 99)
# A link in an attribute, a srcset, a CSS url() or a Markdown image
_LINK = re.compile(
    r"(?<=[\"'(\s=,])([^\"'()\s<>,]{1,300}\.(?:png|gif|jpe?g|webp))(?=[?#\"')\s,])",
    re.IGNORECASE,
)
# Any word that ends like an image file name, for the check before deleting
_NAME = re.compile(r"[^\s\"'()<>,/\\]{1,300}\.(?:png|gif|jpe?g|webp)\b", re.IGNORECASE)


def _is_relative(link):
    return "://" not in link and not link.startswith(("/", "data:"))


def _resolve(page, link):
    return Path(os.path.normpath(page.parent / unquote(link)))


def _versions_to_rewrite(site):
    versions = json.loads((site / "versions.json").read_text())
    return [v["version"] for v in versions if "latest" not in v["aliases"]]


def _files(folder, suffixes):
    return [p for p in folder.rglob("*") if p.suffix in suffixes and p.is_file()]


def _images(folder):
    return {
        p
        for p in folder.rglob("*")
        if p.suffix.lower() in IMAGE_SUFFIXES and p.is_file() and not p.is_symlink()
    }


def _absolute_links(site):
    """Returns every link to an image by an absolute or root path, on any page."""
    return {
        unquote(link)
        for page in _files(site, PAGE_SUFFIXES)
        for link in _LINK.findall(page.read_text(errors="ignore"))
        if not _is_relative(link)
    }


def _pinned(site, version, absolute_links):
    """Returns the images of a version that some page links to by an absolute path."""
    marker = f"/{version}/"
    return {
        Path(os.path.normpath(site / version / link.split(marker, 1)[1]))
        for link in absolute_links
        if marker in link
    }


def _names_left(folder):
    """Returns the image file names the version's files still mention, in lower case."""
    return {
        unquote(Path(name).name).lower()
        for page in _files(folder, TEXT_SUFFIXES)
        for name in _NAME.findall(page.read_text(errors="ignore"))
    }


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


def rewrite_version(site, version, pinned, compress=False):
    """Moves one version's images to the shared folder and rewrites its links.

    Args:
        site: The root of the gh-pages worktree.
        version: The version folder to rewrite.
        pinned: The images that stay in place because a page links to them by an
            absolute or root path.
        compress: Whether to shrink each PNG it stores with pngquant.

    Returns:
        The images kept in place because a file of the version still names them.
    """
    folder = site / version
    images = _images(folder) - pinned
    moved = {image: _store(site, image, compress) for image in sorted(images)}
    for page in _files(folder, PAGE_SUFFIXES):
        _rewrite_page(page, moved)
    named = _names_left(folder)
    kept = sorted(image for image in moved if image.name.lower() in named)
    for image in moved.keys() - set(kept):
        image.unlink()
    return kept


def rewrite_site(site, compress=False):
    """Rewrites every version except the one aliased `latest`.

    Args:
        site: The root of the gh-pages worktree.
        compress: Whether to shrink each PNG it stores with pngquant.

    Returns:
        The images kept in place because a file of their version still names them.
    """
    absolute_links = _absolute_links(site)
    kept = []
    for version in _versions_to_rewrite(site):
        pinned = _pinned(site, version, absolute_links)
        kept += rewrite_version(site, version, pinned, compress)
    return kept


def _main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("site", type=Path, help="the gh-pages worktree")
    parser.add_argument(
        "--compress", action="store_true", help="shrink each stored PNG with pngquant"
    )
    args = parser.parse_args()
    for image in rewrite_site(args.site, args.compress):
        print(f"kept, still named by its version: {image}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
