#!/usr/bin/env bash
# Builds this version of the docs and pushes it to gh-pages as `latest`. Run by the
# docs deploy of pipeline.yml and of docs.yml, from a checkout with the full history,
# because mike pulls the gh-pages branch.
set -euo pipefail

# Configure Git for the bot commit
git config --global user.name "jaehyeon-kim"
git config --global user.email "1023748+jaehyeon-kim@users.noreply.github.com"

# The version string from pyproject.toml
VERSION=$(grep -m 1 '^version = ' pyproject.toml | cut -d '"' -f 2)
echo "Extracted version: $VERSION"

# Deploy the version and tag it as 'latest'
uv run mike deploy --update-aliases "$VERSION" latest

# Set 'latest' as the default landing page when navigating to the root URL
uv run mike set-default latest

# GitHub Pages answers every missing path under the site with the 404.html
# at the root of gh-pages, but mike writes one into each version folder
# only. Copy this version's page to the root. Its links point at latest/,
# so it renders at any depth.
git worktree add "$RUNNER_TEMP/gh-pages" gh-pages
cp "$RUNNER_TEMP/gh-pages/$VERSION/404.html" "$RUNNER_TEMP/gh-pages/404.html"
# Assistants look for llms.txt at the root of the site. Its links already
# point at latest/, so the copy works from there.
cp "$RUNNER_TEMP/gh-pages/$VERSION/llms.txt" "$RUNNER_TEMP/gh-pages/llms.txt"
cp "$RUNNER_TEMP/gh-pages/$VERSION/llms-full.txt" "$RUNNER_TEMP/gh-pages/llms-full.txt"
# Each version folder holds its own copy of every image. Store the images of
# the versions other than latest once, in a folder they share, so the site
# stays under the 1 GB that Pages publishes, and shrink their PNGs.
sudo apt-get update -q && sudo apt-get install -y --no-install-recommends pngquant
uv run python scripts/pages_images.py "$RUNNER_TEMP/gh-pages" --compress
git -C "$RUNNER_TEMP/gh-pages" add -A
git -C "$RUNNER_TEMP/gh-pages" diff --cached --quiet || git -C "$RUNNER_TEMP/gh-pages" commit -m "Root 404 page and llms.txt from $VERSION, and shared images of the older versions"
# mike commits to the local gh-pages branch (after updating it from the remote),
# so everything above goes out in one push and starts one Pages build.
git -C "$RUNNER_TEMP/gh-pages" push origin gh-pages
