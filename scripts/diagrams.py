"""Export the draw.io diagrams under docs/assets/diagrams/ to the images the docs use.

Each `<name>.drawio` becomes one image beside it:

- `<name>.gif` when an edge has draw.io's flow animation (`flowAnimation=1`). draw.io
  writes that animation into an SVG export as a CSS animation of `stroke-dashoffset`,
  which a PNG cannot hold, so this script sets the offset for each frame, renders each
  frame with headless Chrome and joins the frames with ffmpeg into a GIF that loops.
- `<name>.gif` as well when the file has more than one page: each page is one step of a
  sequence, held for `STEP_SECONDS`.
- `<name>.png` otherwise.

Files whose name starts with an underscore, such as `_legend.drawio`, hold the shared
shapes and are not exported.

Each tape in `TAPES`, under docs/assets/tapes/, records a terminal session with vhs
to the GIF it names, from the repository root, when it is named on the command line.
Run it with `uv run`, so the session finds the `desflow` command of this checkout.

The images are committed, so the docs build needs none of the tools this script uses:
the draw.io desktop app (`drawio` on the PATH), Google Chrome (the macOS path, or the
`CHROME` environment variable) and ffmpeg.

Usage: uv run python scripts/diagrams.py [name ...]
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ASSETS = Path(__file__).resolve().parent.parent / "docs" / "assets"
DIAGRAMS = ASSETS / "diagrams"
# Each vhs tape, by name, and the GIF it records; tests/docs/test_docs_diagrams.py
# checks that every tape is listed and every GIF is used on a page
TAPES = {"explain": ASSETS / "charts" / "explain.gif"}
CHROME = os.environ.get(
    "CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
)
SCALE = "1.5"
BORDER = "10"
FLOW_FRAMES = 12
STEP_SECONDS = 2

_DASH_OFFSET = re.compile(r"stroke-dashoffset: ([0-9.]+);")
_ANIMATION = re.compile(r"animation: ([0-9.]+)ms[^;\"]*;?")
_SVG_SIZE = re.compile(r'<svg[^>]*\swidth="([0-9.]+)px"[^>]*\sheight="([0-9.]+)px"')


def _run(args):
    subprocess.run(args, check=True, capture_output=True)


def _require(tool):
    if shutil.which(tool) is None and not Path(tool).exists():
        sys.exit(f"{tool} is needed to export diagrams and was not found")


def _pages(source):
    return source.read_text().count("<diagram ")


def _has_flow(source):
    return "flowAnimation=1" in source.read_text()


def _export(source, fmt, out, page=None):
    args = ["drawio", "-x", "-f", fmt, "-s", SCALE, "-b", BORDER, "-o", str(out)]
    if page is not None:
        args += ["-p", str(page)]
    _run(args + [str(source)])


def _render_svg(svg_text, out, work):
    """Render one SVG frame to PNG with headless Chrome, on a white page of its size."""
    match = _SVG_SIZE.search(svg_text)
    if match is None:
        sys.exit("could not read the size of an exported SVG")
    width, height = (int(float(v)) + 1 for v in match.groups())
    html = work / (out.stem + ".html")
    body = svg_text[svg_text.index("<svg") :]
    html.write_text(
        f"<html><body style='margin:0;background:#fff'>{body}</body></html>"
    )
    _run(
        [
            CHROME,
            "--headless=new",
            "--disable-gpu",
            "--hide-scrollbars",
            f"--window-size={width},{height}",
            f"--screenshot={out}",
            html.as_uri(),
        ]
    )


def _gif(frames_pattern, fps, out):
    _run(
        [
            "ffmpeg",
            "-loglevel",
            "error",
            "-y",
            "-framerate",
            str(fps),
            "-i",
            frames_pattern,
            "-vf",
            "split[a][b];[a]palettegen[p];[b][p]paletteuse",
            "-loop",
            "0",
            str(out),
        ]
    )


def _flow_frames(source, page, work, prefix):
    """Write FLOW_FRAMES PNGs of one page, the dashes moved one step each time.

    draw.io animates `stroke-dashoffset` from one dash period down to 0 and repeats.
    The period and the time it takes are read from the SVG export, so the frames follow
    the app: in draw.io 31.5.3 both depend on the edge style and the export scale. Every
    animated edge in one diagram must share them. The frames and that time are returned.
    """
    svg = work / f"{prefix}.svg"
    _export(source, "svg", svg, page)
    text = svg.read_text()
    # The keyframes end at an offset of 0; each animated edge starts at one period.
    periods = {float(o) for o in _DASH_OFFSET.findall(text)} - {0.0}
    cycles = {float(c) for c in _ANIMATION.findall(text)}
    if len(periods) != 1 or len(cycles) != 1:
        sys.exit(
            f"{source.name}: expected one dash period and one speed for every animated "
            f"edge, found {sorted(periods)} and {sorted(cycles)} ms"
        )
    period, cycle_ms = periods.pop(), cycles.pop()
    text = _ANIMATION.sub("animation: none;", text)
    paths = []
    for i in range(FLOW_FRAMES):
        frame = work / f"{prefix}-{i:03d}.png"
        step = period - period * i / FLOW_FRAMES
        _render_svg(
            _DASH_OFFSET.sub(f"stroke-dashoffset: {step:g};", text), frame, work
        )
        paths.append(frame)
    return paths, cycle_ms


def _export_one(source):
    pages = _pages(source)
    flow = _has_flow(source)
    if pages == 1 and not flow:
        out = source.with_suffix(".png")
        _export(source, "png", out)
        return out
    out = source.with_suffix(".gif")
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        frames, fps = [], FLOW_FRAMES * 2  # a still step's rate, when nothing flows
        for page in range(1, pages + 1):
            if flow:
                cycle, cycle_ms = _flow_frames(source, page, work, f"p{page:02d}")
                fps = round(FLOW_FRAMES * 1000 / cycle_ms)
                # One cycle loops on its own; a step of a sequence is held longer.
                frames += (
                    cycle
                    if pages == 1
                    else cycle * round(STEP_SECONDS * 1000 / cycle_ms)
                )
            else:
                still = work / f"p{page:02d}.png"
                _export(source, "png", still, page)
                frames += [still] * (STEP_SECONDS * fps)
        for i, frame in enumerate(frames):
            shutil.copy(frame, work / f"frame-{i:05d}.png")
        _gif(str(work / "frame-%05d.png"), fps, out)
    return out


def _record(name):
    """Records a tape with vhs from the repository root."""
    _require("vhs")
    tape = ASSETS / "tapes" / f"{name}.tape"
    subprocess.run(
        ["vhs", "-o", str(TAPES[name]), str(tape)], check=True, cwd=ASSETS.parents[1]
    )
    return TAPES[name]


def main(names):
    tapes = [n for n in names if n in TAPES]
    for name in tapes:
        print(f"{name}.tape -> {_record(name).name}")
    names = [n for n in names if n not in TAPES]
    if tapes and not names:
        return
    for tool in ("drawio", "ffmpeg", CHROME):
        _require(tool)
    sources = sorted(p for p in DIAGRAMS.glob("*.drawio") if not p.name.startswith("_"))
    if names:
        sources = [p for p in sources if p.stem in names]
    for source in sources:
        print(f"{source.name} -> {_export_one(source).name}")


if __name__ == "__main__":
    main(sys.argv[1:])
