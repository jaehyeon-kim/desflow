"""Pages the owner has confirmed keep their exact text.

Why desflow is the owner's own story, confirmed as written. A docs revision must not
reword it. Change it only when the owner asks, then record the new hash here.
"""

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

CONFIRMED = {
    "docs/about/why-desflow.md": "17fd1789327e092a0443933f0b567c132d812a605bab52121d942826d5cd99e7",
}


def test_confirmed_pages_are_unchanged():
    changed = [
        path
        for path, digest in CONFIRMED.items()
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest
    ]
    assert not changed, f"Owner-confirmed pages changed; ask the owner first: {changed}"
