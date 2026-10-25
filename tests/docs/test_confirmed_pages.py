"""Pages the owner has confirmed keep their exact text.

Why desflow is the owner's own story, confirmed as written. A docs revision must not
reword it. Change it only when the owner asks, then record the new hash here.
"""

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

CONFIRMED = {
    "docs/about/why-desflow.md": "8cd67552fef2b541896c8ce313dbe99f26860e96773b88803b9966f4ee22ddee",
}


def test_confirmed_pages_are_unchanged():
    changed = [
        path
        for path, digest in CONFIRMED.items()
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest
    ]
    assert not changed, f"Owner-confirmed pages changed; ask the owner first: {changed}"
