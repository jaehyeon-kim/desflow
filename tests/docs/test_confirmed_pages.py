"""Pages the owner has confirmed keep their exact text.

Why desflow is the owner's own story, confirmed as written. A docs revision must not
reword it. Change it only when the owner asks, then record the new hash here.
"""

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

CONFIRMED = {
    "docs/about/why-desflow.md": "90e9b74fe202b967f98610bcae8e7131090a6526f16dd341c4328a57317612cf",
}


def test_confirmed_pages_are_unchanged():
    changed = [
        path
        for path, digest in CONFIRMED.items()
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest
    ]
    assert not changed, f"Owner-confirmed pages changed; ask the owner first: {changed}"
