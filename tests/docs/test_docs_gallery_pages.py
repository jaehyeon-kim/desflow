"""The Data Generation pages of the Gallery show every file of `examples/gallery/`.

Each entry is on one page with its three files, and an entry that needs no service
shows the first events of `desflow run FILE --sample 5` in a record block that
`test_docs_record_blocks.py` compares with a run. The three forms of an entry give the
same records: `test_yaml_examples.py` checks that.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGES = sorted((ROOT / "docs" / "gallery" / "data-generation").glob("*.md"))
SLUGS = sorted(p.stem for p in (ROOT / "examples" / "gallery").glob("*.yaml"))
# Entries that need a broker or a database, so they show no records
NO_RECORDS = {
    "retail_records_to_three_sinks",
    "customer_deletes_become_kafka_tombstones",
}


def _text():
    return {p.name: p.read_text(encoding="utf-8") for p in PAGES}


def test_every_entry_is_on_exactly_one_page_with_its_three_files():
    assert len(SLUGS) == 21
    pages = _text()
    for slug in SLUGS:
        holders = [
            n for n, t in pages.items() if f'title="examples/gallery/{slug}.yaml"' in t
        ]
        assert len(holders) == 1, (slug, holders)
        text = pages[holders[0]]
        for form in ("builder", "low_level"):
            assert f'title="examples/gallery/{slug}_{form}.py"' in text, (slug, form)


def test_an_entry_shows_records_unless_it_needs_a_service():
    pages = "\n".join(_text().values())
    shown = set(re.findall(r"<!-- records: gallery-(\w+) -->", pages))
    assert shown == set(SLUGS) - NO_RECORDS


def test_the_pages_keep_to_the_heading_limit():
    for page in PAGES:
        text = re.sub(
            r"^ *```.*?^ *```", "", page.read_text(encoding="utf-8"), flags=re.S | re.M
        )
        headings = re.findall(r"^(#{2,6}) ", text, re.M)
        assert headings.count("##") <= 10 and len(headings) <= 15, page.name
