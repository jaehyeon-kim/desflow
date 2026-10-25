"""Every name in `desflow.__all__` has a tested example, an API reference entry and a
place in the skill. feature_lists.py says what a tested example is."""

import io
import re
import textwrap
import tokenize

import pytest
from feature_lists import API, DOCS, LABELLED, PAGES, SKILL

# A type the reader receives from a call and never builds. An example that imports it
# or annotates a variable with it teaches nothing, so it needs instead a page that
# names it and reads its fields in a labelled block: the page, and the variable the
# block reads.
_RECEIVED = {
    "FitResult": ("user-guide/input-fitting.md", "result"),
    "ExperimentResult": ("user-guide/experiments/index.md", "result"),
    "Replications": ("user-guide/replications.md", "result"),
}
# A type the reader can build but the pages do not, each with the reason. It needs
# only a page that names it.
_NAMED_ONLY = {
    # run_experiment builds one from a mapping or a file, the two forms the pages
    # teach; the class is named for a script that builds the settings itself.
    "ExperimentSpec": "user-guide/experiments/index.md",
}


def _code_names(code):
    """The names the code uses, leaving out its comments and strings. A block that
    is part of a statement, such as an open call, gives the names before its end."""
    names = set()
    tokens = tokenize.generate_tokens(io.StringIO(textwrap.dedent(code)).readline)
    try:
        for token in tokens:
            if token.type == tokenize.NAME:
                names.add(token.string)
    except tokenize.TokenError:
        pass
    return names


def _used():
    """The names every labelled Python block on the pages uses."""
    names = set()
    for page in PAGES:
        for _, language, code in LABELLED.findall(page.read_text(encoding="utf-8")):
            if language == "python":
                names |= _code_names(code)
    return names


USED = _used()


def _page_names(page, name):
    """The text of `page`, which must name `name` in code format."""
    text = (DOCS / page).read_text(encoding="utf-8")
    assert f"`{name}`" in text, f"{name}: not named on {page}"
    return text


@pytest.mark.parametrize("name", API)
def test_every_public_name_has_an_example_a_reference_and_a_skill_entry(name):
    if name in _RECEIVED:
        page, variable = _RECEIVED[name]
        text = _page_names(page, name)
        blocks = [
            code for _, language, code in LABELLED.findall(text) if language == "python"
        ]
        assert any(f"{variable}." in code for code in blocks), (
            f"{name}: no labelled block on {page} reads {variable}'s fields"
        )
    elif name in _NAMED_ONLY:
        _page_names(_NAMED_ONLY[name], name)
    else:
        assert name in USED, (
            f"{name}: no labelled Python block on a Learn, How-To, User Guide or "
            "Examples page uses it"
        )
    api_page = (DOCS / "api.md").read_text(encoding="utf-8")
    assert re.search(rf"^::: desflow(\.\w+)*\.{name}$", api_page, re.M), name
    api_skill = (SKILL / "references" / "api.md").read_text(encoding="utf-8")
    assert f"`{name}`" in api_skill, f"{name}: not in references/api.md"
