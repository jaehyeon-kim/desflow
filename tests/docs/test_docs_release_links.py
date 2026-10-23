"""The built site links to the release tag, not to `main`.

The Markdown links to files on `main`, and `hooks/release_links.py` rewrites them to
`v<version>` in the rendered pages. This builds one page with the hook and checks the
output, and checks that the site's `mkdocs.yml` loads the hook.
"""

import importlib.util
from pathlib import Path

from mkdocs.commands.build import build
from mkdocs.config import load_config

ROOT = Path(__file__).resolve().parents[2]
_HOOK = ROOT / "hooks" / "release_links.py"
_spec = importlib.util.spec_from_file_location("release_links", _HOOK)
assert _spec is not None and _spec.loader is not None
release_links = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(release_links)

_REPO = "https://github.com/jaehyeon-kim/desflow"
_RAW = "https://raw.githubusercontent.com/jaehyeon-kim/desflow"
_PAGE = f"""# Links

[A file]({_REPO}/blob/main/examples/yaml/local.yaml) and
[a folder]({_REPO}/tree/main/examples).

```bash
curl -O {_RAW}/main/examples/local_replay_orders.csv
```
"""


def test_built_page_links_to_the_release_tag(tmp_path):
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "index.md").write_text(_PAGE, encoding="utf-8")
    (tmp_path / "mkdocs.yml").write_text(
        f"site_name: Links\nhooks:\n  - {_HOOK}\n", encoding="utf-8"
    )
    config = load_config(str(tmp_path / "mkdocs.yml"), site_dir=str(tmp_path / "site"))
    build(config)

    html = (tmp_path / "site" / "index.html").read_text(encoding="utf-8")
    tag = release_links.release_tag()
    assert "/main/" not in html
    assert f"{_REPO}/blob/{tag}/examples/yaml/local.yaml" in html
    assert f"{_REPO}/tree/{tag}/examples" in html
    assert f"{_RAW}/{tag}/examples/local_replay_orders.csv" in html


def test_site_loads_the_hook():
    config = load_config(str(ROOT / "mkdocs.yml"))
    loaded = {Path(hook.__file__).resolve() for hook in config.hooks.values()}
    assert _HOOK.resolve() in loaded
