"""The YAML Keys reference page lists every blueprint field.

The page is generated from the JSON Schema, and the schema from the models, so a
field could still be left out of it by a schema override. This walks every Pydantic
model reachable from `Blueprint` and requires each field name on the page in a code
span, alone or followed by a colon.
"""

import re
import typing
from desflow.blueprint import models
from feature_lists import gen_pages


def _models_in(annotation: object) -> list:
    """Every `_Model` subclass reachable from a type annotation."""
    found: list = []
    if isinstance(annotation, type) and issubclass(annotation, models._Model):
        found.append(annotation)
    for arg in typing.get_args(annotation):
        found.extend(_models_in(arg))
    return found


# The page lists every blueprint key except the distribution parameters, which it
# defers to the Distributions reference (services and arrivals). An arrival
# subclasses the distribution model, so the parameters are deferred on both models. They are deferred per model, not by name, so the same
# name on another model (such as a future `TimeoutAfter.value`) is still required.
_DISTRIBUTION_PARAMS = set(models.Distribution.model_fields)
_PARAM_MODELS = (models.Distribution, models.Arrival)


def _all_fields() -> set:
    """Every field name of every model reachable from `Blueprint`, except the
    distribution parameters the page documents on the Distributions reference."""
    seen: set = set()
    queue = [models.Blueprint]
    fields: set = set()
    while queue:
        model = queue.pop()
        if model in seen:
            continue
        seen.add(model)
        for name, info in model.model_fields.items():
            if not (model in _PARAM_MODELS and name in _DISTRIBUTION_PARAMS):
                # A blueprint writes the alias, such as `from` for `from_`
                fields.add(info.alias or name)
            for nested in _models_in(info.annotation):
                if nested not in seen:
                    queue.append(nested)
    return fields


def _documented(page: str) -> set:
    """Field names the page shows: an inline code span, a YAML key in a block, or a
    section header."""
    names: set = set()
    names.update(re.findall(r"`([A-Za-z_][A-Za-z0-9_]*):?`", page))
    names.update(re.findall(r"(?<![\w.])([A-Za-z_][A-Za-z0-9_]*):", page))
    # A group of keys written as one span, such as `{time_to_failure, repair, work}`.
    for group in re.findall(r"`\{([^`]*)\}`", page):
        names.update(re.findall(r"[A-Za-z_]\w*", group))
    for header in re.findall(r"(?m)^#+ (.*)", page):
        names.update(re.findall(r"[A-Za-z_]\w*", header))
    return names


def test_every_blueprint_field_is_on_yaml_keys():
    page = gen_pages().yaml_keys_page()
    documented = _documented(page)
    missing = sorted(name for name in _all_fields() if name not in documented)
    assert not missing, f"fields missing from YAML Keys: {missing}"
