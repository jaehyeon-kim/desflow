"""Data Contract rules in a schema registry response, which desflow does not run."""

from typing import Any

from desflow.connectors._schema_registry.errors import SerializationError

_READ_MODES = ("READ", "WRITEREAD")
_RULE_LISTS = {"domainRules": "domain rule", "encodingRules": "encoding rule"}


def refuse_rules(schema_response: dict[str, Any]) -> None:
    """Refuses a schema whose rules would change a message on read.

    A rule of `domainRules` or `encodingRules` with mode `READ` or `WRITEREAD`,
    not disabled, may check or transform a message, for example by decrypting a
    field. desflow runs no rule, so decoding without it could return wrong data.
    Migration rules are ignored: they run only when a reader asks for another
    schema version.

    Args:
        schema_response (dict[str, Any]): A registry response that may hold `ruleSet`.

    Raises:
        SerializationError: If an enabled read rule is present. The message names
            the rule.
    """
    rule_set = schema_response.get("ruleSet") or {}
    for list_name, kind in _RULE_LISTS.items():
        for rule in rule_set.get(list_name) or []:
            if rule.get("mode") in _READ_MODES and not rule.get("disabled"):
                raise SerializationError(
                    f"The schema has the {kind} '{rule.get('name')}' "
                    f"(mode {rule.get('mode')}). desflow does not run Data Contract "
                    "rules, so it does not read this schema."
                )
