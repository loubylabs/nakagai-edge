"""Which broker schema violations are worth refusing a payload over.

A downstream MCP server publishes an `outputSchema` and the SDK validates every
result against it, unconditionally, with no opt-out. That is worth having: a
field changing type or going missing is a broker saying something untrue about
data Nakagai reads. It is NOT worth having for `additionalProperties: false`,
which is a closed-world claim about the server's own payload that we never had
any reason to enforce, and that a live broker can break by adding one field
nothing here reads.

Robinhood does exactly that: the tool that lists accounts declares its account
objects closed and returns `unsettled_funds` anyway, which took the entire
portfolio sweep down over a field no capability map mentions.

Pure: no I/O, no state, no clock, no logging. Same posture as capability.py,
guardrails.py and warrant.py, so the whole rule is testable without a broker.
The caller in hub.py owns the logging and the decision to re-raise.
"""

from typing import Any

from jsonschema import SchemaError
from jsonschema.validators import validator_for


def _is_undeclared_property(error) -> bool:
    """True when this leaf error is `additionalProperties: false` refusing a
    property the schema never declared.

    `validator_value is False` is what separates the two spellings.
    `additionalProperties` given a SCHEMA means extras are allowed but must
    have a shape, and jsonschema descends into it and yields ordinary typed
    errors instead of this one.

    An error carrying `.context` came from `anyOf`/`oneOf`. It is never treated
    as tolerable, however tolerable its leaves look: under `anyOf` the instance
    is invalid only when every branch failed, and deciding that a branch failed
    only over extras means re-running branch selection here. Fail closed.
    """
    if error.context:
        return False
    return error.validator == "additionalProperties" and error.validator_value is False


def _names(error) -> set[str]:
    """The property names one undeclared-property error is about.

    Computed from the instance and the schema rather than parsed out of
    jsonschema's message prose, which is not an interface.
    """
    if not isinstance(error.instance, dict):
        return set()
    declared = set((error.schema or {}).get("properties") or {})
    return set(error.instance) - declared


def undeclared_properties(schema: dict | None, instance: Any) -> list[str] | None:
    """The properties a payload carried that its schema did not declare, or
    None when the payload is invalid for any other reason.

    None means FATAL, and it is never "nothing was wrong": a caller reaches
    here only after validation has already failed, so a payload that validates
    cleanly here means the caller and this module disagree about which schema
    is in play, and the safe reading of a disagreement is to refuse.

    `iter_errors`, not `validate`: `validate` raises the single best-matching
    error, so a payload carrying BOTH an undeclared property and a wrong type
    could present as the tolerable one and lose the fatal one behind it. Every
    error in the tree has to be seen before anything is tolerated.

    `check_schema` runs explicitly: building a validator and calling
    `iter_errors` never validates the schema itself, so a malformed schema
    (an unknown `type` value, say) surfaces as whatever exception the type
    checker happens to raise rather than as `SchemaError`. Checking first
    routes every such case through the one branch meant to catch it.
    """
    if not isinstance(schema, dict):
        return None
    try:
        validator = validator_for(schema)(schema)
        validator.check_schema(schema)
        errors = list(validator.iter_errors(instance))
    except SchemaError:
        # A schema that will not compile says nothing about the payload.
        return None
    if not errors:
        return None
    found: set[str] = set()
    for error in errors:
        if not _is_undeclared_property(error):
            return None
        found |= _names(error)
    return sorted(found)
