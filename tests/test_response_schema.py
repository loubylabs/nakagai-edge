"""Classifying one broker's schema violation: tolerable, or a lie about a field.

Pure, like the module it tests. No broker, no session, no network.

The rule under test: a property the schema never declared is tolerable, because
`additionalProperties: false` is a closed-world claim we never had reason to
enforce on someone else's server. Everything else is fatal, because it says a
field we may READ is not what it promised to be.
"""

import pytest

from nakagai_edge.response_schema import undeclared_properties

# The shape Robinhood publishes for get_accounts, reduced to what matters here.
ACCOUNTS = {
    "type": "object",
    "properties": {
        "data": {
            "type": "object",
            "properties": {
                "accounts": {
                    "type": "array",
                    "items": {
                        "type": ["null", "object"],
                        "properties": {
                            "account_number": {"type": "string"},
                            "type": {"type": "string"},
                            "deactivated": {"type": "boolean"},
                        },
                        "required": ["account_number", "type"],
                        "additionalProperties": False,
                    },
                }
            },
        }
    },
}


def _payload(*accounts):
    return {"data": {"accounts": list(accounts)}}


GOOD = {"account_number": "463605220", "type": "cash", "deactivated": False}


def test_an_undeclared_property_is_tolerable_and_names_itself():
    """The live 2026-08-04 failure: a field nothing in Nakagai reads."""
    payload = _payload({**GOOD, "unsettled_funds": "0.0000"})
    assert undeclared_properties(ACCOUNTS, payload) == ["unsettled_funds"]


def test_several_undeclared_properties_are_all_named():
    payload = _payload({**GOOD, "unsettled_funds": "0.0", "sweep_balance": "1.0"})
    assert undeclared_properties(ACCOUNTS, payload) == ["sweep_balance",
                                                       "unsettled_funds"]


def test_a_wrong_type_is_fatal():
    """`type` promised a string. A field we read changing shape is the whole
    reason this validation is worth having."""
    payload = _payload({**GOOD, "deactivated": "no longer a boolean"})
    assert undeclared_properties(ACCOUNTS, payload) is None


def test_a_missing_required_field_is_fatal():
    payload = _payload({"type": "cash"})
    assert undeclared_properties(ACCOUNTS, payload) is None


def test_an_extra_property_beside_a_wrong_type_is_fatal():
    """The case a bare `validate()` would hide: it raises only the best match,
    so the type error can sit behind the extra property. iter_errors is what
    makes this fail closed."""
    payload = _payload({**GOOD, "deactivated": "nope", "unsettled_funds": "0.0"})
    assert undeclared_properties(ACCOUNTS, payload) is None


def test_one_bad_row_beside_one_tolerable_row_is_fatal():
    """Rows are not judged independently: any fatal error anywhere is fatal."""
    payload = _payload({**GOOD, "unsettled_funds": "0.0"}, {"type": "cash"})
    assert undeclared_properties(ACCOUNTS, payload) is None


def test_a_clean_payload_returns_none():
    """None is always "do not tolerate". A caller only reaches here after
    validation already failed, so a clean payload here means the caller and
    this module disagree, and the safe reading of that is to refuse."""
    assert undeclared_properties(ACCOUNTS, _payload(GOOD)) is None


def test_an_extra_property_nested_under_any_of_is_fatal():
    """Deliberately conservative. Under anyOf the instance is invalid only when
    EVERY branch failed, and deciding a branch failed "only over extras" means
    re-running branch selection here. Fail closed instead."""
    schema = {"anyOf": [
        {"type": "object", "properties": {"a": {"type": "string"}},
         "additionalProperties": False},
        {"type": "object", "properties": {"b": {"type": "string"}},
         "additionalProperties": False},
    ]}
    assert undeclared_properties(schema, {"a": "x", "c": "y"}) is None


def test_additional_properties_as_a_schema_is_not_this_case():
    """`additionalProperties` given a SCHEMA rather than False says extras are
    allowed but must have a shape. jsonschema descends and yields ordinary
    typed errors, and those stay fatal."""
    schema = {"type": "object", "properties": {"a": {"type": "string"}},
              "additionalProperties": {"type": "string"}}
    assert undeclared_properties(schema, {"a": "x", "b": 7}) is None


def test_a_pattern_matched_property_is_not_undeclared():
    """`patternProperties` legally declares a key even though it never
    appears in `properties`. Only a key neither names is undeclared, so the
    warning must not name a legal property as the culprit."""
    schema = {"type": "object",
              "properties": {"a": {"type": "string"}},
              "patternProperties": {"^x_": {"type": "string"}},
              "additionalProperties": False}
    payload = {"a": "keep", "x_fine": "legal", "junk": "not declared"}
    assert undeclared_properties(schema, payload) == ["junk"]


def test_a_null_schema_is_fatal():
    """A tool with no declared output schema never reaches validation, so a
    None here means the caller lost track of which tool it was validating."""
    assert undeclared_properties(None, {"anything": 1}) is None


def test_a_broken_schema_is_fatal():
    """A schema jsonschema cannot compile says nothing about the payload."""
    assert undeclared_properties({"type": "not-a-real-type"}, {"a": 1}) is None


def test_an_unresolvable_ref_is_fatal():
    """A `$ref` pointing at a `$defs` entry that does not exist passes
    check_schema (it is syntactically valid) and only fails once iter_errors
    tries to resolve it. That failure must return None like every other
    "cannot judge this" case, not raise into the caller.

    This exercises the classifier's own fail-closed contract, not the hub
    path: `hub.call` cannot reach here with a dangling $ref. The SDK's own
    validate() only catches ValidationError and SchemaError, so the
    jsonschema-internal exception a dangling $ref raises escapes
    `_validate_tool_result`, and then `hub.call`, uncaught, rather than
    becoming the RuntimeError our override classifies. It stays fail-closed
    either way, because the payload is refused rather than relayed, whichever
    route raises it."""
    schema = {"type": "object", "properties": {"x": {"$ref": "#/$defs/missing"}}}
    assert undeclared_properties(schema, {"x": 1}) is None


@pytest.mark.parametrize("instance", ["a string", 7, None, ["a", "list"]])
def test_a_non_object_payload_is_fatal(instance):
    assert undeclared_properties(ACCOUNTS, instance) is None
