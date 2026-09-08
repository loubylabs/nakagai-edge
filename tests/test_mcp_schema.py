"""Local JSON Schema references become self-contained MCP tool schemas."""

from copy import deepcopy

import pytest


def _inline(schema: dict) -> dict:
    try:
        from nakagai_edge.mcp_schema import inline_local_refs
    except ModuleNotFoundError:
        pytest.fail("nakagai_edge.mcp_schema has not been implemented")
    return inline_local_refs(schema)


def test_nested_policy_refs_inline_without_mutating_source():
    schema = {
        "$defs": {
            "Policy": {
                "type": "object",
                "properties": {"fee": {"$ref": "#/$defs/Fee"}},
                "required": ["fee"],
            },
            "Fee": {"type": "number", "minimum": 0},
        },
        "type": "object",
        "properties": {"execution": {"$ref": "#/$defs/Policy"}},
    }
    original = deepcopy(schema)

    out = _inline(schema)

    assert out["properties"]["execution"]["properties"]["fee"] == {
        "type": "number",
        "minimum": 0,
    }
    assert schema == original
    assert "$defs" not in out


def test_inline_schema_is_copied_without_changing_its_contract():
    schema = {
        "type": "object",
        "properties": {
            "count": {"type": "integer", "minimum": 1},
            "label": {"type": "string", "default": "daily"},
        },
        "required": ["count"],
        "additionalProperties": False,
    }

    out = _inline(schema)

    assert out == schema
    assert out is not schema
    assert out["properties"] is not schema["properties"]


def test_reference_siblings_remain_conjoined_and_keep_the_visible_default():
    schema = {
        "$defs": {
            "Count": {"type": "integer", "minimum": 1, "default": 2},
        },
        "type": "object",
        "properties": {
            "count": {
                "$ref": "#/$defs/Count",
                "maximum": 0,
                "default": -1,
            },
        },
    }

    count = _inline(schema)["properties"]["count"]

    assert count["allOf"] == [
        {"type": "integer", "minimum": 1, "default": 2},
    ]
    assert count["maximum"] == 0
    assert count["default"] == -1


def test_reference_target_default_stays_visible_with_other_siblings():
    schema = {
        "$defs": {
            "Mode": {"type": "string", "enum": ["paper"], "default": "paper"},
        },
        "type": "object",
        "properties": {
            "mode": {"$ref": "#/$defs/Mode", "description": "Execution mode"},
        },
    }

    mode = _inline(schema)["properties"]["mode"]

    assert mode["allOf"] == [
        {"type": "string", "enum": ["paper"], "default": "paper"},
    ]
    assert mode["description"] == "Execution mode"
    assert mode["default"] == "paper"


def test_reference_preserves_an_existing_all_of_sibling():
    schema = {
        "$defs": {"Count": {"type": "integer", "minimum": 1}},
        "type": "object",
        "properties": {
            "count": {
                "$ref": "#/$defs/Count",
                "allOf": [{"maximum": 10}],
            },
        },
    }

    count = _inline(schema)["properties"]["count"]

    assert count == {
        "allOf": [
            {"type": "integer", "minimum": 1},
            {"maximum": 10},
        ],
    }


def test_literal_reference_keys_are_data_and_property_names_are_unchanged():
    literal = {"$ref": "https://example.test/instance", "nested": [{"$ref": 4}]}
    schema = {
        "type": "object",
        "properties": {
            "$ref": {"type": "string"},
            "payload": {
                "type": "object",
                "default": literal,
                "const": literal,
                "enum": [literal],
                "examples": [literal],
            },
        },
    }

    out = _inline(schema)

    assert out == schema
    assert out["properties"]["payload"]["default"] == literal


@pytest.mark.parametrize(
    ("schema", "message"),
    [
        (
            {"type": "object", "properties": {"x": {"$ref": "#/$defs/Missing"}}},
            "missing local reference",
        ),
        (
            {"type": "object", "properties": {"x": {"$ref": "https://example.test/X"}}},
            "unsupported reference",
        ),
        (
            {"type": "object", "properties": {"x": {"$dynamicRef": "#node"}}},
            "unsupported reference keyword",
        ),
        (
            {
                "$defs": {"A": {"$ref": "#/$defs/B"}, "B": {"$ref": "#/$defs/A"}},
                "type": "object",
                "properties": {"x": {"$ref": "#/$defs/A"}},
            },
            "cyclic local reference",
        ),
    ],
    ids=["missing", "external", "dynamic", "cyclic"],
)
def test_unsupported_references_fail_closed_with_a_bounded_diagnostic(schema, message):
    with pytest.raises(ValueError, match=message) as caught:
        _inline(schema)

    assert len(str(caught.value)) <= 200


def test_schema_recursion_is_bounded():
    nested: dict = {"type": "string"}
    for _ in range(80):
        nested = {"allOf": [nested]}

    with pytest.raises(ValueError, match="depth limit"):
        _inline({"type": "object", "properties": {"x": nested}})


def test_acyclic_reference_expansion_is_bounded():
    defs: dict[str, dict] = {"Level0": {"type": "string"}}
    for level in range(1, 16):
        prior = f"#/$defs/Level{level - 1}"
        defs[f"Level{level}"] = {"anyOf": [{"$ref": prior}, {"$ref": prior}]}
    schema = {
        "$defs": defs,
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/Level15"}},
    }

    with pytest.raises(ValueError, match="expansion limit"):
        _inline(schema)


def test_wide_scalar_annotation_fanout_counts_every_expanded_entry():
    defs: dict[str, dict] = {"Level0": {"type": "integer", "enum": list(range(5_000))}}
    for level in range(1, 9):
        prior = f"#/$defs/Level{level - 1}"
        defs[f"Level{level}"] = {"anyOf": [{"$ref": prior}, {"$ref": prior}]}
    schema = {
        "$defs": defs,
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/Level8"}},
    }

    with pytest.raises(ValueError, match="expansion limit"):
        _inline(schema)


def test_repeated_wide_string_payload_is_bounded_during_fanout():
    defs: dict[str, dict] = {"Level0": {"type": "string", "enum": ["x" * 5_000]}}
    for level in range(1, 9):
        prior = f"#/$defs/Level{level - 1}"
        defs[f"Level{level}"] = {"anyOf": [{"$ref": prior}, {"$ref": prior}]}
    schema = {
        "$defs": defs,
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/Level8"}},
    }

    with pytest.raises(ValueError, match="payload limit"):
        _inline(schema)


@pytest.mark.parametrize(
    ("kind", "message"),
    [("missing", "missing"), ("non-schema", "not a schema"), ("cyclic", "cyclic")],
)
def test_reference_bearing_errors_truncate_long_local_names(kind, message):
    name = "x" * 400
    ref = f"#/$defs/{name}"
    if kind == "missing":
        defs = {}
    elif kind == "non-schema":
        defs = {name: "not a schema"}
    else:
        defs = {name: {"$ref": ref}}
    schema = {
        "$defs": defs,
        "type": "object",
        "properties": {"x": {"$ref": ref}},
    }

    with pytest.raises(ValueError) as caught:
        _inline(schema)

    assert message in str(caught.value)
    assert len(str(caught.value)) <= 200


def test_oversized_reference_fails_with_a_fixed_bounded_diagnostic():
    ref = "#/$defs/" + "x" * 5_000
    schema = {"type": "object", "properties": {"x": {"$ref": ref}}}

    with pytest.raises(ValueError, match="reference length limit") as caught:
        _inline(schema)

    assert len(str(caught.value)) <= 200
