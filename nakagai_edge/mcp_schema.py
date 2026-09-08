"""Bounded local-reference expansion for MCP tool input schemas."""

from typing import Any


MAX_SCHEMA_DEPTH = 64
MAX_EXPANDED_CONTAINERS = 10_000

_SCHEMA_MAP_KEYWORDS = {
    "$defs",
    "definitions",
    "dependentSchemas",
    "patternProperties",
    "properties",
}
_SCHEMA_LIST_KEYWORDS = {"allOf", "anyOf", "oneOf", "prefixItems"}
_SCHEMA_KEYWORDS = {
    "additionalProperties",
    "contains",
    "contentSchema",
    "else",
    "if",
    "items",
    "not",
    "propertyNames",
    "then",
    "unevaluatedItems",
    "unevaluatedProperties",
}
_UNSUPPORTED_REFERENCE_KEYWORDS = {"$dynamicRef", "$recursiveRef"}


class SchemaInliningError(ValueError):
    """The generated schema cannot be safely made self-contained."""


class _Inliner:
    def __init__(self, root: dict[str, Any]):
        self.root = root
        self.containers = 0

    def _container(self) -> None:
        self.containers += 1
        if self.containers > MAX_EXPANDED_CONTAINERS:
            raise SchemaInliningError(
                f"schema expansion limit is {MAX_EXPANDED_CONTAINERS} containers"
            )

    @staticmethod
    def _depth(depth: int) -> None:
        if depth > MAX_SCHEMA_DEPTH:
            raise SchemaInliningError(
                f"schema depth limit is {MAX_SCHEMA_DEPTH} containers"
            )

    def _literal(self, value: Any, depth: int) -> Any:
        """Copy instance-valued annotations without interpreting their keys."""
        self._depth(depth)
        if isinstance(value, dict):
            self._container()
            return {key: self._literal(item, depth + 1)
                    for key, item in value.items()}
        if isinstance(value, list):
            self._container()
            return [self._literal(item, depth + 1) for item in value]
        if isinstance(value, tuple):
            self._container()
            return tuple(self._literal(item, depth + 1) for item in value)
        return value

    def _target(self, ref: Any) -> dict | bool:
        if not isinstance(ref, str) or not ref.startswith("#/$defs/"):
            raise SchemaInliningError("unsupported reference; expected local #/$defs/... ref")
        parts = ref[2:].split("/")
        value: Any = self.root
        for encoded in parts:
            token = encoded.replace("~1", "/").replace("~0", "~")
            if not isinstance(value, dict) or token not in value:
                raise SchemaInliningError(f"missing local reference {ref!r}")
            value = value[token]
        if not isinstance(value, (dict, bool)):
            raise SchemaInliningError(f"local reference {ref!r} is not a schema")
        return value

    def _schema_map(self, value: Any, depth: int,
                    active: tuple[str, ...]) -> dict:
        if not isinstance(value, dict):
            raise SchemaInliningError("schema map keyword must contain an object")
        self._container()
        return {
            key: self._schema(item, depth + 1, active)
            for key, item in value.items()
        }

    def _schema_list(self, value: Any, depth: int,
                     active: tuple[str, ...]) -> list:
        if not isinstance(value, list):
            raise SchemaInliningError("schema list keyword must contain an array")
        self._container()
        return [self._schema(item, depth + 1, active) for item in value]

    def _schema(self, schema: Any, depth: int,
                active: tuple[str, ...]) -> dict | bool:
        self._depth(depth)
        if isinstance(schema, bool):
            return schema
        if not isinstance(schema, dict):
            raise SchemaInliningError("schema node must be an object or boolean")
        self._container()

        if "$ref" in schema:
            ref = schema["$ref"]
            if not isinstance(ref, str):
                raise SchemaInliningError("unsupported reference; $ref must be a string")
            if ref in active:
                raise SchemaInliningError(f"cyclic local reference {ref!r}")
            target = self._schema(self._target(ref), depth + 1, (*active, ref))
            siblings = self._schema_members(schema, depth, active, omit_ref=True)
            if not siblings:
                return target
            if isinstance(target, dict) and "default" in target \
                    and "default" not in siblings:
                siblings["default"] = self._literal(target["default"], depth + 1)
            existing_all_of = siblings.pop("allOf", [])
            self._container()
            siblings["allOf"] = [target, *existing_all_of]
            return siblings

        return self._schema_members(schema, depth, active)

    def _schema_members(self, schema: dict, depth: int,
                        active: tuple[str, ...], *, omit_ref: bool = False) -> dict:
        out = {}
        for key, value in schema.items():
            if key == "$defs" or (omit_ref and key == "$ref"):
                continue
            if key in _UNSUPPORTED_REFERENCE_KEYWORDS:
                raise SchemaInliningError(f"unsupported reference keyword {key!r}")
            if key in _SCHEMA_MAP_KEYWORDS:
                out[key] = self._schema_map(value, depth, active)
            elif key in _SCHEMA_LIST_KEYWORDS:
                out[key] = self._schema_list(value, depth, active)
            elif key in _SCHEMA_KEYWORDS:
                if key == "items" and isinstance(value, list):
                    out[key] = self._schema_list(value, depth, active)
                else:
                    out[key] = self._schema(value, depth + 1, active)
            else:
                out[key] = self._literal(value, depth + 1)
        return out


def inline_local_refs(schema: dict) -> dict:
    """Return a self-contained copy of one generated MCP input schema.

    Only acyclic references rooted under ``#/$defs/`` are supported. Schema
    keywords are traversed explicitly so a ``$ref`` key inside a default,
    example, enum, or const remains ordinary instance data.
    """
    if not isinstance(schema, dict):
        raise TypeError("schema must be an object")
    result = _Inliner(schema)._schema(schema, 0, ())
    if not isinstance(result, dict):
        raise SchemaInliningError("root schema must be an object")
    return result
