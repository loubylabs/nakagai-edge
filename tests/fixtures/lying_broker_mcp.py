"""A downstream MCP server that contradicts its own declared output schema.

Robinhood's `get_accounts` declares its account objects closed
(`additionalProperties: false`) and then returns `unsettled_funds`. An
`MCPServer` cannot stand in for that: it derives a tool's output schema from
the return annotation, so the schema and the payload can never disagree, and
the declared shape is the one it then validates against before sending.

The low-level `Server` is the one path where the two are written separately:
`on_list_tools` publishes the schema, `on_call_tool` hands back a
`types.CallToolResult` already built, and nothing in between reconciles them.
That is what lets this fixture lie the way a real broker does.

Runnable as a stdio server, like the other fixtures here:

    uv run python tests/fixtures/lying_broker_mcp.py
"""

import json

import mcp.types as types
from mcp.server.lowlevel import Server

ACCOUNT_SCHEMA = {
    "type": "object",
    "properties": {
        "accounts": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "account_number": {"type": "string"},
                    "type": {"type": "string"},
                },
                "required": ["account_number", "type"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["accounts"],
}


async def list_tools(ctx, params) -> types.ListToolsResult:
    return types.ListToolsResult(tools=[
        types.Tool(name="get_accounts",
                   description="Accounts, with one property the schema forbids.",
                   inputSchema={"type": "object", "properties": {}},
                   outputSchema=ACCOUNT_SCHEMA),
        types.Tool(name="get_accounts_wrong_type",
                   description="Accounts, with a declared field of the wrong type.",
                   inputSchema={"type": "object", "properties": {}},
                   outputSchema=ACCOUNT_SCHEMA),
    ])


def _result(account: dict) -> types.CallToolResult:
    """Text and structured content carrying the same document, which is what a
    real broker sends and what makes hub.serialize_result drop the redundant
    copy and hand the caller `data`."""
    structured = {"accounts": [account]}
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=json.dumps(structured))],
        structuredContent=structured)


async def call_tool(ctx, params: types.CallToolRequestParams) -> types.CallToolResult:
    if params.name == "get_accounts_wrong_type":
        # `type` is declared a string. This is the violation that must stay fatal.
        return _result({"account_number": "463605220", "type": 7})
    # `unsettled_funds` is not in the declared properties, under
    # additionalProperties: false. This is the violation to tolerate.
    return _result({"account_number": "463605220", "type": "cash",
                    "unsettled_funds": "0.0000"})


server = Server("lying-broker", on_list_tools=list_tools, on_call_tool=call_tool)


if __name__ == "__main__":
    import anyio
    from mcp.server.stdio import stdio_server

    async def main() -> None:
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())

    anyio.run(main)
