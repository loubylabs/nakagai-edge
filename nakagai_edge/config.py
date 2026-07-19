"""Connector schema: what a downstream MCP server is and how Nakagai may use it.

A ConnectorSpec is the whole contract for one downstream server: transport,
credentials (by env-var reference, never by value), and the guardrails that
decide which of its tools an upstream agent is allowed to reach.

The registry file (`config/connectors.yaml`) is git-tracked, so no field here
ever holds a secret: `bearer` and `headers` name environment variables, and
OAuth tokens live under `secrets/` (gitignored) or in Postgres.
"""

import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from nakagai_edge._env import read_env_ref
from nakagai_edge.slug import safe_slug

# Verb prefixes that mark a downstream tool as state-changing when the server
# gives us no readOnlyHint. Deliberately broad: a false "write" costs a config
# line, a false "read" can place an order.
DEFAULT_WRITE_PREFIXES = [
    "place_", "cancel_", "update_", "create_", "add_", "remove_", "delete_",
    "set_", "follow_", "unfollow_", "buy_", "sell_", "submit_", "modify_",
    "send_", "notify_",
]

TRANSPORTS = ("stdio", "http")
KIND_TO_TRANSPORT = {"mcp-stdio": "stdio", "mcp-http": "http"}

# The roles a connector may still have. `notify` was one until we deleted the
# outbound-notification stack. Nakagai has no outbound channels; the agent pulls,
# and the agent's own harness owns reaching the human.
#
# This is a live-file concern, not a repo one: production's NAKAGAI_ROOT=/data is a
# Fly volume, and the entrypoint seeds config/ only when a file is ABSENT. It
# never overwrites one the operator has edited. So the deployed connectors.yaml
# still lists `id: imessage / role: notify`, pointing at a module that no longer
# exists. Drop entries whose role we no longer have rather than serve them to an
# agent (or dial them): same fail-closed posture as the mandate's read door.
ROLES = ("signals", "broker", "data", "view")


class OAuthConfig(BaseModel):
    """OAuth 2.1. Empty client_id means dynamic client registration (RFC 7591)."""
    client_id: str = ""
    scopes: str = ""
    redirect_port: int = 8722


class AuthConfig(BaseModel):
    mode: Literal["none", "bearer", "headers", "oauth"] = "none"
    token_env: str = ""                    # bearer: env var holding the token
    headers: dict[str, str] = Field(default_factory=dict)  # header name -> ENV VAR name
    oauth: OAuthConfig = Field(default_factory=OAuthConfig)

    @field_validator("headers")
    @classmethod
    def _headers_name_env_vars(cls, v: dict[str, str]) -> dict[str, str]:
        # The value is an env var NAME, not a token. Catch pasted secrets early:
        # env var names are conventionally SHOUT_CASE and never contain spaces.
        for header, env_name in v.items():
            if not env_name or " " in env_name or env_name != env_name.upper():
                raise ValueError(
                    f"headers.{header} must be an environment variable NAME "
                    f"(e.g. MY_TOKEN), not a secret value"
                )
        return v


class ToolFilter(BaseModel):
    allow: list[str] = Field(default_factory=list)  # fnmatch globs; [] = all
    deny: list[str] = Field(default_factory=list)   # deny wins over allow


class AccountFilter(BaseModel):
    """Restrict which brokerage accounts a connector may act on.

    Two tiers. `allow` is full access: reads and writes alike. `read` is the
    display tier: a read-classified call may name the account, a write never
    may. Numbers, not actions. Both empty means no account restriction.
    """
    allow: list[str] = Field(default_factory=list)  # [] = no account restriction
    read: list[str] = Field(default_factory=list)   # read-only tier
    require_account_arg: bool = True  # a write must NAME an account when tiers exist
    arg_names: list[str] = Field(
        default_factory=lambda: ["account_number", "account_id", "account"])


class ApprovalConfig(BaseModel):
    require_for: list[str] = Field(default_factory=list)  # globs needing a human
    ttl_s: int = 900


class OrderShape(BaseModel):
    """How to read symbol / side / quantity / price / stop out of THIS broker's
    order payload, and WHICH of its tools places a plain share order. Nakagai does
    not own the shape, the downstream MCP server does, so the owner names the
    keys, exactly as `AccountFilter.arg_names` does for account ids.

    Unconfigured (any required key list empty) means this connector can never be
    auto-executed against: the envelope cannot bound what it cannot read. Fail
    closed, same posture as `unknown_is_write`.

    `stock_tools` is why autopilot auto-executes SHARE orders and nothing else. A
    broker's approval gate is a glob (`place_*`), which catches `place_equity_order`
    and `place_option_order` alike, but `Order.notional` is `quantity x price`,
    and for an option that is `contracts x premium`, missing the x100 contract
    multiplier. Four contracts at $2.50 would compute as $10 of notional against a
    $2,000 per-order cap, when the real exposure is $1,000. The envelope's central
    dollar fence would be wrong by two orders of magnitude.

    So the owner DECLARES which tools place shares, and autopilot auto-executes only
    those. A positive gate, not a blacklist: a broker that adds futures or crypto
    tomorrow is refused by default rather than waved through with a notional nobody
    checked. An undeclared `stock_tools` means no auto-execution at all, and the
    refused order still reaches a human tap, as every refusal here does.
    """
    symbol_keys: list[str] = Field(default_factory=list)
    side_keys: list[str] = Field(default_factory=list)
    quantity_keys: list[str] = Field(default_factory=list)
    price_keys: list[str] = Field(default_factory=list)
    stop_keys: list[str] = Field(default_factory=list)
    stock_tools: list[str] = Field(default_factory=list)   # e.g. ["place_equity_order"]
    buy_values: list[str] = Field(
        default_factory=lambda: ["buy", "buy_to_open", "buy_to_cover"])
    sell_values: list[str] = Field(
        default_factory=lambda: ["sell", "sell_to_open", "sell_short"])

    @property
    def configured(self) -> bool:
        # `stock_tools` is deliberately NOT here. An order_shape without it is
        # perfectly READABLE, it just is not auto-executable, and check_envelope
        # says so in the owner's words. Folding it in would surface a missing
        # declaration as "the order could not be read", which is a lie.
        return all([self.symbol_keys, self.side_keys, self.quantity_keys,
                    self.price_keys, self.stop_keys])


class GuardrailsConfig(BaseModel):
    tools: ToolFilter = Field(default_factory=ToolFilter)
    allow_writes: bool = False
    write_prefixes: list[str] = Field(default_factory=lambda: list(DEFAULT_WRITE_PREFIXES))
    read_only_tools: list[str] = Field(default_factory=list)   # globs forced to "read"
    write_tools: list[str] = Field(default_factory=list)       # globs forced to "write"
    unknown_is_write: bool = True   # fail closed: unclassifiable tool == write
    accounts: AccountFilter = Field(default_factory=AccountFilter)
    approvals: ApprovalConfig = Field(default_factory=ApprovalConfig)
    order_shape: OrderShape = Field(default_factory=OrderShape)


class ConnectorSpec(BaseModel):
    """One downstream server. A superset of the legacy registry entry, so old
    `config/connectors.yaml` files keep parsing unchanged."""
    id: str
    name: str = ""
    kind: str                       # mcp-stdio | mcp-http | data | view
    role: str                       # signals | broker | data | view
    url: str = ""
    command: str = ""
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    enabled: bool = False
    notes: str = ""
    timeout_s: float = 30.0
    idle_ttl_s: float = 600.0
    auth: AuthConfig = Field(default_factory=AuthConfig)
    guardrails: GuardrailsConfig = Field(default_factory=GuardrailsConfig)

    @field_validator("id")
    @classmethod
    def _id_is_a_slug(cls, v: str) -> str:
        # Doubles as path safety: the id names a token file under secrets/.
        return safe_slug(v, label="connector id")

    @property
    def is_mcp(self) -> bool:
        return self.kind in KIND_TO_TRANSPORT

    @property
    def transport(self) -> str | None:
        return KIND_TO_TRANSPORT.get(self.kind)

    def check_connectable(self) -> None:
        """Raise ValueError unless this spec has what its transport needs."""
        if not self.is_mcp:
            raise ValueError(f"connector {self.id!r} (kind={self.kind}) is not an MCP server")
        if self.transport == "http" and not self.url:
            raise ValueError(f"connector {self.id!r} is mcp-http but has no url")
        if self.transport == "stdio" and not self.command:
            raise ValueError(f"connector {self.id!r} is mcp-stdio but has no command")
        if self.transport == "stdio" and self.auth.mode == "oauth":
            raise ValueError(f"connector {self.id!r}: oauth requires an http transport")


def load_specs(registry: dict) -> dict[str, ConnectorSpec]:
    """Parse `{"connectors": [...]}` into id -> ConnectorSpec, dropping entries whose
    role this codebase no longer has (see ROLES), so a stale `role: notify` on the
    live volume vanishes instead of being served to the agent."""
    specs = {}
    for entry in registry.get("connectors") or []:
        if (entry or {}).get("role") not in ROLES:
            continue
        spec = ConnectorSpec(**entry)
        specs[spec.id] = spec
    return specs


_ENV_REF = re.compile(r"^\$\{([A-Z][A-Z0-9_]*)\}$")


def resolve_env_refs(env: dict[str, str], connector_id: str) -> dict[str, str]:
    """Resolve `${NAME}` values from the host environment, fail-closed.

    The registry is git-tracked, so a stdio connector's secrets must arrive
    by reference. Only the exact form `${NAME}` is a reference; anything else
    is a literal. A reference to an unset or empty variable refuses the
    connection rather than launching a broker process with blank credentials.
    """
    resolved: dict[str, str] = {}
    for key, value in env.items():
        m = _ENV_REF.match(value)
        if not m:
            resolved[key] = value
            continue
        name = m.group(1)
        actual = read_env_ref(name)
        if not actual:
            raise ValueError(f"connector {connector_id!r} needs env var {name}, "
                             f"which is not set")
        resolved[key] = actual
    return resolved
