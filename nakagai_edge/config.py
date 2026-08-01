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

from pydantic import BaseModel, Field, field_validator, model_validator

from nakagai_edge._env import read_env_ref
from nakagai_edge.capability import CAPABILITIES, Capability, CapabilityError
from nakagai_edge.slug import safe_slug

# Verb prefixes that mark a downstream tool as state-changing when the server
# gives us no readOnlyHint. Deliberately broad: a false "write" costs a config
# line, a false "read" can place an order.
DEFAULT_WRITE_PREFIXES = [
    "place_", "cancel_", "update_", "create_", "add_", "remove_", "delete_",
    "set_", "follow_", "unfollow_", "buy_", "sell_", "submit_", "modify_",
    "send_", "notify_",
]

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


class GuardrailsConfig(BaseModel):
    tools: ToolFilter = Field(default_factory=ToolFilter)
    allow_writes: bool = False
    write_prefixes: list[str] = Field(default_factory=lambda: list(DEFAULT_WRITE_PREFIXES))
    read_only_tools: list[str] = Field(default_factory=list)   # globs forced to "read"
    write_tools: list[str] = Field(default_factory=list)       # globs forced to "write"
    unknown_is_write: bool = True   # fail closed: unclassifiable tool == write
    accounts: AccountFilter = Field(default_factory=AccountFilter)
    approvals: ApprovalConfig = Field(default_factory=ApprovalConfig)


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
    capabilities: dict[str, Capability] = Field(default_factory=dict)

    @field_validator("id")
    @classmethod
    def _id_is_a_slug(cls, v: str) -> str:
        # Doubles as path safety: the id names a token file under secrets/.
        return safe_slug(v, label="connector id")

    @field_validator("capabilities")
    @classmethod
    def _names_are_in_the_vocabulary(cls, v: dict) -> dict:
        # A typo must not become a silently absent capability: an unmapped
        # capability already refuses at call time, and a misspelled one would
        # look identical while the correct spelling sat right there in the file.
        unknown = sorted(set(v) - set(CAPABILITIES))
        if unknown:
            raise ValueError(
                f"unknown capabilities: {', '.join(unknown)} "
                f"(known: {', '.join(sorted(CAPABILITIES))})")
        return v

    @field_validator("capabilities")
    @classmethod
    def _required_fields_are_mapped(cls, v: dict, info) -> dict:
        """Every declared capability must say where its required fields live.

        Declared after `_names_are_in_the_vocabulary` so it runs after it, and
        every name reaching here is therefore known.

        Nothing downstream catches this, which is why it has to be caught at
        parse time. `read_partial` deliberately skips the required check, so a
        map with no `symbol` path does not fail loudly: the sweep keeps
        producing position rows that simply have no symbol on them, under a
        connector that looks perfectly configured.

        A position that cannot be named is absent from `held_quantities` AND
        absent from `unreadable()`, because both key off the symbol. That is
        the one combination `reconcile` reads as "the account answered and this
        position is not in it", so the record is RELEASED, which is TERMINAL:
        the brake stops watching a live position, while the portfolio document
        still shows the row under the broker's own key and every display looks
        normal. One omitted line of connector config does that to every
        position on the connector, silently.

        `place_order` and `cancel_order` require nothing (they are written, not
        read), so a map for either parses with no `fields` at all.
        """
        broken = []
        for name in sorted(v):
            missing = sorted(set(CAPABILITIES[name].required) - set(v[name].fields))
            if missing:
                broken.append(f"{name} declares no path for {', '.join(missing)}")
        if broken:
            cid = (info.data or {}).get("id", "?")
            raise ValueError(
                f"connector {cid!r} cannot read what it maps: {'; '.join(broken)}")
        return v

    @model_validator(mode="after")
    def _default_the_account_key(self):
        # The map says where the account goes; check_accounts hunts for it in
        # arbitrary args on the raw call_connector path. Different jobs, and
        # they must not disagree about the key name, so the map inherits it.
        #
        # Copy rather than mutate: a caller may pass an already-constructed
        # Capability instance and reuse that same object across two specs, and
        # pydantic keeps the identical reference rather than copying it. An
        # in-place write to cap.args would then leak the FIRST spec's account
        # key into the SECOND spec's map, since both specs would be looking at
        # the same dict. Building a new Capability keeps each spec's map its
        # own.
        names = self.guardrails.accounts.arg_names
        if not names:
            return self
        for name, cap in self.capabilities.items():
            if "account" in CAPABILITIES[name].args and "account" not in cap.args:
                self.capabilities[name] = cap.model_copy(
                    update={"args": {**cap.args, "account": names[0]}})
        return self

    @property
    def is_mcp(self) -> bool:
        return self.kind in KIND_TO_TRANSPORT

    @property
    def transport(self) -> str | None:
        return KIND_TO_TRANSPORT.get(self.kind)

    @property
    def capability_names(self) -> list[str]:
        return sorted(self.capabilities)

    def capability(self, name: str) -> Capability:
        """This connector's map for `name`, or refuse.

        Fail closed: a connector that never declared how to do something cannot
        be asked to guess, and the agent is told which connector is missing
        which entry.
        """
        cap = self.capabilities.get(name)
        if cap is None:
            raise CapabilityError(
                f"connector {self.id!r} declares no {name!r} capability "
                f"(declares: {', '.join(self.capability_names) or 'none'})")
        return cap

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
