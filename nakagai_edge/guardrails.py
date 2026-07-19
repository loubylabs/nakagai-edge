"""The policy gate between an upstream agent and a downstream MCP server.

Every proxied call passes through `evaluate()` before a byte reaches the
downstream. Pure functions, no I/O, so the whole trading-safety story is
testable without a network or a broker.

Design posture: **fail closed.** A tool nobody classified is treated as a write,
and writes are denied unless the human explicitly set `allow_writes: true` on
that connector. Denials name the guardrail that fired so an agent can explain
itself to its operator rather than silently retrying.
"""

from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any, Literal

from nakagai_edge.config import ConnectorSpec

Decision = Literal["allow", "deny", "approve"]


@dataclass(frozen=True)
class Verdict:
    decision: Decision
    reason: str = ""
    is_write: bool = False

    @property
    def allowed(self) -> bool:
        return self.decision == "allow"

    def to_dict(self) -> dict:
        return {"decision": self.decision, "reason": self.reason, "is_write": self.is_write}


def _matches_any(name: str, globs: list[str]) -> bool:
    return any(fnmatch(name, g) for g in globs)


def classify_write(spec: ConnectorSpec, tool: str, read_only_hint: bool | None = None) -> bool:
    """Is `tool` state-changing? Explicit config beats the server's own hint,
    which beats verb prefixes, which beats `unknown_is_write`."""
    g = spec.guardrails
    if _matches_any(tool, g.read_only_tools):
        return False
    if _matches_any(tool, g.write_tools):
        return True
    if read_only_hint is True:
        return False
    if read_only_hint is False:
        return True
    if any(tool.startswith(p) for p in g.write_prefixes):
        return True
    return g.unknown_is_write


def iter_arg_values(args: Any, depth: int = 0):
    """Yield (key, value) pairs from the top level and one level down.

    Downstream tools nest account ids one level at a time (`{"order":
    {"account_number": ...}}`); deeper than that we stop rather than pretend to
    have inspected the whole tree.

    Public: `check_accounts` walks this to find account ids in the call args.
    """
    if not isinstance(args, dict) or depth > 1:
        return
    for k, v in args.items():
        yield k, v
        if isinstance(v, dict):
            yield from iter_arg_values(v, depth + 1)
        elif isinstance(v, list):
            for item in v:
                yield from iter_arg_values(item, depth + 1)


def check_accounts(spec: ConnectorSpec, args: dict, is_write: bool = False,
                   enforce_account_presence: bool = True) -> str:
    """Return a denial reason if the call names an account outside its tier,
    or (for a write) names no account at all while accounts are tiered.

    `allow` accounts pass everything; `read` accounts pass only reads, so the
    caller must say what kind of call this is. evaluate() classifies the write
    before it checks accounts, so the flag is already in hand there.

    An account-less write would reach the broker and land on ITS default
    account, which may be exactly the one the read tier walls off. So when
    tiers exist, a write must name its account or be refused
    (`require_account_arg`, default true; set false for a broker whose write
    tools identify their target another way, an order id say). An account key
    whose value is not a str/int scalar counts as not named: fail closed.
    `annotate_tools` probes tools with empty args, so it alone skips the
    presence rule via `enforce_account_presence`.
    """
    accounts = spec.guardrails.accounts
    if not accounts.allow and not accounts.read:
        return ""
    permitted = set(accounts.allow)
    if not is_write:
        permitted |= set(accounts.read)
    named = False
    for key, value in iter_arg_values(args):
        if key in accounts.arg_names and isinstance(value, (str, int)):
            named = True
            if str(value) in permitted:
                continue
            if is_write and str(value) in accounts.read:
                return (f"account {value!r} is read-only for connector "
                        f"{spec.id!r}: it may be viewed, never acted on")
            return (f"account {value!r} is not in the allowlist for "
                    f"connector {spec.id!r} "
                    f"(allowed: {', '.join(accounts.allow + accounts.read)})")
    if (is_write and not named and enforce_account_presence
            and accounts.require_account_arg):
        return (f"write names no account for connector {spec.id!r}: accounts "
                f"are tiered here, so a write must say which account it acts on "
                f"instead of falling to the broker's default "
                f"(allowed: {', '.join(accounts.allow)})")
    return ""


def evaluate(spec: ConnectorSpec, tool: str, args: dict,
             read_only_hint: bool | None = None, *,
             enforce_account_presence: bool = True) -> Verdict:
    """Decide whether `tool` may run on `spec` with `args`.

    Gates run in order and the first failure wins, so the reason an agent sees
    is the outermost rule it broke.
    """
    g = spec.guardrails

    if not spec.enabled:
        return Verdict("deny", f"connector {spec.id!r} is disabled")

    if _matches_any(tool, g.tools.deny):
        return Verdict("deny", f"tool {tool!r} is denied by connector {spec.id!r}")
    if g.tools.allow and not _matches_any(tool, g.tools.allow):
        return Verdict("deny",
                       f"tool {tool!r} is not in the allowlist for connector {spec.id!r}")

    is_write = classify_write(spec, tool, read_only_hint)
    if is_write and not g.allow_writes:
        return Verdict("deny",
                       f"tool {tool!r} modifies state and connector {spec.id!r} is "
                       f"read-only (set allow_writes: true to permit it)",
                       is_write=True)

    if reason := check_accounts(spec, args, is_write,
                                enforce_account_presence=enforce_account_presence):
        return Verdict("deny", reason, is_write=is_write)

    if _matches_any(tool, g.approvals.require_for):
        return Verdict("approve",
                       f"tool {tool!r} on connector {spec.id!r} requires human approval",
                       is_write=is_write)

    return Verdict("allow", is_write=is_write)


def annotate_tools(spec: ConnectorSpec, tools: list[dict]) -> list[dict]:
    """Tag each downstream tool with Nakagai's verdict, so an agent can plan
    without probing the guardrails by trial and error.

    `tools` are the downstream's own descriptors (name/description/inputSchema);
    we pass those through verbatim and add a `policy` block.
    """
    annotated = []
    for tool in tools:
        name = tool.get("name", "")
        hint = (tool.get("annotations") or {}).get("readOnlyHint")
        verdict = evaluate(spec, name, {}, read_only_hint=hint,
                           enforce_account_presence=False)
        annotated.append({
            **tool,
            "policy": {
                "allowed": verdict.decision in ("allow", "approve"),
                "is_write": verdict.is_write,
                "requires_approval": verdict.decision == "approve",
                "reason": verdict.reason,
            },
        })
    return annotated
