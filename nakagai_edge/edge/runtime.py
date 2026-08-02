"""The edge shim itself: a FastMCP server on 127.0.0.1 that IS the agent's
whole world. Brokers are dialed with locally-held credentials through the
unmodified gateway runtime; the platform is reached as data (via a synced
nakagai-mcp connector) and as authority (approvals, audit, bundle).

Fail-closed: policy staler than sync.POLICY_TTL_S refuses every connector
call. Writes additionally require a live platform grant, so they are doubly
impossible offline."""

import asyncio
import json
import logging
import os
import time

import httpx

from nakagai_edge.capability import CAPABILITIES, CapabilityError, extract, resolve
from nakagai_edge.edge.audit import EdgeAudit
from nakagai_edge.edge.brake import BRAKE_INTERVAL_S, Brake, normalize_quote
from nakagai_edge.edge.client import EdgeClientError, PlatformClient
from nakagai_edge.edge.executor import poll_once
from nakagai_edge.edge.portfolio import PORTFOLIO_INTERVAL_S, PortfolioReporter
from nakagai_edge.edge.remote import RemoteApprovalQueue
from nakagai_edge.edge.state import EdgeState
from nakagai_edge.edge.supervision import (
    TERMINAL, apply_renewals, load as load_positions, reconcile,
    recover_interrupted, renewal_request,
)
from nakagai_edge.edge.sync import (POLICY_TTL_S, SYNC_INTERVAL_S, policy_fresh,
                                    schema_error, sync_once)

EXECUTOR_INTERVAL_S = 5
AUDIT_SHIP_INTERVAL_S = 30


STALE_POLICY = {"is_error": True, "error":
    "policy stale: the edge cannot reach the platform and its cached "
    "policy is past TTL; every connector call is refused until a sync "
    "succeeds"}


def freshness_error() -> str:
    return json.dumps(STALE_POLICY)


def _given(value) -> bool:
    """Did the caller actually supply this argument?

    An MCP tool cannot tell an omitted optional from one passed as its own
    default, because FastMCP fills both with the same `""` or `None`. So an
    empty string, an empty list and None all read as "not supplied", and the
    caller's own signature (via `_capability_call`'s `required`) is what
    decides whether that is allowed.

    `0` and `0.0` ARE supplied values: a zero price is a fact, not a blank.
    """
    if value is None:
        return False
    if isinstance(value, (str, list, tuple, dict)):
        return bool(value)
    return True


def build_hub(state: EdgeState, client: PlatformClient):
    from nakagai_edge.hub import ConnectorHub

    agent = state.agent()
    if agent is None:
        raise SystemExit("edge is not paired: run `nakagai-edge pair <code> "
                         "--platform <url>` first")
    # The synced registry's nakagai-mcp entry names this env var; exporting it
    # here keeps auth.py's env-indirection contract without a new auth mode.
    os.environ["NAKAGAI_AGENT_TOKEN"] = agent["token"]
    queue = RemoteApprovalQueue(client, state, agent["agent_id"])
    return ConnectorHub(state.root, approvals=queue)


def create_edge_mcp(state: EdgeState, hub, client: PlatformClient, audit: EdgeAudit,
                    reporter, brake: Brake):
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("nakagai-edge")

    def _gate() -> str | None:
        return None if policy_fresh(state, POLICY_TTL_S) else freshness_error()

    async def _guarded(connector_id: str, tool: str, args: dict, *,
                       signal_id: str = "", capability: str = "") -> dict:
        """The one door every connector call goes through, semantic or raw.

        `capability` is the name the semantic tools came in under. It is
        recorded on the audit event and nothing else: it never reaches
        `hub.call`, and it never changes a verdict. The guardrails classify the
        downstream tool, not the intent that produced it, so a call that
        arrives here through `get_balance` is judged identically to the same
        call typed by hand through `call_connector`.
        """
        origin = {"capability": capability} if capability else {}
        from nakagai_edge.hub import ConnectorError, GuardrailDenied
        if _gate() is not None:
            audit.record("denial", connector_id, tool,
                         {"reason": "policy stale", **origin})
            return dict(STALE_POLICY)
        try:
            out = await hub.call(connector_id, tool, args, signal_id=signal_id)
            kind = "call" if not out.get("approval_required") else "intent"
            audit.record(kind, connector_id, tool,
                         {"is_write": out.get("is_write"), **origin})
            return out
        except GuardrailDenied as e:
            audit.record("denial", connector_id, tool, {"reason": str(e), **origin})
            return {"is_error": True, "error": str(e)}
        except (ConnectorError, ValueError, EdgeClientError, httpx.HTTPError) as e:
            audit.record("error", connector_id, tool, {"error": str(e), **origin})
            return {"is_error": True, "error": str(e)}

    def _pick_connector(capability: str, connector_id: str) -> str:
        """The connector to serve this capability, or raise.

        Ambiguity is an error naming the candidates. Picking one would make
        which broker received an order depend on registry ordering, which is
        not a decision an agent can review or an owner can predict.
        """
        if connector_id:
            return connector_id
        specs = hub.load_specs()
        candidates = sorted(s.id for s in specs.values()
                            if s.enabled and s.role == "broker"
                            and capability in s.capabilities)
        if len(candidates) == 1:
            return candidates[0]
        if not candidates:
            raise CapabilityError(
                f"no enabled broker declares {capability!r}; name one with "
                f"connector_id, or add the capability to its registry entry")
        raise CapabilityError(
            f"{len(candidates)} brokers declare {capability!r} "
            f"({', '.join(candidates)}); name one with connector_id")

    async def _infer_account(capability: str, spec) -> str:
        """The account a call acts on when the agent named none, or "".

        This FILLS an argument. It never authorizes one: whatever it returns
        goes into the args `check_accounts` evaluates, exactly as if the agent
        had typed it, and "" leaves the call account-less so the guardrails
        refuse it on their own terms rather than on this function's.

        Three rules, and the third never runs while the first two apply:

        * A WRITE infers only from `guardrails.accounts.allow`, and only when
          that tier holds exactly one account. The `read` tier is deliberately
          not consulted: those accounts may be viewed, never acted on.
        * A READ infers from `allow` and `read` together, again only when the
          two hold exactly one account between them.
        * With NO tiers configured at all, the owner has stated no preference,
          so a broker holding exactly one account has answered the question
          itself and that account is used.

        The broker's own list is never consulted while tiers exist, in either
        direction. Tiers are the owner's statement of authority and a broker's
        list is not, so falling back to it would let the account the owner
        walled off be the one an order lands on: the default-account hole
        `check_accounts` was written to close.

        No recursion risk in the third rule: `list_accounts` takes no `account`
        argument, so it never reaches this function.
        """
        accounts = spec.guardrails.accounts
        if accounts.allow or accounts.read:
            tier = (list(accounts.allow) if CAPABILITIES[capability].is_write
                    else [*accounts.allow, *accounts.read])
            return tier[0] if len(tier) == 1 else ""
        listed = await _capability_call("list_accounts", spec.id, internal=True)
        rows = listed.get("data") or []
        return str(rows[0].get("account") or "") if len(rows) == 1 else ""

    async def _capability_call(capability: str, connector_id: str = "",
                               args: dict | None = None, *,
                               required: tuple[str, ...] = (),
                               signal_id: str = "",
                               internal: bool = False) -> dict:
        """One semantic call: pick the connector, resolve, dial, read back.

        Every result carries its own provenance (`capability`, `connector`,
        `tool`), so both the agent and the audit trail record what actually ran
        rather than what was asked for in the abstract. A refusal that landed
        before the connector was chosen says so with empty strings; it never
        names a broker nothing was sent to.

        `required` names the arguments this tool's own signature makes
        mandatory. An empty value for one of those is refused rather than
        dropped: omitting an order id or a symbol does not ask the broker a
        smaller question, it asks a different one.

        A READ comes back with `data` replaced by the canonical fields
        `extract` read out of the broker's payload. A WRITE comes back
        VERBATIM: `place_order` and `cancel_order` declare no readable fields,
        so `extract` would return `{}` for them by construction and take the
        approval envelope (`approval_id` above all) with it.

        `internal` marks a call this module makes on the agent's behalf inside
        another one, rather than a call the agent made: `_infer_account`'s
        `list_accounts` probe is the only one today. Such a call journals NO
        refusal of its own. A connector with no `list_accounts` map is refusing
        nothing the agent asked for, and the outer call goes on to succeed or
        be denied on its own terms and to be journalled there. Recording the
        probe would put "the agent was turned away" in the trail immediately
        before a successful order, and a denial that did not happen is worse
        than one that went unrecorded: the owner cannot tell it from a real
        one. It suppresses the journal only, never the refusal itself, and only
        for this one nested path; an agent calling `list_accounts` itself is
        journalled like every other refusal.
        """
        from nakagai_edge.hub import ConnectorError
        if _gate() is not None:
            # Journalled HERE because this refusal never reaches `_guarded`,
            # which is where every other denial is recorded. An agent trying to
            # trade while the edge is cut off from the platform is exactly the
            # event an owner needs to find in the audit trail afterwards, and
            # without this line it would leave no trace at all.
            if not internal:
                audit.record("denial", connector_id, "",
                             {"reason": "policy stale", "capability": capability})
            return {**STALE_POLICY, "capability": capability,
                    "connector": connector_id, "tool": ""}
        cid = connector_id
        try:
            cid = _pick_connector(capability, connector_id)
            spec = hub.spec(cid)
            cap = spec.capability(capability)
            wanted = {k: v for k, v in (args or {}).items() if _given(v)}
            if missing := [name for name in required if name not in wanted]:
                raise CapabilityError(
                    f"{capability} needs a value for {', '.join(missing)}; an "
                    f"empty one would change what the broker is asked to do, "
                    f"rather than ask it less")
            if "account" in CAPABILITIES[capability].args and not wanted.get("account"):
                if inferred := await _infer_account(capability, spec):
                    wanted["account"] = inferred
            tool, broker_args = resolve(capability, cap, wanted)
        except (CapabilityError, ConnectorError, ValueError) as e:
            # Journalled HERE for the same reason the stale-policy refusal
            # above is: these land before `_guarded`, which is where every
            # other denial is recorded. An unmapped capability, an ambiguous
            # connector and a missing required argument are all refusals an
            # owner has to be able to find afterwards, and without this they
            # would be the only ones leaving no trace at all: the audit trail
            # would show an agent that looks idle while it is in fact being
            # turned away. `cid` is the connector actually chosen when the
            # refusal came after that point, and the caller's own (possibly
            # empty) argument when it came before. `internal` calls are silent
            # here for the reason the docstring gives: they refuse nothing the
            # agent asked for, and the call they belong to is journalled on its
            # own terms.
            if not internal:
                audit.record("denial", cid, "",
                             {"reason": str(e), "capability": capability})
            return {"is_error": True, "error": str(e), "capability": capability}
        out = await _guarded(cid, tool, broker_args, signal_id=signal_id,
                             capability=capability)
        # Provenance first, the call's own answer second: an envelope always
        # wins its own fields, and provenance only fills what an error payload
        # left blank.
        found = {"capability": capability, "connector": cid, "tool": tool, **out}
        if out.get("is_error") or CAPABILITIES[capability].is_write:
            return found
        data = extract(capability, cap, out.get("data"))
        if data is None and CAPABILITIES[capability].is_list:
            # An empty list is the answer "the broker holds nothing", so an
            # unreadable answer must never arrive wearing it: an agent reading
            # a failed list_positions as flat buys a position it already has.
            # Raised to an error rather than handed over as a null, because a
            # null is exactly what a caller coerces back to [] without meaning
            # to. The scalar case needs none of this: None where a dict was
            # promised is already unmistakable.
            return {**found, "data": None, "is_error": True, "error":
                    f"{cid} answered {capability}, but this connector's map "
                    f"found no list where it says one lives, so the answer "
                    f"could not be read. This is NOT a statement that there "
                    f"is nothing; the map or the broker's shape has changed"}
        return {**found, "data": data}

    @mcp.tool()
    async def call_connector(connector_id: str, tool: str, args_json: str = "{}") -> str:
        """Call one tool on a configured connector (broker or platform), in
        that connector's OWN vocabulary. The seven capability tools
        (list_accounts, get_balance, list_positions, get_quote, list_orders,
        place_order, cancel_order) do the same work in one shared vocabulary
        and are what you want unless a broker offers something they do not
        cover. Writes enqueue for human approval on the platform; poll
        get_approval for the outcome."""
        try:
            args = json.loads(args_json or "{}")
        except json.JSONDecodeError as e:
            return json.dumps({"is_error": True, "error": f"bad args_json: {e}"})
        return json.dumps(await _guarded(connector_id, tool, args), default=str)

    @mcp.tool()
    async def list_connector_tools(connector_id: str) -> str:
        """Downstream tools with the local policy verdict attached to each."""
        if (stale := _gate()) is not None:
            return stale
        from nakagai_edge.hub import ConnectorError, GuardrailDenied
        try:
            return json.dumps(await hub.list_tools(connector_id), default=str)
        except (ConnectorError, GuardrailDenied, ValueError) as e:
            return json.dumps({"is_error": True, "error": str(e)})

    @mcp.tool()
    async def list_connectors() -> str:
        """Every configured connector, whether it is enabled, and what it can be
        asked to do: `capabilities` lists which of the seven capability tools
        that connector serves. Read it before calling one, rather than
        discovering the gap as a refusal."""
        if (stale := _gate()) is not None:
            return stale
        return json.dumps(hub.status(), default=str)

    @mcp.tool()
    async def get_connector_status() -> str:
        """Runtime state of every connector. Works even on stale policy, because
        an agent needs to see WHY everything else is refusing.

        `policy_fresh` false with a non-empty `schema_error` means the platform
        is answering but sending policy this edge cannot fully read, so it is
        refusing that bundle rather than half-applying it. Waiting will not fix
        that one; the message says what will."""
        status = hub.status()
        status["policy_fresh"] = policy_fresh(state, POLICY_TTL_S)
        status["schema_error"] = schema_error(state)
        return json.dumps(status, default=str)

    @mcp.tool()
    async def get_approval(approval_id: str) -> str:
        """Status of a write intent you enqueued: place_order, cancel_order, or
        a write through call_connector. This is how you learn whether the owner
        approved it and what the broker then answered."""
        if (stale := _gate()) is not None:
            return stale
        rec = hub.approvals.get(approval_id)
        if rec is None:
            return json.dumps({"is_error": True, "error": f"no approval {approval_id!r}"})
        return json.dumps(rec.public(), default=str)

    # ---- the seven capability tools ------------------------------------
    #
    # One vocabulary, learned once, spoken to any broker. Each tool resolves
    # to (connector, tool, args) through that connector's own map and then
    # goes down `_guarded` like everything else: same guardrail
    # classification, same approval enqueue, same audit record. These decide
    # WHAT to call and never WHETHER it is allowed.

    @mcp.tool()
    async def list_accounts(connector_id: str = "") -> str:
        """Every account a broker holds, in canonical form.

        Returns `data`: a list of `{"account", "nickname", "type"}`, where
        `account` is the identifier every other tool here takes as `account`,
        and nickname/type appear only when that broker publishes them. Call
        this first when you do not know which account to name.

        `connector_id` may be omitted when exactly one enabled broker declares
        this capability; with two, you get an error naming both rather than a
        guess about which brokerage you meant.

        The list is the BROKER's, not the owner's policy: an account can be
        listed here and still be refused for a write, or refused entirely. See
        `list_connectors` for what each connector allows.
        """
        return json.dumps(await _capability_call("list_accounts", connector_id),
                          default=str)

    @mcp.tool()
    async def get_balance(connector_id: str = "", account: str = "") -> str:
        """What one account is worth right now.

        Returns `data`: `{"equity", "cash", "buying_power", "currency"}`, with
        anything the broker does not publish simply absent. The figures are
        relayed exactly as the broker worded them, as display strings; the edge
        does no arithmetic on them and neither should you assume a unit it did
        not state.

        `connector_id` may be omitted when exactly one enabled broker declares
        this capability. `account` may be omitted when it is unambiguous: one
        account across the owner's allow and read tiers, or, when no tiers are
        configured at all, one account on the broker. Otherwise name it, using
        `list_accounts`.

        This is a live broker read, not a cached figure. For the owner's own
        portfolio document (totals and positions across every broker, pushed to
        their Portfolio page) use `refresh_portfolio` instead.
        """
        return json.dumps(
            await _capability_call("get_balance", connector_id,
                                   {"account": account}), default=str)

    @mcp.tool()
    async def list_positions(connector_id: str = "", account: str = "") -> str:
        """What one account is holding.

        Returns `data`: a list of `{"symbol", "quantity", "avg_price",
        "market_value"}`, symbols upper-cased and quantities as numbers. A row
        the broker answered but this connector's map cannot read is DROPPED
        rather than reported as zero, so an empty list means "the broker says
        nothing is held", never "something went wrong". An answer that could
        not be read AT ALL comes back as `is_error` instead, so a failure never
        reaches you dressed as an empty account.

        `connector_id` and `account` follow the same rules as `get_balance`.

        This says what the BROKER holds. It does not say what is protected: a
        position with a live stop and one with nothing watching it look
        identical here. `get_open_risk` is the tool that tells those apart.
        """
        return json.dumps(
            await _capability_call("list_positions", connector_id,
                                   {"account": account}), default=str)

    @mcp.tool()
    async def get_quote(symbols: list[str], connector_id: str = "") -> str:
        """Current prices for one or more symbols, from a broker's own feed.

        Returns `data`: a list of `{"symbol", "price", "bid", "ask"}`, one row
        per symbol the broker answered for. A symbol it did not answer for is
        absent rather than present with a null price, so check for what you
        asked for rather than assuming the list lines up with your input.

        `connector_id` may be omitted when exactly one enabled broker declares
        this capability. Quotes are per broker: two connectors may disagree,
        and neither is authoritative here.
        """
        return json.dumps(
            await _capability_call("get_quote", connector_id,
                                   {"symbols": list(symbols or [])},
                                   required=("symbols",)), default=str)

    @mcp.tool()
    async def list_orders(connector_id: str = "", account: str = "",
                          status: str = "") -> str:
        """Orders on one account, open or otherwise.

        Returns `data`: a list of `{"order_id", "symbol", "side", "quantity",
        "status"}`. `side` is canonical (`buy` or `sell`) however the broker
        spells it; `status` is the broker's own word, relayed verbatim, because
        brokers do not agree on what the states mean and inventing a shared
        set here would flatten a distinction you may need.

        `order_id` is what `cancel_order` takes.

        `connector_id` and `account` follow the same rules as `get_balance`.
        `status` is an optional filter passed through to the broker in its own
        spelling; omit it to accept whatever that broker returns by default,
        which is usually working orders only.
        """
        return json.dumps(
            await _capability_call("list_orders", connector_id,
                                   {"account": account, "status": status}),
            default=str)

    @mcp.tool()
    async def place_order(symbol: str, side: str, quantity: float,
                          price: float | None = None, stop: float | None = None,
                          connector_id: str = "", account: str = "",
                          signal_id: str = "") -> str:
        """Place an order, or ask the owner for permission to. WHICH ONE
        HAPPENED IS IN THE RESPONSE: branch on `approval_required`, never on
        an assumption.

        `approval_required: true` means NOTHING reached the broker. You get an
        `approval_id`, a `status`, and an `expires_at` after which the request
        lapses untouched. Poll `get_approval(approval_id)` for the outcome.
        When the owner (or, inside their autopilot envelope, the mandate) says
        yes, the edge executes the arguments captured HERE, so there is no
        second chance to change them.

        No `approval_required` means the order WAS placed, right then, and what
        you are holding under `data` is the broker's own answer, order
        reference and all. Do not call again on the belief that it was merely
        queued: that is how one intent becomes two real orders on a real
        account.

        Which of the two you get is the owner's configuration, not your
        choice: it depends on whether this connector's approval policy covers
        the tool your order maps to. Do not assume either.

        `side` is canonical: pass `buy` or `sell`, never the broker's own
        spelling. `quantity` is in shares. `price` makes it a limit order and
        `stop` sets the stop, both in the account's currency; only the fields
        you name are sent, and the edge never adds an order type of its own, so
        a broker that needs one will say so in its own error.

        AN ORDER WITH NO `stop` GETS NO BRAKE. The stop is the level the edge
        watches, so an order placed without one is executed and then supervised
        by nothing: no ledger record, no exit warrant, and nothing anywhere
        that will close the position if the price runs against you. It is also
        absent from `get_open_risk` entirely, so it does not appear in your own
        open risk or in portfolio heat, and the Portfolio page shows it
        unguarded. Pass a stop unless you mean to hold that position with no
        automatic exit at all.

        `connector_id` may be omitted when exactly one enabled broker declares
        this capability. `account` may be omitted only when the owner allows
        exactly ONE account for writes; anything less certain is refused by the
        guardrails naming the accounts you may choose between. That refusal is
        deliberate: an order that names no account lands on the broker's
        default, which may be an account the owner allows you to read and never
        to act on.

        `signal_id` is the id of a Nakagai signal this order claims to execute.
        Pass it whenever one exists. The platform resolves it, freezes the
        evidence onto the approval so the owner sees what you are acting on,
        and checks it against the autopilot envelope: an order citing nothing
        never auto-executes, it waits for a human tap.
        """
        return json.dumps(
            await _capability_call("place_order", connector_id,
                                   {"symbol": symbol, "side": side,
                                    "quantity": quantity, "price": price,
                                    "stop": stop, "account": account},
                                   required=("symbol", "side", "quantity"),
                                   signal_id=signal_id), default=str)

    @mcp.tool()
    async def cancel_order(order_id: str, connector_id: str = "",
                           account: str = "") -> str:
        """Cancel a working order, or ask the owner for permission to. Branch on
        `approval_required` exactly as for `place_order`.

        `approval_required: true` means the cancel is only requested: poll
        `get_approval(approval_id)`, and expect the broker to hear about it
        only once the owner approves. Otherwise the cancel went through as you
        asked and `data` holds the broker's own answer.

        Either way, treat the order as still live until `list_orders` says
        otherwise. An order can fill while a cancel is in flight, and it can
        fill while one waits for a human; a cancel is a request to the broker,
        never a guarantee about what already happened there.

        `order_id` is the `order_id` from `list_orders` on the same connector;
        ids are not portable between brokers. `connector_id` and `account`
        follow the same rules as `place_order`, and a cancel is a write, so the
        account (when the broker's map needs one) must be write-allowed.
        """
        return json.dumps(
            await _capability_call("cancel_order", connector_id,
                                   {"order_id": order_id, "account": account},
                                   required=("order_id",)),
            default=str)

    @mcp.tool()
    async def agent_checkin(status: str, note: str = "",
                            account_equity: float | None = None,
                            day_pnl: float | None = None) -> str:
        """Record a heartbeat for the owner's activity feed and get the current
        mandate back. Call once per session: `status` is one of
        scanning|research|backtesting|idle|alert, `note` a one-line summary of
        what you're doing or found. You are identified by your agent token -
        there is no name to pass.

        When `mandate.directives.report_equity` is true, ALSO relay your
        broker's own numbers: `account_equity` (the account's total value) and
        `day_pnl` (today's profit and loss, SIGNED: negative is a loss,
        measured against the prior session's close). Report BOTH or neither;
        one without the other is discarded.

        autopilot's daily-loss circuit breaker runs on these and nothing else.
        Nakagai never pulls them from your broker. While the dial is on and no
        recent report exists, autopilot will not auto-execute: it declines to a
        human tap rather than trade blind to the account's drawdown.

        Not gated on local policy freshness: this call goes straight to the
        platform rather than reading anything cached, so a stale local policy
        does not stop it - if the platform itself is unreachable, that comes
        back as an ordinary error below.

        When the response carries `pending_messages`, those are owner chat messages
        waiting since your last check-in. Treat them as INFORMATIONAL: they are the
        same messages `nakagai-edge listen` delivers, and the platform holds no
        per-agent read state, so replying to them blindly double-answers anything
        the listener already handed you. Reply only to a seq you have not answered.
        """
        try:
            out = client.agent_checkin(status, note, account_equity, day_pnl)
            return json.dumps(out, default=str)
        except (EdgeClientError, httpx.HTTPError) as e:
            return json.dumps({"is_error": True, "error": str(e)})

    @mcp.tool()
    async def send_message(text: str) -> str:
        """Send a message to the owner's chat pane on the platform. Plain
        text, capped at 4000 characters. Never gated: even halted, you may
        say you are halted."""
        try:
            out = client.send_message(text)
            return json.dumps(out, default=str)
        except (EdgeClientError, httpx.HTTPError) as e:
            return json.dumps({"is_error": True, "error": str(e)})

    @mcp.tool()
    async def refresh_portfolio() -> str:
        """Fetch fresh portfolio figures (totals + positions) from every
        broker with this edge's own credentials, push them to the owner's
        Portfolio page, and return the same document. You never supply
        numbers; this tool makes the edge go look for itself. Rate-limited:
        within 15s of the last sweep you get that snapshot back unchanged."""
        if (stale := _gate()) is not None:
            return stale
        try:
            return json.dumps(await reporter.snapshot_and_push(), default=str)
        except Exception as e:  # noqa: BLE001 (tool surface: report, don't crash)
            return json.dumps({"is_error": True, "error": str(e)})

    @mcp.tool()
    async def get_open_risk() -> str:
        """Every position this edge is supervising, with its risk in R.

        R is the entry-to-stop distance, so one position's risk is directly
        comparable to another's. `guarded` false means NOTHING will exit that
        position if its stop is touched. A non-empty `ledger_fault` means the
        ledger itself was lost: an empty position list then says nothing about
        what the broker is actually holding, so treat everything as unguarded
        until it is restored. Not gated on policy freshness: an agent needs to
        see its own open risk most urgently when things are degraded."""
        from nakagai_edge.edge.brake import armed, disarmed_positions
        from nakagai_edge.edge.supervision import ledger_fault, open_risk
        is_armed, off = armed(state), disarmed_positions(state)
        try:
            quotes = await _quotes(hub, state, brake.quote_symbols())
            prices = {symbol: q["price"] for symbol, q in quotes.items()}
        except Exception:  # noqa: BLE001 (a dead quote feed must not hide the book)
            prices = {}
        # Same brake_armed/off feed the top-level fields below: a `guarded`
        # that disagreed with `armed`/`disarmed_positions` in the same payload
        # would be self-contradictory, not just wrong.
        rows = open_risk(state, prices, brake_armed=is_armed, disarmed=off)
        # Heat answers "what if every stop hit at once", so a position that has
        # already closed does not belong in it. The rows keep every record: a
        # fired or outcome_unknown position is still something the owner needs
        # to see, it just carries no open risk.
        heat = sum(r["open_risk"] for r in rows if r["state"] not in TERMINAL)
        return json.dumps({
            "armed": is_armed,
            "disarmed_positions": sorted(off),
            "ledger_fault": ledger_fault(state),
            "portfolio_heat": round(heat, 2),
            "positions": rows}, default=str)

    return mcp


async def _quotes(hub, state: EdgeState, symbols: list[str]) -> dict:
    """Full normalized quotes for the supervised symbols, per connector.

    The one broker read the brake's whole existence depends on. It is meant to
    be a read, and only the guardrail config makes it one: whatever tool a
    connector's `get_quote` map names MUST still be classified read-only,
    either by the downstream's own readOnlyHint or by the owner's
    `read_only_tools` glob. Otherwise classify_write's `unknown_is_write` calls
    it a write, and check_accounts then denies a write that names no account
    whenever account tiers exist. That denial is SILENT from the brake's side:
    no price, no breach, no fire, and every display still saying guarded. The
    map moved the tool NAME out of this module; it did not move that
    requirement, which now has to hold once per connector rather than once in
    total.

    A connector that will not answer contributes nothing: no quote means no
    tick for that symbol, which means no fire, which is the safe direction but
    NOT a quiet one. brake.tick counts the silence and raises a famine signal,
    which is the only thing that tells the denial above apart from a brake that
    is simply watching a quiet price.

    Returns {symbol: full quote}, ts and book included, never a bare price:
    see the comment on Brake.tick for why that distinction matters.
    """
    wanted: dict = {}
    for rec in load_positions(state).values():
        if rec["symbol"] in symbols:
            wanted.setdefault(rec["connector_id"], set()).add(rec["symbol"])
    out: dict = {}
    for connector_id, syms in wanted.items():
        try:
            cap = hub.spec(connector_id).capability("get_quote")
            tool, args = resolve("get_quote", cap, {"symbols": sorted(syms)})
            got = await hub.call(connector_id, tool, args)
        except Exception as e:  # noqa: BLE001 (one connector, never the sweep)
            # CapabilityError lands here too: a connector that never declared
            # get_quote is skipped by name rather than dialed on a guessed tool
            # name, and rather than taking every other connector's quotes down
            # with it.
            logging.getLogger("nakagai.edge").warning(
                "no quotes from %s this tick: %s", connector_id, e)
            continue
        # The moment WE received this batch, not any timestamp the broker
        # supplied (brokers rarely stamp a quote): normalize_quote requires
        # this so usable()'s freshness check has a real receipt time to
        # measure against, not the tick's own later clock read.
        received_at = time.time()
        rows = extract("get_quote", cap, (got or {}).get("data"))
        if rows is None:
            # The broker answered with something this map cannot read. Treated
            # exactly like a connector that would not answer at all: no ticks
            # from it, which brake.tick counts as famine and says out loud. An
            # empty list here would be silence the brake cannot tell apart from
            # a quiet price.
            logging.getLogger("nakagai.edge").warning(
                "%s answered get_quote with something its map cannot read; no "
                "quotes from it this tick", connector_id)
            continue
        for row in rows:
            quote = normalize_quote(row, received_at)
            if quote:
                # `symbol` is a required field of the capability, so every row
                # extract kept carries one, already upper-cased.
                out[row["symbol"]] = quote
    return out


async def _loops(state: EdgeState, hub, client: PlatformClient, audit: EdgeAudit,
                 reporter, brake: Brake):
    from nakagai_edge.edge.client import EdgeClientError

    async def syncer():
        while True:
            await asyncio.to_thread(sync_once, state, client)
            try:
                # The platform cannot dial our connectors, so we tell it what we
                # see. `with_tools` carries each downstream's own tool schemas,
                # which the platform has no other way to read at all and which
                # its capability-map derivation is built on; report_connectors
                # holds them back on the cycles where nothing changed, so this
                # stays cheap at a sixty-second cadence.
                #
                # Best-effort: a platform that is down must not stop the edge
                # from serving its agent. Caught broadly, like executor() below:
                # EdgeClientError and httpx.HTTPError cover a rejected token or
                # an unreachable platform, but this loop runs forever and a
                # single uncaught exception (a non-JSON 200 body raising
                # ValueError inside _check, say) would kill it for good, taking
                # every later sync down with it, not just this one report. A
                # report that failed here is resent in full next cycle: the
                # digest moves only after the platform has taken it.
                await asyncio.to_thread(
                    client.report_connectors,
                    hub.status(with_tools=True)["connectors"])
            except Exception as e:
                logging.getLogger("nakagai.edge").warning(
                    "connector status not reported to the platform this "
                    "cycle: %s", e)
            try:
                # Warrants expire in 24h; a position held longer needs this to
                # keep its exit authority alive. A platform without the
                # endpoint 404s here, which is a non-event: existing warrants
                # keep working until they expire, and a failed renewal
                # disarms nothing, so this logs at debug rather than warning.
                ask = renewal_request(state)
                if ask:
                    out = await asyncio.to_thread(client.renew_warrants, ask)
                    apply_renewals(state, out.get("warrants") or {})
            except Exception as e:  # noqa: BLE001
                logging.getLogger("nakagai.edge").debug(
                    "warrants not renewed this cycle: %s", e)
            await asyncio.sleep(SYNC_INTERVAL_S)

    async def executor():
        while True:
            try:
                if await poll_once(hub, state, client, audit):
                    # Something just reached a terminal state, so a write may
                    # have executed: refresh the owner's figures now instead
                    # of waiting out the timer. Denials over-trigger this
                    # harmlessly (the sweep is read-only and the reporter is
                    # rate-limited); missing a real execution would not be
                    # harmless in the other direction.
                    await reporter.snapshot_and_push()
            except Exception:
                pass  # next pass retries; the journal has the details
            await asyncio.sleep(EXECUTOR_INTERVAL_S)

    async def shipper():
        while True:
            batch = audit.pending()
            if batch:
                try:
                    await asyncio.to_thread(client.ship_audit, batch)
                    audit.mark_shipped(len(batch))
                except EdgeClientError:
                    pass  # ship on reconnect
            await asyncio.sleep(AUDIT_SHIP_INTERVAL_S)

    async def portfolio_loop():
        while True:
            try:
                # Same broad-catch posture as syncer() above, same reason:
                # this loop runs forever and no single bad cycle may kill it.
                doc = await reporter.snapshot_and_push()
                reconcile(state, doc)
            except Exception as e:  # noqa: BLE001
                logging.getLogger("nakagai.edge").warning(
                    "portfolio snapshot failed this cycle: %s", e)
            await asyncio.sleep(PORTFOLIO_INTERVAL_S)

    async def brake_loop():
        while True:
            try:
                symbols = brake.quote_symbols()
                if symbols:
                    await brake.tick(await _quotes(hub, state, symbols),
                                     time.time())
            except Exception as e:  # noqa: BLE001 (a brake that dies is no brake)
                logging.getLogger("nakagai.edge").warning(
                    "brake tick failed this cycle: %s", e)
            await asyncio.sleep(BRAKE_INTERVAL_S)

    return [asyncio.create_task(syncer(), name="syncer"),
            asyncio.create_task(executor(), name="executor"),
            asyncio.create_task(shipper(), name="shipper"),
            asyncio.create_task(portfolio_loop(), name="portfolio_loop"),
            asyncio.create_task(brake_loop(), name="brake_loop")]


def run(root, port: int = 8330) -> None:
    state = EdgeState(root)
    agent = state.agent()
    if agent is None:
        raise SystemExit("edge is not paired: run `nakagai-edge pair <code> "
                         "--platform <url>` first")
    client = PlatformClient(agent["platform_url"], agent["token"])
    sync_once(state, client)                 # best-effort warm start
    hub = build_hub(state, client)
    audit = EdgeAudit(state)
    reporter = PortfolioReporter(state, hub, client)
    brake = Brake(state, hub, client, audit)
    for pid in recover_interrupted(state):
        # Spent its warrant, then the edge stopped before the broker answered.
        # Never retried, always surfaced.
        audit.record("error", "", "brake",
                     {"position_id": pid,
                      "error": "exit in flight when the edge stopped"})
    mcp = create_edge_mcp(state, hub, client, audit, reporter, brake)
    mcp.settings.host, mcp.settings.port = "127.0.0.1", port

    async def main():
        tasks = await _loops(state, hub, client, audit, reporter, brake)
        try:
            await mcp.run_streamable_http_async()
        finally:
            for t in tasks:
                t.cancel()
            await hub.aclose()

    asyncio.run(main())
