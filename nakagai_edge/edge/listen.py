"""Hold the platform's live channel open and emit its events as lines.

The agent is turn-based and cannot be pushed to, so something local has to hold
the line and turn an arriving event into a wake-up. That is this module.

Two classes of line come out, and the difference is the whole contract. The
platform marks reply authority with the server-authored `response_required`
field after routing each event. Everything else, a plain signal, a market
event, an approval decision, a mandate change, is context the agent absorbs
silently: the scanner writes hundreds of signals and events a session and
answering each one would bury the owner's conversation in their own pane. A
market event names what fired in `detector` and nothing about direction or size
of trade, because it authorizes nothing.
RENDERERS below decides what is delivered at all, and is a security boundary.

Four design points carry most of the weight:

* The hold is CONTINUOUS. Presence on the platform is `holds > 0`
  (nakagai/channel.py), true only while someone is actively holding a long poll,
  so a listener that exited in order to notify would flicker the owner's badge
  off between every message. The HTTP hold stays open; stdout carries the signal.
* The listener is PACED. Every /api/agent/* route shares one per-agent token
  bucket, so an unpaced drain here does not degrade chat, it 429s the trade
  executor and the stop-loss brake: a granted approval then reads as "no such
  approval" and a stop-loss execution report is dropped for good.
* The saved cursor NEVER outruns what has been emitted. A cursor past an
  unemitted owner message is silent message loss, which is the exact failure
  this feature exists to remove. During catch-up nothing is saved at all: the
  gap is buffered, trimmed to the NEWEST `replay` messages, emitted, and only
  then is the cursor written. A crash mid-catch-up re-does the gap, which is
  the right direction to fail (delivery is at-least-once and the consumer
  dedupes on seq).
* Chat and context are TRIMMED SEPARATELY. They arrive orders of magnitude
  apart, so one shared budget would let a quiet day of scanning evict the
  question the owner typed a second before the restart.
"""

import fcntl
import json
import os
import sys
import time
from pathlib import Path

import httpx

from nakagai_edge.edge.client import EdgeClientError

CURSOR_RELPATH = Path("cache") / "channel-cursor.json"
LOCK_RELPATH = Path("cache") / "listen.lock"

DEFAULT_TIMEOUT_S = 45.0
DEFAULT_PACE_S = 0.5
DEFAULT_REPLAY = 20
DEFAULT_MAX_CATCHUP_REQUESTS = 50
BACKOFF_START_S = 1.0
BACKOFF_CAP_S = 60.0
ANCHOR_NOW = -1
# How much of a gap may be held in memory before the oldest end is discarded.
# Well above any sane --replay, so the trim is what bounds delivery, not this.
MAX_BUFFERED = 500


def _named(*fields):
    """A renderer that copies named fields and nothing else."""
    def render(body: dict) -> dict:
        return {f: body.get(f) for f in fields}
    return render


# The fields one digest row carries. Named rather than passed through for the
# same reason every other renderer names its fields: the registry is a security
# boundary, and a nested list is not exempt from it just because it is nested.
_DIGEST_ROW = ("symbol", "direction", "shape", "sessions", "confluence_max",
               "confluence_latest", "timeframes", "latest_bar_ts")

# How many rows survive rendering. The platform caps at 20; this is the
# independent ceiling, because a renderer that trusted the sender's cap would
# not be a boundary.
_DIGEST_MAX = 25


def _digest(body: dict) -> dict:
    rows = body.get("symbols")
    rows = rows if isinstance(rows, list) else []
    # Filtered before it is sliced: the cap bounds legitimate rows, not raw
    # ones, so a run of junk ahead of real rows cannot truncate every real row
    # away.
    valid = [r for r in rows if isinstance(r, dict)]
    return {
        "window_days": body.get("window_days"),
        "shown": body.get("shown"),
        "total": body.get("total"),
        "symbols": [{f: r.get(f) for f in _DIGEST_ROW} for r in valid[:_DIGEST_MAX]],
    }


# Which kinds reach the agent, and exactly which of their fields do.
#
# Naming fields rather than passing `body` through is a security boundary, and
# so is the absence of an entry. `briefing` carries a headline written by an
# external news feed, so rendering it would let an outsider write directly into
# the agent's context; it has no renderer and so is dropped. A kind the
# platform adds later lands here before anyone has judged its body, and is
# dropped for the same reason. Fail closed, then widen deliberately.
#
# `owner_msg` is the only free text in the set, and its author is the owner.
RENDERERS = {
    "owner_msg": lambda b: {"text": b.get("text", ""), "from": b.get("from", "")},
    "signal": _named("id", "symbol", "strategy", "direction", "timeframe",
                     "bar_ts", "entry", "stop", "target", "rr"),
    "approval_decided": _named("approval_id", "status", "decided_by",
                               "connector_id", "tool", "signal_id"),
    "mandate_changed": _named("preset", "kill_switch"),
    # A signal someone put a question mark against: the owner pressing "Ask
    # the agent", or their own confluence dial clearing on its own.
    # `referred_by` says which, an email or the literal "confluence".
    #
    # `note` is free text, and it is admitted for the same reason `owner_msg`
    # is: its author is the owner. That is the whole distinction from
    # `briefing`, whose text an outside news feed writes.
    "signal_referred": _named("signal_id", "symbol", "direction", "timeframe",
                              "confluence", "stacked", "intent", "note",
                              "referred_by"),
    # One recorded observation about one symbol on one bar: a sharp move, a
    # volume surge, a new range extreme, an opening gap. `detector` names which
    # of those fired. It gives no direction and no geometry, no entry, stop,
    # target or RR, so it authorizes nothing. It is context, and it is
    # deliberately not assumed actionable by the edge.
    #
    # `magnitude` is admitted where `briefing` is not, and the difference is
    # authorship rather than shape. It holds numbers computed off OHLCV bars by
    # a platform-authored detector spec, so there is no outsider-written text
    # anywhere inside it. A briefing headline is prose an external news feed
    # wrote, which is the one thing this registry exists to keep out.
    #
    # The detector is `detector` and not `kind` because `kind` is the
    # envelope's word for which event this is. One vocabulary, one meaning per
    # word: a body that shadowed an envelope key would be dropped on delivery
    # (_envelope's own fields win), and the detector name would vanish.
    "market_event": _named("symbol", "detector", "bar_ts", "timeframe",
                           "magnitude"),
    # This account's watchlist folded across a week: which tickers have a
    # multi-day shape. Every value in it is platform-computed, a ticker from
    # the security master, a shape from a closed vocabulary, or an integer, so
    # there is no outsider-written text anywhere inside it. That is the same
    # test `market_event`'s `magnitude` passes and `briefing`'s headline fails.
    #
    # It carries no entry, stop, target or RR, so it authorizes nothing: it
    # says which tickers are worth asking about, and the answer comes from the
    # platform's get_symbol_trend tool.
    "signal_digest": _digest,
    # Which nakagai-edge this owner is running, which one their platform
    # ships, and which one the index has. Every value is a version string or
    # the platform's own name for the agent, so there is no outsider-written
    # text anywhere in it: the same test `market_event`'s magnitude passes.
    #
    # It deliberately carries no upgrade command. The platform does not know
    # how this edge was installed and must not claim to, and a shell command
    # arriving over the channel and landing in an agent's context is exactly
    # what this registry exists to keep out. `nakagai-edge status` knows.
    "edge_update": _named("agent", "running", "server", "latest"),
    # Correspondence is safe only after the platform has routed it to this
    # agent. The body still carries only the verified sender's name and text.
    "agent_msg": _named("text", "agent"),
    "agent_request": _named("text", "agent", "agent_id"),
}

# Server-authored routing metadata. The platform computes each field after it
# routes the event, so the edge copies it and never infers room or reply
# authority from the kind or body.
EVENT_META = (
    "room_id", "reply_to_seq", "sender_agent_id", "dispatch_mode",
    "response_required", "claim_required", "claim_expires_at", "retry_at",
    "recipient_status", "recipient_count", "source_seq", "hop_count",
)

# A malformed response is a data error, not a transport one: httpx.HTTPError
# and EdgeClientError do not cover a 200 carrying a Cloudflare interstitial
# (JSONDecodeError, a ValueError) or a payload missing `cursor` (KeyError) or
# holding a null one (TypeError). One bad response must not end the hold.
POLL_ERRORS = (EdgeClientError, httpx.HTTPError, ValueError, KeyError, TypeError)


class ListenLocked(Exception):
    """Another listener already holds this edge's channel."""


class ListenLock:
    """One listener per edge, enforced with flock on a held descriptor.

    Two listeners would both receive every event and both reply, and presence
    cannot reveal it: `_touch` keys on agent_id, so two holds sum onto a single
    record and `presence()` flattens them to one `connected: true`.

    flock rather than a pid file: the kernel releases it when the process dies,
    however it dies, so there is no stale lock to reap, no create-then-write
    window for a second listener to steal, and no pid-reuse hazard that could
    lock the owner out of their own chat after a reboot.
    """

    def __init__(self, root) -> None:
        self.path = Path(root) / LOCK_RELPATH
        self._fd: int | None = None

    def acquire(self) -> "ListenLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            holder = self._recorded_pid(fd)
            os.close(fd)
            where = f" (pid {holder})" if holder else ""
            raise ListenLocked(
                f"another nakagai-edge listen is running{where}. Stop it first, "
                "or this edge would answer every message twice.") from None
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps({"pid": os.getpid()}).encode())
        self._fd = fd
        return self

    @staticmethod
    def _recorded_pid(fd: int) -> int | None:
        """Best effort, for the error message only. Never load-bearing: the
        flock is what decides, so a truncated or unwritten file is harmless."""
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            return int(json.loads(os.read(fd, 256).decode() or "{}").get("pid"))
        except (OSError, ValueError, TypeError):
            return None

    def release(self) -> None:
        # The file is deliberately left in place. Unlinking it would let this
        # process delete a lock another one has since taken.
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> "ListenLock":
        return self.acquire()

    def __exit__(self, *exc) -> None:
        self.release()


class CursorStore:
    """The read position, on disk so a gap between sessions is recoverable."""

    def __init__(self, root) -> None:
        self.path = Path(root) / CURSOR_RELPATH

    def load(self) -> int | None:
        try:
            return int(json.loads(self.path.read_text())["cursor"])
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def save(self, cursor: int) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"cursor": int(cursor)}))
        os.replace(tmp, self.path)


def _stderr_note(text: str) -> None:
    print(text, file=sys.stderr, flush=True)


class ChannelListener:
    def __init__(self, client, root, *, emit, note=_stderr_note,
                 sleep=time.sleep, now=time.monotonic,
                 replay: int = DEFAULT_REPLAY,
                 timeout_s: float = DEFAULT_TIMEOUT_S,
                 pace_s: float = DEFAULT_PACE_S,
                 max_catchup_requests: int = DEFAULT_MAX_CATCHUP_REQUESTS) -> None:
        self.client = client
        self.cursors = CursorStore(root)
        self.emit = emit
        self.note = note
        self.sleep = sleep
        self.now = now
        self.replay = max(0, int(replay))
        self.timeout_s = float(timeout_s)
        self.pace_s = float(pace_s)
        self.max_catchup_requests = int(max_catchup_requests)

    def run(self, should_continue=lambda: True) -> int:
        cursor = self.cursors.load()
        anchor_needed = cursor is None
        # A resumed cursor may sit behind a real gap. A fresh start cannot,
        # because it anchors to now, so only a resume buffers.
        catching_up = not anchor_needed
        pending: list[dict] = []
        requests = 0
        backoff = BACKOFF_START_S

        while should_continue():
            if anchor_needed:
                try:
                    cursor = self._anchor()
                except POLL_ERRORS as exc:
                    if self._fatal(exc):
                        return 1
                    self.note(f"[listen] cannot anchor yet: {exc}")
                    backoff = self._back_off(backoff)
                    continue
                self.cursors.save(cursor)
                self.note(f"[listen] holding from cursor {cursor}")
                anchor_needed, catching_up, requests = False, False, 0
                backoff = BACKOFF_START_S
                continue

            started = self.now()
            try:
                events, moved = self._poll(cursor, self.timeout_s)
            except POLL_ERRORS as exc:
                if self._fatal(exc):
                    return 1
                self.note(f"[listen] {type(exc).__name__}: {exc}")
                backoff = self._back_off(backoff)
                continue
            backoff = BACKOFF_START_S

            if events and moved <= cursor:
                # Re-delivering the same batch forever would post the same reply
                # forever. Today's server always advances; this keeps a payload
                # anomaly a bounded failure rather than an unbounded one.
                self.note("[listen] batch did not advance the cursor; backing off")
                backoff = self._back_off(backoff)
                continue

            msgs = [m for m in (self._envelope(e, moved) for e in events
                                if isinstance(e, dict)) if m is not None]

            if not catching_up:
                for msg in msgs:
                    self.emit(msg)
                cursor = moved
                self.cursors.save(cursor)
                self._pace(started)
                continue

            pending.extend(msgs)
            if len(pending) > MAX_BUFFERED:
                pending = pending[-MAX_BUFFERED:]
            cursor = moved
            requests += 1
            capped = requests >= self.max_catchup_requests
            if not events or capped:
                self._flush(pending, capped=capped, requests=requests)
                pending, catching_up = [], False
                self.cursors.save(cursor)
                continue
            self._pace(started)
        return 0

    # --- one poll -------------------------------------------------------

    def _anchor(self) -> int:
        payload = self.client.await_events(after=ANCHOR_NOW, timeout_s=0)
        if not isinstance(payload, dict) or "cursor" not in payload:
            raise ValueError("anchor response carried no cursor")
        return int(payload["cursor"])

    def _poll(self, cursor: int, timeout_s: float) -> tuple[list, int]:
        payload = self.client.await_events(after=cursor, timeout_s=timeout_s)
        if not isinstance(payload, dict):
            raise ValueError(f"expected an object, got {type(payload).__name__}")
        events = payload.get("events") or []
        if not isinstance(events, list):
            raise ValueError("events was not a list")
        return events, int(payload.get("cursor", cursor))

    def _pace(self, started: float) -> None:
        """Hold the request rate under the bucket the trade executor shares.

        Measured rather than assumed. A quiet hold already spent its timeout, so
        this sleeps nothing; but if the server ever returns empty promptly, the
        guarantee still holds instead of becoming a hot loop.
        """
        left = self.pace_s - (self.now() - started)
        if left > 0:
            self.sleep(left)

    # --- delivery -------------------------------------------------------

    @staticmethod
    def _envelope(event: dict, cursor: int) -> dict | None:
        """Render one event, or None for a kind that must not be delivered.

        The render is spread FIRST so the server envelope always wins. A body
        naming `response_required` would otherwise decide for itself that the
        agent owes it an answer, and one naming `kind` would relabel its line in
        a stream the agent sorts by kind. No renderer names either today; the
        ordering is what keeps that a fact about the registry rather than a
        thing the next entry can quietly undo. Fail closed, then widen
        deliberately, applies to the envelope too.
        """
        render = RENDERERS.get(event.get("kind"))
        if render is None:
            return None
        kind = event["kind"]
        meta = {field: event.get(field, False)
                if field == "response_required" else event.get(field)
                for field in EVENT_META}
        return {**render(event.get("body") or {}),
                "seq": event.get("seq"), "kind": kind,
                "at": event.get("created_at"), "cursor": cursor,
                **meta}

    def _flush(self, pending: list[dict], *, capped: bool, requests: int) -> None:
        """Deliver the end of a gap: the NEWEST `replay`, never the oldest.

        Someone restarting a listener wants the recent end of the conversation,
        and the newest message is the one most likely to still matter.

        Chat and context are trimmed against separate budgets. They arrive at
        wildly different rates, so one shared newest-N would let a day of
        scanning evict the question the owner typed just before the restart,
        which is the silent loss this module exists to remove.
        """
        chat = [i for i, m in enumerate(pending) if m["response_required"]]
        context = [i for i, m in enumerate(pending) if not m["response_required"]]
        keep_ix = (set(chat[-self.replay:]) | set(context[-self.replay:])
                   if self.replay else set())
        keep = [m for i, m in enumerate(pending) if i in keep_ix]
        dropped = len(pending) - len(keep)
        if capped:
            self.note(f"[listen] catch-up stopped after {requests} requests; "
                      "continuing live from here")
        if dropped:
            self.note(f"[listen] gap held {len(pending)} events; "
                      f"delivering the newest {len(keep)}, skipping {dropped}")
        for msg in keep:
            self.emit(msg)

    # --- failure --------------------------------------------------------

    def _fatal(self, exc: Exception) -> bool:
        """A revoked token is terminal. Reconnecting forever against it leaves
        the owner believing the pipe is live."""
        if isinstance(exc, EdgeClientError) and getattr(exc, "status", None) == 401:
            self.note(f"[listen] {exc}")
            return True
        return False

    def _back_off(self, backoff: float) -> float:
        self.sleep(backoff)
        return min(backoff * 2, BACKOFF_CAP_S)
