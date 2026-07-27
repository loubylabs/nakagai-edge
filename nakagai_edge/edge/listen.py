"""Hold the platform's live channel open and emit owner messages as lines.

The agent is turn-based and cannot be pushed to, so something local has to hold
the line and turn an arriving message into a wake-up. That is this module.

Two design points carry most of the weight:

* The hold is CONTINUOUS. Presence on the platform is `holds > 0`
  (nakagai/channel.py), true only while someone is actively holding a long poll,
  so a listener that exited in order to notify would flicker the owner's badge
  off between every message. The HTTP hold stays open; stdout carries the signal.
* The listener is PACED. Every /api/agent/* route shares one per-agent token
  bucket, so an unpaced drain here does not degrade chat, it 429s the trade
  executor and the stop-loss brake: a granted approval then reads as "no such
  approval" and a stop-loss execution report is dropped for good. Never
  cold-start at cursor 0, bound every catch-up, stay well under the refill rate.
"""

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


class ListenLocked(Exception):
    """Another listener already holds this edge's channel."""


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Someone else's process, but alive.
        return True
    except OSError:
        return False
    return True


class ListenLock:
    """One listener per edge root.

    Two listeners would both receive every event and both reply, and presence
    cannot reveal it: `_touch` keys on agent_id, so two holds sum onto a single
    record and `presence()` flattens them to one `connected: true`.
    """

    def __init__(self, root) -> None:
        self.path = Path(root) / LOCK_RELPATH

    def acquire(self) -> "ListenLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            holder = self._holder()
            if holder is not None and _pid_alive(holder):
                raise ListenLocked(
                    f"another nakagai-edge listen is running (pid {holder}). "
                    "Stop it first, or this edge would answer every message twice.")
            # A killed listener must not lock the owner out of chat forever.
            self.path.unlink(missing_ok=True)
            fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "w") as fh:
            json.dump({"pid": os.getpid()}, fh)
        return self

    def _holder(self) -> int | None:
        try:
            return int(json.loads(self.path.read_text()).get("pid"))
        except (OSError, ValueError, TypeError, AttributeError):
            return None

    def release(self) -> None:
        self.path.unlink(missing_ok=True)

    def __enter__(self) -> "ListenLock":
        return self.acquire()

    def __exit__(self, *exc) -> None:
        self.release()


class CursorStore:
    """The read position, on disk so a gap between sessions is recoverable.

    Written only AFTER a message is emitted, so a crash re-delivers rather than
    skips. Delivery is at-least-once by the channel's own design, and the
    consumer dedupes by seq.
    """

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
                 sleep=time.sleep, replay: int = DEFAULT_REPLAY,
                 timeout_s: float = DEFAULT_TIMEOUT_S,
                 pace_s: float = DEFAULT_PACE_S,
                 max_catchup_requests: int = DEFAULT_MAX_CATCHUP_REQUESTS) -> None:
        self.client = client
        self.cursors = CursorStore(root)
        self.emit = emit
        self.note = note
        self.sleep = sleep
        self.replay = int(replay)
        self.timeout_s = float(timeout_s)
        self.pace_s = float(pace_s)
        self.max_catchup_requests = int(max_catchup_requests)

    def run(self, should_continue=lambda: True) -> int:
        cursor = self.cursors.load()
        anchor_needed = cursor is None
        # A resumed cursor may sit behind a real backlog; a fresh start cannot,
        # because it anchors to now. So the replay bound applies only to a resume.
        budget = None if anchor_needed else self.replay
        backoff = BACKOFF_START_S
        catchup_requests = 0

        while should_continue():
            if anchor_needed:
                try:
                    cursor = int(self._poll(ANCHOR_NOW, 0)["cursor"])
                except EdgeClientError as exc:
                    if self._fatal(exc):
                        return 1
                    backoff = self._back_off(backoff)
                    continue
                except httpx.HTTPError as exc:
                    self.note(f"[listen] transport error anchoring: {exc}")
                    backoff = self._back_off(backoff)
                    continue
                self.cursors.save(cursor)
                self.note(f"[listen] holding from cursor {cursor}")
                anchor_needed, catchup_requests, budget = False, 0, None
                continue

            try:
                payload = self._poll(cursor, self.timeout_s)
            except EdgeClientError as exc:
                if self._fatal(exc):
                    return 1
                self.note(f"[listen] platform error: {exc}")
                backoff = self._back_off(backoff)
                continue
            except httpx.HTTPError as exc:
                # Disjoint from EdgeClientError: catching only the latter would
                # kill the listener on the first network blip.
                self.note(f"[listen] transport error: {exc}")
                backoff = self._back_off(backoff)
                continue
            backoff = BACKOFF_START_S

            # Deliberately no mandate gate here. Chat is never mandate-gated:
            # a halted agent must still be able to say that it is halted, and
            # parking the listener during a dark phase would strand exactly the
            # messages this feature exists to deliver. The mandate governs
            # trading authority, not whether the owner can reach their agent.
            events = payload.get("events") or []
            cursor = int(payload.get("cursor", cursor))
            budget = self._deliver(events, cursor, budget)
            self.cursors.save(cursor)

            if not events:
                catchup_requests, budget = 0, None
                continue

            catchup_requests += 1
            if catchup_requests > self.max_catchup_requests:
                self.note(f"[listen] catch-up exceeded {self.max_catchup_requests} "
                          "requests; re-anchoring to now and skipping the rest")
                anchor_needed = True
                continue
            # Pace only when there is more to fetch. A quiet hold already cost
            # its full timeout, and sleeping after it would just add latency.
            self.sleep(self.pace_s)
        return 0

    def _poll(self, after: int, timeout_s: float) -> dict:
        return self.client.await_events(after=after, timeout_s=timeout_s)

    def _fatal(self, exc: EdgeClientError) -> bool:
        """A revoked token is terminal. Reconnecting forever against it leaves
        the owner believing the pipe is live."""
        if getattr(exc, "status", None) == 401:
            self.note(f"[listen] {exc}")
            return True
        return False

    def _back_off(self, backoff: float) -> float:
        self.sleep(backoff)
        return min(backoff * 2, BACKOFF_CAP_S)

    def _deliver(self, events, cursor: int, budget):
        """Emit owner messages only.

        Every other kind is dropped before it reaches stdout. That is a security
        boundary, not a convenience: `briefing` bodies carry model-written text
        derived from third-party RSS, and house events are cross-tenant today.
        """
        for event in events:
            if event.get("kind") != "owner_msg":
                continue
            if budget is not None:
                if budget <= 0:
                    continue
                budget -= 1
            body = event.get("body") or {}
            self.emit({"seq": event.get("seq"), "text": body.get("text", ""),
                       "from": body.get("from", ""), "at": event.get("created_at"),
                       "cursor": cursor})
        return budget
