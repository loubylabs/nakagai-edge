---
name: nakagai-chat
description: Hold the owner's Nakagai chat channel open for this session and answer messages as they arrive. Use when the owner wants live back-and-forth chat with the agent, says "listen for my messages", "can you hear me", "go live", or asks why chat messages are unanswered.
---

# Live chat with the owner

The edge must be paired and running. From the active Nakagai checkout, verify and start the listener:

```bash
uv run nakagai-edge listen --help
uv run nakagai-edge listen
```

Run the second command as the session's persistent monitor. It holds the
platform channel open and prints one safe JSON object per eligible event. Keep
it alive for the whole session and deduplicate on `seq`.

Each line carries `seq`, `kind`, `at`, `cursor`, and server-authored routing
metadata. Read `response_required` before replying. When `claim_required` is
true, first call `claim_message(message_seq)` and continue only after it
returns `ok: true`. A claim conflict is an ordinary coordination outcome.

Reply with
`send_message(text, room_id, idempotency_key, reply_to_seq=0)`. Copy the
event's `room_id`, use its `seq` as `reply_to_seq`, and retain one stable
`idempotency_key` across retries. Informational events require no reply. Use
`list_peers()` and `request_peer(agent_ids, text, idempotency_key,
source_seq=0)` when the owner asks for peer collaboration. Peer requests stay
visible in Desk.

An idle timeout is normal. Restart the listener if it exits because the API or edge connection was unavailable. Do not start a second listener for the same session while the first is healthy.

Before leaving, read the live mandate, report an idle check-in when appropriate, and schedule the next wake from `directives.check_interval_minutes` and `clock.next_phase_at`. The channel preserves backlog, but only a running listener can wake a turn immediately.
