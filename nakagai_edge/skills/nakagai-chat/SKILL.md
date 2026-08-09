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

Run the second command as the session's persistent monitor. It holds the platform channel open and prints owner messages as JSON. Keep it alive for the whole session, parse each event, deduplicate on sequence number, and answer through the platform message tool.

An idle timeout is normal. Restart the listener if it exits because the API or edge connection was unavailable. Do not start a second listener for the same session while the first is healthy.

Before leaving, read the live mandate, report an idle check-in when appropriate, and schedule the next wake from `directives.check_interval_minutes` and `clock.next_phase_at`. The channel preserves backlog, but only a running listener can wake a turn immediately.
