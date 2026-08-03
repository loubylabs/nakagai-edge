---
name: daily-brief
description: Answer "what's going on?" in one pass across signals, open risk, portfolio and pending approvals. Use when the owner asks for a status update, a morning brief, or what happened today.
---

# Daily brief

One question from the owner, one answer. Do not narrate the tools you are calling.

## Gather, in this order

1. `get_signals` for today's clusters. Note anything suppressed and why: a
   suppressed cluster is still worth reporting, with the suppression leading.
2. `get_open_risk` for exposure on anything currently held.
3. `refresh_portfolio` for the broker's own figures. These are display values
   relayed exactly as the broker worded them. Do no arithmetic on them and do
   not assume a unit the broker did not state.
4. `get_approval` for anything waiting on the owner's tap.

## Report

Lead with whatever needs a decision, because that is the only part that is
time-sensitive. Then holdings, then signals. Keep it to what changed since the
owner last looked; a list of everything is not a brief.

State plainly when a section is empty. "No signals today" is information. An
omitted section reads as an oversight.

If a call fails, say which one and report the rest. A partial brief that names
its gap is useful; a brief that silently drops a section is not.

## Do not

- Do not recommend a trade here. If the owner asks whether to take something,
  that is `check-the-evidence`, and the answer starts with the record.
- Do not present a signal's target as a forecast. It is the level the rule
  names, nothing more.
