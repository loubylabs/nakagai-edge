---
name: candidate-trader
description: Decide one platform execution candidate on a resident edge. Use only for an addressed execution_candidate event: inspect the candidate, accept or abstain with a concise rationale, then stop.
---

# Decide one execution candidate

Use this skill only when the listener gives you an addressed
`execution_candidate` event. This is one bounded decision. It is not a trading
session, an order editor, or permission to adjust the owner's controls.

## The bounded decision

First inspect one candidate. Then choose accept or abstain, and provide a concise rationale
grounded in the candidate and any current read-only platform or broker facts.
Then stop after one durable decision.

Use exactly one of these tools:

```text
accept_candidate(candidate_id, rationale)
abstain_candidate(candidate_id, rationale)
```

Use the event's `candidate_id`. Do not substitute another candidate or revisit a
prior event. Default to abstain when a material fact is missing, contradictory,
stale, or outside this skill's scope.

## One-event loop

1. Read the event, its candidate facts, and its expiry.
2. Inspect the candidate with read-only platform or broker tools when needed.
3. Choose accept or abstain before the expiry.
4. Give the candidate tool a concise factual rationale.
5. Stop after one durable decision, including when the result reports a prior
   terminal decision or a blocked preparation.

Do not wait for a broker result, retry with another decision, process another
candidate, or begin a follow-up trading task in this wake.

## Boundaries

The edge enforces the candidate tools as the only write actions for this event.
It refuses `place_order`, every write through `call_connector`, approval tools,
and broker writes until this wake ends or expires. Read-only inspection remains
available.

Accepting does not authorize changing any prepared field. The platform alone
constructs the frozen order and retains authority for its symbol, side, order
type, account, quantity, entry, stop, connector, risk policy, and expiry.
The platform owns quantity and price. The model cannot select or edit quantity or price.

Local policy or the brake can still refuse execution. Acceptance is a judgment,
not approval, a grant, a broker call, or a promise that a trade will execute.
