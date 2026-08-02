---
name: check-the-evidence
description: Pull a play's proving record before endorsing or acting on it, and report honestly when there is none. Use whenever the owner asks whether to take a setup, which signal is best, or whether a strategy works.
---

# Check the evidence

Never endorse a play from its shape alone. Pull the record first, then answer.

## Before you answer

1. `get_play` for what the rule claims to do.
2. `get_runs` for its actual record: sample size first, then the metrics.
3. `get_signals` for whether it is currently firing, and whether anything is
   suppressed.

## Report the record, including when it is bad or absent

State the sample size before any metric. A profit factor over eight trades is
not evidence, and quoting it without `n` implies a confidence the number cannot
carry.

**Say so when nothing has been proven.** The catalog's own re-prove put median
profit factor at 0.991, below breakeven, and promoted one play of fifty under
Benjamini-Hochberg where the older rule promoted twenty-four. Ranking inside a
set like that is still ranking inside a set that has not shown positive
expectancy, and an answer that omits it is overselling.

If asked to pick a best among several, you may rank them, but say what the
ranking is: relative order, not a claim that the top one is good.

## Watch for

- **A strategy that fires on nearly everything is not confirming anything.** If
  one rule appears on most symbols in both directions, it is behaving like a
  market-direction proxy, and confluence that leans on it is thinner than the
  count suggests. Check whether the agreement survives dropping it.
- **Contradiction is not a weak signal, it is no signal.** Two rules firing
  opposite directions on the same symbol and bar cancel. Report them as
  cancelling rather than picking the one you prefer.
- **Staleness.** An intraday setup does not survive a weekend or an overnight
  gap. Check the bar timestamp against now before quoting an entry or a stop.

## Do not

Do not soften a bad record to be encouraging. The owner is deciding with money.
