"""What `nakagai-edge setup` will do, decided before it does any of it.

The steps are data, not control flow: this module answers "what runs, what is
skipped, and why" as a pure function over edge state, so the decision can be
tested without a browser, a server, or a network. The runner in cli.py executes
what comes back and prints one line per step.
"""

from dataclasses import dataclass

BROKER = "robinhood-trading"


@dataclass(frozen=True)
class Step:
    name: str          # pair | sync | login | run
    run: bool
    reason: str        # why it runs, or why it is skipped


def plan(*, paired: bool, code: str, synced: bool, has_broker_tokens: bool,
         broker_enabled: bool, run_server: bool) -> list[Step]:
    if not paired and not code:
        raise ValueError(
            "this edge is not paired and you gave no pairing code. "
            "Mint one on the Agents page, then run "
            "`nakagai-edge setup <code> --platform <url>`")

    if code:
        reason = "re-pairing with the code you gave" if paired else "pairing this edge"
        pair_step = Step("pair", True, reason)
    else:
        pair_step = Step("pair", False, "already paired")

    # Always sync: the registry and policy are what everything downstream reads,
    # and a stale one denies every call (sync.POLICY_TTL_S).
    sync_step = Step("sync", True,
                     "refreshing the registry" if synced else "fetching the registry")

    if not broker_enabled:
        login_step = Step("login", False, f"{BROKER} is not enabled in the registry")
    elif has_broker_tokens:
        login_step = Step("login", False, f"this edge already has {BROKER} tokens")
    else:
        login_step = Step("login", True, f"{BROKER} needs a one-time browser login")

    run_step = Step("run", run_server,
                    "serving MCP" if run_server else "skipped (--no-run)")

    return [pair_step, sync_step, login_step, run_step]
