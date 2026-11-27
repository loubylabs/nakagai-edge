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
    name: str          # pair | sync | login | service | run
    run: bool
    reason: str        # why it runs, or why it is skipped


def plan(*, paired: bool, code: str, synced: bool, has_broker_tokens: bool,
         broker_enabled: bool, login: bool, run_server: bool, foreground: bool,
         service_supported: bool) -> list[Step]:
    """`login` is False only under --no-login; nothing here ever asks.

    At most one of `service` and `run` runs. The service is the default: it
    serves in the background and starts by itself at login. `run` serves in
    this terminal, under --foreground or where there is no service to hand
    the edge to. --no-run turns both off.
    """
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
    elif not login:
        login_step = Step("login", False, "skipped (--no-login)")
    else:
        login_step = Step("login", True, f"{BROKER} needs a one-time browser login")

    if not run_server:
        service_step = Step("service", False, "skipped (--no-run)")
        run_step = Step("run", False, "skipped (--no-run)")
    elif foreground:
        service_step = Step("service", False, "skipped (--foreground)")
        run_step = Step("run", True, "serving MCP in the foreground (--foreground)")
    elif not service_supported:
        service_step = Step("service", False, "no login service on this machine")
        run_step = Step("run", True, "serving MCP in the foreground")
    else:
        service_step = Step("service", True,
                            "serving in the background: it starts by itself at login")
        run_step = Step("run", False, "the login service serves it")

    return [pair_step, sync_step, login_step, service_step, run_step]
