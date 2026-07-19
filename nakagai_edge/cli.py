"""nakagai-edge: pair this machine with the platform, then serve MCP to your agent.

The whole of the edge's command surface. Deliberately not built on nakagai.cli, which
imports pandas at module scope: that weight is exactly what this package sheds.
"""

import argparse
import sys


def _gateway_run(coro):
    """Run one gateway coroutine and print its JSON result. The hub is async;
    the CLI is not."""
    import asyncio
    import json as _json

    try:
        result = asyncio.run(coro)
    except Exception as e:
        print(_json.dumps({"ok": False, "error": type(e).__name__, "message": str(e)}))
        return 1
    print(_json.dumps(result, indent=2))
    return 0


def _cmd_pair(args) -> int:
    import json as _json

    from nakagai_edge.edge.client import EdgeClientError, pair
    from nakagai_edge.edge.preflight import check_platform
    from nakagai_edge.edge.state import EdgeState, default_root
    try:
        check_platform(args.platform)
        out = pair(args.platform, args.code)
    except EdgeClientError as e:
        print(_json.dumps({"ok": False, "error": str(e)}))
        return 1
    state = EdgeState(default_root())
    state.save_agent(args.platform, out["agent_id"], out["token"])
    print(_json.dumps({"ok": True, "agent_id": out["agent_id"],
                       "root": str(state.root)}))
    return 0


def _edge_client(state):
    """The paired platform client, or None when this edge has never paired."""
    from nakagai_edge.edge.client import PlatformClient
    agent = state.agent()
    if agent is None:
        return None
    return PlatformClient(agent["platform_url"], agent["token"])


def _sync_step(state) -> int:
    """Pull the bundle and write the registry. Returns the connector count.
    Raises EdgeClientError when the pull itself failed.

    sync_once swallows every error and returns False, and it returns False on a
    304 as well, so its return value cannot tell us which happened. Two disk
    facts can. A missing registry means this edge has never synced at all. And
    fetched_at, which sync_once advances only when the platform answered (a 200
    or a 304), means the pull reached the platform: on an edge that synced
    before, the registry is on disk either way, so this stamp is the only thing
    separating a fresh answer from a dead platform.
    """
    import yaml as _yaml

    from nakagai_edge.edge.client import EdgeClientError
    from nakagai_edge.edge.sync import fetched_at, sync_once
    client = _edge_client(state)
    if client is None:
        raise EdgeClientError("edge is not paired")
    before = fetched_at(state)
    sync_once(state, client)
    path = state.root / "config" / "connectors.yaml"
    if not path.exists():
        raise EdgeClientError(
            "sync failed: the platform returned no registry. "
            "Check --platform and that this edge is still paired.")
    if fetched_at(state) <= before:
        raise EdgeClientError(
            "sync failed: the platform did not answer, so the registry on disk "
            "is still whatever the last sync left there. Check that the "
            "--platform URL is reachable, and that this edge is still paired "
            "(a revoked agent token answers 401). Nothing here is current until "
            "a sync succeeds.")
    doc = _yaml.safe_load(path.read_text()) or {}
    return len(doc.get("connectors", []))


def _cmd_sync(args) -> int:
    import json as _json

    from nakagai_edge.edge.client import EdgeClientError
    from nakagai_edge.edge.state import EdgeState, default_root
    state = EdgeState(default_root())
    if state.agent() is None:
        print(_json.dumps({"ok": False, "error": "edge is not paired: run "
                                                 "`nakagai-edge setup <code> --platform <url>`"}))
        return 1
    try:
        count = _sync_step(state)
    except EdgeClientError as e:
        print(_json.dumps({"ok": False, "error": str(e)}))
        return 1
    print(_json.dumps({"ok": True, "connectors": count, "root": str(state.root)}))
    return 0


def _cmd_run(args) -> int:
    from nakagai_edge.edge.runtime import run
    from nakagai_edge.edge.state import default_root
    run(default_root(), port=args.port)
    return 0


def _cmd_login(args) -> int:
    from nakagai_edge.edge.state import default_root
    from nakagai_edge.oauth_login import login
    return _gateway_run(login(default_root(), args.connector_id))


def _broker_enabled(state, broker: str) -> bool:
    import yaml as _yaml

    path = state.root / "config" / "connectors.yaml"
    if not path.exists():
        return False
    doc = _yaml.safe_load(path.read_text()) or {}
    for entry in doc.get("connectors", []):
        if entry.get("id") == broker:
            return bool(entry.get("enabled", False))
    return False


def _edge_facts(state, broker: str) -> dict:
    """What the planner decides on, read fresh off disk each time it is asked."""
    from nakagai_edge.auth import has_oauth_tokens
    return {
        "paired": state.agent() is not None,
        "synced": (state.root / "config" / "connectors.yaml").exists(),
        "has_broker_tokens": has_oauth_tokens(state.root, broker),
        "broker_enabled": _broker_enabled(state, broker),
    }


def _confirm(question: str) -> bool:
    try:
        answer = input(f"{question} [Y/n] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return False
    return answer in ("", "y", "yes")


def _cmd_setup(args) -> int:
    """pair -> sync -> login -> run, in that order, skipping what is done.

    The planner decides; this only executes and prints. It is asked twice: once
    on the edge as we found it (for pair and sync), and again once the sync has
    landed (for login and run), because the login decision reads the registry
    and the registry may not have existed a moment ago.
    """
    from nakagai_edge.edge.client import EdgeClientError, pair
    from nakagai_edge.edge.preflight import check_platform
    from nakagai_edge.edge.runtime import run
    from nakagai_edge.edge.setup import BROKER, plan
    from nakagai_edge.edge.state import EdgeState, default_root

    state = EdgeState(default_root())
    code = args.code or ""
    run_server = not args.no_run

    try:
        steps = {s.name: s for s in plan(code=code, run_server=run_server,
                                         **_edge_facts(state, BROKER))}
    except ValueError as e:
        print(f"  x  {e}")
        return 1

    # 1. pair
    pair_step = steps["pair"]
    if pair_step.run:
        try:
            check_platform(args.platform)
            out = pair(args.platform, code)
        except EdgeClientError as e:
            print(f"  x  {e}")
            return 1
        state.save_agent(args.platform, out["agent_id"], out["token"])
        print(f"  v  paired     {pair_step.reason}: agent {out['agent_id'][:8]}")
    else:
        print(f"  -  paired     {pair_step.reason}")

    # 2. sync
    sync_step = steps["sync"]
    if sync_step.run:
        print(f"  .  syncing    {sync_step.reason}")
        try:
            count = _sync_step(state)
        except EdgeClientError as e:
            print(f"  x  sync failed: {e}")
            return 1
        print(f"  v  synced     {count} connectors")
    else:
        print(f"  -  synced     {sync_step.reason}")

    # The registry is on disk now, so ask the planner again for the two steps
    # that read it. Pairing is behind us, hence code="": either the pair step
    # just ran or the planner already found this edge paired, so plan() cannot
    # object to the missing code.
    steps = {s.name: s for s in plan(code="", run_server=run_server,
                                     **_edge_facts(state, BROKER))}

    # 3. login
    login_step = steps["login"]
    if not login_step.run:
        print(f"  -  login      {login_step.reason}")
    else:
        print(f"  ?  login      {login_step.reason}")
        if _confirm("  do it now?"):
            from nakagai_edge.oauth_login import login
            if _gateway_run(login(state.root, BROKER)) != 0:
                print(f"  x  login failed. Finish it later with: "
                      f"nakagai-edge login {BROKER}")
                return 1
            print(f"  v  login      {BROKER}")
        else:
            print(f"  !  login      skipped. {BROKER} stays dead until you run: "
                  f"nakagai-edge login {BROKER}")

    # 4. run
    run_step = steps["run"]
    url = f"http://127.0.0.1:{args.port}/mcp/"
    print("\n  point your agent at this one endpoint:")
    print(f'    {{ "nakagai": {{ "type": "http", "url": "{url}" }} }}\n')
    if not run_step.run:
        print(f"  -  serving    {run_step.reason}")
        print(f"  next: nakagai-edge run --port {args.port}")
        return 0
    print(f"  -> {run_step.reason} on {url} (Ctrl-C to stop)")
    run(state.root, port=args.port)
    return 0


def _cmd_status(args) -> int:
    import json as _json

    from nakagai_edge.edge.state import EdgeState, default_root
    from nakagai_edge.edge.sync import meta, policy_fresh
    state = EdgeState(default_root())
    agent = state.agent()
    print(_json.dumps({"paired": agent is not None,
                       "agent_id": (agent or {}).get("agent_id", ""),
                       "platform_url": (agent or {}).get("platform_url", ""),
                       "policy_fresh": policy_fresh(state),
                       "meta": meta(state), "root": str(state.root)}, indent=2))
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="nakagai-edge",
        description="Run a Nakagai edge: it holds your broker credentials, and "
                    "executes only what the platform has signed.")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_pair = sub.add_parser("pair", help="exchange a pairing code for an agent token")
    p_pair.add_argument("code")
    p_pair.add_argument("--platform", required=True, help="e.g. https://api.nakag.ai")
    p_pair.set_defaults(func=_cmd_pair)

    p_setup = sub.add_parser(
        "setup", help="pair, sync, log in, and serve: everything, in order")
    p_setup.add_argument("code", nargs="?", default="",
                         help="pairing code from the Agents page (omit to reuse "
                              "an existing pairing)")
    p_setup.add_argument("--platform", default="https://api.nakag.ai",
                         help="only used when pairing; an already-paired edge "
                              "syncs against the platform_url it paired with")
    p_setup.add_argument("--port", type=int, default=8330)
    p_setup.add_argument("--no-run", action="store_true",
                         help="stop after login instead of serving")
    p_setup.set_defaults(func=_cmd_setup)

    p_sync = sub.add_parser("sync", help="pull the connector registry and policy")
    p_sync.set_defaults(func=_cmd_sync)

    p_run = sub.add_parser("run", help="serve MCP to the agent on 127.0.0.1")
    p_run.add_argument("--port", type=int, default=8330)
    p_run.set_defaults(func=_cmd_run)

    p_login = sub.add_parser("login", help="one-time browser OAuth for a broker connector")
    p_login.add_argument("connector_id")
    p_login.set_defaults(func=_cmd_login)

    p_status = sub.add_parser("status", help="pairing + policy freshness")
    p_status.set_defaults(func=_cmd_status)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
