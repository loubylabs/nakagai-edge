"""nakagai-edge: pair this machine with the platform, then serve MCP to your agent.

The whole of the edge's command surface. Deliberately not built on nakagai.cli, which
imports pandas at module scope: that weight is exactly what this package sheds.
"""

import argparse
import json
import sys

from nakagai_edge.edge.state import EdgeState, default_root
from nakagai_edge.identity import package_version


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
    304 as well, so its return value cannot tell us which happened. Three disk
    facts can. A missing registry means this edge has never synced at all.
    fetched_at, which sync_once advances only when the platform answered (a 200
    or a 304), means the pull reached the platform: on an edge that synced
    before, the registry is on disk either way, so this stamp is the only thing
    separating a fresh answer from a dead platform. And schema_error_at means
    the platform answered with a bundle we would not take.

    All three are read either side of the call, never as absolute state, so
    what gets reported is what this sync did rather than whatever the edge was
    already carrying.
    """
    import yaml as _yaml

    from nakagai_edge.edge.client import EdgeClientError
    from nakagai_edge.edge.sync import (fetched_at, schema_error, schema_error_at,
                                        sync_once)
    client = _edge_client(state)
    if client is None:
        raise EdgeClientError("edge is not paired")
    before = fetched_at(state)
    before_refusal = schema_error_at(state)
    sync_once(state, client)
    # A refused bundle is the more specific fault, and it has to be named
    # first: it leaves fetched_at exactly where a dead platform would, so the
    # generic message below would send the owner checking a URL that is fine.
    #
    # Only a refusal from THIS call, though. The reason stays on record until a
    # sync succeeds, and repeating it through a later outage tells an owner who
    # has just upgraded the platform that their upgrade did not take, which is
    # the same wrong-fault-naming arriving from the other side. The timestamp
    # advances on every refusal, so a genuine repeat still reports honestly.
    if schema_error_at(state) > before_refusal:
        raise EdgeClientError(f"sync failed: {schema_error(state)}")
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


def _cmd_restart(args) -> int:
    """Stop what we can prove is ours, then start a replacement we can see.

    Every refusal here exits non-zero and names its own remedy. The one that
    will actually happen on an existing machine is the missing pidfile: a
    daemon started before this version predates the file, so the first restart
    after upgrading is still done by hand, and every one after it is not.
    """
    from nakagai_edge.edge import daemon as d
    from nakagai_edge.edge.state import EdgeState, default_root

    state = EdgeState(default_root())
    edge, reason = d.find_running(state)
    if reason:
        print(f"refused: {reason}", file=sys.stderr)
        return 2

    # The port is the daemon's, never a flag's: an edge started on 9000 comes
    # back on 9000. It survives the daemon too, because a stopping edge
    # releases its pidfile claim without dropping the record, so a restart
    # after a Ctrl-C lands where the old one was rather than silently moving
    # to the default. Choosing a different port is starting a different
    # daemon, which is what `run --port` is for.
    port = (edge.port if edge
            else (d.read_pidfile(state) or {}).get("port") or d.DEFAULT_PORT)
    if edge is not None:
        armed = d.armed_positions(state)
        if armed and not args.force:
            print("refused: the brake is watching "
                  f"{len(armed)} open position"
                  f"{'' if len(armed) == 1 else 's'}", file=sys.stderr)
            for row in armed:
                # A firing position has an exit order in flight to the
                # broker right now: the worse of the two moments to cut the
                # watch, so it is labelled distinctly rather than folded
                # into an undifferentiated count.
                tag = "  <- exit in flight" if row["state"] == "firing" else ""
                print(f"  {row['symbol']:<6} {row['qty']} @ stop {row['stop']}{tag}",
                      file=sys.stderr)
            print("\nA restart stops the brake for a few seconds.\n"
                  "Run during a session only if you mean it:\n"
                  "\n  nakagai-edge restart --force", file=sys.stderr)
            return 3
        if not d.stop(edge, timeout_s=10.0):
            print(f"refused: pid {edge.pid} is still serving 127.0.0.1:{port} "
                  "ten seconds after SIGTERM. Not escalating to SIGKILL on a "
                  "process holding broker credentials; stop it by hand.",
                  file=sys.stderr)
            return 4

    pid = d.spawn(state, port=port)
    if pid < 0:
        print(f"refused: could not launch a replacement on 127.0.0.1:{port}. "
              f"Look at {state.log_path}", file=sys.stderr)
        return 6
    if not d.wait_until_serving(port):
        print(f"started pid {pid}, but 127.0.0.1:{port} never answered. "
              f"Look at {state.log_path}", file=sys.stderr)
        return 5
    print(f"serving 127.0.0.1:{port}, pid {pid}")
    print(f"log: {state.log_path}")
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


def _cmd_connect(args) -> int:
    """Print the connection contract, and wire up any client we recognise.

    The printing is unconditional and comes first. The registration is the part
    that can fail, be declined, or not apply, and a user whose client we have
    never heard of still leaves with everything they need.
    """
    from nakagai_edge.edge import clients
    from nakagai_edge.edge.install import install_skills

    server_url = clients.url(args.port)
    print(f"Nakagai MCP endpoint:\n  {server_url}\n")
    print("Any MCP client can use it directly. Config snippet:\n")
    print(clients.snippet(args.port))
    print()

    if args.no_register:
        return 0

    state_root = default_root()
    found = clients.detected()
    if not found:
        print("No known agent client detected. Paste the snippet above into yours.")
        return 0

    for client in found:
        ok = client.register(server_url)
        print(f"  {client.label}: MCP entry "
              + ("registered" if ok else "NOT registered (add it by hand)"))
        report = install_skills(client.skills_dir(),
                                manifest=state_root / "skills-manifest.json")
        print(f"  {client.label}: skills {report.summary()}")
    return 0


def _cmd_setup(args) -> int:
    """pair -> sync -> login -> connect -> run, in that order, skipping what is done.

    The planner decides; this only executes and prints. It is asked twice: once
    on the edge as we found it (for pair and sync), and again once the sync has
    landed (for login and run), because the login decision reads the registry
    and the registry may not have existed a moment ago.
    """
    from nakagai_edge.edge import clients
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
            # Every _sync_step message already opens with "sync failed:", so
            # prefixing one here reads "sync failed: sync failed: ...".
            print(f"  x  {e}")
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

    # 4. connect the agent. On this side of the serve on purpose: run() blocks
    # until Ctrl-C, so anything placed after it would simply never happen.
    # _cmd_connect honours --no-register itself, which is why it is called
    # unconditionally: the flag withholds the wiring, never the endpoint.
    print()
    _cmd_connect(args)

    # 5. run
    run_step = steps["run"]
    if not run_step.run:
        print(f"  -  serving    {run_step.reason}")
        print(f"  next: nakagai-edge run --port {args.port}")
        return 0
    print(f"  -> {run_step.reason} on {clients.url(args.port)} (Ctrl-C to stop)")
    run(state.root, port=args.port)
    return 0


def _version_key(version: str) -> tuple:
    from nakagai_edge.edge.freshness import _key
    return _key(version)


def _behind(current: str, other: str) -> bool:
    return _version_key(other) > _version_key(current)


def _cmd_status(args) -> int:
    from nakagai_edge.edge.daemon import find_running
    from nakagai_edge.edge.freshness import latest_release
    from nakagai_edge.edge.install_shape import detect
    from nakagai_edge.edge.state import EdgeState, default_root
    from nakagai_edge.edge.sync import (
        meta, policy_fresh, schema_error, server_edge_version,
    )
    state = EdgeState(default_root())
    agent = state.agent()
    running = package_version()
    server = server_edge_version(state)
    # The index's own answer, not a comparison against it. "the newest is what
    # you are running" and "the index did not answer" are different facts, and
    # collapsing both into "" reports an outage as good news.
    latest = latest_release()
    install, upgrade, then = detect(sys.prefix, sys.argv[0])
    edge, reason = find_running(state)
    out = {"paired": agent is not None,
           "agent_id": (agent or {}).get("agent_id", ""),
           "platform_url": (agent or {}).get("platform_url", ""),
           "policy_fresh": policy_fresh(state),
           "version": running,
           # Empty when no bundle has been cached yet, which reads the same as
           # any other unsynced state.
           "server_version": server,
           # A version string, equal to `version` when this edge is current.
           # Null, and only null, when the index said nothing.
           "latest_version": latest,
           "install": install,
           "upgrade": upgrade,
           # `note` carries find_running's reason verbatim, so `status` and
           # `restart` tell one story. Without it status prints
           # `running: false` while restart refuses saying something IS on the
           # port, and the owner has two commands contradicting each other
           # about the same machine. Empty means nothing is listening at all.
           "daemon": ({"running": True, "pid": edge.pid, "port": edge.port,
                       "started_at": edge.started_at, "version": edge.version,
                       "log": str(state.log_path)}
                      if edge else {"running": False, "note": reason,
                                    "log": str(state.log_path)}),
           "meta": meta(state), "root": str(state.root)}
    # Promoted beside policy_fresh, and only when there is one. A refused
    # bundle otherwise shows up here as nothing but `policy_fresh: false`,
    # which reads as an unreachable platform and sends the owner debugging a
    # network that is fine. The message carries its own fix.
    #
    # It does print twice, since `meta` above is a verbatim dump of
    # cache/meta.json and that file is where the reason lives. The copy earns
    # its place: `meta` stays the file exactly as it is on disk, which is what
    # makes it worth pasting into a bug report, and popping a key out of it
    # here would make it a curated view that quietly lies about the file and
    # traps whoever adds the next meta key. Two lines of noise beats that.
    if (refused := schema_error(state)):
        out["schema_error"] = refused
    print(json.dumps(out, indent=2))

    # The advisory goes to stderr, so `nakagai-edge status | jq` still parses
    # and a human at a terminal still sees the line. Printed only when there is
    # something to do: an advisory on every run is a nag, and a nag is ignored.
    behind = [v for v in (server, latest) if v and _behind(running, v)]
    if behind:
        newest = max(behind, key=_version_key)
        lines = [f"\nnakagai-edge {newest} is available (you are on {running})",
                 f"  install: {install}",
                 f"  upgrade: {upgrade}"]
        # Suppressed for the one shape whose upgrade command already
        # relaunches. Printing `nakagai-edge restart` to a uvx user names a
        # command they do not have on PATH.
        if then:
            lines.append(f"  then:    {then}")
        print("\n".join(lines), file=sys.stderr)
    return 0


def _cmd_listen(args) -> int:
    """Hold the owner's chat channel open, printing one JSON line per message.

    Foreground and long-lived on purpose. The platform counts an agent as
    present only while a poll is actually held, so this process staying up is
    what makes the owner's chat pane read "Agent connected" honestly.
    """
    import json as _json
    import shlex as _shlex

    from nakagai_edge.edge.listen import (ChannelListener, ListenLock, ListenLocked,
                                          _stderr_note)
    from nakagai_edge.edge.state import EdgeState, default_root
    from nakagai_edge.edge.wake import WakeRunner

    state = EdgeState(default_root())
    client = _edge_client(state)
    if client is None:
        print(_json.dumps({"ok": False, "error": "edge is not paired: run "
                                                 "`nakagai-edge setup <code> "
                                                 "--platform <url>`"}))
        return 1
    try:
        try:
            lock = ListenLock(state.root).acquire()
        except ListenLocked as e:
            print(_json.dumps({"ok": False, "error": str(e)}))
            return 1
        wake = None
        try:
            wake = (WakeRunner(_shlex.split(args.wake_command), state,
                               note=_stderr_note)
                    if args.wake_command else None)

            def emit(msg):
                print(_json.dumps(msg), flush=True)
                if wake is not None:
                    wake.emit(msg)

            return ChannelListener(
                client, state.root,
                emit=emit,
                replay=args.replay).run()
        except KeyboardInterrupt:
            return 0
        finally:
            if wake is not None:
                wake.close()
            lock.release()
    finally:
        # Its own block: a failed release must not skip closing the pool.
        client.close()


def _cmd_brake(args) -> int:
    from nakagai_edge.edge.brake import (
        armed, clear_local_disarm, disarmed_positions, set_local_disarm,
    )
    from nakagai_edge.edge.supervision import ledger_fault, open_risk

    state = EdgeState(default_root())
    if args.action == "off":
        set_local_disarm(state, all_positions=not args.position,
                         position_id=args.position or "")
    elif args.action == "on":
        clear_local_disarm(state)
    is_armed, off = armed(state), disarmed_positions(state)
    # Read the ledger BEFORE asking for the fault: reading it is what detects a
    # corrupt one and sets it aside. The field earns its place because an empty
    # position list is the same JSON whether this edge is watching nothing or
    # has lost track of everything.
    rows = open_risk(state, {}, brake_armed=is_armed, disarmed=off)
    print(json.dumps({"armed": is_armed,
                      "disarmed_positions": sorted(off),
                      "ledger_fault": ledger_fault(state),
                      "positions": rows}, indent=2, default=str))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """Split out from main() so a test can parse an argument list and inspect
    what it resolved to, without also invoking the subcommand it named."""
    p = argparse.ArgumentParser(
        prog="nakagai-edge",
        description="Run a Nakagai edge: it holds your broker credentials, and "
                    "executes only what the platform has signed.")
    # Before the subparsers, and deliberately not a subcommand: the version is
    # what a bug report opens with, and asking for it should not require knowing
    # which subcommand to hang it off.
    p.add_argument("--version", action="version", version=package_version())
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
    p_setup.add_argument("--no-register", action="store_true",
                         help="do not touch any agent client config")
    p_setup.set_defaults(func=_cmd_setup)

    p_connect = sub.add_parser(
        "connect", help="print the MCP endpoint, and wire up a detected agent client")
    p_connect.add_argument("--port", type=int, default=8330)
    p_connect.add_argument("--no-register", action="store_true",
                           help="print only; do not touch any client config")
    p_connect.set_defaults(func=_cmd_connect)

    p_sync = sub.add_parser("sync", help="pull the connector registry and policy")
    p_sync.set_defaults(func=_cmd_sync)

    p_run = sub.add_parser("run", help="serve MCP to the agent on 127.0.0.1")
    p_run.add_argument("--port", type=int, default=8330)
    p_run.set_defaults(func=_cmd_run)

    p_restart = sub.add_parser(
        "restart", help="stop the running edge and start a fresh one, detached")
    # No --port. A restart serves the port the pidfile names, so it comes back
    # where it was; choosing a different port is starting a different daemon,
    # and `run --port` already does that.
    p_restart.add_argument("--force", action="store_true",
                           help="restart even while the brake watches an armed "
                                "position")
    p_restart.set_defaults(func=_cmd_restart)

    p_login = sub.add_parser("login", help="one-time browser OAuth for a broker connector")
    p_login.add_argument("connector_id")
    p_login.set_defaults(func=_cmd_login)

    p_status = sub.add_parser(
        "status", help="pairing, policy freshness, the daemon, and the "
                       "version picture with the line that upgrades it")
    p_status.set_defaults(func=_cmd_status)

    p_listen = sub.add_parser(
        "listen", help="hold the owner's chat channel open and print each message")
    p_listen.add_argument(
        "--replay", type=int, default=20,
        help="how many of the NEWEST messages to hand back from a gap since the "
             "last run (default 20, 0 to skip the gap entirely); a first-ever "
             "run starts from now and replays nothing")
    p_listen.add_argument(
        "--wake-command", default="",
        help="opt-in command for each response-required event; receives the "
             "rendered event as JSON on stdin and never overlaps another run")
    p_listen.set_defaults(func=_cmd_listen)

    p_brake = sub.add_parser(
        "brake", help="the stop supervisor: status, or disarm/re-arm locally")
    p_brake.add_argument("action", choices=["status", "off", "on"],
                         nargs="?", default="status")
    p_brake.add_argument("--position", default="",
                         help="disarm one position instead of all of them")
    p_brake.set_defaults(func=_cmd_brake)

    return p


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
