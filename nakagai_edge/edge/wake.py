"""Turn actionable listener events into bounded local agent runs."""

from __future__ import annotations

import json
import queue
import subprocess
import threading
from collections.abc import Callable, Sequence

from nakagai_edge.edge.candidate import CandidateWakeScope
from nakagai_edge.edge.state import EdgeState


class WakeRunner:
    """Run one wake command at a time while the listener keeps polling."""

    def __init__(self, command: Sequence[str], state: EdgeState, *,
                 note: Callable[[str], None],
                 run: Callable[..., subprocess.CompletedProcess] = subprocess.run) -> None:
        if not command:
            raise ValueError("wake command must not be empty")
        self.command = tuple(command)
        self.scope = CandidateWakeScope(state)
        self.note = note
        self._run = run
        self._queue: queue.Queue[dict | None] = queue.Queue()
        self._thread = threading.Thread(target=self._work, name="nakagai-wake", daemon=True)
        self._thread.start()

    def emit(self, event: dict) -> None:
        """Queue only events whose server-authored envelope asks for a turn."""
        if event.get("response_required"):
            self._queue.put(dict(event))

    def close(self) -> None:
        self._queue.put(None)
        self._thread.join()

    def _work(self) -> None:
        while True:
            event = self._queue.get()
            try:
                if event is None:
                    return
                token = self.scope.begin(event)
                if event.get("kind") == "execution_candidate" and token is None:
                    self.note(f"[wake] skipped expired execution candidate for seq "
                              f"{event.get('seq')}")
                    continue
                try:
                    options = {
                        "input": json.dumps(event, separators=(",", ":")) + "\n",
                        "text": True,
                        "check": False,
                    }
                    if token is not None:
                        timeout = self.scope.remaining(token)
                        if timeout is None or timeout <= 0:
                            self.note(
                                "[wake] skipped expired execution candidate for seq "
                                f"{event.get('seq')}")
                            continue
                        options["timeout"] = timeout
                    result = self._run(
                        self.command,
                        **options,
                    )
                except subprocess.TimeoutExpired:
                    # subprocess.run kills and waits for its child before it
                    # raises, so the scope remains closed through process death.
                    self.note(f"[wake] command timed out for seq {event.get('seq')}")
                    continue
                finally:
                    self.scope.finish(token)
                if result.returncode:
                    self.note(f"[wake] command exited {result.returncode} for seq "
                              f"{event.get('seq')}")
            except OSError as exc:
                self.note(f"[wake] cannot start command: {exc}")
            finally:
                self._queue.task_done()
