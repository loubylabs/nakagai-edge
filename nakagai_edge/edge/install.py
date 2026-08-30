"""Copy packaged skills onto disk for clients that read skills as files.

Two rules drive the shape. First, this is always a copy: `uvx` runs from an
ephemeral cache, so a symlink or a recorded path would break the moment that
cache is pruned. Second, an upgrade never overwrites something the owner edited.
We record a hash of what we wrote, and on a later run we only replace a file
that still matches it. Anything else is theirs.
"""

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from nakagai_edge.edge import skills


@dataclass
class InstallReport:
    written: list[str] = field(default_factory=list)
    skipped_modified: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)

    def summary(self) -> str:
        bits = []
        if self.written:
            bits.append(f"{len(self.written)} installed")
        if self.unchanged:
            bits.append(f"{len(self.unchanged)} already current")
        if self.skipped_modified:
            bits.append(f"{len(self.skipped_modified)} left alone (you edited them)")
        return ", ".join(bits) or "nothing to do"


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_manifest(manifest: Path) -> dict:
    """Missing or unreadable reads as empty, which fails toward the owner.

    With no record, every on-disk file that differs from what we ship is treated
    as an edit and left alone. The cost is a skipped upgrade, which the report
    says out loud. The alternative cost is eating someone's tuning silently, and
    those two are not the same size.
    """
    try:
        return json.loads(manifest.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def install_skills(dest: Path, *, manifest: Path) -> InstallReport:
    """Copy every packaged skill into `dest/<name>/SKILL.md`.

    `manifest` records the hash of what we last wrote, which is what lets a
    later run tell "unchanged since we wrote it" apart from "the owner edited
    this". Without it, idempotence and not-clobbering are indistinguishable.
    """
    recorded = _load_manifest(manifest)
    report = InstallReport()

    # Take one verified inventory snapshot. An installation must not copy a
    # partial bundle if a packaging change drifts from the declared release.
    packaged = skills.list_skills()
    for name in packaged:
        body = skills.read_skill(name)
        target = dest / name / "SKILL.md"

        if target.exists():
            try:
                current = target.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                # Unreadable means unprovable, and unprovable means the owner's.
                # Without this the whole install raises out of `connect`, which
                # is the onboarding path, so one odd file would take the rest of
                # the skills down with it.
                report.skipped_modified.append(name)
                continue
            if current == body:
                # Byte-identical to what we ship, so there is no edit to lose.
                # Recording it here is what re-adopts a file whose manifest
                # entry went missing, and what picks up files a run left behind
                # when it died partway through.
                report.unchanged.append(name)
                recorded[name] = _digest(body)
                continue
            if _digest(current) != recorded.get(name):
                report.skipped_modified.append(name)
                continue

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body, encoding="utf-8")
        recorded[name] = _digest(body)
        report.written.append(name)

    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps(recorded, indent=2), encoding="utf-8")
    return report
