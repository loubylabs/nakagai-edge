"""Installing the packaged skills onto disk, without eating an owner's edit."""

import json

from nakagai_edge.edge import skills
from nakagai_edge.edge.install import _digest, install_skills


def test_install_writes_every_skill(tmp_path):
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    report = install_skills(dest, manifest=manifest)
    expected = skills.list_skills()
    assert report.written == expected
    assert all((dest / name / "SKILL.md").is_file() for name in expected)


def test_reinstall_is_idempotent(tmp_path):
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    install_skills(dest, manifest=manifest)
    report = install_skills(dest, manifest=manifest)
    assert report.written == []
    assert "daily-brief" in report.unchanged


def test_a_users_edit_is_never_overwritten(tmp_path):
    """The bug this prevents: someone tunes a skill, upgrades, and silently
    loses the edit. They do not trust the tool again after that."""
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    install_skills(dest, manifest=manifest)
    edited = dest / "daily-brief" / "SKILL.md"
    edited.write_text("MY OWN VERSION")

    report = install_skills(dest, manifest=manifest)

    assert edited.read_text() == "MY OWN VERSION"
    assert "daily-brief" in report.skipped_modified
    assert "daily-brief" not in report.written


def test_an_unreadable_target_does_not_abort_the_install(tmp_path):
    """`connect` is the onboarding path, so one odd file must not take the other
    other skills down with it. Unreadable is treated as the owner's, same as an
    edit: we cannot prove we wrote it, so we do not touch it."""
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    install_skills(dest, manifest=manifest)
    (dest / "daily-brief" / "SKILL.md").write_bytes(b"\xff\xfe not utf-8 \x00")

    report = install_skills(dest, manifest=manifest)

    assert "daily-brief" in report.skipped_modified
    assert "check-the-evidence" in report.unchanged, "the install stopped early"
    assert (dest / "daily-brief" / "SKILL.md").read_bytes().startswith(b"\xff\xfe")


def test_a_skill_no_longer_shipped_is_retired_unless_edited(tmp_path):
    """0.7.1 dropped `halt`, whose copy told agents to engage a kill switch
    the platform no longer has. An upgrade removes our untouched copy and
    leaves an edited one alone."""
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    install_skills(dest, manifest=manifest)
    for name, text in (("halt", "old halt skill"), ("mine", "old mine skill")):
        (dest / name).mkdir()
        (dest / name / "SKILL.md").write_text(text)
    recorded = json.loads(manifest.read_text())
    recorded["halt"] = _digest("old halt skill")
    recorded["mine"] = _digest("old mine skill")
    manifest.write_text(json.dumps(recorded))
    (dest / "mine" / "SKILL.md").write_text("MY OWN VERSION")

    report = install_skills(dest, manifest=manifest)

    assert report.removed == ["halt"]
    assert not (dest / "halt").exists()
    assert "mine" in report.skipped_modified
    assert (dest / "mine" / "SKILL.md").read_text() == "MY OWN VERSION"
    assert "halt" not in json.loads(manifest.read_text())
    assert "1 retired" in report.summary()


def test_install_is_a_copy_not_a_reference(tmp_path):
    """Guards the uvx ephemeral-cache trap: the destination must not depend on
    the package still being importable from the same place."""
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    install_skills(dest, manifest=manifest)
    body = (dest / "daily-brief" / "SKILL.md")
    assert not body.is_symlink()
    assert body.stat().st_size > 200
