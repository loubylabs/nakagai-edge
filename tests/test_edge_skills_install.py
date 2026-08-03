"""Installing the packaged skills onto disk, without eating an owner's edit."""

from nakagai_edge.edge.install import install_skills


def test_install_writes_every_skill(tmp_path):
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    report = install_skills(dest, manifest=manifest)
    assert (dest / "halt" / "SKILL.md").is_file()
    assert "halt" in report.written


def test_reinstall_is_idempotent(tmp_path):
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    install_skills(dest, manifest=manifest)
    report = install_skills(dest, manifest=manifest)
    assert report.written == []
    assert "halt" in report.unchanged


def test_a_users_edit_is_never_overwritten(tmp_path):
    """The bug this prevents: someone tunes a skill, upgrades, and silently
    loses the edit. They do not trust the tool again after that."""
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    install_skills(dest, manifest=manifest)
    edited = dest / "halt" / "SKILL.md"
    edited.write_text("MY OWN VERSION")

    report = install_skills(dest, manifest=manifest)

    assert edited.read_text() == "MY OWN VERSION"
    assert "halt" in report.skipped_modified
    assert "halt" not in report.written


def test_install_is_a_copy_not_a_reference(tmp_path):
    """Guards the uvx ephemeral-cache trap: the destination must not depend on
    the package still being importable from the same place."""
    dest, manifest = tmp_path / "skills", tmp_path / "manifest.json"
    install_skills(dest, manifest=manifest)
    body = (dest / "halt" / "SKILL.md")
    assert not body.is_symlink()
    assert body.stat().st_size > 200
