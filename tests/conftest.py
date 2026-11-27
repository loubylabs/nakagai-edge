"""PostgreSQL backing for edge-to-platform integration tests.

The edge package has no platform dependency, so its standalone suite leaves
the platform integration modules skipped. When the platform is present, those
modules exercise the actual required PostgreSQL contract against one disposable
migrated database, just as the platform suite does.
"""

import importlib.util
import os
from pathlib import Path

import pytest


_runtime = None


def _platform_runtime_class():
    """Load the platform source tree's disposable PostgreSQL runtime.

    The platform owns its migrations and its test-database ownership guard, so
    this cross-repo suite uses that exact harness rather than growing another
    database lifecycle beside it.
    """
    spec = importlib.util.find_spec("nakagai_platform")
    if spec is None or not spec.origin:
        return None
    platform_root = Path(spec.origin).resolve().parents[1]
    runtime_path = platform_root / "ops" / "local_postgres.py"
    if not runtime_path.is_file():
        raise pytest.UsageError(
            "edge platform integration requires the platform source tree, "
            f"but {runtime_path} is absent"
        )
    module_spec = importlib.util.spec_from_file_location(
        "nakagai_edge_platform_test_runtime", runtime_path
    )
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module.PostgresTestRuntime, platform_root


def pytest_sessionstart(session):
    """Start the platform's owned disposable database when it is installed."""
    global _runtime
    runtime_parts = _platform_runtime_class()
    if runtime_parts is None:
        return
    runtime_class, platform_root = runtime_parts
    _runtime = runtime_class(
        repo_root=platform_root,
        environ=os.environ,
        purpose="edge-integration",
    )
    _runtime.start()


@pytest.fixture(autouse=True)
def no_real_service_manager(monkeypatch, tmp_path_factory):
    """No test may load, restart or remove a real login service.

    The machine running this suite may serve its own edge as a LaunchAgent or
    a systemd unit, and `setup`, `restart` and `status` all consult the
    service now. So the real command runner fails the test outright, and the
    service's home is a fresh directory: an installed unit on this machine
    must never read as installed to a test. A test that wants a service
    passes its own fake runner.
    """
    from nakagai_edge.edge import install_shape, service

    def refuse(cmd):
        pytest.fail(f"a test reached the real service manager: {cmd}")

    home = tmp_path_factory.mktemp("service-home")
    monkeypatch.setattr(service, "run_command", refuse)
    monkeypatch.setattr(service, "_home", lambda: home)
    # The uvx check asks the real `uv cache dir`; tests that fake sys.prefix
    # need the path-shape fallback, whatever uv says on this machine.
    monkeypatch.setattr(install_shape, "_uv_cache_dir", lambda: "")


def pytest_sessionfinish(session, exitstatus):
    """Drop only the sentinel-protected database resources this run created."""
    global _runtime
    if _runtime is None:
        return
    try:
        _runtime.cleanup()
    finally:
        _runtime = None
