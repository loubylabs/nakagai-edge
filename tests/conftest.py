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


def pytest_sessionfinish(session, exitstatus):
    """Drop only the sentinel-protected database resources this run created."""
    global _runtime
    if _runtime is None:
        return
    try:
        _runtime.cleanup()
    finally:
        _runtime = None


@pytest.fixture
def platform_database():
    """A short-lived direct handle for test setup through durable stores."""
    if _runtime is None:
        raise RuntimeError("platform database requested without platform integration")
    from nakagai_platform.api.db import Database

    database = Database.from_env()
    try:
        yield database
    finally:
        database.close()
