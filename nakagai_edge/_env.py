"""The gateway's ONE dynamic env lookup.

Connector specs name env vars at runtime (auth.token_env, ${VAR} interpolation
in config/connectors.yaml), so these reads cannot be static fields on
nakagai.settings.Settings. Route them through here so `grep read_env_ref`
finds every dynamic lookup.
"""

import os


def read_env_ref(name: str) -> str:
    return os.environ.get(name, "")
