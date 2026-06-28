import os
from pathlib import Path

ENV_VAR_NAME = "XSPATULA_ENVIRONMENT_DIR"

def get_environment_dir():
    configured = os.environ.get(ENV_VAR_NAME)
    if configured:
        return Path(configured).resolve()

    return Path(__file__).resolve().parent / "environment"