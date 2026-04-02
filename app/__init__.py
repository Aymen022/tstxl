import os
import re
import threading
from typing import Any

import connexion
import yaml

from app.config.settings import config_by_name
from app.utils.logging import setup_logging

_ENV_VAR_PATTERN = re.compile(r"\$\{(\w+)\}")

# In-memory set for idempotency (provisioningId dedup)
processed_ids: set[str] = set()
processed_ids_lock: threading.Lock = threading.Lock()


def _resolve_env_vars(value: Any) -> Any:
    """Recursively resolve ${VAR_NAME} placeholders from environment."""
    if isinstance(value, str):
        return _ENV_VAR_PATTERN.sub(
            lambda m: os.environ.get(m.group(1), m.group(0)), value
        )
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def load_instances_config(path: str) -> dict[str, Any]:
    """Load XLD instance registry from YAML, resolve env vars, merge defaults."""
    with open(path, "r") as f:
        raw: dict[str, Any] = yaml.safe_load(f)

    # Resolve ${VAR} placeholders
    config: dict[str, Any] = _resolve_env_vars(raw)

    # Merge default_credentials into each instance
    defaults: dict[str, Any] = config.pop("default_credentials", {})
    for name, instance in config.get("instances", {}).items():
        config["instances"][name] = {**defaults, **instance}

    return config


def create_app(config_name: str | None = None) -> connexion.FlaskApp:
    """Application factory using connexion (API-first)."""
    if config_name is None:
        config_name = os.environ.get("FLASK_ENV", "development")

    # Create connexion app — spec_dir points to project root
    spec_dir = os.path.dirname(os.path.dirname(__file__))
    cxn_app = connexion.FlaskApp(
        __name__,
        specification_dir=spec_dir,
    )

    # Load Flask config
    flask_app = cxn_app.app
    flask_app.config.from_object(config_by_name[config_name])

    # Setup structured logging
    setup_logging(flask_app.config.get("LOG_LEVEL", "INFO"))

    # Load XLD instances config
    instances_config: dict[str, Any] = load_instances_config(
        flask_app.config["INSTANCES_CONFIG"]
    )
    flask_app.config["XLD_INSTANCES"] = instances_config.get("instances", {})
    flask_app.config["ALLOWED_INSTANCES_BY_APP"] = instances_config.get(
        "allowed_instances_by_app", {}
    )

    # Add API from OpenAPI spec — connexion handles routing, validation, Swagger UI
    cxn_app.add_api(
        "openapi_spec.yaml",
        validate_responses=False,
    )

    return cxn_app