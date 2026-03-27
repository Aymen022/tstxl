import os
import threading
from typing import Any

import yaml
from flask import Flask
from flask_smorest import Api

from app.config.settings import config_by_name
from app.utils.logging import setup_logging


# In-memory set for idempotency (provisioningId dedup)
processed_ids: set[str] = set()
processed_ids_lock: threading.Lock = threading.Lock()


def load_instances_config(path: str) -> dict[str, Any]:
    """Load XLD instance registry from YAML."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def create_app(config_name: str | None = None) -> Flask:
    """Flask application factory."""
    if config_name is None:
        config_name = os.environ.get("FLASK_ENV", "development")

    app = Flask(__name__)
    app.config.from_object(config_by_name[config_name])

    # Setup structured logging
    setup_logging(app.config.get("LOG_LEVEL", "INFO"))

    # Load XLD instances config
    instances_config: dict[str, Any] = load_instances_config(app.config["INSTANCES_CONFIG"])
    app.config["XLD_INSTANCES"] = instances_config.get("instances", {})
    app.config["APPLICATION_INSTANCES"] = instances_config.get("application_instances", {})

    # Initialize flask-smorest API (Swagger UI + OpenAPI)
    api = Api(app)

    # Define API key security scheme in OpenAPI spec
    api.spec.components.security_scheme(
        "ApiKeyAuth",
        {"type": "apiKey", "in": "header", "name": "X-API-Key"},
    )

    # Register blueprints
    from app.api.provisioning import provisioning_blp
    from app.api.health import health_blp

    api.register_blueprint(provisioning_blp, url_prefix="/api/v1")
    api.register_blueprint(health_blp, url_prefix="/api/v1")

    return app
