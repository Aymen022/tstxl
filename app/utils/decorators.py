import hmac
from functools import wraps
from typing import Callable, Any

from flask import current_app, jsonify, request


def require_api_key(f: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to enforce API key authentication via X-API-Key header."""

    @wraps(f)
    def decorated(*args: Any, **kwargs: Any) -> Any:
        api_key: str | None = request.headers.get("X-API-Key")
        if not api_key:
            return jsonify({"error": "Missing X-API-Key header"}), 401
        expected: str = current_app.config["API_KEY"]
        if not hmac.compare_digest(api_key, expected):
            return jsonify({"error": "Invalid API key"}), 401
        return f(*args, **kwargs)

    return decorated
