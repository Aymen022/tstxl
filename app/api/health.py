from flask_smorest import Blueprint

from app.api.schemas import HealthResponseSchema

health_blp = Blueprint(
    "health", __name__,
    description="Health check operations",
)


@health_blp.route("/health", methods=["GET"])
@health_blp.response(200, HealthResponseSchema)
def health_check() -> dict[str, str]:
    """Check API health status."""
    return {"status": "healthy"}
