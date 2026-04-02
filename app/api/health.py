def health_check() -> tuple[dict[str, str], int]:
    """Check API health status."""
    return {"status": "healthy"}, 200