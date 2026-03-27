import os


class Config:
    """Base configuration."""
    SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production")
    API_KEY = os.environ.get("FACADE_API_KEY", "change-me-in-production")

    # IAM callback
    IAM_CALLBACK_URL = os.environ.get("IAM_CALLBACK_URL", "https://iam.example.com/api/v1/callback")
    IAM_CALLBACK_TIMEOUT = int(os.environ.get("IAM_CALLBACK_TIMEOUT", "30"))
    IAM_CALLBACK_RETRIES = int(os.environ.get("IAM_CALLBACK_RETRIES", "3"))

    # XLDeploy
    XLD_LOGIN_ROLE = os.environ.get("XLD_LOGIN_ROLE", "XLD_LOGIN")
    XLD_API_TIMEOUT = int(os.environ.get("XLD_API_TIMEOUT", "30"))

    # Paths
    INSTANCES_CONFIG = os.environ.get(
        "INSTANCES_CONFIG",
        os.path.join(os.path.dirname(__file__), "instances.yaml"),
    )

    # OpenAPI / Swagger
    API_TITLE = "SGIAM Facade API"
    API_VERSION = "v1"
    OPENAPI_VERSION = "3.0.3"
    OPENAPI_URL_PREFIX = "/swagger"
    OPENAPI_SWAGGER_UI_PATH = ""
    OPENAPI_SWAGGER_UI_URL = "https://cdn.jsdelivr.net/npm/swagger-ui-dist/"

    # Logging
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")


class DevelopmentConfig(Config):
    DEBUG = True
    LOG_LEVEL = "DEBUG"


class ProductionConfig(Config):
    DEBUG = False


class TestingConfig(Config):
    TESTING = True
    LOG_LEVEL = "DEBUG"


config_by_name = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "testing": TestingConfig,
}