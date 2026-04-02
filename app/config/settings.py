import os


class Config:
    """Base configuration."""
    # IAM callback
    IAM_CALLBACK_URL = os.environ.get(
        "IAM_CALLBACK_URL",
        "https://iam.example.com/api/v1/callback",
    )
    IAM_CALLBACK_TIMEOUT = int(
        os.environ.get("IAM_CALLBACK_TIMEOUT", "30")
    )
    IAM_CALLBACK_RETRIES = int(
        os.environ.get("IAM_CALLBACK_RETRIES", "3")
    )

    # XLDeploy
    XLD_LOGIN_ROLE = os.environ.get(
        "XLD_LOGIN_ROLE", "XLD_LOGIN"
    )
    XLD_API_TIMEOUT = int(
        os.environ.get("XLD_API_TIMEOUT", "30")
    )
    XLD_API_RETRIES = int(
        os.environ.get("XLD_API_RETRIES", "3")
    )

    # Paths
    INSTANCES_CONFIG = os.environ.get(
        "INSTANCES_CONFIG",
        os.path.join(
            os.path.dirname(__file__), "instances.yaml"
        ),
    )

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
