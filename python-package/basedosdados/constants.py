"""
Constants for the project.
"""

__all__ = ["config", "constants"]

from dataclasses import dataclass
from enum import Enum


@dataclass
class config:
    """
    Configuration for the project.
    """

    billing_project_id: str = None
    project_config_path: str = None

    verbose: bool = True
    from_file: bool = False


class constants(Enum):
    """
    Constants for the project.
    """

    ENV_CONFIG: str = "BASEDOSDADOS_CONFIG"
    ENV_CREDENTIALS_PREFIX: str = "BASEDOSDADOS_CREDENTIALS_"
    ENV_CREDENTIALS_PROD: str = "BASEDOSDADOS_CREDENTIALS_PROD"
    ENV_CREDENTIALS_STAGING: str = "BASEDOSDADOS_CREDENTIALS_STAGING"
    TOKEN_FILE: str = ".token.json"
    TOKEN_URL: str = "/api/token/"
    REFRESH_TOKEN_URL: str = "/api/token/refresh/"
    VERIFY_TOKEN_URL: str = "/api/token/verify/"
    TEST_ENDPOINT: str = "/api/v1/private/bigquerytypes/"
    BACKEND_SEARCH_URL: str = "https://backend.basedosdados.org/search"
    BACKEND_GRAPHQL_URL: str = "https://backend.basedosdados.org/graphql"
