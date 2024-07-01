from __future__ import annotations
from pydantic import BaseModel, SecretStr
from typing import Dict, List, Optional, Required
from datadog_api_client.v2.api import metrics_api
from datadog_api_client.v2 import ApiClient, ThreadedApiClient
from datadog_api_client import Configuration as DataDogConfiguration

class DataDogEnvVarsUsedAsCredentialsNotFound(Exception):
    """
    An exception raised when the necessary environment variables to authenticate the DataDog API: 'DD_APP_KEY' and 'DD_API_KEY' are not provided.
    """

    pass

class DataDogUsedMetricsNotAccessibleByAuthenticatedUser(Exception):
    """
    An exception raised when the DataDog used metrics are not accessible by the authenticated user.
    """

    pass