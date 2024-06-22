from __future__ import annotations

class DataDogEnvVarsUsedAsCredentialsNotFound(Exception):
    """
    An exception raised when the necessary environment variables to authenticate the DataDog API: 'DD_APP_KEY' and 'DD_API_KEY' are not provided.
    """

    pass

class DataDogRequiredMetricsNotAccessibleByAuthenticatedUser(Exception):
    """
    An exception raised when the DataDog required metrics: 'avg:kubernetes.cpu.requests{*}' and 'avg:container.cpu.usage{*}' are not accessible by the authenticated user.
    """

    pass