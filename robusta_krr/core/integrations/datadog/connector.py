from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Optional

from robusta_krr.core.models.config import settings
from robusta_krr.core.models.objects import K8sWorkload, PodData
from datadog_api_client import Configuration
from datadog_api_client.v2 import ApiClient, ApiException
from .datadog_utils import DataDogEnvVarsUsedAsCredentialsNotFound, DataDogUsedMetricsNotAccessibleByAuthenticatedUser

if TYPE_CHECKING:
    from robusta_krr.core.abstract.strategies import BaseStrategy, MetricsPodData

logger = logging.getLogger("krr")

class DataDogConnector:
    def __init__(self, *, cluster: Optional[str] = None) -> None:
        """
        Initializes the DataDog Loader.

        Args:
            cluster (Optional[str]): The name of the cluster. Defaults to None.
        """

    def connect(self, url: Optional[str] = None) -> None:
        """Connect to the DataDog API using a URL."""
        config = Configuration(host=url) 
        self._connect(config)
        logger.info(f"{config.host()} connected successfully")

    def _connect(self, config: Configuration) -> None:
        required_datadog_env_vars = ('DD_APP_KEY', 'DD_API_KEY')
        for var in required_datadog_env_vars:
            if var not in os.environ:
                raise DataDogEnvVarsUsedAsCredentialsNotFound
        with ApiClient(config) as api_client:
            metrics_api = metrics_api.MetricsApi(api_client)
            required_metrics = ("avg:kubernetes.cpu.requests{*}", "avg:container.cpu.usage{*}")
            for metric in required_metrics:
                current_time = datetime.datetime.now()
                to_timestamp = int(current_time.timestamp() * 1000)
                _from_timestamp = current_time - datetime.timedelta(hours=1)
                from_timestamp = int(_from_timestamp.timestamp() * 1000)
                metrics_api = metrics_api.MetricsApi(api_client)
                body = TimeseriesFormulaQueryRequest(
                    data=TimeseriesFormulaRequest(
                        attributes=TimeseriesFormulaRequestAttributes(
                            _from=from_timestamp,
                            queries=TimeseriesFormulaRequestQueries(
                            [
                                MetricsTimeseriesQuery(
                                    data_source=MetricsDataSource.METRICS,
                                    query=metric,
                                ),
                            ]
                        ),
                        to=to_timestamp
                        ),
                    type=TimeseriesFormulaRequestType.TIMESERIES_REQUEST,
                    ),
                )
                try:
                    response = metrics_api.query_timeseries_data(body=body)
                except ApiException:
                    raise DataDogUsedMetricsNotAccessibleByAuthenticatedUser
