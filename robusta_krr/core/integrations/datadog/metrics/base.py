from __future__ import annotations

import abc
import asyncio
import datetime
import enum
from concurrent.futures import ThreadPoolExecutor
from functools import reduce
from typing import Any, Optional, TypedDict

import numpy as np
import pydantic as pd
from ..connector import DataDogConnector
from datadog_api_client.v2.api import metrics_api
from datadog_api_client.v2 import ApiClient, ApiException
from robusta_krr.core.abstract.metrics import BaseMetric
from robusta_krr.core.abstract.strategies import PodsTimeData
from robusta_krr.core.models.objects import K8sWorkload
from datadog_api_client.v2.model.timeseries_formula_query_request import TimeseriesFormulaQueryRequest
from datadog_api_client.v2.model.timeseries_formula_request_queries import TimeseriesFormulaRequestQueries
from datadog_api_client.v2.model.timeseries_formula_request import TimeseriesFormulaRequest
from datadog_api_client.v2.model.metrics_timeseries_query import MetricsTimeseriesQuery
from datadog_api_client.v2.model.metrics_data_source import MetricsDataSource
from datadog_api_client.v2.model.timeseries_formula_request_type import TimeseriesFormulaRequestType
from datadog_api_client.v2.model.timeseries_formula_request_attributes import TimeseriesFormulaRequestAttributes
from ..datadog_utils import DataDogUsedMetricsNotAccessibleByAuthenticatedUser
from datadog_api_client import Configuration

class DataDogSeries(TypedDict):
    metric: dict[str, Any]
    values: list[list[float]]


class QueryType(str, enum.Enum):
    Query = "query"
    QueryRange = "query_range"


class DataDogMetricData(pd.BaseModel):
    query: str
    start_time: datetime.datetime
    end_time: datetime.datetime
    type: QueryType


class DataDogMetric(BaseMetric):
    """
    Base class for all metric loaders.

    Metric loaders are used to load metrics from a specified source (like DataDog in this case).

    `query_type`: the type of query to use when querying DataDog.
    Can be either `QueryType.Query` or `QueryType.QueryRange`.
    By default, `QueryType.Query` is used.

    `pods_batch_size`: if the number of pods is too large for a single query, the query is split into multiple sub-queries.
    Each sub-query result is then combined into a single result using the `combine_batches` method.
    You can override this method to change the way the results are combined.
    This parameter specifies the maximum number of pods per query.
    Set to None to disable batching
    """

    query_type: QueryType = QueryType.Query
    pods_batch_size: Optional[int] = 50
    warning_on_no_data: bool = True

    def __init__(
        self,
        datadog_api_client: ApiClient,
        executor: Optional[ThreadPoolExecutor] = None,
    ) -> None:
        self.datadog = datadog

        self.executor = executor

        if self.pods_batch_size is not None and self.pods_batch_size <= 0:
            raise ValueError("pods_batch_size must be positive")

    @abc.abstractmethod
    def get_query(self, object: K8sWorkload, duration: str) -> str:
        """
        This method should be implemented by all subclasses to provide a query string to fetch metrics.

        Args:
        object (K8sObjectData): The object for which metrics need to be fetched.
        duration (str): a string for duration of the query.

        Returns:
        str: The query string.
        """

        pass

    def _query_datadog_sync(self, data: DataDogMetricData) -> list[DataDogSeries]:
        if data.type == QueryType.QueryRange:
            start_time = int(data.start_time.timestamp() * 1000)
            end_time = int(data.end_time.timestamp() * 1000)
        else:
            current_time = datetime.datetime.now()
            start_time = int(current_time.timestamp() * 1000)
            end_time = int(current_time.timestamp() * 1000)

        config = Configuration()
        config.unstable_operations["query_timeseries_data"] = True

        with self.datadog_api_client(config) as dd_api_client:
            api_instance = metrics_api.MetricsApi(api_client)
            body = TimeseriesFormulaQueryRequest(
                attributes=TimeseriesFormulaRequestAttributes(
                    _from=start_time,
                    queries=TimeseriesFormulaRequestQueries(
                        [
                            MetricsTimeseriesQuery(
                                data_source=MetricsDataSource.METRICS,
                                query=data.query,
                            ),
                        ]
                    ),
                    to=data.end_time,
                ),
                type=TimeseriesFormulaRequestType.TIMESERIES_REQUEST,
            ),
            # datadog provides a base step depending on the length of time you're querying for
            # TODO: we might want to introduce a "step", like we do for Prometheus,
            # but doing so might mean getting data in batches. We need to investigate
            # the ideal length of each batch so we can maximize the amount of datapoints we get
            try:
                sanitized_data = dd_api_client.sanitize_for_serialization(api_instance.query_timeseries_data(body=body))
            except ApiException as e:
                raise DataDogUsedMetricsNotAccessibleByAuthenticatedUser
            return sanitized_data

    async def query_datadog(self, data: DataDogMetricData) -> list[DataDogSeries]:
        """
        Asynchronous method that queries DataDog to fetch metrics.

        Args:
        metric (Metric): An instance of the Metric class specifying what metrics to fetch.

        Returns:
        list[dict]: A list of dictionary where each dictionary represents metrics for a pod.
        """

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, lambda: self._query_datadog_sync(data))

    async def load_data(
        self, object: K8sWorkload, period: datetime.timedelta, step: datetime.timedelta
    ) -> PodsTimeData:
        """
        Asynchronous method that loads metric data for a specific object.

        Args:
        object (K8sObjectData): The object for which metrics need to be loaded.
        period (datetime.timedelta): The time period for which metrics need to be loaded.
        step (datetime.timedelta): The time interval between successive metric values.

        Returns:
        ResourceHistoryData: An instance of the ResourceHistoryData class representing the loaded metrics.
        """

        step_str = f"{round(step.total_seconds())}s"
        duration_str = self._step_to_string(period)

        query = self.get_query(object, duration_str, step_str)
        end_time = datetime.datetime.utcnow().replace(second=0, microsecond=0)
        start_time = end_time - period

        # Here if we split the object into multiple sub-objects, we query each sub-object recursively.
        if self.pods_batch_size is not None and object.pods_count > self.pods_batch_size:
            results = await asyncio.gather(
                *[
                    self.load_data(splitted_object, period, step)
                    for splitted_object in object.split_into_batches(self.pods_batch_size)
                ]
            )
            return self.combine_batches(results)

        result = await self.query_datadog(
            DataDogMetricData(
                query=query,
                start_time=start_time,
                end_time=end_time,
                type=self.query_type,
            )
        )

        if result == []:
            return {}

        return {pod_result["attributes"]["values"]: np.array(pod_result["data"], dtype=np.float64) for pod_result in result}

    # --------------------- Batching Queries --------------------- #

    def combine_batches(self, results: list[PodsTimeData]) -> PodsTimeData:
        """
        Combines the results of multiple queries into a single result.

        Args:
        results (list[MetricPodData]): A list of query results.

        Returns:
        MetricPodData: A combined result.
        """

        return reduce(lambda x, y: x | y, results, {})
