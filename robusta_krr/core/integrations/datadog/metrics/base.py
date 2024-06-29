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

from robusta_krr.core.abstract.metrics import BaseMetric
from robusta_krr.core.abstract.strategies import PodsTimeData
from robusta_krr.core.models.objects import K8sWorkload


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
        datadog: DataDogConnector,
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
            response = self.datadog.safe_custom_query_range(
                # datadog provides a base step depending on the length of time you're querying for
                # TODO: we might want to introduce a "step", like we do for Prometheus,
                # but doing so might mean getting data in batches. We need to investigate
                # the ideal length of each batch so we can maximize the amount of datapoints we get
                query=data.query,
                start_time=data.start_time,
                end_time=data.end_time,
            )
            return response["result"]
        else:
            # regular query, lighter on preformance
            try:
                response = self.datadog.safe_custom_query(query=data.query)
            except Exception as e:
                raise ValueError(f"Failed to run query: {data.query}") from e
            results = response["result"]
            # format the results to return the same format as custom_query_range
            for result in results:
                result["values"] = [result.pop("value")]
            return results

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