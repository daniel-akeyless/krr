from robusta_krr.core.models.allocations import ResourceType
from robusta_krr.core.models.objects import K8sObjectData

from .base_filtered_metric import BaseFilteredMetricLoader
from .base_metric import bind_metric


@bind_metric(ResourceType.Memory)
class MemoryMetricLoader(BaseFilteredMetricLoader):
    def get_query(self, object: K8sObjectData) -> str:
        pods_selector = "|".join(pod.name for pod in object.pods)
        cluster_label = self.get_prometheus_cluster_label()

        return f"""
            sum(max_over_time(container_memory_working_set_bytes{{
                namespace="{object.namespace}",
                pod=~"{pods_selector}",
                container="{object.container}"
                {cluster_label}
            }}[5m])) by (container, pod, job)
        """
