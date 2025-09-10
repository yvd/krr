import textwrap
from typing import Optional

import pydantic as pd

from robusta_krr.core.abstract.strategies import (
    BaseStrategy,
    StrategySettings,
    MetricsPodData,
    K8sObjectData,
    RunResult,
    ResourceType,
    ResourceRecommendation,
)
from robusta_krr.core.integrations.prometheus.metrics import PrometheusMetric


class DummyStrategySettings(StrategySettings):
    reduction_percentage: float = pd.Field(
        10.0, 
        gt=0, 
        le=100, 
        description="The percentage to reduce from current k8s resource allocations."
    )


class DummyStrategy(BaseStrategy[DummyStrategySettings]):
    display_name = "dummy"
    rich_console = True

    @property
    def metrics(self) -> list[type[PrometheusMetric]]:
        # This strategy doesn't require any metrics data
        return []

    @property
    def description(self) -> str:
        return textwrap.dedent(f"""\
            CPU request: current k8s value - {self.settings.reduction_percentage}%, limit: current k8s value - {self.settings.reduction_percentage}%
            Memory request: current k8s value - {self.settings.reduction_percentage}%, limit: current k8s value - {self.settings.reduction_percentage}%
            
            This strategy doesn't require any historical data and simply recommends reducing current resource allocations by a fixed percentage.
            
            Example: `krr dummy --reduction_percentage=15` to reduce resources by 15% instead of the default 10%.
            """)

    def __calculate_cpu_recommendation(self, object_data: K8sObjectData) -> ResourceRecommendation:
        current_request = object_data.allocations.requests.get(ResourceType.CPU)
        current_limit = object_data.allocations.limits.get(ResourceType.CPU)
        
        # Calculate 10% reduction
        reduction_factor = 1 - (self.settings.reduction_percentage / 100)
        
        recommended_request = None
        recommended_limit = None
        
        if current_request is not None and current_request != "?" and current_request > 0:
            recommended_request = float(current_request) * reduction_factor
        
        if current_limit is not None and current_limit != "?" and current_limit > 0:
            recommended_limit = float(current_limit) * reduction_factor
        
        # If no current values are set, return undefined
        if recommended_request is None and recommended_limit is None:
            return ResourceRecommendation.undefined(info="No current CPU allocation defined")
        
        return ResourceRecommendation(
            request=recommended_request, 
            limit=recommended_limit,
            info=f"Reduced by {self.settings.reduction_percentage}% from current allocation"
        )

    def __calculate_memory_recommendation(self, object_data: K8sObjectData) -> ResourceRecommendation:
        current_request = object_data.allocations.requests.get(ResourceType.Memory)
        current_limit = object_data.allocations.limits.get(ResourceType.Memory)
        
        # Calculate 10% reduction
        reduction_factor = 1 - (self.settings.reduction_percentage / 100)
        
        recommended_request = None
        recommended_limit = None
        
        if current_request is not None and current_request != "?" and current_request > 0:
            recommended_request = float(current_request) * reduction_factor
        
        if current_limit is not None and current_limit != "?" and current_limit > 0:
            recommended_limit = float(current_limit) * reduction_factor
        
        # If no current values are set, return undefined
        if recommended_request is None and recommended_limit is None:
            return ResourceRecommendation.undefined(info="No current Memory allocation defined")
        
        return ResourceRecommendation(
            request=recommended_request, 
            limit=recommended_limit,
            info=f"Reduced by {self.settings.reduction_percentage}% from current allocation"
        )

    def run(self, history_data: MetricsPodData, object_data: K8sObjectData) -> RunResult:
        return {
            ResourceType.CPU: self.__calculate_cpu_recommendation(object_data),
            ResourceType.Memory: self.__calculate_memory_recommendation(object_data),
        }