import logging
import asyncio
from typing import Dict, Any, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException

from robusta_krr.core.models.objects import K8sObjectData, KindLiteral
from robusta_krr.core.models.allocations import ResourceType
from robusta_krr.core.models.config import settings
from robusta_krr.utils.resource_units import format as format_resource

logger = logging.getLogger("krr")

class ResourcePatcher:
    """Handles applying resource recommendations to Kubernetes workloads"""
    
    def __init__(self, cluster: Optional[str] = None):
        self.api_client = settings.get_kube_client(cluster)
        self.apps_v1 = client.AppsV1Api(api_client=self.api_client)
        self.batch_v1 = client.BatchV1Api(api_client=self.api_client)
        self.core_v1 = client.CoreV1Api(api_client=self.api_client)
        self.custom_objects = client.CustomObjectsApi(api_client=self.api_client)

    def _build_resource_patch(self, 
                             container_index: int, 
                             resource_type: ResourceType, 
                             value: float) -> Dict[str, Any]:
        """Build a JSON patch for updating container resource requests"""
        
        # Convert value to appropriate string format
        if resource_type == ResourceType.CPU:
            # CPU values in cores, convert to millicores (e.g., 0.5 cores -> 500m)
            resource_value = f"{int(value * 1000)}m"
        elif resource_type == ResourceType.Memory:
            # Memory values in bytes (e.g., 134217728 -> 128Mi)
            resource_value = format_resource(value)
        else:
            resource_value = str(value)

        return {
            "op": "replace",
            "path": f"/spec/template/spec/containers/{container_index}/resources/requests/{resource_type.value}",
            "value": resource_value
        }

    def _build_container_path(self, kind: KindLiteral) -> str:
        """Get the JSON path to containers based on workload type"""
        if kind == "CronJob":
            return "/spec/jobTemplate/spec/template/spec/containers"
        elif kind in ["Deployment", "StatefulSet", "DaemonSet"]:
            return "/spec/template/spec/containers"
        elif kind == "Job":
            return "/spec/template/spec/containers"
        elif kind in ["Rollout", "DeploymentConfig"]:
            return "/spec/template/spec/containers"
        else:
            logger.warning(f"Unknown workload kind {kind}, using default container path")
            return "/spec/template/spec/containers"

    def _get_container_index(self, object_data: K8sObjectData) -> int:
        """Find the index of the target container in the workload spec"""
        try:
            api_resource = object_data._api_resource
            if not api_resource:
                logger.error(f"No API resource available for {object_data}")
                return 0
            # Get containers based on workload type
            if object_data.kind == "CronJob":
                containers = api_resource.spec.job_template.spec.template.spec.containers
            elif object_data.kind in ["Deployment", "StatefulSet", "DaemonSet", "Job"]:
                containers = api_resource.spec.template.spec.containers
            else:
                logger.warning(f"Unsupported workload type {object_data.kind}")
                return 0
            # Find container by name
            for i, container in enumerate(containers):
                if container.name == object_data.container:
                    return i
            logger.warning(f"Container {object_data.container} not found in {object_data.kind} {object_data.name}")
            return 0
        except Exception as e:
            logger.error(f"Error finding container index: {e}")
            return 0

    async def apply_resource_recommendation(self,
                                          object_data: K8sObjectData,
                                          resource_type: ResourceType,
                                          recommended_value: float,
                                          dry_run: bool) -> bool:
        """Apply a resource recommendation to a Kubernetes workload

        Args:
            object_data: The Kubernetes object to patch
            resource_type: CPU or Memory
            recommended_value: The recommended resource value

        Returns:
            bool: True if patch was successful, False otherwise
        """
        try:
            if object_data.kind != "Deployment":
                logger.info(f"Skipping {object_data.kind} {object_data.namespace}/{object_data.name} for resource recommendation, currently only supporting Deployment")
                return False

            container_index = self._get_container_index(object_data)
            
            # Create patch for updating the resource request
            patch_path = f"/spec/template/spec/containers/{container_index}/resources/requests/{resource_type.value}"
            if object_data.kind == "CronJob":
                patch_path = f"/spec/jobTemplate/spec/template/spec/containers/{container_index}/resources/requests/{resource_type.value}"
            current_value = getattr(object_data.allocations, "requests").get(resource_type)
            # Format the resource value appropriately
            if resource_type == ResourceType.CPU:
                resource_value = f"{int(recommended_value * 1000)}m"
                current_value = f"{int(current_value * 1000)}m"
            elif resource_type == ResourceType.Memory:
                resource_value = format_resource(recommended_value)
                current_value = format_resource(current_value)
            else:
                resource_value = str(recommended_value)
                current_value = str(current_value)

            patch = [{
                "op": "replace",
                "path": patch_path,
                "value": resource_value
            }]

            logger.info(f"Applying {resource_type.value} recommendation for {object_data.kind} {object_data.namespace}/{object_data.name}, container {object_data.container}: {current_value} -> {resource_value}")
            if not dry_run:
                # Apply the patch based on workload type
                success = await self._patch_workload(object_data, patch)
                if success:
                    logger.info(f"Successfully applied {resource_type.value} recommendation to {object_data.kind} {object_data.namespace}/{object_data.name}")
                    return True
                else:
                    logger.error(f"Failed to apply {resource_type.value} recommendation to {object_data.kind} {object_data.namespace}/{object_data.name}")
                    return False
            else:
                return True
        except Exception as e:
            logger.error(f"Error applying resource recommendation: {e}")
            return False

    async def _patch_workload(self, object_data: K8sObjectData, patch: list) -> bool:
        """Patch a workload based on its type"""
        loop = asyncio.get_running_loop()

        try:
            if object_data.kind == "Deployment":
                await loop.run_in_executor(
                    None,
                    lambda: self.apps_v1.patch_namespaced_deployment(
                        name=object_data.name,
                        namespace=object_data.namespace,
                        body=patch
                    )
                )
            elif object_data.kind == "StatefulSet":
                await loop.run_in_executor(
                    None,
                    lambda: self.apps_v1.patch_namespaced_stateful_set(
                        name=object_data.name,
                        namespace=object_data.namespace,
                        body=patch
                    )
                )
            elif object_data.kind == "DaemonSet":
                await loop.run_in_executor(
                    None,
                    lambda: self.apps_v1.patch_namespaced_daemon_set(
                        name=object_data.name,
                        namespace=object_data.namespace,
                        body=patch
                    )
                )
            elif object_data.kind == "Job":
                await loop.run_in_executor(
                    None,
                    lambda: self.batch_v1.patch_namespaced_job(
                        name=object_data.name,
                        namespace=object_data.namespace,
                        body=patch
                    )
                )
            elif object_data.kind == "CronJob":
                await loop.run_in_executor(
                    None,
                    lambda: self.batch_v1.patch_namespaced_cron_job(
                        name=object_data.name,
                        namespace=object_data.namespace,
                        body=patch
                    )
                )
            elif object_data.kind == "Rollout":
                # Argo Rollouts CRD
                await loop.run_in_executor(
                    None,
                    lambda: self.custom_objects.patch_namespaced_custom_object(
                        group="argoproj.io",
                        version="v1alpha1",
                        namespace=object_data.namespace,
                        plural="rollouts",
                        name=object_data.name,
                        body=patch
                    )
                )
            elif object_data.kind == "DeploymentConfig":
                # OpenShift DeploymentConfig
                await loop.run_in_executor(
                    None,
                    lambda: self.custom_objects.patch_namespaced_custom_object(
                        group="apps.openshift.io",
                        version="v1",
                        namespace=object_data.namespace,
                        plural="deploymentconfigs",
                        name=object_data.name,
                        body=patch
                    )
                )
            elif object_data.kind == "StrimziPodSet":
                # Strimzi PodSet
                await loop.run_in_executor(
                    None,
                    lambda: self.custom_objects.patch_namespaced_custom_object(
                        group="core.strimzi.io",
                        version="v1beta2",
                        namespace=object_data.namespace,
                        plural="strimzipodsets",
                        name=object_data.name,
                        body=patch
                    )
                )
            else:
                logger.error(f"Unsupported workload type for patching: {object_data.kind}")
                return False
            return True
        except ApiException as e:
            logger.error(f"Kubernetes API error patching {object_data.kind} {object_data.namespace}/{object_data.name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error patching {object_data.kind} {object_data.namespace}/{object_data.name}: {e}")
            return False

def create_resource_patcher(cluster: Optional[str] = None) -> ResourcePatcher:
    """Factory function to create a ResourcePatcher instance"""
    return ResourcePatcher(cluster)