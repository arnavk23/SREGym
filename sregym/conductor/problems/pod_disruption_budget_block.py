from kubernetes import client
from kubernetes.client.rest import ApiException

from sregym.conductor.oracles.llm_as_a_judge.llm_as_a_judge_oracle import LLMAsAJudgeOracle
from sregym.conductor.oracles.sustained_readiness import SustainedReadinessOracle
from sregym.conductor.oracles.pod_disruption_budget_mitigation import PodDisruptionBudgetMitigationOracle
from sregym.conductor.problems.base import Problem
from sregym.service.apps.hotel_reservation import HotelReservation
from sregym.service.kubectl import KubeCtl
from sregym.utils.decorators import mark_fault_injected


class PDBBlockHotelReservation(Problem):
    """PodDisruptionBudget misconfiguration that blocks voluntary disruptions.

    Creates a PodDisruptionBudget with `minAvailable == replicas` so
    `allowedDisruptions == 0`. Attempts to evict a pod are rejected by the
    eviction API with the typical message about violating the disruption
    budget, leaving the deployment under-replicated.
    """

    def __init__(self, app_name: str = "hotel_reservation", faulty_service: str = "frontend"):
        if app_name != "hotel_reservation":
            raise ValueError("PDBBlock currently only supports the hotel_reservation app")

        self.app = HotelReservation()
        super().__init__(app=self.app, namespace=self.app.namespace)

        self.kubectl = KubeCtl()
        self.faulty_service = faulty_service

        self.root_cause = self.build_structured_root_cause(
            component=f"deployment/{self.faulty_service}",
            namespace=self.namespace,
            description=(
                "A PodDisruptionBudget has been created with `minAvailable` equal to the "
                "deployment's replica count, making `allowedDisruptions=0`. Voluntary "
                "disruptions (evictions/drains) are therefore rejected by the Eviction API, "
                "leaving the deployment permanently under-replicated even though pods and "
                "their specs are healthy."
            ),
        )

        self.diagnosis_oracle = LLMAsAJudgeOracle(problem=self, expected=self.root_cause)
        self.mitigation_oracle = PodDisruptionBudgetMitigationOracle(problem=self, deployment_name=self.faulty_service)

    @mark_fault_injected
    def inject_fault(self):
        print("== Fault Injection: PodDisruptionBudget block ==")

        # Read current replica count from the deployment
        deployment = self.kubectl.get_deployment(self.faulty_service, self.namespace)
        replicas = int(getattr(deployment.spec, "replicas", 1) or 1)

        pdb_name = f"{self.faulty_service}-availability-guard"

        # Build a selector that matches the deployment pods
        selector = None
        if deployment.spec and deployment.spec.selector and deployment.spec.selector.match_labels:
            selector = deployment.spec.selector.match_labels

        pdb_spec = client.V1PodDisruptionBudget(
            metadata=client.V1ObjectMeta(name=pdb_name),
            spec=client.V1PodDisruptionBudgetSpec(
                min_available=replicas,
                selector=client.V1LabelSelector(match_labels=selector) if selector else None,
            ),
        )

        policy_api = client.PolicyV1Api()

        try:
            policy_api.create_namespaced_pod_disruption_budget(namespace=self.namespace, body=pdb_spec)
            print(f"Created PDB '{pdb_name}' in namespace {self.namespace} (minAvailable={replicas})")
        except ApiException as e:
            if e.status == 409:
                print(f"PDB '{pdb_name}' already exists; continuing")
            else:
                print(f"Error creating PDB: {e}")

        # Attempt to evict a pod using the eviction API so the disruption budget error surfaces
        core_api = client.CoreV1Api()
        label_selector = ",".join(f"{k}={v}" for k, v in (selector or {}).items()) if selector else None
        pods = core_api.list_namespaced_pod(namespace=self.namespace, label_selector=label_selector).items

        if not pods:
            print(f"No pods found for service {self.faulty_service} in namespace {self.namespace}")
            return

        target_pod = pods[0].metadata.name
        eviction = client.V1Eviction(
            metadata=client.V1ObjectMeta(name=target_pod, namespace=self.namespace),
            delete_options=client.V1DeleteOptions(grace_period_seconds=0),
        )

        try:
            core_api.create_namespaced_pod_eviction(name=target_pod, namespace=self.namespace, body=eviction)
            print(f"Eviction requested for pod {target_pod} (unexpectedly succeeded)")
        except ApiException as e:
            msg = str(e)
            if "disruption budget" in msg or (hasattr(e, "status") and e.status in (429, 400)):
                print(f"Eviction rejected as expected: {msg}")
            else:
                print(f"Eviction failed with unexpected error: {msg}")

        print(f"Fault: pdb_block | Service: {self.faulty_service} | Namespace: {self.namespace}\n")

    @mark_fault_injected
    def recover_fault(self):
        print("== Fault Recovery: remove PDB ==")
        pdb_name = f"{self.faulty_service}-availability-guard"
        policy_api = client.PolicyV1Api()
        try:
            policy_api.delete_namespaced_pod_disruption_budget(name=pdb_name, namespace=self.namespace)
            print(f"Deleted PDB '{pdb_name}' from namespace {self.namespace}")
        except ApiException as e:
            if e.status == 404:
                print(f"PDB '{pdb_name}' not found (already removed)")
            else:
                print(f"Error deleting PDB: {e}")
