import json

from sregym.conductor.oracles.base import Oracle


class PodDisruptionBudgetMitigationOracle(Oracle):
    """Mitigation oracle that detects a PDB blocking evictions and removes it.

    Strategy:
    - Find PDBs in the problem namespace whose `minAvailable` is >= the deployment replicas
      (or that match the deployment selector).
    - If one is found, delete the PDB and wait for pods to become ready.
    """

    def __init__(self, problem, deployment_name: str):
        super().__init__(problem)
        self.kubectl = problem.kubectl
        self.namespace = problem.namespace
        self.deployment_name = deployment_name

    def evaluate(self) -> dict:
        print("== PodDisruptionBudget Mitigation Evaluation ==")

        try:
            # Get deployment info
            dep_json = self.kubectl.exec_command(
                f"kubectl get deployment {self.deployment_name} -n {self.namespace} -o json"
            )
            dep = json.loads(dep_json)
            replicas = dep.get("spec", {}).get("replicas", 1) or 1

            # List PDBs in namespace
            pdbs_json = self.kubectl.exec_command(f"kubectl get pdb -n {self.namespace} -o json")
            pdbs = json.loads(pdbs_json).get("items", [])

            candidate = None
            for pdb in pdbs:
                spec = pdb.get("spec", {})
                min_avail = spec.get("minAvailable")
                # Kubernetes may represent minAvailable as string or int
                try:
                    min_avail_val = int(min_avail) if min_avail is not None else None
                except Exception:
                    min_avail_val = None

                if min_avail_val is not None and min_avail_val >= replicas:
                    candidate = pdb
                    break

            if not candidate:
                print("No PDB found that blocks evictions (minAvailable < replicas).")
                return {"success": False}

            pdb_name = candidate.get("metadata", {}).get("name")
            print(f"Found blocking PDB: {pdb_name} (minAvailable={candidate.get('spec', {}).get('minAvailable')})")

            # Delete the PDB
            delete_out = self.kubectl.exec_command(f"kubectl delete pdb {pdb_name} -n {self.namespace}")
            print(f"Deleted PDB: {delete_out.strip()}")

            # Wait for rollout/readiness
            self.kubectl.wait_for_ready(self.namespace)

            # Check deployment available replicas
            check_json = self.kubectl.exec_command(
                f"kubectl get deployment {self.deployment_name} -n {self.namespace} -o json"
            )
            check = json.loads(check_json)
            avail = check.get("status", {}).get("availableReplicas", 0)
            desired = check.get("spec", {}).get("replicas", 1) or 1

            if avail >= desired:
                print(f"Mitigation successful: availableReplicas={avail} desired={desired}")
                return {"success": True}
            else:
                print(f"Mitigation incomplete: availableReplicas={avail} desired={desired}")
                return {"success": False}

        except Exception as e:
            print(f"Error during mitigation evaluation: {e}")
            return {"success": False}
