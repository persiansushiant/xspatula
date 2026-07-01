from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List


class GraphContract:
    """
    Shared graph contract for all future Xspatula graph outputs.

    Current graph types:
    - pipeline graph
    - schema graph

    Future graph types:
    - data lineage graph
    - database dependency graph
    - execution run graph
    - audit graph
    """

    kind: str = "xspatula_graph"
    contract_version: str = "1.0.0"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "contract_version": self.contract_version,
            "nodes": self._serialize(getattr(self, "nodes", [])),
            "edges": self._serialize(getattr(self, "edges", [])),
            "metadata": self._serialize(getattr(self, "metadata", {})),
            "warnings": self._serialize(getattr(self, "warnings", [])),
        }

    def _serialize(self, value: Any) -> Any:
        if is_dataclass(value):
            return asdict(value)

        if isinstance(value, list):
            return [self._serialize(item) for item in value]

        if isinstance(value, dict):
            return {
                key: self._serialize(item)
                for key, item in value.items()
            }

        return value


def graph_capabilities() -> Dict[str, bool]:
    return {
        "pipeline_graph": True,
        "schema_graph": True,
        "traceability": True,
        "lineage_ready": True,
        "audit_ready": True,
        "dispatcher_ready": True,
        "execution_script": True,
        "node_expansion": True,
        "graph_export_ready": True,
    }


def default_graph_types() -> List[Dict[str, str]]:
    return [
        {
            "id": "pipeline",
            "label": "Pipeline Graph",
            "description": "Shows how Xspatula moves from Scheme to Job, Pilot, Process Files, Dispatcher and PostgreSQL.",
        },
        {
            "id": "schema",
            "label": "Schema Graph",
            "description": "Shows database schemas, tables, columns and inferred table dependencies.",
        },
        {
            "id": "lineage",
            "label": "Data Lineage Graph",
            "description": "Future graph for tracking where records came from, who created them and how they moved.",
        },
    ]