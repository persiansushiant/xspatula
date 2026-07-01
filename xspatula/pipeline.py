from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class PipelineNode:
    id: str
    label: str
    type: str
    status: str = "pending"
    metadata: Dict[str, Any] = field(default_factory=dict)
    expandable: bool = False


@dataclass
class PipelineEdge:
    source: str
    target: str
    label: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineGraph:
    nodes: List[PipelineNode]
    edges: List[PipelineEdge]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [asdict(node) for node in self.nodes],
            "edges": [asdict(edge) for edge in self.edges],
            "metadata": self.metadata,
        }


class XspatulaPipeline:
    """
    Builds a real graph representation of the current Xspatula execution pipeline.

    Important:
    - This class does not render UI.
    - It does not generate HTML.
    - It only returns structured graph data.
    - Launcher must not hard-code pipeline nodes.
    """

    def __init__(self, facade: Any):
        self.facade = facade

    def build(self) -> Dict[str, Any]:
        setup_path = self._get_setup_path()
        scheme_path = self._get_scheme_path()
        pilot_path = self._get_pilot_path()

        scheme_data = self._read_json_safe(scheme_path)
        pilot_data = self._read_json_safe(pilot_path)

        process_files = self._discover_process_files(setup_path, pilot_data)
        job_bundle_files = self._discover_job_bundle_files(setup_path, pilot_data, process_files)

        nodes = [
            PipelineNode(
                id="scheme",
                label="Scheme",
                type="scheme",
                status="pending",
                metadata={
                    "path": str(scheme_path) if scheme_path else None,
                    "exists": scheme_path.exists() if scheme_path else False,
                    "keys": list(scheme_data.keys()) if isinstance(scheme_data, dict) else [],
                },
            ),
            PipelineNode(
                id="pilot",
                label="Pilot Configuration",
                type="pilot",
                status="pending",
                metadata={
                    "path": str(pilot_path) if pilot_path else None,
                    "exists": pilot_path.exists() if pilot_path else False,
                    "keys": list(pilot_data.keys()) if isinstance(pilot_data, dict) else [],
                },
            ),
            PipelineNode(
                id="process_file",
                label="Process File",
                type="process_file",
                status="pending",
                metadata={
                    "count": len(process_files),
                    "files": process_files,
                },
                expandable=True,
            ),
            PipelineNode(
                id="job_bundle",
                label="Job Bundle",
                type="job_bundle",
                status="pending",
                metadata={
                    "count": len(job_bundle_files),
                    "files": job_bundle_files,
                },
                expandable=True,
            ),
            PipelineNode(
                id="postgresql_action",
                label="PostgreSQL Action",
                type="postgresql_action",
                status="pending",
                metadata={
                    "engine": "postgresql",
                    "source": "xspatula",
                },
            ),
        ]

        edges = [
            PipelineEdge(source="scheme", target="pilot"),
            PipelineEdge(source="pilot", target="process_file"),
            PipelineEdge(source="process_file", target="job_bundle"),
            PipelineEdge(source="job_bundle", target="postgresql_action"),
        ]

        graph = PipelineGraph(
            nodes=nodes,
            edges=edges,
            metadata={
                "version": "0.1.0",
                "kind": "xspatula_pipeline_graph",
                "supports": {
                    "expand_nodes": True,
                    "token_animation": True,
                    "node_status": True,
                    "graph_export": True,
                },
            },
        )

        return graph.to_dict()

    def _get_setup_path(self) -> Path:
        for attr in ("setup_path", "path_setup", "project_path", "root_path"):
            value = getattr(self.facade, attr, None)
            if value:
                return Path(value)

        return Path(".")

    def _get_scheme_path(self) -> Optional[Path]:
        for attr in ("scheme_path", "scheme_file", "_scheme_path", "_scheme_file"):
            value = getattr(self.facade, attr, None)
            if value:
                return Path(value)

        return None

    def _get_pilot_path(self) -> Optional[Path]:
        for attr in ("pilot_path", "pilot_file", "_pilot_path", "_pilot_file"):
            value = getattr(self.facade, attr, None)
            if value:
                return Path(value)

        return None

    def _read_json_safe(self, path: Optional[Path]) -> Dict[str, Any]:
        if not path or not path.exists():
            return {}

        try:
            with path.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _discover_process_files(
        self,
        setup_path: Path,
        pilot_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        candidates = self._extract_json_paths_from_data(pilot_data)

        files = []
        for candidate in candidates:
            path = self._resolve_path(setup_path, candidate)
            if path and path.exists():
                files.append(self._file_metadata(path))

        return self._unique_files(files)

    def _discover_job_bundle_files(
        self,
        setup_path: Path,
        pilot_data: Dict[str, Any],
        process_files: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        discovered = []

        for item in process_files:
            path = Path(item["path"])
            data = self._read_json_safe(path)
            candidates = self._extract_json_paths_from_data(data)

            for candidate in candidates:
                resolved = self._resolve_path(setup_path, candidate)
                if resolved and resolved.exists():
                    discovered.append(self._file_metadata(resolved))

        if discovered:
            return self._unique_files(discovered)

        fallback_files = list(setup_path.rglob("*.json"))

        return self._unique_files([
            self._file_metadata(path)
            for path in fallback_files
            if path.name not in {
                Path(item["path"]).name for item in process_files
            }
        ])

    def _extract_json_paths_from_data(self, data: Any) -> List[str]:
        results = []

        def walk(value: Any):
            if isinstance(value, dict):
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)
            elif isinstance(value, str):
                if value.endswith(".json"):
                    results.append(value)

        walk(data)
        return results

    def _resolve_path(self, setup_path: Path, value: str) -> Optional[Path]:
        raw = Path(value)

        if raw.is_absolute():
            return raw

        direct = setup_path / raw
        if direct.exists():
            return direct

        matches = list(setup_path.rglob(raw.name))
        if matches:
            return matches[0]

        return direct

    def _file_metadata(self, path: Path) -> Dict[str, Any]:
        return {
            "name": path.name,
            "path": str(path),
            "size": path.stat().st_size if path.exists() else None,
            "suffix": path.suffix,
        }

    def _unique_files(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        unique = []

        for item in files:
            key = item["path"]
            if key not in seen:
                seen.add(key)
                unique.append(item)

        return unique