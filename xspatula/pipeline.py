from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class PipelineNode:
    id: str
    label: str
    type: str
    status: str = "pending"
    expandable: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


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
    execution: List[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [asdict(node) for node in self.nodes],
            "edges": [asdict(edge) for edge in self.edges],
            "execution": self.execution,
            "metadata": self.metadata,
        }


class XspatulaPipeline:
    def __init__(self, facade: Any):
        self.facade = facade

    def build(self) -> Dict[str, Any]:
        setup_path = Path(self.facade.setup_path).resolve()

        scheme_path = self._resolve_path(setup_path, self.facade.scheme_file)
        pilot_path = self._resolve_path(setup_path, self.facade.pilot_file)

        scheme_data = self._read_json_safe(scheme_path)
        pilot_data = self._read_json_safe(pilot_path)

        job_name = self._infer_job_name()
        process_files = self._discover_process_files(setup_path, pilot_data)
        job_files = self._discover_job_files(setup_path, process_files)

        token_count = max(len(job_files), 1)

        nodes = [
            self._file_node(
                node_id="scheme",
                label="Scheme",
                node_type="scheme",
                path=scheme_path,
                setup_path=setup_path,
                requested_value=self.facade.scheme_file,
                description="Execution environment: database settings, users, project paths and defaults.",
                parsed_keys=list(scheme_data.keys()) if isinstance(scheme_data, dict) else [],
            ),
            PipelineNode(
                id="job",
                label="Job",
                type="job",
                metadata={
                    "name": job_name,
                    "path": None,
                    "absolute_path": None,
                    "description": "The selected execution goal.",
                    "trace": {
                        "role": "job",
                        "source": "inferred_from_selected_pilot",
                        "selected_pilot": self.facade.pilot_file,
                        "resolved_name": job_name,
                        "setup_path": str(setup_path),
                    },
                },
            ),
            self._file_node(
                node_id="pilot",
                label="Pilot",
                node_type="pilot",
                path=pilot_path,
                setup_path=setup_path,
                requested_value=self.facade.pilot_file,
                description="Execution order definition. It points Xspatula to process files.",
                parsed_keys=list(pilot_data.keys()) if isinstance(pilot_data, dict) else [],
            ),
            PipelineNode(
                id="process_files",
                label="Process Files",
                type="process_files",
                expandable=True,
                metadata={
                    "count": len(process_files),
                    "files": process_files,
                    "path": process_files[0]["absolute_path"] if process_files else None,
                    "absolute_path": process_files[0]["absolute_path"] if process_files else None,
                    "description": "Process files that point to executable JSON process definitions.",
                    "trace": {
                        "role": "process_files",
                        "source": "discovered_from_pilot",
                        "setup_path": str(setup_path),
                        "resolved_count": len(process_files),
                    },
                },
            ),
            PipelineNode(
                id="dispatcher",
                label="Dispatcher",
                type="dispatcher",
                metadata={
                    "path": None,
                    "absolute_path": None,
                    "description": "Reads process definitions and maps them to Python actions.",
                    "trace": {
                        "role": "dispatcher",
                        "source": "xspatula_runtime",
                        "input_files": len(job_files),
                        "setup_path": str(setup_path),
                    },
                },
            ),
            PipelineNode(
                id="python_actions",
                label="Python Actions",
                type="python_actions",
                expandable=True,
                metadata={
                    "count": len(job_files),
                    "files": job_files,
                    "path": None,
                    "absolute_path": None,
                    "description": "Resolved Python-level operations executed by Xspatula.",
                    "trace": {
                        "role": "python_actions",
                        "source": "dispatcher_resolution",
                        "input_files": len(job_files),
                        "setup_path": str(setup_path),
                    },
                },
            ),
            PipelineNode(
                id="metadata_registry",
                label="Metadata Registry",
                type="metadata_registry",
                metadata={
                    "path": str(setup_path),
                    "absolute_path": str(setup_path),
                    "description": "Stores process definitions, parameters, permissions and mappings.",
                    "trace": {
                        "role": "metadata_registry",
                        "source": "xspatula_database_metadata",
                        "setup_path": str(setup_path),
                    },
                },
            ),
            PipelineNode(
                id="postgresql",
                label="PostgreSQL",
                type="postgresql",
                metadata={
                    "engine": "postgresql",
                    "path": str(setup_path),
                    "absolute_path": str(setup_path),
                    "description": "Final persisted database state.",
                    "trace": {
                        "role": "postgresql",
                        "source": "database_engine",
                        "setup_path": str(setup_path),
                    },
                },
            ),
        ]

        edges = [
            PipelineEdge("scheme", "job", "selects"),
            PipelineEdge("job", "pilot", "loads"),
            PipelineEdge("pilot", "process_files", "resolves"),
            PipelineEdge("process_files", "dispatcher", "dispatches"),
            PipelineEdge("dispatcher", "python_actions", "maps"),
            PipelineEdge("python_actions", "metadata_registry", "registers"),
            PipelineEdge("metadata_registry", "postgresql", "persists"),
        ]

        execution = [
            {"from": "scheme", "to": "job", "tokens": 1, "parallel": False},
            {"from": "job", "to": "pilot", "tokens": 1, "parallel": False},
            {"from": "pilot", "to": "process_files", "tokens": 1, "parallel": False},
            {
                "from": "process_files",
                "to": "dispatcher",
                "tokens": token_count,
                "parallel": True,
                "burst": True,
                "token_label": "process",
            },
            {
                "from": "dispatcher",
                "to": "python_actions",
                "tokens": token_count,
                "parallel": True,
                "token_label": "action",
            },
            {
                "from": "python_actions",
                "to": "metadata_registry",
                "tokens": 1,
                "parallel": False,
                "merge": True,
            },
            {"from": "metadata_registry", "to": "postgresql", "tokens": 1, "parallel": False},
        ]

        return PipelineGraph(
            nodes=nodes,
            edges=edges,
            execution=execution,
            metadata={
                "kind": "xspatula_pipeline_graph",
                "version": "2.4.0",
                "setup_path": str(setup_path),
                "traceability": True,
                "job_name": job_name,
                "process_file_count": len(process_files),
                "job_file_count": len(job_files),
            },
        ).to_dict()

    def _file_node(
        self,
        node_id: str,
        label: str,
        node_type: str,
        path: Optional[Path],
        setup_path: Path,
        requested_value: Optional[str],
        description: str,
        parsed_keys: Optional[List[str]] = None,
    ) -> PipelineNode:
        return PipelineNode(
            id=node_id,
            label=label,
            type=node_type,
            metadata=self._file_metadata(
                path=path,
                setup_path=setup_path,
                requested_value=requested_value,
                role=node_type,
                description=description,
                parsed_keys=parsed_keys or [],
            ),
        )

    def _infer_job_name(self) -> str:
        pilot = str(self.facade.pilot_file or "").lower()

        if "setup_db" in pilot:
            return "setup_db"

        if "setup_process" in pilot:
            return "setup_processes"

        if "delete" in pilot:
            return "delete_db"

        return str(self.facade.pilot_file or "unknown_job")

    def _resolve_path(self, setup_path: Path, value: Optional[str]) -> Optional[Path]:
        if not value:
            return None

        raw = Path(value)

        if raw.is_absolute():
            return raw.resolve()

        direct = (setup_path / raw).resolve()
        if direct.exists():
            return direct

        matches = list(setup_path.rglob(raw.name))
        if matches:
            return matches[0].resolve()

        return direct

    def _read_json_safe(self, path: Optional[Path]) -> Dict[str, Any]:
        if not path or not path.exists():
            return {}

        try:
            with path.open("r", encoding="utf-8") as file:
                return json.load(file)
        except Exception:
            return {}

    def _read_text_safe(self, path: Optional[Path]) -> str:
        if not path or not path.exists():
            return ""

        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""

    def _discover_process_files(
        self,
        setup_path: Path,
        pilot_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        refs = self._extract_file_references(
            pilot_data,
            allowed_suffixes={".txt", ".json"},
        )

        files = []

        for ref in refs:
            resolved = self._resolve_path(setup_path, ref)
            if resolved:
                files.append(
                    self._file_metadata(
                        path=resolved,
                        setup_path=setup_path,
                        requested_value=ref,
                        role="process_file",
                        description="Discovered process file.",
                    )
                )

        return self._unique_files(files)

    def _discover_job_files(
        self,
        setup_path: Path,
        process_files: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        files = []

        for process_file in process_files:
            process_path = Path(process_file["absolute_path"])
            text = self._read_text_safe(process_path)

            refs = self._extract_file_references(
                text,
                allowed_suffixes={".json"},
            )

            for ref in refs:
                resolved = self._resolve_path(setup_path, ref)

                if resolved:
                    files.append(
                        self._file_metadata(
                            path=resolved,
                            setup_path=setup_path,
                            requested_value=ref,
                            role="process_json",
                            description="JSON process definition discovered from process file.",
                            discovered_from=str(process_path),
                        )
                    )

        return self._unique_files(files)

    def _extract_file_references(
        self,
        data: Any,
        allowed_suffixes: set[str],
    ) -> List[str]:
        results: List[str] = []

        def inspect_string(value: str):
            normalized = value.replace("\\", "/")
            candidates = re.findall(r"[A-Za-z0-9_\-./]+(?:\.json|\.txt)", normalized)

            for candidate in candidates:
                candidate = candidate.strip().strip('"').strip("'").strip(",")

                if any(candidate.endswith(suffix) for suffix in allowed_suffixes):
                    results.append(candidate)

        def walk(value: Any):
            if isinstance(value, dict):
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)
            elif isinstance(value, str):
                inspect_string(value)

        walk(data)
        return results

    def _file_metadata(
        self,
        path: Optional[Path],
        setup_path: Path,
        requested_value: Optional[str],
        role: str,
        description: str,
        parsed_keys: Optional[List[str]] = None,
        discovered_from: Optional[str] = None,
    ) -> Dict[str, Any]:
        exists = bool(path and path.exists())
        absolute_path = str(path.resolve()) if path else None
        relative_path = self._relative_path(path, setup_path) if path else None

        return {
            "role": role,
            "name": path.name if path else None,
            "path": absolute_path,
            "absolute_path": absolute_path,
            "relative_path": relative_path,
            "exists": exists,
            "size": path.stat().st_size if exists else None,
            "suffix": path.suffix if path else None,
            "description": description,
            "parsed_keys": parsed_keys or [],
            "trace": {
                "role": role,
                "requested_value": requested_value,
                "resolved_path": absolute_path,
                "relative_path": relative_path,
                "exists": exists,
                "discovered_from": discovered_from,
                "setup_path": str(setup_path),
            },
        }

    def _relative_path(self, path: Optional[Path], setup_path: Path) -> Optional[str]:
        if not path:
            return None

        try:
            return str(path.resolve().relative_to(setup_path.resolve()))
        except Exception:
            return str(path)

    def _unique_files(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        unique = []

        for file in files:
            key = file.get("absolute_path") or file.get("path") or file.get("name")

            if key not in seen:
                seen.add(key)
                unique.append(file)

        return unique